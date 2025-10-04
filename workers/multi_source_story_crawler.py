"""Utilities for coordinating multi-source story crawling.

This module centralises the logic that was previously scattered between
``main.py`` and ``workers/crawler_missing_chapter.py``. The goal is to make the
behaviour around iterating story sources, loading adapters and invoking the
chapter pipeline consistent for both the full crawl and the missing chapter
crawl flows.

The :class:`MultiSourceStoryCrawler` class exposes a single entry point
(:meth:`crawl_story_until_complete`) which mirrors the previous
``crawl_all_sources_until_full`` function while keeping the implementation
encapsulated and easier to test.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from adapters.base_site_adapter import BaseSiteAdapter
from adapters.factory import get_adapter
from config import config as app_config
from scraper import initialize_scraper
from utils.batch_utils import smart_delay
from utils.chapter_utils import (
    count_dead_chapters,
    crawl_missing_chapters_for_story,
    get_saved_chapters_files,
    slugify_title,
)
from utils.domain_utils import get_site_key_from_url
from utils.health_monitor import site_health_monitor
from utils.logger import (
    anti_bot_logger,
    chapter_error_logger,
    logger,
    progress_logger,
)
from utils.metrics_tracker import metrics_tracker
from utils.skip_manager import (
    is_story_skipped,
    load_skipped_stories,
    mark_story_as_skipped,
)
from utils.errors import CrawlError, classify_crawl_exception, is_retryable_error


DEFAULT_SOURCE_COOLDOWN_SECONDS = 180.0
DEFAULT_SITE_COOLDOWN_SECONDS = 420.0
DEFAULT_STORY_COOLDOWN_SECONDS = 900.0
DEFAULT_COOLDOWN_BACKOFF = 2.0


@dataclass(slots=True)
class SourceFailureState:
    """Keep track of the health of a particular story source."""

    total_failures: int = 0
    permanent_failures: int = 0
    transient_failures: int = 0
    cooldown_until: float = 0.0
    last_error: CrawlError = CrawlError.UNKNOWN

    def register_success(self) -> None:
        self.transient_failures = 0
        self.last_error = CrawlError.UNKNOWN
        self.cooldown_until = 0.0

    def register_failure(
        self,
        error: CrawlError,
        now: float,
        base_cooldown: float,
        backoff_multiplier: float,
        max_retry: int,
    ) -> None:
        self.total_failures += 1
        self.last_error = error
        if is_retryable_error(error):
            self.transient_failures += 1
            delay = base_cooldown * (backoff_multiplier ** (self.transient_failures - 1))
            self.cooldown_until = max(self.cooldown_until, now + delay)
        else:
            self.permanent_failures = max(self.permanent_failures, max_retry)
            self.transient_failures = 0
            self.cooldown_until = 0.0

    def is_in_cooldown(self, now: float) -> bool:
        return self.cooldown_until > now

    def is_exhausted(self, max_retry: int) -> bool:
        if self.permanent_failures >= max_retry:
            return True
        return self.total_failures >= max_retry and not is_retryable_error(self.last_error)


@dataclass(slots=True)
class StorySource:
    """Representation of a crawlable source for a story."""

    url: str
    site_key: str
    priority: int = 100


@dataclass(slots=True)
class StoryCrawlRequest:
    """Parameters required to run a multi-source crawl."""

    site_key: str
    session: Any  # ``aiohttp.ClientSession`` in runtime, kept generic for tests
    story_data_item: Dict[str, Any]
    current_discovery_genre_data: Dict[str, Any]
    story_folder_path: str
    crawl_state: Dict[str, Any]
    num_batches: int = 10
    state_file: Optional[str] = None
    adapter: Optional[BaseSiteAdapter] = None


def _sort_sources(sources: Iterable[Dict[str, Any]]) -> List[StorySource]:
    """Normalise and sort raw source dictionaries.

    The logic mirrors the historical implementation in ``main.py`` but returns
    :class:`StorySource` instances to keep the rest of the pipeline strongly
    typed.
    """

    result: List[StorySource] = []
    for raw in sources:
        if not isinstance(raw, dict):
            continue
        url = raw.get("url")
        if not url:
            continue
        site_key = (
            raw.get("site_key")
            or raw.get("site")
            or get_site_key_from_url(url)
        )
        if not site_key:
            continue
        priority = raw.get("priority") if isinstance(raw.get("priority"), int) else 100
        result.append(StorySource(url=url, site_key=site_key, priority=priority))

    return sorted(result, key=lambda s: s.priority)


class MultiSourceStoryCrawler:
    """Coordinate crawling a story across multiple mirrored sources."""

    def __init__(self) -> None:
        self._adapter_cache: Dict[str, BaseSiteAdapter] = {}
        self._source_failures: Dict[str, SourceFailureState] = {}
        self._site_cooldowns: Dict[str, float] = {}
        self._source_cooldown_seconds = float(
            getattr(app_config, "SOURCE_COOLDOWN_SECONDS", DEFAULT_SOURCE_COOLDOWN_SECONDS)
        )
        self._site_cooldown_seconds = float(
            getattr(app_config, "SITE_COOLDOWN_SECONDS", DEFAULT_SITE_COOLDOWN_SECONDS)
        )
        self._story_cooldown_seconds = float(
            getattr(app_config, "STORY_COOLDOWN_SECONDS", DEFAULT_STORY_COOLDOWN_SECONDS)
        )
        self._cooldown_backoff = float(
            getattr(app_config, "COOLDOWN_BACKOFF_MULTIPLIER", DEFAULT_COOLDOWN_BACKOFF)
        )

    async def crawl_story_until_complete(
        self, request: StoryCrawlRequest
    ) -> Optional[int]:
        """Attempt to crawl all chapters for ``request.story_data_item``.

        The method keeps the retry semantics and logging from the previous
        implementation while exposing a cleaner surface that can be re-used by
        other workers.
        """

        story = request.story_data_item
        story_slug = slugify_title(story["title"])
        total_chapters = story.get("total_chapters_on_site") or story.get("total_chapters")
        if not total_chapters:
            logger.error(f"Không xác định được tổng số chương cho '{story['title']}'")
            metrics_tracker.story_failed(story_slug, "missing_total_chapters")
            return None

        story_url = story.get("url")
        story_cooldowns = request.crawl_state.setdefault("story_cooldowns", {})
        current_ts = time.time()
        if story_url:
            cooldown_until = max(
                float(story.get("_cooldown_until") or 0),
                float(story_cooldowns.get(story_url) or 0),
            )
            if cooldown_until and cooldown_until > current_ts:
                human_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cooldown_until))
                logger.info(
                    f"[COOLDOWN][STORY] '{story['title']}' đang trong thời gian chờ đến {human_ts}, bỏ qua lượt này."
                )
                metrics_tracker.story_on_cooldown(story_slug, cooldown_until)
                story["_cooldown_until"] = cooldown_until
                return None
            story_cooldowns.pop(story_url, None)
            story.pop("_cooldown_until", None)

        sources = _sort_sources(story.get("sources", []))
        if not sources:
            url = story.get("url")
            if url:
                logger.warning(
                    f"[AUTO-FIX] Chưa có sources cho '{story['title']}', auto thêm nguồn chính."
                )
                sources = [StorySource(url=url, site_key=request.site_key, priority=1)]
                story["sources"] = [
                    {"url": source.url, "site_key": source.site_key, "priority": source.priority}
                    for source in sources
                ]
            else:
                logger.error(
                    f"Không xác định được URL cho '{story['title']}', không thể auto-fix sources."
                )
                metrics_tracker.story_failed(story_slug, "missing_sources")
                return None

        metrics_tracker.story_started(
            story_slug,
            story["title"],
            total_chapters,
            primary_site=request.site_key,
        )
        progress_logger.info(
            f"[START] Bắt đầu crawl '{story['title']}' với {total_chapters} chương (site chính: {request.site_key})."
        )

        if request.adapter and request.site_key not in self._adapter_cache:
            self._adapter_cache[request.site_key] = request.adapter

        max_retry = 3
        retry_round = 0
        applied_chapter_limit: Optional[int] = None

        while True:
            current_ts = time.time()
            if is_story_skipped(story):
                logger.warning(
                    f"[SKIP][LOOP] Truyện '{story['title']}' đã bị đánh dấu skip, bỏ qua vòng lặp sources."
                )
                break

            files_before = len(get_saved_chapters_files(request.story_folder_path))
            files_after = files_before
            prev_files_after = files_before
            crawled_any = False

            for source in sources:
                state = self._source_failures.setdefault(source.url, SourceFailureState())
                now_monotonic = time.monotonic()
                if state.is_exhausted(max_retry):
                    logger.warning(f"[SKIP] Nguồn {source.url} bị lỗi quá nhiều, bỏ qua.")
                    continue

                site_cooldown_until = self._site_cooldowns.get(source.site_key)
                if site_cooldown_until and site_cooldown_until > current_ts:
                    logger.info(
                        f"[COOLDOWN][SITE] Tạm hoãn crawl {source.site_key} đến {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(site_cooldown_until))}"
                    )
                    continue

                if state.is_in_cooldown(now_monotonic):
                    logger.debug(
                        f"[COOLDOWN][SOURCE] {source.url} đang chờ thêm {state.cooldown_until - now_monotonic:.1f}s trước khi thử lại"
                    )
                    continue

                try:
                    adapter = await self._ensure_adapter(source.site_key)
                except Exception as ex:  # pragma: no cover - network/IO guard
                    logger.warning(
                        f"[SOURCE] Không lấy được adapter cho site '{source.site_key}' (url={source.url}): {ex}"
                    )
                    error_type = classify_crawl_exception(ex)
                    state.register_failure(
                        error_type,
                        now_monotonic,
                        self._source_cooldown_seconds,
                        self._cooldown_backoff,
                        max_retry,
                    )
                    site_health_monitor.record_failure(source.site_key, error_type)
                    metrics_tracker.update_story_progress(
                        story_slug,
                        last_source=source.site_key,
                        last_error=error_type.value,
                    )
                    if error_type in {CrawlError.ANTI_BOT, CrawlError.RATE_LIMIT}:
                        self._apply_site_cooldown(source.site_key)
                    continue

                try:
                    chapters = await adapter.get_chapter_list(
                        story_url=source.url,
                        story_title=story["title"],
                        site_key=source.site_key,
                        total_chapters=total_chapters,
                        max_pages=app_config.MAX_CHAPTER_PAGES_TO_CRAWL,
                    )
                except Exception as ex:  # pragma: no cover - adapter/network guard
                    error_type = classify_crawl_exception(ex)
                    state.register_failure(
                        error_type,
                        now_monotonic,
                        self._source_cooldown_seconds,
                        self._cooldown_backoff,
                        max_retry,
                    )
                    self._log_source_error(source, error_type, ex)
                    site_health_monitor.record_failure(source.site_key, error_type)
                    metrics_tracker.update_story_progress(
                        story_slug,
                        last_source=source.site_key,
                        last_error=error_type.value,
                    )
                    if error_type in {CrawlError.ANTI_BOT, CrawlError.RATE_LIMIT}:
                        self._apply_site_cooldown(source.site_key)
                    continue

                state.register_success()
                site_health_monitor.record_success(source.site_key)
                limit_from_missing = await crawl_missing_chapters_for_story(
                    source.site_key,
                    request.session,
                    chapters,
                    story,
                    request.current_discovery_genre_data,
                    request.story_folder_path,
                    request.crawl_state,
                    num_batches=request.num_batches,
                    state_file=request.state_file,
                    adapter=adapter,
                )

                if isinstance(limit_from_missing, int):
                    applied_chapter_limit = (
                        limit_from_missing
                        if applied_chapter_limit is None
                        else min(applied_chapter_limit, limit_from_missing)
                    )

                load_skipped_stories()
                if is_story_skipped(story):
                    logger.warning(
                        f"[SKIP][AFTER CRAWL] Truyện '{story['title']}' đã bị đánh dấu skip, thoát khỏi vòng lặp sources."
                    )
                    break

                files_after = len(get_saved_chapters_files(request.story_folder_path))
                crawled_any = True
                new_files = max(files_after - prev_files_after, 0)
                prev_files_after = files_after

                dead_chapters = count_dead_chapters(request.story_folder_path)
                expected_total = len(chapters) if chapters else (total_chapters or 0)
                missing = max(expected_total - (files_after + dead_chapters), 0)
                metrics_tracker.update_story_progress(
                    story_slug,
                    crawled_chapters=files_after,
                    missing_chapters=missing,
                    last_source=source.site_key,
                )
                if new_files > 0:
                    progress_logger.info(
                        f"[PROGRESS] '{story['title']}' đã crawl thêm {new_files} chương (còn thiếu {missing})."
                    )

                if (files_after + dead_chapters) >= expected_total:
                    logger.info(
                        f"Đã crawl đủ chương {files_after}/{total_chapters} cho '{story['title']}' (từ nguồn {source.site_key})"
                    )
                    metrics_tracker.story_completed(story_slug)
                    progress_logger.info(
                        f"[DONE] Hoàn thành '{story['title']}' với {files_after} chương."
                    )
                    if applied_chapter_limit is not None:
                        story["_chapter_limit"] = applied_chapter_limit
                    return applied_chapter_limit

            if not crawled_any or files_after == files_before:
                if self._has_viable_sources(sources, max_retry):
                    cooldown_until = current_ts + self._story_cooldown_seconds
                    story["_cooldown_until"] = cooldown_until
                    if story_url:
                        story_cooldowns[story_url] = cooldown_until
                    human_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cooldown_until))
                    logger.warning(
                        f"[COOLDOWN][STORY] Đặt truyện '{story['title']}' vào hàng chờ đến {human_ts} do lỗi tạm thời từ các nguồn."
                    )
                    metrics_tracker.story_on_cooldown(story_slug, cooldown_until)
                    metrics_tracker.update_story_progress(
                        story_slug,
                        status="cooldown",
                        last_error="temporary_failures",
                        cooldown_until=cooldown_until,
                    )
                    progress_logger.warning(
                        f"[COOLDOWN] '{story['title']}' tạm nghỉ đến {human_ts} vì lỗi tạm thời."
                    )
                    break

                logger.warning(
                    f"[ALERT] Đã thử hết nguồn nhưng không crawl thêm được chương nào cho '{story['title']}'. Đánh dấu skip và next truyện."
                )
                metrics_tracker.story_failed(story_slug, "exhausted_sources")
                mark_story_as_skipped(story, reason="sources_fail_or_all_chapter_skipped")
                break

            retry_round += 1
            if retry_round >= app_config.RETRY_STORY_ROUND_LIMIT:
                logger.error(
                    f"[FATAL] Vượt quá retry cho truyện {story['title']}, sẽ bỏ qua."
                )
                metrics_tracker.story_failed(story_slug, "retry_limit_exceeded")
                break

            if retry_round % 20 == 0:
                missing = max(total_chapters - files_after, 0)
                message = (
                    f"[ALERT] Truyện '{story['title']}' còn thiếu {missing} chương sau {retry_round} vòng thử tất cả nguồn."
                )
                logger.error(message)
                chapter_error_logger.error(message)

            await smart_delay()

        if applied_chapter_limit is not None:
            story["_chapter_limit"] = applied_chapter_limit
        return applied_chapter_limit

    async def _ensure_adapter(self, site_key: str) -> BaseSiteAdapter:
        """Return (and cache) an adapter for ``site_key``."""

        if site_key not in self._adapter_cache:
            adapter = get_adapter(site_key)
            await initialize_scraper(site_key)
            self._adapter_cache[site_key] = adapter
        return self._adapter_cache[site_key]

    def _apply_site_cooldown(self, site_key: str) -> None:
        cooldown_until = time.time() + self._site_cooldown_seconds
        self._site_cooldowns[site_key] = cooldown_until
        logger.warning(
            f"[COOLDOWN][SITE] Site '{site_key}' gặp lỗi tạm thời, sẽ thử lại sau {self._site_cooldown_seconds:.0f}s."
        )

    @staticmethod
    def _log_source_error(source: StorySource, error_type: CrawlError, exc: Exception) -> None:
        prefix = "[SOURCE]"
        target_logger = logger
        if error_type in {CrawlError.ANTI_BOT, CrawlError.RATE_LIMIT}:
            target_logger = anti_bot_logger
        elif error_type in {CrawlError.NOT_FOUND, CrawlError.DEAD_LINK}:
            target_logger = chapter_error_logger

        if error_type == CrawlError.NOT_FOUND:
            message = (
                f"{prefix} Nguồn {source.url} trả về 404/không tồn tại. Sẽ không retry nhiều lần."
            )
        elif error_type == CrawlError.ANTI_BOT:
            message = (
                f"{prefix} Bị chặn anti-bot khi crawl {source.url}. Sẽ tạm dừng trước khi retry."
            )
        elif error_type == CrawlError.RATE_LIMIT:
            message = (
                f"{prefix} Gặp giới hạn truy cập khi crawl {source.url}. Sẽ chuyển sang nguồn khác tạm thời."
            )
        else:
            message = f"{prefix} Lỗi '{error_type.value}' khi crawl {source.url}: {exc}"

        target_logger.warning(message)
        if target_logger is not logger:
            logger.warning(message)

    def _has_viable_sources(self, sources: List[StorySource], max_retry: int) -> bool:
        now_ts = time.time()
        for source in sources:
            state = self._source_failures.get(source.url)
            if state is None:
                return True
            if not state.is_exhausted(max_retry):
                return True
            if is_retryable_error(state.last_error) and state.total_failures < max_retry:
                return True
            site_cooldown = self._site_cooldowns.get(source.site_key)
            if site_cooldown and site_cooldown > now_ts:
                return True
        return False


# Singleton used across modules to avoid recreating the cache repeatedly.
multi_source_story_crawler = MultiSourceStoryCrawler()

