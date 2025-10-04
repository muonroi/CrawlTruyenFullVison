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
)
from utils.domain_utils import get_site_key_from_url
from utils.logger import logger
from utils.skip_manager import (
    is_story_skipped,
    load_skipped_stories,
    mark_story_as_skipped,
)


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

    async def crawl_story_until_complete(
        self, request: StoryCrawlRequest
    ) -> Optional[int]:
        """Attempt to crawl all chapters for ``request.story_data_item``.

        The method keeps the retry semantics and logging from the previous
        implementation while exposing a cleaner surface that can be re-used by
        other workers.
        """

        story = request.story_data_item
        total_chapters = story.get("total_chapters_on_site") or story.get("total_chapters")
        if not total_chapters:
            logger.error(f"Không xác định được tổng số chương cho '{story['title']}'")
            return None

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
                return None

        if request.adapter and request.site_key not in self._adapter_cache:
            self._adapter_cache[request.site_key] = request.adapter

        error_count_by_source: Dict[str, int] = {source.url: 0 for source in sources}
        max_retry = 3
        retry_round = 0
        applied_chapter_limit: Optional[int] = None

        while True:
            if is_story_skipped(story):
                logger.warning(
                    f"[SKIP][LOOP] Truyện '{story['title']}' đã bị đánh dấu skip, bỏ qua vòng lặp sources."
                )
                break

            files_before = len(get_saved_chapters_files(request.story_folder_path))
            files_after = files_before
            crawled_any = False

            for source in sources:
                if error_count_by_source.get(source.url, 0) >= max_retry:
                    logger.warning(f"[SKIP] Nguồn {source.url} bị lỗi quá nhiều, bỏ qua.")
                    continue

                try:
                    adapter = await self._ensure_adapter(source.site_key)
                except Exception as ex:  # pragma: no cover - network/IO guard
                    logger.warning(
                        f"[SOURCE] Không lấy được adapter cho site '{source.site_key}' (url={source.url}): {ex}"
                    )
                    error_count_by_source[source.url] = error_count_by_source.get(source.url, 0) + 1
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
                    logger.warning(f"[SOURCE] Lỗi crawl chapters từ {source.url}: {ex}")
                    error_count_by_source[source.url] = error_count_by_source.get(source.url, 0) + 1
                    continue

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

                expected_total = len(chapters) if chapters else (total_chapters or 0)
                if (files_after + count_dead_chapters(request.story_folder_path)) >= expected_total:
                    logger.info(
                        f"Đã crawl đủ chương {files_after}/{total_chapters} cho '{story['title']}' (từ nguồn {source.site_key})"
                    )
                    if applied_chapter_limit is not None:
                        story["_chapter_limit"] = applied_chapter_limit
                    return applied_chapter_limit

            if not crawled_any or files_after == files_before:
                logger.warning(
                    f"[ALERT] Đã thử hết nguồn nhưng không crawl thêm được chương nào cho '{story['title']}'. Đánh dấu skip và next truyện."
                )
                mark_story_as_skipped(story, reason="sources_fail_or_all_chapter_skipped")
                break

            retry_round += 1
            if retry_round >= app_config.RETRY_STORY_ROUND_LIMIT:
                logger.error(
                    f"[FATAL] Vượt quá retry cho truyện {story['title']}, sẽ bỏ qua."
                )
                break

            if retry_round % 20 == 0:
                missing = max(total_chapters - files_after, 0)
                logger.error(
                    f"[ALERT] Truyện '{story['title']}' còn thiếu {missing} chương sau {retry_round} vòng thử tất cả nguồn."
                )

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


# Singleton used across modules to avoid recreating the cache repeatedly.
multi_source_story_crawler = MultiSourceStoryCrawler()

