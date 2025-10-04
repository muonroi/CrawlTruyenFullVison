from __future__ import annotations
import asyncio
import glob
import json
import random
import sys
import time
import importlib.util
try:
    _AIOHTTP_SPEC = importlib.util.find_spec("aiohttp")
except ModuleNotFoundError:  # pragma: no cover - optional dependency missing
    _AIOHTTP_SPEC = None

if _AIOHTTP_SPEC:
    import aiohttp  # type: ignore
else:  # pragma: no cover - optional dependency missing
    aiohttp = None  # type: ignore
import os
from typing import Dict, Any, Optional, Tuple, List
try:
    _AIOGRAM_SPEC = importlib.util.find_spec("aiogram")
except ModuleNotFoundError:  # pragma: no cover - optional dependency missing
    _AIOGRAM_SPEC = None

if _AIOGRAM_SPEC:
    from aiogram import Router  # type: ignore
else:  # pragma: no cover - simple stand-in
    class Router:  # type: ignore
        def __init__(self, *args, **kwargs) -> None:
            pass
try:
    from pydantic import BaseModel  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback
    class BaseModel:  # type: ignore
        def __init__(self, **kwargs) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

        def model_dump(self) -> dict:
            return self.__dict__.copy()
from adapters.base_site_adapter import BaseSiteAdapter
from adapters.factory import get_adapter
from config.proxy_provider import load_proxies
from core.config_loader import apply_env_overrides
from utils.batch_utils import get_optimal_batch_size, smart_delay, split_batches
from utils.chapter_utils import (
    crawl_missing_chapters_for_story,
    export_chapter_metadata_sync,
    get_saved_chapters_files,
    slugify_title,
    count_dead_chapters,
    get_real_total_chapters,
    get_chapter_filename,
)
from utils.domain_utils import get_site_key_from_url, is_url_for_site
from utils.io_utils import (
    create_proxy_template_if_not_exists,
    ensure_directory_exists,
    log_failed_genre,
)
from utils.logger import logger
from config import config as app_config
from scraper import initialize_scraper
from utils.meta_utils import (
    add_missing_story,
    backup_crawl_state,
    sanitize_filename,
    save_story_metadata_file,
)
from utils.state_utils import (
    clear_specific_state_keys,
    load_crawl_state,
    merge_all_missing_workers_to_main,
    save_crawl_state,
)
from workers.crawler_missing_chapter import (
    check_and_crawl_missing_all_stories,
)
from workers.crawler_single_missing_chapter import crawl_single_story_worker

from utils.chapter_utils import slugify_title
from kafka.kafka_producer import send_job, close_producer
from workers.missing_background_loop import (
    start_missing_background_loop,
    stop_missing_background_loop,
)
from utils.skip_manager import (
    load_skipped_stories,
    mark_story_as_skipped,
    is_story_skipped,
    get_all_skipped_stories,
)

router = Router()
is_crawling = False
GENRE_SEM: asyncio.Semaphore
STORY_SEM: asyncio.Semaphore
STORY_BATCH_SIZE: int
DATA_FOLDER: str
PROXIES_FILE: str
PROXIES_FOLDER: str
FAILED_GENRES_FILE: str


def refresh_runtime_settings() -> None:
    global GENRE_SEM, STORY_SEM, STORY_BATCH_SIZE
    global DATA_FOLDER, PROXIES_FILE, PROXIES_FOLDER, FAILED_GENRES_FILE

    STORY_BATCH_SIZE = app_config.STORY_BATCH_SIZE
    GENRE_SEM = asyncio.Semaphore(app_config.GENRE_ASYNC_LIMIT)
    STORY_SEM = asyncio.Semaphore(app_config.STORY_ASYNC_LIMIT)
    DATA_FOLDER = app_config.DATA_FOLDER
    PROXIES_FILE = app_config.PROXIES_FILE
    PROXIES_FOLDER = app_config.PROXIES_FOLDER
    FAILED_GENRES_FILE = app_config.FAILED_GENRES_FILE


refresh_runtime_settings()


class WorkerSettings:
    def __init__(self, genre_batch_size, genre_async_limit, proxies_file, failed_genres_file, retry_genre_round_limit, retry_sleep_seconds):
        self.genre_batch_size = genre_batch_size
        self.genre_async_limit = genre_async_limit
        self.proxies_file = proxies_file
        self.failed_genres_file = failed_genres_file
        self.retry_genre_round_limit = retry_genre_round_limit
        self.retry_sleep_seconds = retry_sleep_seconds




async def crawl_single_story_by_title(title, site_key, genre_name=None):

    slug = slugify_title(title)
    folder = os.path.join(DATA_FOLDER, slug)
    meta_path = os.path.join(folder, "metadata.json")
    if not os.path.exists(meta_path):
        raise Exception(f"Không tìm thấy metadata cho truyện '{title}' (slug {slug})")
    with open(meta_path, "r", encoding="utf-8") as f:
        story_data_item = json.load(f)
    site_key = story_data_item.get("site_key") or get_site_key_from_url(
        story_data_item.get("url")
    )
    assert site_key, "Không xác định được site_key"
    adapter = get_adapter(site_key)
    crawl_state = {}
    await process_story_item(None, story_data_item, {}, folder, crawl_state, adapter, site_key)  # type: ignore


async def process_genre_with_limit(session, genre, crawl_state, adapter, site_key):
    async with GENRE_SEM:
        await process_genre_item(session, genre, crawl_state, adapter, site_key)


async def process_story_with_limit(
    session: aiohttp.ClientSession,
    story: Dict[str, Any],
    genre_data: Dict[str, Any],
    crawl_state: Dict[str, Any],
    adapter: BaseSiteAdapter,
    site_key: str,
) -> bool:
    async with STORY_SEM:
        slug = slugify_title(story["title"])
        folder = os.path.join(DATA_FOLDER, slug)
        await ensure_directory_exists(folder)
        return await process_story_item(
            session, story, genre_data, folder, crawl_state, adapter, site_key
        )


def sort_sources(sources):
    return sorted(sources, key=lambda s: s.get("priority", 100))


def _normalize_story_sources(
    story_data_item: Dict[str, Any],
    current_site_key: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """Merge and normalize source list between story data and metadata."""

    def _iter_raw_sources() -> List[Any]:
        raw: List[Any] = []
        if metadata and isinstance(metadata.get("sources"), list):
            raw.extend(metadata["sources"])
        if isinstance(story_data_item.get("sources"), list):
            raw.extend(story_data_item["sources"])
        primary_url = metadata.get("url") if metadata else None
        if primary_url:
            raw.append({"url": primary_url, "site_key": metadata.get("site_key")})
        current_url = story_data_item.get("url")
        if current_url:
            raw.append({"url": current_url, "site_key": current_site_key, "priority": 1})
        return raw

    raw_sources = _iter_raw_sources()
    normalized: List[Dict[str, Any]] = []
    seen: Dict[tuple[str, str], int] = {}

    def _upsert(url: Optional[str], site_key: Optional[str], priority: Any) -> None:
        if not url:
            return
        url = url.strip()
        if not url:
            return

        derived_site = get_site_key_from_url(url)
        site_candidate = site_key or derived_site or current_site_key
        if not site_candidate:
            return
        if derived_site and derived_site != site_candidate:
            site_candidate = derived_site
        if not is_url_for_site(url, site_candidate):
            return

        identity = (url, site_candidate)
        priority_val = priority if isinstance(priority, (int, float)) else None

        if identity in seen:
            existing = normalized[seen[identity]]
            if priority_val is not None:
                existing_priority = existing.get("priority")
                if (
                    not isinstance(existing_priority, (int, float))
                    or priority_val < existing_priority
                ):
                    existing["priority"] = priority_val
            return

        entry: Dict[str, Any] = {"url": url, "site_key": site_candidate}
        if priority_val is not None:
            entry["priority"] = priority_val
        normalized.append(entry)
        seen[identity] = len(normalized) - 1

    for source in raw_sources:
        if isinstance(source, str):
            _upsert(source, None, None)
        elif isinstance(source, dict):
            _upsert(source.get("url"), source.get("site_key") or source.get("site"), source.get("priority"))

    normalized_sorted = sort_sources(normalized)
    previous_story_sources = story_data_item.get("sources")
    story_data_item["sources"] = normalized_sorted

    metadata_changed = False
    if metadata is not None:
        if metadata.get("sources") != normalized_sorted:
            metadata["sources"] = normalized_sorted
            metadata_changed = True

    return metadata_changed or previous_story_sources != normalized_sorted


async def crawl_all_sources_until_full(
    site_key,
    session,
    story_data_item,
    current_discovery_genre_data,
    story_folder_path,
    crawl_state,
    num_batches=10,
    state_file=None,
    adapter: BaseSiteAdapter = None,  # type: ignore
):
    sources = sort_sources(story_data_item.get("sources", []))
    error_count_by_source = {src["url"]: 0 for src in sources}
    MAX_SOURCE_RETRY = 3

    adapter_cache: Dict[str, BaseSiteAdapter] = getattr(
        crawl_all_sources_until_full, "_adapter_cache", {}
    )
    if adapter and site_key not in adapter_cache:
        adapter_cache[site_key] = adapter
    crawl_all_sources_until_full._adapter_cache = adapter_cache  # type: ignore[attr-defined]

    total_chapters = story_data_item.get(
        "total_chapters_on_site"
    ) or story_data_item.get("total_chapters")
    if not total_chapters:
        logger.error(
            f"Không xác định được tổng số chương cho '{story_data_item['title']}'"
        )
        return

    if not sources:
        logger.warning(
            f"[AUTO-FIX] Chưa có sources cho '{story_data_item['title']}', auto thêm nguồn chính."
        )
        url = story_data_item.get("url")
        if url:
            sources = [{"url": url, "site_key": site_key, "priority": 1}]
            story_data_item["sources"] = sources
        else:
            logger.error(
                f"Không xác định được URL cho '{story_data_item['title']}', không thể auto-fix sources."
            )
            return

    retry_full = 0
    applied_chapter_limit: Optional[int] = None
    while True:
        if is_story_skipped(story_data_item):
            logger.warning(f"[SKIP][LOOP] Truyện '{story_data_item['title']}' đã bị đánh dấu skip, bỏ qua vòng lặp sources.")
            break
        files_before = len(get_saved_chapters_files(story_folder_path))
        crawled = False
        for source in sources:
            url = source.get("url")
            if not url:
                continue
            if error_count_by_source.get(url, 0) >= MAX_SOURCE_RETRY:
                logger.warning(f"[SKIP] Nguồn {url} bị lỗi quá nhiều, bỏ qua.")
                continue
            source_site_key = (
                source.get("site_key")
                or story_data_item.get("site_key")
                or get_site_key_from_url(url)
                or site_key
            )
            try:
                if source_site_key not in crawl_all_sources_until_full._adapter_cache:  # type: ignore[attr-defined]
                    crawl_all_sources_until_full._adapter_cache[source_site_key] = get_adapter(source_site_key)
                    await initialize_scraper(source_site_key)
                source_adapter = crawl_all_sources_until_full._adapter_cache[source_site_key]
            except Exception as ex:
                logger.warning(
                    f"[SOURCE] Không lấy được adapter cho site '{source_site_key}' (url={url}): {ex}"
                )
                continue
            try:
                chapters = await source_adapter.get_chapter_list(
                    story_url=url,
                    story_title=story_data_item["title"],
                    site_key=source_site_key,
                    total_chapters=total_chapters,
                    max_pages=app_config.MAX_CHAPTER_PAGES_TO_CRAWL,
                )
            except Exception as ex:
                logger.warning(f"[SOURCE] Lỗi crawl chapters từ {url}: {ex}")
                error_count_by_source[url] = error_count_by_source.get(url, 0) + 1
                continue

            limit_from_missing = await crawl_missing_chapters_for_story(
                source_site_key,
                session,
                chapters,
                story_data_item,
                current_discovery_genre_data,
                story_folder_path,
                crawl_state,
                num_batches=num_batches,
                state_file=state_file,
                adapter=source_adapter,
            )
            if isinstance(limit_from_missing, int):
                applied_chapter_limit = (
                    limit_from_missing
                    if applied_chapter_limit is None
                    else min(applied_chapter_limit, limit_from_missing)
                )
            load_skipped_stories()
            if is_story_skipped(story_data_item):
                logger.warning(f"[SKIP][AFTER CRAWL] Truyện '{story_data_item['title']}' đã bị đánh dấu skip, thoát khỏi vòng lặp sources.")
                break
            files_now = len(get_saved_chapters_files(story_folder_path))
            if (files_now + count_dead_chapters(story_folder_path)) >= (len(chapters) if chapters else (total_chapters or 0)):
                logger.info(
                    f"Đã crawl đủ chương {files_now}/{total_chapters} cho '{story_data_item['title']}' (từ nguồn {source_site_key})"
                )
                return
            crawled = True

            files_after = len(get_saved_chapters_files(story_folder_path))
            if (files_after + count_dead_chapters(story_folder_path)) >= (len(chapters) if chapters else (total_chapters or 0)):
                break
            if not crawled or files_after == files_before:
                logger.warning(
                    f"[ALERT] Đã thử hết nguồn nhưng không crawl thêm được chương nào cho '{story_data_item['title']}'. Đánh dấu skip và next truyện."
                )
                mark_story_as_skipped(story_data_item, reason="sources_fail_or_all_chapter_skipped")
                break 
            retry_full += 1
        if retry_full >= app_config.RETRY_STORY_ROUND_LIMIT:
            logger.error(
                f"[FATAL] Vượt quá retry cho truyện {story_data_item['title']}, sẽ bỏ qua."
            )
            break
        if retry_full % 20 == 0:
            logger.error(
                f"[ALERT] Truyện '{story_data_item['title']}' còn thiếu {total_chapters - files_after} chương sau {retry_full} vòng thử tất cả nguồn."
            )
        await smart_delay()

    if applied_chapter_limit is not None:
        story_data_item["_chapter_limit"] = applied_chapter_limit
    return applied_chapter_limit


async def initialize_and_log_setup_with_state(site_key) -> Tuple[str, Dict[str, Any]]:
    await ensure_directory_exists(DATA_FOLDER)
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)
    await initialize_scraper(site_key)
    try:
        if app_config.AI_PRINT_METRICS:
            from ai.selector_ai import print_ai_metrics_summary

            print_ai_metrics_summary()
    except Exception:
        pass
    homepage_url = app_config.BASE_URLS[site_key].rstrip("/") + "/"
    state_file = app_config.get_state_file(site_key)
    crawl_state = await load_crawl_state(state_file, site_key)

    logger.info("=== BẮT ĐẦU QUÁ TRÌNH CRAWL ASYNC ===")
    logger.info(f"Thư mục lưu dữ liệu: {os.path.abspath(DATA_FOLDER)}")
    logger.info(f"Sử dụng {len(app_config.LOADED_PROXIES)} proxy(s).")
    logger.info(
        f"Giới hạn: {app_config.MAX_GENRES_TO_CRAWL or 'Không giới hạn'} thể loại, "
        f"{app_config.MAX_STORIES_TOTAL_PER_GENRE or 'Không giới hạn'} truyện/thể loại."
    )
    logger.info(
        f"Giới hạn chương xử lý ban đầu/truyện: {app_config.MAX_CHAPTERS_PER_STORY or 'Không giới hạn'}."
    )
    logger.info(f"Số lượt thử lại cho các chương lỗi: {app_config.RETRY_FAILED_CHAPTERS_PASSES}.")
    logger.info(
        f"Giới hạn số trang truyện/thể loại: {app_config.MAX_STORIES_PER_GENRE_PAGE or 'Không giới hạn'}."
    )
    logger.info(
        f"Giới hạn số trang danh sách chương: {app_config.MAX_CHAPTER_PAGES_TO_CRAWL or 'Không giới hạn'}."
    )
    if crawl_state:
        loggable = {
            k: v
            for k, v in crawl_state.items()
            if k
            not in [
                "processed_chapter_urls_for_current_story",
                "globally_completed_story_urls",
            ]
        }
        if "processed_chapter_urls_for_current_story" in crawl_state:
            loggable["processed_chapters_count"] = len(
                crawl_state["processed_chapter_urls_for_current_story"]
            )
        if "globally_completed_story_urls" in crawl_state:
            loggable["globally_completed_stories_count"] = len(
                crawl_state["globally_completed_story_urls"]
            )
        logger.info(f"Tìm thấy trạng thái crawl trước đó: {loggable}")
    logger.info("-----------------------------------------")
    return homepage_url, crawl_state


async def process_story_item(
    session: aiohttp.ClientSession,
    story_data_item: Dict[str, Any],
    current_discovery_genre_data: Dict[str, Any],
    story_global_folder_path: str,
    crawl_state: Dict[str, Any],
    adapter: BaseSiteAdapter,
    site_key: str,
) -> bool:
    await ensure_directory_exists(story_global_folder_path)
    logger.info(f"\n  --- Xử lý truyện: {story_data_item['title']} ---")

    metadata_file = os.path.join(story_global_folder_path, "metadata.json")
    fields_need_check = [
        "description",
        "status",
        "source",
        "rating_value",
        "rating_count",
        "total_chapters_on_site",
    ]
    metadata = None
    need_update = False

    story_data_item.setdefault("site_key", site_key)

    # 1. Đọc hoặc cập nhật metadata nếu thiếu
    if os.path.exists(metadata_file):
        with open(metadata_file, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        for field in fields_need_check:
            if metadata.get(field) is None:
                need_update = True
                break
    else:
        need_update = True

    if need_update:
        details = await adapter.get_story_details(
            story_data_item["url"], story_data_item["title"]
        )
        await save_story_metadata_file(
            story_data_item,
            current_discovery_genre_data,
            story_global_folder_path,
            details,
            metadata,
        )
        if metadata:
            for field in fields_need_check:
                if details.get(field) is not None:  # type: ignore
                    metadata[field] = details[field]  # type: ignore
            metadata["metadata_updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
    else:
        details = metadata or {}

    # Sau khi có thể đã cập nhật, đọc lại metadata mới nhất để đồng bộ
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)
        except Exception:
            pass

    if details:
        story_data_item.update(details)

    metadata_for_update = metadata.copy() if isinstance(metadata, dict) else {}
    metadata_dirty = False

    primary_site_key = (
        metadata_for_update.get("site_key")
        or story_data_item.get("site_key")
        or site_key
    )
    if not primary_site_key:
        primary_site_key = site_key

    if story_data_item.get("site_key") != primary_site_key:
        story_data_item["site_key"] = primary_site_key

    if metadata_for_update.get("site_key") != primary_site_key:
        metadata_for_update["site_key"] = primary_site_key
        metadata_dirty = True

    if _normalize_story_sources(story_data_item, primary_site_key, metadata_for_update):
        metadata_dirty = True

    if metadata_for_update and story_data_item.get("sources"):
        # Đảm bảo thông tin nguồn có mặt trong story_data_item trước khi crawl
        story_data_item["sources"] = metadata_for_update.get(
            "sources", story_data_item.get("sources")
        )

    crawl_state["current_story_url"] = story_data_item["url"]
    if (
        crawl_state.get("previous_story_url_in_state_for_chapters")
        != story_data_item["url"]
    ):
        crawl_state["processed_chapter_urls_for_current_story"] = []
    crawl_state["previous_story_url_in_state_for_chapters"] = story_data_item["url"]
    state_file = app_config.get_state_file(site_key)
    await save_crawl_state(crawl_state, state_file, site_key=site_key)

    # 2. Crawl tất cả nguồn cho đến khi đủ chương
    chapter_limit_hint = await crawl_all_sources_until_full(
        site_key,
        session,
        story_data_item,
        current_discovery_genre_data,
        story_global_folder_path,
        crawl_state,
        num_batches=app_config.NUM_CHAPTER_BATCHES,
        state_file=state_file,
        adapter=adapter,
    )
    if isinstance(chapter_limit_hint, int) and chapter_limit_hint >= 0:
        story_data_item["_chapter_limit"] = chapter_limit_hint

    # 3. Kiểm tra số chương đã crawl thực tế
    files_actual = get_saved_chapters_files(story_global_folder_path)
    raw_total_chapters = details.get("total_chapters_on_site")  # type: ignore
    metadata_total = (
        raw_total_chapters
        if isinstance(raw_total_chapters, int) and raw_total_chapters > 0
        else None
    )
    total_chapters_on_site = metadata_total
    chapter_limit_config = story_data_item.get("_chapter_limit")
    if isinstance(chapter_limit_config, int) and chapter_limit_config >= 0:
        if total_chapters_on_site is None:
            total_chapters_on_site = chapter_limit_config
        else:
            total_chapters_on_site = min(total_chapters_on_site, chapter_limit_config)
    story_title = story_data_item["title"]
    story_url = story_data_item["url"]
    real_total_on_site: Optional[int] = None
    try:
        real_total_on_site = await get_real_total_chapters(story_data_item, adapter)
    except Exception as ex:  # pragma: no cover - network/adapter issues
        logger.warning(
            f"[DONE][STORY] Không lấy được tổng chương thực tế trên web cho '{story_title}': {ex}"
        )

    if (
        isinstance(real_total_on_site, int)
        and real_total_on_site > 0
        and isinstance(chapter_limit_config, int)
        and chapter_limit_config >= 0
    ):
        real_total_on_site = min(real_total_on_site, chapter_limit_config)

    total_candidates_for_check = [
        value
        for value in (
            total_chapters_on_site
            if isinstance(total_chapters_on_site, int) and total_chapters_on_site > 0
            else None,
            real_total_on_site
            if isinstance(real_total_on_site, int) and real_total_on_site > 0
            else None,
        )
        if value is not None
    ]
    expected_total_chapters = (
        max(total_candidates_for_check) if total_candidates_for_check else None
    )
    # Lần crawl đầu tiên (metadata vừa tạo file, chưa có file chương nào)
    is_new_crawl = os.path.exists(metadata_file) and (
        files_actual is not None and len(files_actual) == 0
    )

    if expected_total_chapters:
        crawled_chapters = len(files_actual)

        # Crawl mới hoàn toàn: chỉ log, KHÔNG kiểm tra thiếu chương
        if is_new_crawl:
            logger.info(
                f"[NEW] Đang crawl mới truyện '{story_title}': {crawled_chapters}/{expected_total_chapters} chương. Đợi crawl hoàn tất rồi mới kiểm tra thiếu chương."
            )
        else:
            # (Chỉ kiểm tra thiếu chương nếu KHÔNG phải crawl mới)
            if crawled_chapters < 0.1 * expected_total_chapters:
                logger.error(
                    f"[ALERT] Parse chương có thể lỗi HTML hoặc bị chặn: {story_title} ({crawled_chapters}/{expected_total_chapters})"
                )

            if crawled_chapters < expected_total_chapters:
                # Xác định danh sách file chương bị thiếu
                expected_files = []
                chapter_list = details.get("chapter_list", [])  # type: ignore
                for i in range(expected_total_chapters):
                    if chapter_list and i < len(chapter_list):
                        chapter_title = chapter_list[i].get("title", "untitled")
                    else:
                        chapter_title = "untitled"
                    filename = f"{i+1:04d}_{sanitize_filename(chapter_title)}.txt"
                    expected_files.append(filename)
                files_actual_set = set(files_actual)
                missing_files = [
                    fname for fname in expected_files if fname not in files_actual_set
                ]
                logger.error(
                    f"[BLOCK] Truyện '{story_title}' còn thiếu {expected_total_chapters - crawled_chapters} chương. Không next!"
                )
                logger.error(f"[BLOCK] Danh sách file chương thiếu: {missing_files}")
                await add_missing_story(
                    story_title,
                    story_url,
                    expected_total_chapters,
                    crawled_chapters,
                )
                return False
            else:
                logger.info(
                    f"Truyện '{story_title}' đã crawl đủ {crawled_chapters}/{expected_total_chapters} chương."
                )

    # 5. Đánh dấu completed và clear state
    is_complete = False
    # Hoan tat khi so file chuong >= tong so chuong tren site.
    # Khong phu thuoc truong 'status' de tranh khong next du du lieu da day du.
    total = expected_total_chapters

    if total and (len(files_actual) + count_dead_chapters(story_global_folder_path)) >= total:
        is_complete = True
        try:
            txt_count_final = len(files_actual)
            dead_count_final = count_dead_chapters(story_global_folder_path)
            logger.info(
                f"[DONE][STORY][FINAL] '{story_title}' txt+dead={txt_count_final + dead_count_final}/{total}"
            )
        except Exception:
            pass
        completed = set(crawl_state.get("globally_completed_story_urls", []))
        completed.add(story_data_item["url"])
        crawl_state["globally_completed_story_urls"] = sorted(completed)
    backup_crawl_state(state_file)
    state_file = app_config.get_state_file(site_key)
    await save_crawl_state(crawl_state, state_file, site_key=site_key)
    await clear_specific_state_keys(
        crawl_state, ["processed_chapter_urls_for_current_story"], state_file
    )
    # 6. Export lại metadata chương cho đồng bộ DB/index
    try:
        # Lấy lại list chương từ web (ưu tiên nguồn chính)
        chapters = None
        for src in story_data_item.get("sources", []):
            url = src.get("url")
            if not url:
                continue
            source_site = (
                src.get("site_key")
                or story_data_item.get("site_key")
                or get_site_key_from_url(url)
                or site_key
            )
            try:
                adapter_cache_meta: Dict[str, BaseSiteAdapter] = getattr(
                    crawl_all_sources_until_full, "_adapter_cache", {}
                )
                source_adapter = adapter_cache_meta.get(source_site)
                if not source_adapter:
                    source_adapter = get_adapter(source_site)
                    adapter_cache_meta[source_site] = source_adapter
                    crawl_all_sources_until_full._adapter_cache = adapter_cache_meta  # type: ignore[attr-defined]
                    await initialize_scraper(source_site)
            except Exception as ex:
                logger.debug(
                    f"[CHAPTER_META] Bỏ qua nguồn {url} do không lấy được adapter: {ex}"
                )
                continue

            chapters = await source_adapter.get_chapter_list(
                story_url=url,
                story_title=story_data_item.get("title"),
                site_key=source_site,
                total_chapters=expected_total_chapters,
            )
            if chapters and len(chapters) > 0:
                break
        if chapters and len(chapters) > 0:
            export_chapter_metadata_sync(story_global_folder_path, chapters)
        else:
            logger.warning(
                f"[CHAPTER_META] Không lấy được danh sách chương khi export metadata cho {story_data_item.get('title')}"
            )
    except Exception as ex:
        logger.warning(f"[CHAPTER_META] Lỗi khi export chapter_metadata.json: {ex}")

    # 7. Gửi job fallback để kiểm tra lại chương bị thiếu
    if is_complete:
        await send_job({
            "type": "check_missing_chapters",
            "story_folder_path": story_global_folder_path,
        })
    else:
        logger.info(
            f"[MISSING][SKIP] '{story_title}' chưa đủ chương (txt={len(files_actual)}) nên chưa gửi job kiểm tra missing."
        )

    if metadata_dirty:
        if metadata_for_update:
            metadata_for_write = metadata_for_update.copy()
            metadata_for_write.pop("chapters", None)
        else:
            metadata_for_write = {}
            for key in (
                "title",
                "url",
                "author",
                "cover",
                "description",
                "categories",
                "status",
                "source",
                "rating_value",
                "rating_count",
                "total_chapters_on_site",
            ):
                if story_data_item.get(key) is not None:
                    metadata_for_write[key] = story_data_item.get(key)
            metadata_for_write["metadata_updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            metadata_for_write.setdefault(
                "crawled_at", time.strftime("%Y-%m-%d %H:%M:%S")
            )
        metadata_for_write["site_key"] = primary_site_key
        metadata_for_write["sources"] = story_data_item.get("sources", [])
        try:
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata_for_write, f, ensure_ascii=False, indent=4)
        except Exception as ex:
            logger.error(
                f"[META] Không thể lưu metadata đã cập nhật nguồn cho '{story_title}': {ex}"
            )

    return is_complete


async def process_genre_item(
    session: aiohttp.ClientSession,
    genre_data: Dict[str, Any],
    crawl_state: Dict[str, Any],
    adapter: BaseSiteAdapter,
    site_key: str,
) -> None:
    logger.info(f"\n--- Xử lý thể loại: {genre_data['name']} ---")
    crawl_state["current_genre_url"] = genre_data["url"]
    if crawl_state.get("previous_genre_url_in_state_for_stories") != genre_data["url"]:
        crawl_state["current_story_index_in_genre"] = 0
    crawl_state["previous_genre_url_in_state_for_stories"] = genre_data["url"]
    state_file = app_config.get_state_file(site_key)
    await save_crawl_state(crawl_state, state_file, site_key=site_key)

    retry_time = 0
    max_retry = 5

    while True:
        try:
            (
                stories,
                total_pages,
                crawled_pages,
            ) = await adapter.get_all_stories_from_genre_with_page_check(
                genre_data["name"],
                genre_data["url"],
                site_key,
                app_config.MAX_STORIES_PER_GENRE_PAGE,
            )  # type: ignore
            if not stories or len(stories) == 0:
                raise Exception(
                    f"Danh sách truyện rỗng cho genre {genre_data['name']} ({genre_data['url']})"
                )

            if (
                total_pages
                and (crawled_pages is not None)
                and crawled_pages < total_pages
            ):
                logger.warning(
                    f"Thể loại {genre_data['name']} chỉ crawl được {crawled_pages}/{total_pages} trang, sẽ retry lần {retry_time+1}..."
                )
                retry_time += 1
                if retry_time >= max_retry:
                    logger.error(
                        f"Thể loại {genre_data['name']} không crawl đủ số trang sau {max_retry} lần."
                    )
                    log_failed_genre(genre_data)
                    return
                await asyncio.sleep(5)
                continue  # Retry tiếp
            break
        except Exception as ex:
            logger.error(
                f"Lỗi khi crawl genre {genre_data['name']} ({genre_data['url']}): {ex}"
            )
            log_failed_genre(genre_data)
            return

    completed_global = set(crawl_state.get("globally_completed_story_urls", []))
    start_idx = crawl_state.get("current_story_index_in_genre", 0)

    stories_to_process: list[tuple[int, Dict[str, Any]]] = []
    for idx, story in enumerate(stories):
        if idx < start_idx:
            continue
        if app_config.MAX_STORIES_TOTAL_PER_GENRE and idx >= app_config.MAX_STORIES_TOTAL_PER_GENRE:
            break
        stories_to_process.append((idx, story))

    if not stories_to_process:
        return

    num_batches = max(
        1,
        (len(stories_to_process) + STORY_BATCH_SIZE - 1) // STORY_BATCH_SIZE,
    )
    batches = split_batches(stories_to_process, num_batches)

    async def handle_story(idx, story):
        load_skipped_stories()
        if is_story_skipped(story):
            logger.warning(f"[SKIP] Truyện {story['title']} đã bị skip vĩnh viễn trước đó, bỏ qua.")
            return True, story["url"], idx  # Mark as done để batch không retry nữa
        async with STORY_SEM:
            slug = slugify_title(story["title"])
            folder = os.path.join(DATA_FOLDER, slug)
            await ensure_directory_exists(folder)
            if story["url"] in completed_global:
                details = await adapter.get_story_details(story["url"], story["title"])
                await save_story_metadata_file(story, genre_data, folder, details, None)
                return True, story["url"], idx
            local_state = crawl_state.copy()
            local_state["current_story_index_in_genre"] = idx
            result = await process_story_item(
                session, story, genre_data, folder, local_state, adapter, site_key
            )
            return result, story["url"], idx

    for batch in batches:
        remaining = batch
        retry_counts = {idx: 0 for idx, _ in batch}
        skipped_in_batch = []
        while remaining:
            tasks = [asyncio.create_task(handle_story(i, s)) for i, s in remaining]
            results = await asyncio.gather(*tasks)
            remaining = []
            for done, url, idx in results:
                if done:
                    completed_global.add(url)
                else:
                    retry_counts[idx] = retry_counts.get(idx, 0) + 1
                    if retry_counts[idx] >= app_config.RETRY_STORY_ROUND_LIMIT:
                        story_title = next(st['title'] for j, st in batch if j == idx)
                        story_obj = next(st for j, st in batch if j == idx)
                        logger.error(
                            f"[FATAL] Vượt quá retry cho truyện {story_title}, bỏ qua."
                        )
                        mark_story_as_skipped(story_obj, "retry quá giới hạn")
                        skipped_in_batch.append(story_title)
                        continue
                    remaining.append((idx, next(st for j, st in batch if j == idx)))
            if remaining:
                logger.warning(
                    f"[BATCH] {len(remaining)} truyện chưa đủ chương, sẽ thử lại trong batch hiện tại"
                )
                await smart_delay()
        crawl_state["globally_completed_story_urls"] = sorted(completed_global)
        crawl_state["current_story_index_in_genre"] = batch[-1][0] + 1
        await save_crawl_state(crawl_state, state_file, site_key=site_key)
        if skipped_in_batch:
            logger.warning(
                "[BATCH] Các truyện bị skip trong batch này: " + ", ".join(skipped_in_batch)
            )
            all_titles = ", ".join(
                s.get("title") for s in get_all_skipped_stories().values()
            )
            logger.info("[SKIP LIST] " + all_titles)

    await clear_specific_state_keys(
        crawl_state,
        [
            "current_story_index_in_genre",
            "current_genre_url",
            "previous_genre_url_in_state_for_stories",
        ],
        state_file,
    )


async def retry_failed_genres(
    adapter, site_key, settings: WorkerSettings, shuffle_func
):
    round_idx = 0
    while True:
        if not os.path.exists(settings.failed_genres_file):
            break
        with open(settings.failed_genres_file, "r", encoding="utf-8") as f:
            failed_genres = json.load(f)
        if not failed_genres:
            break

        round_idx += 1
        logger.warning(
            f"=== [RETRY ROUND {round_idx}] Đang retry {len(failed_genres)} thể loại bị fail... ==="
        )

        to_remove = []
        random.shuffle(failed_genres)
        import aiohttp

        async with aiohttp.ClientSession() as session:
            for genre in failed_genres:
                delay = min(60, 5 * (2 ** genre.get("fail_count", 1)))
                await smart_delay(delay)
                try:
                    from main import process_genre_with_limit

                    await process_genre_with_limit(
                        session, genre, {}, adapter, site_key
                    )
                    to_remove.append(genre)
                    logger.info(f"[RETRY] Thành công genre: {genre['name']}")
                except Exception as ex:
                    genre["fail_count"] = genre.get("fail_count", 1) + 1
                    logger.error(f"[RETRY] Vẫn lỗi genre: {genre['name']}: {ex}")

        if to_remove:
            failed_genres = [g for g in failed_genres if g not in to_remove]
            with open(settings.failed_genres_file, "w", encoding="utf-8") as f:
                json.dump(failed_genres, f, ensure_ascii=False, indent=4)

        if failed_genres:
            if round_idx < settings.retry_genre_round_limit:
                shuffle_func()
                logger.warning(
                    f"Còn {len(failed_genres)} genre fail, bắt đầu vòng retry tiếp theo..."
                )
                continue
            else:
                shuffle_func()
                genre_names = ", ".join([g["name"] for g in failed_genres])
                logger.error(
                    f"Sleep {settings.retry_sleep_seconds // 60} phút rồi retry lại các genre fail: {genre_names}"
                )
                await asyncio.sleep(settings.retry_sleep_seconds)
                round_idx = 0
                continue
        else:
            logger.info("Tất cả genre fail đã retry thành công.")
            break


async def run_genres(
    site_key: str,
    settings: WorkerSettings,
    crawl_state: Optional[Dict[str, Any]] = None,
):
    logger.info(f"[GENRE] Bắt đầu crawl thể loại cho site: {site_key}")
    await initialize_scraper(site_key)
    adapter = get_adapter(site_key)
    genres = await adapter.get_genres()
    if app_config.MAX_GENRES_TO_CRAWL:
        limited_genres = genres[: app_config.MAX_GENRES_TO_CRAWL]
        if len(limited_genres) != len(genres):
            logger.info(
                f"[GENRE] Áp dụng giới hạn {app_config.MAX_GENRES_TO_CRAWL} thể loại đầu tiên (từ tổng {len(genres)})."
            )
        genres = limited_genres
    await run_crawler(adapter, site_key, genres, settings, crawl_state)


async def run_missing(site_key: str, homepage_url: Optional[str] = None):
    homepage_url = homepage_url or app_config.BASE_URLS[site_key].rstrip("/") + "/"
    logger.info("[MISSING] Bắt đầu crawl chương thiếu...")
    await initialize_scraper(site_key)
    adapter = get_adapter(site_key)
    await check_and_crawl_missing_all_stories(adapter, homepage_url, site_key=site_key)


async def crawl_all_missing_stories(
    site_key: Optional[str] = None,
    homepage_url: Optional[str] = None,
    force_unskip: bool = False,
):
    """Trigger crawl for missing chapters.

    Nếu truyền site_key → chỉ xử lý site đó (tái sử dụng run_missing).
    Nếu không → gọi worker để quét toàn bộ site.
    """

    if site_key:
        await run_missing(site_key, homepage_url)
    else:
        await loop_once_multi_sites(force_unskip=force_unskip)


async def run_single_story(
    title: str, site_key: Optional[str] = None, genre_name: Optional[str] = None
):
    from main import crawl_single_story_by_title

    logger.info(
        f"[SINGLE] Crawl truyện '{title}' (site: {site_key or 'auto-detect'})..."
    )
    await crawl_single_story_by_title(title, site_key, genre_name)


async def run_crawler(
    adapter,
    site_key,
    genres,
    settings: WorkerSettings,
    crawl_state: Optional[Dict[str, Any]] = None,
):
    state_file = get_state_file(site_key)
    crawl_state = crawl_state or await load_crawl_state(state_file, site_key)
    total_genres = len(genres)
    genres_done = 0

    async with aiohttp.ClientSession() as session:
        batch_size = settings.genre_batch_size
        batches = split_batches(
            genres, max(1, (len(genres) + batch_size - 1) // batch_size)
        )
        for batch_idx, genre_batch in enumerate(batches):
            tasks = []
            for genre in genre_batch:
                tasks.append(
                    process_genre_with_limit(
                        session, genre, crawl_state, adapter, site_key
                    )
                )
            logger.info(
                f"=== Đang crawl batch thể loại {batch_idx + 1}/{len(batches)} ({len(genre_batch)} genres song song) ==="
            )
            results = await asyncio.gather(*tasks)
            genres_done += len(genre_batch)
            percent = int(genres_done * 100 / total_genres)
            msg = f"⏳ Tiến độ: {genres_done}/{total_genres} thể loại ({percent}%) đã crawl xong cho {site_key}."
            logger.info(msg)
            await smart_delay()

    logger.info("=== HOÀN TẤT TOÀN BỘ QUÁ TRÌNH CRAWL ===")


async def run_all_sites(crawl_mode: Optional[str] = None):
    """Run crawler for all configured sites in parallel."""
    tasks = []

    for site_key in app_config.BASE_URLS.keys():
        async def run_site(key=site_key):
            try:
                await run_single_site(key, crawl_mode=crawl_mode)
            except Exception as e:
                logger.error(f"[MAIN] Site {key} failed: {e}")
        tasks.append(asyncio.create_task(run_site()))

    await asyncio.gather(*tasks)



async def run_retry_passes(site_key: str):
    passes = app_config.RETRY_FAILED_CHAPTERS_PASSES
    if not passes or passes <= 0:
        return

    logger.info(f"=== BẮT ĐẦU CÁC LƯỢT RETRY CHƯƠNG ĐÃ LỖI (PASSES={passes}) ===")
    for i in range(passes):
        logger.info(f"--- Lượt {i+1}/{passes} ---")
        
        dead_files = glob.glob(os.path.join(app_config.DATA_FOLDER, "**", "dead_chapters.json"), recursive=True)
        if not dead_files:
            logger.info("Không tìm thấy chương nào bị đánh dấu 'dead'. Bỏ qua lượt này.")
            break

        total_queued = 0
        for dead_file in dead_files:
            try:
                with open(dead_file, 'r', encoding='utf-8') as f:
                    dead_chapters = json.load(f)
                
                story_folder = os.path.dirname(dead_file)
                metadata_path = os.path.join(story_folder, "metadata.json")
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)

                for chapter in dead_chapters:
                    job = {
                        "type": "retry_chapter",
                        "site_key": metadata.get("site_key"),
                        "chapter_url": chapter.get("url"),
                        "chapter_title": chapter.get("title"),
                        "story_title": metadata.get("title"),
                        "filename": os.path.join(story_folder, get_chapter_filename(chapter.get("title"), chapter.get("index")))
                    }
                    await send_job(job)
                    total_queued += 1
                
                # Xóa file sau khi đã gửi job
                os.remove(dead_file)
                logger.info(f"Đã gửi {len(dead_chapters)} job retry từ file {dead_file} và xóa file.")

            except Exception as e:
                logger.error(f"Lỗi khi xử lý file dead_chapters: {dead_file} - {e}")

        if total_queued == 0:
            logger.info("Không có chương nào cần retry trong lượt này.")
            break

        if i < passes - 1:
            logger.info(f"Đã gửi {total_queued} job. Chờ {app_config.RETRY_SLEEP_SECONDS} giây trước khi bắt đầu lượt tiếp theo...")
            await asyncio.sleep(app_config.RETRY_SLEEP_SECONDS)

    logger.info("=== KẾT THÚC CÁC LƯỢT RETRY ===")


async def run_single_site(
    site_key: str,
    env_overrides: Optional[Dict[str, str]] = None,
    crawl_mode: Optional[str] = None,
):
    from config.proxy_provider import shuffle_proxies

    if env_overrides:
        apply_env_overrides({"env_override": env_overrides})

    logger.info(f"[MAIN] Đang chạy crawler cho site: {site_key} với mode={crawl_mode}")
    load_skipped_stories()
    merge_all_missing_workers_to_main(site_key)
    homepage_url, crawl_state = await initialize_and_log_setup_with_state(site_key)

    background_started = False
    try:
        await start_missing_background_loop()
        background_started = True
    except Exception as ex:  # pragma: no cover - defensive guard
        logger.error(f"[MISSING][BACKGROUND] Không thể khởi động background loop: {ex}")

    settings = WorkerSettings(
        genre_batch_size=app_config.GENRE_BATCH_SIZE,
        genre_async_limit=app_config.GENRE_ASYNC_LIMIT,
        proxies_file=PROXIES_FILE,
        failed_genres_file=FAILED_GENRES_FILE,
        retry_genre_round_limit=app_config.RETRY_GENRE_ROUND_LIMIT,
        retry_sleep_seconds=app_config.RETRY_SLEEP_SECONDS,
    )

    try:
        if crawl_mode == "genres_only":
            await run_genres(site_key, settings, crawl_state)
            await retry_failed_genres(
                get_adapter(site_key), site_key, settings, shuffle_proxies
            )
        elif crawl_mode == "missing_only":
            await crawl_all_missing_stories(site_key, homepage_url)
        elif crawl_mode == "missing_single":
            url = next(
                (arg.split("=")[1] for arg in sys.argv if arg.startswith("--url=")), None
            )
            title = next(
                (arg.split("=")[1] for arg in sys.argv if arg.startswith("--title=")), None
            )
            await crawl_single_story_worker(story_url=url, title=title)
            await crawl_all_missing_stories(site_key, homepage_url)
        else:
            await run_genres(site_key, settings, crawl_state)
            await retry_failed_genres(
                get_adapter(site_key), site_key, settings, shuffle_proxies
            )
            await crawl_all_missing_stories(site_key, homepage_url)

        # Cuối cùng, chạy các lượt retry cho các chương đã thất bại
        await run_retry_passes(site_key)
    finally:
        if background_started:
            await stop_missing_background_loop()


if __name__ == "__main__":
    mode = app_config.DEFAULT_MODE or (sys.argv[1] if len(sys.argv) > 1 else None)
    crawl_mode = app_config.DEFAULT_CRAWL_MODE or (sys.argv[2] if len(sys.argv) > 2 else None)

    try:
        # If mode is 'full', treat it as 'all_sites' for backward compatibility and default runs
        if mode == 'full':
            mode = 'all_sites'

        if mode == "all_sites":
            asyncio.run(run_all_sites(crawl_mode=crawl_mode))
        elif mode and mode in app_config.BASE_URLS.keys():
            asyncio.run(run_single_site(site_key=mode, crawl_mode=crawl_mode))
        else:
            valid_keys = ', '.join(app_config.BASE_URLS.keys())
            print(f"❌ Chế độ '{mode}' không hợp lệ. Hãy truyền một site_key hợp lệ ({valid_keys}) hoặc 'all_sites'.")
            sys.exit(1)
    finally:
        asyncio.run(close_producer())
