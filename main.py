import asyncio
import glob
import json
import random
import sys
import time
import aiohttp
import os
from typing import Dict, Any, Optional, Tuple
from aiogram import Router
from pydantic import BaseModel
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
)
from utils.domain_utils import get_site_key_from_url
from utils.io_utils import (
    create_proxy_template_if_not_exists,
    ensure_directory_exists,
    log_failed_genre,
)
from utils.logger import logger
from config.config import (
    GENRE_ASYNC_LIMIT,
    STORY_ASYNC_LIMIT,
    STORY_BATCH_SIZE,
    LOADED_PROXIES,
    get_state_file,
    RETRY_STORY_ROUND_LIMIT,
)
from config.config import (
    BASE_URLS,
    DATA_FOLDER,
    PROXIES_FILE,
    PROXIES_FOLDER,
    MAX_GENRES_TO_CRAWL,
    MAX_STORIES_PER_GENRE_PAGE,
    MAX_STORIES_TOTAL_PER_GENRE,
    MAX_CHAPTERS_PER_STORY,
    MAX_CHAPTER_PAGES_TO_CRAWL,
    RETRY_FAILED_CHAPTERS_PASSES,
    SKIPPED_STORIES_FILE
)
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
from workers.crawler_missing_chapter import check_and_crawl_missing_all_stories
from workers.crawler_single_missing_chapter import crawl_single_story_worker
from workers.missing_chapter_worker import crawl_all_missing_stories
from utils.chapter_utils import slugify_title
from utils.skip_manager import (
    load_skipped_stories,
    mark_story_as_skipped,
    is_story_skipped,
    get_all_skipped_stories,
)

router = Router()
is_crawling = False
GENRE_SEM = asyncio.Semaphore(GENRE_ASYNC_LIMIT)
STORY_SEM = asyncio.Semaphore(STORY_ASYNC_LIMIT)

    
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
            try:
                chapters = await adapter.get_chapter_list( 
                    story_url=url,
                    story_title=story_data_item["title"],
                    site_key=site_key,
                    total_chapters=total_chapters,
                )
            except Exception as ex:
                logger.warning(f"[SOURCE] Lỗi crawl chapters từ {url}: {ex}")
                error_count_by_source[url] = error_count_by_source.get(url, 0) + 1
                continue

            await crawl_missing_chapters_for_story(
                site_key,
                session,
                chapters,
                story_data_item,
                current_discovery_genre_data,
                story_folder_path,
                crawl_state,
                num_batches=num_batches,
                state_file=state_file,
                adapter=adapter,
            )
            load_skipped_stories()
            if is_story_skipped(story_data_item):
                logger.warning(f"[SKIP][AFTER CRAWL] Truyện '{story_data_item['title']}' đã bị đánh dấu skip, thoát khỏi vòng lặp sources.")
                break
            files_now = len(get_saved_chapters_files(story_folder_path))
            if (files_now + count_dead_chapters(story_folder_path)) >= (len(chapters) if chapters else (total_chapters or 0)):
                logger.info(
                    f"Đã crawl đủ chương {files_now}/{total_chapters} cho '{story_data_item['title']}' (từ nguồn {source.get('site_key')})"
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
        if retry_full >= RETRY_STORY_ROUND_LIMIT:
            logger.error(
                f"[FATAL] Vượt quá retry cho truyện {story_data_item['title']}, sẽ bỏ qua."
            )
            break
        if retry_full % 20 == 0:
            logger.error(
                f"[ALERT] Truyện '{story_data_item['title']}' còn thiếu {total_chapters - files_after} chương sau {retry_full} vòng thử tất cả nguồn."
            )
        await smart_delay()


async def initialize_and_log_setup_with_state(site_key) -> Tuple[str, Dict[str, Any]]:
    await ensure_directory_exists(DATA_FOLDER)
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)
    await initialize_scraper(site_key)
    try:
        import os as _os
        if (_os.getenv("AI_PRINT_METRICS", "false").lower() == "true"):
            from ai.selector_ai import print_ai_metrics_summary
            print_ai_metrics_summary()
    except Exception:
        pass
    homepage_url = BASE_URLS[site_key].rstrip("/") + "/"
    state_file = get_state_file(site_key)
    crawl_state = await load_crawl_state(state_file, site_key)

    logger.info("=== BẮT ĐẦU QUÁ TRÌNH CRAWL ASYNC ===")
    logger.info(f"Thư mục lưu dữ liệu: {os.path.abspath(DATA_FOLDER)}")
    logger.info(f"Sử dụng {len(LOADED_PROXIES)} proxy(s).")
    logger.info(
        f"Giới hạn: {MAX_GENRES_TO_CRAWL or 'Không giới hạn'} thể loại, "
        f"{MAX_STORIES_TOTAL_PER_GENRE or 'Không giới hạn'} truyện/thể loại."
    )
    logger.info(
        f"Giới hạn chương xử lý ban đầu/truyện: {MAX_CHAPTERS_PER_STORY or 'Không giới hạn'}."
    )
    logger.info(f"Số lượt thử lại cho các chương lỗi: {RETRY_FAILED_CHAPTERS_PASSES}.")
    logger.info(
        f"Giới hạn số trang truyện/thể loại: {MAX_STORIES_PER_GENRE_PAGE or 'Không giới hạn'}."
    )
    logger.info(
        f"Giới hạn số trang danh sách chương: {MAX_CHAPTER_PAGES_TO_CRAWL or 'Không giới hạn'}."
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

    if details:
        story_data_item.update(details)

    crawl_state["current_story_url"] = story_data_item["url"]
    if (
        crawl_state.get("previous_story_url_in_state_for_chapters")
        != story_data_item["url"]
    ):
        crawl_state["processed_chapter_urls_for_current_story"] = []
    crawl_state["previous_story_url_in_state_for_chapters"] = story_data_item["url"]
    state_file = get_state_file(site_key)
    await save_crawl_state(crawl_state, state_file)

    # 2. Crawl tất cả nguồn cho đến khi đủ chương
    await crawl_all_sources_until_full(
        site_key,
        session,
        story_data_item,
        current_discovery_genre_data,
        story_global_folder_path,
        crawl_state,
        num_batches=10,
        state_file=state_file,
        adapter=adapter,
    )

    # 3. Kiểm tra số chương đã crawl thực tế
    files_actual = get_saved_chapters_files(story_global_folder_path)
    total_chapters_on_site = details.get("total_chapters_on_site")  # type: ignore
    story_title = story_data_item["title"]
    story_url = story_data_item["url"]
    # Lần crawl đầu tiên (metadata vừa tạo file, chưa có file chương nào)
    is_new_crawl = os.path.exists(metadata_file) and (
        files_actual is not None and len(files_actual) == 0
    )

    if total_chapters_on_site:
        crawled_chapters = len(files_actual)

        # Crawl mới hoàn toàn: chỉ log, KHÔNG kiểm tra thiếu chương
        if is_new_crawl:
            logger.info(
                f"[NEW] Đang crawl mới truyện '{story_title}': {crawled_chapters}/{total_chapters_on_site} chương. Đợi crawl hoàn tất rồi mới kiểm tra thiếu chương."
            )
        else:
            # (Chỉ kiểm tra thiếu chương nếu KHÔNG phải crawl mới)
            if crawled_chapters < 0.1 * total_chapters_on_site:
                logger.error(
                    f"[ALERT] Parse chương có thể lỗi HTML hoặc bị chặn: {story_title} ({crawled_chapters}/{total_chapters_on_site})"
                )

            if crawled_chapters < total_chapters_on_site:
                # Xác định danh sách file chương bị thiếu
                expected_files = []
                chapter_list = details.get("chapter_list", [])  # type: ignore
                for i in range(total_chapters_on_site):
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
                    f"[BLOCK] Truyện '{story_title}' còn thiếu {total_chapters_on_site - crawled_chapters} chương. Không next!"
                )
                logger.error(f"[BLOCK] Danh sách file chương thiếu: {missing_files}")
                await add_missing_story(
                    story_title, story_url, total_chapters_on_site, crawled_chapters
                )
                return False
            else:
                logger.info(
                    f"Truyện '{story_title}' đã crawl đủ {crawled_chapters}/{total_chapters_on_site} chương."
                )

    # 5. Đánh dấu completed và clear state
    is_complete = False
    # Hoan tat khi so file chuong >= tong so chuong tren site.
    # Khong phu thuoc truong 'status' de tranh khong next du du lieu da day du.
    total = details.get("total_chapters_on_site")  # type: ignore
    if not total:
        try:
            total = await get_real_total_chapters(story_data_item, adapter)
        except Exception:
            pass
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
    state_file = get_state_file(site_key)
    await save_crawl_state(crawl_state, state_file)
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
            chapters = await adapter.get_chapter_list(
                story_url=url, story_title=story_data_item.get("title"), site_key=site_key, total_chapters=total_chapters_on_site
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
    state_file = get_state_file(site_key)
    await save_crawl_state(crawl_state, state_file)

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
                MAX_STORIES_PER_GENRE_PAGE,
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
        if MAX_STORIES_TOTAL_PER_GENRE and idx >= MAX_STORIES_TOTAL_PER_GENRE:
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
                    if retry_counts[idx] >= RETRY_STORY_ROUND_LIMIT:
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
        await save_crawl_state(crawl_state, state_file)
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
    await run_crawler(adapter, site_key, genres, settings, crawl_state)


async def run_missing(site_key: str, homepage_url: Optional[str] = None):
    homepage_url = homepage_url or BASE_URLS[site_key].rstrip("/") + "/"
    logger.info("[MISSING] Bắt đầu crawl chương thiếu...")
    await initialize_scraper(site_key)
    adapter = get_adapter(site_key)
    await check_and_crawl_missing_all_stories(adapter, homepage_url, site_key=site_key)


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
        batch_size = int(os.getenv("BATCH_SIZE") or get_optimal_batch_size(len(genres)))
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

    for site_key in BASE_URLS.keys():
        async def run_site(key=site_key):
            try:
                await run_single_site(key, crawl_mode=crawl_mode)
            except Exception as e:
                logger.error(f"[MAIN] Site {key} failed: {e}")
        tasks.append(asyncio.create_task(run_site()))

    await asyncio.gather(*tasks)



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

    settings = WorkerSettings(
        genre_batch_size=int(os.getenv("GENRE_BATCH_SIZE", 3)),
        genre_async_limit=int(os.getenv("GENRE_ASYNC_LIMIT", 3)),
        proxies_file=os.getenv("PROXIES_FILE", "proxies/proxies.txt"),
        failed_genres_file=os.getenv("FAILED_GENRES_FILE", "failed_genres.json"),
        retry_genre_round_limit=int(os.getenv("RETRY_GENRE_ROUND_LIMIT", 3)),
        retry_sleep_seconds=int(os.getenv("RETRY_SLEEP_SECONDS", 1800)),
    )

    if crawl_mode == "genres_only":
        await run_genres(site_key, settings, crawl_state)
        await retry_failed_genres(
            get_adapter(site_key), site_key, settings, shuffle_proxies
        )
    elif crawl_mode == "missing_only":
        await run_missing(site_key, homepage_url)
        await crawl_all_missing_stories()
    elif crawl_mode == "missing_single":
        url = next(
            (arg.split("=")[1] for arg in sys.argv if arg.startswith("--url=")), None
        )
        title = next(
            (arg.split("=")[1] for arg in sys.argv if arg.startswith("--title=")), None
        )
        await crawl_single_story_worker(story_url=url, title=title)
        await crawl_all_missing_stories()
    else:
        await run_genres(site_key, settings, crawl_state)
        await retry_failed_genres(
            get_adapter(site_key), site_key, settings, shuffle_proxies
        )
        await run_missing(site_key, homepage_url)
        await crawl_all_missing_stories()


if __name__ == "__main__":
    mode = os.getenv("MODE") or (sys.argv[1] if len(sys.argv) > 1 else None)
    crawl_mode = os.getenv("CRAWL_MODE") or (sys.argv[2] if len(sys.argv) > 2 else None)

    if mode == "all_sites":
        asyncio.run(run_all_sites(crawl_mode=crawl_mode))
    elif mode:  # mode là site_key
        asyncio.run(run_single_site(site_key=mode, crawl_mode=crawl_mode))
    else:
        print("❌ Cần truyền site_key hoặc 'all_sites' làm đối số.")
        sys.exit(1)
