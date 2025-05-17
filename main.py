import asyncio
import glob
import json
import time
import aiohttp
import os
from urllib.parse import urlparse
from typing import Dict, Any, List, Tuple

from utils.batch_utils import smart_delay, split_batches
from utils.chapter_utils import async_download_and_save_chapter, process_chapter_batch
from utils.io_utils import create_proxy_template_if_not_exists, ensure_directory_exists
from utils.logger import logger
from config.config import loaded_proxies

from config.config import (
    BASE_URL, DATA_FOLDER, NUM_CHAPTER_BATCHES, PROXIES_FILE, PROXIES_FOLDER,
    MAX_GENRES_TO_CRAWL, MAX_STORIES_PER_GENRE_PAGE,
    MAX_STORIES_TOTAL_PER_GENRE, MAX_CHAPTERS_PER_STORY,
    MAX_CHAPTER_PAGES_TO_CRAWL, RETRY_FAILED_CHAPTERS_PASSES
)
from config.proxy_provider import  load_proxies
from scraper import initialize_scraper
from analyze.parsers import (
    get_all_genres, get_all_stories_from_genre,
    get_chapters_from_story,
    get_story_details
)
from utils.meta_utils import add_missing_story, backup_crawl_state, count_txt_files, sanitize_filename, save_story_metadata_file
from utils.state_utils import clear_specific_state_keys, load_crawl_state, save_crawl_state



def get_saved_chapters_files(story_folder_path: str) -> set:
    """Trả về set tên file đã lưu trong folder truyện."""
    if not os.path.exists(story_folder_path):
        return set()
    files = glob.glob(os.path.join(story_folder_path, "*.txt"))
    return set(os.path.basename(f) for f in files)

async def crawl_missing_chapters_for_story(
    session: aiohttp.ClientSession,
    chapters: List[Dict[str, Any]],
    story_data_item: Dict[str, Any],
    current_discovery_genre_data: Dict[str, Any],
    story_folder_path: str,
    crawl_state: Dict[str, Any]
) -> int:
    """Quét lại các chương thiếu và crawl bù"""
    saved_files = get_saved_chapters_files(story_folder_path)
    missing_chapters = []
    for idx, ch in enumerate(chapters):
        fname_only = f"{idx+1:04d}_{sanitize_filename(ch['title']) or 'untitled'}.txt"
        if fname_only not in saved_files:
            missing_chapters.append((idx, ch, fname_only))
    if not missing_chapters:
        logger.info(f"Tất cả chương đã đủ, không có chương missing trong '{story_data_item['title']}'")
        return 0
    logger.info(f"Có {len(missing_chapters)} chương thiếu, sẽ crawl bù cho truyện '{story_data_item['title']}'...")
    tasks = []
    successful, failed = set(), []
    for idx, ch, fname_only in missing_chapters:
        full_path = os.path.join(story_folder_path, fname_only)
        tasks.append(asyncio.create_task(
            async_download_and_save_chapter(ch, story_data_item, current_discovery_genre_data,
                full_path, fname_only, "Crawl bù missing", f"{idx+1}/{len(chapters)}", crawl_state, successful, failed
            )
        ))
        await smart_delay()
    await asyncio.gather(*tasks)
    if failed:
        logger.warning(f"Vẫn còn {len(failed)} chương bù không crawl được.")
    return len(successful)

async def initialize_and_log_setup_with_state() -> Tuple[str, Dict[str, Any]]:
    await ensure_directory_exists(DATA_FOLDER)
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await initialize_scraper()
    homepage_url = BASE_URL.rstrip('/') + '/'
    crawl_state = await load_crawl_state()

    logger.info("=== BẮT ĐẦU QUÁ TRÌNH CRAWL ASYNC ===")
    logger.info(f"Thư mục lưu dữ liệu: {os.path.abspath(DATA_FOLDER)}")
    logger.info(f"Sử dụng {len(loaded_proxies)} proxy(s).")
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
    logger.info(f"Giới hạn số trang danh sách chương: {MAX_CHAPTER_PAGES_TO_CRAWL or 'Không giới hạn'}.")
    if crawl_state:
        loggable = {k: v for k, v in crawl_state.items()
                    if k not in ['processed_chapter_urls_for_current_story', 'globally_completed_story_urls']}
        if 'processed_chapter_urls_for_current_story' in crawl_state:
            loggable['processed_chapters_count'] = len(crawl_state['processed_chapter_urls_for_current_story'])
        if 'globally_completed_story_urls' in crawl_state:
            loggable['globally_completed_stories_count'] = len(crawl_state['globally_completed_story_urls'])
        logger.info(f"Tìm thấy trạng thái crawl trước đó: {loggable}")
    logger.info("-----------------------------------------")
    return homepage_url, crawl_state




async def process_all_chapters_for_story(
    session: aiohttp.ClientSession,
    chapters: List[Dict[str, Any]], story_data_item: Dict[str, Any],
    current_discovery_genre_data: Dict[str, Any], story_folder_path: str,
    crawl_state: Dict[str, Any]
) -> int:
    if not chapters:
        return 0
    for idx, ch in enumerate(chapters):
        ch['idx'] = idx

    batches = split_batches(chapters, NUM_CHAPTER_BATCHES)
    total_batch = len(batches)
    batch_tasks = []
    for batch_idx, batch_chapters in enumerate(batches):
        batch_tasks.append(asyncio.create_task(
            process_chapter_batch(
                session, batch_chapters, story_data_item, current_discovery_genre_data,
                story_folder_path, crawl_state, batch_idx, total_batch
            )
        ))
    # Chạy đồng thời các batch
    results = await asyncio.gather(*batch_tasks)
    
    # Tổng hợp kết quả từ tất cả batch
    successful = set()
    failed = []
    for suc, fail in results:
        successful.update(suc)
        failed.extend(fail)

    # Retry tương tự, nhưng cho từng batch nếu muốn (hoặc cứ gom failed lại rồi retry)
    for rp in range(RETRY_FAILED_CHAPTERS_PASSES):
        if not failed:
            break
        curr, failed = failed.copy(), []
        logger.info(f"    --- Lượt thử lại {rp+1} cho {len(curr)} chương lỗi ---")
        retry_tasks = []
        for item in curr:
            ch = item['chapter_data']
            idx = item.get('original_idx')
            if idx is None:
                try:
                    name = item.get('filename_only', '')
                    idx = int(name.split('_')[0]) - 1 if name and '_' in name else 0
                except Exception:
                    idx = 0
            fname_only = f"{idx+1:04d}_{sanitize_filename(ch['title']) or 'untitled'}.txt"
            full_path = os.path.join(story_folder_path, fname_only)
            retry_tasks.append(asyncio.create_task(
                async_download_and_save_chapter(
                    ch, story_data_item, current_discovery_genre_data,
                    full_path, fname_only, f"Lượt thử lại {rp+1}", str(idx+1),
                    crawl_state, successful, failed, original_idx=idx
                )
            ))
        await smart_delay()
        await asyncio.gather(*retry_tasks)

    if failed:
        for fitem in failed:
            logger.error(f"Truyện: {story_data_item['title']} - Chương lỗi: {fitem['chapter_data']['title']}")
    return len(successful)



async def process_story_item(
    session: aiohttp.ClientSession,
    story_data_item: Dict[str, Any], current_discovery_genre_data: Dict[str, Any],
    story_global_folder_path: str, crawl_state: Dict[str, Any]
) -> bool:
    logger.info(f"\n  --- Xử lý truyện: {story_data_item['title']} ---")

    metadata_file = os.path.join(story_global_folder_path, "metadata.json")
    fields_need_check = ["description", "status", "source", "rating_value", "rating_count", "total_chapters_on_site"]
    metadata = None
    need_update = False

    if os.path.exists(metadata_file):
        with open(metadata_file, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        for field in fields_need_check:
            if metadata.get(field) is None:
                need_update = True
                break
    else:
        need_update = True

    # Chỉ gọi lại get_story_details nếu metadata chưa đủ hoặc chưa có file
    if need_update:
        details = await get_story_details(story_data_item['url'], story_data_item['title'])
        await save_story_metadata_file(
            story_data_item, current_discovery_genre_data,
            story_global_folder_path, details,
            metadata
        )
        if metadata:
            # merge các field detail vào metadata và ghi lại cho chắc chắn
            for field in fields_need_check:
                if details.get(field) is not None:
                    metadata[field] = details[field]
            metadata['metadata_updated_at'] = time.strftime("%Y-%m-%d %H:%M:%S")
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
    else:
        # Nếu metadata đã đủ, dùng lại (không gọi lại get_story_details)
        details = metadata

    crawl_state['current_story_url'] = story_data_item['url']
    if crawl_state.get('previous_story_url_in_state_for_chapters') != story_data_item['url']:
        crawl_state['processed_chapter_urls_for_current_story'] = []
    crawl_state['previous_story_url_in_state_for_chapters'] = story_data_item['url']
    await save_crawl_state(crawl_state)

    # Chapters async
    chapters = await get_chapters_from_story(
        story_data_item['url'], story_data_item['title'], MAX_CHAPTER_PAGES_TO_CRAWL,
        total_chapters_on_site=details.get('total_chapters_on_site')#type: ignore
    )
    await ensure_directory_exists(story_global_folder_path)
    count = await process_all_chapters_for_story(
        session, chapters, story_data_item,
        current_discovery_genre_data, story_global_folder_path, crawl_state
    )

    # === KIỂM TRA VÀ GHI LẠI TRUYỆN THIẾU CHƯƠNG ===
    total_chapters_on_site = details.get('total_chapters_on_site')#type: ignore
    story_title = story_data_item['title']
    story_url = story_data_item['url']
    if total_chapters_on_site:
        crawled_chapters = count_txt_files(story_global_folder_path)

        if crawled_chapters < 0.1 * total_chapters_on_site:
            logger.error(f"[ALERT] Parse chương có thể lỗi HTML hoặc bị chặn: {story_title} ({crawled_chapters}/{total_chapters_on_site})")

        if crawled_chapters < total_chapters_on_site:
            logger.warning(f"Truyện '{story_title}' chỉ crawl được {crawled_chapters}/{total_chapters_on_site} chương! Ghi lại để crawl bù.")
            add_missing_story(story_title, story_url, total_chapters_on_site, crawled_chapters)
        else:
            logger.info(f"Truyện '{story_title}' đã crawl đủ {crawled_chapters}/{total_chapters_on_site} chương.")

    # Check completion
    is_complete = False
    status = details.get('status') #type: ignore
    total = details.get('total_chapters_on_site')  #type: ignore
    if status and total and count >= total:
        is_complete = True
        completed = set(crawl_state.get('globally_completed_story_urls', []))
        completed.add(story_data_item['url'])
        crawl_state['globally_completed_story_urls'] = sorted(completed)
    backup_crawl_state()
    await save_crawl_state(crawl_state)
    await clear_specific_state_keys(crawl_state, ['processed_chapter_urls_for_current_story'])
    return is_complete

async def process_genre_item(
    session: aiohttp.ClientSession,
    genre_data: Dict[str, Any], crawl_state: Dict[str, Any]
) -> None:
    logger.info(f"\n--- Xử lý thể loại: {genre_data['name']} ---")
    crawl_state['current_genre_url'] = genre_data['url']
    if crawl_state.get('previous_genre_url_in_state_for_stories') != genre_data['url']:
        crawl_state['current_story_index_in_genre'] = 0
    crawl_state['previous_genre_url_in_state_for_stories'] = genre_data['url']
    await save_crawl_state(crawl_state)

    stories = await get_all_stories_from_genre(
        genre_data['name'], genre_data['url'], MAX_STORIES_PER_GENRE_PAGE
    )
    completed_global = set(crawl_state.get('globally_completed_story_urls', []))
    start_idx = crawl_state.get('current_story_index_in_genre', 0)

    for idx, story in enumerate(stories):
        if idx < start_idx:
            continue
        if MAX_STORIES_TOTAL_PER_GENRE and idx >= MAX_STORIES_TOTAL_PER_GENRE:
            break
        slug = sanitize_filename(urlparse(story['url']).path.strip('/').split('/')[-1])
        folder = os.path.join(DATA_FOLDER, slug)
        await ensure_directory_exists(folder)

        if story['url'] in completed_global:
            # update metadata only
            details = await get_story_details(story['url'], story['title'])
            await save_story_metadata_file(story, genre_data, folder, details, None)
            crawl_state['current_story_index_in_genre'] = idx + 1
            await save_crawl_state(crawl_state)
            continue

        crawl_state['current_story_index_in_genre'] = idx
        await save_crawl_state(crawl_state)
        done = await process_story_item(session, story, genre_data, folder, crawl_state)
        if done:
            completed_global.add(story['url'])
        crawl_state['globally_completed_story_urls'] = sorted(completed_global)
        crawl_state['current_story_index_in_genre'] = idx + 1
        await save_crawl_state(crawl_state)

    await clear_specific_state_keys(crawl_state, [
        'current_story_index_in_genre', 'current_genre_url',
        'previous_genre_url_in_state_for_stories'
    ])

async def run_crawler():
    await load_proxies(PROXIES_FILE)
    homepage_url, crawl_state = await initialize_and_log_setup_with_state()
    genres = await get_all_genres(homepage_url)
    async with aiohttp.ClientSession() as session:
        for idx, genre in enumerate(genres):
            if idx < crawl_state.get('current_genre_index', 0):
                continue
            if MAX_GENRES_TO_CRAWL and idx >= MAX_GENRES_TO_CRAWL:
                break
            crawl_state['current_genre_index'] = idx
            await save_crawl_state(crawl_state)
            await process_genre_item(session, genre, crawl_state)
    logger.info("=== HOÀN TẤT TOÀN BỘ QUÁ TRÌNH CRAWL ===")

if __name__ == '__main__':
    asyncio.run(run_crawler())