import asyncio
import glob
import aiohttp
import os
from urllib.parse import urlparse
from typing import Dict, Any, List, Tuple

from utils.logger import logger

from utils.utils import (
    add_missing_story, async_save_chapter_with_hash_check, backup_crawl_state, count_txt_files, log_error_chapter, queue_failed_chapter, sanitize_filename, ensure_directory_exists,
    create_proxy_template_if_not_exists, load_crawl_state,
    save_crawl_state,
    clear_specific_state_keys, save_story_metadata_file, smart_delay
)
from config.config import (
    BASE_URL, DATA_FOLDER, PROXIES_FILE, PROXIES_FOLDER,
    MAX_GENRES_TO_CRAWL, MAX_STORIES_PER_GENRE_PAGE,
    MAX_STORIES_TOTAL_PER_GENRE, MAX_CHAPTERS_PER_STORY,
    MAX_CHAPTER_PAGES_TO_CRAWL, RETRY_FAILED_CHAPTERS_PASSES
)
from config.proxy_provider import load_proxies, loaded_proxies
from scraper import initialize_scraper
from analyze.parsers import (
    get_all_genres, get_all_stories_from_genre,
    get_chapters_from_story, get_story_chapter_content,
    get_story_details
)

SEM = asyncio.Semaphore(5)


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
    await load_proxies(PROXIES_FILE)
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

async def async_download_and_save_chapter(
    chapter_info: Dict[str, Any], story_data_item: Dict[str, Any],
    current_discovery_genre_data: Dict[str, Any],
    chapter_filename_full_path: str, chapter_filename_only: str,
    pass_description: str, chapter_display_idx_log: str,
    crawl_state: Dict[str, Any], successfully_saved: set, failed_list: List[Dict[str, Any]],original_idx: int = 0
) -> None:
    url = chapter_info['url']
    logger.info(f"        {pass_description} - Chương {chapter_display_idx_log}: Đang tải '{chapter_info['title']}' ({url})")
    async with SEM:
        content = await get_story_chapter_content(url, chapter_info['title'])

    if content:
        try:
            # Gộp nội dung chuẩn file .txt
            chapter_content = (
                f"Nguồn: {url}\n\nTruyện: {story_data_item['title']}\n"
                f"Thể loại: {current_discovery_genre_data['name']}\n"
                f"Chương: {chapter_info['title']}\n\n"
                f"{content}"
            )

            # Sử dụng hàm hash check
            save_result = await async_save_chapter_with_hash_check(chapter_filename_full_path, chapter_content)
            if save_result == "new":
                logger.info(f"          Đã lưu ({pass_description}): {chapter_filename_only}")
            elif save_result == "unchanged":
                logger.debug(f"          Chương '{chapter_filename_only}' đã tồn tại, không thay đổi nội dung.")
            elif save_result == "updated":
                logger.info(f"          Chương '{chapter_filename_only}' đã được cập nhật do nội dung thay đổi.")

            if save_result in ("new", "updated", "unchanged"):
                successfully_saved.add(chapter_filename_full_path)
                processed = crawl_state.get('processed_chapter_urls_for_current_story', [])
                if url not in processed:
                    processed.append(url)
                    crawl_state['processed_chapter_urls_for_current_story'] = processed
                    await save_crawl_state(crawl_state)
        except Exception as e:
            logger.error(f"          Lỗi lưu '{chapter_filename_only}': {e}")
            log_error_chapter(
                story_title=story_data_item['title'],
                chapter_title=chapter_info['title'],
                chapter_url=chapter_info['url'],
                error_msg=str(e)
            )
            # --- Queue retry chương lỗi ---
            queue_failed_chapter({
                "chapter_url": chapter_info['url'],
                "chapter_title": chapter_info['title'],
                "story_title": story_data_item['title'],
                "story_url": story_data_item['url'],
                "filename": chapter_filename_full_path,
                "reason": f"Lỗi lưu: {e}"
            })
            failed_list.append({
                'chapter_data': chapter_info,
                'filename': chapter_filename_full_path,
                'filename_only': chapter_filename_only,
                'original_idx': original_idx
            })
    else:
        logger.warning(f"          Không lấy được nội dung '{chapter_info['title']}'")
        log_error_chapter(
            story_title=story_data_item['title'],
            chapter_title=chapter_info['title'],
            chapter_url=chapter_info['url'],
            error_msg="Không lấy được nội dung"
        )
        # --- Queue retry chương lỗi ---
        queue_failed_chapter({
            "chapter_url": chapter_info['url'],
            "chapter_title": chapter_info['title'],
            "story_title": story_data_item['title'],
            "story_url": story_data_item['url'],
            "filename": chapter_filename_full_path,
            "reason": "Không lấy được nội dung"
        })
        failed_list.append({
            'chapter_data': chapter_info,
            'filename': chapter_filename_full_path,
            'filename_only': chapter_filename_only,
            'original_idx': None
        })

async def process_all_chapters_for_story(
    session: aiohttp.ClientSession,
    chapters: List[Dict[str, Any]], story_data_item: Dict[str, Any],
    current_discovery_genre_data: Dict[str, Any], story_folder_path: str,
    crawl_state: Dict[str, Any]
) -> int:
    if not chapters:
        return 0
    successful, failed = set(), []
    already_crawled = set(crawl_state.get('processed_chapter_urls_for_current_story', []))
    target = len(chapters)

    # Lượt 1
    tasks = []
    for idx, ch in enumerate(chapters[:target]):
        if ch['url'] in already_crawled:
            continue
        fname_only = f"{idx+1:04d}_{sanitize_filename(ch['title']) or 'untitled'}.txt"
        full_path = os.path.join(story_folder_path, fname_only)
        tasks.append(
            asyncio.create_task(
                async_download_and_save_chapter(
                    ch, story_data_item, current_discovery_genre_data,
                    full_path, fname_only, "Lượt 1", f"{idx+1}/{target}",
                    crawl_state, successful, failed, original_idx=idx
                )
            )
        )
        await smart_delay()
    await asyncio.gather(*tasks)

    # Retry
    for rp in range(RETRY_FAILED_CHAPTERS_PASSES):
        if not failed:
            break
        curr, failed = failed.copy(), []
        logger.info(f"    --- Lượt thử lại {rp+1} cho {len(curr)} chương lỗi ---")
        tasks = []
        for item in curr:
            ch = item['chapter_data']
            # Xử lý fallback index thông minh nếu thiếu original_idx
            idx = item.get('original_idx')
            if idx is None:
                # Thử lấy từ filename nếu có định dạng "0001_...txt"
                try:
                    name = item.get('filename_only', '')
                    idx = int(name.split('_')[0]) - 1 if name and '_' in name else 0
                except Exception:
                    idx = 0
                logger.warning(f"Chưa có original_idx cho chương '{ch.get('title')}', fallback idx={idx}")
            if ch['url'] in crawl_state.get('processed_chapter_urls_for_current_story', []):
                continue
            fname_only = f"{idx+1:04d}_{sanitize_filename(ch['title']) or 'untitled'}.txt"
            full_path = os.path.join(story_folder_path, fname_only)
            tasks.append(
                asyncio.create_task(
                    async_download_and_save_chapter(
                        ch, story_data_item, current_discovery_genre_data,
                        full_path, fname_only, f"Lượt thử lại {rp+1}", str(idx+1),
                        crawl_state, successful, failed, original_idx=idx
                    )
                )
            )
        await smart_delay()
        await asyncio.gather(*tasks)

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
    # Fetch details async
    details = await get_story_details(story_data_item['url'], story_data_item['title'])
    # Save metadata
    await save_story_metadata_file(
        story_data_item, current_discovery_genre_data,
        story_global_folder_path, details,
        None
    )

    crawl_state['current_story_url'] = story_data_item['url']
    if crawl_state.get('previous_story_url_in_state_for_chapters') != story_data_item['url']:
        crawl_state['processed_chapter_urls_for_current_story'] = []
    crawl_state['previous_story_url_in_state_for_chapters'] = story_data_item['url']
    await save_crawl_state(crawl_state)

    # Chapters async
    chapters = await get_chapters_from_story(
        story_data_item['url'], story_data_item['title'], MAX_CHAPTER_PAGES_TO_CRAWL
    )
    await ensure_directory_exists(story_global_folder_path)
    count = await process_all_chapters_for_story(
        session, chapters, story_data_item,
        current_discovery_genre_data, story_global_folder_path, crawl_state
    )

    # === KIỂM TRA VÀ GHI LẠI TRUYỆN THIẾU CHƯƠNG ===
    total_chapters_on_site = details.get('total_chapters_on_site')
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
    status = details.get('status')
    total = details.get('total_chapters_on_site')
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