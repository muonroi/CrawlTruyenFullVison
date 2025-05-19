import asyncio
from dataclasses import asdict
import aiohttp
import os
from typing import Dict, Any, Tuple
from adapters.base_site_adapter import BaseSiteAdapter
from config.proxy_provider import load_proxies
from core.meta_service import  save_story_metadata_file
from core.state_service import clear_specific_state_keys, load_crawl_state, save_crawl_state
from pipelines.story_pipeline import process_story_item
from scraper import initialize_scraper
from utils.batch_utils import smart_delay, split_batches
from utils.chapter_utils import   slugify_title
from utils.io_utils import  create_proxy_template_if_not_exists, ensure_directory_exists, log_failed_genre,  sanitize_filename
from utils.logger import logger
from config.config import  GENRE_ASYNC_LIMIT, GENRE_BATCH_SIZE, LOADED_PROXIES, get_state_file
from core.models.story import  Genre


from config.config import (
    BASE_URLS, DATA_FOLDER,  PROXIES_FILE, PROXIES_FOLDER,
    MAX_GENRES_TO_CRAWL, MAX_STORIES_PER_GENRE_PAGE,
    MAX_STORIES_TOTAL_PER_GENRE, MAX_CHAPTERS_PER_STORY,
    MAX_CHAPTER_PAGES_TO_CRAWL, RETRY_FAILED_CHAPTERS_PASSES
)

GENRE_SEM = asyncio.Semaphore(GENRE_ASYNC_LIMIT)


async def initialize_and_log_setup_with_state(site_key) -> Tuple[str, Dict[str, Any]]:
    await ensure_directory_exists(DATA_FOLDER)
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await initialize_scraper(site_key)
    homepage_url = BASE_URLS[site_key].rstrip('/') + '/'
    state_file = get_state_file(site_key)
    crawl_state = await load_crawl_state(state_file)

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

async def process_genre_with_limit(session, genre, crawl_state, adapter, site_key):
    async with GENRE_SEM:
        await process_genre_item(session, genre, crawl_state, adapter, site_key)

async def process_genre_item(
    session: aiohttp.ClientSession,
    genre_data: Genre,                # Đã là object Genre
    crawl_state: Dict[str, Any],
    adapter: BaseSiteAdapter,
    site_key: str
) -> None:
    logger.info(f"\n--- Xử lý thể loại: {genre_data.name} ---")
    crawl_state['current_genre_url'] = genre_data.url
    if crawl_state.get('previous_genre_url_in_state_for_stories') != genre_data.url:
        crawl_state['current_story_index_in_genre'] = 0
    crawl_state['previous_genre_url_in_state_for_stories'] = genre_data.url
    state_file = get_state_file(site_key)
    await save_crawl_state(crawl_state, state_file)

    retry_time = 0
    max_retry = 5

    while True:
        try:
            stories, total_pages, crawled_pages = await adapter.get_all_stories_from_genre_with_page_check(
                genre_data.name, genre_data.url, MAX_STORIES_PER_GENRE_PAGE
            )
            if not stories or len(stories) == 0:
                raise Exception(f"Danh sách truyện rỗng cho genre {genre_data.name} ({genre_data.url})")

            if total_pages and (crawled_pages is not None) and crawled_pages < total_pages:
                logger.warning(
                    f"Thể loại {genre_data.name} chỉ crawl được {crawled_pages}/{total_pages} trang, sẽ retry lần {retry_time+1}..."
                )
                retry_time += 1
                if retry_time >= max_retry:
                    logger.error(f"Thể loại {genre_data.name} không crawl đủ số trang sau {max_retry} lần.")
                    log_failed_genre({"name": genre_data.name, "url": genre_data.url})  # log lại dưới dạng dict
                    return
                await asyncio.sleep(5)
                continue
            break
        except Exception as ex:
            logger.error(f"Lỗi khi crawl genre {genre_data.name} ({genre_data.url}): {ex}")
            log_failed_genre({"name": genre_data.name, "url": genre_data.url})
            return

    completed_global = set(crawl_state.get('globally_completed_story_urls', []))
    start_idx = crawl_state.get('current_story_index_in_genre', 0)

    for idx, story in enumerate(stories):  # story: Story
        if idx < start_idx:
            continue
        if MAX_STORIES_TOTAL_PER_GENRE and idx >= MAX_STORIES_TOTAL_PER_GENRE:
            break
        slug = slugify_title(story.title)
        folder = os.path.join(DATA_FOLDER, slug)
        await ensure_directory_exists(folder)

        if story.url in completed_global:
            # update metadata only
            details = await adapter.get_story_details(story.url, story.title)
            await save_story_metadata_file(story, asdict(genre_data), folder, details, None)
            crawl_state['current_story_index_in_genre'] = idx + 1
            await save_crawl_state(crawl_state, state_file)
            continue

        crawl_state['current_story_index_in_genre'] = idx
        await save_crawl_state(crawl_state, state_file)
        done = await process_story_item(session, story, asdict(genre_data), folder, crawl_state, adapter, site_key)
        if done:
            completed_global.add(story.url)
        crawl_state['globally_completed_story_urls'] = sorted(completed_global)
        crawl_state['current_story_index_in_genre'] = idx + 1
        await save_crawl_state(crawl_state, state_file)

    await clear_specific_state_keys(crawl_state, [
        'current_story_index_in_genre', 'current_genre_url',
        'previous_genre_url_in_state_for_stories'
    ], state_file)

async def run_crawler(adapter, site_key):
    await load_proxies(PROXIES_FILE)
    homepage_url, crawl_state = await initialize_and_log_setup_with_state(site_key)
    genres = await adapter.get_genres()
    async with aiohttp.ClientSession() as session:
        batches = split_batches(genres, GENRE_BATCH_SIZE)
        for batch_idx, genre_batch in enumerate(batches):
            tasks = []
            for genre in genre_batch:
                tasks.append(process_genre_with_limit(session, genre, crawl_state, adapter, site_key))
            logger.info(f"=== Đang crawl batch thể loại {batch_idx+1}/{len(batches)} ({len(genre_batch)} genres song song) ===")
            await asyncio.gather(*tasks)
            await smart_delay()
    logger.info("=== HOÀN TẤT TOÀN BỘ QUÁ TRÌNH CRAWL ===")