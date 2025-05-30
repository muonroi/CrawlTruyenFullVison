import asyncio
import glob
import json
import random
import sys
import time
import aiohttp
import os
from typing import Dict, Any, List, Tuple
from aiogram import  Router
from adapters.base_site_adapter import BaseSiteAdapter
from adapters.factory import get_adapter
from utils.batch_utils import smart_delay, split_batches
from utils.chapter_utils import async_download_and_save_chapter, count_txt_files, extract_real_chapter_number, get_chapter_filename, process_chapter_batch, slugify_title
from utils.io_utils import  create_proxy_template_if_not_exists, ensure_directory_exists, log_failed_genre, safe_write_file
from utils.logger import logger
from config.config import FAILED_GENRES_FILE, GENRE_ASYNC_LIMIT, GENRE_BATCH_SIZE, LOADED_PROXIES, RETRY_GENRE_ROUND_LIMIT, RETRY_SLEEP_SECONDS, get_state_file

from config.config import (
    BASE_URLS, DATA_FOLDER, NUM_CHAPTER_BATCHES, PROXIES_FILE, PROXIES_FOLDER,
    MAX_GENRES_TO_CRAWL, MAX_STORIES_PER_GENRE_PAGE,
    MAX_STORIES_TOTAL_PER_GENRE, MAX_CHAPTERS_PER_STORY,
    MAX_CHAPTER_PAGES_TO_CRAWL, RETRY_FAILED_CHAPTERS_PASSES
)
from config.proxy_provider import  load_proxies, shuffle_proxies
from scraper import initialize_scraper
from utils.meta_utils import add_missing_story, backup_crawl_state, sanitize_filename, save_story_metadata_file
from utils.state_utils import clear_specific_state_keys, load_crawl_state, merge_all_missing_workers_to_main, save_crawl_state

router = Router()
is_crawling = False
GENRE_SEM = asyncio.Semaphore(GENRE_ASYNC_LIMIT)
BATCH_SEMAPHORE_LIMIT = 5

async def process_genre_with_limit(session, genre, crawl_state, adapter, site_key):
    async with GENRE_SEM:
        await process_genre_item(session, genre, crawl_state, adapter, site_key)

async def crawl_all_sources_until_full(
    site_key, session, story_data_item, current_discovery_genre_data, story_folder_path, crawl_state,
    num_batches=10, state_file=None, adapter=None
):
    # 1. Lấy tổng số chương cần crawl
    total_chapters = story_data_item.get('total_chapters_on_site')
    if not total_chapters:
        logger.error(f"Không xác định được tổng số chương cho '{story_data_item['title']}'")
        return

    sources = story_data_item.get('sources', [])
    if not sources:
        logger.error(f"Không có nguồn nào trong metadata của '{story_data_item['title']}'")
        return

    retry_full = 0
    while True:
        files_before = len(get_saved_chapters_files(story_folder_path))
        for source in sources:
            url = source.get('url')
            if not url:
                continue
            # Dùng adapter phù hợp cho từng nguồn nếu multi-site, còn 1 site thì giữ nguyên
            chapters = await adapter.get_chapter_list(url, story_data_item['title'], site_key, total_chapters=total_chapters) # type: ignore
            await crawl_missing_chapters_for_story(
                site_key, session, chapters, story_data_item, current_discovery_genre_data, story_folder_path, crawl_state,
                num_batches=num_batches, state_file=state_file
            )
            files_now = len(get_saved_chapters_files(story_folder_path))
            if files_now >= total_chapters:
                logger.info(f"Đã crawl đủ chương {files_now}/{total_chapters} cho '{story_data_item['title']}' (từ nguồn {source.get('site')})")
                return  # Đã đủ chương thì thoát luôn
        files_after = len(get_saved_chapters_files(story_folder_path))
        if files_after >= total_chapters:
            break
        if files_after == files_before:
            logger.warning(f"[ALERT] Đã thử hết nguồn nhưng không crawl thêm được chương nào cho '{story_data_item['title']}'. Sẽ lặp lại.")
        retry_full += 1
        if retry_full % 20 == 0:
            logger.error(f"[ALERT] Truyện '{story_data_item['title']}' còn thiếu {total_chapters - files_after} chương sau {retry_full} vòng thử tất cả nguồn.")
        await smart_delay()


def get_saved_chapters_files(story_folder_path: str) -> set:
    """Trả về set tên file đã lưu trong folder truyện."""
    if not os.path.exists(story_folder_path):
        return set()
    files = glob.glob(os.path.join(story_folder_path, "*.txt"))
    return set(os.path.basename(f) for f in files)

from utils.state_utils import save_crawl_state

async def crawl_missing_chapters_for_story(
    site_key, session, chapters, story_data_item, current_discovery_genre_data, story_folder_path, crawl_state,
    num_batches=10, state_file=None
):
    total_chapters = story_data_item.get('total_chapters_on_site', len(chapters))
    retry_count = 0

    def split_batches(items, num_batches):
        if not items or num_batches <= 0:
            return []
        k, m = divmod(len(items), num_batches)
        return [items[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(num_batches)]

    while True:
        saved_files = get_saved_chapters_files(story_folder_path)
        if len(saved_files) >= total_chapters:
            logger.info(f"ĐÃ ĐỦ {len(saved_files)}/{total_chapters} chương cho '{story_data_item['title']}'")
            break

        # Xác định các chương thiếu thực tế dựa trên danh sách chương
        missing_chapters = []
        for idx, ch in enumerate(chapters):
            fname_only = get_chapter_filename(ch['title'], idx)
            if fname_only not in saved_files:
                missing_chapters.append((idx, ch, fname_only))

        if not missing_chapters:
            break  # An toàn

        logger.warning(
            f"Truyện '{story_data_item['title']}' còn thiếu {len(missing_chapters)} chương (retry: {retry_count + 1})"
        )

        # Tính số batch dựa trên số chương còn thiếu, mỗi batch tối đa 120 chương
        num_batches_now = max(1, (len(missing_chapters) + 119) // 120)
        batches = split_batches(missing_chapters, num_batches_now)
        logger.info(f"Crawl {len(missing_chapters)} chương với {num_batches_now} batch (mỗi batch tối đa 120 chương)")

        async def crawl_batch(batch, batch_idx):
            successful, failed = set(), []
            sem = asyncio.Semaphore(BATCH_SEMAPHORE_LIMIT)
            tasks = []
            for i, (idx, ch, fname_only) in enumerate(batch):
                full_path = os.path.join(story_folder_path, fname_only)
                logger.info(f"[Batch {batch_idx}/{num_batches_now}] Đang crawl chương {idx+1}: {ch['title']}")

                async def wrapped(ch=ch, idx=idx, fname_only=fname_only, full_path=full_path):
                    async with sem:
                        try:
                            await asyncio.wait_for(
                                async_download_and_save_chapter(
                                    ch, story_data_item, current_discovery_genre_data,
                                    full_path, fname_only, "Crawl bù missing",
                                    f"{idx+1}/{len(chapters)}", crawl_state, successful, failed, idx,
                                    site_key=site_key, state_file=state_file
                                ),
                                timeout=300  # Tăng timeout lên 300 giây
                            )
                        except Exception as ex:
                            logger.error(f"[Batch {batch_idx}] Lỗi khi crawl chương {idx+1}: {ch['title']} - {ex}")
                            failed.append({
                                'chapter_data': ch,
                                'filename': full_path,
                                'filename_only': fname_only,
                                'original_idx': idx
                            })
                tasks.append(asyncio.create_task(wrapped()))
            await asyncio.gather(*tasks, return_exceptions=True)
            if state_file:
                await save_crawl_state(crawl_state, state_file)
            return successful, failed

        # Thực thi từng batch tuần tự thay vì song song
        for batch_idx, batch in enumerate(batches):
            if not batch:
                continue
            await crawl_batch(batch, batch_idx + 1)

        retry_count += 1
        await smart_delay()  # Để tránh spam request quá nhanh

        # Cứ mỗi 20 lần retry mà vẫn còn chương thiếu thì log kỹ lại để check dead chương
        if retry_count % 20 == 0:
            logger.error(
                f"[ALERT] Truyện '{story_data_item['title']}' còn các chương sau mãi chưa crawl được: {[f for _,_,f in missing_chapters]}"
            )

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




async def process_all_chapters_for_story(
    session: aiohttp.ClientSession,
    chapters: List[Dict[str, Any]], story_data_item: Dict[str, Any],
    current_discovery_genre_data: Dict[str, Any], story_folder_path: str,
    crawl_state: Dict[str, Any], site_key: str
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
                story_folder_path, crawl_state, batch_idx, total_batch, adapter, site_key
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
            fname_only = get_chapter_filename(ch['title'], idx)
            full_path = os.path.join(story_folder_path, fname_only)
            retry_tasks.append(asyncio.create_task(
                async_download_and_save_chapter(
                    ch, story_data_item, current_discovery_genre_data,
                    full_path, fname_only, f"Lượt thử lại {rp+1}", str(idx+1),
                    crawl_state, successful, failed, original_idx=idx, site_key=site_key, state_file=get_state_file(site_key)
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
    story_data_item: Dict[str, Any], 
    current_discovery_genre_data: Dict[str, Any],
    story_global_folder_path: str, 
    crawl_state: Dict[str, Any],
    adapter: BaseSiteAdapter, 
    site_key: str
) -> bool:
    await ensure_directory_exists(story_global_folder_path)
    logger.info(f"\n  --- Xử lý truyện: {story_data_item['title']} ---")

    metadata_file = os.path.join(story_global_folder_path, "metadata.json")
    fields_need_check = [
        "description", "status", "source", 
        "rating_value", "rating_count", "total_chapters_on_site"
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
        details = await adapter.get_story_details(story_data_item['url'], story_data_item['title'])
        await save_story_metadata_file(
            story_data_item, current_discovery_genre_data,
            story_global_folder_path, details,
            metadata
        )
        if metadata:
            for field in fields_need_check:
                if details.get(field) is not None:
                    metadata[field] = details[field]
            metadata['metadata_updated_at'] = time.strftime("%Y-%m-%d %H:%M:%S")
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
    else:
        details = metadata

    crawl_state['current_story_url'] = story_data_item['url']
    if crawl_state.get('previous_story_url_in_state_for_chapters') != story_data_item['url']:
        crawl_state['processed_chapter_urls_for_current_story'] = []
    crawl_state['previous_story_url_in_state_for_chapters'] = story_data_item['url']
    state_file = get_state_file(site_key)
    await save_crawl_state(crawl_state, state_file)

    # 2. Crawl tất cả nguồn cho đến khi đủ chương
    await crawl_all_sources_until_full(
        site_key, session, story_data_item, current_discovery_genre_data, 
        story_global_folder_path, crawl_state, 
        num_batches=10, state_file=state_file, adapter=adapter
    )

    # 3. Kiểm tra số chương đã crawl thực tế
    files_actual = get_saved_chapters_files(story_global_folder_path)
    total_chapters_on_site = details.get('total_chapters_on_site')
    story_title = story_data_item['title']
    story_url = story_data_item['url']

    # 4. Nếu còn thiếu chương, không next và báo log đầy đủ
    if total_chapters_on_site:
        crawled_chapters = len(files_actual)
        if crawled_chapters < 0.1 * total_chapters_on_site:
            logger.error(f"[ALERT] Parse chương có thể lỗi HTML hoặc bị chặn: {story_title} ({crawled_chapters}/{total_chapters_on_site})")

        if crawled_chapters < total_chapters_on_site:
            # Xác định danh sách file chương bị thiếu
            missing_files = [
                f"{i+1:04d}_{sanitize_filename(details.get('chapter_list', [{}]*total_chapters_on_site)[i].get('title', 'untitled'))}.txt"
                if 'chapter_list' in details and i < len(details['chapter_list']) else f"{i+1:04d}_untitled.txt"
                for i in range(total_chapters_on_site)
                if f"{i+1:04d}_" not in ''.join(files_actual)
            ]
            logger.error(f"[BLOCK] Truyện '{story_title}' còn thiếu {total_chapters_on_site - crawled_chapters} chương. Không next!")
            logger.error(f"[BLOCK] Danh sách file chương thiếu: {missing_files}")
            await add_missing_story(story_title, story_url, total_chapters_on_site, crawled_chapters)
            # Option: chờ hoặc retry sau. Ở đây return False để không next
            return False
        else:
            logger.info(f"Truyện '{story_title}' đã crawl đủ {crawled_chapters}/{total_chapters_on_site} chương.")

        # === CẬP NHẬT crawled_chapters vào sources ===
        try:
            if os.path.exists(metadata_file):
                with open(metadata_file, "r", encoding="utf-8") as f:
                    metadata_latest = json.load(f)
            else:
                metadata_latest = details

            sources = metadata_latest.get("sources", []) # type: ignore
            for src in sources:
                if src.get("site") == site_key:
                    src["crawled_chapters"] = crawled_chapters
                else:
                    src.setdefault("crawled_chapters", None)

            metadata_latest["sources"] = sources
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata_latest, f, ensure_ascii=False, indent=4)
        except Exception as ex:
            logger.warning(f"Lỗi khi cập nhật crawled_chapters vào sources: {ex}")

    # 5. Đánh dấu completed và clear state
    is_complete = False
    status = details.get('status')
    total = details.get('total_chapters_on_site')
    if status and total and len(files_actual) >= total:
        is_complete = True
        completed = set(crawl_state.get('globally_completed_story_urls', []))
        completed.add(story_data_item['url'])
        crawl_state['globally_completed_story_urls'] = sorted(completed)
    backup_crawl_state(state_file)
    state_file = get_state_file(site_key)
    await save_crawl_state(crawl_state, state_file)
    await clear_specific_state_keys(crawl_state, ['processed_chapter_urls_for_current_story'], state_file)
    return is_complete


async def process_genre_item(
    session: aiohttp.ClientSession,
    genre_data: Dict[str, Any], crawl_state: Dict[str, Any], adapter: BaseSiteAdapter, site_key: str
) -> None:
    logger.info(f"\n--- Xử lý thể loại: {genre_data['name']} ---")
    crawl_state['current_genre_url'] = genre_data['url']
    if crawl_state.get('previous_genre_url_in_state_for_stories') != genre_data['url']:
        crawl_state['current_story_index_in_genre'] = 0
    crawl_state['previous_genre_url_in_state_for_stories'] = genre_data['url']
    state_file = get_state_file(site_key)
    await save_crawl_state(crawl_state, state_file)

    retry_time = 0
    max_retry = 5

    while True:
        try:
            stories, total_pages, crawled_pages = await adapter.get_all_stories_from_genre_with_page_check(
                genre_data['name'], genre_data['url'], MAX_STORIES_PER_GENRE_PAGE
            )
            if not stories or len(stories) == 0:
                raise Exception(f"Danh sách truyện rỗng cho genre {genre_data['name']} ({genre_data['url']})")

            if total_pages and (crawled_pages is not None) and crawled_pages < total_pages:
                logger.warning(
                    f"Thể loại {genre_data['name']} chỉ crawl được {crawled_pages}/{total_pages} trang, sẽ retry lần {retry_time+1}..."
                )
                retry_time += 1
                if retry_time >= max_retry:
                    logger.error(f"Thể loại {genre_data['name']} không crawl đủ số trang sau {max_retry} lần.")
                    log_failed_genre(genre_data)
                    return
                await asyncio.sleep(5)
                continue  # Retry tiếp
            break
        except Exception as ex:
            logger.error(f"Lỗi khi crawl genre {genre_data['name']} ({genre_data['url']}): {ex}")
            log_failed_genre(genre_data)
            return

    completed_global = set(crawl_state.get('globally_completed_story_urls', []))
    start_idx = crawl_state.get('current_story_index_in_genre', 0)

    for idx, story in enumerate(stories):
        if idx < start_idx:
            continue
        if MAX_STORIES_TOTAL_PER_GENRE and idx >= MAX_STORIES_TOTAL_PER_GENRE:
            break
        slug = slugify_title(story['title'])
        folder = os.path.join(DATA_FOLDER, slug)
        await ensure_directory_exists(folder)

        if story['url'] in completed_global:
            # update metadata only
            details = await adapter.get_story_details(story['url'], story['title'])
            await save_story_metadata_file(story, genre_data, folder, details, None)
            crawl_state['current_story_index_in_genre'] = idx + 1
            await save_crawl_state(crawl_state, state_file)
            continue

        crawl_state['current_story_index_in_genre'] = idx
        await save_crawl_state(crawl_state, state_file)
        done = await process_story_item(session, story, genre_data, folder, crawl_state, adapter, site_key)
        if done:
            completed_global.add(story['url'])
        crawl_state['globally_completed_story_urls'] = sorted(completed_global)
        crawl_state['current_story_index_in_genre'] = idx + 1
        await save_crawl_state(crawl_state, state_file)

    await clear_specific_state_keys(crawl_state, [
        'current_story_index_in_genre', 'current_genre_url',
        'previous_genre_url_in_state_for_stories'
    ], state_file)


async def retry_failed_genres(adapter, site_key):
    round_idx = 0
    while True:
        if not os.path.exists(FAILED_GENRES_FILE):
            break
        with open(FAILED_GENRES_FILE, "r", encoding="utf-8") as f:
            failed_genres = json.load(f)
        if not failed_genres:
            break

        round_idx += 1
        logger.warning(f"=== [RETRY ROUND {round_idx}] Đang retry {len(failed_genres)} thể loại bị fail... ===")

        to_remove = []
        random.shuffle(failed_genres)
        async with aiohttp.ClientSession() as session:
            for genre in failed_genres:
                delay = min(60, 5 * (2 ** genre.get('fail_count', 1)))  # Tối đa 1 phút delay/gen
                await smart_delay(delay)
                try:
                    await process_genre_with_limit(session, genre, {}, adapter, site_key)
                    # Nếu không lỗi thì xóa khỏi fail
                    to_remove.append(genre)
                    logger.info(f"[RETRY] Thành công genre: {genre['name']}")
                except Exception as ex:
                    genre['fail_count'] = genre.get('fail_count', 1) + 1
                    logger.error(f"[RETRY] Vẫn lỗi genre: {genre['name']}: {ex}")

        # Update lại file failed_genres.json
        if to_remove:
            failed_genres = [g for g in failed_genres if g not in to_remove]
            with open(FAILED_GENRES_FILE, "w", encoding="utf-8") as f:
                json.dump(failed_genres, f, ensure_ascii=False, indent=4)

        # Nếu còn fail và chưa đủ số vòng retry thì retry lại tiếp, ngược lại thì sleep 30 phút rồi thử lại
        if failed_genres:
            if round_idx < RETRY_GENRE_ROUND_LIMIT:
                shuffle_proxies()  # <-- Xáo proxy pool trước khi retry tiếp
                logger.warning(f"Còn {len(failed_genres)} genre fail, bắt đầu vòng retry tiếp theo...")
                continue
            else:
                shuffle_proxies()
                # Đủ số vòng retry, gửi cảnh báo và sleep rồi retry lại từ đầu
                genre_names = ", ".join([g["name"] for g in failed_genres])
                logger.error(f"Sleep {RETRY_SLEEP_SECONDS//60} phút rồi retry lại các genre fail: {genre_names}")
                time.sleep(RETRY_SLEEP_SECONDS)
                round_idx = 0  # Reset lại số vòng retry sau sleep
                continue
        else:
            logger.info("Tất cả genre fail đã retry thành công.")
            break

async def run_crawler(adapter, site_key):
    import aiohttp
    from utils.notifier import send_telegram_notify

    await load_proxies(PROXIES_FILE)
    homepage_url, crawl_state = await initialize_and_log_setup_with_state(site_key)
    genres = await adapter.get_genres()
    total_genres = len(genres)
    genres_done = 0

    async with aiohttp.ClientSession() as session:
        batches = split_batches(genres, GENRE_BATCH_SIZE)
        for batch_idx, genre_batch in enumerate(batches):
            tasks = []
            for genre in genre_batch:
                tasks.append(process_genre_with_limit(session, genre, crawl_state, adapter, site_key))
            logger.info(f"=== Đang crawl batch thể loại {batch_idx+1}/{len(batches)} ({len(genre_batch)} genres song song) ===")
            results = await asyncio.gather(*tasks)
            genres_done += len(genre_batch)
            percent = int(genres_done * 100 / total_genres)
            msg = f"⏳ Tiến độ: {genres_done}/{total_genres} thể loại ({percent}%) đã crawl xong cho {site_key}."
            logger.info(msg)
            await smart_delay()

    logger.info("=== HOÀN TẤT TOÀN BỘ QUÁ TRÌNH CRAWL ===")


if __name__ == '__main__':
    site_key = "metruyenfull"
    if len(sys.argv) > 1:
        site_key = sys.argv[1]
    print(f"[MAIN] Đang chạy crawler cho site: {site_key}")
    merge_all_missing_workers_to_main(site_key)
    adapter = get_adapter(site_key)
    asyncio.run(run_crawler(adapter, site_key))
    asyncio.run(retry_failed_genres(adapter, site_key))