import hashlib
import json
import os
from typing import Any, Dict, List
import re
from filelock import FileLock
from unidecode import unidecode
import aiofiles
from adapters.factory import get_adapter
from config.config import LOCK, get_state_file
from analyze.truyenfull_vision_parse import get_story_chapter_content
from utils.async_utils import SEM
from utils.html_parser import clean_header
from utils.io_utils import  safe_write_file, safe_write_json
from utils.logger import logger
from utils.batch_utils import smart_delay
from utils.meta_utils import sanitize_filename
from utils.state_utils import save_crawl_state


async def async_save_chapter_with_hash_check(filename, content: str):
    """
    Lưu file chương, kiểm tra hash để tránh ghi lại nếu nội dung không đổi.
    Trả về: "new" (chưa tồn tại, đã ghi), "unchanged" (tồn tại, giống hệt), "updated" (tồn tại, đã cập nhật).
    """
    hash_val = hashlib.sha256(content.encode('utf-8')).hexdigest()
    file_exists = os.path.exists(filename)
    if file_exists:
        async with aiofiles.open(filename, 'r', encoding='utf-8') as f:
            old_content = await f.read()
        old_hash = hashlib.sha256(old_content.encode('utf-8')).hexdigest()
        if old_hash == hash_val:
            logger.debug(f"Chương '{filename}' đã tồn tại với nội dung giống hệt, bỏ qua ghi lại.")
            return "unchanged"
        else:
            content = clean_header(content)
            await safe_write_file(filename, content)  
            logger.info(f"Chương '{filename}' đã được cập nhật do nội dung thay đổi.")
            return "updated"
    else:
        content = clean_header(content)
        await safe_write_file(filename, content)
        logger.info(f"Chương '{filename}' mới đã được lưu.")
        return "new"
    


async def log_error_chapter(item, filename="error_chapters.json"):
    async with LOCK:
        arr = []
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    arr = json.load(f)
            except Exception:
                arr = []
        arr.append(item)
        await safe_write_json(filename,arr)



async def queue_failed_chapter(chapter_data, filename='chapter_retry_queue.json'):
    """Ghi chương lỗi vào queue JSON để retry."""
    path = os.path.join(os.getcwd(), filename)
    # Đọc danh sách cũ
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        print(f"[ERROR] File {path} bị lỗi hoặc rỗng, sẽ tạo lại file mới.")
        data = {}
    else:
        data = []
    for item in data:
        if item.get("url") == chapter_data.get("url"):
            return
    data.append(chapter_data)  # type: ignore
    await safe_write_json(filename,data)

async def async_download_and_save_chapter(
    chapter_info: Dict[str, Any], story_data_item: Dict[str, Any],
    current_discovery_genre_data: Dict[str, Any],
    chapter_filename_full_path: str, chapter_filename_only: str,
    pass_description: str, chapter_display_idx_log: str,
    crawl_state: Dict[str, Any], successfully_saved: set, failed_list: List[Dict[str, Any]],original_idx: int = 0,
    site_key: str="unknown",
    state_file: str = None    # type: ignore
) -> None:
    url = chapter_info['url']
    logger.info(f"        {pass_description} - Chương {chapter_display_idx_log}: Đang tải '{chapter_info['title']}' ({url})")
    async with SEM:
        content = await get_story_chapter_content(url, chapter_info['title'])

    if content:
        try:
            category_name = get_category_name(story_data_item, current_discovery_genre_data)
            # Gộp nội dung chuẩn file .txt
            chapter_content = (
                f"Nguồn: {url}\n\nTruyện: {story_data_item['title']}\n"
                f"Thể loại: {category_name}\n"
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
                    file_to_save = state_file or get_state_file(site_key)
                    await save_crawl_state(crawl_state, file_to_save)
        except Exception as e:
            logger.error(f"          Lỗi lưu '{chapter_filename_only}': {e}")
            await log_error_chapter({
                "story_title": story_data_item['title'],
                "chapter_title": chapter_info['title'],
                "chapter_url": chapter_info['url'],
                "error_msg": str(e)
            })
            # --- Queue retry chương lỗi ---
            await queue_failed_chapter({
                "chapter_url": chapter_info['url'],
                "chapter_title": chapter_info['title'],
                "story_title": story_data_item['title'],
                "story_url": story_data_item['url'],
                "filename": chapter_filename_full_path,
                'site': site_key,
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
        await log_error_chapter({
            "story_title": story_data_item['title'],
            "chapter_title": chapter_info['title'],
            "chapter_url": chapter_info['url'],
            "error_msg": "Không lấy được nội dung"
        })
        # --- Queue retry chương lỗi ---
        await queue_failed_chapter({
            "chapter_url": chapter_info['url'],
            "chapter_title": chapter_info['title'],
            "story_title": story_data_item['title'],
            "story_url": story_data_item['url'],
            "filename": chapter_filename_full_path,
            'site': site_key,
            "reason": "Không lấy được nội dung"
        })
        failed_list.append({
            'chapter_data': chapter_info,
            'filename': chapter_filename_full_path,
            'filename_only': chapter_filename_only,
            'original_idx': None
        })

async def process_chapter_batch(
    session, batch_chapters, story_data_item, current_discovery_genre_data,
    story_folder_path, crawl_state, batch_idx, total_batch, adapter, site_key
):
    successful, failed = set(), []
    already_crawled = set(crawl_state.get('processed_chapter_urls_for_current_story', []))
    for idx, ch in enumerate(batch_chapters):
        if ch['url'] in already_crawled:
            continue
        fname_only = f"{ch['idx']+1:04d}_{sanitize_filename(ch['title']) or 'untitled'}.txt"
        full_path = os.path.join(story_folder_path, fname_only)
        await async_download_and_save_chapter(
            ch, story_data_item, current_discovery_genre_data,
            full_path, fname_only, f"Batch {batch_idx+1}/{total_batch}", f"{ch['idx']+1}",
            crawl_state, successful, failed, original_idx=ch['idx'], site_key=site_key, state_file=get_state_file(site_key)
        )
        await smart_delay()
    return successful, failed


def get_category_name(story_data_item, current_discovery_genre_data):
    if 'categories' in story_data_item and isinstance(story_data_item['categories'], list):
        if story_data_item['categories']:
            return story_data_item['categories'][0].get('name', '')
    return current_discovery_genre_data.get('name', '')

def get_existing_chapter_nums(story_folder):
    files = [f for f in os.listdir(story_folder) if f.endswith('.txt')]
    chapter_nums = set()
    for f in files:
        # 0001_Tên chương.txt => lấy 0001
        num = f.split('_')[0]
        chapter_nums.add(num)
    return chapter_nums

def get_missing_chapters(chapters, existing_nums):
    # chapters: list từ web, existing_nums: set 0001, 0002...
    missing = []
    for idx, ch in enumerate(chapters):
        num = f"{idx+1:04d}"
        if num not in existing_nums:
            missing.append((idx, ch))
    return missing

def slugify_title(title: str) -> str:
    s = unidecode(title)
    s = re.sub(r'[^\w\s-]', '', s.lower())  # bỏ ký tự đặc biệt, lowercase
    s = re.sub(r'[\s]+', '-', s)            # khoảng trắng thành dấu -
    return s.strip('-_')


def count_txt_files(story_folder_path):
    if not os.path.exists(story_folder_path):
        return 0
    return len([f for f in os.listdir(story_folder_path) if f.endswith('.txt')])


async def async_save_chapter_with_lock(filename, content):
    lockfile = filename + ".lock"
    lock = FileLock(lockfile, timeout=60)
    with lock:
        await async_save_chapter_with_hash_check(filename, content)

