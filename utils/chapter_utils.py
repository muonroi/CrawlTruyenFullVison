import hashlib
import json
import os
from typing import Any, Dict, List

import aiofiles
from config.config import ERROR_CHAPTERS_FILE, LOCK
from analyze.parsers import get_story_chapter_content
from utils.async_utils import SEM
from utils.html_parser import clean_header
from utils.io_utils import atomic_write, atomic_write_json
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
            await atomic_write(filename, content) 
            logger.info(f"Chương '{filename}' đã được cập nhật do nội dung thay đổi.")
            return "updated"
    else:
        content = clean_header(content)
        await atomic_write(filename, content)
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
        atomic_write_json(arr, filename)



def queue_failed_chapter(chapter_data, filename='chapter_retry_queue.json'):
    """Ghi chương lỗi vào queue JSON để retry."""
    path = os.path.join(os.getcwd(), filename)
    # Đọc danh sách cũ
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = []
    # Check đã tồn tại (dựa vào url hoặc filename)
    for item in data:
        if item.get("url") == chapter_data.get("url"):
            return
    data.append(chapter_data)
    atomic_write_json(data, filename)

 



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
                    await save_crawl_state(crawl_state)
        except Exception as e:
            logger.error(f"          Lỗi lưu '{chapter_filename_only}': {e}")
            await log_error_chapter({
                "story_title": story_data_item['title'],
                "chapter_title": chapter_info['title'],
                "chapter_url": chapter_info['url'],
                "error_msg": str(e)
            })
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
        await log_error_chapter({
            "story_title": story_data_item['title'],
            "chapter_title": chapter_info['title'],
            "chapter_url": chapter_info['url'],
            "error_msg": "Không lấy được nội dung"
        })
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

async def process_chapter_batch(
    session, batch_chapters, story_data_item, current_discovery_genre_data,
    story_folder_path, crawl_state, batch_idx, total_batch
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
            crawl_state, successful, failed, original_idx=ch['idx']
        )
        await smart_delay()
    return successful, failed


def get_category_name(story_data_item, current_discovery_genre_data):
    if 'categories' in story_data_item and isinstance(story_data_item['categories'], list):
        if story_data_item['categories']:
            return story_data_item['categories'][0].get('name', '')
    return current_discovery_genre_data.get('name', '')