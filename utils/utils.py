import hashlib
import os
import random
import json
import shutil
import time
import logging
import asyncio
import aiofiles
from typing import Any, Dict, List, Optional
from logging.handlers import RotatingFileHandler
from config.config import ERROR_CHAPTERS_FILE, REQUEST_DELAY, STATE_FILE
from analyze.parsers import get_story_chapter_content
from utils.logger import logger

SEM = asyncio.Semaphore(5)

CSTATE_LOCK = asyncio.Lock()
# --- Thiết lập Logging (giữ nguyên) ---
LOG_FILE_PATH = "crawler.log"

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
fh = RotatingFileHandler(LOG_FILE_PATH, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)

async def load_crawl_state() -> Dict[str, Any]:
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, STATE_FILE)
    if exists:
        try:
            async with aiofiles.open(STATE_FILE, 'r', encoding='utf-8') as f:
                data = await f.read()
            state = json.loads(data)
            logger.info(f"Đã tải trạng thái crawl từ {STATE_FILE}: {state}")
            return state
        except Exception as e:
            logger.error(f"Lỗi khi tải trạng thái crawl từ {STATE_FILE}: {e}. Bắt đầu crawl mới.")
    return {}

async def save_crawl_state(state: Dict[str, Any]) -> None:
    async with CSTATE_LOCK:
        try:
            content = json.dumps(state, ensure_ascii=False, indent=4)
            await atomic_write(STATE_FILE, content)
            logger.info(f"Đã lưu trạng thái crawl vào {STATE_FILE}")
        except Exception as e:
            logger.error(f"Lỗi khi lưu trạng thái crawl vào {STATE_FILE}: {e}")

async def clear_specific_state_keys(state: Dict[str, Any], keys_to_remove: List[str]) -> None:
    updated = False
    for key in keys_to_remove:
        if key in state:
            del state[key]
            updated = True
            logger.debug(f"Đã xóa key '{key}' khỏi trạng thái crawl.")
    if updated:
        await save_crawl_state(state)

async def clear_crawl_state_component(state: Dict[str, Any], component_key: str) -> None:
    if component_key in state:
        del state[component_key]
        if component_key == "current_genre_url":
            state.pop("current_story_url", None)
            state.pop("current_story_index_in_genre", None)
            state.pop("processed_chapter_urls_for_current_story", None)
        elif component_key == "current_story_url":
            state.pop("processed_chapter_urls_for_current_story", None)
    await save_crawl_state(state)

async def clear_all_crawl_state() -> None:
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, STATE_FILE)
    if exists:
        try:
            await loop.run_in_executor(None, os.remove, STATE_FILE)
            logger.info(f"Đã xóa file trạng thái crawl: {STATE_FILE}")
        except Exception as e:
            logger.error(f"Lỗi khi xóa file trạng thái crawl: {e}")

async def ensure_directory_exists(dir_path: str) -> bool:
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, dir_path)
    if not exists:
        try:
            await loop.run_in_executor(None, os.makedirs, dir_path, True)
            logger.info(f"Đã tạo thư mục: {dir_path}")
            return True
        except Exception as e:
            logger.error(f"LỖI khi tạo thư mục {dir_path}: {e}")
            return False
    return True

async def create_proxy_template_if_not_exists(proxies_file_path: str, proxies_folder_path: str) -> bool:
    if not await ensure_directory_exists(proxies_folder_path):
        return False
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, proxies_file_path)
    if not exists:
        try:
            async with aiofiles.open(proxies_file_path, 'w', encoding='utf-8') as f:
                await f.write("""# Thêm proxy của bạn ở đây, mỗi proxy một dòng.
# Ví dụ: http://host:port
# Ví dụ: http://user:pass@host:port
# Ví dụ (IP:PORT sẽ dùng GLOBAL credentials): 123.45.67.89:1080
""")
            logger.info(f"Đã tạo file proxies mẫu: {proxies_file_path}")
            return True
        except Exception as e:
            logger.error(f"LỖI khi tạo file proxies mẫu {proxies_file_path}: {e}")
            return False
    return True

# ----------------------------------------------------------------------------
# Hàm xử lý metadata truyện (Async)
# ----------------------------------------------------------------------------

async def save_story_metadata_file(
    story_base_data: Dict[str, Any],
    current_discovery_genre_data: Optional[Dict[str, Any]],
    story_folder_path: str,
    fetched_story_details: Optional[Dict[str, Any]],
    existing_metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Bất đồng bộ: Lưu hoặc cập nhật file metadata.json cho truyện.
    Trả về dict metadata đã lưu.
    """
    await ensure_directory_exists(story_folder_path)
    metadata_file = os.path.join(story_folder_path, "metadata.json")
    metadata_to_save = existing_metadata.copy() if existing_metadata else {}

    # Cập nhật fields cơ bản
    metadata_to_save["title"] = story_base_data.get("title", metadata_to_save.get("title"))
    metadata_to_save["url"] = story_base_data.get("url", metadata_to_save.get("url"))
    metadata_to_save.setdefault("author", story_base_data.get("author"))
    metadata_to_save.setdefault("image_url", story_base_data.get("image_url"))
    metadata_to_save["crawled_by"] = "muonroi"

    # Cập nhật categories
    current_cats = metadata_to_save.get("categories", [])
    seen_urls = {cat.get("url") for cat in current_cats if cat.get("url")}
    if current_discovery_genre_data and current_discovery_genre_data.get("url") not in seen_urls:
        current_cats.append({"name": current_discovery_genre_data.get("name"), "url": current_discovery_genre_data.get("url")})
    metadata_to_save["categories"] = sorted(current_cats, key=lambda x: (x.get("name") or "").lower())

    # Cập nhật chi tiết
    if fetched_story_details:
        for key in ["description","status","source","rating_value","rating_count","total_chapters_on_site"]:
            if fetched_story_details.get(key) is not None:
                metadata_to_save[key] = fetched_story_details.get(key)
            else:
                metadata_to_save.setdefault(key, None)

    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    metadata_to_save["metadata_updated_at"] = now_str
    if "crawled_at" not in metadata_to_save:
        metadata_to_save["crawled_at"] = now_str

    try:
        async with aiofiles.open(metadata_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(metadata_to_save, ensure_ascii=False, indent=4))
        logger.info(f"Đã lưu/cập nhật metadata cho truyện vào: {metadata_file}")
        return metadata_to_save
    except Exception as e:
        logger.error(f"LỖI khi lưu metadata '{metadata_file}': {e}")
        return metadata_to_save

# Đồng bộ: hàm này chỉ làm sạch tên file

def sanitize_filename(filename):
    # Đơn giản hóa tên file, tránh lỗi tên
    import re
    filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
    return filename.strip()

def is_story_complete(story_folder_path: str, total_chapters_on_site: int) -> bool:
    """Kiểm tra số file .txt đã crawl có đủ không."""
    files = [f for f in os.listdir(story_folder_path) if f.endswith('.txt')]
    return len(files) >= total_chapters_on_site

def count_txt_files(story_folder_path):
    return len([f for f in os.listdir(story_folder_path) if f.endswith('.txt')])

def add_missing_story(story_title, story_url, total_chapters, crawled_chapters, filename="missing_chapters.json"):
    """Thêm truyện thiếu chương vào file json."""
    path = os.path.join(os.getcwd(), filename)
    # Đọc danh sách cũ
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = []
    # Check đã tồn tại chưa
    for item in data:
        if item.get("url") == story_url:
            return  # Không thêm trùng
    data.append({
        "title": story_title,
        "url": story_url,
        "total_chapters": total_chapters,
        "crawled_chapters": crawled_chapters
    })
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

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
            async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
                await f.write(content)
            logger.info(f"Chương '{filename}' đã được cập nhật do nội dung thay đổi.")
            return "updated"
    else:
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(content)
        logger.info(f"Chương '{filename}' mới đã được lưu.")
        return "new"
    
async def smart_delay(base=REQUEST_DELAY):
    delay = random.uniform(base*0.7, base*1.3)
    await asyncio.sleep(delay)

def log_error_chapter(story_title, chapter_title, chapter_url,
    error_msg="Không lấy được nội dung"):
    import json
    data = {
        "story": story_title,
        "chapter": chapter_title,
        "url": chapter_url,
        "error_msg": error_msg
    }
    if os.path.exists(ERROR_CHAPTERS_FILE):
        with open(ERROR_CHAPTERS_FILE, 'r', encoding='utf-8') as f:
            arr = json.load(f)
    else:
        arr = []
    arr.append(data)
    with open(ERROR_CHAPTERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(arr, f, ensure_ascii=False, indent=2)

def backup_crawl_state(state_file='crawl_state.json'):
    ts = time.strftime("%Y%m%d_%H%M%S")
    backup_file = f"{state_file}.bak_{ts}"
    shutil.copy(state_file, backup_file)
    logger.info(f"Đã backup state: {backup_file}")

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
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def atomic_write(filename, content):
    tmpfile = filename + ".tmp"
    async with aiofiles.open(tmpfile, 'w', encoding='utf-8') as f: 
        await f.write(content)
    os.replace(tmpfile, filename) 

def split_batches(lst, num_batches):
    k, m = divmod(len(lst), num_batches)
    batches = []
    start = 0
    for i in range(num_batches):
        end = start + k + (1 if i < m else 0)
        batches.append(lst[start:end])
        start = end
    return batches

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
