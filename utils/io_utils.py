import asyncio
import json
import os
import re
import shutil
import aiofiles
from filelock import FileLock, Timeout

from config.config import COMPLETED_FOLDER, FAILED_GENRES_FILE
from utils.logger import logger


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
            await safe_write_file(proxies_file_path, """# Thêm proxy của bạn ở đây, mỗi proxy một dòng.
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

async def safe_write_json(filepath, obj, timeout=60):
    import json
    lock_path = filepath + '.lock'
    tmp_path = filepath + '.tmp'
    lock = FileLock(lock_path, timeout=timeout)
    try:
        with lock:
            async with aiofiles.open(tmp_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(obj, ensure_ascii=False, indent=4))
            await asyncio.sleep(0.05)
            os.replace(tmp_path, filepath)
    except Timeout:
        logger.error(f"Timeout khi ghi file {filepath}. File lock: {lock_path} bị kẹt!")
        # Tự remove lock nếu cần, như bên trên
    except Exception as e:
        logger.error(f"Lỗi khi ghi file {filepath}: {e}")


def ensure_backup_folder(backup_folder="backup"):
    if not os.path.exists(backup_folder):
        os.makedirs(backup_folder)

def load_patterns(pattern_file):
    with open(pattern_file, encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    patterns = [re.compile(l, re.IGNORECASE) for l in lines]
    return patterns

def filter_lines_by_patterns(lines, patterns):
    result = []
    for line in lines:
        l = line.strip()
        if not l:
            continue
        if any(p.search(l) for p in patterns):
            continue
        if len(l) < 5 and not l.endswith(('.', '?', '!', ':', '"', "'")):
            continue
        result.append(l)
    return result

def move_story_to_completed(story_folder, genre_name):
    dest_genre_folder = os.path.join(COMPLETED_FOLDER, genre_name)
    os.makedirs(dest_genre_folder, exist_ok=True)
    # Move truyện vào folder completed/genre_name/
    dest_folder = os.path.join(dest_genre_folder, os.path.basename(story_folder))
    if not os.path.exists(dest_folder):
        shutil.move(story_folder, dest_folder)
        print(f"[INFO] Đã chuyển truyện sang {dest_genre_folder}")

def log_failed_genre(genre_data):
    try:
        if os.path.exists(FAILED_GENRES_FILE):
            with open(FAILED_GENRES_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f)
        else:
            arr = []
        if genre_data not in arr:
            arr.append(genre_data)
            with open(FAILED_GENRES_FILE, "w", encoding="utf-8") as f:
                json.dump(arr, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"Lỗi khi log failed genre: {e}")

async def safe_write_file(file_path, content, timeout=30, auto_remove_lock=True):
    lock_path = file_path + ".lock"
    tmp_path = file_path + ".tmp"
    lock = FileLock(lock_path, timeout=timeout)
    try:
        with lock:
            async with aiofiles.open(tmp_path, "w", encoding="utf-8") as f:
                await f.write(content)
            await asyncio.sleep(0.01)
            os.replace(tmp_path, file_path)
    except Timeout:
        logger.error(f"Timeout khi ghi file {file_path}. File lock: {lock_path} bị kẹt! Hãy xóa file .lock này rồi chạy lại.")
        if auto_remove_lock and os.path.exists(lock_path):
            os.remove(lock_path)
            logger.warning(f"Đã tự động xóa file lock: {lock_path}")
    except Exception as e:
        logger.error(f"Lỗi khi ghi file {file_path}: {e}")

