import asyncio
import os
import re
import aiofiles

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
            await atomic_write(proxies_file_path, """# Thêm proxy của bạn ở đây, mỗi proxy một dòng.
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

async def atomic_write(filename, content):
    tmpfile = filename + ".tmp"
    async with aiofiles.open(tmpfile, 'w', encoding='utf-8') as f: 
        await f.write(content)
    os.replace(tmpfile, filename)

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