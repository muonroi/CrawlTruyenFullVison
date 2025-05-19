
import os
import re
from filelock import FileLock
from unidecode import unidecode

from core.chapter_service import async_save_chapter_with_hash_check

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