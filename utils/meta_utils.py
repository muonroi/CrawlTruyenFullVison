import json
import os
import shutil
import time
from typing import Any, Dict, Optional

import aiofiles
from utils.logger import logger
from utils.io_utils import atomic_write, ensure_backup_folder, ensure_directory_exists


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
        await atomic_write(metadata_file, json.dumps(metadata_to_save, ensure_ascii=False, indent=4))
        logger.info(f"Đã lưu/cập nhật metadata cho truyện vào: {metadata_file}")
        return metadata_to_save
    except Exception as e:
        logger.error(f"LỖI khi lưu metadata '{metadata_file}': {e}")
        return metadata_to_save
    
def is_story_complete(story_folder_path: str, total_chapters_on_site: int) -> bool:
    """Kiểm tra số file .txt đã crawl có đủ không."""
    files = [f for f in os.listdir(story_folder_path) if f.endswith('.txt')]
    return len(files) >= total_chapters_on_site

def count_txt_files(story_folder_path):
    return len([f for f in os.listdir(story_folder_path) if f.endswith('.txt')])

def sanitize_filename(filename):
    # Đơn giản hóa tên file, tránh lỗi tên
    import re
    filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
    return filename.strip()


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


def backup_crawl_state(state_file='crawl_state.json', backup_folder="backup"):
    ensure_backup_folder(backup_folder)
    ts = time.strftime("%Y%m%d_%H%M%S")
    base_name = os.path.basename(state_file)
    backup_file = os.path.join(backup_folder, f"{base_name}.bak_{ts}")
    shutil.copy(state_file, backup_file)