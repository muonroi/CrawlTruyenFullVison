import os
import json
import asyncio
from typing import Optional
from adapters.factory import get_adapter
from utils.chapter_utils import get_chapter_filename, crawl_missing_chapters_for_story
from utils.io_utils import ensure_directory_exists
from utils.logger import logger
from utils.domain_utils import get_site_key_from_url
from config.config import DATA_FOLDER


async def crawl_missing_from_metadata_worker(story_folder: str, site_key: Optional[str] = None):
    """
    Worker đọc danh sách chương thiếu từ missing_from_metadata.json và crawl lại.
    """
    missing_path = os.path.join(story_folder, "missing_from_metadata.json")
    metadata_path = os.path.join(story_folder, "metadata.json")

    if not os.path.exists(missing_path):
        return

    if not os.path.exists(metadata_path):
        logger.warning(f"[WORKER] Không tìm thấy metadata.json trong {story_folder}, không thể crawl.")
        return

    with open(missing_path, "r", encoding="utf-8") as f:
        missing_chapters = json.load(f)

    if not missing_chapters:
        logger.info(f"[WORKER] Không có chương nào cần crawl lại trong {story_folder}.")
        return

    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    site_key = site_key or metadata.get("site_key") or get_site_key_from_url(metadata.get("url"))
    if not site_key:
        logger.warning(f"[WORKER] Không xác định được site_key cho {story_folder}, bỏ qua.")
        return

    adapter = get_adapter(site_key)
    category = metadata.get("categories", [{}])[0]  # Lấy category đầu tiên hoặc rỗng

    logger.info(f"[WORKER] Bắt đầu crawl lại {len(missing_chapters)} chương thiếu cho '{metadata.get('title')}'")
    await crawl_missing_chapters_for_story(
        site_key,
        session=None,
        missing_chapters=missing_chapters,
        metadata=metadata,
        current_category=category,
        story_folder=story_folder,
        crawl_state={},
        num_batches=1,
        state_file=None
    )
    logger.info(f"[WORKER] Đã hoàn tất crawl lại missing cho '{metadata.get('title')}'")

    # Xóa file sau khi hoàn tất
    os.remove(missing_path)
    logger.info(f"[WORKER] Đã xóa file: {missing_path}")


async def crawl_all_missing_stories():
    story_folders = [
        os.path.join(DATA_FOLDER, f)
        for f in os.listdir(DATA_FOLDER)
        if os.path.isdir(os.path.join(DATA_FOLDER, f))
    ]
    tasks = []
    for folder in story_folders:
        if os.path.exists(os.path.join(folder, "missing_from_metadata.json")):
            tasks.append(crawl_missing_from_metadata_worker(folder))
    if tasks:
        await asyncio.gather(*tasks)
    else:
        logger.info("[WORKER] Không có truyện nào cần crawl missing từ metadata.")


if __name__ == '__main__':
    asyncio.run(crawl_all_missing_stories())
