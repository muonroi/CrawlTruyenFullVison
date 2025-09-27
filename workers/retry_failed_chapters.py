import asyncio
import os
from utils.logger import logger
from utils.chapter_utils import async_save_chapter_with_hash_check, get_category_name
from utils.io_utils import ensure_directory_exists
from adapters.factory import get_adapter
from scraper import initialize_scraper
from utils.anti_bot import is_anti_bot_content

async def retry_single_chapter(chapter_data: dict):
    """
    Xử lý việc tải lại một chương duy nhất từ thông tin job trên Kafka.
    """
    site_key = chapter_data.get('site') or chapter_data.get('site_key')
    url = chapter_data.get('chapter_url')
    chapter_title = chapter_data.get('chapter_title')
    story_title = chapter_data.get('story_title')
    filename_path = chapter_data.get('filename')

    if not all([site_key, url, chapter_title, story_title, filename_path]):
        logger.error(f"[RetryWorker] Job thiếu thông tin cần thiết: {chapter_data}")
        return

    logger.info(f"[RetryWorker] Bắt đầu retry chương: '{chapter_title}' của truyện '{story_title}'")

    try:
        adapter = get_adapter(site_key)
        await initialize_scraper(site_key)

        content = await adapter.get_chapter_content(url, chapter_title, site_key)

        if content and not is_anti_bot_content(content):
            # Lấy category từ chapter_data nếu có, nếu không thì để rỗng
            story_data_item = chapter_data.get('story_data_item', {})
            current_discovery_genre_data = chapter_data.get('current_discovery_genre_data', {})
            category_name = get_category_name(story_data_item, current_discovery_genre_data)

            # Gộp nội dung chuẩn file .txt
            full_content = (
                f"Nguồn: {url}\n\nTruyện: {story_title}\n"
                f"Thể loại: {category_name}\n"
                f"Chương: {chapter_title}\n\n"
                f"{content}"
            )

            dir_path = os.path.dirname(filename_path)
            await ensure_directory_exists(dir_path)
            save_result = await async_save_chapter_with_hash_check(filename_path, full_content)
            logger.info(f"[RetryWorker] ✅ Retry thành công chương '{chapter_title}'. Kết quả: {save_result}")
        else:
            reason = "anti-bot" if content and is_anti_bot_content(content) else "nội dung rỗng"
            logger.warning(f"[RetryWorker] ❌ Retry thất bại, {reason}: '{chapter_title}'")
            # Optional: Gửi tới một topic "dead letter" khác để xử lý thủ công
            # from kafka.kafka_producer import send_job
            # await send_job(chapter_data, topic="dead_letter_chapters")

    except Exception as e:
        logger.exception(f"[RetryWorker] ❌ Lỗi nghiêm trọng khi retry chương '{chapter_title}': {e}")