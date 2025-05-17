import json
import asyncio
import os
from datetime import datetime
from analyze.parsers import get_story_chapter_content
from config.config import PROXIES_FILE, PROXIES_FOLDER
from config.proxy_provider import load_proxies
from scraper import initialize_scraper
from utils.chapter_utils import async_save_chapter_with_hash_check
from utils.io_utils import atomic_write_json, create_proxy_template_if_not_exists

async def retry_queue(filename='chapter_retry_queue.json', interval=900):  # 900 giây = 15 phút
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)
    await initialize_scraper()  
    print(f"[RetryQueue] Bắt đầu quan sát file {filename}, mỗi {interval//60} phút...")
    while True:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if not os.path.exists(filename):
            print(f"[{now}] [RetryQueue] Không tìm thấy file queue: {filename}. Đợi {interval//60} phút rồi kiểm tra lại...")
            await asyncio.sleep(interval)
            continue
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                queue = json.load(f)
        except Exception as e:
            print(f"[{now}] [RetryQueue] Lỗi đọc file {filename}: {e}. Đợi {interval//60} phút...")
            await asyncio.sleep(interval)
            continue
        if not queue:
            print(f"[{now}] [RetryQueue] Queue rỗng, đợi {interval//60} phút rồi kiểm tra lại...")
            await asyncio.sleep(interval)
            continue

        print(f"[{now}] [RetryQueue] Bắt đầu retry {len(queue)} chương lỗi...")
        to_remove = []
        for item in queue:
            url = item['chapter_url']
            chapter_title = item['chapter_title']
            story_title = item['story_title']
            filename_path = item['filename']

            print(f"Retry chương: {chapter_title} ({url}) ... của truyện: {story_title}")
            content = await get_story_chapter_content(url, chapter_title)
            if content:
                save_result = await async_save_chapter_with_hash_check(filename_path, content)
                print(f"-> Result: {save_result}")
                to_remove.append(item)
            else:
                print("-> Vẫn lỗi")

        # Xoá chương đã thành công khỏi queue
        if to_remove:
            queue = [item for item in queue if item not in to_remove]
            atomic_write_json(queue, filename)
            print(f"[{now}] [RetryQueue] Đã xoá {len(to_remove)} chương khỏi queue.")

        print(f"[{now}] [RetryQueue] Đợi {interval//60} phút trước khi kiểm tra lại queue.")
        await asyncio.sleep(interval)

if __name__ == "__main__":
    asyncio.run(retry_queue())
