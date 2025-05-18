from collections import defaultdict
import json
import asyncio
import os
from datetime import datetime
from analyze.truyenfull_vision_parse import get_story_chapter_content
from config.config import PROXIES_FILE, PROXIES_FOLDER
from config.proxy_provider import load_proxies
from scraper import initialize_scraper
from utils.chapter_utils import async_save_chapter_with_hash_check
from utils.io_utils import create_proxy_template_if_not_exists, ensure_directory_exists, safe_write_json
from utils.notifier import send_telegram_notify
from adapters.factory import get_adapter

MAX_RETRY_FAILS = 5
retry_fail_count = 0


async def retry_queue(filename='chapter_retry_queue.json', interval=900):
    retry_fail_count = 0
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)
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

        # --- Nhóm các chương lỗi theo từng site ---
        site_to_items = defaultdict(list)
        for item in queue:
            site_key = item.get('site', 'truyenfull')
            site_to_items[site_key].append(item)

        to_remove = []
        for site_key, items in site_to_items.items():
            adapter = get_adapter(site_key)
            await initialize_scraper(adapter)  # <-- ĐÃ TRUYỀN ĐÚNG ADAPTER!
            print(f"[{now}] [RetryQueue][{site_key}] Retry {len(items)} chương lỗi...")
            for item in items:
                url = item['chapter_url']
                chapter_title = item['chapter_title']
                story_title = item['story_title']
                filename_path = item['filename']

                print(f"Retry chương: {chapter_title} ({url}) ... của truyện: {story_title} (site: {site_key})")
                content = await adapter.get_chapter_content(url, chapter_title)
                if content:
                    retry_fail_count = 0
                    dir_path = os.path.dirname(filename_path)
                    await ensure_directory_exists(dir_path)
                    save_result = await async_save_chapter_with_hash_check(filename_path, content)
                    print(f"-> Result: {save_result}")
                    to_remove.append(item)
                else:
                    retry_fail_count += 1
                    if retry_fail_count >= MAX_RETRY_FAILS:
                        asyncio.create_task(send_telegram_notify(
                            f"[Crawl Notify][{site_key}] Retry quá nhiều lần nhưng toàn bộ đều lỗi (proxy/blocked)! Queue: {filename}"))
                        retry_fail_count = 0

        # Xoá chương đã thành công khỏi queue
        if to_remove:
            queue = [item for item in queue if item not in to_remove]
            await safe_write_json(filename, queue)
            print(f"[{now}] [RetryQueue] Đã xoá {len(to_remove)} chương khỏi queue.")

        print(f"[{now}] [RetryQueue] Đợi {interval//60} phút trước khi kiểm tra lại queue.")
        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(retry_queue())
