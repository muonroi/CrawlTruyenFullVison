import json
import asyncio
import os
import time
from analyze.parsers import get_story_chapter_content
from utils.utils import async_save_chapter_with_hash_check

async def retry_queue(filename='chapter_retry_queue.json', interval=30):
    print(f"[RetryQueue] Bắt đầu quan sát file {filename}, mỗi {interval}s...")
    while True:
        if not os.path.exists(filename):
            print(f"[RetryQueue] Không tìm thấy file queue: {filename}. Đợi {interval}s rồi kiểm tra lại...")
            await asyncio.sleep(interval)
            continue
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                queue = json.load(f)
        except Exception as e:
            print(f"[RetryQueue] Lỗi đọc file {filename}: {e}. Đợi {interval}s...")
            await asyncio.sleep(interval)
            continue
        if not queue:
            print(f"[RetryQueue] Queue rỗng, đợi {interval}s rồi kiểm tra lại...")
            await asyncio.sleep(interval)
            continue

        print(f"[RetryQueue] Bắt đầu retry {len(queue)} chương lỗi...")
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
                # Nếu thành công, đánh dấu để xoá khỏi queue
                to_remove.append(item)
            else:
                print("-> Vẫn lỗi")

        # Xoá chương đã thành công khỏi queue
        if to_remove:
            queue = [item for item in queue if item not in to_remove]
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(queue, f, ensure_ascii=False, indent=2)
            print(f"[RetryQueue] Đã xoá {len(to_remove)} chương khỏi queue.")

        print(f"[RetryQueue] Đợi {interval}s trước khi kiểm tra lại queue.")
        await asyncio.sleep(interval)

if __name__ == "__main__":
    asyncio.run(retry_queue())
