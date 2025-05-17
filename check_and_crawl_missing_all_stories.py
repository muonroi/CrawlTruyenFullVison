import os
import asyncio
import json
import datetime

from config.config import DATA_FOLDER, PROXIES_FILE, PROXIES_FOLDER
from config.proxy_provider import load_proxies
from scraper import initialize_scraper, make_request
from utils.async_utils import SEM
from utils.io_utils import create_proxy_template_if_not_exists
from utils.meta_utils import count_txt_files
from analyze.parsers import get_chapters_from_story, get_story_details
from main import crawl_missing_chapters_for_story
from utils.state_utils import load_crawl_state

async def crawl_missing_with_limit(*args, **kwargs):
    print(f"[START] Crawl missing for {args[2]['title']} ...")
    async with SEM:
        result = await crawl_missing_chapters_for_story(*args, **kwargs)
    print(f"[DONE] Crawl missing for {args[2]['title']} ...")
    return result

async def check_and_crawl_missing_all_stories():
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)
    await initialize_scraper()  
    story_folders = [os.path.join(DATA_FOLDER, f) for f in os.listdir(DATA_FOLDER) if os.path.isdir(os.path.join(DATA_FOLDER, f))]
    crawl_state = await load_crawl_state()
    tasks = []
    for story_folder in story_folders:
        metadata_path = os.path.join(story_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            print(f"[SKIP] Không có metadata.json trong {story_folder}")
            continue

        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        total_chapters = metadata.get("total_chapters_on_site")

        # Auto fix metadata nếu thiếu
        if not total_chapters or total_chapters < 1:
            print(f"[SKIP] '{story_folder}' thiếu total_chapters_on_site -> [FIXED] Đang lấy lại metadata qua proxy...")
            details = await get_story_details(metadata.get("url"), metadata.get("title"))
            if details and details.get("total_chapters_on_site"):
                print(f"[FIXED] Cập nhật lại metadata, tổng chương: {details['total_chapters_on_site']}")
                metadata.update(details)
                metadata['total_chapters_on_site'] = details['total_chapters_on_site']
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)
                total_chapters = metadata['total_chapters_on_site']
            else:
                print(f"[SKIP] '{story_folder}' không lấy được metadata mới!")
                continue

        crawled_files = count_txt_files(story_folder)
        if crawled_files >= total_chapters:
            print(f"[OK] '{metadata['title']}' đủ chương ({crawled_files}/{total_chapters})")
            continue

        # Lấy lại danh sách chương thực tế trên web
        story_url = metadata.get("url")
        if not story_url:
            print(f"[SKIP] '{story_folder}' không có url truyện.")
            continue

        print(f"[MISSING] '{metadata['title']}' thiếu chương ({crawled_files}/{total_chapters}) -> Đang kiểm tra/crawl bù...")

        chapters = await get_chapters_from_story(
            story_url, metadata['title'],
            total_chapters_on_site=total_chapters
        )
        if chapters and isinstance(chapters, list):
            print("DEBUG first chapter:", chapters[0], type(chapters[0]))

        current_category = metadata['categories'][0] if metadata.get('categories') and isinstance(metadata['categories'], list) and metadata['categories'] else {}

        # **CHỈ append vào tasks, KHÔNG await trực tiếp!**
        tasks.append(
            crawl_missing_with_limit(None, chapters, metadata, current_category, story_folder, crawl_state)
        )
    # **Chạy song song các task**
    await asyncio.gather(*tasks)


async def loop_every_1h():
    while True:
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n===== [START] Check missing at {now} =====")
        try:
            await check_and_crawl_missing_all_stories()
        except Exception as e:
            print(f"[ERROR] Lỗi khi kiểm tra/crawl missing: {e}")
        print(f"===== [DONE] Sleeping 1 hour =====\n")
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(loop_every_1h())
