import os
import glob
import asyncio
import aiohttp
import json
import datetime

from config.config import DATA_FOLDER
from utils.meta_utils import count_txt_files
from analyze.parsers import get_chapters_from_story
from main import crawl_missing_chapters_for_story, get_saved_chapters_files
from utils.state_utils import load_crawl_state, save_crawl_state

async def check_and_crawl_missing_all_stories():
    story_folders = [os.path.join(DATA_FOLDER, f) for f in os.listdir(DATA_FOLDER) if os.path.isdir(os.path.join(DATA_FOLDER, f))]
    crawl_state = await load_crawl_state()
    async with aiohttp.ClientSession() as session:
        for story_folder in story_folders:
            metadata_path = os.path.join(story_folder, "metadata.json")
            if not os.path.exists(metadata_path):
                print(f"[SKIP] Không có metadata.json trong {story_folder}")
                continue
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            total_chapters = metadata.get("total_chapters_on_site")
            if not total_chapters or total_chapters < 1:
                print(f"[SKIP] '{story_folder}' thiếu total_chapters_on_site")
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
            print("DEBUG type(metadata):", type(metadata))
            print("DEBUG metadata:", metadata)
            print("DEBUG chapters type:", type(chapters))
            if chapters and isinstance(chapters, list):
                print("DEBUG first chapter:", chapters[0], type(chapters[0]))
            print("DEBUG story_folder:", story_folder)
            await crawl_missing_chapters_for_story(
                session,
                chapters,
                metadata,
                metadata['categories'],
                story_folder,
                crawl_state
            )

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
