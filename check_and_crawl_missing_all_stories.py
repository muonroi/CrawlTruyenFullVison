import os
import asyncio
import json
import datetime
import shutil
from typing import cast
from config.config import COMPLETED_FOLDER, DATA_FOLDER, PROXIES_FILE, PROXIES_FOLDER
from config.proxy_provider import load_proxies
from scraper import initialize_scraper, make_request
from utils.async_utils import SEM
from utils.io_utils import create_proxy_template_if_not_exists
from utils.meta_utils import count_txt_files
from analyze.parsers import get_all_stories_from_genre, get_chapters_from_story, get_story_details
from main import crawl_missing_chapters_for_story
from utils.notifier import send_telegram_notify
from utils.state_utils import load_crawl_state

async def crawl_missing_with_limit(*args, **kwargs):
    print(f"[START] Crawl missing for {args[2]['title']} ...")
    async with SEM:
        result = await crawl_missing_chapters_for_story(*args, **kwargs)
    print(f"[DONE] Crawl missing for {args[2]['title']} ...")
    return result
import os
import shutil

async def check_genre_complete_and_notify(genre_name, genre_url):
    # 1. Lấy danh sách truyện trên web
    stories_on_web = await get_all_stories_from_genre(genre_name, genre_url)
    
    # 2. Lấy danh sách truyện đã crawl xong trong completed
    completed_folders = os.listdir(os.path.join(COMPLETED_FOLDER, genre_name))
    completed_titles = []
    for folder in completed_folders:
        meta_path = os.path.join(COMPLETED_FOLDER, genre_name, folder, "metadata.json")
        if os.path.exists(meta_path):
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
                completed_titles.append(meta.get("title"))
    
    # 3. Check còn truyện nào trên web mà chưa nằm trong completed không
    missing = [story for story in stories_on_web if story["title"] not in completed_titles]
    if not missing:
        await send_telegram_notify(f"🎉 Đã crawl xong **TẤT CẢ** truyện của thể loại [{genre_name}] trên web!")


async def check_and_crawl_missing_all_stories():
    # Step 1: Chuẩn bị thông tin genre (name->url)
    from analyze.parsers import get_all_genres
    HOME_PAGE_URL = "https://truyenfull.vision"
    all_genres = await get_all_genres(HOME_PAGE_URL)
    genre_name_to_url = {g['name']: g['url'] for g in all_genres}
    genre_complete_checked = set()
    os.makedirs(COMPLETED_FOLDER, exist_ok=True)
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)
    await initialize_scraper()

    # Step 2: Lấy danh sách tất cả story folder cần crawl
    story_folders = [
        os.path.join(DATA_FOLDER, cast(str, f))
        for f in os.listdir(DATA_FOLDER)
        if os.path.isdir(os.path.join(DATA_FOLDER, cast(str, f)))
    ]
    crawl_state = await load_crawl_state()
    tasks = []
    for story_folder in story_folders:
        # Bỏ qua nếu truyện đã ở completed
        if os.path.dirname(story_folder) == os.path.abspath(COMPLETED_FOLDER):
            continue
        metadata_path = os.path.join(story_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            print(f"[SKIP] Không có metadata.json trong {story_folder}")
            continue

        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        total_chapters = metadata.get("total_chapters_on_site")

        # Lấy tên thể loại đầu tiên
        genre_name = None
        if metadata.get('categories') and isinstance(metadata['categories'], list) and metadata['categories']:
            genre_name = metadata['categories'][0].get('name')
        if not genre_name:
            genre_name = "Unknown"  # fallback nếu không có thể loại

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
        if crawled_files < total_chapters:
            print(f"[MISSING] '{metadata['title']}' thiếu chương ({crawled_files}/{total_chapters}) -> Đang kiểm tra/crawl bù...")
            chapters = await get_chapters_from_story(
                metadata.get("url"), metadata['title'], total_chapters_on_site=total_chapters
            )
            current_category = metadata['categories'][0] if metadata.get('categories') and isinstance(metadata['categories'], list) and metadata['categories'] else {}
            tasks.append(
                crawl_missing_with_limit(None, chapters, metadata, current_category, story_folder, crawl_state)
            )
    # Step 3: Đợi tất cả các task crawl missing chương xong
    if tasks:
        await asyncio.gather(*tasks)

    # Step 4: Move tất cả các truyện đã đủ chương vào completed và kiểm tra notify thể loại
    for story_folder in story_folders:
        metadata_path = os.path.join(story_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            continue
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        total_chapters = metadata.get("total_chapters_on_site")
        genre_name = None
        if metadata.get('categories') and isinstance(metadata['categories'], list) and metadata['categories']:
            genre_name = metadata['categories'][0].get('name')
        if not genre_name:
            genre_name = "Unknown"
        crawled_files = count_txt_files(story_folder)
        if crawled_files >= total_chapters:
            dest_genre_folder = os.path.join(COMPLETED_FOLDER, genre_name)
            os.makedirs(dest_genre_folder, exist_ok=True)
            dest_folder = os.path.join(dest_genre_folder, os.path.basename(story_folder))
            if not os.path.exists(dest_folder):
                shutil.move(story_folder, dest_folder)
                print(f"[INFO] Đã chuyển truyện '{metadata['title']}' sang {dest_genre_folder}")
            if genre_name not in genre_complete_checked:
                genre_url = genre_name_to_url.get(genre_name)
                if genre_url:
                    await check_genre_complete_and_notify(genre_name, genre_url)
                genre_complete_checked.add(genre_name)



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
