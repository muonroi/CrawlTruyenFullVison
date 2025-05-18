import os
import asyncio
import json
import datetime
import shutil
from typing import cast

from filelock import FileLock
from adapters.factory import get_adapter
from config.config import BASE_URLS, COMPLETED_FOLDER, DATA_FOLDER, LOADED_PROXIES, PROXIES_FILE, PROXIES_FOLDER
from config.proxy_provider import load_proxies
from scraper import initialize_scraper
from utils.chapter_utils import count_txt_files
from utils.logger import logger
from utils.async_utils import SEM
from utils.io_utils import create_proxy_template_if_not_exists, safe_write_file
from analyze.truyenfull_vision_parse import get_all_genres, get_all_stories_from_genre, get_chapters_from_story, get_story_details
from main import crawl_missing_chapters_for_story
from utils.notifier import send_telegram_notify
from utils.state_utils import load_crawl_state

MAX_CONCURRENT_STORIES = 3
STORY_SEM = asyncio.Semaphore(MAX_CONCURRENT_STORIES)

async def crawl_story_with_limit(*args, **kwargs):
    async with STORY_SEM:
        await crawl_missing_with_limit(*args, **kwargs)

async def crawl_missing_with_limit(*args, **kwargs):
    print(f"[START] Crawl missing for {args[2]['title']} ...")
    async with SEM:
        result = await crawl_missing_chapters_for_story(*args, **kwargs)
    print(f"[DONE] Crawl missing for {args[2]['title']} ...")
    return result

async def check_genre_complete_and_notify(genre_name, genre_url):
    stories_on_web = await get_all_stories_from_genre(genre_name, genre_url)
    completed_folder = os.path.join(COMPLETED_FOLDER, genre_name)
    completed_folders = os.listdir(completed_folder)
    completed_titles = []
    for folder in completed_folders:
        meta_path = os.path.join(completed_folder, folder, "metadata.json")
        if os.path.exists(meta_path):
            lock_path = meta_path + ".lock"
            lock = FileLock(lock_path, timeout=30)
            with lock:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                    completed_titles.append(meta.get("title"))
    missing = [story for story in stories_on_web if story["title"] not in completed_titles]
    if not missing:
        await send_telegram_notify(f"🎉 Đã crawl xong **TẤT CẢ** truyện của thể loại [{genre_name}] trên web!")


def get_auto_batch_count(fixed=None, default=10, min_batch=1, max_batch=20, num_items=None):
    if fixed is not None:
        return fixed
    batch = default
    if num_items:
        batch = min(batch, num_items)
    return min(batch, max_batch)

async def check_and_crawl_missing_all_stories(adapter):
    HOME_PAGE_URL = "https://truyenfull.vision"
    all_genres = await get_all_genres(HOME_PAGE_URL)
    genre_name_to_url = {g['name']: g['url'] for g in all_genres}
    genre_complete_checked = set()
    os.makedirs(COMPLETED_FOLDER, exist_ok=True)
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)
    await initialize_scraper(adapter)

    # Lấy danh sách tất cả story folder cần crawl
    story_folders = [
        os.path.join(DATA_FOLDER, cast(str, f))
        for f in os.listdir(DATA_FOLDER)
        if os.path.isdir(os.path.join(DATA_FOLDER, cast(str, f)))
    ]
    crawl_state = await load_crawl_state()
    tasks = []
    for story_folder in story_folders:
        if os.path.dirname(story_folder) == os.path.abspath(COMPLETED_FOLDER):
            continue
        metadata_path = os.path.join(story_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            print(f"[SKIP] Không có metadata.json trong {story_folder}")
            continue

        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        total_chapters = metadata.get("total_chapters_on_site")
        genre_name = None
        if metadata.get('categories') and isinstance(metadata['categories'], list) and metadata['categories']:
            genre_name = metadata['categories'][0].get('name')
        if not genre_name:
            genre_name = "Unknown"

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
            print(f"[MISSING] '{metadata['title']}' thiếu chương ({crawled_files}/{total_chapters}) -> Đang kiểm tra/crawl bù từ mọi nguồn...")

            # --- Bổ sung crawl từ mọi nguồn ---
            for source in metadata.get("sources", []):
                site_key = source.get("site")
                url = source.get("url")
                if not site_key or not url:
                    continue
                adapter = get_adapter(site_key)
                # Lấy danh sách chương từ nguồn này
                try:
                    chapters = await adapter.get_chapter_list(url, metadata['title'])
                except Exception as ex:
                    print(f"  [ERROR] Không lấy được chapter list từ {site_key}: {ex}")
                    continue

                # Chỉ crawl những chương thiếu thực sự
                existing_files = set(os.listdir(story_folder))
                missing_chapters = []
                for idx, ch in enumerate(chapters):
                    fname_only = f"{idx+1:04d}_{ch.get('title', 'untitled')}.txt"
                    if fname_only not in existing_files:
                        ch['idx'] = idx # type: ignore
                        missing_chapters.append(ch)
                if not missing_chapters:
                    print(f"  Không còn chương nào thiếu ở nguồn {site_key}.")
                    continue

                print(f"  Bắt đầu crawl bổ sung {len(missing_chapters)} chương từ nguồn {site_key}")
                current_category = metadata['categories'][0] if metadata.get('categories') and isinstance(metadata['categories'], list) and metadata['categories'] else {}
                num_batches = get_auto_batch_count(fixed=10)
                logger.info(f"Auto chọn {num_batches} batch cho truyện {metadata['title']} (site: {site_key}, proxy usable: {len(LOADED_PROXIES)})")
                # Bắt buộc truyền site_key xuống các hàm dưới nếu cần
                tasks.append(
                    crawl_story_with_limit(None, missing_chapters, metadata, current_category, story_folder, crawl_state, num_batches=num_batches)
                )
    if tasks:
        await asyncio.gather(*tasks)

    # Move completed stories + notify
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

        # ==== Bổ sung kiểm tra fields bắt buộc ====
        fields_required = ['description', 'author', 'cover', 'categories', 'title', 'total_chapters_on_site']
        meta_ok = all(metadata.get(f) for f in fields_required)
        if not meta_ok:
            print(f"[SKIP] '{story_folder}' thiếu trường quan trọng, sẽ cố gắng lấy lại metadata...")
            # get_story_details là async
            details = await get_story_details(metadata.get("url"), metadata.get("title"))
            if details and all(details.get(f) for f in fields_required):
                print(f"[FIXED] Đã bổ sung metadata đủ cho '{metadata.get('title')}'")
                metadata.update(details)
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)
            else:
                print(f"[ERROR] Không lấy đủ metadata cho '{metadata.get('title')}'! Sẽ bỏ qua move.")
                continue
        # ==== End check meta ====

        if crawled_files >= metadata.get("total_chapters_on_site"):
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


async def loop_every_1h_multi_sites():
    while True:
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n===== [START] Check missing for all sites at {now} =====")
        tasks = []
        for site_key in BASE_URLS:
            adapter = get_adapter(site_key)
            tasks.append(check_and_crawl_missing_all_stories(adapter))
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            print(f"[ERROR] Lỗi khi kiểm tra/crawl missing: {e}")
        print(f"===== [DONE] Sleeping 1 hour =====\n")
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(loop_every_1h_multi_sites())
