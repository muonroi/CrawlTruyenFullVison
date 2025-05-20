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
from utils.state_utils import get_missing_worker_state_file, load_crawl_state

MAX_CONCURRENT_STORIES = 3
STORY_SEM = asyncio.Semaphore(MAX_CONCURRENT_STORIES)


async def crawl_story_with_limit(
    site_key: str,
    session, 
    missing_chapters: list,
    metadata: dict,
    current_category: dict,
    story_folder: str,
    crawl_state: dict,
    num_batches: int = 10,
    state_file: str = None # type: ignore
):
    async with STORY_SEM:
        await crawl_missing_with_limit(
            site_key, session, missing_chapters, metadata,
            current_category, story_folder, crawl_state, num_batches,
            state_file=state_file
        )

def sync_metadata_total_chapters(story_folder):
    meta_path = os.path.join(story_folder, "metadata.json")
    if not os.path.exists(meta_path):
        return
    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)
    txt_files = [f for f in os.listdir(story_folder) if f.endswith('.txt')]
    if len(txt_files) > meta.get("total_chapters_on_site", 0):
        meta["total_chapters_on_site"] = len(txt_files)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)
        print(f"[SYNC] Đã cập nhật lại total_chapters_on_site cho '{os.path.basename(story_folder)}' thành {len(txt_files)}")


async def crawl_missing_with_limit(
    site_key: str,
    session,
    missing_chapters: list,
    metadata: dict,
    current_category: dict,
    story_folder: str,
    crawl_state: dict,
    num_batches: int = 10,
    state_file: str = None # type: ignore
):
    if not state_file:
        state_file = get_missing_worker_state_file(site_key)
    print(f"[START] Crawl missing for {metadata['title']} ...")
    async with SEM:
        result = await crawl_missing_chapters_for_story(
            site_key, session, missing_chapters, metadata,
            current_category, story_folder, crawl_state, num_batches,
            state_file=state_file
        )
    print(f"[DONE] Crawl missing for {metadata['title']} ...")
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

async def fix_metadata_with_retry(metadata, metadata_path, story_folder):
    """Retry tối đa 3 lần lấy lại metadata nếu thiếu total_chapters_on_site.
    Nếu fail, set skip_crawl và return False."""
    if metadata.get("skip_crawl", False):
        print(f"[SKIP] Truyện '{metadata.get('title')}' đã bị đánh dấu bỏ qua (skip_crawl), không crawl lại nữa.")
        return False
    retry_count = metadata.get("meta_retry_count", 0)
    total_chapters = metadata.get("total_chapters_on_site")
    while retry_count < 3 and (not total_chapters or total_chapters < 1):
        print(f"[SKIP] '{story_folder}' thiếu total_chapters_on_site -> [FIXED] Đang lấy lại metadata lần {retry_count+1} qua proxy...")
        details = await get_story_details(metadata.get("url"), metadata.get("title")) 
        retry_count += 1
        metadata["meta_retry_count"] = retry_count
        if details and details.get("total_chapters_on_site"):
            print(f"[FIXED] Cập nhật lại metadata, tổng chương: {details['total_chapters_on_site']}")
            metadata.update(details)
            metadata['total_chapters_on_site'] = details['total_chapters_on_site']
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            return True
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        total_chapters = metadata.get("total_chapters_on_site")
    if not total_chapters or total_chapters < 1:
        print(f"[SKIP] '{story_folder}' lấy meta 3 lần vẫn lỗi, sẽ không crawl lại truyện này nữa!")
        metadata["skip_crawl"] = True
        metadata["skip_reason"] = "meta_failed"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        return False
    return True

async def check_and_crawl_missing_all_stories(adapter, home_page_url, site_key):
    state_file = get_missing_worker_state_file(site_key)   # <--- dùng file phụ!
    crawl_state = await load_crawl_state(state_file)
    all_genres = await get_all_genres(home_page_url)
    genre_name_to_url = {g['name']: g['url'] for g in all_genres}
    genre_complete_checked = set()
    os.makedirs(COMPLETED_FOLDER, exist_ok=True)
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)
    await initialize_scraper(site_key)


    # Lấy danh sách tất cả story folder cần crawl
    story_folders = [
        os.path.join(DATA_FOLDER, cast(str, f))
        for f in os.listdir(DATA_FOLDER)
        if os.path.isdir(os.path.join(DATA_FOLDER, cast(str, f)))
    ]
    crawl_state = await load_crawl_state(state_file)
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
        # Skip nếu đã flag
        if metadata.get("skip_crawl", False):
            print(f"[SKIP] Truyện '{metadata.get('title')}' đã bị đánh dấu bỏ qua (skip_crawl), không crawl lại nữa.")
            continue
        # Auto fix metadata nếu thiếu (và skip nếu quá 3 lần)
        if not await fix_metadata_with_retry(metadata, metadata_path, story_folder):
            continue

        total_chapters = metadata.get("total_chapters_on_site")
        genre_name = None
        if metadata.get('categories') and isinstance(metadata['categories'], list) and metadata['categories']:
            genre_name = metadata['categories'][0].get('name')
        if not genre_name:
            genre_name = "Unknown"

        crawled_files = count_txt_files(story_folder)
        if crawled_files < total_chapters:
            print(f"[MISSING] '{metadata['title']}' thiếu chương ({crawled_files}/{total_chapters}) -> Đang kiểm tra/crawl bù từ mọi nguồn...")

            # --- Bổ sung crawl từ mọi nguồn ---
            for source in metadata.get("sources", []):
                url = source.get("url")
                if not site_key or not url:
                    continue
                adapter = get_adapter(site_key)
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
                tasks.append(
                    crawl_story_with_limit(
                        site_key, None, missing_chapters, metadata, current_category,
                        story_folder, crawl_state, num_batches=num_batches, state_file=state_file
                    )
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
        if metadata.get("skip_crawl", False):
            print(f"[SKIP] Truyện '{metadata.get('title')}' đã bị đánh dấu bỏ qua (skip_crawl), không crawl lại nữa.")
            continue
        # Lần nữa, fix meta nếu thiếu (tránh lỗi do có thể có truyện chưa retry)
        if not await fix_metadata_with_retry(metadata, metadata_path, story_folder):
            continue

        total_chapters = metadata.get("total_chapters_on_site")
        genre_name = None
        if metadata.get('categories') and isinstance(metadata['categories'], list) and metadata['categories']:
            genre_name = metadata['categories'][0].get('name')
        if not genre_name:
            genre_name = "Unknown"
        crawled_files = count_txt_files(story_folder)

        # ==== Kiểm tra fields bắt buộc ====
        fields_required = ['description', 'author', 'cover', 'categories', 'title', 'total_chapters_on_site']
        meta_ok = all(metadata.get(f) for f in fields_required)
        if not meta_ok:
            print(f"[SKIP] '{story_folder}' thiếu trường quan trọng, sẽ cố gắng lấy lại metadata...")
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
        sync_metadata_total_chapters(story_folder)
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

async def loop_once_multi_sites():
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n===== [START] Check missing for all sites at {now} =====")
    tasks = []
    for site_key, url in BASE_URLS.items():
        adapter = get_adapter(site_key)
        tasks.append(check_and_crawl_missing_all_stories(adapter, url, site_key=site_key))
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        print(f"[ERROR] Lỗi khi kiểm tra/crawl missing: {e}")
    print(f"===== [DONE] =====\n")


if __name__ == "__main__":
    asyncio.run(loop_once_multi_sites())
