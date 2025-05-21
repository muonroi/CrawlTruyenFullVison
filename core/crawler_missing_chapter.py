import os
import asyncio
import json
import datetime
import shutil
from typing import cast
from adapters.factory import get_adapter
from config.config import BASE_URLS, COMPLETED_FOLDER, DATA_FOLDER, LOADED_PROXIES, PROXIES_FILE, PROXIES_FOLDER
from config.proxy_provider import load_proxies
from main import crawl_missing_chapters_for_story
from scraper import initialize_scraper
from utils.chapter_utils import count_txt_files
from utils.logger import logger
from utils.async_utils import SEM
from utils.io_utils import create_proxy_template_if_not_exists
from analyze.truyenfull_vision_parse import get_all_genres, get_all_stories_from_genre, get_story_details
from utils.notifier import send_telegram_notify
from utils.state_utils import get_missing_worker_state_file, load_crawl_state
from filelock import FileLock


auto_fixed_titles = []

MAX_CONCURRENT_STORIES = 3
STORY_SEM = asyncio.Semaphore(MAX_CONCURRENT_STORIES)

def update_metadata_from_details(metadata: dict, details: dict) -> bool:
    changed = False
    for k, v in details.items():
        if v is not None and v != "" and metadata.get(k) != v:
            metadata[k] = v
            changed = True
    return changed


async def loop_once_multi_sites(force_unskip=False):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"\n===== [START] Check missing for all sites at {now} =====")
    tasks = []
    for site_key, url in BASE_URLS.items():
        adapter = get_adapter(site_key)
        tasks.append(check_and_crawl_missing_all_stories(adapter, url, site_key=site_key, force_unskip=force_unskip))
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"[ERROR] Lỗi khi kiểm tra/crawl missing: {e}")
    logger.info(f"===== [DONE] =====\n")
    # Sau khi crawl xong:
    await send_telegram_notify(f"✅ DONE: Đã crawl/check missing xong toàn bộ ({now})")

async def check_and_crawl_missing_all_stories(adapter, home_page_url, site_key, force_unskip=False):
    state_file = get_missing_worker_state_file(site_key)
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

    # ============ 1. Tạo tasks crawl missing ============
    for story_folder in story_folders:
        need_autofix = False
        metadata = None
        if auto_fixed_titles:
            msg = "[AUTO-FIX] Đã tự động tạo metadata cho các truyện: " + ", ".join(auto_fixed_titles[:10])
            if len(auto_fixed_titles) > 10:
                msg += f" ... (và {len(auto_fixed_titles)-10} truyện nữa)"
            #await send_telegram_notify(msg)
            auto_fixed_titles.clear()
        if os.path.dirname(story_folder) == os.path.abspath(COMPLETED_FOLDER):
            continue
        metadata_path = os.path.join(story_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            guessed_url = f"{BASE_URLS.get(site_key, '').rstrip('/')}/{os.path.basename(story_folder)}"
            logger.info(f"[AUTO-FIX] Không có metadata.json, đang lấy metadata chi tiết từ {guessed_url}")
            details = await get_story_details(guessed_url, os.path.basename(story_folder).replace("-", " "))
            metadata = autofix_metadata(story_folder, site_key)
            if details:
                # Merge tất cả các trường (kể cả trường mới hoặc chỉ có trong details)
                for k, v in details.items():
                    if v is not None and v != "" and metadata.get(k) != v:
                        logger.info(f"[UPDATE] {metadata['title']}: Trường '{k}' được cập nhật.")
                        metadata[k] = v
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)
                logger.info(f"[AUTO-FIX] Đã tạo metadata đầy đủ/merge cho '{metadata.get('title')}' ({metadata.get('total_chapters_on_site', 0)} chương)")
                # Log trường thiếu cho dev dễ debug adapter
                fields_required = ['title', 'categories', 'total_chapters_on_site', 'author', 'description', 'cover', 'sources']
                missing = [f for f in fields_required if not metadata.get(f)]
                if missing:
                    logger.warning(f"[AUTO-FIX] Metadata của '{metadata.get('title')}' vẫn còn thiếu các trường: {missing}")
            else:
                logger.info(f"[AUTO-FIX] Tạo metadata tạm cho '{metadata['title']}' ({metadata.get('total_chapters_on_site', 0)} chương)")
            auto_fixed_titles.append(metadata["title"])


        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            # Validate cấu trúc sources và các trường bắt buộc
            if not isinstance(metadata.get("sources", []), list):
                need_autofix = True
            fields_required = ['title', 'categories', 'total_chapters_on_site']
            if not all(metadata.get(f) for f in fields_required):
                need_autofix = True
        except Exception as ex:
            logger.warning(f"[AUTO-FIX] metadata.json lỗi/parsing fail tại {story_folder}, sẽ xoá file và tạo lại! {ex}")
            need_autofix = True

        if need_autofix:
            try:
                os.remove(metadata_path)
            except Exception as ex:
                logger.error(f"Lỗi xóa metadata lỗi: {ex}")
            metadata = autofix_metadata(story_folder, site_key)
            auto_fixed_titles.append(metadata["title"])

        # Nếu force_unskip: Xoá skip_crawl & meta_retry_count nếu có
        if force_unskip:
            changed = False
            if metadata.get("skip_crawl"): #type:ignore
                metadata.pop("skip_crawl", None) #type:ignore
                changed = True
            if "meta_retry_count" in metadata: #type:ignore
                metadata.pop("meta_retry_count", None) #type:ignore
                changed = True
            if changed:
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)
                logger.info(f"[UNSKIP] Tự động unskip: {metadata.get('title')}") #type:ignore

        if metadata.get("skip_crawl", False): #type:ignore
            logger.info(f"[SKIP] Truyện '{metadata.get('title')}' đã bị đánh dấu bỏ qua (skip_crawl), không crawl lại nữa.") #type:ignore
            continue

        # Auto fix metadata nếu thiếu (và skip nếu quá 3 lần)
        if not await fix_metadata_with_retry(metadata, metadata_path, story_folder, site_key=site_key, adapter=adapter):
            continue

        total_chapters = metadata.get("total_chapters_on_site") #type:ignore
        crawled_files = count_txt_files(story_folder)
        if crawled_files < total_chapters: #type:ignore
                # Trước khi crawl missing, luôn update lại metadata từ web!
            logger.info(f"[RECHECK] Đang cập nhật lại metadata từ web cho '{metadata['title']}' trước khi crawl missing...") #type:ignore
            new_details = await get_story_details(metadata.get('url'), metadata.get('title')) #type:ignore
            if update_metadata_from_details(metadata, new_details):
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)
                logger.info(f"[RECHECK] Metadata đã được cập nhật lại từ web!")
            logger.info(f"[MISSING] '{metadata['title']}' thiếu chương ({crawled_files}/{total_chapters}) -> Đang kiểm tra/crawl bù từ mọi nguồn...") #type:ignore
            for source in metadata.get("sources", []): #type:ignore
                url = source.get("url")
                if not site_key or not url:
                    continue
                adapter = get_adapter(site_key)
                try:
                    chapters = await adapter.get_chapter_list(url, metadata['title']) #type:ignore
                except Exception as ex:
                    logger.error(f"  [ERROR] Không lấy được chapter list từ {site_key}: {ex}")
                    continue
                existing_files = set(os.listdir(story_folder))
                missing_chapters = []
                for idx, ch in enumerate(chapters):
                    if isinstance(ch, dict):
                        title = ch.get('title', 'untitled')
                    elif isinstance(ch, str):
                        title = 'untitled'
                        logger.warning(f"[WARNING] Chương nhận về là str, không phải dict! Dữ liệu: {ch[:100]}")
                        ch = {'title': ch, 'url': ch}
                    else:
                        title = 'untitled'
                    fname_only = f"{idx+1:04d}_{title}.txt"
                    file_path = os.path.join(story_folder, fname_only)
                    if fname_only in existing_files and os.path.getsize(file_path) > 10:
                        logger.debug(f"File '{fname_only}' đã tồn tại, bỏ qua.")
                        continue
                    ch['idx'] = idx #type:ignore
                    missing_chapters.append(ch)
                if not missing_chapters:
                    logger.info(f"  Không còn chương nào thiếu ở nguồn {site_key}.")
                    continue

                logger.info(f"  Bắt đầu crawl bổ sung {len(missing_chapters)} chương từ nguồn {site_key}")
                current_category = metadata['categories'][0] if metadata.get('categories') and isinstance(metadata['categories'], list) and metadata['categories'] else {} #type:ignore
                num_batches = get_auto_batch_count(fixed=10)
                logger.info(f"Auto chọn {num_batches} batch cho truyện {metadata['title']} (site: {site_key}, proxy usable: {len(LOADED_PROXIES)})") #type:ignore
                tasks.append(
                    crawl_story_with_limit(
                        site_key, None, missing_chapters, metadata, current_category, #type:ignore
                        story_folder, crawl_state, num_batches=num_batches, state_file=state_file
                    )
                )

    # ============ 2. Chờ crawl bù xong ============
    if tasks:
        await asyncio.gather(*tasks)

    # ============ 3. Quét lại & move, cảnh báo, đồng bộ ============
    notified_titles = set()
    for story_folder in story_folders:
        need_autofix = False
        metadata = None
        # Skip nếu truyện đã move sang completed
        genre_folders = [os.path.join(COMPLETED_FOLDER, d) for d in os.listdir(COMPLETED_FOLDER) if os.path.isdir(os.path.join(COMPLETED_FOLDER, d))]
        if any(os.path.join(gf, os.path.basename(story_folder)) == story_folder for gf in genre_folders):
            continue

        meta_path = os.path.join(story_folder, "metadata.json")
        with FileLock(meta_path + ".lock", timeout=10):
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta_path, f, ensure_ascii=False, indent=4)
        if not os.path.exists(meta_path):
            metadata = autofix_metadata(story_folder, site_key)
            auto_fixed_titles.append(metadata["title"])
        else:
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                # ==== AUTO-FIX SOURCES (NÊN BỎ ĐÂY) ====
                if "sources" in metadata and isinstance(metadata["sources"], list):
                    fixed_sources = []
                    for src in metadata["sources"]:
                        if isinstance(src, dict):
                            fixed_sources.append(src)
                        elif isinstance(src, str):
                            fixed_sources.append({"url": src})
                    if len(fixed_sources) != len(metadata["sources"]):
                        logger.warning(f"[FIX] Đã phát hiện và sửa nguồn 'sources' bị sai type ở {story_folder}")
                        metadata["sources"] = fixed_sources
                        with open(metadata_path, "w", encoding="utf-8") as f:
                            json.dump(metadata, f, ensure_ascii=False, indent=4)
                # ==== END AUTO-FIX SOURCES ====
            except Exception as ex:
                metadata = autofix_metadata(story_folder, site_key)
                auto_fixed_titles.append(metadata["title"])

        chapter_count = recount_chapters(story_folder)

        # Update metadata nếu số chương tăng lên
        if metadata.get("total_chapters_on_site", 0) < chapter_count:
            metadata["total_chapters_on_site"] = chapter_count
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            logger.info(f"[RECOUNT] Cập nhật lại metadata: {chapter_count} chương cho '{os.path.basename(story_folder)}'")

        # Cảnh báo thiếu chương (chỉ 1 lần/truyện)
        if chapter_count < metadata.get("total_chapters_on_site", 0):
            title = metadata.get('title')
            if title and title not in notified_titles:
                warning_msg = f"[WARNING] Sau crawl bù, truyện '{title}' vẫn thiếu chương: {chapter_count}/{metadata.get('total_chapters_on_site', 0)}"
                logger.warning(warning_msg)
                await send_telegram_notify(warning_msg)
                notified_titles.add(title)

        if metadata.get("skip_crawl", False):
            logger.info(f"[SKIP] Truyện '{metadata.get('title')}' đã bị đánh dấu bỏ qua (skip_crawl), không crawl lại nữa.")
            continue

        # Fix metadata nếu thiếu trường quan trọng (chỉ gọi 1 lần)
        fields_required = ['description', 'author', 'cover', 'categories', 'title', 'total_chapters_on_site']
        meta_ok = all(metadata.get(f) for f in fields_required)
        if not meta_ok:
            logger.info(f"[SKIP] '{story_folder}' thiếu trường quan trọng, sẽ cố gắng lấy lại metadata...")
            details = await get_story_details(metadata.get("url"), metadata.get("title"))
            if update_metadata_from_details(metadata, details):
                meta_ok = all(metadata.get(f) for f in fields_required)
                if meta_ok:
                    logger.info(f"[FIXED] Đã bổ sung metadata đủ cho '{metadata.get('title')}'")
                    with open(meta_path, "w", encoding="utf-8") as f:
                        json.dump(metadata, f, ensure_ascii=False, indent=4)
                else:
                    logger.error(f"[ERROR] Không lấy đủ metadata cho '{metadata.get('title')}'! Sẽ bỏ qua move.")
                    continue
            else:
                logger.error(f"[ERROR] Không lấy đủ metadata cho '{metadata.get('title')}'! Sẽ bỏ qua move.")
                continue


        # Move nếu đủ chương, và chỉ move nếu folder chưa nằm ở completed
        if chapter_count >= metadata.get("total_chapters_on_site", 0):
            genre_name = "Unknown"
            if metadata.get('categories') and isinstance(metadata['categories'], list) and metadata['categories']:
                genre_name = metadata['categories'][0].get('name')
            dest_genre_folder = os.path.join(COMPLETED_FOLDER, genre_name)
            os.makedirs(dest_genre_folder, exist_ok=True)
            dest_folder = os.path.join(dest_genre_folder, os.path.basename(story_folder))
            if not os.path.exists(dest_folder):
                shutil.move(story_folder, dest_folder)
                logger.info(f"[INFO] Đã chuyển truyện '{metadata['title']}' sang {dest_genre_folder}")
            if genre_name not in genre_complete_checked:
                genre_url = genre_name_to_url.get(genre_name)
                if genre_url:
                    await check_genre_complete_and_notify(genre_name, genre_url)
                genre_complete_checked.add(genre_name)



def recount_chapters(story_folder):
    """Trả về số file .txt thực tế trong folder truyện."""
    return len([f for f in os.listdir(story_folder) if f.endswith('.txt')])



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
        

async def fix_metadata_with_retry(metadata, metadata_path, story_folder, site_key=None, adapter=None):
    """
    Retry tối đa 3 lần lấy lại metadata nếu thiếu total_chapters_on_site hoặc thiếu url/title.
    Nếu fail, set skip_crawl và return False.
    """
    if metadata.get("skip_crawl", False):
        logger.info(f"[SKIP] Truyện '{metadata.get('title')}' đã bị đánh dấu bỏ qua (skip_crawl), không crawl lại nữa.")
        return False

    retry_count = metadata.get("meta_retry_count", 0)
    total_chapters = metadata.get("total_chapters_on_site")
    url = metadata.get("url")
    title = metadata.get("title")

    # Bổ sung: lấy lại url/title từ sources theo site_key
    for _ in range(3):
        if url and title:
            break
        # Ưu tiên lấy url từ sources đúng site_key
        if not url and metadata.get("sources"):
            url_found = None
            if site_key:
                for src in metadata["sources"]:
                    if src.get("site") == site_key and src.get("url"):
                        url_found = src["url"]
                        break
            if not url_found:
                for src in metadata["sources"]:
                    if src.get("url"):
                        url_found = src["url"]
                        break
            if url_found:
                url = url_found
                logger.info(f"[FIX] Bổ sung lại url cho '{story_folder}' theo site_key '{site_key}': {url}")
            metadata["url"] = url
        # Lấy lại title từ sources (nếu có title hợp lệ), ưu tiên đúng site_key
        if not title and metadata.get("sources"):
            title_found = None
            if site_key:
                for src in metadata["sources"]:
                    if src.get("site") == site_key and src.get("title"):
                        title_found = src["title"]
                        break
            if not title_found:
                for src in metadata["sources"]:
                    if src.get("title"):
                        title_found = src["title"]
                        break
            if title_found:
                title = title_found
                logger.info(f"[FIX] Bổ sung lại title cho '{story_folder}' từ sources: {title}")
                metadata["title"] = title
        # Nếu vẫn chưa có, lấy lại title từ folder
        if not title:
            folder_title = os.path.basename(story_folder).replace("-", " ").title()
            logger.info(f"[FIX] Bổ sung lại title cho '{story_folder}' từ folder: {folder_title}")
            title = folder_title
            metadata["title"] = title
        retry_count += 1

    # === BỔ SUNG: fallback đoán url theo slug folder + tìm qua category ===
    if not url:
        # 1. Đoán url dựa trên slug folder và BASE_URL
        from config.config import BASE_URLS
        if site_key and site_key in BASE_URLS:
            base_url = BASE_URLS[site_key].rstrip("/")
            slug = os.path.basename(story_folder)
            guessed_url = f"{base_url}/{slug}"
            # Test thử request vào guessed_url
            try:
                from scraper import make_request
                resp = await asyncio.get_event_loop().run_in_executor(None, make_request, guessed_url)
                if resp and getattr(resp, "status_code", None) == 200:
                    url = guessed_url
                    metadata["url"] = url
                    logger.info(f"[GUESS] Đã đoán lại url cho '{story_folder}': {url}")
            except Exception as e:
                logger.error(f"[GUESS-FAIL] Lỗi khi thử guessed url: {e}")

        # 2. Tìm lại url trong danh sách truyện của category
        if not url and metadata.get("categories") and adapter is not None:
            try:
                for cat in metadata["categories"]:
                    stories = await adapter.get_all_stories_from_genre(cat["name"], cat["url"])
                    for story in stories:
                        if story.get("title", "").strip().lower() == (metadata.get("title") or "").strip().lower():
                            url = story["url"]
                            metadata["url"] = url
                            logger.info(f"[FIND] Đã tìm lại url từ category '{cat['name']}': {url}")
                            break
                    if url:
                        break
            except Exception as e:
                logger.error(f"[FIND-FAIL] Lỗi khi tìm url trong category: {e}")

    # === END BỔ SUNG ===

    if not url or not title:
        logger.info(f"[SKIP] '{story_folder}' thiếu url/title (đã thử 3 lần + fallback), không thể lấy lại metadata!")
        metadata["skip_crawl"] = True
        metadata["skip_reason"] = "missing_url_title"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        return False

    # Nếu đã có đủ url/title, tiếp tục retry lấy metadata như bình thường
    retry_count = metadata.get("meta_retry_count", 0)
    while retry_count < 3 and (not total_chapters or total_chapters < 1):
        logger.info(f"[SKIP] '{story_folder}' thiếu total_chapters_on_site -> [FIXED] Đang lấy lại metadata lần {retry_count+1} qua proxy...")
        # Lưu ý: phải truyền đúng adapter cho hàm này (bạn cần truyền adapter vào khi gọi fix_metadata_with_retry)
        details = await get_story_details(url, title)
        retry_count += 1
        metadata["meta_retry_count"] = retry_count
        if details and details.get("total_chapters_on_site"):
            logger.info(f"[FIXED] Cập nhật lại metadata, tổng chương: {details['total_chapters_on_site']}")
            metadata.update(details)
            metadata['total_chapters_on_site'] = details['total_chapters_on_site']
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            return True
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        total_chapters = metadata.get("total_chapters_on_site")
    if not total_chapters or total_chapters < 1:
        logger.info(f"[SKIP] '{story_folder}' lấy meta 3 lần vẫn lỗi, sẽ không crawl lại truyện này nữa!")
        metadata["skip_crawl"] = True
        metadata["skip_reason"] = "meta_failed"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        return False
    return True



def autofix_metadata(story_folder, site_key=None):
    folder_name = os.path.basename(story_folder)
    chapter_count = recount_chapters(story_folder)
    guessed_url = f"{BASE_URLS.get(site_key, '').rstrip('/')}/{folder_name}" if site_key else None
    parent_folder = os.path.basename(os.path.dirname(story_folder))
    genre_guess = parent_folder if parent_folder and parent_folder != os.path.basename(DATA_FOLDER) else None
    meta = {
        "title": folder_name.replace("-", " ").replace("_", " ").title().strip(),
        "url": guessed_url,
        "total_chapters_on_site": chapter_count,
        "description": "",
        "author": "",
        "cover": "",
        "categories": [{"name": genre_guess}] if genre_guess else [],
        "sources": [],
        "skip_crawl": False,
        "site_key": site_key
    }
    meta_path = os.path.join(story_folder, "metadata.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=4)
    logger.info(f"[AUTO-FIX] Đã tạo metadata cho '{meta['title']}' ({chapter_count} chương)")
    return meta


def get_auto_batch_count(fixed=None, default=10, min_batch=1, max_batch=20, num_items=None):
    if fixed is not None:
        return fixed
    batch = default
    if num_items:
        batch = min(batch, num_items)
    return min(batch, max_batch)



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
    logger.info(f"[START] Crawl missing for {metadata['title']} ...")
    async with SEM:
        result = await crawl_missing_chapters_for_story(
            site_key, session, missing_chapters, metadata,
            current_category, story_folder, crawl_state, num_batches,
            state_file=state_file
        )
    logger.info(f"[DONE] Crawl missing for {metadata['title']} ...")
    return result

