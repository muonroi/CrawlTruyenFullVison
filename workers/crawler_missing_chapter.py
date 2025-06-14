import os
import asyncio
import json
import datetime
import re
import traceback
from typing import cast
from adapters.factory import get_adapter
from config.config import BASE_URLS, COMPLETED_FOLDER, DATA_FOLDER, LOADED_PROXIES, PROXIES_FILE, PROXIES_FOLDER
from config.proxy_provider import load_proxies
from scraper import initialize_scraper 
from utils.async_utils import sync_chapter_with_yy_first_batch
from utils.chapter_utils import SEM, count_txt_files, crawl_missing_chapters_for_story, export_chapter_metadata_sync, extract_real_chapter_number, get_actual_chapters_for_export, get_chapter_filename, get_real_total_chapters
from utils.domain_utils import  get_site_key_from_url, is_url_for_site, resolve_site_key
from utils.logger import logger
from utils.io_utils import create_proxy_template_if_not_exists, move_story_to_completed
from utils.notifier import send_discord_notify
from utils.state_utils import get_missing_worker_state_file, load_crawl_state
from filelock import FileLock
auto_fixed_titles = []
MAX_CONCURRENT_STORIES = 3
STORY_SEM = asyncio.Semaphore(MAX_CONCURRENT_STORIES)

def get_existing_real_chapter_numbers(story_folder):
    files = [f for f in os.listdir(story_folder) if f.endswith('.txt')]
    nums = set()
    for f in files:
        match = re.match(r'(\d{4})_', f)
        if match:
            nums.add(int(match.group(1)))
    return nums

def update_metadata_from_details(metadata: dict, details: dict) -> bool:
    if not details:
        return False
    changed = False
    for k, v in details.items():
        if v is not None and v != "" and metadata.get(k) != v:
            metadata[k] = v
            changed = True
    return changed


def check_and_fix_chapter_filename(story_folder: str, ch: dict, real_num: int, idx: int):
    """
    Nếu tên file hiện tại không khớp với tên dự kiến từ title → rename lại cho đúng.
    """
    # Danh sách file trong folder
    existing_files = [f for f in os.listdir(story_folder) if f.endswith(".txt")]

    # Tên dự kiến từ title chương
    expected_name = get_chapter_filename(ch.get("title", ""), real_num)
    expected_path = os.path.join(story_folder, expected_name)

    # Nếu file đích đã tồn tại đúng → OK
    if os.path.exists(expected_path):
        return

    # Tìm file sai tên theo prefix số chương
    prefix = f"{real_num:04d}_"
    for fname in existing_files:
        if fname.startswith(prefix):
            current_path = os.path.join(story_folder, fname)
            # Nếu khác tên → rename
            if current_path != expected_path:
                try:
                    os.rename(current_path, expected_path)
                    logger.info(f"[RENAME] Đã rename file '{fname}' → '{expected_name}'")
                except Exception as e:
                    logger.warning(f"[RENAME ERROR] Không thể rename '{fname}' → '{expected_name}': {e}")
            break


def get_missing_chapters(story_folder: str, chapters: list[dict]) -> list[dict]:
    """
    So sánh file txt hiện có với danh sách từ chapter_metadata.json (ưu tiên),
    nếu không có thì dùng chapters truyền vào (danh sách chapter từ web)
    """
    # Ưu tiên đọc từ chapter_metadata.json (nếu có)
    chapter_meta_path = os.path.join(story_folder, "chapter_metadata.json")
    chapter_items = None
    if os.path.exists(chapter_meta_path):
        with open(chapter_meta_path, "r", encoding="utf-8") as f:
            chapter_items = json.load(f)
    else:
        chapter_items = chapters

    existing_files = set([f for f in os.listdir(story_folder) if f.endswith('.txt')])

    missing = []
    for idx, ch in enumerate(chapter_items):
        # Dùng đúng tên file quy chuẩn từ chapter_metadata.json
        expected_file = ch.get("file")
        if not expected_file:
            # fallback nếu chapter_metadata chưa chuẩn hóa
            real_num = ch.get("index", idx+1)
            title = ch.get("title", "") or ""
            expected_file = get_chapter_filename(title, real_num)
        file_path = os.path.join(story_folder, expected_file)
        if expected_file not in existing_files or not os.path.exists(file_path) or os.path.getsize(file_path) < 20:
            # append đủ thông tin cho crawl lại
            ch_for_missing = {**ch, "idx": idx}
            missing.append(ch_for_missing)
    return missing



def get_current_category(metadata):
    categories = autofix_category(metadata)
    return categories[0]


async def loop_once_multi_sites(force_unskip=False):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"\n===== [START] Check missing for all sites at {now} =====")
    tasks = []
    for site_key, url in BASE_URLS.items():
        adapter = get_adapter(site_key)
        tasks.append(asyncio.create_task(check_and_crawl_missing_all_stories(adapter, url, site_key=site_key, force_unskip=force_unskip)))
    try:
        logger.info("Before await gather")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task {i} bị lỗi: {result}\n{traceback.format_exc()}")
        logger.info(f"===== [DONE] =====\n")
    except Exception as e:
        logger.error(f"[ERROR] Lỗi khi kiểm tra/crawl missing: {e}")
    logger.info(f"===== [DONE] =====\n")
    #await send_discord_notify(f"✅ DONE: Đã crawl/check missing xong toàn bộ ({now})")
async def crawl_missing_until_complete(
    site_key, session, chapters_from_web, metadata, current_category, story_folder, crawl_state, state_file, max_retry=3
):
    retry = 0
    while retry < max_retry:
        missing_chapters = get_missing_chapters(story_folder, chapters_from_web)
        for ch in chapters_from_web:
            title = ch.get('title', '') or ''
            real_num = extract_real_chapter_number(title) or (ch.get('idx', 0) + 1)
            check_and_fix_chapter_filename(story_folder, ch, real_num, ch.get('idx', 0))

        if not missing_chapters:
            logger.info(f"[COMPLETE] Đã đủ tất cả chương cho '{metadata['title']}'")
            chapters_for_export = get_actual_chapters_for_export(story_folder)
            export_chapter_metadata_sync(story_folder, chapters_for_export)
            return True
        logger.info(f"[RETRY] {len(missing_chapters)} chương còn thiếu, bắt đầu crawl lần {retry+1}/{max_retry}")
        # Tính số batch dựa trên số chương còn thiếu, mỗi batch tối đa 120 chương
        num_batches = max(1, (len(missing_chapters) + 119) // 120)  # Chia thành các batch 120 chương
        logger.info(f"Crawl {len(missing_chapters)} chương với {num_batches} batch (mỗi batch tối đa 120 chương)")
        await crawl_story_with_limit(
            site_key,
            session,
            missing_chapters,
            metadata,
            current_category,
            story_folder,
            crawl_state,
            num_batches=num_batches,
            state_file=state_file,
            adapter=adapter,
        )
        # Kiểm tra lại sau khi crawl
        missing_chapters = get_missing_chapters(story_folder, chapters_from_web)
        if not missing_chapters:
            logger.info(f"[COMPLETE] Đã đủ tất cả chương sau lần crawl {retry+1}")
            return True
        retry += 1
    logger.warning(f"[FAILED] Sau {max_retry} lần retry vẫn còn thiếu {len(missing_chapters)} chương cho '{metadata['title']}'")
    chapters_for_export = get_actual_chapters_for_export(story_folder)
    export_chapter_metadata_sync(story_folder, chapters_for_export)
    if retry >= max_retry:
        logger.warning(f"[FATAL] Sau {max_retry} lần vẫn còn thiếu chương. Đánh dấu dead_chapters và bỏ qua.")
        # Đánh dấu dead luôn cho các chương còn thiếu
        for ch in missing_chapters:
            await mark_dead_chapter(folder, {
                "index": ch.get("real_num"),
                "title": ch.get("title"),
                "url": ch.get("url"),
                "reason": "max_retry_reached"
            })
        return False
    return False
def autofix_category(metadata):
    """
    Đảm bảo metadata có 'categories' là list[dict] chuẩn.
    Nếu thiếu hoặc sai kiểu thì set lại Unknown.
    """
    categories = metadata.get('categories')
    if not (isinstance(categories, list) and categories and isinstance(categories[0], dict) and 'name' in categories[0]):
        logger.warning(f"[AUTO-FIX] Metadata '{metadata.get('title')}' thiếu hoặc sai categories. Set lại Unknown.")
        metadata['categories'] = [{"name": "Unknown", "url": ""}]
    return metadata['categories']


def normalize_source_list(metadata):
    source_list = []
    source_seen = set()
    for src in metadata.get("sources", []):
        if isinstance(src, dict):
            url = src.get("url")
            site_key = src.get("site_key") or src.get("site") or get_site_key_from_url(url) or metadata.get("site_key")
        elif isinstance(src, str):
            url = src
            site_key = get_site_key_from_url(url) or metadata.get("site_key")
        else:
            continue

        if not url or not site_key:
            continue
        if not is_url_for_site(url, site_key):
            continue  # ⚠️ Bỏ những cặp sai domain
        if (url, site_key) in source_seen:
            continue

        source_list.append({"url": url, "site_key": site_key})
        source_seen.add((url, site_key))

    # Ưu tiên thêm url chính nếu chưa có
    main_url = metadata.get("url") 
    main_key = metadata.get("site_key")
    if main_url and main_key and is_url_for_site(main_url, main_key):
        if (main_url, main_key) not in source_seen:
            source_list.append({"url": main_url, "site_key": main_key})
            source_seen.add((main_url, main_key))
    return source_list


async def check_and_crawl_missing_all_stories(adapter, home_page_url, site_key, force_unskip=False):
    state_file = get_missing_worker_state_file(site_key)
    crawl_state = await load_crawl_state(state_file, site_key)
    adapter = get_adapter(site_key) 
    all_genres = await adapter.get_stories_in_genre(home_page_url,1)
    genre_name_to_url = {g['name']: g['url'] for g in all_genres if isinstance(g, dict) and 'name' in g and 'url' in g}
    if not all_genres:
        logger.error(f"[{site_key}] Không lấy được danh sách thể loại (all_genres rỗng) từ {home_page_url}!")
        return

    genre_complete_checked = set()
    os.makedirs(COMPLETED_FOLDER, exist_ok=True)
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)
    await initialize_scraper(site_key)
    crawl_state = await load_crawl_state(state_file, site_key)

    # Lấy danh sách tất cả story folder cần crawl
    story_folders = [
        os.path.join(DATA_FOLDER, cast(str, f))
        for f in os.listdir(DATA_FOLDER)
        if os.path.isdir(os.path.join(DATA_FOLDER, cast(str, f)))
    ]
    tasks = []

    # ============ 1. Tạo tasks crawl missing ============
    for story_folder in story_folders:
        metadata_path = os.path.join(story_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            continue
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        has_yy = any(
            (src.get("site_key") == "truyenyy" or src.get("site") == "truyenyy")
            for src in metadata.get("sources", [])
            if isinstance(src, dict)
        )

        if has_yy:
            if site_key != "truyenyy":
                logger.debug(f"[SKIP] '{metadata.get('title')}' đã có truyenyy, chỉ để truyenyy crawl.")
                continue
        else:
            await sync_chapter_with_yy_first_batch(story_folder, metadata)
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            has_yy_after = any(
                (src.get("site_key") == "truyenyy" or src.get("site") == "truyenyy")
                for src in metadata.get("sources", [])
                if isinstance(src, dict)
            )
            if has_yy_after:
                if site_key != "truyenyy":
                    logger.debug(f"[SKIP] '{metadata.get('title')}' đã sync được truyenyy, chỉ để truyenyy crawl.")
                    continue
            else:
                if site_key != metadata.get("site_key"):
                    logger.debug(f"[SKIP] '{metadata.get('title')}' không có yy, chỉ crawl site gốc {metadata.get('site_key')}.")
                    continue
        need_autofix = False
        metadata = None
        if auto_fixed_titles:
            msg = "[AUTO-FIX] Đã tự động tạo metadata cho các truyện: " + ", ".join(auto_fixed_titles[:10])
            if len(auto_fixed_titles) > 10:
                msg += f" ... (và {len(auto_fixed_titles)-10} truyện nữa)"
            #await send_discord_notify(msg)
            auto_fixed_titles.clear()
        if os.path.dirname(story_folder) == os.path.abspath(COMPLETED_FOLDER):
            continue
        metadata_path = os.path.join(story_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            guessed_url = f"{BASE_URLS.get(site_key, '').rstrip('/')}/{os.path.basename(story_folder)}"
            logger.info(f"[AUTO-FIX] Không có metadata.json, đang lấy metadata chi tiết từ {guessed_url}")
            details = await adapter.get_story_details(guessed_url, os.path.basename(story_folder).replace("-", " "))
            logger.info("... sau await get_story_details ...")
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
            # --- Clean sources sai domain ở đây ---
            fixed_sources = []
            for src in metadata.get("sources", []):
                s_url = src.get("url") if isinstance(src, dict) else src
                s_key = get_site_key_from_url(s_url) or (src.get("site_key") if isinstance(src, dict) else None) or (src.get("site") if isinstance(src, dict) else None) or metadata.get("site_key")
                if s_url and s_key and is_url_for_site(s_url, s_key):
                    fixed_sources.append(src)
                else:
                    logger.warning(f"[FIX] Source có url {s_url} không đúng domain với key {s_key}, đã loại khỏi sources.")
            metadata["sources"] = fixed_sources
            # --------------------------------------

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
            new_details = await adapter.get_story_details(metadata.get('url'), metadata.get('title')) #type:ignore
            if update_metadata_from_details(metadata, new_details): #type:ignore
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)
                logger.info(f"[RECHECK] Metadata đã được cập nhật lại từ web!")
            logger.info(f"[MISSING] '{metadata['title']}' thiếu chương ({crawled_files}/{total_chapters}) -> Đang kiểm tra/crawl bù từ mọi nguồn...") #type:ignore

            # 1. Duyệt qua tất cả sources nếu có
            source_list = normalize_source_list(metadata)

            for idx, source in enumerate(source_list):
                logger.info(f"[CRAWL SOURCE {idx+1}/{len(source_list)}] site_key={source['site_key']}, url={source['url']}")

            for source in source_list:
                url = source.get("url")
                src_site_key = source.get("site_key") or metadata.get("site_key")#type:ignore
                if not src_site_key or not url:
                    continue
                adapter = get_adapter(src_site_key)
                try:
                    chapters = await adapter.get_chapter_list(url, metadata['title'], src_site_key)#type:ignore
                    current_category = get_current_category(metadata)  # <- Dùng hàm helper
                    crawl_done = await crawl_missing_until_complete(
                        src_site_key, None, chapters, metadata, current_category,
                        story_folder, crawl_state, state_file
                    )
                    if crawl_done:
                        # Check + move completed ở đây (nếu muốn move ngay, khỏi phải sweep lại sau)
                        real_total = len(chapters)
                        chapter_count = recount_chapters(story_folder)
                        if chapter_count >= real_total and real_total > 0:
                            # move to completed_folder
                            genre_name = current_category['name'] if current_category else 'Unknown'
                            await move_story_to_completed(story_folder, genre_name)
                        break
                except Exception as ex:
                    logger.error(f"  [ERROR] Không lấy được chapter list từ {src_site_key}: {ex}")
                    continue
                missing_chapters = get_missing_chapters(story_folder, chapters)
                if not missing_chapters:
                    logger.info(f"  Không còn chương nào thiếu ở nguồn {src_site_key}.")
                    logger.info(f"[NEXT STORY] Done process for {os.path.basename(story_folder)} (KHÔNG missing chapter)")
                    continue

                logger.info(f"  Bắt đầu crawl bổ sung {len(missing_chapters)} chương từ nguồn {src_site_key}")
                num_batches = get_auto_batch_count(fixed=10)
                logger.info(f"Auto chọn {num_batches} batch cho truyện {metadata['title']} (site: {src_site_key}, proxy usable: {len(LOADED_PROXIES)})") #type:ignore
                if not os.path.exists(story_folder):
                    logger.warning(f"[SKIP][TASK] Không tồn tại folder, bỏ qua: {story_folder}")
                    continue
                tasks.append(asyncio.create_task(
                    crawl_story_with_limit(
                        src_site_key,
                        None,
                        missing_chapters,
                        metadata,
                        current_category,  # type:ignore
                        story_folder,
                        crawl_state,
                        num_batches=num_batches,
                        state_file=state_file,
                        adapter=adapter,
                    )
                ))
        logger.info(f"[NEXT] Kết thúc process cho story: {story_folder}")


    # ============ 2. Chờ crawl bù xong ============
    if tasks:
        await asyncio.gather(*tasks)
    if 'metadata' in locals() and metadata and metadata.get("title"):
        logger.info(f"[NEXT STORY] Done process for {metadata['title']}")


    # ============ 3. Quét lại & move, cảnh báo, đồng bộ ============
    notified_titles = set()
    for story_folder in story_folders:
        # Skip nếu truyện đã move sang completed
        genre_folders = [os.path.join(COMPLETED_FOLDER, d) for d in os.listdir(COMPLETED_FOLDER) if os.path.isdir(os.path.join(COMPLETED_FOLDER, d))]
        if any(os.path.join(gf, os.path.basename(story_folder)) == story_folder for gf in genre_folders):
            continue

        meta_path = os.path.join(story_folder, "metadata.json")
        with FileLock(meta_path + ".lock", timeout=10):
            if not os.path.exists(meta_path):
                metadata = autofix_metadata(story_folder, site_key)
                auto_fixed_titles.append(metadata["title"])
            else:
                with open(meta_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                # Sửa lại sources nếu cần
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
                        with open(meta_path, "w", encoding="utf-8") as f:
                            json.dump(metadata, f, ensure_ascii=False, indent=4)

        real_total = await get_real_total_chapters(metadata, adapter)
        chapter_count = recount_chapters(story_folder)

        # Luôn cập nhật lại metadata cho đúng số chương thực tế từ web
        if metadata.get("total_chapters_on_site") != real_total:
            metadata["total_chapters_on_site"] = real_total
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            logger.info(f"[RECOUNT] Cập nhật lại metadata: {real_total} chương cho '{os.path.basename(story_folder)}'")

        logger.info(f"[CHECK] {metadata.get('title')} - txt: {chapter_count} / web: {real_total}")

        # Move nếu đủ chương thực tế trên web
        if chapter_count >= real_total and real_total > 0:
            genre_name = "Unknown"
            if metadata.get('categories') and isinstance(metadata['categories'], list) and metadata['categories']:
                genre_name = metadata['categories'][0].get('name')
            await move_story_to_completed(story_folder, genre_name)
            if genre_name not in genre_complete_checked:
                genre_url = genre_name_to_url.get(genre_name)
                if genre_url:
                    await check_genre_complete_and_notify(genre_name, genre_url, site_key)
                genre_complete_checked.add(genre_name)
        else:
            # Cảnh báo thiếu chương (chỉ 1 lần/truyện)
            title = metadata.get('title')
            if title and title not in notified_titles:
                warning_msg = f"[WARNING] Sau crawl bù, truyện '{title}' vẫn thiếu chương: {chapter_count}/{real_total}"
                logger.warning(warning_msg)
                await send_discord_notify(warning_msg)
                notified_titles.add(title)

        # Fix metadata nếu thiếu trường quan trọng (chỉ gọi 1 lần)
        fields_required = ['description', 'author', 'cover', 'categories', 'title', 'total_chapters_on_site']
        meta_ok = all(metadata.get(f) for f in fields_required)
        if not meta_ok:
            logger.info(f"[SKIP] '{story_folder}' thiếu trường quan trọng, sẽ cố gắng lấy lại metadata...")
            details = await adapter.get_story_details(metadata.get("url"), metadata.get("title"))
            if update_metadata_from_details(metadata, details):#type:ignore
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

    logger.info(f"[TASK END] Task {site_key} đã xong toàn bộ story.")

def recount_chapters(story_folder):
    """Trả về số file .txt thực tế trong folder truyện."""
    return len([f for f in os.listdir(story_folder) if f.endswith('.txt')])



async def check_genre_complete_and_notify(genre_name, genre_url, site_key):
    adapter = get_adapter(site_key)
    stories_on_web = await  adapter.get_all_stories_from_genre(genre_name, genre_url)
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
        await send_discord_notify(f"🎉 Đã crawl xong **TẤT CẢ** truyện của thể loại [{genre_name}] trên web!")

async def fix_metadata_with_retry(metadata, metadata_path, story_folder, site_key=None, adapter=None):
    from scraper import make_request

    def is_url_for_site(url, site_key):
        from config.config import BASE_URLS
        base = BASE_URLS.get(site_key)
        return base and url and url.startswith(base)

    if metadata.get("skip_crawl", False):
        logger.info(f"[SKIP] Truyện '{metadata.get('title')}' đã bị đánh dấu bỏ qua.")
        return False

    retry_count = metadata.get("meta_retry_count", 0)
    url = metadata.get("url")
    title = metadata.get("title")
    total_chapters = metadata.get("total_chapters_on_site")

    # === Ưu tiên gán lại url từ sources nếu thiếu ===
    if not url and metadata.get("sources"):
        for src in metadata["sources"]:
            s_url = src.get("url") if isinstance(src, dict) else src
            if s_url:
                url = s_url
                metadata["url"] = url
                logger.info(f"[FIX] Gán lại url từ sources cho '{story_folder}': {url}")
                break

    # === Ưu tiên lấy title từ sources nếu thiếu ===
    if not title and metadata.get("sources"):
        for src in metadata["sources"]:
            s_title = src.get("title") if isinstance(src, dict) else None
            if s_title:
                title = s_title
                metadata["title"] = title
                logger.info(f"[FIX] Gán lại title từ sources: {title}")
                break

    # === Gán lại title nếu vẫn thiếu bằng tên thư mục ===
    if not title:
        folder_title = os.path.basename(story_folder).replace("-", " ").title()
        metadata["title"] = title = folder_title
        logger.info(f"[FALLBACK] Gán title từ folder: {title}")

    # === Nếu vẫn thiếu url, đoán từ base_url + slug ===
    if not url and site_key:
        from config.config import BASE_URLS
        base_url = BASE_URLS.get(site_key, "").rstrip("/")
        slug = os.path.basename(story_folder)
        guessed_url = f"{base_url}/{slug}"
        try:
            resp = await make_request(guessed_url, site_key)
            if resp and getattr(resp, "status_code", None) == 200:
                url = guessed_url
                metadata["url"] = url
                logger.info(f"[GUESS] Đoán url thành công cho '{story_folder}': {url}")
        except Exception as e:
            logger.warning(f"[GUESS FAIL] Lỗi khi request guessed url {guessed_url}: {e}")

    # === Nếu vẫn không có url/title thì skip ===
    if not url or not title:
        metadata["skip_crawl"] = True
        metadata["skip_reason"] = "missing_url_title"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        logger.warning(f"[SKIP] '{story_folder}' thiếu url/title → bỏ qua.")
        return False

    # === Lấy lại metadata nếu thiếu total_chapters ===
    retry_count = metadata.get("meta_retry_count", 0)
    while retry_count < 3 and (not total_chapters or total_chapters < 1):
        logger.info(f"[META] Đang lấy metadata lần {retry_count+1} từ web...")
        adapter = get_adapter(site_key) #type:ignore
        details = await adapter.get_story_details(url, title)
        update_metadata_from_details(metadata, details) #type:ignore
        retry_count += 1
        metadata["meta_retry_count"] = retry_count

        if isinstance(details, dict) and details.get("total_chapters_on_site"):
            metadata.update(details)
            metadata["total_chapters_on_site"] = details["total_chapters_on_site"]
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            logger.info(f"[META] Đã cập nhật thành công metadata cho '{title}'")
            return True
        else:
            logger.warning(f"[META FAIL] Không lấy được metadata hợp lệ từ {url}")
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)

    # === Nếu thất bại sau 3 lần: xoá folder ===
    if not metadata.get("total_chapters_on_site", 0):
        try:
            shutil.rmtree(story_folder)
            logger.info(f"[REMOVE] Xoá '{story_folder}' do không lấy được metadata.")
        except Exception as e:
            logger.error(f"[ERROR] Không thể xoá folder '{story_folder}': {e}")
        return False

    return True



def autofix_metadata(story_folder, site_key=None):
    if not os.path.exists(story_folder):
        logger.warning(f"[AUTO-FIX] Không tồn tại folder để autofix: {story_folder}")
        return {}
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
    # Chỉ tạo file nếu chưa tồn tại
    if not os.path.exists(meta_path):
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)
        logger.info(f"[AUTO-FIX] Đã tạo metadata cho '{meta['title']}' ({chapter_count} chương)")
    return meta


def split_to_batches(items, num_batches):
    k, m = divmod(len(items), num_batches)
    return [items[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(num_batches)]

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
    state_file: str = None,  # type: ignore
    adapter=None,
):
    await STORY_SEM.acquire()
    try:
        batches = split_to_batches(missing_chapters, num_batches)
        for batch_idx, batch in enumerate(batches):
            if not batch:
                continue
            logger.info(f"[Batch {batch_idx+1}/{len(batches)}] Crawl {len(batch)} chương")
            await crawl_missing_with_limit(
                site_key,
                session,
                batch,
                metadata,
                current_category,
                story_folder,
                crawl_state,
                1,
                state_file=state_file,
                adapter=adapter,
            )
        # =====================================================
    finally:
        STORY_SEM.release()
    logger.info(f"[DONE-CRAWL-STORY-WITH-LIMIT] {metadata.get('title')}")

async def crawl_missing_with_limit(
    site_key: str,
    session,
    missing_chapters: list,
    metadata: dict,
    current_category: dict,
    story_folder: str,
    crawl_state: dict,
    num_batches: int = 10,
    state_file: str = None,  # type: ignore
    adapter=None,
):
    if not state_file:
        state_file = get_missing_worker_state_file(site_key)
    logger.info(f"[START] Crawl missing for {metadata['title']} ...")
    async with SEM:
        result = await asyncio.wait_for(
            crawl_missing_chapters_for_story(
                site_key,
                session,
                missing_chapters,
                metadata,
                current_category,
                story_folder,
                crawl_state,
                num_batches,
                state_file=state_file,
                adapter=adapter,
            ),
            timeout=60,
        )
    logger.info(f"[DONE] Crawl missing for {metadata['title']} ...")
    return result
  