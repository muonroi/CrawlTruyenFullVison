from typing import Optional
from adapters.factory import get_adapter
from config.config import DATA_FOLDER, COMPLETED_FOLDER, PROXIES_FILE, PROXIES_FOLDER
from config.proxy_provider import load_proxies
from scraper import initialize_scraper
from utils.chapter_utils import count_txt_files, get_actual_chapters_for_export, slugify_title, crawl_missing_chapters_for_story, export_chapter_metadata_sync, extract_real_chapter_number, get_chapter_filename
from utils.io_utils import create_proxy_template_if_not_exists, move_story_to_completed, async_rename, async_remove
from utils.logger import logger
from utils.cleaner import ensure_sources_priority
from utils.domain_utils import get_site_key_from_url
from utils.state_utils import get_missing_worker_state_file, load_crawl_state
import os
import re
import sys
import json
import asyncio

# --- Auto-fix sources nếu thiếu ---
def autofix_sources(meta, meta_path=None):
    url = meta.get("url")
    site_key = meta.get("site_key") or (get_site_key_from_url(url) if url else None)
    if not meta.get("sources") and url and site_key:
        meta["sources"] = [{"url": url, "site_key": site_key}]
    # Bổ sung priority cho tất cả nguồn
    if "sources" in meta:
        meta["sources"] = ensure_sources_priority(meta["sources"])
    if meta_path:
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)
    logger.info(f"[AUTO-FIX] Đã bổ sung sources cho truyện: {meta.get('title')}")
    return meta

def autofix_metadata(folder, site_key=None):
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    folder_name = os.path.basename(folder)
    meta_path = os.path.join(folder, "metadata.json")
    meta = {
        "title": folder_name.replace("-", " ").replace("_", " ").title().strip(),
        "url": "", 
        "total_chapters_on_site": 0,
        "description": "",
        "author": "",
        "cover": "",
        "categories": [],
        "sources": [],
        "skip_crawl": False,
        "site_key": site_key
    }
    # Nếu có url đầu vào thì set vào meta luôn cho chuẩn
    if site_key and site_key in meta:
        meta['site_key'] = site_key
    if not os.path.exists(meta_path):
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)
        logger.info(f"[AUTO-FIX] Đã tạo metadata cho '{meta['title']}' (0 chương)")
    return meta


async def crawl_single_story_worker(story_url: Optional[str]=None, title: Optional[str]=None):
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)

    assert story_url or title, "Cần truyền url hoặc title!"
    # Xác định slug và folder
    slug = slugify_title(os.path.basename(story_url.rstrip("/"))) if story_url else slugify_title(title)#type: ignore
    folder = os.path.join(DATA_FOLDER, slug)
    meta_path = os.path.join(folder, "metadata.json")
    # Nếu chưa có folder → tạo mới
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    # Nếu chưa có metadata hoặc metadata lỗi thì tạo auto-fix
    meta = None
    if not os.path.exists(meta_path):
        site_key = get_site_key_from_url(story_url) if story_url else None
        await initialize_scraper(site_key)
        meta = autofix_metadata(folder, site_key)
        # Nếu có url truyền vào thì bổ sung luôn cho meta
        if story_url:
            meta['url'] = story_url
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=4)
    else:
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            fields = ["title", "url", "total_chapters_on_site", "categories", "site_key"]
            if not all(meta.get(f) for f in fields):
                logger.warning("[AUTO-FIX] metadata.json thiếu trường, sẽ autofix lại.")
                site_key = get_site_key_from_url(story_url) if story_url else meta.get("site_key")
                meta = autofix_metadata(folder, site_key)
                if story_url:
                    meta['url'] = story_url
                    with open(meta_path, "w", encoding="utf-8") as f:
                        json.dump(meta, f, ensure_ascii=False, indent=4)
        except Exception as ex:
            logger.warning(f"[AUTO-FIX] metadata.json lỗi/parsing fail ({ex}), sẽ autofix lại.")
            site_key = get_site_key_from_url(story_url) if story_url else None
            meta = autofix_metadata(folder, site_key)
            if story_url:
                meta['url'] = story_url
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=4)

    # --- Luôn reload metadata và autofix sources ---
    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)
    meta = autofix_sources(meta, meta_path)
    site_key = meta.get("site_key")
    adapter = get_adapter(site_key)

    # --- Always update lại metadata từ web ---
    logger.info(f"[SYNC] Đang cập nhật lại metadata từ web cho '{meta['title']}'...")
    details = await adapter.get_story_details(meta.get("url"), meta.get("title"))
    if details:
        for k, v in details.items():
            if v is not None and v != "" and meta.get(k) != v:
                meta[k] = v
        autofix_sources(meta, meta_path)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)
        logger.info(f"[SYNC] Metadata đã được cập nhật lại từ web!")

    chapters = None
    for source in meta["sources"]:
        url = source.get("url")
        src_site_key = source.get("site_key") or meta.get("site_key")
        adapter_src = get_adapter(src_site_key)
        try:
            chapters = await adapter_src.get_chapter_list(url, meta.get("title"), src_site_key)
            if chapters and len(chapters) > 0:
                break
        except Exception as ex:
            logger.warning(f"[SOURCE] Lỗi lấy chapter list từ {src_site_key}: {ex}")

    if not chapters or len(chapters) == 0:
        logger.error(f"[CRAWL] Không lấy được danh sách chương từ bất kỳ nguồn nào! meta.sources = {meta['sources']}")
        return
    else:
        logger.info(f"[CRAWL] Đã lấy được {len(chapters)} chương từ nguồn {src_site_key}")

    # Sắp xếp danh sách chương theo số thực sự để đảm bảo đúng thứ tự
    chapters.sort(key=lambda ch: extract_real_chapter_number(ch.get('title', '')) or 0)

    # Cập nhật lại tổng số chương trong metadata nếu cần
    real_total = len(chapters)
    if real_total != meta.get("total_chapters_on_site"):
        meta["total_chapters_on_site"] = real_total
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)

    # Lưu lại danh sách chapter vào chapter_metadata.json để lấy đúng title
    export_chapter_metadata_sync(folder, [
        {"index": extract_real_chapter_number(ch.get('title', '')) or (i+1),
         "title": ch.get('title', ''),
         "url": ch.get('url', '')}
        for i, ch in enumerate(chapters)
    ])


    # --- Check và rename lại file chương theo đúng thứ tự/tên title ---
    existing_files = [f for f in os.listdir(folder) if f.endswith('.txt')]
    for idx, ch in enumerate(chapters):
        real_num = extract_real_chapter_number(ch.get('title', '')) or (idx+1)
        expected_name = get_chapter_filename(ch.get("title", ""), real_num)
        prefix = f"{real_num:04d}_"
        found = None
        for fname in existing_files:
            if fname.startswith(prefix) and fname != expected_name:
                old_path = os.path.join(folder, fname)
                new_path = os.path.join(folder, expected_name)
                if os.path.exists(old_path) and not os.path.exists(new_path):
                    await async_rename(old_path, new_path)
                    logger.info(f"[RENAME] {fname} -> {expected_name}")
                break
    
    # Xóa các file chương có index = 0 (sau khi rename)
    for fname in os.listdir(folder):
        if fname.startswith("0000_") and fname.endswith(".txt"):
            await async_remove(os.path.join(folder, fname))
            logger.info(f"[REMOVE] Xoá file chương lỗi index 0: {fname}")

    # --- Load crawl state và check chương missing ---
    state_file = get_missing_worker_state_file(site_key)
    crawl_state = await load_crawl_state(state_file, site_key)

    async def crawl_missing_until_complete(site_key, chapters_from_web, meta, folder, crawl_state, state_file, max_retry=3):
        retry = 0
        def get_existing_real_chapter_numbers(folder):
            nums = set()
            for f in os.listdir(folder):
                m = re.match(r'(\d{4})_', f)
                if m:
                    nums.add(int(m.group(1)))
            return nums
        def get_missing_chapters(folder, chapters):
            existing_nums = get_existing_real_chapter_numbers(folder)
            missing = []
            for idx, ch in enumerate(chapters):
                real_num = extract_real_chapter_number(ch.get('title', '')) or (idx+1)
                fname = get_chapter_filename(ch.get("title", ""), real_num)
                path = os.path.join(folder, fname)
                if real_num not in existing_nums or not os.path.exists(path) or os.path.getsize(path) < 20:
                    missing.append({**ch, "real_num": real_num, "idx": idx})
            return missing

        while retry < max_retry:
            missing_chapters = get_missing_chapters(folder, chapters_from_web)
            if not missing_chapters:
                logger.info(f"[COMPLETE] Đã đủ tất cả chương cho '{meta['title']}'")
                return True
            logger.info(f"[RETRY] {len(missing_chapters)} chương còn thiếu, crawl lần {retry+1}/{max_retry}")
            num_batches = max(1, (len(missing_chapters)+119)//120)
            # Crawl theo batch (1 batch/lần để tối ưu)
            await crawl_missing_chapters_for_story(
                site_key,
                None,
                missing_chapters,
                meta,
                meta.get("categories", [{}])[0] if meta.get("categories") else {},
                folder,
                crawl_state,
                num_batches=num_batches,
                state_file=state_file,
                adapter=adapter,
            )
            # Kiểm tra lại
            missing_chapters = get_missing_chapters(folder, chapters_from_web)
            if not missing_chapters:
                logger.info(f"[COMPLETE] Đã đủ tất cả chương sau lần crawl {retry+1}")
                return True
            retry += 1
        logger.warning(f"[FAILED] Sau {max_retry} lần vẫn còn thiếu chương.")
        return False
 
    # --- Bắt đầu crawl bù ---
    await crawl_missing_until_complete(site_key, chapters, meta, folder, crawl_state, state_file, max_retry=3)

    # --- Recount lại sau crawl ---
    num_txt = count_txt_files(folder)
    real_total = len(chapters)
    logger.info(f"[CHECK] {meta.get('title')} - txt: {num_txt} / web: {real_total}")

    # --- Export lại chapter_metadata.json ---
    chapters_for_export = get_actual_chapters_for_export(folder)
    export_chapter_metadata_sync(folder, chapters_for_export)
    logger.info(f"[META] Đã cập nhật lại chapter_metadata.json ({len(chapters_for_export)} chương)")

    # --- Move sang completed nếu đủ ---
    if num_txt >= real_total and real_total > 0:
        genre = "Unknown"
        if meta.get('categories') and isinstance(meta['categories'], list) and meta['categories']:
            genre = meta['categories'][0].get('name')
        await move_story_to_completed(folder, genre)
    else:
        logger.warning(f"[WARNING] Truyện chưa đủ chương ({num_txt}/{real_total})")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="Link truyện muốn crawl/update")
    parser.add_argument("--title", help="Tên truyện (nếu đã từng crawl)")
    args = parser.parse_args()
    if args.url or args.title:
        asyncio.run(crawl_single_story_worker(story_url=args.url, title=args.title))
    else:
        print("Phải truyền vào --url hoặc --title!")
        sys.exit(1)  