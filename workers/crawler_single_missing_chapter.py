from datetime import datetime
from typing import Optional
from adapters.factory import get_adapter
from config.config import (
    DATA_FOLDER,
    PROXIES_FILE,
    PROXIES_FOLDER,
    COMPLETED_FOLDER,
)
from config.proxy_provider import load_proxies
from scraper import initialize_scraper
from utils.chapter_utils import (
    count_txt_files,
    count_dead_chapters,
    get_actual_chapters_for_export,
    slugify_title,
    crawl_missing_chapters_for_story,
    export_chapter_metadata_sync,
    extract_real_chapter_number,
    get_chapter_filename,
    get_missing_chapters, # Thêm import này
)
from utils.io_utils import create_proxy_template_if_not_exists, move_story_to_completed, async_rename, async_remove
from utils.logger import logger
from utils.cleaner import ensure_sources_priority
from utils.domain_utils import get_site_key_from_url
from utils.state_utils import get_missing_worker_state_file, load_crawl_state
from utils.cache_utils import cached_get_story_details, cached_get_chapter_list
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


async def crawl_single_story_worker(story_url: Optional[str]=None, title: Optional[str]=None, story_folder_path: Optional[str]=None):
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)

    if story_folder_path:
        folder = story_folder_path
    elif story_url or title:
        slug = slugify_title(os.path.basename(story_url.rstrip("/"))) if story_url else slugify_title(title) #type: ignore
        folder = os.path.join(DATA_FOLDER, slug)
    else:
        raise ValueError("Cần truyền vào story_folder_path, story_url hoặc title!")

    logger.info(f"[SingleStoryWorker] Bắt đầu xử lý: {folder}")
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
    site_key = meta.get("site_key") or (get_site_key_from_url(meta.get("url")) if meta.get("url") else None)
    adapter = get_adapter(site_key)

    # --- Always update lại metadata từ web ---
    logger.info(f"[SYNC] Đang cập nhật lại metadata từ web cho '{meta['title']}'...")
    details = await cached_get_story_details(adapter, meta.get("url"), meta.get("title"))
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
            chapters = await cached_get_chapter_list(
                adapter_src,
                url,
                meta.get("title"),
                src_site_key,
            )
            if chapters and len(chapters) > 0:
                meta["total_chapters_on_site"] = len(chapters)
                for source in meta.get("sources", []):
                    if source.get("url") == url:
                        source["total_chapters"] = len(chapters)
                        source["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=4)
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


    # --- Load crawl state và check chương missing ---
    state_file = get_missing_worker_state_file(site_key)
    crawl_state = await load_crawl_state(state_file, site_key)

    # Sử dụng hàm get_missing_chapters từ utils
    missing_chapters = get_missing_chapters(folder, chapters)

    if missing_chapters:
        logger.info(f"[RETRY] Tìm thấy {len(missing_chapters)} chương còn thiếu, bắt đầu crawl...")
        # Gọi thẳng hàm crawl từ utils, nó đã có sẵn logic retry bên trong
        await crawl_missing_chapters_for_story(
            site_key,
            None, # session không cần thiết khi dùng adapter
            missing_chapters,
            meta,
            meta.get("categories", [{}])[0] if meta.get("categories") else {},
            folder,
            crawl_state,
            num_batches=max(1, (len(missing_chapters) + 119) // 120),
            state_file=state_file,
            adapter=adapter,
        )
    else:
        logger.info(f"[COMPLETE] Không có chương nào thiếu cho '{meta['title']}'.")

    # --- Recount lại sau crawl ---
    num_txt = count_txt_files(folder)
    real_total = len(chapters)
    logger.info(f"[CHECK] {meta.get('title')} - txt: {num_txt} / web: {real_total}")

    dead_count = count_dead_chapters(folder)
    # --- Move sang completed nếu đủ ---
    if num_txt + dead_count >= real_total and real_total > 0:
        genre = "Unknown"
        if meta.get('categories') and isinstance(meta['categories'], list) and meta['categories']:
            genre = meta['categories'][0].get('name')
        await move_story_to_completed(folder, genre)
    else:
        logger.warning(
            f"[WARNING] Truyện chưa đủ chương ({num_txt}+{dead_count}/{real_total})"
        )

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
