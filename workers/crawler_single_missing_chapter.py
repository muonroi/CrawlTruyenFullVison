import os
import sys
import json
import asyncio
import shutil
from typing import Optional
from utils.domain_utils import get_site_key_from_url
from adapters.factory import get_adapter
from config.config import DATA_FOLDER, COMPLETED_FOLDER
from utils.chapter_utils import slugify_title, count_txt_files, extract_real_chapter_number, get_chapter_filename
from utils.logger import logger
from utils.state_utils import get_missing_worker_state_file, load_crawl_state
from utils.meta_utils import sanitize_filename
from filelock import FileLock

# --- Auto-fix sources nếu thiếu ---
def autofix_sources(meta, meta_path=None):
    url = meta.get("url")
    site_key = meta.get("site_key") or (get_site_key_from_url(url) if url else None)
    if not meta.get("sources") and url and site_key:
        meta["sources"] = [{"url": url, "site_key": site_key}]
        if meta_path:
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=4)
        logger.info(f"[AUTO-FIX] Đã bổ sung sources cho truyện: {meta.get('title')}")
    return meta

async def crawl_single_story_worker(story_url: Optional[str]=None, title: Optional[str]=None):
    assert story_url or title, "Cần truyền url hoặc title!"
    # Xác định slug và folder
    slug = slugify_title(os.path.basename(story_url.rstrip("/"))) if story_url else slugify_title(title)
    folder = os.path.join(DATA_FOLDER, slug)
    meta_path = os.path.join(folder, "metadata.json")
    # Nếu chưa có folder → tạo mới
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    # Nếu chưa có metadata, crawl mới
    if not os.path.exists(meta_path):
        if not story_url:
            raise Exception("Không tìm thấy truyện. Nếu truyền theo tên thì phải đã từng crawl!")
        site_key = get_site_key_from_url(story_url)
        adapter = get_adapter(site_key)
        details = await adapter.get_story_details(story_url, slug.replace("-", " "))
        details["site_key"] = site_key
        autofix_sources(details)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(details, f, ensure_ascii=False, indent=4)

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

    # --- Lấy lại chapter list mới nhất từ đúng sources ---
    from utils.domain_utils import get_adapter_from_url
    chapters = None
    for source in meta["sources"]:
        url = source.get("url")
        src_site_key = source.get("site_key") or meta.get("site_key")
        adapter_src = get_adapter(src_site_key)
        chapters = await adapter_src.get_chapter_list(url, meta.get("title"), src_site_key)
        if chapters and len(chapters) > 0:
            break
    if not chapters or len(chapters) == 0:
        raise Exception("Không lấy được danh sách chương từ bất kỳ nguồn nào!")

    # --- Check và rename lại file chương theo đúng thứ tự/tên title ---
    existing_files = [f for f in os.listdir(folder) if f.endswith('.txt')]
    for idx, ch in enumerate(chapters):
        real_num = extract_real_chapter_number(ch.get('title', '')) or (idx+1)
        expected_name = get_chapter_filename(ch.get("title", ""), real_num)
        prefix = f"{real_num:04d}_"
        for fname in existing_files:
            if fname.startswith(prefix) and fname != expected_name:
                os.rename(os.path.join(folder, fname), os.path.join(folder, expected_name))
                logger.info(f"[RENAME] {fname} -> {expected_name}")

    # --- Load crawl state và check chương missing ---
    state_file = get_missing_worker_state_file(site_key)
    crawl_state = await load_crawl_state(state_file)

    # --- Crawl bù missing theo batch/retry ---
    from main import crawl_missing_chapters_for_story
    from utils.async_utils import SEM

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
                site_key, None, missing_chapters, meta, meta.get("categories", [{}])[0] if meta.get("categories") else {},
                folder, crawl_state, num_batches=num_batches, state_file=state_file
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

    # --- Move sang completed nếu đủ ---
    if num_txt >= real_total and real_total > 0:
        genre = "Unknown"
        if meta.get('categories') and isinstance(meta['categories'], list) and meta['categories']:
            genre = meta['categories'][0].get('name')
        dest_genre_folder = os.path.join(COMPLETED_FOLDER, genre)
        os.makedirs(dest_genre_folder, exist_ok=True)
        dest_folder = os.path.join(dest_genre_folder, slug)
        if not os.path.exists(dest_folder):
            shutil.move(folder, dest_folder)
            logger.info(f"[INFO] Đã move truyện '{meta['title']}' sang {dest_genre_folder}")
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
