import os
import json
import asyncio
from adapters.factory import get_adapter
from utils.logger import logger
from utils.cleaner import ensure_sources_priority
from utils.domain_utils import get_site_key_from_url
from utils.chapter_utils import slugify_title, get_chapter_filename
from config.config import DATA_FOLDER, COMPLETED_FOLDER

INVALID_LOG = "invalid_stories.log"

def check_missing_chapters(story_folder):
    chapter_meta_path = os.path.join(story_folder, "chapter_metadata.json")
    if not os.path.exists(chapter_meta_path):
        print(f"[SKIP][MISSING] Không tìm thấy chapter_metadata.json tại {story_folder}")
        return
    with open(chapter_meta_path, "r", encoding="utf-8") as f:
        chapters = json.load(f)
    missing = []
    for ch in chapters:
        file_path = os.path.join(story_folder, ch["file"])
        if not os.path.exists(file_path) or os.path.getsize(file_path) < 20:
            missing.append(ch)
    if missing:
        print(f"[WARNING] {len(missing)}/{len(chapters)} chương bị thiếu hoặc lỗi trong '{story_folder}':")
        for ch in missing[:10]:  # Hiện tối đa 10 chương thiếu
            print(f"    - #{ch['index']} {ch['title']}  ({ch['file']})")
        if len(missing) > 10:
            print(f"    ... và {len(missing) - 10} chương nữa.")
    else:
        print(f"[OK] Đã đủ chương cho '{story_folder}'.")
    # Nếu muốn trả về để dùng tiếp, có thể return missing
    return missing


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

def log_invalid_story(story_folder, meta, reason):
    with open(INVALID_LOG, "a", encoding="utf-8") as f:
        f.write(f"{story_folder}\t{meta.get('title')}\t{reason}\n")
    print(f"[SKIP][INVALID] {meta.get('title') or story_folder}: {reason}")

async def update_chapter_metadata_for_folder(story_folder):
    meta_path = os.path.join(story_folder, "metadata.json")
    if not os.path.exists(meta_path):
        print(f"[SKIP] Không có metadata tại {story_folder}")
        return
    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)
    meta = autofix_sources(meta, meta_path)
    url = meta.get("url")

    # === Tự động đoán lại url nếu thiếu ===
    if not url:
        slug = os.path.basename(story_folder)
        # Thử đoán với các base_url đã biết (tùy project bạn config)
        from config.config import BASE_URLS
        found = False
        for site_key, base_url in BASE_URLS.items():
            guess_url = f"{base_url.rstrip('/')}/{slug}"
            # Bạn có thể kiểm tra HTTP 200/Existence ở đây nếu muốn, hoặc gán luôn để fix lỗi
            meta["url"] = guess_url
            meta["site_key"] = site_key
            url = guess_url
            found = True
            print(f"[AUTO-FIX] Đã đoán lại url cho {story_folder}: {guess_url}")
            break
        if not found:
            log_invalid_story(story_folder, meta, "Thiếu url, không đoán được")
            return
        # Lưu lại metadata đã sửa
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)

    site_key = meta.get("site_key") or get_site_key_from_url(url)
    if not site_key:
        log_invalid_story(story_folder, meta, "Không xác định được site_key")
        return

    try:
        adapter = get_adapter(site_key)
    except Exception as ex:
        log_invalid_story(story_folder, meta, f"Unknown site_key: {site_key}")
        return

    # Lấy chapter list mới nhất từ web (ưu tiên sources đúng)
    chapters = None
    for source in meta.get("sources", []):
        src_url = source.get("url")
        src_site_key = source.get("site_key") or site_key
        try:
            adapter_src = get_adapter(src_site_key)
            chapters = await adapter_src.get_chapter_list(
               story_url=src_url,
                story_title=meta.get("title"),
                site_key=src_site_key,
                total_chapters=meta.get("total_chapters_on_site")
            )
            if chapters and len(chapters) > 0:
                break
        except Exception as e:
            continue
    if not chapters or len(chapters) == 0:
        log_invalid_story(story_folder, meta, "Không lấy được danh sách chương")
        return

    # Build metadata cho từng chương
    chapter_list = []
    for idx, ch in enumerate(chapters):
        real_num = idx + 1
        title = ch.get("title", "")
        url = ch.get("url", "")
        # Tên file txt chuẩn hóa đúng index + title
        filename = get_chapter_filename(title, real_num)
        chapter_list.append({
            "index": real_num,
            "title": title,
            "url": url,
            "file": filename
        })
    # Ghi ra file
    chapter_meta_path = os.path.join(story_folder, "chapter_metadata.json")
    with open(chapter_meta_path, "w", encoding="utf-8") as f:
        json.dump(chapter_list, f, ensure_ascii=False, indent=4)
    print(f"[DONE] {meta.get('title')} ({story_folder}): đã export chapter_metadata.json ({len(chapter_list)} chương)")
    # Check missing ngay sau khi export
    check_missing_chapters(story_folder)

async def scan_and_update_all(root_folder):
    count = 0
    for dirpath, dirnames, filenames in os.walk(root_folder):
        if "metadata.json" in filenames:
            await update_chapter_metadata_for_folder(dirpath)
            count += 1
    print(f"[FINISH] Đã cập nhật chapter metadata cho {count} truyện trong {root_folder}")

async def main():
    await scan_and_update_all(DATA_FOLDER)
    await scan_and_update_all(COMPLETED_FOLDER)

if __name__ == "__main__":
    asyncio.run(main())
