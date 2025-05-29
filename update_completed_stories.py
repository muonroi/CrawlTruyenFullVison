import os
import shutil
import json
import asyncio

from filelock import FileLock
from adapters.factory import get_adapter
from config.config import BASE_URLS, DATA_FOLDER, COMPLETED_FOLDER
from utils.chapter_utils import sanitize_filename, count_txt_files

# Move truyện từ completed về data để tiếp tục crawl
def move_story_back_to_data_folder(story_slug):
    src = os.path.join(COMPLETED_FOLDER, story_slug)
    dest = os.path.join(DATA_FOLDER, story_slug)
    src_lock = FileLock(src + ".lock", timeout=60)
    dest_lock = FileLock(dest + ".lock", timeout=60)
    with src_lock, dest_lock:
        if os.path.exists(src):
            if os.path.exists(dest):
                shutil.rmtree(dest)
            shutil.move(src, dest)

async def check_and_update_all_completed_stories(all_sites):
    for site_key in all_sites:
        for slug in os.listdir(COMPLETED_FOLDER):
            move_story_back_to_data_folder(slug)
            folder = os.path.join(DATA_FOLDER, slug)
            meta_file = os.path.join(folder, 'metadata.json')
            if not os.path.exists(meta_file):
                print(f"Thiếu metadata ở {folder}, bỏ qua.")
                continue
            with open(meta_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            title = metadata.get("title")
            if not title:
                print(f"Thiếu title ở {folder}, bỏ qua.")
                continue
            for source in metadata.get("sources", []):
                url = source.get("url")
                if not site_key or not url:
                    continue
                adapter = get_adapter(site_key)
                print(f"[{slug}] Check chương mới từ nguồn {site_key} ...")
                total_chapters = metadata.get("total_chapters_on_site") or 0
                chapters = await adapter.get_chapter_list(url, title, total_chapters=total_chapters)
                total_existing = count_txt_files(folder)
                if len(chapters) > total_existing:
                    print(f"  Có chương mới ({len(chapters)}/{total_existing}) từ {site_key}. Đang bổ sung ...")
                    added = 0
                    existing_files = set(os.listdir(folder))
                    for idx, ch in enumerate(chapters):
                        fname_only = f"{idx+1:04d}_{sanitize_filename(ch['title']) or 'untitled'}.txt"
                        if fname_only not in existing_files:
                            content = await adapter.get_chapter_content(ch['url'], ch['title'])
                            if content:
                                with open(os.path.join(folder, fname_only), "w", encoding="utf-8") as fch:
                                    fch.write(content)
                                added += 1
                    print(f"  Đã bổ sung {added} chương mới cho truyện '{title}' từ nguồn {site_key}")
            print(f"[{slug}] Đã kiểm tra xong.")

if __name__ == "__main__":
    for site_key, url in BASE_URLS.items():
        asyncio.run(check_and_update_all_completed_stories([site_key]))
