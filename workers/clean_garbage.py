import os
import json
import time
from datetime import datetime, timedelta
import shutil
from utils.chapter_utils import count_dead_chapters

DATA_FOLDER = "truyen_data"
COMPLETED_FOLDER = "completed_stories"
BACKUP_FOLDER = "backup_truyen_data"
RECYCLE_BIN = "recycle_bin"

DEBUG_FILES = ["debug_empty_chapter.html"]
LOG_FILE = "crawler.log"
LOG_ROTATE_PREFIX = "crawler.log."
ERROR_JSONS = ["error_chapters.json", "chapter_retry_queue.json", "missing_chapters.json"]

# Tạo thư mục recycle_bin nếu chưa có
os.makedirs(RECYCLE_BIN, exist_ok=True)

def move_to_recycle(path):
    basename = os.path.basename(path)
    new_path = os.path.join(RECYCLE_BIN, basename)
    if os.path.exists(new_path):
        os.remove(new_path)
    shutil.move(path, new_path)
    print(f"Đã chuyển {path} sang {RECYCLE_BIN}")

def clean_debug_files():
    for file in DEBUG_FILES:
        if os.path.exists(file):
            move_to_recycle(file)

def clean_stale_locks(folder='truyen_data'):
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith('.lock'):
                fpath = os.path.join(root, file)
                if (time.time() - os.path.getmtime(fpath)) > 600:
                    os.remove(fpath)
                    print(f"Deleted stale lock: {fpath}")

def clean_lock_files(max_age_hours=24):
    count = 0
    now = time.time()
    for root, dirs, files in os.walk('.'):
        for file in files:
            if file.endswith('.lock'):
                lock_path = os.path.join(root, file)
                # Nếu file quá cũ (theo thời gian chỉnh sửa) thì dọn
                age_hours = (now - os.path.getmtime(lock_path)) / 3600
                if age_hours > max_age_hours:
                    move_to_recycle(lock_path)
                    count += 1
    if count > 0:
        print(f"Đã dọn {count} file .lock cũ hơn {max_age_hours} giờ.")
    else:
        print("Không có file .lock cũ cần dọn.")

def clean_logs():
    for f in os.listdir('.'):
        if f.startswith(LOG_FILE) and f != LOG_FILE:
            move_to_recycle(f)

def clean_empty_json_or_txt():
    for folder in [DATA_FOLDER, COMPLETED_FOLDER]:
        for root, dirs, files in os.walk(folder):
            for file in files:
                if file.endswith('.json') or file.endswith('.txt'):
                    path = os.path.join(root, file)
                    if os.path.getsize(path) == 0:
                        move_to_recycle(path)

def clean_error_jsons(max_age_days=7):
    now = time.time()
    for file in ERROR_JSONS:
        if os.path.exists(file):
            with open(file, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except:
                    data = []
            new_data = []
            for item in data:
                dt = item.get("error_time")
                if not dt:
                    # Nếu không có trường time, bỏ qua (hoặc thêm vào)
                    continue
                try:
                    t = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
                    if (datetime.now() - t).days < max_age_days:
                        new_data.append(item)
                except:
                    new_data.append(item)
            if len(new_data) < len(data):
                with open(file, "w", encoding="utf-8") as f:
                    json.dump(new_data, f, ensure_ascii=False, indent=4)
                print(f"Đã dọn bớt {file}: {len(data)-len(new_data)} mục cũ")
            # Nếu trống hoàn toàn thì move luôn file đó
            if not new_data:
                move_to_recycle(file)

def clean_abandoned_stories(min_percent=10, days=30):
    """Dọn truyện crawl dở (crawled < 10% số chương), meta cũ hơn X ngày"""
    now = datetime.now()
    for folder in os.listdir(DATA_FOLDER):
        story_path = os.path.join(DATA_FOLDER, folder)
        if not os.path.isdir(story_path): continue
        meta_file = os.path.join(story_path, "metadata.json")
        if not os.path.exists(meta_file): continue
        try:
            with open(meta_file, "r", encoding="utf-8") as f:
                meta = json.load(f)
            total = meta.get("total_chapters_on_site", 0) or 0
            txt_count = len([f for f in os.listdir(story_path) if f.endswith('.txt')])
            dead_count = count_dead_chapters(story_path)
            crawled = txt_count + dead_count
            meta_time = meta.get("metadata_updated_at")
            if not meta_time: continue
            updated_at = datetime.strptime(meta_time, "%Y-%m-%d %H:%M:%S")
            percent = (crawled / total * 100) if total else 0
            if percent < min_percent and (now - updated_at).days > days:
                move_to_recycle(story_path)
        except Exception as ex:
            print(f"Lỗi đọc meta {meta_file}: {ex}")

def main():
    print("=== BẮT ĐẦU QUÁ TRÌNH DỌN RÁC ===")
    clean_debug_files()
    clean_logs()
    clean_lock_files()
    clean_empty_json_or_txt()
    clean_error_jsons()
    clean_abandoned_stories()
    clean_stale_locks()
    print("=== DỌN RÁC XONG ===")

if __name__ == "__main__":
    main()
