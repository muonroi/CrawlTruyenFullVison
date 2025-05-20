import os
import json
import shutil
from utils.notifier import send_telegram_notify  # Đảm bảo import đúng như trong project bạn

COMPLETED_FOLDER = "completed_stories"
DATA_FOLDER = "truyen_data"
REQUIRED_META_FIELDS = [
    "title", "author", "description", "categories", "total_chapters_on_site", "cover"
]

def check_story_completed(story_folder):
    meta_path = os.path.join(story_folder, "metadata.json")
    if not os.path.exists(meta_path):
        return False, "MISSING_METADATA_JSON"
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception as e:
        return False, f"METADATA_READ_ERROR: {e}"

    for field in REQUIRED_META_FIELDS:
        if not meta.get(field):
            return False, f"MISSING_FIELD_{field.upper()}"
    total_chapters = meta.get("total_chapters_on_site", 0)
    if not isinstance(total_chapters, int):
        try:
            total_chapters = int(total_chapters)
        except Exception:
            return False, "TOTAL_CHAPTERS_INVALID"
    txt_files = [f for f in os.listdir(story_folder) if f.endswith('.txt')]
    if len(txt_files) != total_chapters:
        return False, f"CHAPTER_COUNT_MISMATCH (meta: {total_chapters}, txt: {len(txt_files)})"

    return True, "OK"

def move_to_data_folder(story_folder, genre_name):
    story_name = os.path.basename(story_folder)
    dest_folder = os.path.join(DATA_FOLDER, story_name)
    if os.path.exists(dest_folder):
        shutil.rmtree(dest_folder)
    shutil.move(story_folder, dest_folder)
    print(f"[MOVE] Đã chuyển truyện '{story_name}' từ completed_stories/{genre_name} về {DATA_FOLDER}/{story_name}")

async def send_telegram_report(error_stories, ok_count, total):
    if not error_stories:
        await send_telegram_notify("✅ [Check Completed Stories] Tất cả truyện đã OK! Không phát hiện lỗi thiếu meta hoặc chương nào.")
        return
    msg_lines = [
        f"⚠️ [Check Completed Stories] Có {len(error_stories)}/{total} truyện lỗi. {ok_count} truyện OK.",
        "",
        "Danh sách truyện lỗi:"
    ]
    max_show = 25
    for idx, item in enumerate(error_stories[:max_show]):
        msg_lines.append(f"{idx+1}. [{item['genre']}] {item['story']} — {item['message']}")
    if len(error_stories) > max_show:
        msg_lines.append(f"... Và {len(error_stories) - max_show} truyện lỗi khác (xem report_check_completed.json)")
    await send_telegram_notify('\n'.join(msg_lines))

def main():
    import asyncio

    report = []
    error_stories = []
    ok_count = 0
    genre_folders = [os.path.join(COMPLETED_FOLDER, d) for d in os.listdir(COMPLETED_FOLDER) if os.path.isdir(os.path.join(COMPLETED_FOLDER, d))]
    for genre_folder in genre_folders:
        story_folders = [os.path.join(genre_folder, s) for s in os.listdir(genre_folder) if os.path.isdir(os.path.join(genre_folder, s))]
        for story_folder in story_folders:
            ok, msg = check_story_completed(story_folder)
            story_name = os.path.basename(story_folder)
            genre_name = os.path.basename(genre_folder)
            item = {
                "genre": genre_name,
                "story": story_name,
                "path": story_folder,
                "status": "OK" if ok else "ERROR",
                "message": msg
            }
            if not ok:
                move_to_data_folder(story_folder, genre_name)
                error_stories.append(item)
            else:
                ok_count += 1
            print(f"[{item['status']}] {genre_name}/{story_name}: {msg}")
            report.append(item)

    # Xuất report ra file
    with open("report_check_completed.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=4)

    # Gửi báo cáo qua Telegram (async)
    asyncio.run(send_telegram_report(error_stories, ok_count, len(report)))

if __name__ == "__main__":
    main()
