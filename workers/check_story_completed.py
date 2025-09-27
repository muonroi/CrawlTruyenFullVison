import os
import json
import shutil

def check_story_can_move(story_folder):
    meta_path = os.path.join(story_folder, "metadata.json")
    reasons = []

    # 1. Kiểm tra metadata.json
    if not os.path.exists(meta_path):
        reasons.append("Thiếu metadata.json")
        return False, reasons

    # 2. Load metadata
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except Exception as ex:
        reasons.append(f"Lỗi đọc metadata: {ex}")
        return False, reasons

    # Sau khi load metadata, giờ mới kiểm tra có phải dict không
    if not isinstance(metadata, dict):  # Kiểm tra metadata có phải là dict không
        reasons.append("metadata.json không đúng định dạng dict/object")
        return False, reasons

    # 3. Kiểm tra các trường bắt buộc
    required_fields = ['title', 'categories', 'total_chapters_on_site', 'author', 'description', 'cover']
    for field in required_fields:
        if not metadata.get(field):
            reasons.append(f"Thiếu trường {field}")

    # 4. Kiểm tra skip_crawl
    if metadata.get("skip_crawl", False):
        reasons.append("Đánh dấu skip_crawl")

    # 5. Kiểm tra số chương thực tế
    try:
        txt_count = len([f for f in os.listdir(story_folder) if f.endswith('.txt')])
        dead_count = count_dead_chapters(story_folder)
        chapter_count = txt_count + dead_count
    except Exception as ex:
        reasons.append(f"Lỗi khi đếm file chương: {ex}")
        return False, reasons

    total_chapters = metadata.get("total_chapters_on_site", 0)
    if chapter_count < total_chapters:
        reasons.append(f"Thiếu chương: {chapter_count}/{total_chapters}")

    if reasons:
        return False, reasons
    else:
        return True, ["Đủ điều kiện move"]


def scan_flat_folder(data_folder):
    """Scan truyện ở folder 1 cấp (truyen_data)"""
    print(f"\n--- Kiểm tra truyện trong {data_folder} ---")
    story_folders = [
        os.path.join(data_folder, name)
        for name in os.listdir(data_folder)
        if os.path.isdir(os.path.join(data_folder, name))
    ]
    results = []
    for folder in story_folders:
        ok, reasons = check_story_can_move(folder)
        name = os.path.basename(folder)
        results.append((name, ok, reasons))
    return results

def scan_nested_folder(parent_folder):
    print(f"\n--- Kiểm tra truyện trong {parent_folder} (theo thể loại) ---")
    all_results = []
    genres = os.listdir(parent_folder)
    print("Các thể loại tìm thấy:", genres)
    for genre in genres:
        genre_path = os.path.join(parent_folder, genre)
        if os.path.isdir(genre_path):
            print(f"  Thể loại: {genre}")
            for story in os.listdir(genre_path):
                story_folder = os.path.join(genre_path, story)
                if os.path.isdir(story_folder):
                    print(f"    Truyện: {story}")
                    ok, reasons = check_story_can_move(story_folder)
                    name = f"{genre}/{story}"
                    all_results.append((name, ok, reasons))
    return all_results


def print_report(results, folder_label):
    total = len(results)
    # Sắp xếp OK trước, NO sau
    results_sorted = sorted(results, key=lambda x: not x[1])  # OK = True -> 0, NO = False -> 1

    print(f"\n--- TỔNG KẾT ({folder_label}) ---")
    print(f"Tổng số truyện: {total}")

    print("\n== ĐỦ ĐIỀU KIỆN MOVE ==")
    for name, ok, reasons in results_sorted:
        if ok:
            print(f"[OK]  {name}: Có thể move qua completed")

    print("\n== CHƯA ĐỦ ĐIỀU KIỆN ==")
    for name, ok, reasons in results_sorted:
        if not ok:
            print(f"[NO]  {name}: {'; '.join(reasons)}")

    can_move = sum(1 for _, ok, _ in results if ok)
    cant_move = sum(1 for _, ok, _ in results if not ok)
    print(f"\nSố truyện đủ điều kiện: {can_move}")
    print(f"Số truyện chưa đủ: {cant_move}")


def move_no_stories_to_data_folder(results, completed_folder, data_folder):
    count = 0
    for name, ok, reasons in results:
        if not ok:
            # name dạng "theloai/truyen"
            try:
                genre, story = name.split('/', 1)
            except ValueError:
                print(f"[LỖI] Không tách được thể loại/truyện từ tên: {name}")
                continue
            src_folder = os.path.join(completed_folder, genre, story)
            dest_folder = os.path.join(data_folder, story)
            if not os.path.isdir(src_folder):
                print(f"[SKIP] Không tìm thấy thư mục: {src_folder}")
                continue
            if os.path.exists(dest_folder):
                print(f"[SKIP] {dest_folder} đã tồn tại, không move!")
                continue
            try:
                shutil.move(src_folder, dest_folder)
                print(f"[MOVE] {name} → truyen_data/{story}: {'; '.join(reasons)}")
                count += 1
            except Exception as ex:
                print(f"[LỖI MOVE] {name}: {ex}")
    print(f"\n== ĐÃ MOVE {count} TRUYỆN CHƯA ĐỦ VỀ truyen_data ==")

if __name__ == "__main__":
    # Folder 1 cấp
    truyen_data_results = scan_flat_folder("./truyen_data")
    print_report(truyen_data_results, "truyen_data")
    can_move_truyen_data = sum(1 for _, ok, _ in truyen_data_results if ok)
    cant_move_truyen_data = sum(1 for _, ok, _ in truyen_data_results if not ok)
    print(f"\n== Tổng kết truyen_data ==")
    print(f"Số truyện đủ điều kiện: {can_move_truyen_data}")
    print(f"Số truyện chưa đủ: {cant_move_truyen_data}")

    # # Folder nhiều cấp (theo thể loại)
    # completed_results = scan_nested_folder("./completed_stories")
    # print_report(completed_results, "completed_stories")
    # can_move_completed = sum(1 for _, ok, _ in completed_results if ok)
    # cant_move_completed = sum(1 for _, ok, _ in completed_results if not ok)
    # print(f"\n== Tổng kết completed_stories ==")
    # print(f"Số truyện đủ điều kiện: {can_move_completed}")
    # print(f"Số truyện chưa đủ: {cant_move_completed}")

