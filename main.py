import os
import json
import time

from utils.utils import logger, sanitize_filename, ensure_directory_exists, create_proxy_template_if_not_exists
from config.config import (
    BASE_URL, DATA_FOLDER, PROXIES_FILE, PROXIES_FOLDER,
    MAX_GENRES_TO_CRAWL, MAX_STORIES_PER_GENRE_PAGE, MAX_STORIES_TOTAL_PER_GENRE,
    MAX_CHAPTERS_PER_STORY, MAX_CHAPTER_PAGES_TO_CRAWL, RETRY_FAILED_CHAPTERS_PASSES,
    REQUEST_DELAY
)
from config.proxy import load_proxies, loaded_proxies
from scraper import initialize_scraper
from analyze.parsers import (
    get_all_genres, get_all_stories_from_genre,
    get_chapters_from_story, get_story_chapter_content
)

def run_crawler():
    """
    Hàm chính để chạy crawler.
    """
    # --- Khởi tạo ---
    ensure_directory_exists(DATA_FOLDER)
    create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    load_proxies() # Tải proxies vào biến toàn cục trong module proxies
    initialize_scraper()

    homepage_url = BASE_URL + ("/" if not BASE_URL.endswith("/") else "")

    logger.info("=== BẮT ĐẦU QUÁ TRÌNH CRAWL TRUYỆN ===")
    logger.info(f"Thư mục lưu dữ liệu: {os.path.abspath(DATA_FOLDER)}")
    logger.info(f"Sử dụng {len(loaded_proxies)} proxy(s).") # Truy cập loaded_proxies từ module proxies
    logger.info(
        f"Giới hạn: "
        f"{MAX_GENRES_TO_CRAWL if MAX_GENRES_TO_CRAWL is not None else 'Không giới hạn'} thể loại, "
        f"{MAX_STORIES_TOTAL_PER_GENRE if MAX_STORIES_TOTAL_PER_GENRE is not None else 'Không giới hạn'} truyện/thể loại."
    )
    logger.info(
        f"Giới hạn chương xử lý ban đầu/truyện: {MAX_CHAPTERS_PER_STORY if MAX_CHAPTERS_PER_STORY is not None else 'Không giới hạn'}."
    )
    logger.info(
        f"Số lượt thử lại tổng thể cho các chương lỗi: {RETRY_FAILED_CHAPTERS_PASSES}."
    )
    logger.info(
        f"Giới hạn số trang truyện/thể loại (khi lấy ds truyện): {MAX_STORIES_PER_GENRE_PAGE if MAX_STORIES_PER_GENRE_PAGE is not None else 'Không giới hạn'}."
    )
    logger.info(f"Giới hạn số trang danh sách chương/truyện: {MAX_CHAPTER_PAGES_TO_CRAWL}.")
    logger.info("-----------------------------------------")

    # --- Bắt đầu Crawl ---
    genres = get_all_genres(homepage_url)
    ultimately_failed_chapters_log = []

    if not genres:
        logger.error("Không lấy được danh sách thể loại. Dừng script.")
        return

    genres_processed_count = 0
    for genre_idx, genre_data in enumerate(genres):
        if (
            MAX_GENRES_TO_CRAWL is not None
            and genres_processed_count >= MAX_GENRES_TO_CRAWL
        ):
            logger.info(f"Đã đạt giới hạn {MAX_GENRES_TO_CRAWL} thể loại. Dừng.")
            break

        genre_name_cleaned = sanitize_filename(genre_data["name"])
        if not genre_name_cleaned or genre_name_cleaned == "untitled": # Kiểm tra cả "untitled"
            logger.warning(
                f"Tên thể loại '{genre_data['name']}' không hợp lệ sau khi làm sạch ('{genre_name_cleaned}'). Bỏ qua."
            )
            continue

        genre_folder_path = os.path.join(DATA_FOLDER, genre_name_cleaned)
        if not ensure_directory_exists(genre_folder_path):
            logger.error(f"Không thể tạo thư mục cho thể loại '{genre_name_cleaned}'. Bỏ qua thể loại này.")
            continue

        logger.info(
            f"\n--- [{genres_processed_count + 1}/{MAX_GENRES_TO_CRAWL if MAX_GENRES_TO_CRAWL is not None else len(genres)}] "
            f"Đang crawl thể loại: {genre_data['name']} ({genre_data['url']}) ---"
        )

        stories_in_genre = get_all_stories_from_genre(
            genre_name=genre_data['name'], # Truyền tên thể loại để log
            genre_url=genre_data["url"],
            max_pages_to_crawl=MAX_STORIES_PER_GENRE_PAGE
        )

        if not stories_in_genre:
            logger.info(f"Không tìm thấy truyện nào cho thể loại: {genre_data['name']}")
        else:
            logger.info(
                f"Tìm thấy {len(stories_in_genre)} truyện trong thể loại '{genre_data['name']}'. "
                f"Sẽ xử lý tối đa {MAX_STORIES_TOTAL_PER_GENRE if MAX_STORIES_TOTAL_PER_GENRE is not None else 'tất cả'}."
            )

            stories_actually_processed_count = 0
            for story_idx, story_data_item in enumerate(stories_in_genre):
                if (
                    MAX_STORIES_TOTAL_PER_GENRE is not None
                    and stories_actually_processed_count >= MAX_STORIES_TOTAL_PER_GENRE
                ):
                    logger.info(
                        f"  Đã đạt giới hạn {MAX_STORIES_TOTAL_PER_GENRE} truyện cho thể loại '{genre_data['name']}'."
                    )
                    break

                story_title_cleaned = sanitize_filename(story_data_item["title"])
                if not story_title_cleaned or story_title_cleaned == "untitled":
                    logger.warning(
                        f"  Tên truyện '{story_data_item['title']}' không hợp lệ sau khi làm sạch ('{story_title_cleaned}'). Bỏ qua truyện này."
                    )
                    continue
                
                story_folder_path = os.path.join(genre_folder_path, story_title_cleaned)
                if not ensure_directory_exists(story_folder_path):
                    logger.error(f"  LỖI khi tạo thư mục cho truyện '{story_title_cleaned}'. Bỏ qua truyện này.")
                    continue
                
                logger.info(
                    f"\n  --- [{stories_actually_processed_count + 1}] Đang crawl truyện: {story_data_item.get('title')} ({story_data_item.get('url')}) ---"
                )
                logger.info(f"    Tác giả: {story_data_item.get('author', 'N/A')}")
                logger.info(f"    Ảnh bìa: {story_data_item.get('image_url', 'N/A')}")

                story_metadata_filename = os.path.join(story_folder_path, "metadata.json")
                if not os.path.exists(story_metadata_filename): # Chỉ tạo nếu chưa có
                    try:
                        metadata_to_save = {
                            "title": story_data_item.get("title"),
                            "url": story_data_item.get("url"),
                            "author": story_data_item.get("author"),
                            "image_url": story_data_item.get("image_url"),
                            "genre_name": genre_data.get("name"),
                            "genre_url": genre_data.get("url"),
                            "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S")
                        }
                        with open(story_metadata_filename, "w", encoding="utf-8") as meta_file:
                            json.dump(metadata_to_save, meta_file, ensure_ascii=False, indent=4)
                        logger.info(f"    Đã lưu metadata cho truyện vào: {os.path.basename(story_metadata_filename)}")
                    except Exception as e_meta:
                        logger.error(f"    LỖI khi lưu file metadata '{os.path.basename(story_metadata_filename)}': {e_meta}")
                
                chapters = get_chapters_from_story(
                    story_url=story_data_item["url"],
                    story_title=story_data_item["title"], # Truyền title để log
                    max_pages=MAX_CHAPTER_PAGES_TO_CRAWL
                )

                if not chapters:
                    logger.info(f"  Không tìm thấy chương nào cho truyện: {story_data_item['title']}")
                else:
                    max_chapters_to_process_initially = MAX_CHAPTERS_PER_STORY if MAX_CHAPTERS_PER_STORY is not None else len(chapters)
                    logger.info(
                        f"  Tìm thấy {len(chapters)} chương cho truyện '{story_data_item['title']}'. "
                        f"Sẽ xử lý ban đầu tối đa {max_chapters_to_process_initially} chương."
                    )
                    
                    chapters_attempted_in_pass1_count = 0
                    successfully_saved_chapters_filenames = set() # Lưu tên file đã lưu thành công
                    failed_chapters_for_retry = []

                    # --- LƯỢT 1: TẢI CÁC CHƯƠNG BAN ĐẦU ---
                    logger.info(f"    --- Bắt đầu LƯỢT 1: Tải các chương cho truyện '{story_data_item['title']}' ---")
                    for chapter_idx, chapter_info in enumerate(chapters):
                        if chapters_attempted_in_pass1_count >= max_chapters_to_process_initially:
                            logger.info(
                                f"      Đã xử lý {max_chapters_to_process_initially} chương theo giới hạn (trong lượt 1)."
                            )
                            break

                        chapter_title_cleaned = sanitize_filename(chapter_info["title"])
                        if not chapter_title_cleaned or chapter_title_cleaned == "untitled":
                            # Tạo tên file dựa trên index nếu tên chương không hợp lệ
                            chapter_title_cleaned = f"chuong_{chapter_idx + 1:04d}_khong_ten"
                        
                        # Đảm bảo tên file có cả số thứ tự để duy nhất và dễ sắp xếp
                        chapter_filename_only = f"{chapter_idx + 1:04d}_{chapter_title_cleaned}.txt"
                        chapter_filename_full_path = os.path.join(story_folder_path, chapter_filename_only)

                        chapters_attempted_in_pass1_count += 1

                        if (
                            os.path.exists(chapter_filename_full_path)
                            and os.path.getsize(chapter_filename_full_path) > 50 # Check size để tránh file rỗng
                        ):
                            logger.info(
                                f"        Chương '{chapter_info['title']}' (file: {chapter_filename_only}) đã tồn tại và có nội dung. Bỏ qua."
                            )
                            successfully_saved_chapters_filenames.add(chapter_filename_full_path)
                            continue
                        
                        logger.info(
                            f"        Lượt 1 - Chương {chapters_attempted_in_pass1_count}/{max_chapters_to_process_initially} "
                            f"(Tổng index: {chapter_idx + 1}/{len(chapters)}): Đang tải '{chapter_info['title']}' ({chapter_info['url']})"
                        )
                        
                        chapter_content = get_story_chapter_content(chapter_info["url"], chapter_info["title"])
                        
                        if chapter_content:
                            try:
                                with open(chapter_filename_full_path, "w", encoding="utf-8") as f:
                                    f.write(f"Nguồn: {chapter_info['url']}\n\n")
                                    f.write(f"Truyện: {story_data_item['title']}\n")
                                    f.write(f"Thể loại: {genre_data['name']}\n") # Thêm tên thể loại vào file chương
                                    f.write(f"Chương: {chapter_info['title']}\n\n")
                                    f.write(chapter_content)
                                logger.info(
                                    f"          Đã lưu (Lượt 1): '{chapter_info['title']}' vào {chapter_filename_only}"
                                )
                                successfully_saved_chapters_filenames.add(chapter_filename_full_path)
                            except Exception as e_save:
                                logger.error(
                                    f"          LỖI khi lưu (Lượt 1) '{chapter_filename_only}': {e_save}"
                                )
                                failed_chapters_for_retry.append({'chapter_data': chapter_info, 'filename': chapter_filename_full_path, 'original_idx': chapter_idx, 'filename_only': chapter_filename_only})
                        else:
                            logger.warning(
                                f"          Không lấy được nội dung (Lượt 1) cho chương: '{chapter_info['title']}'. Đánh dấu thử lại sau."
                            )
                            failed_chapters_for_retry.append({'chapter_data': chapter_info, 'filename': chapter_filename_full_path, 'original_idx': chapter_idx, 'filename_only': chapter_filename_only})
                        
                        if chapters_attempted_in_pass1_count < max_chapters_to_process_initially:
                            time.sleep(REQUEST_DELAY / 2 if REQUEST_DELAY > 0.2 else 0.1)

                    # --- CÁC LƯỢT THỬ LẠI CHO CÁC CHƯƠNG LỖI ---
                    for retry_pass_idx in range(RETRY_FAILED_CHAPTERS_PASSES):
                        if not failed_chapters_for_retry:
                            logger.info(f"    Không còn chương nào cần thử lại cho truyện '{story_data_item['title']}'.")
                            break

                        current_batch_to_retry = list(failed_chapters_for_retry)
                        failed_chapters_for_retry.clear()

                        logger.info(
                            f"    --- Bắt đầu LƯỢT THỬ LẠI TỔNG THỂ {retry_pass_idx + 1}/{RETRY_FAILED_CHAPTERS_PASSES} "
                            f"cho {len(current_batch_to_retry)} chương bị lỗi của truyện '{story_data_item['title']}' ---"
                        )
                        
                        for failed_info in current_batch_to_retry:
                            chapter_info_retry = failed_info['chapter_data']
                            chapter_filename_retry = failed_info['filename']
                            chapter_filename_retry_only = failed_info['filename_only']
                            
                            if os.path.exists(chapter_filename_retry) and os.path.getsize(chapter_filename_retry) > 50:
                                logger.info(f"        Chương '{chapter_info_retry['title']}' (file: {chapter_filename_retry_only}) đã được tải (có thể ở lần retry trước). Bỏ qua.")
                                if chapter_filename_retry not in successfully_saved_chapters_filenames:
                                     successfully_saved_chapters_filenames.add(chapter_filename_retry)
                                continue

                            logger.info(
                                f"        Lượt thử lại {retry_pass_idx + 1} - Chương [index gốc {failed_info['original_idx'] + 1}]: "
                                f"Đang tải '{chapter_info_retry['title']}' ({chapter_info_retry['url']})"
                            )
                            chapter_content = get_story_chapter_content(chapter_info_retry["url"], chapter_info_retry["title"])

                            if chapter_content:
                                try:
                                    with open(chapter_filename_retry, "w", encoding="utf-8") as f:
                                        f.write(f"Nguồn: {chapter_info_retry['url']}\n\n")
                                        f.write(f"Truyện: {story_data_item['title']}\n")
                                        f.write(f"Thể loại: {genre_data['name']}\n")
                                        f.write(f"Chương: {chapter_info_retry['title']}\n\n")
                                        f.write(chapter_content)
                                    logger.info(
                                        f"          Đã lưu (Lượt thử lại {retry_pass_idx + 1}): '{chapter_info_retry['title']}' vào {chapter_filename_retry_only}"
                                    )
                                    successfully_saved_chapters_filenames.add(chapter_filename_retry)
                                except Exception as e_save_retry:
                                    logger.error(
                                        f"          LỖI khi lưu (Lượt thử lại {retry_pass_idx + 1}) '{chapter_filename_retry_only}': {e_save_retry}"
                                    )
                                    failed_chapters_for_retry.append(failed_info)
                            else:
                                logger.warning(
                                    f"          Không lấy được nội dung (Lượt thử lại {retry_pass_idx + 1}) cho chương: '{chapter_info_retry['title']}'"
                                )
                                failed_chapters_for_retry.append(failed_info)
                            
                            if len(failed_chapters_for_retry) < len(current_batch_to_retry) or len(current_batch_to_retry) > 1:
                                time.sleep(REQUEST_DELAY) # Delay giữa các lần thử lại

                    if failed_chapters_for_retry:
                        logger.warning(
                            f"    SAU TẤT CẢ CÁC LƯỢT THỬ LẠI, VẪN CÒN {len(failed_chapters_for_retry)} chương bị lỗi cho truyện '{story_data_item['title']}':"
                        )
                        for ch_info_final_fail in failed_chapters_for_retry:
                            log_entry = (f"Truyện: '{story_data_item['title']}' - Chương lỗi: '{ch_info_final_fail['chapter_data']['title']}' "
                                         f"- URL: {ch_info_final_fail['chapter_data']['url']} - File dự kiến: {ch_info_final_fail['filename_only']}")
                            logger.warning(f"      - {log_entry}")
                            ultimately_failed_chapters_log.append(log_entry)
                    elif chapters_attempted_in_pass1_count > 0 and any(item['original_idx'] for item in failed_chapters_for_retry for _ in range(RETRY_FAILED_CHAPTERS_PASSES) ): # Kiểm tra xem có item nào từng lỗi không
                        logger.info(f"    Tất cả các chương lỗi của truyện '{story_data_item['title']}' đã được tải thành công sau khi thử lại.")
                    elif chapters_attempted_in_pass1_count > 0:
                        logger.info(f"    Tất cả các chương (trong giới hạn) cho truyện '{story_data_item['title']}' đã được xử lý (có thể từ lượt 1).")
                
                stories_actually_processed_count += 1
            
        genres_processed_count += 1
        logger.info(f"--- Hoàn thành crawl thể loại: {genre_data['name']} ---")

    if ultimately_failed_chapters_log:
        logger.error("\n\n=== CÁC CHƯƠNG CUỐI CÙNG VẪN BỊ LỖI SAU TẤT CẢ CÁC LƯỢT THỬ LẠI ===")
        for entry in ultimately_failed_chapters_log:
            logger.error(entry)
        failed_log_path = os.path.join(DATA_FOLDER, "_ultimately_failed_chapters_log.txt")
        try:
            with open(failed_log_path, "w", encoding="utf-8") as f_log:
                f_log.write("=== DANH SÁCH CÁC CHƯƠNG LỖI CUỐI CÙNG ===\n")
                for entry in ultimately_failed_chapters_log:
                    f_log.write(entry + "\n")
            logger.info(f"Đã ghi danh sách các chương lỗi cuối cùng vào: {failed_log_path}")
        except Exception as e_log_save:
            logger.error(f"Lỗi khi ghi file log các chương thất bại: {e_log_save}")


    logger.info("\n=== HOÀN TẤT TOÀN BỘ QUÁ TRÌNH CRAWL ===")


if __name__ == "__main__":
    run_crawler()