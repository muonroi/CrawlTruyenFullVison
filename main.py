import os
import json
import time
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse

from utils.utils import ( logger, sanitize_filename, ensure_directory_exists,
                          create_proxy_template_if_not_exists, load_crawl_state,
                          save_crawl_state, clear_all_crawl_state, clear_specific_state_keys )
from config.config import (
    BASE_URL, DATA_FOLDER, PROXIES_FILE, PROXIES_FOLDER,
    MAX_GENRES_TO_CRAWL, MAX_STORIES_PER_GENRE_PAGE, MAX_STORIES_TOTAL_PER_GENRE,
    MAX_CHAPTERS_PER_STORY, MAX_CHAPTER_PAGES_TO_CRAWL, RETRY_FAILED_CHAPTERS_PASSES,
    REQUEST_DELAY, STATE_FILE
)
from config.proxy import load_proxies, loaded_proxies
from scraper import initialize_scraper
from analyze.parsers import (
    get_all_genres, get_all_stories_from_genre,
    get_chapters_from_story, get_story_chapter_content,
    get_story_details
)

# --- CÁC HÀM LƯU/TẢI TRẠNG THÁI ---
# (Giữ nguyên)

def initialize_and_log_setup_with_state() -> Tuple[str, Dict[str, Any]]:
    """Handles initial directory setup, proxy loading, scraper init, logging, and loads crawl state."""
    ensure_directory_exists(DATA_FOLDER)
    create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    load_proxies()
    initialize_scraper()

    homepage_url = BASE_URL + ("/" if not BASE_URL.endswith("/") else "")
    crawl_state = load_crawl_state()

    logger.info("=== BẮT ĐẦU QUÁ TRÌNH CRAWL TRUYỆN ===")
    logger.info(f"Thư mục lưu dữ liệu: {os.path.abspath(DATA_FOLDER)}")
    logger.info(f"Sử dụng {len(loaded_proxies)} proxy(s).")
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
    logger.info(f"Giới hạn số trang danh sách chương/truyện: {MAX_CHAPTER_PAGES_TO_CRAWL if MAX_CHAPTER_PAGES_TO_CRAWL is not None else 'Không giới hạn'}.")

    if crawl_state:
        loggable_state = {k: v for k, v in crawl_state.items() if k not in ['processed_chapter_urls_for_current_story', 'globally_completed_story_urls']}
        if crawl_state.get('processed_chapter_urls_for_current_story'):
            loggable_state['processed_chapters_count'] = len(crawl_state['processed_chapter_urls_for_current_story'])
        if crawl_state.get('globally_completed_story_urls'):
            loggable_state['globally_completed_stories_count'] = len(crawl_state['globally_completed_story_urls'])
        logger.info(f"Tìm thấy trạng thái crawl trước đó. Cố gắng tiếp tục từ: {loggable_state}")
    logger.info("-----------------------------------------")
    return homepage_url, crawl_state


def get_global_story_folder_path(story_data_item: Dict[str, Any]) -> str:
    """
    Tạo đường dẫn thư mục toàn cục cho một truyện.
    """
    parsed_url = urlparse(story_data_item["url"])
    story_slug = parsed_url.path.strip('/').split('/')[-1]
    sanitized_slug = sanitize_filename(story_slug if story_slug else story_data_item.get("title", "untitled_story"))
    if not sanitized_slug or sanitized_slug == "untitled":
        sanitized_slug = sanitize_filename(story_data_item.get("title", "untitled_story")) + "_" + str(hash(story_data_item["url"]))[-6:]

    global_stories_dir = os.path.join(DATA_FOLDER, "_global_stories_data_")
    ensure_directory_exists(global_stories_dir)
    return os.path.join(global_stories_dir, sanitized_slug)


def save_story_metadata_file(story_base_data: Dict[str, Any],
                             current_discovery_genre_data: Optional[Dict[str, Any]],
                             story_folder_path: str,
                             fetched_story_details: Optional[Dict[str, Any]],
                             existing_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]: # Trả về metadata đã lưu
    """
    Lưu hoặc cập nhật file metadata.json cho một truyện. Trả về dict metadata đã được lưu.
    """
    story_metadata_filename = os.path.join(story_folder_path, "metadata.json")
    metadata_to_save = existing_metadata.copy() if existing_metadata else {}

    metadata_to_save["title"] = story_base_data.get("title", metadata_to_save.get("title"))
    metadata_to_save["url"] = story_base_data.get("url", metadata_to_save.get("url"))
    if not metadata_to_save.get("author") and story_base_data.get("author"):
        metadata_to_save["author"] = story_base_data.get("author")
    if not metadata_to_save.get("image_url") and story_base_data.get("image_url"):
        metadata_to_save["image_url"] = story_base_data.get("image_url")
    metadata_to_save["crawled_by"] = "muonroi"

    current_categories_in_meta: List[Dict[str, Optional[str]]] = metadata_to_save.get("categories", [])
    seen_category_urls = {cat.get("url") for cat in current_categories_in_meta if cat.get("url")}

    if current_discovery_genre_data and current_discovery_genre_data.get("url"):
        if current_discovery_genre_data.get("url") not in seen_category_urls:
            current_categories_in_meta.append({
                "name": current_discovery_genre_data.get("name"),
                "url": current_discovery_genre_data.get("url")
            })
            seen_category_urls.add(current_discovery_genre_data.get("url"))
    elif current_discovery_genre_data and current_discovery_genre_data.get("name") and \
         not any(cat.get("name") == current_discovery_genre_data.get("name") for cat in current_categories_in_meta):
        current_categories_in_meta.append({"name": current_discovery_genre_data.get("name"), "url": None})

    if fetched_story_details and "detailed_genres" in fetched_story_details:
        for detailed_genre in fetched_story_details.get("detailed_genres", []):
            if isinstance(detailed_genre, dict) and detailed_genre.get("url"):
                if detailed_genre.get("url") not in seen_category_urls:
                    current_categories_in_meta.append(detailed_genre)
                    seen_category_urls.add(detailed_genre.get("url"))
            elif isinstance(detailed_genre, str) and not any(cat.get("name") == detailed_genre for cat in current_categories_in_meta):
                logger.warning(f"Thể loại chi tiết '{detailed_genre}' từ parser chỉ có tên. Thêm với URL None.")
                current_categories_in_meta.append({"name": detailed_genre, "url": None})
    
    metadata_to_save["categories"] = sorted(current_categories_in_meta, key=lambda x: (x.get("name") or "").lower())

    if fetched_story_details:
        keys_from_details = ["description", "status", "source", "rating_value", "rating_count", "total_chapters_on_site"]
        for key in keys_from_details:
            if fetched_story_details.get(key) is not None:
                metadata_to_save[key] = fetched_story_details.get(key)
            elif key not in metadata_to_save:
                 metadata_to_save[key] = None # Đảm bảo key tồn tại nếu chưa có

    metadata_to_save["metadata_updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    if "crawled_at" not in metadata_to_save:
        metadata_to_save["crawled_at"] = metadata_to_save["metadata_updated_at"]

    try:
        with open(story_metadata_filename, "w", encoding="utf-8") as meta_file:
            json.dump(metadata_to_save, meta_file, ensure_ascii=False, indent=4)
        logger.info(f"    Đã lưu/cập nhật metadata cho truyện vào: {os.path.basename(story_metadata_filename)}")
        return metadata_to_save # Trả về metadata đã lưu/cập nhật
    except Exception as e_meta:
        logger.error(f"    LỖI khi lưu/cập nhật file metadata '{os.path.basename(story_metadata_filename)}': {e_meta}")
        return existing_metadata or {} # Trả về metadata cũ hoặc rỗng nếu lỗi


def _handle_chapter_processing_pass(
        chapters_to_process: List[Dict[str, Any]],
        story_data_item: Dict[str, Any],
        current_discovery_genre_data: Dict[str, Any],
        story_folder_path: str, # Đây là global_story_folder
        successfully_saved_filenames_set: set,
        failed_chapters_list: List[Dict[str, Any]],
        pass_description: str,
        crawl_state: Dict[str, Any],
        chapter_offset: int = 0, 
        current_attempt_count_func: Optional[callable] = None
    ):
    for idx_in_current_batch, chapter_item_info in enumerate(chapters_to_process):
        if 'chapter_data' in chapter_item_info: 
            chapter_info = chapter_item_info['chapter_data']
            chapter_filename_full_path = chapter_item_info['filename']
            chapter_filename_only = chapter_item_info['filename_only']
            original_idx = chapter_item_info['original_idx'] 
            chapter_display_idx_log = f"[index gốc {original_idx + 1}]"
        else: 
            chapter_info = chapter_item_info
            original_idx = chapter_offset 
            chapter_title_cleaned = sanitize_filename(chapter_info["title"])
            if not chapter_title_cleaned or chapter_title_cleaned == "untitled":
                chapter_title_cleaned = f"chuong_{original_idx + 1:04d}_khong_ten"
            chapter_filename_only = f"{original_idx + 1:04d}_{chapter_title_cleaned}.txt"
            chapter_filename_full_path = os.path.join(story_folder_path, chapter_filename_only)
            chapter_display_idx_log = current_attempt_count_func() if current_attempt_count_func else f"gốc {original_idx+1}"

        processed_chapters_for_current_story = crawl_state.get('processed_chapter_urls_for_current_story', [])
        if chapter_info['url'] in processed_chapters_for_current_story:
            logger.info(f"        Chương '{chapter_info['title']}' ({chapter_info['url']}) đã được xử lý theo state. Bỏ qua.")
            if chapter_filename_full_path not in successfully_saved_filenames_set:
                 if os.path.exists(chapter_filename_full_path) and os.path.getsize(chapter_filename_full_path) > 50:
                    successfully_saved_filenames_set.add(chapter_filename_full_path)
            continue

        if os.path.exists(chapter_filename_full_path) and os.path.getsize(chapter_filename_full_path) > 50:
            logger.info(f"        Chương '{chapter_info['title']}' (file: {chapter_filename_only}) đã tồn tại và có nội dung (file system). Bỏ qua {pass_description.lower()}.")
            successfully_saved_filenames_set.add(chapter_filename_full_path)
            if chapter_info['url'] not in processed_chapters_for_current_story:
                processed_chapters_for_current_story.append(chapter_info['url'])
                crawl_state['processed_chapter_urls_for_current_story'] = processed_chapters_for_current_story
                save_crawl_state(crawl_state)
            continue

        success = download_and_save_chapter(
            chapter_info, story_data_item, current_discovery_genre_data,
            chapter_filename_full_path, chapter_filename_only,
            pass_description, chapter_display_idx_log
        )

        if success:
            successfully_saved_filenames_set.add(chapter_filename_full_path)
            processed_chapters_for_current_story = crawl_state.get('processed_chapter_urls_for_current_story', [])
            if chapter_info['url'] not in processed_chapters_for_current_story:
                processed_chapters_for_current_story.append(chapter_info['url'])
            crawl_state['processed_chapter_urls_for_current_story'] = processed_chapters_for_current_story
            save_crawl_state(crawl_state)
        else:
            retry_item_info = {
                'chapter_data': chapter_info,
                'filename': chapter_filename_full_path,
                'original_idx': original_idx,
                'filename_only': chapter_filename_only
            }
            if retry_item_info not in failed_chapters_list:
                 failed_chapters_list.append(retry_item_info)

def download_and_save_chapter(chapter_info: Dict[str, Any],
                              story_data_item: Dict[str, Any],
                              current_discovery_genre_data: Dict[str, Any],
                              chapter_filename_full_path: str,
                              chapter_filename_only: str,
                              pass_description: str = "Lượt 1",
                              chapter_display_idx_log: str = "") -> bool:
    log_prefix = f"        {pass_description} - Chương {chapter_display_idx_log}" if chapter_display_idx_log else f"        {pass_description}"
    logger.info(f"{log_prefix}: Đang tải '{chapter_info['title']}' ({chapter_info['url']})")

    chapter_content = get_story_chapter_content(chapter_info["url"], chapter_info["title"])
    if chapter_content:
        try:
            with open(chapter_filename_full_path, "w", encoding="utf-8") as f:
                f.write(f"Nguồn: {chapter_info['url']}\n\n")
                f.write(f"Truyện: {story_data_item['title']}\n")
                f.write(f"Thể loại phát hiện: {current_discovery_genre_data['name']}\n")
                f.write(f"Chương: {chapter_info['title']}\n\n")
                f.write(chapter_content)
            logger.info(
                f"          Đã lưu ({pass_description}): '{chapter_info['title']}' vào {chapter_filename_only}"
            )
            return True
        except Exception as e_save:
            logger.error(
                f"          LỖI khi lưu ({pass_description}) '{chapter_filename_only}': {e_save}"
            )
            return False
    else:
        logger.warning(
            f"          Không lấy được nội dung ({pass_description}) cho chương: '{chapter_info['title']}'."
        )
        return False

def process_all_chapters_for_story(chapters: List[Dict[str, Any]],
                                   story_data_item: Dict[str, Any],
                                   current_discovery_genre_data: Dict[str, Any],
                                   story_folder_path: str,
                                   ultimately_failed_chapters_log: List[str],
                                   crawl_state: Dict[str, Any],
                                   story_status_from_details: Optional[str],
                                   total_chapters_on_site_from_details: Optional[int]
                                   ) -> int: # Trả về số lượng chương đã tải thành công và có file
    if not chapters:
        logger.info(f"  Truyện '{story_data_item['title']}': Không tìm thấy chương nào từ parser.")
        return 0

    successfully_saved_filenames = set()
    failed_chapters_for_next_retry_pass: List[Dict[str, Any]] = []
    processed_in_state = set(crawl_state.get('processed_chapter_urls_for_current_story', []))
    
    for ch_idx, ch_info_state_check in enumerate(chapters):
        if ch_info_state_check['url'] in processed_in_state:
            ch_title_cleaned_state = sanitize_filename(ch_info_state_check.get("title", f"chuong_{ch_idx + 1}"))
            if not ch_title_cleaned_state or ch_title_cleaned_state == "untitled":
                ch_title_cleaned_state = f"chuong_{ch_idx + 1:04d}_khong_ten"
            ch_fn_only_state = f"{ch_idx + 1:04d}_{ch_title_cleaned_state}.txt"
            ch_fn_full_state = os.path.join(story_folder_path, ch_fn_only_state)
            if os.path.exists(ch_fn_full_state) and os.path.getsize(ch_fn_full_state) > 50:
                successfully_saved_filenames.add(ch_fn_full_state)

    num_already_processed_effectively = len(successfully_saved_filenames)
    chapters_to_attempt_this_session: List[Dict[str, Any]] = []
    unprocessed_chapters_not_in_state = [ch for ch in chapters if ch['url'] not in processed_in_state]

    # Xác định số chương mục tiêu cần tải
    # Nếu truyện "Hoàn thành" và có tổng số chương, đó là mục tiêu (nếu không có MAX_CHAPTERS_PER_STORY)
    # Nếu "Đang ra" hoặc không có tổng số chương, hoặc có MAX_CHAPTERS_PER_STORY, thì mục tiêu là MAX_CHAPTERS_PER_STORY (nếu được đặt)
    # hoặc tất cả các chương tìm thấy (nếu MAX_CHAPTERS_PER_STORY là None).

    target_new_chapters_to_download = 0
    if story_status_from_details and story_status_from_details.lower() in ["hoàn thành", "full", "completed"] and \
       total_chapters_on_site_from_details is not None and total_chapters_on_site_from_details > 0:
        if MAX_CHAPTERS_PER_STORY is None: # Crawl hết nếu không có giới hạn chương
            target_new_chapters_to_download = total_chapters_on_site_from_details - num_already_processed_effectively
        else: # Có giới hạn, lấy min của giới hạn và số chương còn lại để hoàn thành
            target_new_chapters_to_download = min(MAX_CHAPTERS_PER_STORY, total_chapters_on_site_from_details) - num_already_processed_effectively
    elif MAX_CHAPTERS_PER_STORY is not None: # Truyện đang ra hoặc không rõ, có giới hạn
        target_new_chapters_to_download = MAX_CHAPTERS_PER_STORY - num_already_processed_effectively
    else: # Truyện đang ra hoặc không rõ, không có giới hạn chương -> tải hết những gì chưa có
        target_new_chapters_to_download = len(unprocessed_chapters_not_in_state)

    if target_new_chapters_to_download < 0:
        target_new_chapters_to_download = 0
    
    chapters_to_attempt_this_session = unprocessed_chapters_not_in_state[:target_new_chapters_to_download]
    initial_chapters_to_attempt_count = len(chapters_to_attempt_this_session)

    if initial_chapters_to_attempt_count > 0:
        logger.info(f"    Truyện '{story_data_item['title']}': Số chương đã có (state/file): {num_already_processed_effectively}. Số chương mới cần thử tải trong lượt 1: {initial_chapters_to_attempt_count}")
        logger.info(f"    --- Bắt đầu LƯỢT 1 (cho {initial_chapters_to_attempt_count} chương mới): Tải các chương cho truyện '{story_data_item['title']}' ---")
    elif num_already_processed_effectively > 0:
        limit_msg = f"(giới hạn {MAX_CHAPTERS_PER_STORY})" if MAX_CHAPTERS_PER_STORY is not None else ""
        if story_status_from_details and story_status_from_details.lower() in ["hoàn thành", "full", "completed"] and \
           total_chapters_on_site_from_details is not None and num_already_processed_effectively >= total_chapters_on_site_from_details:
            logger.info(f"    Truyện '{story_data_item['title']}' (Hoàn thành - {total_chapters_on_site_from_details} chương): Đã có {num_already_processed_effectively} chương. Tất cả chương đã được xử lý.")
        elif MAX_CHAPTERS_PER_STORY is not None and num_already_processed_effectively >= MAX_CHAPTERS_PER_STORY:
            logger.info(f"    Truyện '{story_data_item['title']}': Đã có {num_already_processed_effectively} chương {limit_msg}. Không tải thêm chương mới.")
        elif not unprocessed_chapters_not_in_state:
             logger.info(f"    Truyện '{story_data_item['title']}': Tất cả {len(chapters)} chương đã được xử lý (từ state/file).")
        else: # Trường hợp còn unprocessed nhưng target_new_chapters_to_download = 0
            logger.info(f"    Truyện '{story_data_item['title']}': Không có chương mới nào được lên lịch tải trong lượt này (đã đủ theo giới hạn hoặc các chương còn lại sẽ được xử lý sau).")


    chapters_attempted_count_pass1 = 0
    for chapter_info_to_attempt in chapters_to_attempt_this_session:
        chapters_attempted_count_pass1 += 1
        original_idx = -1 
        for i, original_ch_info in enumerate(chapters):
            if original_ch_info['url'] == chapter_info_to_attempt['url']:
                original_idx = i
                break
        if original_idx == -1: 
            logger.error(f"Lỗi logic: Không tìm thấy original_idx cho chương {chapter_info_to_attempt['url']}. Bỏ qua.")
            continue
        _handle_chapter_processing_pass(
            chapters_to_process=[chapter_info_to_attempt], story_data_item=story_data_item,
            current_discovery_genre_data=current_discovery_genre_data, story_folder_path=story_folder_path,
            successfully_saved_filenames_set=successfully_saved_filenames,
            failed_chapters_list=failed_chapters_for_next_retry_pass, pass_description="Lượt 1",
            crawl_state=crawl_state, chapter_offset=original_idx,
            current_attempt_count_func=lambda o_idx=original_idx, c_attempt=chapters_attempted_count_pass1: f"{c_attempt}/{initial_chapters_to_attempt_count} (Index gốc: {o_idx + 1})"
        )
        if chapters_attempted_count_pass1 < initial_chapters_to_attempt_count:
             time.sleep(REQUEST_DELAY / 2 if REQUEST_DELAY > 0.2 else 0.1)

    initial_failure_count_this_session = len(failed_chapters_for_next_retry_pass)

    for retry_pass_idx in range(RETRY_FAILED_CHAPTERS_PASSES):
        if not failed_chapters_for_next_retry_pass:
            break
        current_batch_to_retry = list(failed_chapters_for_next_retry_pass)
        failed_chapters_for_next_retry_pass.clear()
        pass_description = f"Lượt thử lại {retry_pass_idx + 1}"
        logger.info(
            f"    --- Bắt đầu {pass_description.upper()} cho {len(current_batch_to_retry)} chương bị lỗi của truyện '{story_data_item['title']}' ---"
        )
        _handle_chapter_processing_pass(
            chapters_to_process=current_batch_to_retry, story_data_item=story_data_item,
            current_discovery_genre_data=current_discovery_genre_data, story_folder_path=story_folder_path,
            successfully_saved_filenames_set=successfully_saved_filenames,
            failed_chapters_list=failed_chapters_for_next_retry_pass,
            pass_description=pass_description, crawl_state=crawl_state
        )
        if failed_chapters_for_next_retry_pass:
            time.sleep(REQUEST_DELAY)

    final_successfully_downloaded_this_session = len(successfully_saved_filenames) - (num_already_processed_effectively - initial_chapters_to_attempt_count + chapters_attempted_count_pass1 - initial_failure_count_this_session)
    # Hoặc đơn giản hơn, là số chương trong successfully_saved_filenames không có trong processed_in_state ban đầu
    newly_successful_chapters_count = 0
    for chap_url in processed_in_state: # Đếm những chương trong state mà thực sự có file
        # Cần đường dẫn file để kiểm tra, logic này hơi phức tạp để tính chính xác ở đây
        pass # Bỏ qua tính toán phức tạp này, dựa vào successfully_saved_filenames


    if failed_chapters_for_next_retry_pass:
        logger.warning(f"    SAU TẤT CẢ CÁC LƯỢT THỬ LẠI, VẪN CÒN {len(failed_chapters_for_next_retry_pass)} chương bị lỗi (trong session này) cho truyện '{story_data_item['title']}':")
        for failed_item in failed_chapters_for_next_retry_pass:
            log_entry = (f"Truyện: '{story_data_item['title']}' ({story_data_item['url']}) - Chương lỗi: '{failed_item['chapter_data']['title']}' "
                         f"- URL: {failed_item['chapter_data']['url']} - File dự kiến: {failed_item['filename_only']}")
            logger.warning(f"      - {log_entry}")
            ultimately_failed_chapters_log.append(log_entry)
    elif initial_failure_count_this_session > 0:
        logger.info(f"    Tất cả các chương ({initial_failure_count_this_session}) ban đầu bị lỗi (trong session này) của truyện '{story_data_item['title']}' đã được tải thành công sau khi thử lại.")
    elif initial_chapters_to_attempt_count > 0: # Đã thử tải chương mới và không có lỗi nào từ đầu
        logger.info(f"    Đã xử lý thành công {initial_chapters_to_attempt_count} chương mới cho truyện '{story_data_item['title']}'.")
    
    # Trả về tổng số chương đã được xác nhận là có file và hợp lệ (bao gồm cả cũ và mới trong session này)
    return len(successfully_saved_filenames)


def process_story_item(story_data_item: Dict[str, Any],
                       current_discovery_genre_data: Dict[str, Any],
                       story_global_folder_path: str, # SỬA: Nhận đường dẫn này
                       story_processing_count_display: str,
                       ultimately_failed_chapters_log: List[str],
                       crawl_state: Dict[str, Any]
                       # SỬA: Loại bỏ fetched_story_details khỏi tham số ở đây
                       ) -> bool:
    
    logger.info(f"\n  --- {story_processing_count_display} Đang xử lý truyện: {story_data_item.get('title')} ({story_data_item.get('url')}) ---")
    logger.info(f"    Phát hiện trong thể loại: {current_discovery_genre_data.get('name')}")

    story_metadata_filepath = os.path.join(story_global_folder_path, "metadata.json")
    existing_metadata = None
    fetched_details_for_this_run = None # Sẽ chứa dữ liệu mới nếu cần fetch
    needs_detail_fetch = True # Mặc định là cần fetch nếu metadata thiếu hoặc chưa có genre hiện tại

    if os.path.exists(story_metadata_filepath):
        try:
            with open(story_metadata_filepath, 'r', encoding='utf-8') as f_meta_check:
                existing_metadata = json.load(f_meta_check)
            logger.info(f"    Đã tìm thấy file metadata.json cho '{story_data_item.get('title')}'.")
            
            has_desc = bool(existing_metadata.get("description"))
            has_status = bool(existing_metadata.get("status"))
            has_total_chapters = existing_metadata.get("total_chapters_on_site") is not None
            categories_in_meta = existing_metadata.get("categories", [])
            current_genre_already_in_meta = any(
                isinstance(cat, dict) and cat.get("url") == current_discovery_genre_data.get("url")
                for cat in categories_in_meta
            )

            if has_desc and has_status and has_total_chapters and current_genre_already_in_meta:
                logger.info("      Metadata (description, status, total_chapters, category hiện tại) đã đầy đủ. Không cần fetch lại chi tiết truyện.")
                needs_detail_fetch = False
                fetched_details_for_this_run = existing_metadata # Dùng lại metadata cũ cho save_story_metadata_file
            else:
                if not has_desc: logger.info("      Description bị thiếu. Sẽ fetch chi tiết.")
                if not has_status: logger.info("      Status truyện bị thiếu. Sẽ fetch chi tiết.")
                if not has_total_chapters: logger.info("      Total_chapters_on_site bị thiếu. Sẽ fetch chi tiết.")
                if not current_genre_already_in_meta: logger.info(f"      Thể loại '{current_discovery_genre_data.get('name')}' chưa có. Sẽ fetch chi tiết.")
        except Exception as e_read_meta:
            logger.warning(f"    Lỗi khi đọc metadata cho '{story_data_item.get('title')}': {e_read_meta}. Sẽ fetch lại chi tiết.")
            existing_metadata = {} 
    else:
        logger.info(f"    Không tìm thấy file metadata.json cho '{story_data_item.get('title')}'. Sẽ fetch chi tiết truyện.")

    if needs_detail_fetch:
        logger.info(f"    Đang fetch chi tiết cho truyện '{story_data_item.get('title')}'...")
        details_from_parser = get_story_details(story_data_item["url"], story_data_item["title"])
        if not details_from_parser:
            logger.warning(f"    Không lấy được chi tiết nào cho truyện '{story_data_item.get('title')}' sau khi fetch.")
            # Nếu fetch lỗi, vẫn cố gắng dùng existing_metadata (nếu có) để cập nhật category
            fetched_details_for_this_run = existing_metadata.copy() if existing_metadata else {}
        else:
            fetched_details_for_this_run = details_from_parser
            # Log nếu các trường quan trọng vẫn thiếu sau khi fetch
            if not fetched_details_for_this_run.get("description") and (not existing_metadata or not existing_metadata.get("description")):
                 logger.warning(f"    Vẫn không lấy được description cho truyện '{story_data_item.get('title')}' sau khi fetch.")
            if not fetched_details_for_this_run.get("status") and (not existing_metadata or not existing_metadata.get("status")):
                 logger.warning(f"    Vẫn không lấy được status cho truyện '{story_data_item.get('title')}' sau khi fetch.")
            if fetched_details_for_this_run.get("total_chapters_on_site") is None and (not existing_metadata or existing_metadata.get("total_chapters_on_site") is None):
                 logger.warning(f"    Vẫn không lấy được total_chapters_on_site cho truyện '{story_data_item.get('title')}' sau khi fetch.")
    
    updated_metadata = save_story_metadata_file(story_data_item, current_discovery_genre_data, story_global_folder_path, fetched_details_for_this_run, existing_metadata)
    story_status = updated_metadata.get("status")
    total_chapters_on_site = updated_metadata.get("total_chapters_on_site")
    if isinstance(total_chapters_on_site, str) and total_chapters_on_site.isdigit():
        total_chapters_on_site = int(total_chapters_on_site)
    elif not isinstance(total_chapters_on_site, int):
        total_chapters_on_site = None

    crawl_state['current_story_url'] = story_data_item['url']
    crawl_state['current_story_title_for_debug'] = story_data_item['title']
    if crawl_state.get('previous_story_url_in_state_for_chapters') != story_data_item['url']:
        logger.debug(f"Truyện mới hoặc khác state (cho chương): '{story_data_item['title']}'. Reset danh sách chương đã xử lý trong state.")
        crawl_state['processed_chapter_urls_for_current_story'] = []
    else:
        logger.debug(f"Tiếp tục chương cho truyện từ state: '{story_data_item['title']}'. Đã xử lý: {len(crawl_state.get('processed_chapter_urls_for_current_story',[]))} chương.")
    crawl_state['previous_story_url_in_state_for_chapters'] = story_data_item['url']
    save_crawl_state(crawl_state)

    chapters = get_chapters_from_story(
        story_url=story_data_item["url"],
        story_title=story_data_item["title"],
        max_pages=MAX_CHAPTER_PAGES_TO_CRAWL
    )
    
    num_successfully_saved_chapters = process_all_chapters_for_story(
                                   chapters, story_data_item, current_discovery_genre_data, story_global_folder_path,
                                   ultimately_failed_chapters_log, crawl_state,
                                   story_status, total_chapters_on_site)

    is_globally_complete = False
    if story_status and story_status.lower() in ["hoàn thành", "full", "completed"]:
        if total_chapters_on_site is not None and total_chapters_on_site > 0:
            if num_successfully_saved_chapters >= total_chapters_on_site:
                is_globally_complete = True
                logger.info(f"    Truyện '{story_data_item['title']}' (Hoàn thành - {total_chapters_on_site} chương) đã có đủ {num_successfully_saved_chapters} chương. Đánh dấu hoàn thành toàn cục.")
            else:
                logger.info(f"    Truyện '{story_data_item['title']}' (Hoàn thành - {total_chapters_on_site} chương) mới có {num_successfully_saved_chapters} chương. Chưa hoàn thành toàn cục.")
        elif total_chapters_on_site == 0 or (total_chapters_on_site is None and not chapters):
             is_globally_complete = True
             logger.info(f"    Truyện '{story_data_item['title']}' (Hoàn thành) không có chương nào hoặc không tìm thấy chương. Đánh dấu hoàn thành toàn cục.")
        else: 
            logger.warning(f"    Truyện '{story_data_item['title']}' (Hoàn thành) nhưng không có thông tin tổng số chương. Sẽ không đánh dấu hoàn thành toàn cục trừ khi không có chương nào được tìm thấy.")
            if not chapters and num_successfully_saved_chapters == 0:
                is_globally_complete = True
    elif story_status: 
        logger.info(f"    Truyện '{story_data_item['title']}' có trạng thái '{story_status}'. Sẽ không đánh dấu hoàn thành toàn cục.")
    else: 
        logger.warning(f"    Truyện '{story_data_item['title']}': Không có thông tin trạng thái. Sẽ không đánh dấu hoàn thành toàn cục.")

    clear_specific_state_keys(crawl_state, ['processed_chapter_urls_for_current_story'])
    save_crawl_state(crawl_state)
    return is_globally_complete

def process_genre_item(genre_data: Dict[str, Any],
                       genre_count_display: str,
                       ultimately_failed_chapters_log: List[str],
                       crawl_state: Dict[str, Any]):
    logger.info(f"\n--- {genre_count_display} Đang xử lý thể loại: {genre_data['name']} ({genre_data['url']}) ---")

    crawl_state['current_genre_url'] = genre_data['url']
    crawl_state['current_genre_name_for_debug'] = genre_data['name']
    if crawl_state.get('previous_genre_url_in_state_for_stories') != genre_data['url']:
        logger.debug(f"Thể loại mới hoặc khác state: '{genre_data['name']}'. Reset story index.")
        crawl_state['current_story_index_in_genre'] = 0
    crawl_state['previous_genre_url_in_state_for_stories'] = genre_data['url']
    save_crawl_state(crawl_state)

    stories_in_genre_list = get_all_stories_from_genre(
        genre_name=genre_data['name'],
        genre_url=genre_data["url"],
        max_pages_to_crawl=MAX_STORIES_PER_GENRE_PAGE
    )

    if not stories_in_genre_list:
        logger.info(f"Không tìm thấy truyện nào cho thể loại: {genre_data['name']}")
    else:
        logger.info(
            f"Tìm thấy {len(stories_in_genre_list)} truyện trong thể loại '{genre_data['name']}'. "
            f"Sẽ xử lý tối đa {MAX_STORIES_TOTAL_PER_GENRE if MAX_STORIES_TOTAL_PER_GENRE is not None else 'tất cả'}."
        )

        stories_processed_count_for_this_genre_this_run = 0
        start_story_idx_for_this_genre = crawl_state.get('current_story_index_in_genre', 0)
        globally_completed_story_urls = set(crawl_state.get('globally_completed_story_urls', []))

        for story_idx, story_data_item in enumerate(stories_in_genre_list):
            if story_idx < start_story_idx_for_this_genre:
                logger.info(f"  Bỏ qua truyện (index {story_idx} < start_idx {start_story_idx_for_this_genre}): {story_data_item['title']}")
                continue

            story_global_folder = get_global_story_folder_path(story_data_item)
            ensure_directory_exists(story_global_folder)
            story_metadata_global_path = os.path.join(story_global_folder, "metadata.json")

            if story_data_item['url'] in globally_completed_story_urls:
                logger.info(f"  Truyện '{story_data_item.get('title')}' ({story_data_item.get('url')}) đã được xử lý hoàn toàn trước đó (globally).")
                
                existing_global_meta = None
                if os.path.exists(story_metadata_global_path):
                    try:
                        with open(story_metadata_global_path, 'r', encoding='utf-8') as f:
                            existing_global_meta = json.load(f)
                    except Exception as e:
                        logger.warning(f"Lỗi đọc metadata toàn cục cho truyện đã hoàn thành '{story_data_item.get('title')}': {e}.")
                
                logger.info(f"    Cập nhật/Xác nhận thông tin thể loại '{genre_data.get('name')}' cho metadata của truyện đã hoàn thành '{story_data_item.get('title')}'.")
                details_for_completed_story = get_story_details(story_data_item["url"], story_data_item["title"])
                save_story_metadata_file(story_data_item, genre_data, story_global_folder, details_for_completed_story, existing_global_meta)

                crawl_state['current_story_index_in_genre'] = story_idx + 1
                save_crawl_state(crawl_state)
                stories_processed_count_for_this_genre_this_run += 1
                continue

            if MAX_STORIES_TOTAL_PER_GENRE is not None and stories_processed_count_for_this_genre_this_run >= MAX_STORIES_TOTAL_PER_GENRE:
                logger.info(f"  Đã đạt giới hạn {MAX_STORIES_TOTAL_PER_GENRE} truyện cho thể loại '{genre_data['name']}' trong lần chạy này.")
                break
            
            crawl_state['current_story_index_in_genre'] = story_idx
            save_crawl_state(crawl_state)

            story_processing_count_display = (
                f"Truyện [{stories_processed_count_for_this_genre_this_run + 1}/"
                f"{min(len(stories_in_genre_list) - start_story_idx_for_this_genre, MAX_STORIES_TOTAL_PER_GENRE) if MAX_STORIES_TOTAL_PER_GENRE is not None else len(stories_in_genre_list) - start_story_idx_for_this_genre}]"
            )
            
            # process_story_item giờ trả về is_globally_chapter_complete
            is_story_now_globally_complete = process_story_item(
                                                story_data_item, genre_data, story_global_folder,
                                                story_processing_count_display,
                                                ultimately_failed_chapters_log, crawl_state
                                            )
            
            stories_processed_count_for_this_genre_this_run += 1
            if is_story_now_globally_complete:
                globally_completed_story_urls.add(story_data_item['url'])
                crawl_state['globally_completed_story_urls'] = sorted(list(globally_completed_story_urls))
            
            crawl_state['current_story_index_in_genre'] = story_idx + 1
            save_crawl_state(crawl_state)


    logger.info(f"--- Hoàn thành xử lý thể loại: {genre_data['name']} ---")
    keys_to_clear = [
        'current_story_index_in_genre', 'current_story_url', 'current_story_title_for_debug',
        'processed_chapter_urls_for_current_story',
        'current_genre_url', 'current_genre_name_for_debug',
        'previous_story_url_in_state_for_chapters', 'previous_genre_url_in_state_for_stories',
        'completed_stories_in_current_genre'
    ]
    clear_specific_state_keys(crawl_state, keys_to_clear)
    return True

def log_final_summary(ultimately_failed_chapters_log: List[str]):
    # ... (Giữ nguyên)
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

def run_crawler():
    homepage_url, crawl_state = initialize_and_log_setup_with_state()
    all_genres_from_site = get_all_genres(homepage_url)
    ultimately_failed_chapters_log = []

    if not all_genres_from_site:
        logger.error("Không lấy được danh sách thể loại. Dừng script.")
        return

    start_genre_idx = crawl_state.get('current_genre_index', 0)
    genres_processed_count_this_run = 0

    for genre_idx, genre_data in enumerate(all_genres_from_site):
        if genre_idx < start_genre_idx:
            logger.info(f"Bỏ qua thể loại đã được xử lý (index {genre_idx} < start_idx {start_genre_idx}): {genre_data['name']}")
            continue

        if MAX_GENRES_TO_CRAWL is not None and genres_processed_count_this_run >= MAX_GENRES_TO_CRAWL:
            logger.info(f"Đã đạt giới hạn {MAX_GENRES_TO_CRAWL} thể loại cần xử lý trong lần chạy này. Dừng.")
            break
        
        crawl_state['current_genre_index'] = genre_idx
        
        genre_count_display = (
            f"Thể loại [{genres_processed_count_this_run + 1}/"
            f"{MAX_GENRES_TO_CRAWL if MAX_GENRES_TO_CRAWL is not None else (len(all_genres_from_site) - start_genre_idx)}]"
        )
        if process_genre_item(genre_data, genre_count_display, ultimately_failed_chapters_log, crawl_state):
            genres_processed_count_this_run += 1
            crawl_state['current_genre_index'] = genre_idx + 1
            save_crawl_state(crawl_state)
        else:
            logger.error(f"Xảy ra lỗi nghiêm trọng khi xử lý thể loại '{genre_data['name']}'. State hiện tại: {crawl_state}")
            crawl_state['current_genre_index'] = genre_idx + 1 
            keys_to_clear_on_genre_fail = [
                'current_genre_url', 'current_genre_name_for_debug',
                'current_story_index_in_genre', 'current_story_url',
                'current_story_title_for_debug',
                'processed_chapter_urls_for_current_story',
                'completed_stories_in_current_genre',
                'previous_genre_url_in_state_for_stories',
                'previous_story_url_in_state_for_chapters'
            ]
            clear_specific_state_keys(crawl_state, keys_to_clear_on_genre_fail)
            save_crawl_state(crawl_state)

    log_final_summary(ultimately_failed_chapters_log)
    
    final_genre_index_reached = crawl_state.get('current_genre_index', 0)
    all_genres_count = len(all_genres_from_site)
    
    should_clear_state = False
    if final_genre_index_reached >= all_genres_count: 
        should_clear_state = True
    elif MAX_GENRES_TO_CRAWL is not None and genres_processed_count_this_run >= MAX_GENRES_TO_CRAWL:
        if start_genre_idx + genres_processed_count_this_run >= all_genres_count:
            should_clear_state = True 
    
    if should_clear_state:
        logger.info("Tất cả các thể loại đã được xử lý theo cấu hình và/hoặc đã duyệt hết danh sách. Xóa file trạng thái.")
        clear_all_crawl_state()
    else:
        logger.info(f"Quá trình crawl chưa hoàn tất tất cả thể loại hoặc bị dừng bởi giới hạn. Trạng thái đã được lưu tại: {STATE_FILE}")

if __name__ == "__main__":
    run_crawler()
