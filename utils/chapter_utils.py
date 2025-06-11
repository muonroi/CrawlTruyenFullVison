import asyncio
import glob
import hashlib
import json
import os
from typing import Any, Dict, List
import re
from filelock import FileLock
from unidecode import unidecode
import aiofiles
from config.config import ASYNC_SEMAPHORE_LIMIT, HEADER_RE, LOCK, get_state_file
from utils.domain_utils import get_adapter_from_url
from utils.io_utils import  safe_write_file, safe_write_json
from utils.logger import logger
from utils.batch_utils import get_optimal_batch_size, smart_delay, split_batches
from utils.state_utils import save_crawl_state

SEM = asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)

BATCH_SEMAPHORE_LIMIT = 5

async def mark_dead_chapter(story_folder_path, ch_info):
    dead_path = os.path.join(story_folder_path, "dead_chapters.json")
    dead_list = []
    if os.path.exists(dead_path):
        with open(dead_path, "r", encoding="utf-8") as f:
            dead_list = json.load(f)
    # Đảm bảo không ghi trùng
    if any(x.get("url") == ch_info.get("url") for x in dead_list):
        return
    dead_list.append({
        "index": ch_info.get("index"),
        "title": ch_info.get("title"),
        "url": ch_info.get("url"),
        "reason": ch_info.get("reason", ""),
    })
    with open(dead_path, "w", encoding="utf-8") as f:
        json.dump(dead_list, f, ensure_ascii=False, indent=2)

def get_saved_chapters_files(story_folder_path: str) -> set:
    if not os.path.exists(story_folder_path):
        return set()
    files = glob.glob(os.path.join(story_folder_path, "*.txt"))
    return set(os.path.basename(f) for f in files)


async def crawl_missing_chapters_for_story(
    site_key,
    session,
    chapters,
    story_data_item,
    current_discovery_genre_data,
    story_folder_path,
    crawl_state,
    num_batches=10,
    state_file=None,
    adapter=None,
):
    total_chapters = story_data_item.get('total_chapters_on_site', len(chapters))
    retry_count = 0
    max_global_retry = 5  # tránh loop vô hạn

    while retry_count < max_global_retry:
        saved_files = get_saved_chapters_files(story_folder_path)
        if len(saved_files) >= total_chapters:
            logger.info(f"ĐÃ ĐỦ {len(saved_files)}/{total_chapters} chương cho '{story_data_item['title']}'")
            break

        missing_chapters = []
        for idx, ch in enumerate(chapters):
            fname_only = get_chapter_filename(ch['title'], idx + 1)
            fpath = os.path.join(story_folder_path, fname_only)
            if fname_only not in saved_files or not os.path.exists(fpath) or os.path.getsize(fpath) < 20:
                missing_chapters.append((idx, ch, fname_only))

        if not missing_chapters:
            break

        logger.warning(
            f"Truyện '{story_data_item['title']}' còn thiếu {len(missing_chapters)} chương (retry: {retry_count + 1})"
        )

        batch_size = int(os.getenv("BATCH_SIZE") or get_optimal_batch_size(len(missing_chapters)))
        num_batches_now = max(1, (len(missing_chapters) + batch_size - 1) // batch_size)
        batches = split_batches(missing_chapters, num_batches_now)
        logger.info(f"Crawl {len(missing_chapters)} chương với {num_batches_now} batch (batch size={batch_size})")

        async def crawl_batch(batch, batch_idx):
            successful, failed = set(), []
            sem = asyncio.Semaphore(BATCH_SEMAPHORE_LIMIT)
            tasks = []
            for i, (idx, ch, fname_only) in enumerate(batch):
                full_path = os.path.join(story_folder_path, fname_only)
                logger.info(f"[Batch {batch_idx}/{num_batches_now}] Đang crawl chương {idx+1}: {ch['title']}")

                async def wrapped(ch=ch, idx=idx, fname_only=fname_only, full_path=full_path):
                    async with sem:
                        try:
                            await asyncio.wait_for(
                                async_download_and_save_chapter(
                                    ch,
                                    story_data_item,
                                    current_discovery_genre_data,
                                    full_path,
                                    fname_only,
                                    "Crawl bù missing",
                                    f"{idx+1}/{len(chapters)}",
                                    crawl_state,
                                    successful,
                                    failed,
                                    idx,
                                    site_key=site_key,
                                    state_file=state_file,  # type: ignore
                                    adapter=adapter,
                                ),
                                timeout=300
                            )
                        except Exception as ex:
                            logger.error(f"[Batch {batch_idx}] Lỗi khi crawl chương {idx+1}: {ch['title']} - {ex}")
                            failed.append({
                                'chapter_data': ch,
                                'filename': full_path,
                                'filename_only': fname_only,
                                'original_idx': idx
                            })
                tasks.append(asyncio.create_task(wrapped()))
            await asyncio.gather(*tasks, return_exceptions=True)
            if state_file:
                await save_crawl_state(crawl_state, state_file)
            return successful, failed

        for batch_idx, batch in enumerate(batches):
            if not batch:
                continue
            await crawl_batch(batch, batch_idx + 1)

        retry_count += 1
        await smart_delay()

        if retry_count >= max_global_retry:
            logger.error(f"[FATAL] Vượt quá retry cho truyện {story_data_item['title']}, sẽ bỏ qua.")
            break

        if retry_count % 20 == 0:
            logger.error(
                f"[ALERT] Truyện '{story_data_item['title']}' còn các chương sau mãi chưa crawl được: {[f for _,_,f in missing_chapters]}"
            )

def clean_header_only_chapter(text: str):
    lines = text.splitlines()
    out = []
    skipping = True
    for line in lines:
        l = line.strip()
        if not l:
            continue
        if skipping and HEADER_RE.match(l):
            continue
        skipping = False
        out.append(line)
    return "\n".join(out).strip()

async def async_save_chapter_with_hash_check(filename, content: str):
    """
    Lưu file chương, kiểm tra hash để tránh ghi lại nếu nội dung không đổi.
    Trả về: "new" (chưa tồn tại, đã ghi), "unchanged" (tồn tại, giống hệt), "updated" (tồn tại, đã cập nhật).
    """
    hash_val = hashlib.sha256(content.encode('utf-8')).hexdigest()
    file_exists = os.path.exists(filename)
    if file_exists:
        async with aiofiles.open(filename, 'r', encoding='utf-8') as f:
            old_content = await f.read()
        old_hash = hashlib.sha256(old_content.encode('utf-8')).hexdigest()
        if old_hash == hash_val:
            logger.debug(f"Chương '{filename}' đã tồn tại với nội dung giống hệt, bỏ qua ghi lại.")
            return "unchanged"
        else:
            content = clean_header_only_chapter(content)
            await safe_write_file(filename, content)  
            logger.info(f"Chương '{filename}' đã được cập nhật do nội dung thay đổi.")
            return "updated"
    else:
        content = clean_header_only_chapter(content)
        await safe_write_file(filename, content)
        logger.info(f"Chương '{filename}' mới đã được lưu.")
        return "new"


def deduplicate_by_index(filename: str) -> None:
    """Remove duplicate chapter files that share the same index prefix."""
    base = os.path.basename(filename)
    m = re.match(r"(\d{4})[_.]", base)
    if not m:
        return
    prefix = m.group(1)
    folder = os.path.dirname(filename)
    pattern = os.path.join(folder, f"{prefix}*.txt")
    files = [f for f in glob.glob(pattern) if os.path.basename(f) != base]
    if not files:
        return
    # Keep the largest file (more content) and remove the rest
    files.append(filename)
    best = max(files, key=lambda f: os.path.getsize(f))
    for f in files:
        if f != best and os.path.exists(f):
            os.remove(f)
            logger.info(f"[DEDUP] Đã xoá file chương trùng: {os.path.basename(f)}")
    if best != filename and not os.path.exists(filename):
        os.rename(best, filename)
        logger.info(f"[DEDUP] Đổi tên {os.path.basename(best)} → {base}")

    


async def log_error_chapter(item, filename="error_chapters.json"):
    async with LOCK:
        arr = []
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    arr = json.load(f)
            except Exception:
                arr = []
        arr.append(item)
        await safe_write_json(filename,arr)



async def queue_failed_chapter(chapter_data, filename='chapter_retry_queue.json'):
    """Ghi chương lỗi vào queue JSON để retry."""
    path = os.path.join(os.getcwd(), filename)
    # Đọc danh sách cũ
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        print(f"[ERROR] File {path} bị lỗi hoặc rỗng, sẽ tạo lại file mới.")
        data = []
    for item in data:
        if item.get("url") == chapter_data.get("url"):
            return
    data.append(chapter_data)  # type: ignore
    await safe_write_json(filename,data)

async def async_download_and_save_chapter(
    chapter_info: Dict[str, Any],
    story_data_item: Dict[str, Any],
    current_discovery_genre_data: Dict[str, Any],
    chapter_filename_full_path: str,
    chapter_filename_only: str,
    pass_description: str,
    chapter_display_idx_log: str,
    crawl_state: Dict[str, Any],
    successfully_saved: set,
    failed_list: List[Dict[str, Any]],
    original_idx: int = 0,
    site_key: str = "unknown",
    state_file: str = None,    # type: ignore
    adapter=None,# type: ignore
) -> None:
    url = chapter_info['url']
    logger.info(f"        {pass_description} - Chương {chapter_display_idx_log}: Đang tải '{chapter_info['title']}' ({url})")
    async with SEM:
        content = await adapter.get_chapter_content(url, chapter_info['title'], site_key)# type: ignore

    if content:
        try:
            category_name = get_category_name(story_data_item, current_discovery_genre_data)
            # Gộp nội dung chuẩn file .txt
            chapter_content = (
                f"Nguồn: {url}\n\nTruyện: {story_data_item['title']}\n"
                f"Thể loại: {category_name}\n"
                f"Chương: {chapter_info['title']}\n\n"
                f"{content}"
            )

            # Sử dụng hàm hash check
            save_result = await async_save_chapter_with_hash_check(chapter_filename_full_path, chapter_content)
            deduplicate_by_index(chapter_filename_full_path)
            if save_result == "new":
                logger.info(f"          Đã lưu ({pass_description}): {chapter_filename_only}")
            elif save_result == "unchanged":
                logger.debug(f"          Chương '{chapter_filename_only}' đã tồn tại, không thay đổi nội dung.")
            elif save_result == "updated":
                logger.info(f"          Chương '{chapter_filename_only}' đã được cập nhật do nội dung thay đổi.")

            if save_result in ("new", "updated", "unchanged"):
                successfully_saved.add(chapter_filename_full_path)
                processed = crawl_state.get('processed_chapter_urls_for_current_story', [])
                if url not in processed:
                    processed.append(url)
                    crawl_state['processed_chapter_urls_for_current_story'] = processed
                    file_to_save = state_file or get_state_file(site_key)
                    await save_crawl_state(crawl_state, file_to_save)
        except Exception as e:
            logger.error(f"          Lỗi lưu '{chapter_filename_only}': {e}")
            await log_error_chapter({
                "story_title": story_data_item['title'],
                "chapter_title": chapter_info['title'],
                "chapter_url": chapter_info['url'],
                "error_msg": str(e)
            })
            # --- Queue retry chương lỗi ---
            await queue_failed_chapter({
                "chapter_url": chapter_info['url'],
                "chapter_title": chapter_info['title'],
                "story_title": story_data_item['title'],
                "story_url": story_data_item['url'],
                "filename": chapter_filename_full_path,
                'site': site_key,
                "reason": f"Lỗi lưu: {e}"
            })
            failed_list.append({
                'chapter_data': chapter_info,
                'filename': chapter_filename_full_path,
                'filename_only': chapter_filename_only,
                'original_idx': original_idx
            })
                # Đánh dấu dead nếu đã quá số lần retry (ví dụ: 3)
            try:
                await mark_dead_chapter(os.path.dirname(chapter_filename_full_path), {
                    "index": original_idx,
                    "title": chapter_info['title'],
                    "url": chapter_info['url'],
                    "reason": "empty content"
                })
            except Exception as ex:
                logger.warning(f"Lỗi khi ghi dead_chapters.json: {ex}")
    else:
        logger.warning(f"          Không lấy được nội dung '{chapter_info['title']}'")
        await log_error_chapter({
            "story_title": story_data_item['title'],
            "chapter_title": chapter_info['title'],
            "chapter_url": chapter_info['url'],
            "error_msg": "Không lấy được nội dung"
        })
        # --- Queue retry chương lỗi ---
        await queue_failed_chapter({
            "chapter_url": chapter_info['url'],
            "chapter_title": chapter_info['title'],
            "story_title": story_data_item['title'],
            "story_url": story_data_item['url'],
            "filename": chapter_filename_full_path,
            'site': site_key,
            "reason": "Không lấy được nội dung"
        })
        failed_list.append({
            'chapter_data': chapter_info,
            'filename': chapter_filename_full_path,
            'filename_only': chapter_filename_only,
            'original_idx': None
        })

async def process_chapter_batch(
    session, batch_chapters, story_data_item, current_discovery_genre_data,
    story_folder_path, crawl_state, batch_idx, total_batch, adapter, site_key
):
    successful, failed = set(), []
    already_crawled = set(crawl_state.get('processed_chapter_urls_for_current_story', []))
    for idx, ch in enumerate(batch_chapters):
        if ch['url'] in already_crawled:
            continue

        fname_only = get_chapter_filename(ch['title'], ch['idx'])
        full_path = os.path.join(story_folder_path, fname_only)

        await async_download_and_save_chapter(
            ch,
            story_data_item,
            current_discovery_genre_data,
            full_path,
            fname_only,
            f"Batch {batch_idx+1}/{total_batch}",
            f"{ch['idx']+1}",
            crawl_state,
            successful,
            failed,
            original_idx=ch['idx'],
            site_key=site_key,
            state_file=get_state_file(site_key),
            adapter=adapter,
        )
        await smart_delay()
    return successful, failed


def get_category_name(story_data_item, current_discovery_genre_data):
    if 'categories' in story_data_item and isinstance(story_data_item['categories'], list):
        if story_data_item['categories']:
            return story_data_item['categories'][0].get('name', '')
    return current_discovery_genre_data.get('name', '')

def get_existing_chapter_nums(story_folder):
    files = [f for f in os.listdir(story_folder) if f.endswith('.txt')]
    chapter_nums = set()
    for f in files:
        # 0001_Tên chương.txt => lấy 0001
        num = f.split('_')[0]
        chapter_nums.add(num)
    return chapter_nums

def get_missing_chapters(chapters, existing_nums):
    # chapters: list từ web, existing_nums: set 0001, 0002...
    dead_chapters_path = os.path.join(story_folder, "dead_chapters.json")
    dead_urls = set()
    if os.path.exists(dead_chapters_path):
        with open(dead_chapters_path, "r", encoding="utf-8") as f:
            dead = json.load(f)
            dead_urls = set(x["url"] for x in dead if "url" in x)
    missing = []
    for idx, ch in enumerate(chapter_items):
        expected_file = ch.get("file")
        if not expected_file:
            real_num = ch.get("index", idx+1)
            title = ch.get("title", "") or ""
            expected_file = get_chapter_filename(title, real_num)
        file_path = os.path.join(story_folder, expected_file)
        # THÊM:
        if ch.get("url") in dead_urls:
            continue  # Bỏ qua chương đã được đánh dấu dead
        if expected_file not in existing_files or not os.path.exists(file_path) or os.path.getsize(file_path) < 20:
            ch_for_missing = {**ch, "idx": idx}
            missing.append(ch_for_missing)
    return missing

def slugify_title(title: str) -> str:
    s = unidecode(title)
    s = re.sub(r'[^\w\s-]', '', s.lower())  # bỏ ký tự đặc biệt, lowercase
    s = re.sub(r'[\s]+', '-', s)            # khoảng trắng thành dấu -
    return s.strip('-_')


def count_txt_files(story_folder_path):
    if not os.path.exists(story_folder_path):
        return 0
    return len([f for f in os.listdir(story_folder_path) if f.endswith('.txt')])


async def async_save_chapter_with_lock(filename, content):
    lockfile = filename + ".lock"
    lock = FileLock(lockfile, timeout=60)
    with lock:
        await async_save_chapter_with_hash_check(filename, content)


def extract_real_chapter_number(title: str) -> int | None:
    """
    Trích xuất số chương thực tế từ tiêu đề chương.
    Hỗ trợ các định dạng: Chương 123, Chapter 456, 001. Tên chương, ...
    """
    if not title:
        return None

    match = re.search(r'(?:chương|chapter|chap|ch)\s*[:\-]?\s*(\d{1,5})', title, re.IGNORECASE)
    if match:
        return int(match.group(1))

    match = re.match(r'^\s*(\d{1,5})[\.\-\s]', title)
    if match:
        return int(match.group(1))

    return None

def get_chapter_filename(title: str, real_num: int) -> str:
    """
    Tạo tên file chương chuẩn: 0001_ten-chuong.txt (không dấu, cách bằng -)
    """
    from unidecode import unidecode
    s = unidecode(title)
    s = re.sub(r'[^\w\s-]', '', s.lower())
    s = re.sub(r'\s+', '-', s)
    clean_title = s.strip('-_') or "untitled"
    return f"{real_num:04d}_{clean_title}.txt"


def export_chapter_metadata_sync(story_folder, chapters) -> List[Dict[str, Any]]:
    """
    Xuất lại file chapter_metadata.json. Nếu thiếu file chương tương ứng thì đánh dấu missing để crawl lại.
    Trả về danh sách chương missing để xử lý sau.
    """
    files = [f for f in os.listdir(story_folder) if f.endswith('.txt')]
    files_set = set(files)
    valid_chapters = []
    missing_chapters = []

    for idx, ch in enumerate(chapters):
        real_num = ch.get("index", idx + 1)
        title = ch.get("title", "")
        url = ch.get("url", "")
        expected_name = get_chapter_filename(title, real_num)
        expected_path = os.path.join(story_folder, expected_name)

        if expected_name not in files_set or not os.path.exists(expected_path):
            # Tìm theo prefix số chương
            prefix = f"{real_num:04d}_"
            found = next((f for f in files if f.startswith(prefix)), None)
            if found and found != expected_name:
                os.rename(os.path.join(story_folder, found), expected_path)
                logger.info(f"[RENAME][META] {found} -> {expected_name}")
                files_set.remove(found)
                files_set.add(expected_name)
            elif not found:
                missing_chapters.append({
                    "index": real_num,
                    "title": title,
                    "url": url
                })
                continue  # bỏ qua không thêm vào valid

        ch["file"] = expected_name
        ch["index"] = real_num
        valid_chapters.append(ch)

    # Lưu lại metadata chuẩn
    chapter_meta_path = os.path.join(story_folder, "chapter_metadata.json")
    with open(chapter_meta_path, "w", encoding="utf-8") as f:
        json.dump(valid_chapters, f, ensure_ascii=False, indent=4)
    logger.info(f"[META] Exported chapter_metadata.json ({len(valid_chapters)} chương) for {os.path.basename(story_folder)}")

    # Nếu có missing, ghi ra file để trigger crawl lại
    if missing_chapters:
        missing_path = os.path.join(story_folder, "missing_from_metadata.json")
        with open(missing_path, "w", encoding="utf-8") as f:
            json.dump(missing_chapters, f, ensure_ascii=False, indent=2)
        logger.warning(f"[MISSING][META] Phát hiện {len(missing_chapters)} chương bị thiếu file, đã ghi vào: {missing_path}")

    return missing_chapters



def remove_chapter_number_from_title(title):
    # Tách đầu "Chương xx: " hoặc "Chap xx - ", "Chapter xx - " ... (các biến thể phổ biến)
    new_title = re.sub(
        r"^(chương|chapter|chap|ch)\s*\d+\s*[:\-\.]?\s*", "",
        title,
        flags=re.IGNORECASE
    )
    return new_title.strip()

def get_actual_chapters_for_export(story_folder):
    """
    Quét thư mục lấy danh sách file chương thực tế,
    tách số chương (index), title, tên file cho chuẩn metadata.
    """
    chapters = []
    files = [f for f in os.listdir(story_folder) if f.endswith('.txt')]
    files.sort()  # Đảm bảo theo thứ tự tăng dần

    for fname in files:
        # Định dạng 0001_Tên chương.txt hoặc 0001.Tên chương.txt
        m = re.match(r"(\d{4})[_\.](.*)\.txt", fname)
        if m:
            index = int(m.group(1))
            raw_title = m.group(2).strip()
            title = remove_chapter_number_from_title(raw_title)
        else:
            # Nếu không đúng định dạng, fallback
            index = len(chapters) + 1
            raw_title = fname[:-4]  # bỏ .txt
            title = remove_chapter_number_from_title(raw_title)
        chapters.append({
            "index": index,
            "title": title,
            "file": fname,
            "url": ""  # Nếu lấy được thì bổ sung thêm, còn không để rỗng
        })
    return chapters

async def get_real_total_chapters(metadata, adapter):
    # Ưu tiên lấy từ sources nếu có
    if metadata.get("sources"):
        for source in metadata["sources"]:
            url = source.get("url")
            adapter, site_key = get_adapter_from_url(url,adapter)
            if not adapter or not url:
                continue
            chapters = await adapter.get_chapter_list(url, metadata.get("title"), site_key)
            if chapters and len(chapters) > 0:
                return len(chapters)
    # Nếu không có sources, fallback dùng url + site_key hiện tại trong metadata
    url = metadata.get("url")
    site_key = metadata.get("site_key")
    if url and site_key:
        chapters = await adapter.get_chapter_list(story_url=url, story_title=metadata.get("title"), site_key=site_key)
        if chapters:
            return len(chapters)
    return 0