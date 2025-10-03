import asyncio
import glob
import hashlib
import json
import os
import datetime
import math
from typing import Any, Dict, List, Optional, Tuple
import re
import unicodedata
from bs4 import BeautifulSoup
from filelock import FileLock
from unidecode import unidecode
import aiofiles
from adapters.base_site_adapter import BaseSiteAdapter
from config.config import (
    ASYNC_SEMAPHORE_LIMIT,
    HEADER_RE,
    LOCK,
    get_state_file,
    MAX_CHAPTER_RETRY,
    MAX_CHAPTERS_PER_STORY,
    MAX_CHAPTER_PAGES_TO_CRAWL,
)
from utils.domain_utils import get_adapter_from_url
from utils.io_utils import  safe_write_file, safe_write_json
from utils.logger import logger
from utils.batch_utils import get_optimal_batch_size, smart_delay, split_batches
from utils.anti_bot import is_anti_bot_content
from utils.state_utils import save_crawl_state
from utils.errors import CrawlError
SEM = asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)

BATCH_SEMAPHORE_LIMIT = 5


def _html_fragment_to_text(content: str) -> str:
    """Convert an HTML fragment to normalized plain text paragraphs."""
    if not content:
        return ''

    soup = BeautifulSoup(content, 'html.parser')

    # Replace <br> with newline markers so they are preserved on get_text
    for br in soup.find_all('br'):
        br.replace_with('\n')

    text = soup.get_text(separator='\n')

    lines: List[str] = []
    for raw_line in text.splitlines():
        normalized = raw_line.replace('\xa0', ' ').strip()
        if not normalized:
            if lines and lines[-1] != '':
                lines.append('')
            continue
        lines.append(normalized)

    return '\n'.join(lines).strip()

async def mark_dead_chapter(story_folder_path, ch_info):
    dead_path = os.path.join(story_folder_path, "dead_chapters.json")
    dead_list = []
    if os.path.exists(dead_path):
        with open(dead_path, "r", encoding="utf-8") as f:
            dead_list = json.load(f)
    # Dam bao khong ghi trung
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
    target_indexes: set[int] | None = None,
    chapter_limit: Optional[int] = None,
):
    chapters = chapters or []

    total_hint = story_data_item.get('total_chapters_on_site')
    total_hint = total_hint if isinstance(total_hint, int) and total_hint >= 0 else None

    limit_candidates: List[int] = []
    if chapters:
        limit_candidates.append(len(chapters))
    if total_hint:
        limit_candidates.append(total_hint)
    if isinstance(chapter_limit, int) and chapter_limit >= 0:
        limit_candidates.append(chapter_limit)
    if MAX_CHAPTERS_PER_STORY:
        limit_candidates.append(MAX_CHAPTERS_PER_STORY)

    if MAX_CHAPTER_PAGES_TO_CRAWL:
        per_page_hint = None
        try:
            if adapter and hasattr(adapter, "get_chapters_per_page_hint"):
                per_page_hint = int(adapter.get_chapters_per_page_hint())
        except Exception:
            per_page_hint = None
        if not per_page_hint or per_page_hint <= 0:
            per_page_hint = 100
        limit_candidates.append(MAX_CHAPTER_PAGES_TO_CRAWL * per_page_hint)

    effective_limit = min(limit_candidates) if limit_candidates else len(chapters)
    effective_limit = max(0, effective_limit)

    if chapters and effective_limit < len(chapters):
        logger.info(
            f"[LIMIT] Chỉ crawl {effective_limit}/{len(chapters)} chương cho '{story_data_item.get('title')}' theo cấu hình."
        )

    chapters_to_process = chapters[:effective_limit]
    total_chapters = effective_limit if effective_limit else 0

    if total_chapters == 0:
        logger.info(
            f"[LIMIT] Không có chương nào cần crawl cho '{story_data_item.get('title')}' sau khi áp dụng giới hạn."
        )
        return 0

    retry_count = 0
    max_global_retry = MAX_CHAPTER_RETRY
    fail_counts: Dict[str, int] = {}
    permanent_missing: List[Dict[str, Any]] = []
    skip_file = os.path.join(story_folder_path, "skipped_chapters.json")
    try:
        with open(skip_file, "r", encoding="utf-8") as f:
            skipped_chapters = json.load(f)
    except Exception:
        skipped_chapters = {}

    normal_rounds = max(0, max_global_retry - 1)

    target_zero_indexes: set[int] | None = None
    if target_indexes:
        target_zero_indexes = set()
        for raw in target_indexes:
            try:
                target_zero_indexes.add(int(raw))
            except (TypeError, ValueError):
                continue

    def matches_target(idx: int, ch: Dict[str, Any]) -> bool:
        if target_zero_indexes is None:
            return True

        candidates = {idx}
        ch_idx = ch.get("idx")
        if isinstance(ch_idx, int):
            candidates.add(ch_idx)

        ch_index = ch.get("index")
        if isinstance(ch_index, int):
            candidates.add(ch_index - 1)

        real_num = extract_real_chapter_number(ch.get("title", ""))
        if isinstance(real_num, int):
            candidates.add(real_num - 1)

        return any(c in target_zero_indexes for c in candidates)

    async def crawl_batch(batch, batch_idx, num_batches_now):
        successful, failed = set(), []
        sem = asyncio.Semaphore(BATCH_SEMAPHORE_LIMIT)
        tasks = []
        for i, (idx, ch, fname_only) in enumerate(batch):
            full_path = os.path.join(story_folder_path, fname_only)
            logger.info(f"[Batch {batch_idx}/{num_batches_now}] Dang crawl chuong {idx+1}: {ch['title']}")

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
                                "Crawl bu missing",
                                f"{idx+1}/{len(chapters_to_process)}",
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
                        logger.error(f"[Batch {batch_idx}] Loi khi crawl chuong {idx+1}: {ch['title']} - {ex}")
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

    while retry_count < normal_rounds:
        saved_files = get_saved_chapters_files(story_folder_path)
        if len(saved_files) >= total_chapters:
            logger.info(f"DA DU {len(saved_files)}/{total_chapters} chuong cho '{story_data_item['title']}'")
            break

        missing_chapters = []
        for idx, ch in enumerate(chapters_to_process):
            if not matches_target(idx, ch):
                continue
            fname_only = get_chapter_filename(ch['title'], idx + 1)
            if fail_counts.get(fname_only, 0) >= MAX_CHAPTER_RETRY:
                continue
            if fname_only in skipped_chapters:
                continue
            fpath = os.path.join(story_folder_path, fname_only)
            if fname_only not in saved_files or not os.path.exists(fpath) or os.path.getsize(fpath) < 20:
                missing_chapters.append((idx, ch, fname_only))

        if not missing_chapters:
            break

        logger.warning(
            f"Truyen '{story_data_item['title']}' con thieu {len(missing_chapters)} chuong (retry: {retry_count + 1})"
        )

        batch_size = int(os.getenv("BATCH_SIZE") or get_optimal_batch_size(len(missing_chapters)))
        num_batches_now = max(1, (len(missing_chapters) + batch_size - 1) // batch_size)
        batches = split_batches(missing_chapters, num_batches_now)
        logger.info(f"Crawl {len(missing_chapters)} chuong voi {num_batches_now} batch (batch size={batch_size})")

        for batch_idx, batch in enumerate(batches):
            if not batch:
                continue
            _, failed = await crawl_batch(batch, batch_idx + 1, num_batches_now)
            for item in failed:
                fname_only = item.get('filename_only')
                fail_counts[fname_only] = fail_counts.get(fname_only, 0) + 1
                if fail_counts[fname_only] >= MAX_CHAPTER_RETRY:
                    await mark_dead_chapter(story_folder_path, {
                        "index": (item.get('original_idx') or 0) + 1,
                        "title": item['chapter_data'].get('title'),
                        "url": item['chapter_data'].get('url'),
                        "reason": "max_retry_reached",
                    })
                    permanent_missing.append({
                        "index": (item.get('original_idx') or 0) + 1,
                        "title": item['chapter_data'].get('title'),
                        "url": item['chapter_data'].get('url'),
                    })
                    skipped_chapters[fname_only] = {
                        "reason": "max_retry_reached",
                        "retry": fail_counts[fname_only],
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }

        retry_count += 1
        await smart_delay()

        if retry_count >= max_global_retry:
            logger.error(f"[FATAL] Vuot qua retry cho truyen {story_data_item['title']}, se bo qua.")
            break

        if retry_count % 20 == 0:
            logger.error(
                f"[ALERT] Truyen '{story_data_item['title']}' con cac chuong sau mai chua crawl duoc: {[f for _,_,f in missing_chapters]}"
            )

    # --- Final retry for remaining failed chapters ---
    saved_files = get_saved_chapters_files(story_folder_path)
    final_missing = []
    for idx, ch in enumerate(chapters_to_process):
        if not matches_target(idx, ch):
            continue
        fname_only = get_chapter_filename(ch['title'], idx + 1)
        if fname_only in skipped_chapters:
            continue
        if fname_only in saved_files:
            continue
        final_missing.append((idx, ch, fname_only))

    if final_missing:
        logger.warning(
            f"[FINAL] Thu lai {len(final_missing)} chuong loi cuoi cung"
        )
        batch_size = int(
            os.getenv("BATCH_SIZE") or get_optimal_batch_size(len(final_missing))
        )
        num_batches_now = max(1, (len(final_missing) + batch_size - 1) // batch_size)
        batches = split_batches(final_missing, num_batches_now)
        for batch_idx, batch in enumerate(batches):
            _, failed = await crawl_batch(batch, batch_idx + 1, num_batches_now)
            for item in failed:
                fname_only = item.get('filename_only')
                fail_counts[fname_only] = fail_counts.get(fname_only, 0) + 1
                skipped_chapters[fname_only] = {
                    "reason": "final_retry_fail",
                    "retry": fail_counts[fname_only],
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                permanent_missing.append({
                    "index": (item.get('original_idx') or 0) + 1,
                    "title": item['chapter_data'].get('title'),
                    "url": item['chapter_data'].get('url'),
                })

    if permanent_missing:
        report_path = os.path.join(story_folder_path, "missing_permanent.json")
        try:
            if os.path.exists(report_path):
                with open(report_path, "r", encoding="utf-8") as f:
                    old = json.load(f)
            else:
                old = []
        except Exception:
            old = []
        for item in permanent_missing:
            if not any(o.get("url") == item.get("url") for o in old):
                old.append(item)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(old, f, ensure_ascii=False, indent=2)
        logger.warning(f"[REPORT] Da ghi {len(permanent_missing)} chuong loi vinh vien vao {report_path}")

    if skipped_chapters:
        with open(skip_file, "w", encoding="utf-8") as f:
            json.dump(skipped_chapters, f, ensure_ascii=False, indent=2)

    fail_total = len(skipped_chapters)
    if fail_total >= math.ceil(total_chapters * 2 / 3):
        from utils.skip_manager import mark_story_as_skipped
        mark_story_as_skipped(story_data_item, "too_many_failed_chapters")

    return total_chapters

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
    Luu file chuong, kiem tra hash de tranh ghi lai neu noi dung khong doi.
    Tra ve: "new" (chua ton tai, da ghi), "unchanged" (ton tai, giong het), "updated" (ton tai, da cap nhat).
    """
    hash_val = hashlib.sha256(content.encode('utf-8')).hexdigest()
    file_exists = os.path.exists(filename)
    if file_exists:
        async with aiofiles.open(filename, 'r', encoding='utf-8') as f:
            old_content = await f.read()
        old_hash = hashlib.sha256(old_content.encode('utf-8')).hexdigest()
        if old_hash == hash_val:
            logger.debug(f"Chuong '{filename}' da ton tai voi noi dung giong het, bo qua ghi lai.")
            return "unchanged"
        else:
            content = clean_header_only_chapter(content)
            await safe_write_file(filename, content)  
            logger.info(f"Chuong '{filename}' da duoc cap nhat do noi dung thay doi.")
            return "updated"
    else:
        content = clean_header_only_chapter(content)
        await safe_write_file(filename, content)
        logger.info(f"Chuong '{filename}' moi da duoc luu.")
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
            logger.info(f"[DEDUP] Da xoa file chuong trung: {os.path.basename(f)}")
    if best != filename and not os.path.exists(filename):
        os.rename(best, filename)
        logger.info(f"[DEDUP] Doi ten {os.path.basename(best)} - {base}")

    


async def log_error_chapter(item, filename="error_chapters.json"):
    async with LOCK:
        arr = []
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    arr = json.load(f)
            except Exception:
                arr = []
        item.setdefault("error_time", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        arr.append(item)
        await safe_write_json(filename,arr)



async def queue_failed_chapter(chapter_data):
    """Ghi chuong loi vao Kafka topic de retry."""
    from kafka.kafka_producer import send_job
    
    # Them job type de dispatcher co the nhan dien
    job_to_send = chapter_data.copy()
    job_to_send['type'] = 'retry_chapter'
    
    await send_job(job_to_send)

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
    logger.info(f"        {pass_description} - Chuong {chapter_display_idx_log}: Dang tai '{chapter_info['title']}' ({url})")
    async with SEM:
        content = await adapter.get_chapter_content(url, chapter_info['title'], site_key)# type: ignore

    if content and not is_anti_bot_content(content):
        try:
            category_name = get_category_name(story_data_item, current_discovery_genre_data)
            normalized_content = _html_fragment_to_text(content)
            if not normalized_content:
                raise ValueError("Empty content after stripping HTML")
            # Gop noi dung chuan file .txt
            chapter_content = (
                f"Nguon: {url}\n\nTruyen: {story_data_item['title']}\n"
                f"The loai: {category_name}\n"
                f"Chuong: {chapter_info['title']}\n\n"
                f"{normalized_content}"
            )

            # Su dung ham hash check
            save_result = await async_save_chapter_with_hash_check(chapter_filename_full_path, chapter_content)
            deduplicate_by_index(chapter_filename_full_path)
            if save_result == "new":
                logger.info(f"          Da luu ({pass_description}): {chapter_filename_only}")
            elif save_result == "unchanged":
                logger.debug(f"          Chuong '{chapter_filename_only}' da ton tai, khong thay doi noi dung.")
            elif save_result == "updated":
                logger.info(f"          Chuong '{chapter_filename_only}' da duoc cap nhat do noi dung thay doi.")

            if save_result in ("new", "updated", "unchanged"):
                successfully_saved.add(chapter_filename_full_path)
                processed = crawl_state.get('processed_chapter_urls_for_current_story', [])
                if url not in processed:
                    processed.append(url)
                    crawl_state['processed_chapter_urls_for_current_story'] = processed
                    file_to_save = state_file or get_state_file(site_key)
                    await save_crawl_state(crawl_state, file_to_save)
        except Exception as e:
            logger.error(f"          Loi luu '{chapter_filename_only}': {e}")
            await log_error_chapter({
                "story_title": story_data_item['title'],
                "chapter_title": chapter_info['title'],
                "chapter_url": chapter_info['url'],
                "error_msg": str(e)
            })
            # --- Queue retry chuong loi ---
            await queue_failed_chapter({
                "chapter_url": chapter_info['url'],
                "chapter_title": chapter_info['title'],
                "story_title": story_data_item['title'],
                "story_url": story_data_item['url'],
                "filename": chapter_filename_full_path,
                'site': site_key,
                "reason": f"Loi luu: {e}",
                "error_type": CrawlError.WRITE_FAIL.value,
            })
            failed_list.append({
                'chapter_data': chapter_info,
                'filename': chapter_filename_full_path,
                'filename_only': chapter_filename_only,
                'original_idx': original_idx
            })
                # Danh dau dead neu da qua so lan retry (vi du: 3)
            try:
                await mark_dead_chapter(os.path.dirname(chapter_filename_full_path), {
                    "index": original_idx,
                    "title": chapter_info['title'],
                    "url": chapter_info['url'],
                    "reason": "empty content"
                })
            except Exception as ex:
                logger.warning(f"Loi khi ghi dead_chapters.json: {ex}")
    else:
        if content and is_anti_bot_content(content):
            logger.warning(f"          Noi dung chuong bi phat hien anti-bot '{chapter_info['title']}'")
            reason = 'anti-bot'
        else:
            logger.warning(f"          Khong lay duoc noi dung '{chapter_info['title']}'")
            reason = 'Khong lay duoc noi dung'
        await log_error_chapter({
            "story_title": story_data_item['title'],
            "chapter_title": chapter_info['title'],
            "chapter_url": chapter_info['url'],
            "error_msg": reason,
        })
        # --- Queue retry chuong loi ---
        await queue_failed_chapter({
            "chapter_url": chapter_info['url'],
            "chapter_title": chapter_info['title'],
            "story_title": story_data_item['title'],
            "story_url": story_data_item['url'],
            "filename": chapter_filename_full_path,
            'site': site_key,
            "reason": reason,
            "error_type": CrawlError.ANTI_BOT.value if reason == 'anti-bot' else CrawlError.UNKNOWN.value,
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
        # 0001_Ten chuong.txt => lay 0001
        num = f.split('_')[0]
        chapter_nums.add(num)
    return chapter_nums

async def get_max_page_by_playwright(url, site_key=None):
    from scraper import _make_request_playwright
    resp = await _make_request_playwright(
        url,
        site_key or "truyenyy",
        wait_for_selector="ul.flex.flex-wrap",
    )
    if not resp or not getattr(resp, "text", None):
        return 1
    from lxml import html as lxml_html
    tree = lxml_html.fromstring(resp.text)
    li_list = tree.xpath(
        "/html/body/main/div[2]/main/div[2]/div/div/div[2]/div[2]/div[1]/div/div[1]/ul/li"
    )
    max_page = 1
    for li in reversed(li_list):
        text = "".join(li.xpath(".//a/text()")).strip()
        if text.isdigit():
            max_page = int(text)
            break
    return max_page


def _load_chapter_items(story_folder: str, chapters: list[dict]) -> list[dict]:
    """Helper that loads chapter metadata from disk if present."""

    chapter_meta_path = os.path.join(story_folder, "chapter_metadata.json")
    if os.path.exists(chapter_meta_path):
        try:
            with open(chapter_meta_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
        except Exception:  # pragma: no cover - best effort loading
            logger.warning(
                "[MISSING] Không đọc được chapter_metadata.json tại %s, fallback sang dữ liệu crawl.",
                story_folder,
            )
    return chapters or []


def _collect_existing_chapter_numbers(story_folder: str) -> set[int]:
    """Collect the chapter indexes that already exist as .txt files."""

    existing_numbers: set[int] = set()
    for fname in os.listdir(story_folder):
        if not fname.endswith(".txt"):
            continue
        match = re.match(r"(\d+)", fname)
        if not match:
            continue
        try:
            existing_numbers.add(int(match.group(1)))
        except ValueError:
            continue
    return existing_numbers


def _collect_dead_chapters(story_folder: str) -> tuple[set[str], set[int]]:
    """Return the URLs and indexes that are marked as dead."""

    dead_urls: set[str] = set()
    dead_indexes: set[int] = set()
    dead_path = os.path.join(story_folder, "dead_chapters.json")
    if not os.path.exists(dead_path):
        return dead_urls, dead_indexes

    try:
        with open(dead_path, "r", encoding="utf-8") as f:
            dead_list = json.load(f)
        if not isinstance(dead_list, list):
            return dead_urls, dead_indexes
    except Exception:  # pragma: no cover - ignore malformed files
        return dead_urls, dead_indexes

    for item in dead_list:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        if url:
            dead_urls.add(url)
        idx = item.get("index")
        if isinstance(idx, int):
            dead_indexes.add(idx)
    return dead_urls, dead_indexes


def _normalize_title_for_matching(title: str) -> str:
    """Normalize a chapter title so that two variants can be compared reliably."""

    cleaned = remove_title_number(title or "")
    cleaned = unidecode(cleaned)
    cleaned = cleaned.lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    return cleaned.strip()


def _resolve_numbering_offset(
    chapter_items: list[dict],
    chapters: list[dict],
    existing_numbers: set[int],
) -> tuple[int, bool]:
    """Return the best offset and a flag indicating whether the source should be skipped."""

    canonical_map: dict[str, set[int]] = {}
    for idx, ch in enumerate(chapter_items or []):
        if not isinstance(ch, dict):
            continue
        title = ch.get("title", "") or ""
        normalized = _normalize_title_for_matching(title)
        if not normalized:
            continue
        real_num = ch.get("index") if isinstance(ch.get("index"), int) else extract_real_chapter_number(title)
        if not isinstance(real_num, int):
            real_num = idx + 1
        canonical_map.setdefault(normalized, set()).add(real_num)

    if not canonical_map:
        # Không có dữ liệu chuẩn để so sánh -> không thể tính offset nhưng cũng không cần skip.
        return 0, False

    offset_counter: dict[int, int] = {}
    match_count = 0

    for idx, ch in enumerate(chapters or []):
        if not isinstance(ch, dict):
            continue
        title = ch.get("title", "") or ""
        normalized = _normalize_title_for_matching(title)
        if not normalized or normalized not in canonical_map:
            continue
        real_num = ch.get("index") if isinstance(ch.get("index"), int) else extract_real_chapter_number(title)
        if not isinstance(real_num, int):
            real_num = idx + 1
        match_count += 1
        for canonical_num in canonical_map[normalized]:
            offset = canonical_num - real_num
            offset_counter[offset] = offset_counter.get(offset, 0) + 1

    if not offset_counter:
        # Không match được title nào. Nếu đã có chương local -> nguồn này nguy hiểm, nên skip.
        if existing_numbers:
            return 0, True
        return 0, False

    best_offset, best_count = max(offset_counter.items(), key=lambda item: item[1])
    total_matched = sum(offset_counter.values())
    second_best = max(
        (count for offset, count in offset_counter.items() if offset != best_offset),
        default=0,
    )

    # Đánh giá mức độ tin cậy.
    confidence = best_count / total_matched if total_matched else 0
    confident = best_count >= 3 or confidence >= 0.6 or match_count <= 2

    # Nếu có hơn một offset ứng viên và chênh lệch quá nhỏ -> coi như không đáng tin.
    if best_offset != 0 and second_best and (best_count - second_best) <= 1:
        confident = False

    if not confident and existing_numbers:
        return best_offset, True

    return best_offset, False


def _adjust_real_number(raw_num: int | None, fallback: int, offset: int) -> int:
    """Utility to apply offset and ensure positive numbering."""

    real_num = raw_num if isinstance(raw_num, int) else fallback
    adjusted = real_num + offset
    return max(1, adjusted)


def get_missing_chapters(story_folder: str, chapters: list[dict], site_key: str) -> list[dict]:
    """Identify chapters that are missing locally based on sequential numbering."""

    chapter_items = _load_chapter_items(story_folder, chapters)
    if not chapter_items:
        return []

    existing_numbers = _collect_existing_chapter_numbers(story_folder)
    dead_urls, dead_indexes = _collect_dead_chapters(story_folder)

    offset, should_skip = _resolve_numbering_offset(chapter_items, chapters, existing_numbers)
    if should_skip:
        logger.warning(
            "[MISMATCH] Bỏ qua nguồn chương vì số chương không đồng nhất với dữ liệu đã crawl." \
            " (story=%s)",
            os.path.basename(story_folder),
        )
        return []

    # Thu thập thông tin chương từ dữ liệu nguồn để xác nhận chương hợp lệ.
    available_by_index: dict[int, dict] = {}
    available_urls: set[str] = set()
    for idx, ch in enumerate(chapters or []):
        if not isinstance(ch, dict):
            continue
        url = ch.get("url")
        if url:
            available_urls.add(url)
        title = ch.get("title", "") or ""
        raw_num = ch.get("index") if isinstance(ch.get("index"), int) else extract_real_chapter_number(title)
        real_num = _adjust_real_number(raw_num, idx + 1, offset)
        ch["aligned_index"] = real_num
        available_by_index.setdefault(real_num, ch)

    missing: list[dict] = []
    for idx, ch in enumerate(chapter_items):
        if not isinstance(ch, dict):
            continue
        title = ch.get("title", "") or ""
        raw_num = ch.get("index") if isinstance(ch.get("index"), int) else extract_real_chapter_number(title)
        real_num = _adjust_real_number(raw_num, idx + 1, 0)
        if real_num in existing_numbers:
            continue

        expected_file = ch.get("file") or get_chapter_filename(title, real_num)
        file_path = os.path.join(story_folder, expected_file)
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            # File tồn tại nhưng prefix sai -> cập nhật existing_numbers để tránh crawl lại.
            existing_numbers.add(real_num)
            continue

        ch_url = ch.get("url")
        if (ch_url and ch_url in dead_urls) or (real_num in dead_indexes):
            continue

        # Bỏ qua nếu chương thực sự không có trên web.
        present_on_web = False
        if ch_url and ch_url in available_urls:
            present_on_web = True
        elif real_num in available_by_index:
            present_on_web = True

        if not present_on_web:
            logger.debug(
                "[MISSING] Bỏ qua chương thiếu trên website: %s (index=%s, url=%s)",
                expected_file,
                real_num,
                ch_url,
            )
            continue

        ch_for_missing = {**ch, "idx": idx, "index": real_num, "real_num": real_num}
        missing.append(ch_for_missing)

    # Sắp xếp theo chỉ số thực tế để crawl tuần tự.
    missing.sort(key=lambda item: item.get("index", 0))
    return missing


def slugify_title(title: str) -> str:
    s = unidecode(title)
    s = re.sub(r'[^\w\s-]', '', s.lower())  # bo ky tu dac biet, lowercase
    s = re.sub(r'[\s]+', '-', s)            # khoang trang thanh dau -
    return s.strip('-_')


def count_txt_files(story_folder_path):
    if not os.path.exists(story_folder_path):
        return 0
    return len([f for f in os.listdir(story_folder_path) if f.endswith('.txt')])

def count_dead_chapters(story_folder_path: str) -> int:
    """Return number of chapters marked as dead for the story."""
    path = os.path.join(story_folder_path, "dead_chapters.json")
    if not os.path.exists(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return len(data)
    except Exception:
        pass
    return 0


async def async_save_chapter_with_lock(filename, content):
    lockfile = filename + ".lock"
    lock = FileLock(lockfile, timeout=60)
    with lock:
        await async_save_chapter_with_hash_check(filename, content)


def extract_real_chapter_number(title: str) -> int | None:
    """
    Trich xuat so chuong thuc te tu tieu de chuong.
    Ho tro cac dinh dang: Chuong 123, Chapter 456, 001. Ten chuong, ...
    """
    if not title:
        return None

    import unicodedata

    cleaned = title.translate(str.maketrans({'Đ': 'D', 'đ': 'd'}))
    normalized = unicodedata.normalize('NFD', cleaned)
    normalized = ''.join(ch for ch in normalized if not unicodedata.combining(ch))

    match = re.search(r'(?:chuong|chapter|chap|ch)\s*[:\-]?\s*(\d{1,5})', normalized, re.IGNORECASE)
    if match:
        return int(match.group(1))

    match = re.match(r'^\s*(\d{1,5})[.\-\s]', normalized)
    if match:
        return int(match.group(1))

    return None


def remove_title_number(title: str) -> str:
    """Remove the leading chapter number prefix while keeping the rest intact."""
    if not title:
        return ''

    import unicodedata

    cleaned = title.translate(str.maketrans({'Đ': 'D', 'đ': 'd'}))
    normalized = unicodedata.normalize('NFD', cleaned)
    normalized = ''.join(ch for ch in normalized if not unicodedata.combining(ch))

    prefix_pattern = re.compile(r'^(?:chuong|chapter|chap|ch)\s*\d+\s*[:\-\.\)]?\s*', re.IGNORECASE)
    match = prefix_pattern.match(normalized)
    if match:
        return title[match.end():].strip()

    match = re.match(r'^\s*\d+\s*[:\-\.\)]?\s*', normalized)
    if match:
        return title[match.end():].strip()

    return title.strip()


def get_chapter_filename(title: str, real_num: int) -> str:
    """
    Tao ten file chuong chuan: 0001_ten-chuong.txt (khong dau, cach bang -)
    """
    from unidecode import unidecode
    s = unidecode(title)
    s = re.sub(r'[^\w\s-]', '', s.lower())
    s = re.sub(r'\s+', '-', s)
    clean_title = s.strip('-_') or "untitled"
    return f"{real_num:04d}_{clean_title}.txt"


def export_chapter_metadata_sync(story_folder, chapters) -> None:
    """
    Xuat lai file chapter_metadata.json voi danh sach chapter day du tu web.
    """
    chapter_list_to_write = []
    for idx, ch in enumerate(chapters):
        # Luon gan so thu tu thuc te neu co, neu khong thi dung index
        real_num = extract_real_chapter_number(ch.get("title", "")) or (idx + 1)
        title = ch.get("title", "")
        url = ch.get("url", "")
        
        # Tao ten file chuan hoa
        expected_name = get_chapter_filename(title, real_num)
        
        chapter_list_to_write.append({
            "index": real_num,
            "title": title,
            "url": url,
            "file": expected_name
        })

    # Ghi toan bo danh sach da duoc chuan hoa ra file
    chapter_meta_path = os.path.join(story_folder, "chapter_metadata.json")
    with open(chapter_meta_path, "w", encoding="utf-8") as f:
        json.dump(chapter_list_to_write, f, ensure_ascii=False, indent=4)
    logger.info(f"[META] Exported chapter_metadata.json ({len(chapter_list_to_write)} chuong) for {os.path.basename(story_folder)}")




def remove_chapter_number_from_title(title):
    """Backward compatible function that removes leading chapter numbers."""
    import unicodedata

    if not title:
        return ''

    cleaned = title.translate(str.maketrans({'Đ': 'D', 'đ': 'd'}))
    normalized = unicodedata.normalize('NFD', cleaned)
    normalized = ''.join(ch for ch in normalized if not unicodedata.combining(ch))

    prefix_pattern = re.compile(r'^(?:chuong|chapter|chap|ch)\s*\d+\s*[:\-\.\)]?\s*', re.IGNORECASE)
    match = prefix_pattern.match(normalized)
    if match:
        return title[match.end():].strip()

    match = re.match(r'^\s*\d+\s*[:\-\.\)]?\s*', normalized)
    if match:
        return title[match.end():].strip()

    return title.strip()


def get_actual_chapters_for_export(story_folder):
    """
    Quet thu muc lay danh sach file chuong thuc te,
    tach so chuong (index), title, ten file cho chuan metadata.
    """
    chapters = []
    files = [f for f in os.listdir(story_folder) if f.endswith('.txt')]
    files.sort()  # Dam bao theo thu tu tang dan

    for fname in files:
        # Dinh dang 0001_Ten chuong.txt hoac 0001.Ten chuong.txt
        m = re.match(r"(\d{4})[_\.](.*)\.txt", fname)
        if m:
            index = int(m.group(1))
            raw_title = m.group(2).strip()
            title = remove_chapter_number_from_title(raw_title)
        else:
            # Neu khong dung dinh dang, fallback
            index = len(chapters) + 1
            raw_title = fname[:-4]  # bo .txt
            title = remove_chapter_number_from_title(raw_title)
        chapters.append({
            "index": index,
            "title": title,
            "file": fname,
            "url": ""  # Neu lay duoc thi bo sung them, con khong de rong
        })
    return chapters



def get_chapter_sort_key(chapter: Dict[str, str]) -> Tuple[int, str]:
    url = chapter.get('url', '')
    title = chapter.get('title', '')
    number_match = re.search(r'(?:chuong|chapter)[^0-9]*([0-9]+)', url, re.IGNORECASE)
    if not number_match:
        number_match = re.search(r'(?:ch(?:u|\u01b0)\u01a1ng|chapter)\s*([0-9]+)', title, re.IGNORECASE)
    number = int(number_match.group(1)) if number_match else 0
    return number, url


async def get_real_total_chapters(metadata, adapter: BaseSiteAdapter):
    # Uu tien lay tu sources neu co
    if metadata.get("sources"):
        for source in metadata["sources"]:
            url = source.get("url")
            adapter, site_key = get_adapter_from_url(url,adapter) # type: ignore
            if not adapter or not url:
                continue
            chapters = await adapter.get_chapter_list(story_url=url, story_title=metadata.get("title"), site_key=site_key, total_chapters=metadata.get("total_chapters_on_site"))
            if chapters and len(chapters) > 0:
                return len(chapters)
    # Neu khong co sources, fallback dung url + site_key hien tai trong metadata
    url = metadata.get("url")
    site_key = metadata.get("site_key")
    if url and site_key:
        chapters = await adapter.get_chapter_list(story_url=url, story_title=metadata.get("title"), site_key=site_key, total_chapters=metadata.get("total_chapters_on_site"))
        if chapters:
            return len(chapters)
    return 0
