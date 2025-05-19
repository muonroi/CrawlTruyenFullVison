import asyncio
import glob
import json
import time
import aiohttp
import os
from typing import Dict, Any
from adapters.base_site_adapter import BaseSiteAdapter
from core.chapter_service import async_download_and_save_chapter
from core.meta_service import add_missing_story, backup_crawl_state, save_story_metadata_file
from core.state_service import clear_specific_state_keys, save_crawl_state
from utils.batch_utils import smart_delay, split_batches
from utils.chapter_utils import  count_txt_files
from utils.io_utils import   ensure_directory_exists, safe_write_file, sanitize_filename
from utils.logger import logger
from config.config import  get_state_file
from core.models.story import Story, Genre, Chapter
from config.config import MAX_CHAPTER_PAGES_TO_CRAWL

BATCH_SEMAPHORE_LIMIT = 5

from core.models.story import Story, Genre, Chapter

async def process_story_item(
    session: aiohttp.ClientSession,
    story_data_item: Story,                      # <- Sửa: dùng Story object
    current_discovery_genre_data: Genre,         # <- Sửa: dùng Genre object
    story_global_folder_path: str,
    crawl_state: Dict[str, Any],
    adapter: BaseSiteAdapter,
    site_key: str
) -> bool:
    await ensure_directory_exists(story_global_folder_path)
    
    logger.info(f"\n  --- Xử lý truyện: {story_data_item.title} ---")
    metadata_file = os.path.join(story_global_folder_path, "metadata.json")
    fields_need_check = ["description", "status", "source", "rating_value", "rating_count", "total_chapters_on_site"]
    metadata = None
    need_update = False

    if os.path.exists(metadata_file):
        with open(metadata_file, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        for field in fields_need_check:
            if metadata.get(field) is None:
                need_update = True
                break
    else:
        need_update = True

    # Cập nhật metadata nếu thiếu
    if need_update:
        details = await adapter.get_story_details(story_data_item.url, story_data_item.title)
        await save_story_metadata_file(
            story_data_item,
            current_discovery_genre_data.dict() if hasattr(current_discovery_genre_data, 'dict')
                else {"name": current_discovery_genre_data.name, "url": current_discovery_genre_data.url},
            story_global_folder_path,
            details,
            metadata
        )
        if metadata and isinstance(details, dict):
            for field in fields_need_check:
                if details.get(field) is not None:
                    metadata[field] = details[field]
            metadata['metadata_updated_at'] = time.strftime("%Y-%m-%d %H:%M:%S")
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
    else:
        details = metadata

    crawl_state['current_story_url'] = story_data_item.url
    if crawl_state.get('previous_story_url_in_state_for_chapters') != story_data_item.url:
        crawl_state['processed_chapter_urls_for_current_story'] = []
    crawl_state['previous_story_url_in_state_for_chapters'] = story_data_item.url
    state_file = get_state_file(site_key)
    await save_crawl_state(crawl_state, state_file)

    # Lấy danh sách chương mới nhất từ web
    chapters = await adapter.get_chapter_list(
        story_data_item.url, story_data_item.title,
        MAX_CHAPTER_PAGES_TO_CRAWL,
        details.get('total_chapters_on_site') if isinstance(details, dict) else getattr(details, "total_chapters_on_site", None)
    )

    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    # Lấy hoặc khởi tạo danh sách nguồn
    if isinstance(details, dict):
        if "sources" not in details:
            details["sources"] = []
        found = False
        for src in details["sources"]:
            if src.get("site") == site_key:
                src["url"] = story_data_item.url
                src["total_chapters"] = len(chapters)
                src["last_update"] = now_str
                found = True
        if not found:
            details["sources"].append({
                "site": site_key,
                "url": story_data_item.url,
                "total_chapters": len(chapters),
                "last_update": now_str
            })
        # Lưu lại metadata (đầy đủ nhất)
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(details, f, ensure_ascii=False, indent=4)

    existing_files = set(os.listdir(story_global_folder_path)) if os.path.exists(story_global_folder_path) else set()
    added = 0
    for idx, ch in enumerate(chapters):  # ch: Chapter
        fname_only = f"{idx+1:04d}_{sanitize_filename(ch.title) or 'untitled'}.txt"
        fpath = os.path.join(story_global_folder_path, fname_only)
        if fname_only not in existing_files:
            content = await adapter.get_chapter_content(ch.url, ch.title)
            if content:
                await safe_write_file(fpath, content)
                added += 1
            else:
                logger.warning(f"Không lấy được nội dung chương {idx+1}: {ch.title} từ nguồn {story_data_item.url}")
    if added > 0:
        logger.info(f"Đã thêm {added} chương mới cho '{story_data_item.title}' từ nguồn {story_data_item.url}")
    else:
        logger.info(f"Không phát hiện chương mới ở '{story_data_item.title}'.")

    # Kiểm tra số chương đã crawl và update trạng thái
    total_chapters_on_site = (details.get('total_chapters_on_site')
                              if isinstance(details, dict) else getattr(details, "total_chapters_on_site", None))
    story_title = story_data_item.title
    story_url = story_data_item.url
    if total_chapters_on_site:
        crawled_chapters = count_txt_files(story_global_folder_path)
        if crawled_chapters < 0.1 * total_chapters_on_site:
            logger.error(f"[ALERT] Parse chương có thể lỗi HTML hoặc bị chặn: {story_title} ({crawled_chapters}/{total_chapters_on_site})")
        if crawled_chapters < total_chapters_on_site:
            logger.warning(f"Truyện '{story_title}' chỉ crawl được {crawled_chapters}/{total_chapters_on_site} chương! Ghi lại để crawl bù.")
            await add_missing_story(story_title, story_url, total_chapters_on_site, crawled_chapters)
        else:
            logger.info(f"Truyện '{story_title}' đã crawl đủ {crawled_chapters}/{total_chapters_on_site} chương.")
        # === CẬP NHẬT crawled_chapters vào sources ===
        try:
            if os.path.exists(metadata_file):
                with open(metadata_file, "r", encoding="utf-8") as f:
                    metadata_latest = json.load(f)
            else:
                metadata_latest = details

            sources = metadata_latest.get("sources", [])
            for src in sources:
                if src.get("site") == site_key:
                    src["crawled_chapters"] = crawled_chapters  # Thực tế đã crawl trên local
                else:
                    src.setdefault("crawled_chapters", None)
            metadata_latest["sources"] = sources
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata_latest, f, ensure_ascii=False, indent=4)
        except Exception as ex:
            logger.warning(f"Lỗi khi cập nhật crawled_chapters vào sources: {ex}")

    # Check completion
    is_complete = False
    status = (details.get('status') if isinstance(details, dict) else getattr(details, "status", None))
    total = (details.get('total_chapters_on_site') if isinstance(details, dict) else getattr(details, "total_chapters_on_site", None))
    if status and total and crawled_chapters >= total:
        is_complete = True
        completed = set(crawl_state.get('globally_completed_story_urls', []))
        completed.add(story_data_item.url)
        crawl_state['globally_completed_story_urls'] = sorted(completed)
    backup_crawl_state(state_file)
    state_file = get_state_file(site_key)
    await save_crawl_state(crawl_state, state_file)
    await clear_specific_state_keys(crawl_state, ['processed_chapter_urls_for_current_story'], state_file)
    return is_complete


async def crawl_missing_chapters_for_story(
    site_key, session, chapters, story_data_item, current_discovery_genre_data, story_folder_path, crawl_state,
    num_batches=10, state_file=None
):
    saved_files = get_saved_chapters_files(story_folder_path)
    missing_chapters = []
    for idx, ch in enumerate(chapters):
        fname_only = f"{idx+1:04d}_{sanitize_filename(ch['title']) or 'untitled'}.txt"
        if fname_only not in saved_files:
            missing_chapters.append((idx, ch, fname_only))

    if not missing_chapters:
        logger.info(f"Tất cả chương đã đủ, không có chương missing trong '{story_data_item['title']}'")
        # Có thể lưu lại trạng thái nếu muốn, tùy yêu cầu
        if state_file:
            await save_crawl_state(crawl_state, state_file)
        return 0

    num_batches = min(num_batches, max(1, len(missing_chapters)))
    logger.info(
        f"Có {len(missing_chapters)} chương thiếu, chia thành {num_batches} batch để crawl song song cho truyện '{story_data_item['title']}'..."
    )

    batches = split_batches(missing_chapters, num_batches)

    async def crawl_batch(batch, batch_idx):
        successful, failed = set(), []
        sem = asyncio.Semaphore(BATCH_SEMAPHORE_LIMIT)
        tasks = []
        for i, (idx, ch, fname_only) in enumerate(batch):
            full_path = os.path.join(story_folder_path, fname_only)
            logger.info(f"[Batch {batch_idx}] Đang crawl chương {idx+1}: {ch['title']}")

            async def wrapped(ch=ch, idx=idx, fname_only=fname_only, full_path=full_path):
                async with sem:
                    try:
                        await asyncio.wait_for(
                            async_download_and_save_chapter(
                                ch, story_data_item, current_discovery_genre_data,
                                full_path, fname_only, "Crawl bù missing",
                                f"{idx+1}/{len(chapters)}", crawl_state, successful, failed, idx,
                                site_key=site_key, state_file=state_file  #type: ignore # Truyền state_file xuống nếu hàm con có hỗ trợ
                            ),
                            timeout=120  # Timeout mỗi chương
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
        # --- Lưu lại state sau mỗi batch ---
        if state_file:
            await save_crawl_state(crawl_state, state_file)
        return successful, failed

    batch_tasks = [crawl_batch(batch, i+1) for i, batch in enumerate(batches) if batch]
    results = await asyncio.gather(*batch_tasks, return_exceptions=True)
    successful = set()
    failed = []
    for res in results:
        if isinstance(res, tuple):
            suc, fail = res
            successful.update(suc)
            failed.extend(fail)
        elif isinstance(res, Exception):
            logger.error(f"Lỗi khi thực thi batch: {res}")
    if failed:
        logger.warning(f"Vẫn còn {len(failed)} chương bù không crawl được.")

    # --- Lưu lại state lần cuối cùng khi xong hết ---
    if state_file:
        await save_crawl_state(crawl_state, state_file)
    return len(successful)

def get_saved_chapters_files(story_folder_path: str) -> set:
    """Trả về set tên file đã lưu trong folder truyện."""
    if not os.path.exists(story_folder_path):
        return set()
    files = glob.glob(os.path.join(story_folder_path, "*.txt"))
    return set(os.path.basename(f) for f in files)