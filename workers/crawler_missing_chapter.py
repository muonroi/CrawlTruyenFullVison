import os
import asyncio
import json
import datetime
import re
import traceback
from typing import cast
from adapters.factory import get_adapter
from config import config as app_config
from config.config import BASE_URLS, COMPLETED_FOLDER, DATA_FOLDER, LOADED_PROXIES, PROXIES_FILE, PROXIES_FOLDER
from config.proxy_provider import load_proxies
from scraper import initialize_scraper 

from utils.chapter_utils import (
    SEM,
    count_txt_files,
    count_dead_chapters,
    crawl_missing_chapters_for_story,
    export_chapter_metadata_sync,
    extract_real_chapter_number,
    get_actual_chapters_for_export,
    get_chapter_filename,
    get_missing_chapters,
    get_real_total_chapters,
    mark_dead_chapter,
)
from utils.batch_utils import smart_delay
from utils.domain_utils import get_site_key_from_url, is_url_for_site
from utils.logger import logger
from utils.io_utils import create_proxy_template_if_not_exists, move_story_to_completed
from utils.notifier import send_telegram_notify
from utils.cache_utils import cached_get_story_details, cached_get_chapter_list
from utils.state_utils import get_missing_worker_state_file, load_crawl_state
from filelock import FileLock
auto_fixed_titles = []
MAX_CONCURRENT_STORIES = 3
STORY_SEM = asyncio.Semaphore(MAX_CONCURRENT_STORIES)
MISSING_SUMMARY_LOG = "missing_summary.log"
MAX_SOURCE_TIMEOUT_RETRY = 3

def calculate_missing_crawl_timeout(num_chapters: int | None = None) -> float:
    """Return a dynamic timeout for crawling missing chapters."""

    base_timeout = max(1, app_config.MISSING_CRAWL_TIMEOUT_SECONDS)
    if not num_chapters or num_chapters <= 0:
        return base_timeout

    dynamic_timeout = base_timeout + num_chapters * app_config.MISSING_CRAWL_TIMEOUT_PER_CHAPTER
    # Avoid unbounded waits but allow large stories more time to finish.
    return max(base_timeout, min(app_config.MISSING_CRAWL_TIMEOUT_MAX, dynamic_timeout))

def get_existing_real_chapter_numbers(story_folder):
    files = [f for f in os.listdir(story_folder) if f.endswith('.txt')]
    nums = set()
    for f in files:
        match = re.match(r'(\d{4})_', f)
        if match:
            nums.add(int(match.group(1)))
    return nums

def update_metadata_from_details(metadata: dict, details: dict) -> bool:
    if not details:
        return False
    changed = False
    for k, v in details.items():
        if v is not None and v != "" and metadata.get(k) != v:
            metadata[k] = v
            changed = True
    return changed


async def refresh_total_chapters_from_web(metadata: dict, metadata_path: str, adapter) -> int:
    """Ensure metadata.total_chapters_on_site matches the latest number from the web."""
    if not metadata:
        return 0

    metadata_changed = False

    # Chuẩn hóa danh sách nguồn để tránh lỗi khi lấy total chương thực tế
    try:
        normalized_sources = normalize_source_list(metadata)
    except Exception as ex:
        logger.warning(f"[REFRESH] Lỗi khi chuẩn hóa sources cho '{metadata.get('title')}': {ex}")
        normalized_sources = metadata.get("sources", [])

    if normalized_sources != metadata.get("sources"):
        metadata["sources"] = normalized_sources
        metadata_changed = True

    latest_total = metadata.get("total_chapters_on_site") or 0

    try:
        real_total = await get_real_total_chapters(metadata, adapter)
    except Exception as ex:  # pragma: no cover - network/adapter issues
        logger.warning(
            f"[REFRESH] Không lấy được total chương thực tế cho '{metadata.get('title')}' từ web: {ex}"
        )
        real_total = 0

    if real_total and real_total > 0 and real_total != latest_total:
        logger.info(
            f"[REFRESH] Cập nhật total_chapters_on_site cho '{metadata.get('title')}' từ {latest_total} -> {real_total}"
        )
        metadata["total_chapters_on_site"] = real_total
        latest_total = real_total
        metadata_changed = True
    else:
        latest_total = max(latest_total, real_total)

    if metadata_changed and metadata_path:
        try:
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
        except Exception as ex:
            logger.warning(f"[REFRESH] Không thể lưu metadata cho '{metadata.get('title')}': {ex}")

    return latest_total or 0


def check_and_fix_chapter_filename(story_folder: str, ch: dict, real_num: int, idx: int):
    """
    Nếu tên file hiện tại không khớp với tên dự kiến từ title → rename lại cho đúng.
    """
    # Danh sách file trong folder
    existing_files = [f for f in os.listdir(story_folder) if f.endswith(".txt")]

    # Tên dự kiến từ title chương
    expected_name = get_chapter_filename(ch.get("title", ""), real_num)
    expected_path = os.path.join(story_folder, expected_name)

    # Nếu file đích đã tồn tại đúng → OK
    if os.path.exists(expected_path):
        return

    # Tìm file sai tên theo prefix số chương
    prefix = f"{real_num:04d}_"
    for fname in existing_files:
        if fname.startswith(prefix):
            current_path = os.path.join(story_folder, fname)
            # Nếu khác tên → rename
            if current_path != expected_path:
                try:
                    os.rename(current_path, expected_path)
                    logger.info(f"[RENAME] Đã rename file '{fname}' → '{expected_name}'")
                except Exception as e:
                    logger.warning(f"[RENAME ERROR] Không thể rename '{fname}' → '{expected_name}': {e}")
            break


def get_current_category(metadata):
    categories = autofix_category(metadata)
    return categories[0]


async def loop_once_multi_sites(force_unskip=False):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"\n===== [START] Check missing for all sites at {now} =====")
    tasks = []
    for site_key, url in BASE_URLS.items():
        adapter = get_adapter(site_key)
        tasks.append(asyncio.create_task(check_and_crawl_missing_all_stories(adapter, url, site_key=site_key, force_unskip=force_unskip)))
    try:
        logger.info("Before await gather")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task {i} bị lỗi: {result}\n{traceback.format_exc()}")
        logger.info(f"===== [DONE] =====\n")
    except Exception as e:
        logger.error(f"[ERROR] Lỗi khi kiểm tra/crawl missing: {e}")
    logger.info(f"===== [DONE] =====\n")
    await send_telegram_notify(f"✅ DONE: Đã crawl/check missing xong toàn bộ ({now})")
async def crawl_missing_until_complete(
    adapter, site_key, session, chapters_from_web, metadata, current_category, story_folder, crawl_state, state_file, max_retry=3
):
    retry = 0
    while retry < max_retry:
        missing_chapters = get_missing_chapters(story_folder, chapters_from_web, site_key)
        for ch in chapters_from_web:
            title = ch.get('title', '') or ''
            aligned = ch.get('aligned_index')
            if isinstance(aligned, int):
                real_num = aligned
            else:
                real_num = extract_real_chapter_number(title)
                if not isinstance(real_num, int):
                    real_num = ch.get('idx', 0) + 1
            check_and_fix_chapter_filename(story_folder, ch, real_num, ch.get('idx', 0))

        if not missing_chapters:
            logger.info(f"[COMPLETE] Đã đủ tất cả chương cho '{metadata['title']}'")
            chapters_for_export = get_actual_chapters_for_export(story_folder)
            export_chapter_metadata_sync(story_folder, chapters_for_export)
            return True
        logger.info(f"[RETRY] {len(missing_chapters)} chương còn thiếu, bắt đầu crawl lần {retry+1}/{max_retry}")
        # Tính số batch dựa trên số chương còn thiếu, mỗi batch tối đa 120 chương
        num_batches = max(1, (len(missing_chapters) + 119) // 120)  # Chia thành các batch 120 chương
        logger.info(f"Crawl {len(missing_chapters)} chương với {num_batches} batch (mỗi batch tối đa 120 chương)")
        await crawl_story_with_limit(
            site_key,
            session,
            missing_chapters,
            metadata,
            current_category,
            story_folder,
            crawl_state,
            num_batches=num_batches,
            state_file=state_file,
            adapter=adapter,
            chapters_all=chapters_from_web,
        )
        # Kiểm tra lại sau khi crawl
        missing_chapters = get_missing_chapters(story_folder, chapters_from_web, site_key)
        if not missing_chapters:
            logger.info(f"[COMPLETE] Đã đủ tất cả chương sau lần crawl {retry+1}")
            return True
        retry += 1
    logger.warning(
        f"[FAILED] Sau {max_retry} lần retry vẫn còn thiếu {len(missing_chapters)} chương cho '{metadata['title']}'"
    )
    chapters_for_export = get_actual_chapters_for_export(story_folder)
    export_chapter_metadata_sync(story_folder, chapters_for_export)
    if retry >= max_retry:
        logger.warning(
            f"[FATAL] Sau {max_retry} lần vẫn còn thiếu chương. Đánh dấu dead_chapters và bỏ qua."
        )
        for ch in missing_chapters:
            await mark_dead_chapter(
                story_folder,
                {
                    "index": ch.get("real_num"),
                    "title": ch.get("title"),
                    "url": ch.get("url"),
                    "reason": "max_retry_reached",
                },
            )
        warn_msg = (
            f"[MISSING] '{metadata['title']}' vẫn thiếu {len(missing_chapters)} chương sau khi thử mọi nguồn"
        )
        logger.warning(warn_msg)
        await send_telegram_notify(warn_msg)
        try:
            with open(MISSING_SUMMARY_LOG, "a", encoding="utf-8") as f:
                f.write(f"{metadata.get('title')}\t{story_folder}\t{len(missing_chapters)}\n")
        except Exception:
            pass
        return False
    return False
def autofix_category(metadata):
    """
    Đảm bảo metadata có 'categories' là list[dict] chuẩn.
    Nếu thiếu hoặc sai kiểu thì set lại Unknown.
    """
    categories = metadata.get('categories')
    if not (isinstance(categories, list) and categories and isinstance(categories[0], dict) and 'name' in categories[0]):
        logger.warning(f"[AUTO-FIX] Metadata '{metadata.get('title')}' thiếu hoặc sai categories. Set lại Unknown.")
        metadata['categories'] = [{"name": "Unknown", "url": ""}]
    return metadata['categories']


def normalize_source_list(metadata):
    """Chuẩn hoá danh sách nguồn và gắn cờ nguồn chuẩn."""

    normalized: list[dict] = []
    seen: set[tuple[str, str]] = set()
    primary_key: tuple[str, str] | None = None

    raw_sources = metadata.get("sources", []) or []
    for raw in raw_sources:
        if isinstance(raw, dict):
            url = raw.get("url")
            site_key = (
                raw.get("site_key")
                or raw.get("site")
                or get_site_key_from_url(url)
                or metadata.get("site_key")
            )
            entry = dict(raw)
        elif isinstance(raw, str):
            url = raw
            site_key = get_site_key_from_url(url) or metadata.get("site_key")
            entry = {"url": url}
        else:
            continue

        if not url or not site_key:
            continue
        if not is_url_for_site(url, site_key):
            continue

        key = (url, site_key)
        if key in seen:
            continue

        entry["url"] = url
        entry["site_key"] = site_key
        normalized.append(entry)
        seen.add(key)

        if primary_key is None:
            primary_key = key

    main_url = metadata.get("url")
    main_key = metadata.get("site_key") or get_site_key_from_url(main_url)
    if main_url and main_key and is_url_for_site(main_url, main_key):
        key = (main_url, main_key)
        if key not in seen:
            normalized.append({"url": main_url, "site_key": main_key})
            seen.add(key)
        if primary_key is None:
            primary_key = key

    if not normalized:
        return normalized

    primary_sources: list[dict] = []
    fallback_sources: list[dict] = []
    for entry in normalized:
        entry_copy = dict(entry)
        key = (entry_copy.get("url"), entry_copy.get("site_key"))
        is_primary = primary_key is not None and key == primary_key
        entry_copy["is_primary"] = is_primary
        if is_primary:
            primary_sources.append(entry_copy)
        else:
            fallback_sources.append(entry_copy)

    return primary_sources + fallback_sources


def ensure_primary_source(metadata: dict, metadata_path: str | None = None) -> tuple[list[dict], dict | None]:
    """Normalize sources, persist them, và đảm bảo site_key trỏ về Nguồn Chuẩn."""

    try:
        source_list = normalize_source_list(metadata)
    except Exception as ex:  # pragma: no cover - defensive guard
        logger.warning(
            f"[SOURCE] Lỗi khi chuẩn hoá sources của '{metadata.get('title')}': {ex}"
        )
        source_list = metadata.get("sources", []) or []

    primary_source = next((src for src in source_list if src.get("is_primary")), None)

    changed = False
    if source_list and metadata.get("sources") != source_list:
        metadata["sources"] = source_list
        changed = True

    if primary_source:
        primary_site_key = primary_source.get("site_key")
        if primary_site_key and metadata.get("site_key") != primary_site_key:
            metadata["site_key"] = primary_site_key
            changed = True

    if changed and metadata_path:
        try:
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
        except Exception as ex:  # pragma: no cover - IO guard
            logger.warning(
                f"[SOURCE] Không thể lưu metadata sau khi cập nhật nguồn cho '{metadata.get('title')}': {ex}"
            )

    return source_list, primary_source


async def check_and_crawl_missing_all_stories(adapter, home_page_url, site_key, force_unskip=False):
    state_file = get_missing_worker_state_file(site_key)
    crawl_state = await load_crawl_state(state_file, site_key)
    adapter = get_adapter(site_key) 
    all_genres = await adapter.get_genres()
    genre_name_to_url = {g['name']: g['url'] for g in all_genres if isinstance(g, dict) and 'name' in g and 'url' in g}
    if not all_genres:
        logger.error(f"[{site_key}] Không lấy được danh sách thể loại (all_genres rỗng) từ {home_page_url}!")
        return

    genre_complete_checked = set()
    os.makedirs(COMPLETED_FOLDER, exist_ok=True)
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)
    await initialize_scraper(site_key)
    crawl_state = await load_crawl_state(state_file, site_key)

    # Lấy danh sách tất cả story folder cần crawl
    story_folders = [
        os.path.join(DATA_FOLDER, cast(str, f))
        for f in os.listdir(DATA_FOLDER)
        if os.path.isdir(os.path.join(DATA_FOLDER, cast(str, f)))
    ]
    story_retry_counts: dict[str, int] = {}

    # ============ 1. Kiểm tra và crawl thiếu theo từng truyện ============
    for story_folder in story_folders:
        metadata_path = os.path.join(story_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            continue
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        source_list, primary_source = ensure_primary_source(metadata, metadata_path)

        if site_key != metadata.get("site_key"):
            logger.debug(
                f"[SKIP] '{metadata.get('title')}' thuộc nguồn chuẩn {metadata.get('site_key')} – bỏ qua tại worker {site_key}."
            )
            continue
        need_autofix = False
        metadata = None
        if auto_fixed_titles:
            msg = "[AUTO-FIX] Đã tự động tạo metadata cho các truyện: " + ", ".join(auto_fixed_titles[:10])
            if len(auto_fixed_titles) > 10:
                msg += f" ... (và {len(auto_fixed_titles)-10} truyện nữa)"
            #await send_discord_notify(msg)
            auto_fixed_titles.clear()
        if os.path.dirname(story_folder) == os.path.abspath(COMPLETED_FOLDER):
            continue
        metadata_path = os.path.join(story_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            guessed_url = f"{BASE_URLS.get(site_key, '').rstrip('/')}/{os.path.basename(story_folder)}"
            logger.info(f"[AUTO-FIX] Không có metadata.json, đang lấy metadata chi tiết từ {guessed_url}")
            details = await cached_get_story_details(
                adapter, guessed_url, os.path.basename(story_folder).replace("-", " ")
            )
            logger.info("... sau await get_story_details ...")
            metadata = autofix_metadata(story_folder, site_key)
            if details:
                # Merge tất cả các trường (kể cả trường mới hoặc chỉ có trong details)
                for k, v in details.items():
                    if v is not None and v != "" and metadata.get(k) != v:
                        logger.info(f"[UPDATE] {metadata['title']}: Trường '{k}' được cập nhật.")
                        metadata[k] = v
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)
                logger.info(f"[AUTO-FIX] Đã tạo metadata đầy đủ/merge cho '{metadata.get('title')}' ({metadata.get('total_chapters_on_site', 0)} chương)")
                # Log trường thiếu cho dev dễ debug adapter
                fields_required = ['title', 'categories', 'total_chapters_on_site', 'author', 'description', 'cover', 'sources']
                missing = [f for f in fields_required if not metadata.get(f)]
                if missing:
                    logger.warning(f"[AUTO-FIX] Metadata của '{metadata.get('title')}' vẫn còn thiếu các trường: {missing}")
            else:
                logger.info(f"[AUTO-FIX] Tạo metadata tạm cho '{metadata['title']}' ({metadata.get('total_chapters_on_site', 0)} chương)")
            auto_fixed_titles.append(metadata["title"])


        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            # --- Clean sources sai domain ở đây ---
            fixed_sources = []
            for src in metadata.get("sources", []):
                s_url = src.get("url") if isinstance(src, dict) else src
                s_key = (
                    get_site_key_from_url(s_url)
                    or (src.get("site_key") if isinstance(src, dict) else None)
                    or (src.get("site") if isinstance(src, dict) else None)
                    or metadata.get("site_key")
                )
                if s_url and s_key and is_url_for_site(s_url, s_key):
                    fixed_sources.append(src)
                else:
                    logger.warning(
                        f"[FIX] Source có url {s_url} không đúng domain với key {s_key}, đã loại khỏi sources."
                    )
            metadata["sources"] = fixed_sources
            # --------------------------------------

            source_list, primary_source = ensure_primary_source(metadata, metadata_path)

            # Validate cấu trúc sources và các trường bắt buộc
            if not isinstance(metadata.get("sources", []), list):
                need_autofix = True
            fields_required = ['title', 'categories', 'total_chapters_on_site']
            if not all(metadata.get(f) for f in fields_required):
                need_autofix = True
        except Exception as ex:
            logger.warning(f"[AUTO-FIX] metadata.json lỗi/parsing fail tại {story_folder}, sẽ xoá file và tạo lại! {ex}")
            need_autofix = True


        if need_autofix:
            try:
                os.remove(metadata_path)
            except Exception as ex:
                logger.error(f"Lỗi xóa metadata lỗi: {ex}")
            metadata = autofix_metadata(story_folder, site_key)
            auto_fixed_titles.append(metadata["title"])

        source_list, primary_source = ensure_primary_source(metadata, metadata_path)

        # Nếu force_unskip: Xoá skip_crawl & meta_retry_count nếu có
        if force_unskip:
            changed = False
            if metadata.get("skip_crawl"): #type:ignore
                metadata.pop("skip_crawl", None) #type:ignore
                changed = True
            if "meta_retry_count" in metadata: #type:ignore
                metadata.pop("meta_retry_count", None) #type:ignore
                changed = True
            if changed:
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)
                logger.info(f"[UNSKIP] Tự động unskip: {metadata.get('title')}") #type:ignore

        if metadata.get("skip_crawl", False): #type:ignore
            logger.info(f"[SKIP] Truyện '{metadata.get('title')}' đã bị đánh dấu bỏ qua (skip_crawl), không crawl lại nữa.") #type:ignore
            continue

        # Auto fix metadata nếu thiếu (và skip nếu quá 3 lần)
        if not await fix_metadata_with_retry(metadata, metadata_path, story_folder, site_key=site_key, adapter=adapter):
            continue

        latest_total = await refresh_total_chapters_from_web(metadata, metadata_path, adapter)
        if not latest_total:
            latest_total = metadata.get("total_chapters_on_site") or 0

        crawled_files = count_txt_files(story_folder)
        if crawled_files < latest_total: #type:ignore
            logger.info(
                f"[MISSING] '{metadata['title']}' thiếu chương ({crawled_files}/{latest_total}) -> Đang kiểm tra/crawl bù theo nguồn chuẩn trước."
            )

            if not source_list:
                logger.error(f"Không có nguồn nào hợp lệ cho truyện '{metadata['title']}'. Bỏ qua.")
                continue

            primary_source = next((src for src in source_list if src.get("is_primary")), source_list[0]) if source_list else None
            fallback_sources = [src for src in source_list if src is not primary_source]
            ordered_sources = [primary_source] + fallback_sources if primary_source else source_list

            canonical_chapters = None
            canonical_source = None
            retry_story = False
            for source in ordered_sources:
                url = source.get("url")
                src_site_key = source.get("site_key")
                if not src_site_key or not url:
                    continue

                adapter = get_adapter(src_site_key)
                try:
                    if source.get("is_primary"):
                        logger.info(
                            f"Đang lấy danh sách chương chuẩn từ Nguồn Chuẩn ({src_site_key})."
                        )
                    else:
                        logger.info(
                            f"Nguồn Chuẩn không khả dụng, thử lấy danh sách chương chuẩn từ nguồn dự phòng: {src_site_key}."
                        )
                    chapters = await cached_get_chapter_list(
                        adapter,
                        url,
                        metadata['title'],
                        src_site_key,
                        total_chapters=metadata.get("total_chapters_on_site"),
                    )
                    if chapters:
                        canonical_chapters = chapters
                        canonical_source = source
                        export_chapter_metadata_sync(story_folder, canonical_chapters)
                        logger.info(
                            f"Lấy được {len(canonical_chapters)} chương chuẩn từ nguồn {src_site_key}."
                        )
                        break
                except Exception as ex:
                    logger.warning(
                        f"Lỗi khi lấy chương chuẩn từ nguồn {src_site_key}: {ex}. Thử nguồn tiếp theo."
                    )

            if not canonical_chapters:
                logger.error(
                    f"Không thể lấy danh sách chương từ bất kỳ nguồn nào cho '{metadata['title']}'. Bỏ qua."
                )
                continue

            canonical_site_key = canonical_source.get("site_key") if canonical_source else None

            def remaining_missing() -> list[dict]:
                if not canonical_chapters or not canonical_site_key:
                    return []
                return get_missing_chapters(story_folder, canonical_chapters, canonical_site_key)

            current_category = get_current_category(metadata)

            for idx, source in enumerate(ordered_sources, start=1):
                url = source.get("url")
                src_site_key = source.get("site_key")
                if not src_site_key or not url:
                    continue

                adapter = get_adapter(src_site_key)
                logger.info(
                    f"[CRAWL SOURCE {idx}/{len(ordered_sources)}] site_key={src_site_key}, url={url}"
                )

                try:
                    if (
                        canonical_source
                        and canonical_source.get("url") == url
                        and canonical_source.get("site_key") == src_site_key
                    ):
                        chapters_from_source = canonical_chapters
                    else:
                        chapters_from_source = await cached_get_chapter_list(
                            adapter,
                            url,
                            metadata['title'],
                            src_site_key,
                            total_chapters=metadata.get("total_chapters_on_site"),
                        )
                    if not chapters_from_source:
                        logger.warning(f"Nguồn {src_site_key} không trả về danh sách chương.")
                        continue

                    missing_chapters = get_missing_chapters(
                        story_folder, chapters_from_source, src_site_key
                    )
                    if not missing_chapters:
                        logger.info(
                            f"Không phát hiện chương thiếu nào từ nguồn {src_site_key}."
                        )
                        if not remaining_missing():
                            logger.info(
                                f"Truyện '{metadata['title']}' đã đủ chương sau khi đối chiếu nguồn {src_site_key}."
                            )
                            break
                        continue

                    logger.info(
                        f"Bắt đầu crawl {len(missing_chapters)} chương thiếu từ nguồn {src_site_key}."
                    )
                    await crawl_story_with_limit(
                        src_site_key,
                        None,
                        missing_chapters,
                        metadata,
                        current_category,
                        story_folder,
                        crawl_state,
                        state_file=state_file,
                        adapter=adapter,
                        chapters_all=chapters_from_source,
                    )

                    if not remaining_missing():
                        logger.info(
                            f"Truyện '{metadata['title']}' đã đủ chương sau khi crawl từ nguồn {src_site_key}."
                        )
                        break
                except asyncio.TimeoutError:
                    attempt = story_retry_counts.get(story_folder, 0) + 1
                    story_retry_counts[story_folder] = attempt
                    if attempt < MAX_SOURCE_TIMEOUT_RETRY:
                        logger.warning(
                            f"[TIMEOUT] Crawl thiếu chương cho '{metadata.get('title')}' từ nguồn {src_site_key} quá 60s (lần {attempt}/{MAX_SOURCE_TIMEOUT_RETRY}). Sẽ thử lại sau.",
                            exc_info=True,
                        )
                        story_folders.append(story_folder)
                        retry_story = True
                        await smart_delay()
                        break
                    else:
                        logger.error(
                            f"[TIMEOUT] Crawl thiếu chương cho '{metadata.get('title')}' từ nguồn {src_site_key} đã quá hạn {MAX_SOURCE_TIMEOUT_RETRY} lần, bỏ qua.",
                            exc_info=True,
                        )
                        continue
                except Exception:
                    logger.exception(
                        f"Lỗi không xác định khi xử lý nguồn {src_site_key}"
                    )
                    continue

            if retry_story:
                continue

        logger.info(f"[NEXT] Kết thúc process cho story: {story_folder}")


    # ============ 3. Quét lại & move, cảnh báo, đồng bộ ============
    notified_titles = set()
    for story_folder in story_folders:
        # Skip nếu truyện đã move sang completed
        genre_folders = [os.path.join(COMPLETED_FOLDER, d) for d in os.listdir(COMPLETED_FOLDER) if os.path.isdir(os.path.join(COMPLETED_FOLDER, d))]
        if any(os.path.join(gf, os.path.basename(story_folder)) == story_folder for gf in genre_folders):
            continue

        meta_path = os.path.join(story_folder, "metadata.json")
        with FileLock(meta_path + ".lock", timeout=10):
            if not os.path.exists(meta_path):
                metadata = autofix_metadata(story_folder, site_key)
                auto_fixed_titles.append(metadata["title"])
            else:
                with open(meta_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                # Sửa lại sources nếu cần
                if "sources" in metadata and isinstance(metadata["sources"], list):
                    fixed_sources = []
                    for src in metadata["sources"]:
                        if isinstance(src, dict):
                            fixed_sources.append(src)
                        elif isinstance(src, str):
                            fixed_sources.append({"url": src})
                    if len(fixed_sources) != len(metadata["sources"]):
                        logger.warning(f"[FIX] Đã phát hiện và sửa nguồn 'sources' bị sai type ở {story_folder}")
                        metadata["sources"] = fixed_sources
                        with open(meta_path, "w", encoding="utf-8") as f:
                            json.dump(metadata, f, ensure_ascii=False, indent=4)

        real_total = await get_real_total_chapters(metadata, adapter)
        chapter_count = recount_chapters(story_folder)
        dead_count = count_dead_chapters(story_folder)

        # Luôn cập nhật lại metadata cho đúng số chương thực tế từ web
        if metadata.get("total_chapters_on_site") != real_total:
            metadata["total_chapters_on_site"] = real_total
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            logger.info(f"[RECOUNT] Cập nhật lại metadata: {real_total} chương cho '{os.path.basename(story_folder)}'")

        logger.info(f"[CHECK] {metadata.get('title')} - txt: {chapter_count} / web: {real_total}")

        # Move nếu đủ chương thực tế trên web (bao gồm chương đã đánh dấu fail)
        if chapter_count + dead_count >= real_total and real_total > 0:
            genre_name = "Unknown"
            if metadata.get('categories') and isinstance(metadata['categories'], list) and metadata['categories']:
                genre_name = metadata['categories'][0].get('name')
            await move_story_to_completed(story_folder, genre_name)
            if genre_name not in genre_complete_checked:
                genre_url = genre_name_to_url.get(genre_name)
                if genre_url:
                    await check_genre_complete_and_notify(genre_name, genre_url, site_key)
                genre_complete_checked.add(genre_name)
        else:
            # Cảnh báo thiếu chương (chỉ 1 lần/truyện)
            title = metadata.get('title')
            if title and title not in notified_titles:
                warning_msg = (
                    f"[WARNING] Sau crawl bù, truyện '{title}' vẫn thiếu chương: {chapter_count}+{dead_count}/{real_total}"
                )
                logger.warning(warning_msg)
                await send_telegram_notify(warning_msg)
                notified_titles.add(title)

        # Fix metadata nếu thiếu trường quan trọng (chỉ gọi 1 lần)
        fields_required = ['description', 'author', 'cover', 'categories', 'title', 'total_chapters_on_site']
        meta_ok = all(metadata.get(f) for f in fields_required)
        if not meta_ok:
            logger.info(f"[SKIP] '{story_folder}' thiếu trường quan trọng, sẽ cố gắng lấy lại metadata...")
            details = await cached_get_story_details(
                adapter, metadata.get("url"), metadata.get("title")
            )
            if update_metadata_from_details(metadata, details):#type:ignore
                meta_ok = all(metadata.get(f) for f in fields_required)
                if meta_ok:
                    logger.info(f"[FIXED] Đã bổ sung metadata đủ cho '{metadata.get('title')}'")
                    with open(meta_path, "w", encoding="utf-8") as f:
                        json.dump(metadata, f, ensure_ascii=False, indent=4)
                else:
                    logger.error(f"[ERROR] Không lấy đủ metadata cho '{metadata.get('title')}'! Sẽ bỏ qua move.")
                    continue
            else:
                logger.error(f"[ERROR] Không lấy đủ metadata cho '{metadata.get('title')}'! Sẽ bỏ qua move.")
                continue

    logger.info(f"[TASK END] Task {site_key} đã xong toàn bộ story.")

def recount_chapters(story_folder):
    """Trả về số file .txt thực tế trong folder truyện."""
    return len([f for f in os.listdir(story_folder) if f.endswith('.txt')])



async def check_genre_complete_and_notify(genre_name, genre_url, site_key):
    adapter = get_adapter(site_key)
    stories_on_web = await  adapter.get_all_stories_from_genre(genre_name, genre_url)
    completed_folder = os.path.join(COMPLETED_FOLDER, genre_name)
    completed_folders = os.listdir(completed_folder)
    completed_titles = []
    for folder in completed_folders:
        meta_path = os.path.join(completed_folder, folder, "metadata.json")
        if os.path.exists(meta_path):
            lock_path = meta_path + ".lock"
            lock = FileLock(lock_path, timeout=30)
            with lock:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                    completed_titles.append(meta.get("title"))
    missing = [story for story in stories_on_web if story["title"] not in completed_titles]
    if not missing:
        await send_telegram_notify(f"🎉 Đã crawl xong **TẤT CẢ** truyện của thể loại [{genre_name}] trên web!")

async def fix_metadata_with_retry(metadata, metadata_path, story_folder, site_key=None, adapter=None):
    from scraper import make_request

    def is_url_for_site(url, site_key):
        from config.config import BASE_URLS
        base = BASE_URLS.get(site_key)
        return base and url and url.startswith(base)

    if metadata.get("skip_crawl", False):
        logger.info(f"[SKIP] Truyện '{metadata.get('title')}' đã bị đánh dấu bỏ qua.")
        return False

    retry_count = metadata.get("meta_retry_count", 0)
    url = metadata.get("url")
    title = metadata.get("title")
    total_chapters = metadata.get("total_chapters_on_site")

    # === Ưu tiên gán lại url từ sources nếu thiếu ===
    if not url and metadata.get("sources"):
        for src in metadata["sources"]:
            s_url = src.get("url") if isinstance(src, dict) else src
            if s_url:
                url = s_url
                metadata["url"] = url
                logger.info(f"[FIX] Gán lại url từ sources cho '{story_folder}': {url}")
                break

    # === Ưu tiên lấy title từ sources nếu thiếu ===
    if not title and metadata.get("sources"):
        for src in metadata["sources"]:
            s_title = src.get("title") if isinstance(src, dict) else None
            if s_title:
                title = s_title
                metadata["title"] = title
                logger.info(f"[FIX] Gán lại title từ sources: {title}")
                break

    # === Gán lại title nếu vẫn thiếu bằng tên thư mục ===
    if not title:
        folder_title = os.path.basename(story_folder).replace("-", " ").title()
        metadata["title"] = title = folder_title
        logger.info(f"[FALLBACK] Gán title từ folder: {title}")

    # === Nếu vẫn thiếu url, đoán từ base_url + slug ===
    if not url and site_key:
        from config.config import BASE_URLS
        base_url = BASE_URLS.get(site_key, "").rstrip("/")
        slug = os.path.basename(story_folder)
        guessed_url = f"{base_url}/{slug}"
        try:
            resp = await make_request(guessed_url, site_key)
            if resp and getattr(resp, "status_code", None) == 200:
                url = guessed_url
                metadata["url"] = url
                logger.info(f"[GUESS] Đoán url thành công cho '{story_folder}': {url}")
        except Exception as e:
            logger.warning(f"[GUESS FAIL] Lỗi khi request guessed url {guessed_url}: {e}")

    # === Nếu vẫn không có url/title thì skip ===
    if not url or not title:
        metadata["skip_crawl"] = True
        metadata["skip_reason"] = "missing_url_title"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        logger.warning(f"[SKIP] '{story_folder}' thiếu url/title → bỏ qua.")
        return False

    # === Lấy lại metadata nếu thiếu total_chapters ===
    retry_count = metadata.get("meta_retry_count", 0)
    while retry_count < 3 and (not total_chapters or total_chapters < 1):
        logger.info(f"[META] Đang lấy metadata lần {retry_count+1} từ web...")
        adapter = get_adapter(site_key)  # type:ignore
        details = await cached_get_story_details(adapter, url, title)
        update_metadata_from_details(metadata, details) #type:ignore
        retry_count += 1
        metadata["meta_retry_count"] = retry_count

        if isinstance(details, dict) and details.get("total_chapters_on_site"):
            metadata.update(details)
            metadata["total_chapters_on_site"] = details["total_chapters_on_site"]
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            logger.info(f"[META] Đã cập nhật thành công metadata cho '{title}'")
            return True
        else:
            logger.warning(f"[META FAIL] Không lấy được metadata hợp lệ từ {url}")
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)

    # === Nếu thất bại sau 3 lần: xoá folder ===
    if not metadata.get("total_chapters_on_site", 0):
        try:
            shutil.rmtree(story_folder)
            logger.info(f"[REMOVE] Xoá '{story_folder}' do không lấy được metadata.")
        except Exception as e:
            logger.error(f"[ERROR] Không thể xoá folder '{story_folder}': {e}")
        return False

    return True



def autofix_metadata(story_folder, site_key=None):
    if not os.path.exists(story_folder):
        logger.warning(f"[AUTO-FIX] Không tồn tại folder để autofix: {story_folder}")
        return {}
    folder_name = os.path.basename(story_folder)
    chapter_count = recount_chapters(story_folder)
    guessed_url = f"{BASE_URLS.get(site_key, '').rstrip('/')}/{folder_name}" if site_key else None
    parent_folder = os.path.basename(os.path.dirname(story_folder))
    genre_guess = parent_folder if parent_folder and parent_folder != os.path.basename(DATA_FOLDER) else None
    meta = {
        "title": folder_name.replace("-", " ").replace("_", " ").title().strip(),
        "url": guessed_url,
        "total_chapters_on_site": chapter_count,
        "description": "",
        "author": "",
        "cover": "",
        "categories": [{"name": genre_guess}] if genre_guess else [],
        "sources": [],
        "skip_crawl": False,
        "site_key": site_key
    }
    meta_path = os.path.join(story_folder, "metadata.json") 
    # Chỉ tạo file nếu chưa tồn tại
    if not os.path.exists(meta_path):
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)
        logger.info(f"[AUTO-FIX] Đã tạo metadata cho '{meta['title']}' ({chapter_count} chương)")
    return meta


def split_to_batches(items, num_batches):
    k, m = divmod(len(items), num_batches)
    return [items[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(num_batches)]

def get_auto_batch_count(fixed=None, default=10, min_batch=1, max_batch=20, num_items=None):
    if fixed is not None:
        return fixed
    batch = default
    if num_items:
        batch = min(batch, num_items)
    return min(batch, max_batch)

async def crawl_story_with_limit(
    site_key: str,
    session,
    missing_chapters: list,
    metadata: dict,
    current_category: dict,
    story_folder: str,
    crawl_state: dict,
    num_batches: int = 10,
    state_file: str = None,  # type: ignore
    adapter=None,
    chapters_all: list | None = None,
):
    await STORY_SEM.acquire()
    try:
        chapters_pool = chapters_all or missing_chapters
        if not chapters_pool:
            logger.info(f"[SKIP] Không có dữ liệu chương để crawl cho '{metadata.get('title')}'")
            return

        index_candidates = []
        for ch in missing_chapters or []:
            if not isinstance(ch, dict):
                continue
            idx = ch.get("idx")
            if isinstance(idx, int):
                index_candidates.append(idx)
                continue
            real_index = ch.get("index")
            if isinstance(real_index, int):
                index_candidates.append(real_index - 1)
                continue

            title = ch.get("title") or ""
            if isinstance(ch.get("aligned_index"), int):
                real_num = ch["aligned_index"]
            else:
                real_num = extract_real_chapter_number(title)
            if isinstance(real_num, int):
                index_candidates.append(real_num - 1)

        if not index_candidates:
            index_candidates = list(range(len(chapters_pool)))

        unique_indexes = sorted({idx for idx in index_candidates if isinstance(idx, int) and idx >= 0})
        if not unique_indexes:
            logger.info(f"[SKIP] Không tìm được index hợp lệ để crawl missing cho '{metadata.get('title')}'")
            return

        batch_count = max(1, min(num_batches, len(unique_indexes)))
        batches = split_to_batches(unique_indexes, batch_count)

        for batch_idx, batch in enumerate(batches):
            if not batch:
                continue
            logger.info(f"[Batch {batch_idx+1}/{len(batches)}] Crawl {len(batch)} chương (indexes: {batch})")
            await crawl_missing_with_limit(
                site_key,
                session,
                chapters_pool,
                metadata,
                current_category,
                story_folder,
                crawl_state,
                target_indexes=set(batch),
                state_file=state_file,
                adapter=adapter,
            )
            await smart_delay()
    finally:
        STORY_SEM.release()
    logger.info(f"[DONE-CRAWL-STORY-WITH-LIMIT] {metadata.get('title')}")


async def crawl_missing_with_limit(
    site_key: str,
    session,
    chapters_all: list,
    metadata: dict,
    current_category: dict,
    story_folder: str,
    crawl_state: dict,
    num_batches: int = 10,
    state_file: str = None,  # type: ignore
    adapter=None,
    target_indexes: set[int] | None = None,
):
    if not state_file:
        state_file = get_missing_worker_state_file(site_key)
    logger.info(f"[START] Crawl missing for {metadata['title']} ...")
    num_targets = len(target_indexes) if target_indexes else len(chapters_all or [])
    timeout_seconds = calculate_missing_crawl_timeout(num_targets)
    async with SEM:
        result = await asyncio.wait_for(
            crawl_missing_chapters_for_story(
                site_key,
                session,
                chapters_all,
                metadata,
                current_category,
                story_folder,
                crawl_state,
                num_batches,
                state_file=state_file,
                adapter=adapter,
                target_indexes=target_indexes,
            ),
            timeout=timeout_seconds,
        )
    logger.info(
        f"[DONE] Crawl missing for {metadata['title']} (timeout={timeout_seconds:.0f}s, targets={num_targets}) ..."
    )
    return result
  
