import asyncio
import glob
import json
import os
from typing import Any, Dict, List
import aiofiles
from config.config import COMPLETED_FOLDER, DATA_FOLDER, STATE_FILE, get_state_file
from utils import logger
from utils.io_utils import safe_write_file
from utils.logger import logger

CSTATE_LOCK = asyncio.Lock()

async def load_crawl_state(state_file) -> Dict[str, Any]:
    if not state_file:
        from config.config import STATE_FILE
        state_file = STATE_FILE
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, state_file)
    if exists:
        try:
            async with aiofiles.open(state_file, 'r', encoding='utf-8') as f:
                data = await f.read()
            state = json.loads(data)
            logger.info(f"Đã tải trạng thái crawl từ {state_file}: {state}")
            return state
        except Exception as e:
            logger.error(f"Lỗi khi tải trạng thái crawl từ {state_file}: {e}. Bắt đầu crawl mới.")
    return {}

async def save_crawl_state(state: Dict[str, Any], state_file: str) -> None:
    async with CSTATE_LOCK:
        try:
            content = json.dumps(state, ensure_ascii=False, indent=4)
            await safe_write_file(state_file, content)
            logger.info(f"Đã lưu trạng thái crawl vào {state_file}")
        except Exception as e:
            logger.error(f"Lỗi khi lưu trạng thái crawl vào {state_file}: {e}")

async def clear_specific_state_keys(state: Dict[str, Any], keys_to_remove: List[str], state_file: str) -> None:
    updated = False
    for key in keys_to_remove:
        if key in state:
            del state[key]
            updated = True
            logger.debug(f"Đã xóa key '{key}' khỏi trạng thái crawl.")
    if updated:
        await save_crawl_state(state, state_file)

async def clear_crawl_state_component(state: Dict[str, Any], component_key: str, state_file: str) -> None:
    if component_key in state:
        del state[component_key]
        if component_key == "current_genre_url":
            state.pop("current_story_url", None)
            state.pop("current_story_index_in_genre", None)
            state.pop("processed_chapter_urls_for_current_story", None)
        elif component_key == "current_story_url":
            state.pop("processed_chapter_urls_for_current_story", None)
    await save_crawl_state(state, state_file)

async def clear_all_crawl_state(state_file:str) -> None:
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, state_file)
    if exists:
        try:
            await loop.run_in_executor(None, os.remove, state_file)
            logger.info(f"Đã xóa file trạng thái crawl: {state_file}")
        except Exception as e:
            logger.error(f"Lỗi khi xóa file trạng thái crawl: {e}")

def is_genre_completed(genre_name):
    src_genre_folder = os.path.join(DATA_FOLDER, genre_name)
    completed_genre_folder = os.path.join(COMPLETED_FOLDER, genre_name)
    src_count = len(os.listdir(src_genre_folder)) if os.path.exists(src_genre_folder) else 0
    completed_count = len(os.listdir(completed_genre_folder)) if os.path.exists(completed_genre_folder) else 0
    return src_count == 0 and completed_count > 0



def merge_all_missing_workers_to_main(site_key):
    import glob
    main_state_file = get_state_file(site_key)
    main_state = {}
    if os.path.exists(main_state_file):
        with open(main_state_file, "r", encoding="utf-8") as f:
            main_state = json.load(f)

    # Tìm tất cả file _missing_worker*.json (nếu nhiều worker phụ)
    files = glob.glob(f"{main_state_file.replace('.json', '')}_missing_worker*.json")
    all_completed = set(main_state.get("globally_completed_story_urls", []))
    for fname in files:
        with open(fname, "r", encoding="utf-8") as f:
            missing_state = json.load(f)
        all_completed |= set(missing_state.get("globally_completed_story_urls", []))
        # Xóa file phụ sau khi merge
        os.remove(fname)
    main_state["globally_completed_story_urls"] = sorted(all_completed)
    with open(main_state_file, "w", encoding="utf-8") as f:
        json.dump(main_state, f, ensure_ascii=False, indent=4)


def get_missing_worker_state_file(site_key):
    base = get_state_file(site_key)
    return base.replace('.json', '_missing_worker.json')