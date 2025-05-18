import asyncio
import json
import os
from typing import Any, Dict, List
import aiofiles
from config.config import COMPLETED_FOLDER, DATA_FOLDER, STATE_FILE
from utils import logger
from utils.io_utils import safe_write_file
from utils.logger import logger

CSTATE_LOCK = asyncio.Lock()

async def load_crawl_state() -> Dict[str, Any]:
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, STATE_FILE)
    if exists:
        try:
            async with aiofiles.open(STATE_FILE, 'r', encoding='utf-8') as f:
                data = await f.read()
            state = json.loads(data)
            logger.info(f"Đã tải trạng thái crawl từ {STATE_FILE}: {state}")
            return state
        except Exception as e:
            logger.error(f"Lỗi khi tải trạng thái crawl từ {STATE_FILE}: {e}. Bắt đầu crawl mới.")
    return {}

async def save_crawl_state(state: Dict[str, Any]) -> None:
    async with CSTATE_LOCK:
        try:
            content = json.dumps(state, ensure_ascii=False, indent=4)
            await safe_write_file(STATE_FILE, content)
            logger.info(f"Đã lưu trạng thái crawl vào {STATE_FILE}")
        except Exception as e:
            logger.error(f"Lỗi khi lưu trạng thái crawl vào {STATE_FILE}: {e}")

async def clear_specific_state_keys(state: Dict[str, Any], keys_to_remove: List[str]) -> None:
    updated = False
    for key in keys_to_remove:
        if key in state:
            del state[key]
            updated = True
            logger.debug(f"Đã xóa key '{key}' khỏi trạng thái crawl.")
    if updated:
        await save_crawl_state(state)

async def clear_crawl_state_component(state: Dict[str, Any], component_key: str) -> None:
    if component_key in state:
        del state[component_key]
        if component_key == "current_genre_url":
            state.pop("current_story_url", None)
            state.pop("current_story_index_in_genre", None)
            state.pop("processed_chapter_urls_for_current_story", None)
        elif component_key == "current_story_url":
            state.pop("processed_chapter_urls_for_current_story", None)
    await save_crawl_state(state)

async def clear_all_crawl_state() -> None:
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, STATE_FILE)
    if exists:
        try:
            await loop.run_in_executor(None, os.remove, STATE_FILE)
            logger.info(f"Đã xóa file trạng thái crawl: {STATE_FILE}")
        except Exception as e:
            logger.error(f"Lỗi khi xóa file trạng thái crawl: {e}")

def is_genre_completed(genre_name):
    src_genre_folder = os.path.join(DATA_FOLDER, genre_name)
    completed_genre_folder = os.path.join(COMPLETED_FOLDER, genre_name)
    src_count = len(os.listdir(src_genre_folder)) if os.path.exists(src_genre_folder) else 0
    completed_count = len(os.listdir(completed_genre_folder)) if os.path.exists(completed_genre_folder) else 0
    return src_count == 0 and completed_count > 0