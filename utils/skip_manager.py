import os
import json
import time
import asyncio
from typing import Dict, Any

from config.config import SKIPPED_STORIES_FILE
from utils.logger import logger
from utils.notifier import send_telegram_notify
from utils.chapter_utils import slugify_title

SKIPPED_STORIES: Dict[str, Dict[str, Any]] = {}


def load_skipped_stories() -> Dict[str, Dict[str, Any]]:
    """Load skipped stories from file once at startup."""
    global SKIPPED_STORIES
    if SKIPPED_STORIES:
        return SKIPPED_STORIES
    if os.path.exists(SKIPPED_STORIES_FILE):
        try:
            with open(SKIPPED_STORIES_FILE, "r", encoding="utf-8") as f:
                SKIPPED_STORIES = json.load(f)
        except Exception as ex:
            logger.error(f"[SKIP] Lỗi đọc file skip: {ex}")
            SKIPPED_STORIES = {}
    else:
        SKIPPED_STORIES = {}
    return SKIPPED_STORIES


def save_skipped_stories() -> None:
    """Persist skip data to file."""
    try:
        with open(SKIPPED_STORIES_FILE, "w", encoding="utf-8") as f:
            json.dump(SKIPPED_STORIES, f, ensure_ascii=False, indent=2)
    except Exception as ex:
        logger.error(f"[SKIP] Không ghi được skipped_stories: {ex}")


def is_story_skipped(story: Dict[str, Any]) -> bool:
    slug = slugify_title(story["title"])
    return slug in SKIPPED_STORIES


def get_all_skipped_stories() -> Dict[str, Dict[str, Any]]:
    return SKIPPED_STORIES


def mark_story_as_skipped(story: Dict[str, Any], reason: str = "") -> None:
    """Mark story as permanently skipped and notify."""
    slug = slugify_title(story["title"])
    SKIPPED_STORIES[slug] = {
        "url": story.get("url"),
        "title": story.get("title"),
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "reason": reason,
    }
    save_skipped_stories()
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[SKIP] {story.get('title')} - {reason} ({now})".strip()
    logger.warning(msg)
    try:
        asyncio.create_task(send_telegram_notify(msg, status="warning"))
    except RuntimeError:
        # in case called outside event loop
        pass
