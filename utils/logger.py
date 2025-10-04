"""Centralised logging helpers for the crawler."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from logging.handlers import RotatingFileHandler
from typing import Dict

LOG_FOLDER = "logs"
DEFAULT_MAX_BYTES = 5 * 1024 * 1024
DEFAULT_BACKUP_COUNT = 3

# Bảo đảm thư mục logs tồn tại
os.makedirs(LOG_FOLDER, exist_ok=True)

_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_CONSOLE_LEVEL = logging.INFO

_CATEGORY_FILE_MAP: Dict[str, str] = {
    "core": "crawler.log",
    "anti_bot": "anti_bot.log",
    "chapter_error": "chapter_errors.log",
    "progress": "progress.log",
}


def _build_rotating_handler(filename: str) -> RotatingFileHandler:
    handler = RotatingFileHandler(
        filename,
        maxBytes=DEFAULT_MAX_BYTES,
        backupCount=DEFAULT_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    return handler


@lru_cache(maxsize=None)
def get_logger(category: str = "core") -> logging.Logger:
    """Return a configured logger for ``category``.

    The crawler historically relied on a single global logger. In order to
    simplify troubleshooting we expose dedicated loggers per functional area
    (anti-bot, chương lỗi, tiến trình hoàn thành, ...). The returned logger
    writes both to stdout and to a rotating file inside ``logs/``.
    """

    category = category or "core"
    logger_name = f"StoryFlow.{category}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    console_handler = logging.StreamHandler()
    console_handler.setLevel(_CONSOLE_LEVEL)
    console_handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    logger.addHandler(console_handler)

    file_name = _CATEGORY_FILE_MAP.get(category, _CATEGORY_FILE_MAP["core"])
    file_path = os.path.join(LOG_FOLDER, file_name)
    file_handler = _build_rotating_handler(file_path)
    logger.addHandler(file_handler)

    return logger


logger = get_logger("core")
anti_bot_logger = get_logger("anti_bot")
chapter_error_logger = get_logger("chapter_error")
progress_logger = get_logger("progress")

__all__ = [
    "anti_bot_logger",
    "chapter_error_logger",
    "get_logger",
    "logger",
    "progress_logger",
]
