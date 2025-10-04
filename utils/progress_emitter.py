"""Async helpers to push crawl progress updates to Kafka."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict

from config import config as app_config
from utils.logger import get_logger

logger = get_logger("progress")


async def _send_event(event: Dict[str, Any], topic: str) -> None:
    """Forward ``event`` to Kafka using the shared async producer."""

    try:
        from kafka.kafka_producer import send_job

        await send_job(event, topic=topic)
    except Exception as exc:  # pragma: no cover - network/IO guard
        logger.warning("[ProgressEmitter] Không thể gửi event tiến trình: %s", exc)


def emit_progress_event(category: str, payload: Dict[str, Any]) -> None:
    """Schedule a Kafka publish for ``payload`` if progress streaming is enabled."""

    topic = getattr(app_config, "PROGRESS_TOPIC", None)
    if not topic:
        return

    event = {
        "category": category,
        "payload": payload,
        "version": 1,
        "emitted_at": time.time(),
    }

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # pragma: no cover - invoked from sync context without loop
        logger.debug(
            "[ProgressEmitter] Không tìm thấy event loop đang chạy, bỏ qua event %s",
            category,
        )
        return

    loop.create_task(_send_event(event, topic))


__all__ = ["emit_progress_event"]
