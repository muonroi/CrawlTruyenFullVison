"""Kafka consumer that renders crawl progress bars for easier monitoring."""

from __future__ import annotations

import asyncio
import json
import math
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from config import config as app_config
from utils.logger import get_logger

logger = get_logger("progress")


@dataclass
class StoryView:
    story_id: str
    title: str
    total_chapters: int
    crawled_chapters: int
    status: str
    percent: int
    genre_name: Optional[str] = None
    primary_site: Optional[str] = None
    last_source: Optional[str] = None
    last_error: Optional[str] = None
    updated_at_ts: Optional[float] = None
    cooldown_until_ts: Optional[float] = None
    remaining_chapters: Optional[int] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: Dict[str, Any], percent_fallback: int) -> "StoryView":
        total = int(payload.get("total_chapters") or 0)
        crawled = int(payload.get("crawled_chapters") or 0)
        percent = _coerce_percent(payload.get("percent"), crawled, total, percent_fallback)

        return cls(
            story_id=str(payload.get("id")),
            title=str(payload.get("title") or "Không rõ"),
            total_chapters=total,
            crawled_chapters=crawled,
            status=str(payload.get("status") or "unknown"),
            percent=percent,
            genre_name=_as_optional_str(payload.get("genre_name")),
            primary_site=_as_optional_str(payload.get("primary_site")),
            last_source=_as_optional_str(payload.get("last_source")),
            last_error=_as_optional_str(payload.get("last_error")),
            updated_at_ts=_as_optional_float(payload.get("updated_at_ts")),
            cooldown_until_ts=_as_optional_float(payload.get("cooldown_until_ts")),
            remaining_chapters=_as_optional_int(payload.get("remaining_chapters")),
            raw=dict(payload),
        )

    def render_line(self) -> str:
        bar = _render_bar(self.percent)
        total = self.total_chapters if self.total_chapters > 0 else "?"
        progress = f"{self.crawled_chapters}/{total}"
        details: List[str] = []
        if self.genre_name:
            details.append(self.genre_name)
        if self.primary_site:
            details.append(f"Site: {self.primary_site}")
        if self.last_source:
            details.append(f"Nguồn: {self.last_source}")
        if self.cooldown_until_ts:
            details.append(
                "Cooldown tới: "
                + time.strftime("%H:%M:%S", time.localtime(self.cooldown_until_ts))
            )
        if self.last_error and self.status in {"failed", "skipped"}:
            details.append(f"Lỗi: {self.last_error}")
        detail_str = " | ".join(details)
        suffix = f" | {detail_str}" if detail_str else ""
        return (
            f"{bar} {self.percent:3d}% | {self.title} [{self.status}] ({progress})"
            f"{suffix}"
        )


class ProgressDashboard:
    def __init__(self) -> None:
        self._stories: Dict[str, StoryView] = {}
        self._last_render: Optional[str] = None

    def apply_story_event(self, action: str, story_payload: Dict[str, Any]) -> None:
        story_id = story_payload.get("id")
        if not story_id:
            return

        view = StoryView.from_payload(story_payload, percent_fallback=0)
        view.raw["action"] = action
        self._stories[str(story_id)] = view
        self._render()

    def _render(self) -> None:
        if not self._stories:
            output = "Không có truyện nào đang crawl."
        else:
            active: List[StoryView] = sorted(
                self._stories.values(),
                key=lambda item: item.updated_at_ts or 0,
                reverse=True,
            )
            lines = ["=== PROGRESS DASHBOARD ==="]
            for view in active[:10]:
                lines.append(view.render_line())
            output = "\n".join(lines)

        if output != self._last_render:
            logger.info("\n%s", output)
            self._last_render = output


def _as_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _as_optional_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        return None


def _as_optional_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        return None


def _coerce_percent(candidate: Any, crawled: int, total: int, fallback: int) -> int:
    try:
        if candidate is not None:
            return max(0, min(int(candidate), 100))
    except (TypeError, ValueError):  # pragma: no cover
        pass

    if total > 0:
        percent = math.floor((crawled / total) * 100)
        return max(0, min(percent, 100))
    return fallback


def _render_bar(percent: int, width: int = 20) -> str:
    filled = int((percent / 100) * width)
    empty = width - filled
    return f"[{'#' * filled}{'-' * empty}]"


async def _create_consumer() -> Optional[AIOKafkaConsumer]:
    topic = getattr(app_config, "PROGRESS_TOPIC", None)
    if not topic:
        logger.error("[ProgressConsumer] PROGRESS_TOPIC chưa được cấu hình.")
        return None

    group_id = getattr(app_config, "PROGRESS_GROUP_ID", None)
    if not group_id:
        logger.error("[ProgressConsumer] PROGRESS_GROUP_ID chưa được cấu hình.")
        return None

    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=app_config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
    )


async def consume_progress() -> None:
    consumer = await _create_consumer()
    if consumer is None:
        return

    dashboard = ProgressDashboard()
    max_retries = app_config.KAFKA_BOOTSTRAP_MAX_RETRIES
    retry_delay = app_config.KAFKA_BOOTSTRAP_RETRY_DELAY
    attempt = 0

    while True:
        attempt += 1
        try:
            await consumer.start()
            if attempt > 1:
                logger.info(
                    "[ProgressConsumer] Kết nối Kafka thành công sau %s lần thử.", attempt
                )
            break
        except KafkaConnectionError as exc:
            logger.warning(
                "[ProgressConsumer] Không thể kết nối Kafka (attempt %s): %s",
                attempt,
                exc,
            )
            await consumer.stop()
            if max_retries and attempt >= max_retries:
                logger.error("[ProgressConsumer] Vượt quá số lần retry kết nối Kafka.")
                return
            await asyncio.sleep(retry_delay)
        except Exception as exc:  # pragma: no cover - unexpected error guard
            logger.exception("[ProgressConsumer] Lỗi khi khởi tạo consumer: %s", exc)
            await consumer.stop()
            return

    try:
        logger.info(
            "[ProgressConsumer] Đang lắng nghe tiến trình trên topic `%s`...",
            app_config.PROGRESS_TOPIC,
        )
        async for msg in consumer:
            event = msg.value
            if not isinstance(event, dict):
                continue
            if event.get("category") != "story":
                continue
            payload = event.get("payload") or {}
            action = str(payload.get("action") or "progress")
            story_payload = payload.get("story")
            if not isinstance(story_payload, dict):
                continue
            dashboard.apply_story_event(action, story_payload)
    except Exception as exc:  # pragma: no cover - runtime guard
        logger.exception("[ProgressConsumer] Lỗi khi xử lý message Kafka: %s", exc)
    finally:
        await consumer.stop()
        logger.info("[ProgressConsumer] Đã dừng consumer tiến trình.")


async def _shutdown(signal_name: str, task: asyncio.Task) -> None:
    logger.info("[ProgressConsumer] Nhận tín hiệu %s, đang dừng...", signal_name)
    task.cancel()


def main() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(consume_progress())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(_shutdown(s.name, task))
            )
        except NotImplementedError:  # pragma: no cover - Windows compatibility
            pass

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:  # pragma: no cover - manual interruption
        sys.exit(0)
