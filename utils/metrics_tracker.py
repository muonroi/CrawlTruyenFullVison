"""Utility helpers to track crawl progress and expose a lightweight dashboard."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Dict, Optional

from utils.logger import logger


@dataclass
class StoryProgress:
    story_id: str
    title: str
    total_chapters: int
    crawled_chapters: int = 0
    missing_chapters: int = 0
    status: str = "queued"
    primary_site: Optional[str] = None
    last_source: Optional[str] = None
    last_error: Optional[str] = None
    started_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    cooldown_until: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.story_id,
            "title": self.title,
            "total_chapters": self.total_chapters,
            "crawled_chapters": self.crawled_chapters,
            "missing_chapters": self.missing_chapters,
            "status": self.status,
            "primary_site": self.primary_site,
            "last_source": self.last_source,
            "last_error": self.last_error,
            "started_at": _to_iso(self.started_at),
            "updated_at": _to_iso(self.updated_at),
            "completed_at": _to_iso(self.completed_at) if self.completed_at else None,
            "cooldown_until": _to_iso(self.cooldown_until) if self.cooldown_until else None,
        }


@dataclass
class SiteHealthSnapshot:
    site_key: str
    success: int = 0
    failure: int = 0
    last_error: Optional[str] = None
    last_alert_at: Optional[float] = None
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        total = self.success + self.failure
        failure_rate = (self.failure / total) if total else 0.0
        return {
            "site_key": self.site_key,
            "success": self.success,
            "failure": self.failure,
            "failure_rate": round(failure_rate, 4),
            "last_error": self.last_error,
            "last_alert_at": _to_iso(self.last_alert_at) if self.last_alert_at else None,
            "updated_at": _to_iso(self.updated_at),
        }


def _to_iso(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))
    except Exception:  # pragma: no cover - defensive guard for invalid timestamps
        return None


class CrawlMetricsTracker:
    """In-memory tracker that periodically dumps progress to ``logs/dashboard.json``."""

    def __init__(self) -> None:
        dashboard_file = os.environ.get("STORYFLOW_DASHBOARD_FILE")
        if dashboard_file:
            self._dashboard_file = dashboard_file
            os.makedirs(os.path.dirname(dashboard_file), exist_ok=True)
        else:
            logs_dir = "logs"
            os.makedirs(logs_dir, exist_ok=True)
            self._dashboard_file = os.path.join(logs_dir, "dashboard.json")

        self._lock = Lock()
        self._stories_in_progress: Dict[str, StoryProgress] = {}
        self._stories_completed: Dict[str, StoryProgress] = {}
        self._stories_skipped: Dict[str, StoryProgress] = {}
        self._queues: Dict[str, int] = {"skipped": 0}
        self._site_stats: Dict[str, SiteHealthSnapshot] = {}

        self._load_existing()

    # ------------------------------------------------------------------
    # Story tracking helpers
    # ------------------------------------------------------------------
    def story_started(
        self,
        story_id: str,
        title: str,
        total_chapters: int,
        *,
        primary_site: Optional[str] = None,
    ) -> None:
        progress = StoryProgress(
            story_id=story_id,
            title=title,
            total_chapters=total_chapters or 0,
            missing_chapters=max(total_chapters or 0, 0),
            status="running",
            primary_site=primary_site,
        )
        with self._lock:
            self._stories_in_progress[story_id] = progress
            self._stories_completed.pop(story_id, None)
            self._stories_skipped.pop(story_id, None)
            self._persist_locked()

    def update_story_progress(
        self,
        story_id: str,
        *,
        crawled_chapters: Optional[int] = None,
        missing_chapters: Optional[int] = None,
        status: Optional[str] = None,
        last_source: Optional[str] = None,
        last_error: Optional[str] = None,
        cooldown_until: Optional[float] = None,
    ) -> None:
        with self._lock:
            story = self._stories_in_progress.get(story_id)
            if not story:
                return
            if crawled_chapters is not None:
                story.crawled_chapters = max(crawled_chapters, 0)
            if missing_chapters is not None:
                story.missing_chapters = max(missing_chapters, 0)
            if status:
                story.status = status
            if last_source:
                story.last_source = last_source
            if last_error:
                story.last_error = last_error
            story.cooldown_until = cooldown_until
            story.updated_at = time.time()
            self._persist_locked()

    def story_on_cooldown(self, story_id: str, cooldown_until: float) -> None:
        self.update_story_progress(
            story_id,
            status="cooldown",
            cooldown_until=cooldown_until,
        )

    def story_completed(self, story_id: str) -> None:
        with self._lock:
            story = self._stories_in_progress.pop(story_id, None)
            if not story:
                return
            story.status = "completed"
            story.completed_at = time.time()
            story.updated_at = story.completed_at
            story.cooldown_until = None
            story.missing_chapters = 0
            self._stories_completed[story_id] = story
            self._persist_locked()

    def story_failed(self, story_id: str, reason: str) -> None:
        with self._lock:
            story = self._stories_in_progress.get(story_id)
            if story:
                story.status = "failed"
                story.last_error = reason
                story.updated_at = time.time()
            self._persist_locked()

    def story_skipped(self, story_id: str, title: str, reason: str) -> None:
        with self._lock:
            story = self._stories_in_progress.pop(story_id, None)
            if story is None:
                story = StoryProgress(
                    story_id=story_id,
                    title=title,
                    total_chapters=0,
                    missing_chapters=0,
                )
            story.status = "skipped"
            story.last_error = reason
            story.updated_at = time.time()
            story.completed_at = None
            story.cooldown_until = None
            self._stories_skipped[story_id] = story
            self._persist_locked()

    # ------------------------------------------------------------------
    # Queue & site helpers
    # ------------------------------------------------------------------
    def update_skipped_queue_size(self, count: int) -> None:
        with self._lock:
            self._queues["skipped"] = max(int(count), 0)
            self._persist_locked()

    def update_site_health(
        self,
        site_key: str,
        *,
        success_delta: int = 0,
        failure_delta: int = 0,
        last_error: Optional[str] = None,
        last_alert_at: Optional[float] = None,
    ) -> None:
        with self._lock:
            snapshot = self._site_stats.get(site_key)
            if not snapshot:
                snapshot = SiteHealthSnapshot(site_key=site_key)
                self._site_stats[site_key] = snapshot
            snapshot.success = max(snapshot.success + success_delta, 0)
            snapshot.failure = max(snapshot.failure + failure_delta, 0)
            snapshot.updated_at = time.time()
            if last_error:
                snapshot.last_error = last_error
            if last_alert_at:
                snapshot.last_alert_at = last_alert_at
            self._persist_locked()

    # ------------------------------------------------------------------
    # Snapshot helpers
    # ------------------------------------------------------------------
    def get_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return self._build_snapshot_locked()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load_existing(self) -> None:
        if not os.path.exists(self._dashboard_file):
            return
        try:
            with open(self._dashboard_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.warning(f"[METRICS] Không đọc được dashboard.json: {exc}")
            return

        stories = payload.get("stories", {})
        for item in stories.get("in_progress", []):
            progress = _story_from_payload(item)
            if progress:
                self._stories_in_progress[progress.story_id] = progress
        for item in stories.get("completed", []):
            progress = _story_from_payload(item)
            if progress:
                self._stories_completed[progress.story_id] = progress
        for item in stories.get("skipped", []):
            progress = _story_from_payload(item)
            if progress:
                self._stories_skipped[progress.story_id] = progress

        queues = payload.get("queues", {})
        if isinstance(queues, dict):
            for key, value in queues.items():
                try:
                    self._queues[key] = int(value)
                except Exception:  # pragma: no cover - defensive guard
                    continue

        for item in payload.get("sites", []):
            if not isinstance(item, dict):
                continue
            site_key = item.get("site_key")
            if not site_key:
                continue
            snapshot = SiteHealthSnapshot(
                site_key=site_key,
                success=int(item.get("success", 0)),
                failure=int(item.get("failure", 0)),
                last_error=item.get("last_error"),
                last_alert_at=_from_iso(item.get("last_alert_at")),
            )
            snapshot.updated_at = time.time()
            self._site_stats[site_key] = snapshot

    def _persist_locked(self) -> None:
        snapshot = self._build_snapshot_locked()
        try:
            with open(self._dashboard_file, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, ensure_ascii=False, indent=2)
        except Exception as exc:  # pragma: no cover - IO guard
            logger.error(f"[METRICS] Không ghi được dashboard.json: {exc}")

    def _build_snapshot_locked(self) -> Dict[str, Any]:
        updated_at = time.time()
        stories_in_progress = [story.to_dict() for story in self._stories_in_progress.values()]
        stories_completed = [story.to_dict() for story in self._stories_completed.values()]
        stories_skipped = [story.to_dict() for story in self._stories_skipped.values()]
        total_missing = sum(story.missing_chapters for story in self._stories_in_progress.values())
        return {
            "updated_at": _to_iso(updated_at),
            "stories": {
                "in_progress": stories_in_progress,
                "completed": stories_completed,
                "skipped": stories_skipped,
            },
            "aggregates": {
                "stories_in_progress": len(stories_in_progress),
                "stories_completed": len(stories_completed),
                "stories_skipped": len(stories_skipped),
                "total_missing_chapters": total_missing,
                "skipped_queue_size": self._queues.get("skipped", 0),
            },
            "queues": dict(self._queues),
            "sites": [snapshot.to_dict() for snapshot in self._site_stats.values()],
        }


def _story_from_payload(payload: Dict[str, Any]) -> Optional[StoryProgress]:
    if not isinstance(payload, dict):
        return None
    story_id = payload.get("id")
    title = payload.get("title")
    if not story_id or not title:
        return None
    progress = StoryProgress(
        story_id=story_id,
        title=title,
        total_chapters=int(payload.get("total_chapters", 0)),
        crawled_chapters=int(payload.get("crawled_chapters", 0)),
        missing_chapters=int(payload.get("missing_chapters", 0)),
        status=payload.get("status", "queued"),
        primary_site=payload.get("primary_site"),
        last_source=payload.get("last_source"),
        last_error=payload.get("last_error"),
    )
    progress.started_at = _from_iso(payload.get("started_at")) or time.time()
    progress.updated_at = _from_iso(payload.get("updated_at")) or progress.started_at
    progress.completed_at = _from_iso(payload.get("completed_at"))
    progress.cooldown_until = _from_iso(payload.get("cooldown_until"))
    return progress


def _from_iso(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        struct = time.strptime(value, "%Y-%m-%d %H:%M:%S")
        return time.mktime(struct)
    except Exception:  # pragma: no cover - defensive guard for malformed input
        return None


metrics_tracker = CrawlMetricsTracker()

__all__ = ["metrics_tracker", "CrawlMetricsTracker"]
