"""Utility helpers to track crawl progress and expose a lightweight dashboard."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Dict, List, Optional

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


@dataclass
class GenreProgress:
    site_key: str
    genre_name: str
    genre_url: str
    position: Optional[int] = None
    total_genres: Optional[int] = None
    total_pages: Optional[int] = None
    crawled_pages: int = 0
    current_page: Optional[int] = None
    total_stories: int = 0
    processed_stories: int = 0
    active_stories: List[str] = field(default_factory=list)
    status: str = "queued"
    last_error: Optional[str] = None
    started_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "site_key": self.site_key,
            "name": self.genre_name,
            "url": self.genre_url,
            "position": self.position,
            "total_genres": self.total_genres,
            "total_pages": self.total_pages,
            "crawled_pages": self.crawled_pages,
            "current_page": self.current_page,
            "total_stories": self.total_stories,
            "processed_stories": self.processed_stories,
            "active_stories": list(self.active_stories),
            "status": self.status,
            "last_error": self.last_error,
            "started_at": _to_iso(self.started_at),
            "updated_at": _to_iso(self.updated_at),
            "completed_at": _to_iso(self.completed_at) if self.completed_at else None,
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
        self._genres_in_progress: Dict[str, GenreProgress] = {}
        self._genres_completed: Dict[str, GenreProgress] = {}
        self._genres_failed: Dict[str, GenreProgress] = {}
        self._site_genre_overview: Dict[str, Dict[str, Any]] = {}

        self._load_existing()

    @staticmethod
    def _genre_key(site_key: str, genre_url: str) -> str:
        return f"{site_key}:{genre_url}"

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
    # Genre tracking helpers
    # ------------------------------------------------------------------
    def site_genres_initialized(self, site_key: str, total_genres: int) -> None:
        with self._lock:
            summary = self._site_genre_overview.setdefault(
                site_key,
                {"total_genres": 0, "genres": {}, "updated_at": time.time()},
            )
            summary["total_genres"] = max(int(total_genres), summary.get("total_genres", 0))
            summary["updated_at"] = time.time()
            self._persist_locked()

    def genre_started(
        self,
        site_key: str,
        genre_name: str,
        genre_url: str,
        *,
        position: Optional[int] = None,
        total_genres: Optional[int] = None,
    ) -> None:
        progress = GenreProgress(
            site_key=site_key,
            genre_name=genre_name,
            genre_url=genre_url,
            position=position,
            total_genres=total_genres,
            status="running",
        )
        key = self._genre_key(site_key, genre_url)
        with self._lock:
            self._genres_in_progress[key] = progress
            self._genres_completed.pop(key, None)
            self._genres_failed.pop(key, None)
            if total_genres is not None:
                summary = self._site_genre_overview.setdefault(
                    site_key,
                    {"total_genres": int(total_genres), "genres": {}, "updated_at": time.time()},
                )
                summary["total_genres"] = max(int(total_genres), summary.get("total_genres", 0))
                summary["updated_at"] = time.time()
            self._persist_locked()

    def update_genre_pages(
        self,
        site_key: str,
        genre_url: str,
        *,
        crawled_pages: Optional[int] = None,
        total_pages: Optional[int] = None,
        current_page: Optional[int] = None,
    ) -> None:
        key = self._genre_key(site_key, genre_url)
        with self._lock:
            genre = self._genres_in_progress.get(key)
            if not genre:
                return
            if total_pages is not None:
                genre.total_pages = int(total_pages) if total_pages else None
            if crawled_pages is not None:
                genre.crawled_pages = max(int(crawled_pages), genre.crawled_pages)
            if current_page is not None:
                genre.current_page = max(int(current_page), genre.current_page or 0)
            genre.status = "fetching_pages"
            genre.updated_at = time.time()
            self._persist_locked()

    def set_genre_story_total(
        self,
        site_key: str,
        genre_url: str,
        total_stories: int,
    ) -> None:
        key = self._genre_key(site_key, genre_url)
        with self._lock:
            genre = self._genres_in_progress.get(key)
            if not genre:
                return
            genre.total_stories = max(int(total_stories), 0)
            genre.updated_at = time.time()
            self._persist_locked()

    def genre_story_started(self, site_key: str, genre_url: str, story_title: str) -> None:
        key = self._genre_key(site_key, genre_url)
        with self._lock:
            genre = self._genres_in_progress.get(key)
            if not genre:
                return
            if story_title not in genre.active_stories:
                genre.active_stories.append(story_title)
                if len(genre.active_stories) > 5:
                    genre.active_stories = genre.active_stories[-5:]
            genre.status = "processing_stories"
            genre.updated_at = time.time()
            self._persist_locked()

    def genre_story_finished(
        self,
        site_key: str,
        genre_url: str,
        story_title: str,
        *,
        processed: bool = False,
    ) -> None:
        key = self._genre_key(site_key, genre_url)
        with self._lock:
            genre = self._genres_in_progress.get(key)
            if not genre:
                return
            genre.active_stories = [title for title in genre.active_stories if title != story_title]
            if processed:
                genre.processed_stories += 1
            genre.updated_at = time.time()
            self._persist_locked()

    def genre_completed(
        self,
        site_key: str,
        genre_url: str,
        *,
        stories_processed: Optional[int] = None,
    ) -> None:
        key = self._genre_key(site_key, genre_url)
        with self._lock:
            genre = self._genres_in_progress.pop(key, None)
            if not genre:
                genre = self._genres_failed.pop(key, None)
                if not genre:
                    return
            if stories_processed is not None:
                genre.processed_stories = max(int(stories_processed), 0)
            genre.status = "completed"
            genre.last_error = None
            genre.active_stories.clear()
            now = time.time()
            genre.completed_at = now
            genre.updated_at = now
            self._genres_completed[key] = genre

            summary = self._site_genre_overview.setdefault(
                site_key,
                {"total_genres": genre.total_genres or 0, "genres": {}, "updated_at": time.time()},
            )
            entry = summary.setdefault("genres", {})
            entry[genre.genre_url] = {
                "name": genre.genre_name,
                "url": genre.genre_url,
                "stories": genre.processed_stories,
                "status": "completed",
                "updated_at": now,
            }
            summary["updated_at"] = now
            if genre.total_genres:
                summary["total_genres"] = max(summary.get("total_genres", 0), int(genre.total_genres))
            self._persist_locked()

    def genre_failed(
        self,
        site_key: str,
        genre_url: str,
        reason: str,
        *,
        genre_name: Optional[str] = None,
    ) -> None:
        key = self._genre_key(site_key, genre_url)
        with self._lock:
            genre = self._genres_in_progress.pop(key, None)
            if not genre:
                genre = GenreProgress(
                    site_key=site_key,
                    genre_name=genre_name or genre_url,
                    genre_url=genre_url,
                )
            if genre_name:
                genre.genre_name = genre_name
            genre.status = "failed"
            genre.last_error = reason
            genre.active_stories.clear()
            now = time.time()
            genre.completed_at = now
            genre.updated_at = now
            self._genres_failed[key] = genre

            summary = self._site_genre_overview.setdefault(
                site_key,
                {"total_genres": genre.total_genres or 0, "genres": {}, "updated_at": time.time()},
            )
            entry = summary.setdefault("genres", {})
            entry[genre.genre_url] = {
                "name": genre.genre_name,
                "url": genre.genre_url,
                "stories": genre.processed_stories,
                "status": "failed",
                "updated_at": now,
                "error": reason,
            }
            summary["updated_at"] = now
            if genre.total_genres:
                summary["total_genres"] = max(summary.get("total_genres", 0), int(genre.total_genres))
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

        genres_payload = payload.get("genres", {})
        for item in genres_payload.get("in_progress", []):
            progress = _genre_from_payload(item)
            if progress:
                key = self._genre_key(progress.site_key, progress.genre_url)
                self._genres_in_progress[key] = progress
        for item in genres_payload.get("completed", []):
            progress = _genre_from_payload(item)
            if progress:
                key = self._genre_key(progress.site_key, progress.genre_url)
                self._genres_completed[key] = progress
        for item in genres_payload.get("failed", []):
            progress = _genre_from_payload(item)
            if progress:
                key = self._genre_key(progress.site_key, progress.genre_url)
                self._genres_failed[key] = progress

        site_genres_payload = payload.get("site_genres", [])
        for site_entry in site_genres_payload:
            if not isinstance(site_entry, dict):
                continue
            site_key = site_entry.get("site_key")
            if not site_key:
                continue
            genres_map: Dict[str, Any] = {}
            for genre in site_entry.get("genres", []):
                if not isinstance(genre, dict):
                    continue
                url = genre.get("url")
                if not url:
                    continue
                genres_map[url] = {
                    "name": genre.get("name", url),
                    "url": url,
                    "stories": int(genre.get("stories", 0)),
                    "status": genre.get("status", "completed"),
                    "updated_at": _from_iso(genre.get("updated_at")) or time.time(),
                    "error": genre.get("error"),
                }
            self._site_genre_overview[site_key] = {
                "total_genres": int(site_entry.get("total_genres", len(genres_map))),
                "genres": genres_map,
                "updated_at": _from_iso(site_entry.get("updated_at")) or time.time(),
            }

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
        genres_in_progress = [genre.to_dict() for genre in self._genres_in_progress.values()]
        genres_completed = [genre.to_dict() for genre in self._genres_completed.values()]
        genres_failed = [genre.to_dict() for genre in self._genres_failed.values()]
        site_genres: List[Dict[str, Any]] = []
        for site_key, overview in self._site_genre_overview.items():
            genres_map = overview.get("genres", {})
            genres_list = []
            for url, data in genres_map.items():
                genres_list.append(
                    {
                        "name": data.get("name", url),
                        "url": url,
                        "stories": int(data.get("stories", 0)),
                        "status": data.get("status", "completed"),
                        "updated_at": _to_iso(data.get("updated_at")),
                        "error": data.get("error"),
                    }
                )
            site_genres.append(
                {
                    "site_key": site_key,
                    "total_genres": int(overview.get("total_genres", len(genres_list))),
                    "completed_genres": len(genres_list),
                    "genres": sorted(genres_list, key=lambda item: item.get("name", "")),
                    "updated_at": _to_iso(overview.get("updated_at")),
                }
            )
        site_genres.sort(key=lambda item: item.get("site_key", ""))
        total_genres_known = sum(entry.get("total_genres", 0) for entry in site_genres)
        total_genres_completed = sum(entry.get("completed_genres", 0) for entry in site_genres)
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
                "genres_in_progress": len(genres_in_progress),
                "genres_completed": len(genres_completed),
                "genres_failed": len(genres_failed),
                "genres_total_configured": total_genres_known,
                "genres_total_completed": total_genres_completed,
            },
            "queues": dict(self._queues),
            "sites": [snapshot.to_dict() for snapshot in self._site_stats.values()],
            "genres": {
                "in_progress": genres_in_progress,
                "completed": genres_completed,
                "failed": genres_failed,
            },
            "site_genres": site_genres,
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


def _genre_from_payload(payload: Dict[str, Any]) -> Optional[GenreProgress]:
    if not isinstance(payload, dict):
        return None
    site_key = payload.get("site_key")
    genre_url = payload.get("url")
    if not site_key or not genre_url:
        return None
    genre_name = payload.get("name") or genre_url
    def _coerce_int(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(value)
        except Exception:  # pragma: no cover - defensive coercion
            return None

    progress = GenreProgress(
        site_key=site_key,
        genre_name=genre_name,
        genre_url=genre_url,
        position=_coerce_int(payload.get("position")),
        total_genres=_coerce_int(payload.get("total_genres")),
        total_pages=_coerce_int(payload.get("total_pages")),
        crawled_pages=int(payload.get("crawled_pages", 0)),
        current_page=_coerce_int(payload.get("current_page")),
        total_stories=int(payload.get("total_stories", 0)),
        processed_stories=int(payload.get("processed_stories", 0)),
        status=payload.get("status", "queued"),
        last_error=payload.get("last_error"),
    )
    active = payload.get("active_stories")
    if isinstance(active, list):
        progress.active_stories = [str(item) for item in active[:5]]
    progress.started_at = _from_iso(payload.get("started_at")) or time.time()
    progress.updated_at = _from_iso(payload.get("updated_at")) or progress.started_at
    progress.completed_at = _from_iso(payload.get("completed_at"))
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
