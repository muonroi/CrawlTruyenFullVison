from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, Optional

from core.category_store import CategoryStore
from utils.logger import logger


class StoryCrawlStatus(str, Enum):
    """Enumeration of crawl states tracked in the story registry."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    COOLDOWN = "cooldown"


@dataclass(frozen=True)
class StoryRegistryEntry:
    """Snapshot of a story row in the registry."""

    story_id: int
    site_key: str
    status: StoryCrawlStatus
    last_category: Optional[str]
    last_crawled_at: Optional[str]
    last_result: Optional[str]
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "StoryRegistryEntry":
        status_value = row["status"]
        try:
            status = StoryCrawlStatus(status_value)
        except ValueError:
            logger.warning("[REGISTRY] Unknown status '%s', treating as pending", status_value)
            status = StoryCrawlStatus.PENDING
        return cls(
            story_id=int(row["story_id"]),
            site_key=str(row["site_key"]),
            status=status,
            last_category=row["last_category"],
            last_crawled_at=row["last_crawled_at"],
            last_result=row["last_result"],
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )


class StoryRegistry:
    """SQLite-backed registry of stories and their crawl status."""

    def __init__(self, db_path: str, category_store: CategoryStore) -> None:
        self.db_path = db_path
        self._store = category_store
        self._lock = threading.Lock()
        self._initialise()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _initialise(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;

                CREATE TABLE IF NOT EXISTS story_crawl_registry (
                    story_id INTEGER PRIMARY KEY,
                    site_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_category TEXT,
                    last_crawled_at TEXT,
                    last_result TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(story_id) REFERENCES stories(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_story_registry_status
                    ON story_crawl_registry(status);
                CREATE INDEX IF NOT EXISTS idx_story_registry_last_crawled
                    ON story_crawl_registry(last_crawled_at);
                CREATE INDEX IF NOT EXISTS idx_story_registry_category
                    ON story_crawl_registry(last_category);
                CREATE INDEX IF NOT EXISTS idx_story_registry_site
                    ON story_crawl_registry(site_key);
                """
            )

    def _now(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def _ensure_story_id(self, site_key: str, story: Dict[str, Any]) -> int:
        url = story.get("url") if isinstance(story, dict) else None
        if not url:
            raise ValueError("story missing required 'url' field for registry tracking")
        return self._store.ensure_story_record(site_key, story)

    def ensure_entry(
        self,
        site_key: str,
        story: Dict[str, Any],
        category_name: Optional[str] = None,
    ) -> StoryRegistryEntry:
        """Ensure a registry entry exists for ``story`` and return it."""

        story_id = self._ensure_story_id(site_key, story)
        with self._lock:
            with self._connect() as conn:
                now = self._now()
                row = conn.execute(
                    "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                    (story_id,),
                ).fetchone()
                if row:
                    if category_name and row["last_category"] != category_name:
                        conn.execute(
                            "UPDATE story_crawl_registry SET last_category = ?, updated_at = ? WHERE story_id = ?",
                            (category_name, now, story_id),
                        )
                        row = conn.execute(
                            "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                            (story_id,),
                        ).fetchone()
                    assert row is not None
                    return StoryRegistryEntry.from_row(row)
                conn.execute(
                    """
                    INSERT INTO story_crawl_registry(
                        story_id, site_key, status, last_category, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        story_id,
                        site_key,
                        StoryCrawlStatus.PENDING.value,
                        category_name,
                        now,
                        now,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                    (story_id,),
                ).fetchone()
                assert row is not None
                return StoryRegistryEntry.from_row(row)

    def try_acquire(
        self,
        story_id: int,
        *,
        category_name: Optional[str] = None,
    ) -> Optional[StoryRegistryEntry]:
        """Transition a story to ``IN_PROGRESS`` if it is not already busy."""

        with self._lock:
            with self._connect() as conn:
                now = self._now()
                cursor = conn.execute(
                    """
                    UPDATE story_crawl_registry
                       SET status = ?,
                           updated_at = ?,
                           last_crawled_at = ?,
                           last_category = COALESCE(?, last_category),
                           last_result = NULL
                     WHERE story_id = ?
                       AND status != ?
                    """,
                    (
                        StoryCrawlStatus.IN_PROGRESS.value,
                        now,
                        now,
                        category_name,
                        story_id,
                        StoryCrawlStatus.IN_PROGRESS.value,
                    ),
                )
                if cursor.rowcount == 0:
                    row = conn.execute(
                        "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                        (story_id,),
                    ).fetchone()
                    if not row:
                        return None
                    entry = StoryRegistryEntry.from_row(row)
                    if entry.status == StoryCrawlStatus.IN_PROGRESS:
                        return None
                    return entry
                row = conn.execute(
                    "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                    (story_id,),
                ).fetchone()
                assert row is not None
                return StoryRegistryEntry.from_row(row)

    def mark_status(
        self,
        story_id: int,
        status: StoryCrawlStatus,
        *,
        result: Optional[str] = None,
        category_name: Optional[str] = None,
    ) -> Optional[StoryRegistryEntry]:
        """Persist the final status for a story and return the updated entry."""

        with self._lock:
            with self._connect() as conn:
                now = self._now()
                last_crawled_at = now if status != StoryCrawlStatus.PENDING else None
                conn.execute(
                    """
                    UPDATE story_crawl_registry
                       SET status = ?,
                           last_result = ?,
                           updated_at = ?,
                           last_category = COALESCE(?, last_category),
                           last_crawled_at = COALESCE(?, last_crawled_at)
                     WHERE story_id = ?
                    """,
                    (
                        status.value,
                        result,
                        now,
                        category_name,
                        last_crawled_at,
                        story_id,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                    (story_id,),
                ).fetchone()
                return StoryRegistryEntry.from_row(row) if row else None

    def ensure_entries(
        self,
        site_key: str,
        stories: Iterable[Dict[str, Any]],
        *,
        category_name: Optional[str] = None,
    ) -> None:
        """Bulk ensure registry entries exist for a list of stories."""

        for story in stories:
            try:
                self.ensure_entry(site_key, story, category_name)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error(
                    "[REGISTRY] Failed to ensure entry for story %s: %s",
                    story.get("url"),
                    exc,
                )
