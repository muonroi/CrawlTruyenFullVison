"""Utilities for building structured crawl plans.

This module encapsulates the *discovery* phase of the crawl so that the core
pipeline can follow a predictable flow:

1. Lấy danh sách category từ adapter.
2. Với từng category, lấy toàn bộ truyện cần crawl.
3. Gom lại thành các ``dict`` ``{category: [story, ...]}`` để chia batch và làm
   thống kê.

The helpers defined here are used directly by :mod:`main` to prepare crawl
plans before any heavy processing begins.  They also remain usable in isolation
for unit tests and tooling.
"""

from __future__ import annotations

import asyncio
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Iterable, Iterator, List, Optional
from urllib.parse import urlparse

from adapters.base_site_adapter import BaseSiteAdapter
from config import config as app_config
from utils.io_utils import log_failed_genre
from utils.logger import logger
from utils.metrics_tracker import metrics_tracker


def _normalise_genre_name(raw_genre: Dict[str, Any]) -> Optional[str]:
    """Return a human readable name for ``raw_genre`` if possible."""

    if not isinstance(raw_genre, dict):
        return None

    for key in ("name", "title", "label", "category"):
        value = raw_genre.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _normalise_genre_url(raw_genre: Dict[str, Any]) -> Optional[str]:
    """Return the URL for ``raw_genre`` if it contains one."""

    if not isinstance(raw_genre, dict):
        return None

    for key in ("url", "link", "href"):
        value = raw_genre.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


@dataclass(slots=True)
class CategoryCrawlPlan:
    """Represents a single category and the stories discovered for it."""

    name: str
    url: str
    stories: List[Dict[str, Any]] = field(default_factory=list)
    total_pages: Optional[int] = None
    crawled_pages: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    raw_genre: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Return a serialisable representation of the plan."""

        payload: Dict[str, Any] = {
            "name": self.name,
            "url": self.url,
            "stories": list(self.stories),
        }
        if self.total_pages is not None:
            payload["total_pages"] = self.total_pages
        if self.crawled_pages is not None:
            payload["crawled_pages"] = self.crawled_pages
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        if self.raw_genre:
            payload["raw_genre"] = dict(self.raw_genre)
        return payload


def _extract_identifier_from_payload(payload: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    for key in ("category_id", "id", "slug", "slug_id", "code"):
        value = payload.get(key)
        if isinstance(value, (str, int)):
            text = str(value).strip()
            if text:
                return text
    return None


def _derive_category_identifier(category: CategoryCrawlPlan) -> str:
    candidate = _extract_identifier_from_payload(category.metadata)
    if not candidate:
        candidate = _extract_identifier_from_payload(category.raw_genre)
    if not candidate:
        candidate = category.url or category.name
    return str(candidate)


def _normalise_domain(url: Optional[str]) -> Optional[str]:
    if not isinstance(url, str):
        return None
    parsed = urlparse(url)
    host = parsed.netloc
    if not host:
        raw = url.strip()
        if raw and "://" not in raw:
            host = raw.split("/")[0]
    host = host.strip().lower()
    return host or None


def _derive_domain(category: CategoryCrawlPlan) -> Optional[str]:
    for story in category.stories:
        if not isinstance(story, dict):
            continue
        domain = _normalise_domain(story.get("url"))
        if domain:
            return domain
    return _normalise_domain(category.url)


def _chunk_list(items: List[Dict[str, Any]], chunk_size: int) -> Iterator[List[Dict[str, Any]]]:
    for start in range(0, len(items), chunk_size):
        yield list(items[start : start + chunk_size])


def _split_category_into_jobs(
    category: CategoryCrawlPlan, chunk_size: int
) -> List[CategoryBatchJob]:
    stories = list(category.stories)
    if not stories:
        return []
    chunk_size = max(1, chunk_size)
    parts = list(_chunk_list(stories, chunk_size))
    total_parts = len(parts)
    category_id = _derive_category_identifier(category)
    domain = _derive_domain(category)
    jobs: List[CategoryBatchJob] = []
    for index, chunk in enumerate(parts, start=1):
        jobs.append(
            CategoryBatchJob(
                category=category,
                stories=chunk,
                category_id=category_id,
                domain=domain,
                part_index=index,
                total_parts=total_parts,
            )
        )
    return jobs


@dataclass(slots=True)
class CategoryBatchJob:
    """Represents a scheduled chunk of work for a category.

    The job keeps a reference to the originating :class:`CategoryCrawlPlan`
    instance so that downstream consumers retain access to the full discovery
    metadata.  ``part_index`` is ``1`` based to make logging friendlier.
    """

    category: CategoryCrawlPlan
    stories: List[Dict[str, Any]]
    category_id: str
    domain: Optional[str]
    part_index: int
    total_parts: int

    def to_payload(self) -> Dict[str, Any]:
        """Return a serialisable representation of the scheduled job."""

        return {
            "category_id": self.category_id,
            "category_name": self.category.name,
            "domain": self.domain,
            "part_index": self.part_index,
            "total_parts": self.total_parts,
            "stories": list(self.stories),
        }


@dataclass(slots=True)
class CrawlPlan:
    """Container that holds the full crawl plan for a site."""

    site_key: str
    categories: List[CategoryCrawlPlan] = field(default_factory=list)

    def add_category(self, category: CategoryCrawlPlan) -> None:
        self.categories.append(category)

    @property
    def total_categories(self) -> int:
        return len(self.categories)

    def as_mapping(self) -> Dict[str, List[Dict[str, Any]]]:
        """Return a ``dict`` mapping category name to the list of stories."""

        return {category.name: list(category.stories) for category in self.categories}

    def to_dict(self) -> Dict[str, Any]:
        """Return a serialisable representation of the full plan."""

        return {
            "site_key": self.site_key,
            "total_categories": self.total_categories,
            "categories": [category.to_dict() for category in self.categories],
        }

    def split_into_batches(self, batch_size: int) -> List[Dict[str, List[Dict[str, Any]]]]:
        """Group categories into batches of ``batch_size`` for workers.

        The function returns a list where each element is a mapping with the
        ``Category -> Stories`` layout requested by the user.  Consumers can use
        it to dispatch independent crawl jobs without recomputing discovery.
        """

        if batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")

        batches: List[Dict[str, List[Dict[str, Any]]]] = []
        for start in range(0, len(self.categories), batch_size):
            chunk = self.categories[start : start + batch_size]
            batches.append({category.name: list(category.stories) for category in chunk})
        return batches

    def schedule_batches_by_quota(
        self,
        *,
        max_batch_size: int,
        max_category_batch_size: Optional[int] = None,
        max_jobs_per_category: Optional[int] = None,
        max_jobs_per_domain: Optional[int] = None,
    ) -> List[List[CategoryBatchJob]]:
        """Split categories into batches while respecting quota constraints.

        Parameters
        ----------
        max_batch_size:
            Maximum number of jobs that can run concurrently in a batch.
        max_category_batch_size:
            Upper bound of how many stories a single job from a category should
            contain.  Large categories will be split across multiple jobs so
            that their work can be spread across batches.  Defaults to
            ``max_batch_size`` when omitted.
        max_jobs_per_category:
            Limit of how many jobs from the same category can appear in a single
            batch.  ``None`` or ``<= 0`` disables the limit.
        max_jobs_per_domain:
            Limit of how many jobs from the same domain can appear in a single
            batch.  ``None`` or ``<= 0`` disables the limit.
        """

        if max_batch_size <= 0:
            raise ValueError("max_batch_size must be a positive integer")

        effective_category_limit = (
            max_jobs_per_category if max_jobs_per_category and max_jobs_per_category > 0 else max_batch_size
        )
        effective_domain_limit = (
            max_jobs_per_domain if max_jobs_per_domain and max_jobs_per_domain > 0 else max_batch_size
        )
        per_category_chunk_size = max_category_batch_size or max_batch_size
        if per_category_chunk_size <= 0:
            raise ValueError("max_category_batch_size must be positive when provided")

        def _iter_category_jobs() -> Iterator[Deque[CategoryBatchJob]]:
            for category in self.categories:
                jobs = _split_category_into_jobs(category, per_category_chunk_size)
                if jobs:
                    yield deque(jobs)

        active_queues: Deque[Deque[CategoryBatchJob]] = deque(_iter_category_jobs())
        if not active_queues:
            return []

        # ``sorted`` ensures that bigger categories start earlier in the
        # rotation so their workload is naturally spread across more batches.
        active_queues = deque(sorted(active_queues, key=len, reverse=True))

        batches: List[List[CategoryBatchJob]] = []
        while active_queues:
            batch: List[CategoryBatchJob] = []
            category_counter: Counter[str] = Counter()
            domain_counter: Counter[str] = Counter()
            skipped_rounds = 0

            while active_queues and len(batch) < max_batch_size:
                queue = active_queues[0]
                job = queue[0]
                if category_counter[job.category_id] >= effective_category_limit:
                    active_queues.rotate(-1)
                    skipped_rounds += 1
                    if skipped_rounds >= len(active_queues):
                        break
                    continue
                if job.domain and domain_counter[job.domain] >= effective_domain_limit:
                    active_queues.rotate(-1)
                    skipped_rounds += 1
                    if skipped_rounds >= len(active_queues):
                        break
                    continue

                queue.popleft()
                batch.append(job)
                category_counter[job.category_id] += 1
                if job.domain:
                    domain_counter[job.domain] += 1
                skipped_rounds = 0
                if queue:
                    active_queues.rotate(-1)
                else:
                    active_queues.popleft()

            if not batch:
                queue = active_queues.popleft()
                job = queue.popleft()
                batch.append(job)
                if queue:
                    active_queues.append(queue)

            batches.append(batch)

        return batches


async def build_category_plan(
    adapter: BaseSiteAdapter,
    raw_genre: Dict[str, Any],
    site_key: str,
    *,
    position: Optional[int] = None,
    total_genres: Optional[int] = None,
    max_pages: Optional[int] = None,
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> Optional[CategoryCrawlPlan]:
    """Create a :class:`CategoryCrawlPlan` for ``raw_genre``.

    The helper mirrors the logic that previously lived inside
    ``process_genre_item`` so that both the planning phase and the execution
    phase can share the same retry semantics.
    """

    genre_name = _normalise_genre_name(raw_genre)
    genre_url = _normalise_genre_url(raw_genre)
    if not genre_name or not genre_url:
        return None

    retry_time = 0
    # ``RETRY_GENRE_ROUND_LIMIT`` is optional in configuration.  Fallback to 5
    # to preserve the previous behaviour.
    max_retry = int(getattr(app_config, "RETRY_GENRE_ROUND_LIMIT", 5) or 5)
    sleep_seconds = float(getattr(app_config, "RETRY_SLEEP_SECONDS", 5) or 5)

    while True:
        try:
            (
                stories,
                total_pages,
                crawled_pages,
            ) = await adapter.get_all_stories_from_genre_with_page_check(
                genre_name,
                genre_url,
                site_key=site_key,
                max_pages=max_pages,
            )
            if not stories:
                raise ValueError(
                    f"Danh sách truyện rỗng cho genre {genre_name} ({genre_url})"
                )

            metrics_tracker.set_genre_story_total(site_key, genre_url, len(stories))

            if (
                total_pages
                and crawled_pages is not None
                and crawled_pages < total_pages
            ):
                logger.warning(
                    "Thể loại %s chỉ crawl được %s/%s trang, sẽ retry lần %s...",
                    genre_name,
                    crawled_pages,
                    total_pages,
                    retry_time + 1,
                )
                retry_time += 1
                if retry_time >= max_retry:
                    logger.error(
                        "Thể loại %s không crawl đủ số trang sau %s lần.",
                        genre_name,
                        max_retry,
                    )
                    log_failed_genre({"name": genre_name, "url": genre_url})
                    metrics_tracker.genre_failed(
                        site_key,
                        genre_url,
                        reason=f"incomplete_pages_{crawled_pages}_{total_pages}",
                        genre_name=genre_name,
                    )
                    return None
                await asyncio.sleep(min(sleep_seconds, 60.0))
                continue

            metadata: Dict[str, Any] = {}
            if isinstance(raw_genre, dict):
                metadata.update(
                    {
                        k: v
                        for k, v in raw_genre.items()
                        if k
                        not in {
                            "name",
                            "title",
                            "label",
                            "category",
                            "url",
                            "link",
                            "href",
                        }
                    }
                )
            if extra_metadata:
                metadata.update(extra_metadata)
            if position is not None:
                metadata.setdefault("position", position)
            if total_genres is not None:
                metadata.setdefault("total_genres", total_genres)

            return CategoryCrawlPlan(
                name=genre_name,
                url=genre_url,
                stories=list(stories),
                total_pages=total_pages,
                crawled_pages=crawled_pages,
                metadata=metadata,
                raw_genre=dict(raw_genre),
            )
        except Exception as ex:  # pragma: no cover - defensive logging branch
            logger.error(
                "Lỗi khi crawl genre %s (%s): %s",
                raw_genre.get("name", genre_name),
                raw_genre.get("url", genre_url),
                ex,
            )
            log_failed_genre({"name": genre_name, "url": genre_url})
            metrics_tracker.genre_failed(
                site_key,
                genre_url or raw_genre.get("url", ""),
                reason=str(ex),
                genre_name=genre_name or raw_genre.get("name", ""),
            )
            return None


async def build_crawl_plan(
    adapter: BaseSiteAdapter,
    *,
    max_pages: Optional[int] = None,
    extra_metadata: Optional[Dict[str, Any]] = None,
    genres: Optional[Iterable[Dict[str, Any]]] = None,
) -> CrawlPlan:
    """Construct a :class:`CrawlPlan` for ``adapter``.

    Parameters
    ----------
    adapter:
        The site adapter responsible for discovery.
    max_pages:
        Optional safety limit passed to ``get_all_stories_from_genre`` to avoid
        crawling beyond the configured number of pages.
    extra_metadata:
        Additional metadata merged into every ``CategoryCrawlPlan`` to simplify
        downstream reporting.
    """

    site_key = getattr(adapter, "site_key", None) or adapter.get_site_key()
    raw_genres: Iterable[Dict[str, Any]]
    if genres is None:
        raw_genres = await adapter.get_genres() or []
    else:
        raw_genres = list(genres)

    plan = CrawlPlan(site_key=site_key)
    raw_genres_list = list(raw_genres)
    total_genres = len(raw_genres_list)

    for index, raw_genre in enumerate(raw_genres_list, start=1):
        category_plan = await build_category_plan(
            adapter,
            raw_genre,
            site_key,
            position=index,
            total_genres=total_genres,
            max_pages=max_pages,
            extra_metadata=extra_metadata,
        )
        if category_plan:
            plan.add_category(category_plan)

    return plan

