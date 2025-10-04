import asyncio
import re
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from adapters.base_site_adapter import BaseSiteAdapter
from analyze.xtruyen_parse import (
    parse_chapter_content,
    parse_chapter_list,
    parse_genres,
    parse_story_info,
    parse_story_list,
)
from config.config import BASE_URLS
from scraper import make_request
from utils.chapter_utils import get_chapter_sort_key
from utils.logger import logger
from utils.metrics_tracker import metrics_tracker
from utils.site_config import load_site_config


def _with_page_parameter(url: str, page: int) -> str:
    if page <= 1:
        return url
    parsed = urlparse(url)
    path = parsed.path
    segment_pattern = re.compile(r'/(page|trang|p)/(\d+)$', re.IGNORECASE)
    suffix_pattern = re.compile(r'-(page|trang|p)-(\d+)$', re.IGNORECASE)

    if segment_pattern.search(path):
        path = segment_pattern.sub(lambda match: f"/{match.group(1)}/{page}", path)
        return urlunparse(parsed._replace(path=path))

    if suffix_pattern.search(path):
        path = suffix_pattern.sub(lambda match: f"-{match.group(1)}-{page}", path)
        return urlunparse(parsed._replace(path=path))

    query = parse_qs(parsed.query, keep_blank_values=True)
    replaced = False
    for key in ("page", "paged", "pageindex"):
        if key in query:
            query[key] = [str(page)]
            replaced = True
    if not replaced:
        query["page"] = [str(page)]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))



class XTruyenAdapter(BaseSiteAdapter):
    site_key = "xtruyen"
    _DEFAULT_CHAPTER_LIST_BATCH = 100

    def __init__(self) -> None:
        self.site_key = self.__class__.site_key
        site_config = load_site_config(self.site_key)
        configured_base_url = site_config.get("base_url")
        self.base_url = configured_base_url or BASE_URLS.get(self.site_key, "https://xtruyen.vn")
        chapter_batch_raw = site_config.get("chapter_list_batch", self._DEFAULT_CHAPTER_LIST_BATCH)
        self._chapter_list_batch = self._coerce_positive_int(
            chapter_batch_raw, self._DEFAULT_CHAPTER_LIST_BATCH
        )
        self._ajax_endpoint = site_config.get("ajax_endpoint", "/wp-admin/admin-ajax.php")
        self._details_cache: Dict[str, Dict[str, Any]] = {}
        self._details_lock = asyncio.Lock()

    @staticmethod
    def _normalize_ranges(raw_ranges: Optional[List[Any]]) -> List[Tuple[int, int]]:
        normalized: List[Tuple[int, int]] = []
        if not raw_ranges:
            return normalized
        for item in raw_ranges:
            start: Optional[int] = None
            end: Optional[int] = None
            if isinstance(item, dict):
                start_val = item.get('from')
                end_val = item.get('to')
                if isinstance(start_val, int):
                    start = start_val
                elif isinstance(start_val, str) and start_val.isdigit():
                    start = int(start_val)
                if isinstance(end_val, int):
                    end = end_val
                elif isinstance(end_val, str) and end_val.isdigit():
                    end = int(end_val)
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                start_candidate, end_candidate = item[0], item[1]
                if isinstance(start_candidate, int):
                    start = start_candidate
                elif isinstance(start_candidate, str) and start_candidate.isdigit():
                    start = int(start_candidate)
                if isinstance(end_candidate, int):
                    end = end_candidate
                elif isinstance(end_candidate, str) and end_candidate.isdigit():
                    end = int(end_candidate)
            if start is None:
                continue
            if end is None:
                end = start
            if end < start:
                start, end = end, start
            normalized.append((start, end))
        normalized.sort()
        return normalized

    def get_chapters_per_page_hint(self) -> int:
        return self._chapter_list_batch

    @staticmethod
    def _coerce_positive_int(value: Any, default: int) -> int:
        try:
            candidate = int(value)
        except (TypeError, ValueError):
            return default
        return candidate if candidate > 0 else default

    async def _fetch_chapters_via_ajax(
        self,
        story_url: str,
        manga_id: Optional[str],
        ajax_nonce: Optional[str],
        total_expected: Optional[int],
        chapter_ranges: Optional[List[Any]] = None,
        max_batches: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        if not manga_id:
            logger.warning(f"[{self.site_key}] Missing manga_id for story {story_url}, cannot load chapters via AJAX.")
            return []

        ajax_url = urljoin(self.base_url, self._ajax_endpoint)
        collected: List[Dict[str, str]] = []
        seen_urls: Set[str] = set()

        extra_headers = {'Referer': story_url}
        base_payload: Dict[str, Any] = {
            'action': 'load_chapter_list_from_to',
            'manga_id': manga_id,
        }
        if ajax_nonce:
            # Some Madara deployments expect nonce under different keys; send both to be safe.
            base_payload.setdefault('security', ajax_nonce)
            base_payload.setdefault('nonce', ajax_nonce)

        ranges = self._normalize_ranges(chapter_ranges)
        has_custom_ranges = bool(ranges)
        if not ranges:
            ranges = []
            start = 1
            while True:
                end = start + self._chapter_list_batch - 1
                ranges.append((start, end))
                if total_expected and end >= total_expected:
                    break
                if len(ranges) >= 50:
                    break
                start = end + 1

        for batch_index, (start, end) in enumerate(ranges, start=1):
            if total_expected and start > total_expected:
                continue
            upper = min(end, total_expected) if total_expected else end
            payload = dict(base_payload)
            payload['from'] = start
            payload['to'] = upper

            logger.debug(f"[{self.site_key}] AJAX chapters request for manga {manga_id}: from={start} to={upper}")

            response = await make_request(
                ajax_url,
                self.site_key,
                method='POST',
                data=payload,
                extra_headers=extra_headers,
            )

            if not response or not getattr(response, 'text', None):
                logger.debug(f"[{self.site_key}] Empty AJAX response for manga {manga_id} (from={payload['from']}).")
                break

            html_snippet = response.text.strip()
            if not html_snippet:
                break

            batch_chapters = parse_chapter_list(html_snippet, self.base_url)
            new_items = []
            for chapter in batch_chapters:
                chapter_url = chapter.get('url')
                if not chapter_url or chapter_url in seen_urls:
                    continue
                seen_urls.add(chapter_url)
                chapter.setdefault('site_key', self.site_key)
                new_items.append(chapter)

            if not new_items:
                logger.debug(f"[{self.site_key}] No new chapters detected in AJAX batch for manga {manga_id}.")
                break

            collected.extend(new_items)

            if total_expected and len(collected) >= total_expected:
                break

            expected_count = max(upper - start + 1, 0)
            if not has_custom_ranges and expected_count > 0 and len(new_items) < expected_count:
                break

            await asyncio.sleep(0.25)

            if max_batches and batch_index >= max_batches:
                logger.debug(
                    f"[{self.site_key}] Reached max chapter batches ({max_batches}) for {story_url}."
                )
                break

        collected.sort(key=get_chapter_sort_key)
        logger.info(f"[{self.site_key}] Loaded {len(collected)} chapters via AJAX for {story_url}.")
        return collected

    async def _fetch_text(self, url: str, wait_for_selector: Optional[str] = None) -> Optional[str]:
        response = await make_request(url, self.site_key, wait_for_selector=wait_for_selector)
        if not response or not getattr(response, "text", None):
            logger.error(f"[{self.site_key}] Failed to fetch URL: {url}")
            return None
        return response.text

    async def get_genres(self) -> List[Dict[str, str]]:
        html = await self._fetch_text(self.base_url, wait_for_selector="div.main-menu")
        if not html:
            return []
        genres = parse_genres(html, self.base_url)
        logger.info(f"[{self.site_key}] Found {len(genres)} genres")
        return genres

    async def get_stories_in_genre(self, genre_url: str, page: int = 1) -> Tuple[List[Dict[str, str]], int]:
        url = _with_page_parameter(genre_url, page)
        logger.debug(f"[{self.site_key}] Fetching stories page: {url}")
        html = await self._fetch_text(url, wait_for_selector="div.popular-item-wrap")
        if not html:
            return [], 0
        stories, max_pages = parse_story_list(html, self.base_url)
        return stories, max_pages

    async def get_all_stories_from_genre_with_page_check(
        self,
        genre_name: str,
        genre_url: str,
        site_key: str,
        max_pages: Optional[int] = None,
        *,
        page_callback: Optional[
            Callable[[List[Dict[str, Any]], int, Optional[int]], Awaitable[None]]
        ] = None,
        collect: bool = True,
    ) -> Tuple[List[Dict[str, str]], int, int]:
        logger.info(f"[{self.site_key}] Crawling stories for genre '{genre_name}'")
        first_page_stories, total_pages = await self.get_stories_in_genre(genre_url, page=1)
        if not first_page_stories:
            logger.warning(f"[{self.site_key}] No stories detected on first page of {genre_name}")
            return [], 0, 0

        metrics_tracker.update_genre_pages(
            site_key,
            genre_url,
            crawled_pages=1,
            total_pages=total_pages,
            current_page=1,
        )

        all_stories: List[Dict[str, Any]] = list(first_page_stories) if collect else []
        for story in first_page_stories:
            story.setdefault('_source_page', 1)

        discovered_total = len(all_stories)
        if discovered_total:
            metrics_tracker.set_genre_story_total(site_key, genre_url, discovered_total)

        if page_callback:
            await page_callback(list(first_page_stories), 1, total_pages)

        crawled_pages = 1
        limit = max_pages or total_pages or 1
        page = 2
        while page <= limit:
            stories, _ = await self.get_stories_in_genre(genre_url, page)
            crawled_pages += 1
            metrics_tracker.update_genre_pages(
                site_key,
                genre_url,
                crawled_pages=crawled_pages,
                total_pages=total_pages,
                current_page=page,
            )
            if not stories:
                logger.info(f"[{self.site_key}] Stop paging {genre_name}: empty page {page}")
                break

            for story in stories:
                story.setdefault('_source_page', page)

            if collect:
                all_stories.extend(stories)
                discovered_total = len(all_stories)
            else:
                discovered_total += len(stories)
            metrics_tracker.set_genre_story_total(site_key, genre_url, discovered_total)

            if page_callback:
                await page_callback(list(stories), page, total_pages)

            page += 1
            await asyncio.sleep(0.5)

        logger.info(f"[{self.site_key}] Total stories for {genre_name}: {discovered_total}")
        return all_stories, total_pages, crawled_pages

    async def get_all_stories_from_genre(
        self, genre_name: str, genre_url: str, max_pages: Optional[int] = None
    ) -> List[Dict[str, str]]:
        return await self.get_all_stories_from_genre_with_page_check(
            genre_name=genre_name,
            genre_url=genre_url,
            site_key=self.site_key,
            max_pages=max_pages,
        )

    async def _get_story_details_internal(self, story_url: str) -> Optional[Dict[str, Any]]:
        html = await self._fetch_text(story_url, wait_for_selector="div.summary__content")
        if not html:
            return None
        details = parse_story_info(html, self.base_url)
        inline_chapters = details.get('chapters') or []
        total_expected = details.get('total_chapters_on_site') if isinstance(details.get('total_chapters_on_site'), int) else None
        needs_ajax = not inline_chapters or (
            total_expected is not None and total_expected > len(inline_chapters)
        )

        if needs_ajax:
            ajax_chapters = await self._fetch_chapters_via_ajax(
                story_url=story_url,
                manga_id=details.get('manga_id'),
                ajax_nonce=details.get('ajax_nonce'),
                total_expected=total_expected,
                chapter_ranges=details.get('chapter_ranges'),
            )
            if ajax_chapters:
                details['chapters'] = ajax_chapters
            else:
                for ch in inline_chapters:
                    ch.setdefault('site_key', self.site_key)
                details['chapters'] = inline_chapters
        else:
            for ch in inline_chapters:
                ch.setdefault('site_key', self.site_key)
        details['sources'] = [
            {'url': story_url, 'site_key': self.site_key, 'priority': 1}
        ]
        details['url'] = story_url
        return details

    async def get_story_details(self, story_url: str, story_title: str) -> Optional[Dict[str, Any]]:
        async with self._details_lock:
            cached = self._details_cache.get(story_url)
            if cached:
                return cached
            details = await self._get_story_details_internal(story_url)
            if details:
                self._details_cache[story_url] = details
            return details

    async def get_chapter_list(
        self,
        story_url: str,
        story_title: str,
        site_key: str,
        max_pages: Optional[int] = None,
        total_chapters: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        """Fetch chapter list, preferring the AJAX endpoint for completeness."""
        logger.debug(f"[{self.site_key}] Getting chapter list for '{story_title}'.")

        details = await self.get_story_details(story_url, story_title)
        if not details:
            logger.error(f"[{self.site_key}] Could not load story details for {story_url} to get chapters.")
            return []

        chapters = list(details.get('chapters') or [])

        if not chapters:
            chapters = await self._fetch_chapters_via_ajax(
                story_url=story_url,
                manga_id=details.get('manga_id'),
                ajax_nonce=details.get('ajax_nonce'),
                total_expected=total_chapters
                if isinstance(total_chapters, int)
                else (
                    details.get('total_chapters_on_site')
                    if isinstance(details.get('total_chapters_on_site'), int)
                    else None
                ),
                chapter_ranges=details.get('chapter_ranges'),
                max_batches=max_pages,
            )
            if chapters:
                details['chapters'] = chapters

        if not chapters:
            html = await self._fetch_text(story_url, wait_for_selector="div.summary__content")
            if not html:
                logger.error(f"[{self.site_key}] Fallback HTML fetch failed for {story_url}.")
                return []
            chapters = parse_chapter_list(html, self.base_url)
            for ch in chapters:
                ch.setdefault('site_key', self.site_key)
            details['chapters'] = chapters
        else:
            for ch in chapters:
                ch.setdefault('site_key', self.site_key)

        async with self._details_lock:
            cached = self._details_cache.get(story_url)
            if cached is not None:
                cached['chapters'] = chapters
            else:
                self._details_cache[story_url] = details

        if max_pages and chapters:
            per_page = max(1, self.get_chapters_per_page_hint())
            limit = max_pages * per_page
            if len(chapters) > limit:
                logger.info(
                    f"[{self.site_key}] Applying chapter page limit: keeping first {limit}/{len(chapters)} entries."
                )
                chapters = chapters[:limit]

        return chapters


    async def get_chapter_content(self, chapter_url: str, chapter_title: str, site_key: str) -> Optional[str]:
        html = await self._fetch_text(chapter_url, wait_for_selector="#chapter-reading-content")
        if not html:
            return None

        parsed = parse_chapter_content(html)
        if not parsed:
            logger.warning(f"[{self.site_key}] Unable to parse content for chapter '{chapter_title}'")
            return None

        content_html = parsed.get('content') if isinstance(parsed, dict) else None
        if not content_html or not content_html.strip():
            logger.warning(f"[{self.site_key}] Empty content for chapter '{chapter_title}'")
            return None

        return content_html
