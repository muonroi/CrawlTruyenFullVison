import asyncio
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from adapters.base_site_adapter import BaseSiteAdapter
from analyze.tangthuvien_parse import (
    find_genre_listing_url,
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



class TangThuVienAdapter(BaseSiteAdapter):
    _CHAPTER_PAGE_SIZE = 50

    def __init__(self) -> None:
        self.site_key = "tangthuvien"
        self.base_url = BASE_URLS.get(self.site_key, "https://tangthuvien.net")
        self._details_cache: Dict[str, Dict[str, Any]] = {}
        self._details_lock = asyncio.Lock()
        self._genre_listing_cache: Dict[str, str] = {}
        self._genre_listing_lock = asyncio.Lock()

    def _normalize_category_url(self, category_url: str) -> str:
        if not category_url:
            return self.base_url
        return urljoin(self.base_url.rstrip("/") + "/", category_url)

    def _normalize_story_url(self, story_url: str) -> str:
        if not story_url:
            return story_url

        absolute_url = urljoin(self.base_url.rstrip("/") + "/", story_url)
        parsed = urlparse(absolute_url)
        path = parsed.path or ""
        normalized_path = "/" + path.lstrip("/") if path else "/"

        if not normalized_path.lower().startswith("/doc-truyen/"):
            alias = normalized_path.strip("/")
            normalized_path = f"/doc-truyen/{alias}" if alias else "/doc-truyen"

        normalized = parsed._replace(path=normalized_path)
        return urlunparse(normalized)

    def get_chapters_per_page_hint(self) -> int:
        return self._CHAPTER_PAGE_SIZE

    async def get_genres(self) -> List[Dict[str, str]]:
        html = await self._fetch_text(self.base_url, wait_for_selector="div.update-wrap")
        if not html:
            return []
        genres = parse_genres(html, self.base_url)
        logger.info(f"[{self.site_key}] Found {len(genres)} genres")
        return genres

    async def _resolve_genre_listing_url(self, genre_url: str) -> str:
        normalized = self._normalize_category_url(genre_url)
        parsed = urlparse(normalized)
        if "/tong-hop" in (parsed.path or ""):
            return normalized

        async with self._genre_listing_lock:
            cached = self._genre_listing_cache.get(normalized)
            if cached:
                return cached

        html = await self._fetch_text(normalized, wait_for_selector="div.update-wrap")
        listing_url = None
        if html:
            listing_url = find_genre_listing_url(html, self.base_url)
            if listing_url:
                logger.debug(
                    f"[{self.site_key}] Resolved listing url for {normalized} -> {listing_url}"
                )

        resolved = listing_url or normalized
        async with self._genre_listing_lock:
            self._genre_listing_cache[normalized] = resolved
        return resolved

    async def _fetch_text(self, url: str, wait_for_selector: Optional[str] = None) -> Optional[str]:
        response = await make_request(url, self.site_key, wait_for_selector=wait_for_selector)
        if isinstance(response, str):
            return response
        if response and getattr(response, "text", None):
            status = getattr(response, "status_code", None)
            if status == 404:
                logger.warning(f"[{self.site_key}] Received 404 when fetching {url}")
                return None
            return response.text
        logger.error(f"[{self.site_key}] Failed to fetch URL: {url}")
        return None

    async def get_stories_in_genre(self, genre_url: str, page: int = 1) -> Tuple[List[Dict[str, str]], int]:
        listing_url = await self._resolve_genre_listing_url(genre_url)
        url = _with_page_parameter(listing_url, page)
        logger.debug(f"[{self.site_key}] Fetching stories page: {url}")
        html = await self._fetch_text(url, wait_for_selector="div.update-list")
        if not html:
            return [], 0
        stories, max_pages = parse_story_list(html, self.base_url)
        return stories, max_pages or 1

    async def get_all_stories_from_genre_with_page_check(
        self,
        genre_name: str,
        genre_url: str,
        site_key: str,
        max_pages: Optional[int] = None,
    ) -> Tuple[List[Dict[str, str]], int, int]:
        logger.info(f"[{self.site_key}] Crawling stories for genre '{genre_name}'")
        first_page_stories, total_pages = await self.get_stories_in_genre(genre_url, page=1)
        if not first_page_stories:
            logger.warning(f"[{self.site_key}] No stories detected on first page of {genre_name}")
            return [], 0, 0

        all_stories = list(first_page_stories)
        limit = max_pages or total_pages or 1
        crawled_pages = 1

        for page_number in range(2, limit + 1):
            stories, _ = await self.get_stories_in_genre(genre_url, page_number)
            crawled_pages += 1
            if not stories:
                logger.info(f"[{self.site_key}] Stop paging {genre_name}: empty page {page_number}")
                break
            all_stories.extend(stories)
            await asyncio.sleep(0.5)

        logger.info(f"[{self.site_key}] Total stories for {genre_name}: {len(all_stories)}")
        return all_stories, total_pages, crawled_pages

    async def get_all_stories_from_genre(
        self, genre_name: str, genre_url: str, max_pages: Optional[int] = None
    ) -> List[Dict[str, str]]:
        stories, _, _ = await self.get_all_stories_from_genre_with_page_check(
            genre_name=genre_name,
            genre_url=genre_url,
            site_key=self.site_key,
            max_pages=max_pages,
        )
        return stories

    async def _fetch_chapters_via_api(
        self,
        story_id: Optional[str],
        story_url: str,
        total_expected: Optional[int] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        if not story_id:
            logger.warning(f"[{self.site_key}] Missing story identifier for {story_url}")
            return []

        chapters: List[Dict[str, str]] = []
        seen: Set[str] = set()
        page_index = 0

        while True:
            if max_pages is not None and page_index >= max_pages:
                break

            api_base = urljoin(self.base_url, f"/doc-truyen/page/{story_id}")
            query_url = f"{api_base}?page={page_index}&limit={self._CHAPTER_PAGE_SIZE}&web=1"
            response = await make_request(query_url, self.site_key)
            if isinstance(response, str):
                text = response
            elif response and getattr(response, "text", None):
                text = response.text
            else:
                text = None

            if not text:
                logger.info(f"[{self.site_key}] Empty chapter payload for {story_url} page {page_index}")
                break

            batch = parse_chapter_list(text, self.base_url)
            new_items = 0
            for chapter in batch:
                url_key = chapter.get("url")
                if not url_key or url_key in seen:
                    continue
                seen.add(url_key)
                chapter.setdefault("site_key", self.site_key)
                chapters.append(chapter)
                new_items += 1

            if total_expected and len(chapters) >= total_expected:
                break

            if new_items < self._CHAPTER_PAGE_SIZE:
                break

            page_index += 1

        chapters.sort(key=get_chapter_sort_key)
        return chapters

    async def _get_story_details_internal(self, story_url: str) -> Optional[Dict[str, Any]]:
        html = await self._fetch_text(story_url, wait_for_selector="div.book-info")
        if not html:
            return None

        details = parse_story_info(html, self.base_url)
        inline_chapters = details.get("chapters") or []
        total_expected = details.get("total_chapters_on_site")
        if not isinstance(total_expected, int):
            total_expected = None

        needs_api = total_expected and len(inline_chapters) < total_expected

        if needs_api or not inline_chapters:
            api_chapters = await self._fetch_chapters_via_api(
                story_id=details.get("story_id"),
                story_url=story_url,
                total_expected=total_expected,
            )
            if api_chapters:
                details["chapters"] = api_chapters
            else:
                for chapter in inline_chapters:
                    chapter.setdefault("site_key", self.site_key)
                details["chapters"] = inline_chapters
        else:
            for chapter in inline_chapters:
                chapter.setdefault("site_key", self.site_key)

        details["sources"] = [
            {"url": story_url, "site_key": self.site_key, "priority": 1}
        ]
        details["url"] = story_url
        return details

    async def get_story_details(self, story_url: str, story_title: str) -> Optional[Dict[str, Any]]:
        normalized_url = self._normalize_story_url(story_url)

        async with self._details_lock:
            cached = self._details_cache.get(normalized_url)
            if cached:
                return cached
            details = await self._get_story_details_internal(normalized_url)
            if details:
                self._details_cache[normalized_url] = details
            return details

    async def get_chapter_list(
        self,
        story_url: str,
        story_title: str,
        site_key: str,
        max_pages: Optional[int] = None,
        total_chapters: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        normalized_url = self._normalize_story_url(story_url)
        logger.debug(f"[{self.site_key}] Getting chapter list for '{story_title}'.")

        details = await self.get_story_details(normalized_url, story_title)
        if not details:
            logger.error(f"[{self.site_key}] Could not load story details for {normalized_url}")
            return []

        chapters = list(details.get("chapters") or [])
        chapter_source = "details"
        if not chapters:
            expected = None
            if isinstance(total_chapters, int):
                expected = total_chapters
            elif isinstance(details.get("total_chapters_on_site"), int):
                expected = details["total_chapters_on_site"]

            chapters = await self._fetch_chapters_via_api(
                story_id=details.get("story_id"),
                story_url=normalized_url,
                total_expected=expected,
                max_pages=max_pages,
            )
            if chapters:
                details["chapters"] = chapters
                chapter_source = "api"

        if not chapters:
            html = await self._fetch_text(normalized_url, wait_for_selector="div.book-info")
            if not html:
                logger.error(f"[{self.site_key}] Fallback HTML fetch failed for {normalized_url}.")
                return []
            chapters = parse_chapter_list(html, self.base_url)
            chapter_source = "html"

        for chapter in chapters:
            chapter.setdefault("site_key", self.site_key)

        async with self._details_lock:
            cached = self._details_cache.get(normalized_url)
            if cached is not None:
                cached["chapters"] = chapters
            else:
                self._details_cache[normalized_url] = details

        if max_pages and chapters:
            per_page = max(1, self.get_chapters_per_page_hint())
            limit = max_pages * per_page
            if len(chapters) > limit:
                logger.info(
                    f"[{self.site_key}] Applying chapter page limit: keeping first {limit}/{len(chapters)} entries."
                )
                chapters = chapters[:limit]
                chapter_source = f"{chapter_source}-trimmed"

        logger.info(
            f"[{self.site_key}] Retrieved {len(chapters)} chapters for '{story_title}' from {chapter_source}."
        )
        return chapters


    async def get_chapter_content(self, chapter_url: str, chapter_title: str, site_key: str) -> Optional[str]:
        html = await self._fetch_text(chapter_url, wait_for_selector="div.chapter-c-content")
        if not html:
            return None

        parsed = parse_chapter_content(html)
        if not parsed:
            logger.warning(f"[{self.site_key}] Unable to parse content for chapter '{chapter_title}'")
            return None

        content_html = parsed.get("content") if isinstance(parsed, dict) else None
        if not content_html or not content_html.strip():
            logger.warning(f"[{self.site_key}] Empty content for chapter '{chapter_title}'")
            return None

        return content_html
