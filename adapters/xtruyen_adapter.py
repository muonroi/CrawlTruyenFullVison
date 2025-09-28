import asyncio
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

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
from utils.logger import logger


def _with_page_parameter(url: str, page: int) -> str:
    if page <= 1:
        return url
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query['page'] = [str(page)]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


class XTruyenAdapter(BaseSiteAdapter):
    def __init__(self) -> None:
        self.site_key = "xtruyen"
        self.base_url = BASE_URLS.get(self.site_key, "https://xtruyen.vn")
        self._details_cache: Dict[str, Dict[str, Any]] = {}
        self._details_lock = asyncio.Lock()

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
    ) -> Tuple[List[Dict[str, str]], int, int]:
        logger.info(f"[{self.site_key}] Crawling stories for genre '{genre_name}'")
        first_page_stories, total_pages = await self.get_stories_in_genre(genre_url, page=1)
        if not first_page_stories:
            logger.warning(f"[{self.site_key}] No stories detected on first page of {genre_name}")
            return [], 0, 0

        all_stories = list(first_page_stories)
        limit = max_pages or total_pages or 1
        crawled_pages = 1

        for page in range(2, limit + 1):
            stories, _ = await self.get_stories_in_genre(genre_url, page)
            crawled_pages += 1
            if not stories:
                logger.info(f"[{self.site_key}] Stop paging {genre_name}: empty page {page}")
                break
            all_stories.extend(stories)
            await asyncio.sleep(0.5)
        logger.info(f"[{self.site_key}] Total stories for {genre_name}: {len(all_stories)}")
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
        if details.get('chapters'):
            for ch in details['chapters']:
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
        """Gets the chapter list by parsing the story page HTML, avoiding the fragile AJAX call."""
        logger.debug(f"[{self.site_key}] Getting chapter list for '{story_title}' from story page HTML.")
        
        html = await self._fetch_text(story_url, wait_for_selector="div.summary__content")
        if not html:
            logger.error(f"[{self.site_key}] Could not fetch story page {story_url} to get chapter list.")
            return []

        chapters = parse_chapter_list(html, self.base_url)
        
        logger.info(f"[{self.site_key}] Found {len(chapters)} chapters for '{story_title}' from HTML.")

        for ch in chapters:
            ch.setdefault('site_key', self.site_key)

        # Update cache if possible
        async with self._details_lock:
            if story_url in self._details_cache:
                self._details_cache[story_url]['chapters'] = chapters

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
