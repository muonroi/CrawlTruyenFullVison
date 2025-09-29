import asyncio
import re
from typing import Any, Dict, List, Optional, Set, Tuple
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
    _CHAPTER_LIST_BATCH = 200

    def __init__(self) -> None:
        self.site_key = "xtruyen"
        self.base_url = BASE_URLS.get(self.site_key, "https://xtruyen.vn")
        self._details_cache: Dict[str, Dict[str, Any]] = {}
        self._details_lock = asyncio.Lock()

    @staticmethod
    def _chapter_sort_key(chapter: Dict[str, str]) -> Tuple[int, str]:
        url = chapter.get('url', '')
        title = chapter.get('title', '')
        number_match = re.search(r'(?:chuong|chapter)[^0-9]*([0-9]+)', url, re.IGNORECASE)
        if not number_match:
            number_match = re.search(r'(?:ch(?:u|\u01b0)\u01a1ng|chapter)\s*([0-9]+)', title, re.IGNORECASE)
        number = int(number_match.group(1)) if number_match else 0
        return number, url

    async def _fetch_chapters_via_ajax(
        self,
        story_url: str,
        manga_id: Optional[str],
        ajax_nonce: Optional[str],
        total_expected: Optional[int],
    ) -> List[Dict[str, str]]:
        if not manga_id:
            logger.warning(f"[{self.site_key}] Missing manga_id for story {story_url}, cannot load chapters via AJAX.")
            return []

        ajax_url = urljoin(self.base_url, '/wp-admin/admin-ajax.php')
        collected: List[Dict[str, str]] = []
        seen_urls: Set[str] = set()
        start = 1

        extra_headers = {'Referer': story_url}
        base_payload: Dict[str, Any] = {
            'action': 'load_chapter_list_from_to',
            'manga_id': manga_id,
        }
        if ajax_nonce:
            # Some Madara deployments expect nonce under different keys; send both to be safe.
            base_payload.setdefault('security', ajax_nonce)
            base_payload.setdefault('nonce', ajax_nonce)

        while True:
            payload = dict(base_payload)
            payload['from'] = start
            payload['to'] = start + self._CHAPTER_LIST_BATCH - 1

            logger.debug(
                f"[{self.site_key}] AJAX chapters request for manga {manga_id}: from={payload['from']} to={payload['to']}"
            )

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

            if len(new_items) < self._CHAPTER_LIST_BATCH:
                break

            start += self._CHAPTER_LIST_BATCH
            await asyncio.sleep(0.25)

        collected.sort(key=self._chapter_sort_key)
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
