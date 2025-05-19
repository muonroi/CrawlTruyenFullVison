import asyncio
from concurrent.futures import ThreadPoolExecutor
from adapters.base_site_adapter import BaseSiteAdapter
from analyze.metruyenfull_parse import (
    get_all_categories,
    get_all_stories_from_category_with_page_check,
    get_stories_from_category,
    get_story_metadata,
    get_chapters_from_story
)
from config.config import BASE_URLS
_executor = ThreadPoolExecutor(max_workers=8)

class MeTruyenFullAdapter(BaseSiteAdapter):
    SITE_KEY = "metruyenfull"
    BASE_URL = BASE_URLS[SITE_KEY]
    async def get_genres(self):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            _executor, get_all_categories, self, self.BASE_URL
        )

    async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            _executor, get_stories_from_category, self, genre_url
        )

    async def get_story_details(self, story_url, story_title):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            _executor, get_story_metadata, self, story_url
        )

    async def get_chapter_list(self, story_url, story_title, max_pages=None, total_chapters=None):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            _executor, get_chapters_from_story, self, story_url
        )

    async def get_chapter_content(self, chapter_url, chapter_title):
        loop = asyncio.get_event_loop()
        from scraper import make_request
        def _get_content(chapter_url):
            resp = make_request(chapter_url)
            if not resp:
                return ""
            html = resp.text
            from utils.html_parser import extract_chapter_content
            return extract_chapter_content(html)
        return await loop.run_in_executor(
            _executor, _get_content, chapter_url
        )
    async def get_all_stories_from_genre_with_page_check(self, genre_name, genre_url, max_pages=None):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            _executor, get_all_stories_from_category_with_page_check, self, genre_name, genre_url, max_pages
        )