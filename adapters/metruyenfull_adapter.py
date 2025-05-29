from bs4 import BeautifulSoup
from adapters.base_site_adapter import BaseSiteAdapter
from analyze.metruyenfull_parse import (
    get_all_categories,
    get_stories_from_category,
    get_story_metadata,
    get_chapters_from_story,
    get_all_stories_from_category_with_page_check
)
from config.config import BASE_URLS
import asyncio
from scraper import make_request 

class MeTruyenFullAdapter(BaseSiteAdapter):
    SITE_KEY = "metruyenfull"
    BASE_URL = BASE_URLS[SITE_KEY]

    async def get_genres(self):
        return await get_all_categories(self, self.BASE_URL)

    async def get_stories_in_genre(self, genre_url, page=1):
        return await get_stories_from_category(self, genre_url)  # page không dùng, để tương thích

    async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None):
        return await get_stories_from_category(self, genre_url)

    async def get_story_details(self, story_url, story_title):
        return await get_story_metadata(self, story_url)

    async def get_chapter_list(self, story_url, story_title,site_key, max_pages=None, total_chapters=None):
        return await get_chapters_from_story(self, story_url, story_title, total_chapters_on_site=total_chapters, site_key=site_key)

    async def get_chapter_content(self, chapter_url, chapter_title):
        loop = asyncio.get_event_loop()
        def _get_content(chapter_url):
            resp = make_request(chapter_url)
            if not resp:
                return ""
            html = resp.text
            return asyncio.run(self.extract_chapter_content(html))  # Sử dụng hàm extract riêng của class
        return await loop.run_in_executor(None, _get_content, chapter_url)

    async def get_all_stories_from_genre_with_page_check(self, genre_name, genre_url, max_pages=None):
        return await get_all_stories_from_category_with_page_check(self, genre_name, genre_url, max_pages)
    
    async def extract_chapter_content(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        chapter_div = soup.find("div", id="chapter-c") or soup.find("div", class_="chapter-c")
        if not chapter_div:
            return ""
        return chapter_div.get_text(separator="\n")
