from bs4 import BeautifulSoup
from adapters.base_site_adapter import BaseSiteAdapter
from analyze.truyenyy_parse import (
    get_all_genres,
    get_stories_from_genre_page,
    get_all_stories_from_genre,
    get_story_details,
    get_chapters_from_story,
    get_story_chapter_content
)
from config.config import BASE_URLS
class TruyenYYAdapter(BaseSiteAdapter):
    SITE_KEY = "truyenyy"
    BASE_URL = BASE_URLS[SITE_KEY]

    async def get_genres(self):
        # Gọi hàm phân tích riêng hoặc tự viết luôn
        return await get_all_genres(self,self.BASE_URL)

    async def get_stories_in_genre(self, genre_url, page=1):
        return await get_stories_from_genre_page(self, genre_url, page)

    async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None):
        return await get_all_stories_from_genre(self, genre_name, genre_url, max_pages)

    async def get_story_details(self, story_url, story_title):
        return await get_story_details(self, story_url, story_title)

    async def get_chapter_list(self, story_url, story_title, max_pages=None, total_chapters=None):
        return await get_chapters_from_story(self, story_url, story_title, max_pages, total_chapters)

    async def get_chapter_content(self, chapter_url, chapter_title):
        return await get_story_chapter_content(self, chapter_url, chapter_title)

    async def get_all_stories_from_genre_with_page_check(self, genre_name, genre_url, max_pages=None):
        return await self.get_all_stories_from_genre_with_page_check(genre_name, genre_url, max_pages)

    async def extract_chapter_content(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        chapter_div = soup.select_one("body > main > main > div > section:nth-child(4) > article")
        if not chapter_div:
            return ""
        return chapter_div.get_text(separator="\n", strip=True)
