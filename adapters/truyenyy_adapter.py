from adapters.base_site_adapter import BaseSiteAdapter
from analyze.truyenyy_parse import (
    get_all_genres,
    get_stories_from_genre_page,
    get_all_stories_from_genre,
    get_story_details,
    get_chapters_from_story,
    get_story_chapter_content,
    get_all_stories_from_genre_with_page_check 
)
from config.config import BASE_URLS
class TruyenYYAdapter(BaseSiteAdapter):
    SITE_KEY = "truyenyy"
    BASE_URL = BASE_URLS[SITE_KEY]

    async def get_genres(self):
        return await get_all_genres(self,self.BASE_URL)

    async def get_stories_in_genre(self, genre_page_url, page):
        return await get_stories_from_genre_page(self, genre_page_url, page)

    async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None):
        return await get_all_stories_from_genre(self, genre_name, genre_url, max_pages)

    async def get_story_details(self, story_url, story_title):
        return await get_story_details(self, story_url, story_title, self.SITE_KEY)

    async def get_chapter_list(self, story_url, story_title, site_key=None, max_pages=None, total_chapters=None):
        return await get_chapters_from_story(self, story_url, story_title, max_pages, total_chapters, site_key)

    async def get_chapter_content(self, chapter_url, chapter_title, site_key):
        return await get_story_chapter_content(self, chapter_url, chapter_title, site_key)

    async def get_all_stories_from_genre_with_page_check(self, genre_name, genre_url, site_key, max_pages=None):
        return await get_all_stories_from_genre_with_page_check(self, genre_name, genre_url, site_key,max_pages)