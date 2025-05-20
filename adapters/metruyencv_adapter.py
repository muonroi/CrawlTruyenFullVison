from adapters.base_site_adapter import BaseSiteAdapter
from analyze.metruyencv_parse import (
    get_all_categories,
    get_all_stories_from_category_with_page_check,
    get_stories_from_category,
    get_story_metadata,
    get_chapters_from_story,
    get_story_chapter_content
)
from config.config import BASE_URLS

class MetruyenCVAdapter(BaseSiteAdapter):
    SITE_KEY = "metruyencv"
    BASE_URL = BASE_URLS.get(SITE_KEY, "https://metruyencv.info")

    async def get_genres(self):
        return await get_all_categories(self.BASE_URL)

    async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None):
        return await get_stories_from_category(genre_url, max_pages)

    async def get_story_details(self, story_url, story_title):
        return await get_story_metadata(story_url)

    async def get_chapter_list(self, story_url, story_title, max_pages=None, total_chapters=None):
        return await get_chapters_from_story(story_url)

    async def get_chapter_content(self, chapter_url, chapter_title):
        return await get_story_chapter_content(chapter_url, chapter_title)

    async def get_all_stories_from_genre_with_page_check(self, genre_name, genre_url, max_pages=None):
        return await get_all_stories_from_category_with_page_check(genre_name, genre_url, max_pages)
