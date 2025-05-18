from adapters.base_site_adapter import BaseSiteAdapter
from analyze.truyenfull_vision_parse import (
    get_all_genres,
    get_stories_from_genre_page,
    get_all_stories_from_genre,
    get_story_details,
    get_chapters_from_story,
    get_story_chapter_content,
)

class TruyenFullAdapter(BaseSiteAdapter):
    SITE_KEY = "truyenfull"
    async def get_genres(self):
        return await get_all_genres("https://truyenfull.vision")

    async def get_stories_in_genre(self, genre_url, page=1):
        return await get_stories_from_genre_page(genre_url)

    async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None):
        return await get_all_stories_from_genre(genre_name, genre_url, max_pages)

    async def get_story_details(self, story_url, story_title):
        return await get_story_details(story_url, story_title)

    async def get_chapter_list(self, story_url, story_title, max_pages=None, total_chapters=None):
        return await get_chapters_from_story(story_url, story_title, max_pages, total_chapters)

    async def get_chapter_content(self, chapter_url, chapter_title):
        return await get_story_chapter_content(chapter_url, chapter_title)
