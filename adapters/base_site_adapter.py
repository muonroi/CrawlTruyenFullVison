from abc import ABC, abstractmethod

class BaseSiteAdapter(ABC):
    async def get_genres(self):
        raise NotImplementedError

    async def get_stories_in_genre(self, genre_url, page=1):
        raise NotImplementedError

    async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None):
        raise NotImplementedError

    async def get_story_details(self, story_url, story_title):
        raise NotImplementedError

    async def get_chapter_list(self, story_url, story_title, max_pages=None, total_chapters=None):
        raise NotImplementedError

    async def get_chapter_content(self, chapter_url, chapter_title):
        raise NotImplementedError
