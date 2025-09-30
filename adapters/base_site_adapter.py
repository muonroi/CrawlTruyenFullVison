from abc import ABC, abstractmethod

class BaseSiteAdapter(ABC):
    @abstractmethod
    async def get_genres(self):
        pass

    @abstractmethod
    async def get_stories_in_genre(self, genre_url, page=1):
        pass

    @abstractmethod
    async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None):
        pass

    @abstractmethod
    async def get_story_details(self, story_url, story_title):
        pass

    @abstractmethod
    async def get_chapter_list(self, story_url, story_title, site_key, max_pages=None, total_chapters=None):
        pass

    @abstractmethod
    async def get_chapter_content(self, chapter_url, chapter_title, site_key):
        pass

    @abstractmethod
    async def get_all_stories_from_genre_with_page_check(self, genre_name, genre_url, site_key,max_pages=None):
        pass

    def get_chapters_per_page_hint(self) -> int:
        """Return an estimated number of chapters per page for chapter listings.

        Adapters can override this value to provide a more accurate hint that will
        be used when applying generic crawl limits.
        """
        return 100
