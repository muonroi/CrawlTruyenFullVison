from typing import List, Dict, Any
from adapters.base_site_adapter import BaseSiteAdapter
from analyze.truyenfull_vision_parse import (
    get_all_genres,
    get_all_stories_from_genre_with_page_check,
    get_stories_from_genre_page,
    get_all_stories_from_genre,
    get_story_details,
    get_chapters_from_story,
    get_story_chapter_content,
)
from config.config import BASE_URLS
from core.models.story import Chapter, Genre, Story
from utils.logger import logger

def normalize_genre_dict(g: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": g.get("name", ""),
        "url": g.get("url", "")
    }

def normalize_story_dict(s: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "title": s.get("title", ""),
        "url": s.get("url", ""),
        "author": s.get("author"),
        "categories": s.get("categories", []),
        "total_chapters_on_site": s.get("total_chapters_on_site"),
        "cover": s.get("cover"),
        "description": s.get("description"),
        "status": s.get("status"),
    }

def normalize_chapter_dict(c: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "title": c.get("title", ""),
        "url": c.get("url", ""),
    }

class TruyenFullAdapter(BaseSiteAdapter):
    SITE_KEY = "truyenfull"
    BASE_URL = BASE_URLS[SITE_KEY]

    async def get_genres(self) -> List[Genre]:
        genres_data = await get_all_genres(self.BASE_URL)
        result = []
        for g in genres_data:
            try:
                result.append(Genre(**normalize_genre_dict(g)))
            except Exception as ex:
                logger.warning(f"[TruyenFullAdapter] Genre parse error: {g} - {ex}")
        return result

    async def get_stories_in_genre(self, genre_url, page=1) -> List[Story]:
        stories_data = await get_stories_from_genre_page(genre_url)
        if isinstance(stories_data, tuple):
            stories_data = stories_data[0]
        if not stories_data or not isinstance(stories_data, list):
            logger.warning(f"[TruyenFullAdapter] get_stories_in_genre trả về dữ liệu không hợp lệ: {stories_data}")
            return []
        result = []
        for s in stories_data:
            if not isinstance(s, dict):
                logger.warning(f"[TruyenFullAdapter] Một story không phải dict: {s}")
                continue
            try:
                result.append(Story(**normalize_story_dict(s)))
            except Exception as ex:
                logger.warning(f"[TruyenFullAdapter] Story parse error: {s} - {ex}")
        return result


    async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None) -> List[Story]:
        stories_data = await get_all_stories_from_genre(genre_name, genre_url, max_pages)
        result = []
        for s in stories_data:
            try:
                result.append(Story(**normalize_story_dict(s)))
            except Exception as ex:
                logger.warning(f"[TruyenFullAdapter] Story parse error: {s} - {ex}")
        return result

    async def get_story_details(self, story_url, story_title) -> Story:
        story_data = await get_story_details(story_url, story_title)
        try:
            return Story(**normalize_story_dict(story_data))
        except Exception as ex:
            logger.warning(f"[TruyenFullAdapter] Story details parse error: {story_data} - {ex}")
            return Story(title=story_title, url=story_url)

    async def get_chapter_list(self, story_url, story_title, max_pages=None, total_chapters=None) -> List[Chapter]:
        chapters_data = await get_chapters_from_story(story_url, story_title, max_pages, total_chapters)
        result = []
        for c in chapters_data:
            try:
                result.append(Chapter(**normalize_chapter_dict(c)))
            except Exception as ex:
                logger.warning(f"[TruyenFullAdapter] Chapter parse error: {c} - {ex}")
        return result

    async def get_chapter_content(self, chapter_url, chapter_title) -> str | None:
        return await get_story_chapter_content(chapter_url, chapter_title)

    async def get_all_stories_from_genre_with_page_check(self, genre_name, genre_url, max_pages=None):
        return await get_all_stories_from_genre_with_page_check(genre_name, genre_url, max_pages)
