from typing import List, Dict, Any
from adapters.base_site_adapter import BaseSiteAdapter
from analyze.metruyenfull_parse import (
    get_all_categories,
    get_all_stories_from_category_with_page_check,
    get_stories_from_category,
    get_story_metadata,
    get_chapters_from_story,
)
from scraper import make_request
from utils.html_parser import extract_chapter_content
import asyncio
from config.config import BASE_URLS
from core.models.story import Genre, Story, Chapter
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

class MeTruyenFullAdapter(BaseSiteAdapter):
    SITE_KEY = "metruyenfull"
    BASE_URL = BASE_URLS[SITE_KEY]

    async def get_genres(self) -> List[Genre]:
        genres_data = await get_all_categories(self, self.BASE_URL)
        result = []
        for g in genres_data:
            try:
                result.append(Genre(**normalize_genre_dict(g)))
            except Exception as ex:
                logger.warning(f"[MeTruyenFullAdapter] Genre parse error: {g} - {ex}")
        return result

    async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None) -> List[Story]:
        stories_data = await get_stories_from_category(self, genre_url)
        result = []
        for s in stories_data:
            try:
                result.append(Story(**normalize_story_dict(s)))
            except Exception as ex:
                logger.warning(f"[MeTruyenFullAdapter] Story parse error: {s} - {ex}")
        return result

    async def get_story_details(self, story_url, story_title) -> Story:
        story_data = await get_story_metadata(self, story_url)
        if not story_data:
            logger.warning(f"[MeTruyenFullAdapter] Story details is None: {story_url}")
            return Story(title=story_title, url=story_url)
        try:
            return Story(**normalize_story_dict(story_data))
        except Exception as ex:
            logger.warning(f"[MeTruyenFullAdapter] Story details parse error: {story_data} - {ex}")
            return Story(title=story_title, url=story_url)


    async def get_chapter_list(self, story_url, story_title, max_pages=None, total_chapters=None) -> List[Chapter]:
        chapters_data = await get_chapters_from_story(self, story_url)
        result = []
        for c in chapters_data:
            try:
                result.append(Chapter(**normalize_chapter_dict(c)))
            except Exception as ex:
                logger.warning(f"[MeTruyenFullAdapter] Chapter parse error: {c} - {ex}")
        return result

    async def get_chapter_content(self, chapter_url, chapter_title) -> str:
        loop = asyncio.get_event_loop()
        def _get_content(chapter_url):
            resp = make_request(chapter_url)
            if not resp:
                return ""
            html = resp.text
            return extract_chapter_content(html)
        return await loop.run_in_executor(None, _get_content, chapter_url)

    async def get_all_stories_from_genre_with_page_check(self, genre_name, genre_url, max_pages=None):
        # Đoạn này trả về list story chuẩn, nhưng nếu parser gốc trả về tuple, bạn cũng cần unpack rồi normalize!
        stories_data, total_pages, crawled_pages = await get_all_stories_from_category_with_page_check(
            self, genre_name, genre_url, max_pages)
        result = []
        for s in stories_data:
            try:
                result.append(Story(**normalize_story_dict(s)))
            except Exception as ex:
                logger.warning(f"[MeTruyenFullAdapter] Story parse error (page_check): {s} - {ex}")
        return result, total_pages, crawled_pages
