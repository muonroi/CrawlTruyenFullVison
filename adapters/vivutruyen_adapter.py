# from bs4 import BeautifulSoup
# from adapters.base_site_adapter import BaseSiteAdapter
# from analyze.vivutruyen_parse import (
#     get_all_genres,
#     get_stories_from_genre,
#     get_story_details,
#     get_chapter_list,
#     get_story_chapter_content,
#     get_all_stories_from_genre_with_page_check
# )
# from config.config import BASE_URLS

# class VivuTruyenAdapter(BaseSiteAdapter):
#     SITE_KEY = "vivutruyen"
#     BASE_URL = BASE_URLS[SITE_KEY]

#     async def get_genres(self):
#         return await get_all_genres(self.BASE_URL)

#     async def get_stories_in_genre(self, genre_url, page=1):
#         return await get_stories_from_genre(genre_url, page)

#     async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None):
#         return await get_stories_from_genre(genre_url, max_pages)

#     async def get_story_details(self, story_url, story_title):
#         return await get_story_details(story_url, story_title)

#     async def get_chapter_list(self, story_url, story_title,site_key, max_pages=None, total_chapters=None):
#         return await get_chapter_list(self, story_url, story_title, max_pages, total_chapters, site_key = site_key)

#     async def get_chapter_content(self, chapter_url, chapter_title):
#         return await get_story_chapter_content(chapter_url, chapter_title)

#     async def get_all_stories_from_genre_with_page_check(self, genre_name, genre_url, max_pages=None):
#         return await get_all_stories_from_genre_with_page_check(genre_name, genre_url, max_pages)

#     async def extract_chapter_content(self, html: str) -> str:
#         soup = BeautifulSoup(html, "html.parser")
#         chapter_div = soup.find("div", class_="truyen")
#         if not chapter_div:
#             return ""
#         return chapter_div.get_text(separator="\n")
