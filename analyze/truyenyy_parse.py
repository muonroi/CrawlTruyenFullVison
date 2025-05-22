import asyncio
from asyncio.log import logger
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scraper import make_request


async def get_all_genres(self, homepage_url):
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, homepage_url)
    if not resp or not getattr(resp, 'text', None):
        logger.error(f"Không lấy được trang chủ {homepage_url}")
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    genres = []
    container = soup.select_one(
        "section.grid.grid-cols-4.mt-6 > div.relative.text-xs.w-full.overflow-hidden.rounded-lg.bg-\\[\\#343a40\\].text-white.py-1 > div.grid.grid-cols-2"
    )
    if not container:
        logger.error("Không tìm thấy container thể loại theo selector")
        return []
    for a in container.find_all("a", href=True):
        name = a.get_text(strip=True)
        href = a['href']# type: ignore
        full_url = urljoin(self.BASE_URL, href)# type: ignore
        genres.append({"name": name, "url": full_url})
    return genres

async def get_stories_from_genre_page(self, genre_url, page=1):
    if page > 1:
        from urllib.parse import urlparse
        parsed = urlparse(genre_url)
        base = parsed._replace(query="").geturl()
        url = f"{base}/danh-sach?p={page}"
    else:
        url = f"{genre_url}/danh-sach"
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, url)
    if not resp or not getattr(resp, 'text', None):
        logger.error(f"Không lấy được trang {url}")
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    stories = []
    for div in soup.select("div.flex.flex-col.space-y-1"):
        a = div.select_one("h3 a[href]")
        if not a:
            continue
        title = a.get_text(strip=True)
        href = a['href']
        full_url = urljoin(self.BASE_URL, href)# type: ignore
        author_tag = div.select_one("p.text-sm.text-gray-400 a")
        author = author_tag.get_text(strip=True) if author_tag else None
        stories.append({
            "title": title,
            "url": full_url,
            "author": author
        })
    return stories

async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None):
    all_stories = []
    page = 1
    while True:
        if max_pages is not None and page > max_pages:
            break
        stories = await self.get_stories_from_genre_page(genre_url, page)
        if not stories:
            break
        all_stories.extend(stories)
        page += 1
    return all_stories

async def get_story_details(self, story_url, story_title):
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, story_url)
    if not resp or not getattr(resp, 'text', None):
        logger.error(f"Không lấy được chi tiết truyện {story_url}")
        return {}
    soup = BeautifulSoup(resp.text, "html.parser")
    details = {
        "title": None,
        "author": None,
        "cover": None,
        "description": None,
        "categories": [],
        "status": None,
        "source": story_url,
        "rating_value": None,
        "rating_count": None,
        "total_chapters_on_site": None,
    }
    # Title
    title_tag = soup.select_one("div.lg\\:hidden h1.text-\\[18px\\].font-title")
    details["title"] = title_tag.get_text(strip=True) if title_tag else story_title
    # Author
    author_tag = soup.select_one("div.lg\\:hidden h1.text-\\[18px\\].font-title + span > p.hover\\:text-\\[\\#269bfa\\]")
    details["author"] = author_tag.get_text(strip=True) if author_tag else None
    # Cover
    img_tag = soup.select_one("div.flex.flex-col.space-y-2 img")
    if img_tag and img_tag.has_attr("src"):
        details["cover"] = img_tag["src"]
    # Description
    desc_tag = soup.select_one("div.lg\\:hidden div.border-\\[1px\\].text-wrap div.px-2\\.py-3 > p.prose")
    if desc_tag:
        details["description"] = desc_tag.get_text(separator="\n", strip=True)
    # Categories, trạng thái, số chương
    for li in soup.select("div.lg\\:hidden ul.mt-2.text-start.flex-col > li"):
        text = li.get_text(strip=True).lower()
        if "thể loại" in text or "category" in text:
            categories = []
            for a in li.select("a[href]"):
                name = a.get_text(strip=True)
                href = urljoin(self.BASE_URL, a["href"])# type: ignore
                categories.append({"name": name, "url": href})
            details["categories"] = categories
        elif "trạng thái" in text or "status" in text:
            details["status"] = li.get_text(strip=True)
        elif "chương" in text or "chapter" in text:
            import re
            match = re.search(r"(\d+)", li.get_text())
            if match:
                details["total_chapters_on_site"] = int(match.group(1))
    # Fallback đếm chương nếu không có
    if not details["total_chapters_on_site"]:
        chapters = await self._get_chapters_from_story(story_url, story_title)
        details["total_chapters_on_site"] = len(chapters)
    return details

async def get_chapters_from_story(self, story_url, story_title, max_pages=None, total_chapters=None):
    chapters = []
    page = 1
    while True:
        if max_pages is not None and page > max_pages:
            break
        if page == 1:
            url = story_url
        else:
            url = f"{story_url.rstrip('/')}/danh-sach-chuong?p={page}"
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(None, make_request, url)
        if not resp or not getattr(resp, "text", None):
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        found_chapters_on_page = False
        for li in soup.select("ul.chapter-list li a[href]"):
            href = li["href"]
            title = li.get_text(strip=True)
            full_url = urljoin(self.BASE_URL, href) # type: ignore
            chapters.append({"url": full_url, "title": title})
            found_chapters_on_page = True
        if not found_chapters_on_page:
            break
        if total_chapters and len(chapters) >= total_chapters:
            break
        pagination = soup.select_one("ul.pagination")
        if not pagination or not pagination.find("a", string=str(page + 1)):
            break
        page += 1
    return chapters

async def get_story_chapter_content(self, chapter_url, chapter_title):
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, chapter_url)
    if not resp or not getattr(resp, "text", None):
        logger.error(f"Không lấy được nội dung chương {chapter_title}")
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    content_div = soup.select_one("body > main > main > div > section:nth-child(4) > article")
    if not content_div:
        logger.warning(f"Nội dung chương trống hoặc không tìm thấy selector: {chapter_title}")
        return None
    return content_div.get_text(separator="\n", strip=True)