import asyncio
from asyncio.log import logger
import re
from typing import Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from scraper import make_request
from utils.html_parser import clean_header, extract_chapter_content, get_total_pages_category

def build_category_list_url(genre_url, page=1):
    base = genre_url.rstrip('/')
    if page > 1:
        return f"{base}?p={page}"
    else:
        return f"{base}"

async def get_all_stories_from_genre_with_page_check(self, genre_name, genre_url, max_pages=None):
    from urllib.parse import urljoin
    loop = asyncio.get_event_loop()

    def build_category_list_url(genre_url, page=1):
        base = genre_url.rstrip('/')
        if page > 1:
            return f"{base}?p={page}"
        else:
            return base

    first_page_url = build_category_list_url(genre_url, page=1)
    resp = await loop.run_in_executor(None, make_request, first_page_url)
    if not resp:
        return [], 0, 0
    html = resp.text
    total_pages = get_total_pages_category(html)
    if max_pages:
        total_pages = min(total_pages, max_pages)

    all_stories = []
    pages_crawled = 0
    seen_urls = set()

    for page in range(1, total_pages + 1):
        page_url = build_category_list_url(genre_url, page)
        resp = await loop.run_in_executor(None, make_request, page_url)
        if not resp:
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        ul = soup.select_one("ul.flex.flex-col")
        if not ul:
            continue
        for li in ul.find_all("li", recursive=False):
            # 1. Lấy url + cover
            a_img = li.select_one("a[href*='/truyen/']")
            cover = None
            url = None
            if a_img:
                url = urljoin(self.BASE_URL, a_img['href'])
                img_tag = a_img.find("img")
                if img_tag and img_tag.has_attr('src'):
                    cover = img_tag['src']

            # 2. Lấy title
            h3 = li.select_one("h3.font-title")
            title = h3.get_text(strip=True) if h3 else None

            # 3. Lấy author
            author_p = li.select_one("p.text-xs.font-thin")
            author = author_p.get_text(strip=True) if author_p else None

            # 4. Lấy số chương
            chapter_p = li.select_one("div.rounded.border > p.text-xs")
            total_chapters = None
            if chapter_p:
                match = re.search(r"(\d+)", chapter_p.get_text())
                if match:
                    total_chapters = int(match.group(1))

            # Check đủ url + title mới append
            if url and title and url not in seen_urls:
                all_stories.append({
                    "title": title,
                    "url": url,
                    "cover": cover,
                    "author": author,
                    "total_chapters": total_chapters
                })
                seen_urls.add(url)
        pages_crawled += 1

    return all_stories, total_pages, pages_crawled


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
        raw_name = a.get_text(strip=True)
        # Loại bỏ số ở cuối và prefix YY nếu có
        name = re.sub(r"\d+$", "", raw_name).strip()
        name = re.sub(r"^YY", "", name).strip()
        href = (a.get('href') or '').rstrip('/') + '/danh-sach'
        full_url = urljoin(self.BASE_URL, href)
        genres.append({"name": name, "url": full_url})
    return genres

async def get_stories_from_genre_page(self, genre_url, page=1):
    base = genre_url.rstrip('/')
    url = f"{base}?p={page}" if page > 1 else base
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, url)
    if not resp or not getattr(resp, 'text', None):
        logger.error(f"Không lấy được trang {url}")
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    ul = soup.select_one("ul.flex.flex-col")
    if not ul:
        logger.error(f"[YY][CATEGORY] Không tìm thấy ul.flex.flex-col ở {url}")
        with open("debug_yy_cat.html", "w", encoding="utf-8") as f:
            f.write(soup.prettify())
        return []
    stories = []
    for li in ul.find_all("li", recursive=False):
        a_tag = li.select_one("a[href*='/truyen/']")
        h3 = li.select_one("h3.font-title")
        title = h3.get_text(strip=True) if h3 else ""
        if not a_tag or not title:
            logger.warning(f"[YY][CATEGORY] Không lấy được url/title cho 1 item ở {url}")
            continue
        detail_url = urljoin(self.BASE_URL, a_tag['href'])
        # Lấy tên truyện
        h3 = a_tag.select_one("h3.font-title")
        title = h3.get_text(strip=True) if h3 else ""
        # Lấy tên tác giả
        author_p = li.select_one("p.text-xs.font-thin")
        author = author_p.get_text(strip=True) if author_p else None
        # Lấy số chương (nếu muốn)
        chapter_p = li.select_one("div.rounded.border > p.text-xs")
        if chapter_p:
            import re
            chapter_match = re.search(r"(\d+)", chapter_p.get_text())
            total_chapters = int(chapter_match.group(1)) if chapter_match else None
        else:
            total_chapters = None
        stories.append({
            "title": title,
            "url": detail_url,
            "author": author,
            "total_chapters": total_chapters
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

async def get_story_details(self, story_url, story_title, site_key):
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
    title_tag = soup.select_one("h1.font-title")
    details["title"] = title_tag.get_text(strip=True) if title_tag else story_title

    # Author
    # Lấy p.font-title đứng sau h1
    author = None
    if title_tag:
        author_p = title_tag.find_next_sibling("p", class_="font-title")
        if author_p:
            author = author_p.get_text(strip=True)
    if not author:
        # fallback: lấy p.font-title đầu tiên không phải h1
        all_p = soup.select("p.font-title")
        if all_p:
            author = all_p[0].get_text(strip=True)
    details["author"] = author

    # Categories
    categories = []
    cat_div = soup.select_one("div.flex.flex-wrap.gap-2.text-\\[12px\\].max-w-\\[640px\\]")
    if cat_div:
        for a in cat_div.find_all("a"):
            href = a.get("href")
            name = a.get_text(strip=True)
            if href and name:
                categories.append({"name": name, "url": urljoin(self.BASE_URL, href)})
    details["categories"] = categories

    # Số chương
    total_chapters = None
    for p in soup.select("p.text-base"):
        if p.find("small") and "chương" in p.find("small").get_text(strip=True).lower():
            match = re.search(r"(\d+)", p.get_text())
            if match:
                total_chapters = int(match.group(1))
    details["total_chapters_on_site"] = total_chapters

    # Description
    desc_tag = soup.select_one("p.prose")
    if desc_tag:
        details["description"] = desc_tag.get_text(separator="\n", strip=True)

    # trạng thái, số chương
    for li in soup.select("div.lg\\:hidden ul.mt-2.text-start.flex-col > li"):
        text = li.get_text(strip=True).lower()
        if "trạng thái" in text or "status" in text:
            details["status"] = li.get_text(strip=True)
        elif "chương" in text or "chapter" in text:
            match = re.search(r"(\d+)", li.get_text())
            if match:
                details["total_chapters_on_site"] = int(match.group(1))

    # Cover
    cover_img = soup.select_one("div.rounded-md.w-\\[160px\\].h-\\[240px\\].overflow-hidden img")
    if cover_img and cover_img.has_attr("src"):
        details["cover"] = cover_img["src"]
    # Fallback đếm chương nếu không có
    if not details["total_chapters_on_site"]:
        chapters = await get_chapters_from_story(self, story_url, story_title, site_key=site_key)
        details["total_chapters_on_site"] = len(chapters)

    # Cuối cùng nếu total_chapters_on_site có giá trị
    if not details.get("chapter_list") and details.get("total_chapters_on_site"):
        # Chỉ lấy chapter_list khi đã có tổng số chương
        chapters = await get_chapters_from_story(self, story_url, story_title, site_key=site_key)
        if chapters:
            details["chapter_list"] = chapters
    return details
def parse_chapters_from_soup(soup, base_url):
    chapters = []
    for li in soup.select('ul.flex.flex-col.w-full.divide-y > li'):
        a = li.select_one('a.flex.flex-row.items-center')
        if not a:
            continue
        chapter_url = urljoin(base_url, a['href'])
        title_tag = a.find('p', class_=['flex-1', 'font-[300]', 'line-clamp-2'])
        chapter_title = title_tag.get_text(strip=True) if title_tag else "Unknown"
        chapters.append({'url': chapter_url, 'title': chapter_title})
    return chapters

async def get_chapters_from_story(self, story_url, story_title, max_pages=None, total_chapters=None, site_key=None):
    chapters = []

    def build_chapter_list_url(base_url, page):
        if page == 1:
            return f"{base_url.rstrip('/')}/danh-sach-chuong"
        else:
            return f"{base_url.rstrip('/')}/danh-sach-chuong?p={page}"

    loop = asyncio.get_event_loop()
    first_url = build_chapter_list_url(story_url, 1)
    resp = await loop.run_in_executor(None, make_request, first_url)
    if not resp or not getattr(resp, 'text', None):
        return chapters
    soup = BeautifulSoup(resp.text, "html.parser")

    select = soup.select_one('select.w-full.py-1.outline-none.ring-0.border-none')
    if select:
        options = select.find_all('option')
        max_page = len(options)
    else:
        max_page = 1

    if max_pages:
        max_page = min(max_page, max_pages)

    for page in range(1, max_page + 1):
        url = build_chapter_list_url(story_url, page)
        resp = await loop.run_in_executor(None, make_request, url)
        if not resp or not getattr(resp, "text", None):
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        chapters += parse_chapters_from_soup(soup, self.BASE_URL)
    return chapters


async def get_story_chapter_content(
    self,
    chapter_url: str, chapter_title: str,
    site_key: str
) -> Optional[str]:
    logger.info(f"Đang tải nội dung chương '{chapter_title}': {chapter_url}")
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(None, make_request, chapter_url)
    if not response or not getattr(response, 'text', None):
        logger.error(f"Chương '{chapter_title}': Không nhận được phản hồi từ {chapter_url}")
        return None
    html = response.text
    content = extract_chapter_content(html, site_key)
    if not content:
        # Ghi debug nếu cần
        with open("debug_truyenyy_empty_chapter.html", "w", encoding="utf-8") as f:
            f.write(html)
        logger.warning(f"Nội dung chương '{chapter_title}' trống sau khi clean header.")
        return None
    return content


