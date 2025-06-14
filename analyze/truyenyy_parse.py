from datetime import datetime
from asyncio.log import logger
import re
from typing import Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from scraper import make_request
from utils.chapter_utils import get_max_page_by_playwright
from utils.html_parser import clean_header, extract_chapter_content, get_total_pages_category

def build_category_list_url(genre_url, page=1):
    base = genre_url.rstrip('/')
    if page > 1:
        return f"{base}?p={page}"
    else:
        return f"{base}"

async def get_all_stories_from_genre_with_page_check(self, genre_name, genre_url, site_key, max_pages=None):
    """Crawl toàn bộ truyện trong 1 category với kiểm soát lặp."""
    from urllib.parse import urljoin

    first_page_url = build_category_list_url(genre_url, page=1)
    resp = await make_request(first_page_url, self.SITE_KEY)
    if not resp:
        return [], 0, 0
    html = resp.text
    total_pages = get_total_pages_category(html)
    if max_pages:
        total_pages = min(total_pages, max_pages)

    all_stories = []
    pages_crawled = 0
    seen_urls = set()
    visited_pages = set()
    last_first_url = None
    repeat_count = 0

    for page in range(1, total_pages + 1):
        page_url = build_category_list_url(genre_url, page)
        if page_url in visited_pages:
            logger.error("[YY][CATEGORY] Lặp trang, dừng crawl")
            break
        visited_pages.add(page_url)

        resp = await make_request(page_url, self.SITE_KEY)
        if not resp or not getattr(resp, 'text', None):
            logger.warning(f"[YY][CATEGORY] Không nhận được dữ liệu {page_url}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        ul = soup.select_one("ul.flex.flex-col")
        if not ul:
            logger.warning(f"[YY][CATEGORY] Không tìm thấy list ở {page_url}")
            break
        for li in ul.find_all("li", recursive=False):
            # 1. Lấy url + cover
            a_img = li.select_one("a[href*='/truyen/']") #type: ignore
            cover = None
            url = None
            if a_img:
                url = urljoin(self.BASE_URL, a_img['href'])#type: ignore
                img_tag = a_img.find("img")
                if img_tag and img_tag.has_attr('src'):#type: ignore
                    cover = img_tag['src']#type: ignore

            # 2. Lấy title
            h3 = li.select_one("h3.font-title")#type: ignore
            title = h3.get_text(strip=True) if h3 else None

            # 3. Lấy author
            author_p = li.select_one("p.text-xs.font-thin")#type: ignore
            author = author_p.get_text(strip=True) if author_p else None

            # 4. Lấy số chương
            chapter_p = li.select_one("div.rounded.border > p.text-xs")#type: ignore
            total_chapters = None
            if chapter_p:
                match = re.search(r"(\d+)", chapter_p.get_text())
                if match:
                    total_chapters = int(match.group(1))

            # Check đủ url + title mới append
            if url and title:
                if url not in seen_urls:
                    all_stories.append({
                        "title": title,
                        "url": url,
                        "cover": cover,
                        "author": author,
                        "total_chapters": total_chapters,
                    })
                    seen_urls.add(url)
                else:
                    logger.warning(f"[YY][CATEGORY] Bỏ qua truyện trùng {url}")

        if not all_stories:
            logger.warning(f"[YY][CATEGORY] Trang {page_url} không có truyện")
            break

        first_li = ul.find('li', recursive=False)
        first_url = None
        if first_li:
            first_a = first_li.select_one("a[href*='/truyen/']") # type: ignore
            if first_a and first_a.has_attr('href'):
                first_url = urljoin(self.BASE_URL, first_a['href']) # type: ignore
        if last_first_url == first_url:
            repeat_count += 1
        else:
            repeat_count = 0
        last_first_url = first_url

        if repeat_count >= 2:
            logger.error("[YY][CATEGORY] Phát hiện lặp trang liên tiếp, dừng")
            break

        pages_crawled += 1

    return all_stories, total_pages, pages_crawled


async def get_all_genres(self, homepage_url):
    resp = await make_request(homepage_url, self.SITE_KEY)
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
        href = (a.get('href') or '').rstrip('/') + '/danh-sach'#type: ignore
        full_url = urljoin(self.BASE_URL, href)
        genres.append({"name": name, "url": full_url})
    return genres

async def get_stories_from_genre_page(self, genre_url, page=1):
    base = genre_url.rstrip('/')
    url = f"{base}?p={page}" if page > 1 else base
    resp = await make_request(url, self.SITE_KEY)
    if not resp or not getattr(resp, 'text', None):
        logger.error(f"Không lấy được trang {url}")
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    ul = soup.select_one("ul.flex.flex-col")
    if not ul:
        logger.error(f"[YY][CATEGORY] Không tìm thấy ul.flex.flex-col ở {url}")
        with open("debug_yy_cat.html", "w", encoding="utf-8") as f:
            f.write(soup.prettify()) #type: ignore
        return []
    stories = []
    for li in ul.find_all("li", recursive=False):
        a_tag = li.select_one("a[href*='/truyen/']")#type: ignore
        h3 = li.select_one("h3.font-title")#type: ignore
        title = h3.get_text(strip=True) if h3 else ""
        if not a_tag or not title:
            logger.warning(f"[YY][CATEGORY] Không lấy được url/title cho 1 item ở {url}")
            continue
        detail_url = urljoin(self.BASE_URL, a_tag['href'])#type: ignore
        # Lấy tên truyện
        h3 = a_tag.select_one("h3.font-title")
        title = h3.get_text(strip=True) if h3 else ""
        # Lấy tên tác giả
        author_p = li.select_one("p.text-xs.font-thin")#type: ignore
        author = author_p.get_text(strip=True) if author_p else None
        # Lấy số chương (nếu muốn)
        chapter_p = li.select_one("div.rounded.border > p.text-xs")#type: ignore
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
        stories = await get_stories_from_genre_page(self,genre_url, page)
        if not stories:
            break
        all_stories.extend(stories)
        page += 1
    return all_stories

async def get_story_details(self, story_url, story_title, site_key):
    from datetime import datetime
    resp = await make_request(story_url, site_key)
    if not resp or not getattr(resp, 'text', None):
        logger.error(f"Không lấy được chi tiết truyện {story_url}")
        return {}
    soup = BeautifulSoup(resp.text, "html.parser")
    details = {
        "title": "",
        "author": "",
        "cover": "",
        "description": "",
        "categories": [],
        "status": "",
        "source": story_url,
        "rating_value": None,
        "rating_count": None,
        "total_chapters_on_site": None,
    }
    # --- Lấy title ---
    title_tag = soup.select_one("h1.font-title")
    details["title"] = title_tag.get_text(strip=True) if title_tag else story_title
    # --- Lấy cover ---
    cover_img = soup.select_one("div.rounded-md.w-\\[160px\\].h-\\[240px\\].overflow-hidden img")
    if cover_img and cover_img.has_attr("src"):
        details["cover"] = cover_img["src"]
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
    # Description
    desc_tag = soup.select_one("p.prose")
    if desc_tag:
        details["description"] = desc_tag.get_text(separator="\n", strip=True)
        # Categories
    categories = []
    cat_div = soup.select_one("div.flex.flex-wrap.gap-2.text-\\[12px\\].max-w-\\[640px\\]")
    if cat_div:
        for a in cat_div.find_all("a"):
            href = a.get("href")#type: ignore
            name = a.get_text(strip=True)
            if href and name:
                categories.append({"name": name, "url": urljoin(self.BASE_URL, href)})#type: ignore
    details["categories"] = categories
       # trạng thái, số chươngMore actions
    for li in soup.select("div.lg\\:hidden ul.mt-2.text-start.flex-col > li"):
        text = li.get_text(strip=True)
        text_lower = text.lower()
        if "trạng thái" in text_lower or "status" in text_lower:
            # Tách phần value sau dấu : hoặc sau từ "trạng thái"
            match = re.search(r"(?:trạng thái|status)\s*[:：]?\s*(.*)", text, re.IGNORECASE)
            if match:
                details["status"] = match.group(1).strip()
            else:
                # fallback: lấy phần sau dấu :
                parts = text.split(":", 1)
                if len(parts) > 1:
                    details["status"] = parts[1].strip()
                else:
                    details["status"] = text  # fallback giữ nguyên
        elif "chương" in text_lower or "chapter" in text_lower:
            match = re.search(r"(\d+)", text)
            if match:
                details["total_chapters_on_site"] = int(match.group(1))
    # --- Fallback: Canonical ---
    canonical = soup.find("link", rel="canonical")
    if canonical and canonical.get("href"): # type: ignore
        details["source"] = canonical["href"] # type: ignore

  
    details["sources"] = [{
        "site": urlparse(details["source"]).netloc, # type: ignore
        "url": details["source"],
        "total_chapters": details["total_chapters_on_site"],
        "last_update": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }]
    # Chuẩn hóa trả về
    for k in list(details.keys()):
        if details[k] is None:
            details[k] = "" if not isinstance(details[k], list) else []

    if not details["categories"]:
        details["categories"] = [{"name": "Unknown", "url": ""}]
    return details



def parse_chapters_from_soup(soup, base_url):
    from urllib.parse import urljoin
    chapters = []
    nodes = soup.select('div.flex.font-light')
    if not nodes:
        nodes = soup.select('ul.flex.flex-col li')
    for div in nodes:
        # Tìm các thẻ <a> chứa link chương
        chapter_links = div.find_all('a', href=True)
        if not chapter_links:
            continue

        href = None
        title = None

        for a in chapter_links:
            link = a['href']
            if not href:
                href = urljoin(base_url, link)
            span = a.find('span') or a.find('p')
            if span and span.get_text(strip=True):
                title = span.get_text(strip=True)
        if not href:
            continue

        # Fallback nếu title bị thiếu
        if not title or title.strip() == '':
            # Thử lấy text từ phần Chương Số
            alt_link = div.find('a', href=True)
            if alt_link:
                alt_text = alt_link.get_text(strip=True)
                if alt_text:
                    title = alt_text
            if not title:
                title = "Unknown"

        chapters.append({
            'url': href,
            'title': title
        })
    return chapters

async def get_chapters_from_story(self, story_url, story_title, max_pages=None, total_chapters=None, site_key=None):
    chapters = []

    def build_chapter_list_url(base_url, page):
        if page == 1:
            return f"{base_url.rstrip('/')}/danh-sach-chuong"
        else:
            return f"{base_url.rstrip('/')}/danh-sach-chuong?p={page}"

    first_url = build_chapter_list_url(story_url, 1)
    resp = await make_request(first_url, site_key)
    if not resp or not getattr(resp, 'text', None):
        return chapters
    soup = BeautifulSoup(resp.text, "html.parser")

    max_page = (total_chapters // 20 + 1) if total_chapters else 1

    logger.info(f"[YY][CHAPTERS] Phát hiện {max_page} page chapter list cho {story_url}")

    for page in range(1, max_page + 1):
        url = build_chapter_list_url(story_url, page)
        resp = await make_request(url, site_key)
        if not resp or not getattr(resp, "text", None):
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        chapters += parse_chapters_from_soup(soup, self.BASE_URL)

    uniq, seen = [], set()
    for ch in chapters:
        if ch['url'] not in seen:
            uniq.append(ch)
            seen.add(ch['url'])

    uniq.sort(
        key=lambda c: float(re.search(r"(\d+)", c['title']).group(1)) if re.search(r"(\d+)", c['title']) else float('inf') # type: ignore
    )

    if total_chapters and abs(len(uniq) - total_chapters) > 5:
        logger.warning(
            f"[YY][CHAPTERS] Meta {total_chapters} != actual {len(uniq)} cho '{story_title}'"
        )

    return uniq


async def get_story_chapter_content(
    self,
    chapter_url: str, chapter_title: str,
    site_key: str
) -> Optional[str]:
    logger.info(f"Đang tải nội dung chương '{chapter_title}': {chapter_url}")
    response = await make_request(chapter_url, site_key)
    if not response or not getattr(response, 'text', None):
        logger.error(f"Chương '{chapter_title}': Không nhận được phản hồi từ {chapter_url}")
        return None
    html = response.text
    content = extract_chapter_content(html,site_key,chapter_title)
    if not content:
        # Ghi debug nếu cần
        with open("debug_truyenyy_empty_chapter.html", "w", encoding="utf-8") as f:
            f.write(html)
        logger.warning(f"Nội dung chương '{chapter_title}' trống sau khi clean header.")
        return None
    return content


