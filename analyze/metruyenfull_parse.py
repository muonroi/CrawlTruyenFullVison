import re
from bs4 import BeautifulSoup
from scraper import make_request
from config.config import PATTERN_FILE, load_blacklist_patterns
from utils.logger import logger
from utils.html_parser import get_total_pages_metruyen_category, parse_stories_from_category_page
PATTERNS, CONTAINS_LIST = load_blacklist_patterns(PATTERN_FILE)

import asyncio

async def get_all_categories(self, home_url):
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, home_url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    categories = []
    for ul_menu in soup.select('.dropdown-menu.multi-column ul.dropdown-menu'):
        for li_item in ul_menu.find_all('li'):
            a_tag = li_item.find('a') #type: ignore
            if a_tag and a_tag.has_attr('href'):#type: ignore
                categories.append({
                    'name': a_tag.get_text(strip=True),#type: ignore
                    'url': a_tag['href']#type: ignore
                })
    return categories


async def get_stories_from_category(self, category_url):
    import asyncio
    loop = asyncio.get_event_loop()
    stories = []
    page_num = 1
    while True:
        current_url = category_url
        if page_num > 1:
            current_url = category_url.rstrip('/') + f"/page/{page_num}"
        resp = await loop.run_in_executor(None, make_request, current_url)
        if not resp:
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        found_stories_on_page = False
        for row in soup.select('div.row[itemtype="https://schema.org/Book"]'):
            a_tag = row.select_one('.truyen-title a')
            if a_tag and a_tag.has_attr('href'):
                title = a_tag.get_text(strip=True)
                href = a_tag['href']
                stories.append({
                    "title": title,
                    "url": href
                })
                found_stories_on_page = True
        if not found_stories_on_page:
            break
        pagination = soup.select_one('ul.pagination')
        if not pagination or not pagination.find("a", string=str(page_num + 1)):
            break
        page_num += 1
    return stories

async def get_story_metadata(self, story_url):
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, story_url)
    if not resp:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")

    # Title
    title_tag = soup.select_one('h1.title')
    title = title_tag.get_text(strip=True) if title_tag else ''

    # Author
    author = None
    author_tag = soup.select_one('.info-holder a[itemprop="author"]')
    if author_tag:
        author = author_tag.get('title') or author_tag.get_text(strip=True)

    # Description
    desc_tag = soup.select_one('div.desc-text.desc-text-full')
    description = desc_tag.get_text(separator="\n", strip=True) if desc_tag else None

    # Categories
    categories = []
    for a_tag in soup.select('.info-holder a[itemprop="genre"]'):
        if a_tag.has_attr('href'):
            categories.append({'name': a_tag.get_text(strip=True), 'url': a_tag['href']})

    # Số chương
    num_chapters = 0
    for label in soup.select('.info-holder .label-success'):
        txt = label.get_text()
        if 'chương' in txt.lower():
            match = re.search(r'(\d+)', txt)
            if match:
                num_chapters = int(match.group(1))
                logger.info(f"Lấy được số chương từ label: {num_chapters} chương")
    # Fallback
    if not num_chapters or num_chapters < 100:
        logger.warning(f"Không lấy được tổng số chương chuẩn, sẽ crawl paginate đếm số chương cho {story_url}")
        chapters = await get_chapters_from_story(self, story_url)
        num_chapters = len(chapters)

    # Cover
    image_url = None
    img_tag = soup.select_one('.info-holder img[itemprop="image"]')
    if img_tag and img_tag.has_attr('src'):
        image_url = img_tag.get('src')

    return {
        "title": title,
        "author": author,
        "description": description,
        "categories": categories,
        "total_chapters_on_site": num_chapters,
        "cover": image_url,
        "url": story_url
    }

async def get_all_stories_from_category_with_page_check(self, genre_name, genre_url, max_pages=None):
    import asyncio
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, genre_url)
    if not resp:
        return [], 0, 0
    html = resp.text
    total_pages = get_total_pages_metruyen_category(html)
    logger.info(f"Category {total_pages} có trang.")
    if max_pages:
        total_pages = min(total_pages, max_pages)
    all_stories = []
    pages_crawled = 0
    seen_urls = set()
    for page in range(1, total_pages+1):
        page_url = genre_url if page == 1 else f"{genre_url.rstrip('/')}/page/{page}"
        resp = await loop.run_in_executor(None, make_request, page_url)
        if not resp:
            break
        stories_on_page = parse_stories_from_category_page(resp.text)
        if not stories_on_page:
            break
        for s in stories_on_page:
            if s['url'] not in seen_urls:
                all_stories.append(s)
                seen_urls.add(s['url'])
        pages_crawled += 1
    logger.info(f"Category {genre_name}: crawl được {len(all_stories)} truyện/{pages_crawled}/{total_pages} trang.")
    return all_stories, total_pages, pages_crawled


async def get_chapters_from_story(self, story_url):
    loop = asyncio.get_event_loop()
    chapters = []
    page_num = 1
    seen = set()
    while True:
        current_url = story_url
        if page_num > 1:
            current_url = story_url.rstrip('/') + f"/page/{page_num}"
        resp = await loop.run_in_executor(None, make_request, current_url)
        if not resp:
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        found_chapters_on_page = False
        for li_item in soup.select('ul.list-chapter li, ul.l-chapters li'):
            a_tag = li_item.find('a')
            if a_tag and a_tag.has_attr('href') and 'chuong-' in a_tag['href']: #type: ignore
                chapter_title = a_tag.get_text(strip=True)
                href = a_tag['href']#type: ignore
                if href not in seen:
                    chapters.append({
                        "title": chapter_title,
                        "url": href
                    })
                    seen.add(href)
                found_chapters_on_page = True
        if not found_chapters_on_page:
            break
        pagination = soup.select_one('ul.pagination')
        if not pagination or not pagination.find("a", string=str(page_num + 1)):
            break
        page_num += 1
    chapters.sort(key=lambda ch: float(re.search(r"(\d+)", ch['title']).group(1)) if re.search(r"(\d+)", ch['title']) else float('inf'))#type: ignore
    return chapters

