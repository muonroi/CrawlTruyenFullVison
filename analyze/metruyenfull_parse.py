import re
import httpx
from bs4 import BeautifulSoup
from scraper import make_request
from config.config import PATTERN_FILE, get_random_headers, load_blacklist_patterns
from utils.logger import logger
from utils.html_parser import get_total_pages_metruyen_category, parse_stories_from_category_page
PATTERNS, CONTAINS_LIST = load_blacklist_patterns(PATTERN_FILE)

import asyncio

async def get_all_categories(self, home_url):
    resp = await make_request(home_url, self.SITE_KEY)
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
    stories = []
    page_num = 1
    while True:
        current_url = category_url
        if page_num > 1:
            current_url = category_url.rstrip('/') + f"/page/{page_num}"
        resp = await make_request(current_url, self.SITE_KEY)
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
    resp = await make_request(story_url, self.SITE_KEY)
    if not resp:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")# type: ignore

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
        chapters = await get_chapters_from_story(self, story_url, title, total_chapters_on_site=num_chapters, site_key=self.SITE_KEY)
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

async def get_all_stories_from_category_with_page_check(self, genre_name, genre_url, site_key, max_pages=None):
    resp = await make_request(genre_url, site_key)
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
        resp = await make_request(page_url, site_key)
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



import re
from bs4 import BeautifulSoup
import httpx


async def get_chapters_from_story(self, story_url, story_title, total_chapters_on_site=None, site_key=None):
    headers =  await get_random_headers(site_key)
    ajax_url = "https://metruyenfull.net/wp-admin/admin-ajax.php"

    async with httpx.AsyncClient(headers=headers) as client:
        # 1. Lấy trang đầu để lấy ID truyện
        print(f"Fetching initial page to get story ID: {story_url}")
        resp = await client.get(story_url)
        initial_soup = BeautifulSoup(resp.text, "html.parser")

        truyen_id_input = initial_soup.find("input", {"id": "truyen-id"}) or initial_soup.find("input",
                                                                                               {"id": "id_post"})
        if not (truyen_id_input and truyen_id_input.get("value")):# type: ignore
            raise Exception("Không lấy được id truyện!")
        truyen_id = truyen_id_input["value"] # type: ignore
        print(f"Found story ID: {truyen_id}")

        # 2. Lấy tổng số trang từ key 'pagination' trong AJAX response
        total_pages = 1
        print("Making a decisive AJAX call to find total pages...")
        try:
            payload = {"action": "tw_ajax", "type": "pagination", "id": truyen_id, "page": 2}
            resp = await client.post(ajax_url, data=payload)
            resp.raise_for_status()

            ajax_data = resp.json()

            # Lấy chuỗi HTML của phần phân trang từ key 'pagination'
            pagination_html = ajax_data.get('pagination', '')

            if pagination_html:
                pagination_soup = BeautifulSoup(pagination_html, 'html.parser')
                total_page_input = pagination_soup.find("input", {"name": "total-page"})
                if total_page_input and total_page_input.get("value", "").isdigit():# type: ignore
                    total_pages = int(total_page_input["value"])# type: ignore
                    print(f"✅ Success! Total pages found: {total_pages}")
                else:
                    print("⚠️ Could not find 'total-page' input in pagination HTML.")
            else:
                print("⚠️ AJAX response did not contain 'pagination' key.")

        except Exception as e:
            print(f"❌ Failed to determine total pages via AJAX. Assuming 1 page. Error: {e}")
            total_pages = 1

        # 3. Thu thập tất cả các chương
        all_chapters = []
        for page in range(1, total_pages + 1):
            print(f"Fetching chapters from page {page}/{total_pages}...")
            chapters_html = ''
            if page == 1:
                # Trang 1 lấy HTML từ lần tải đầu tiên
                chapters_html = str(initial_soup)
            else:
                # Các trang sau gọi AJAX và lấy HTML từ key 'list_chap'
                try:
                    payload = {"action": "tw_ajax", "type": "pagination", "id": truyen_id, "page": page}
                    resp = await client.post(ajax_url, data=payload)
                    ajax_data = resp.json()
                    chapters_html = ajax_data.get('list_chap', '')
                except Exception as e:
                    print(f"⚠️ Failed to fetch page {page}. Skipping. Error: {e}")
                    continue

            # Trích xuất chương từ HTML thu được
            if chapters_html:
                page_soup = BeautifulSoup(chapters_html, 'html.parser')
                for li_item in page_soup.select('ul.list-chapter li'):
                    a_tag = li_item.find('a')
                    if a_tag and a_tag.has_attr('href'):# type: ignore
                        all_chapters.append({"title": a_tag.get_text(strip=True), "url": a_tag['href']})# type: ignore

        # 4. Xử lý trùng và sắp xếp
        print("Deduplicating and sorting chapters...")
        uniq = []
        seen_urls = set()
        for ch in all_chapters:
            if ch['url'] not in seen_urls:
                uniq.append(ch)
                seen_urls.add(ch['url'])

        def sort_key(chapter):
            match = re.search(r"chương-(\d+)", chapter['title'], re.IGNORECASE) or re.search(r"(\d+)", chapter['title'])
            return int(match.group(1)) if match else float('inf')

        uniq.sort(key=sort_key)

        print(f"Finished. Found {len(uniq)} unique chapters.")
        return uniq