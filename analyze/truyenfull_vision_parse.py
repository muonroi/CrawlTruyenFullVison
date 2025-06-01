import datetime
import re
import asyncio
from typing import List, Tuple, Optional, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

import httpx

from utils.batch_utils import smart_delay
from utils.html_parser import extract_chapter_content, get_total_pages_category
from utils.logger import logger
from scraper import make_request
from config.config import (
    BASE_URLS,
    MAX_STORIES_PER_GENRE_PAGE,
    get_random_headers,
)
import re
import asyncio
from typing import Dict, Any, List
from urllib.parse import urljoin
from bs4 import BeautifulSoup

async def get_story_details(story_url: str, story_title_for_log: str) -> Dict[str, Any]:
    logger.info(f"Truyện '{story_title_for_log}': Đang lấy thông tin chi tiết từ {story_url}")
    details = {
        "title": None,
        "author": None,
        "cover": None,
        "description": None,
        "categories": [],
        "status": None,
        "source": None,
        "rating_value": None,
        "rating_count": None,
        "total_chapters_on_site": None
    }
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(None, make_request, story_url)
    if not response or not getattr(response, 'text', None):
        logger.error(f"Truyện '{story_title_for_log}': Không nhận được phản hồi hoặc nội dung rỗng từ {story_url}")
        return details
    soup = BeautifulSoup(response.text, "html.parser")

    # 1. Title
    title_tag = (
        soup.select_one(".col-info-desc h3.title[itemprop='name']")
        or soup.select_one("h1.title[itemprop='name']")
        or soup.select_one("h1.title")
        or soup.select_one("h3.title")
    )
    details["title"] = title_tag.get_text(strip=True) if title_tag else story_title_for_log

    # 2. Author
    author_tag = (
        soup.select_one('.info-holder a[itemprop="author"]')
        or soup.select_one("a[itemprop='author']")
    )
    details["author"] = author_tag.get_text(strip=True) if author_tag else None

    # 3. Cover (img)
    img_tag = soup.select_one('.info-holder img[itemprop="image"]') or soup.select_one("img[itemprop='image']")
    details["cover"] = img_tag['src'] if img_tag and img_tag.has_attr('src') else None

    # 4. Description
    desc_div = (
        soup.find("div", class_="desc-text", itemprop="description")
        or soup.find("div", class_="desc-text")
        or soup.select_one('.desc-text[itemprop="description"]')
    )
    if desc_div:
        if (more := desc_div.find("div", class_="showmore")): more.decompose()#type: ignore
        for br in desc_div.find_all("br"): br.replace_with("\n")#type: ignore
        text = desc_div.get_text(separator="\n", strip=True)
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        details["description"] = "\n".join(lines)
    else:
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):#type: ignore
            details["description"] = meta["content"].strip()#type: ignore

    # 5. Categories
    categories = []
    for a_tag in soup.select('.info-holder a[itemprop="genre"]'):
        if a_tag.has_attr('href'):
            categories.append({'name': a_tag.get_text(strip=True), 'url': a_tag['href']})
    # Fallback cho một số site có thể nằm ở ngoài info-holder
    if not categories:
        for a_tag in soup.select('a[itemprop="genre"]'):
            if a_tag.has_attr('href'):
                categories.append({'name': a_tag.get_text(strip=True), 'url': a_tag['href']})
    details["categories"] = categories

    # 6. Status
    # Trạng thái có thể là text hoặc trong span (ví dụ label label-success)
    status = None
    for el in soup.select('.info-holder, .info'):
        for tag in el.find_all(["span", "label", "div", "h4"], recursive=True):
            txt = tag.get_text(strip=True).lower()
            if "trạng thái" in txt or "status" in txt:
                # Có thể lấy text phía sau, hoặc trong strong, span kế tiếp
                nxt = tag.find_next(string=True)
                if nxt:
                    status = nxt.strip()#type: ignore
                if not status:
                    strong_tag = tag.find("strong")#type: ignore
                    status = strong_tag.get_text(strip=True) if strong_tag else txt#type: ignore
                break
        if status: break
    details["status"] = status

    # 7. Rating
    rate = soup.find("div", class_="rate-holder")
    if rate and rate.get('data-score'): #type: ignore
        details['rating_value'] = rate['data-score']#type: ignore
    rc = soup.select_one("div.small[itemprop='aggregateRating'] span[itemprop='ratingCount']")
    if rc:
        details['rating_count'] = rc.get_text(strip=True)

    # 8. Total chapters
    num_chapters = None
    # (giữ lại code lấy label cũ nếu có)
    # Tìm trong label, span, hoặc input
    for label in soup.select('.info-holder .label-success, .info .label-success, span.label.label-success'):
        txt = label.get_text()
        if 'chương' in txt.lower():
            match = re.search(r'(\d+)\s*[Cc]hương', txt)
            if match:
                num_chapters = int(match.group(1))
                break

    # Nếu không có hoặc < 50 thì paginate để đếm toàn bộ chương
    if not num_chapters or num_chapters < 100:
        logger.warning(f"Không tìm thấy tổng số chương hoặc số nhỏ, paginate để đếm chính xác số chương!")
        from adapters.truyenfull_adapter import TruyenFullAdapter
        # Dùng hàm crawl chapter list (nó sẽ tự động phân trang), LƯU Ý: truyền max_pages=None để lấy hết!
        chapters = await get_chapters_from_story(story_url,story_title_for_log,site_key="truyenfull")
        num_chapters = len(chapters)

    details["total_chapters_on_site"] = num_chapters

    # 9. Sources (bổ sung luôn field cho metadata autofix)
    current_site = None
    for k, v in BASE_URLS.items():
        if v.rstrip('/') in story_url:
            current_site = k
            break
    if not current_site:
        current_site = story_url.split('/')[2]  # fallback lấy domain nếu chưa match

    details["sources"] = [{
        "site": current_site,
        "url": story_url,
        "total_chapters": details.get("total_chapters_on_site", 0),
        "last_update": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }]

    # Fix None -> "" (chuẩn hóa trả về cho autofix dễ merge)
    for k in list(details.keys()):
        if details[k] is None:
            details[k] = "" if not isinstance(details[k], list) else []

    return details


async def get_all_genres(homepage_url: str) -> List[Dict[str, str]]:
    logger.info(f"Đang lấy danh sách thể loại từ: {homepage_url}")
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(None, make_request, homepage_url)
    if not response or not getattr(response, 'text', None):
        logger.error(f"Không nhận được phản hồi từ {homepage_url}")
        return []
    soup = BeautifulSoup(response.text, "html.parser")
    genres: List[Dict[str, str]] = []
    nav_menu = soup.find("ul", class_="control nav navbar-nav") or soup.find("div", class_="navbar-collapse")
    dropdown = None
    for a in nav_menu.find_all("a", class_=re.compile(r"dropdown-toggle|nav-link")):#type: ignore
        if "thể loại" in a.get_text(strip=True).lower():
            parent = a.find_parent("li", class_="dropdown")
            dropdown = parent.find("div", class_=re.compile(r"dropdown-menu")) if parent else a.find_next_sibling("div", class_=re.compile(r"dropdown-menu"))#type: ignore
            break
    if dropdown:
        for link in dropdown.find_all("a", href=True):#type: ignore
            name, href = link.get_text(strip=True), link['href']#type: ignore
            if name and href and any(k in href for k in ["/the-loai/", "/genres/"]):
                genres.append({"name": name, "url": urljoin(homepage_url, href)})#type: ignore
    unique, seen = [], set()
    for g in genres:
        if g['url'] not in seen:
            unique.append(g); seen.add(g['url'])
    logger.info(f"Tìm thấy {len(unique)} thể loại.")
    return unique

async def get_stories_from_genre_page(genre_page_url: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    logger.info(f"Đang lấy truyện từ trang: {genre_page_url}")
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(None, make_request, genre_page_url)
    if not response or not getattr(response, 'text', None):
        logger.error(f"Không nhận được phản hồi từ {genre_page_url}")
        return [], None
    soup = BeautifulSoup(response.text, "html.parser")
    stories: List[Dict[str, Any]] = []

    # Parse từng truyện (mỗi .row trong .list.list-truyen.col-xs-12)
    for row in soup.select(".list.list-truyen.col-xs-12 > .row[itemscope][itemtype='https://schema.org/Book']"):
        # Lấy title và url
        a = row.select_one("h3.truyen-title a[href]")
        title = a.get_text(strip=True) if a else None
        url = a['href'] if a else None
        # Lấy author
        author = row.select_one("span.author")
        author_name = author.get_text(strip=True) if author else None
        # Lấy số chương (nếu muốn)
        chapter_a = row.select_one("div.col-xs-2.text-info a[href]")
        chapter_text = chapter_a.get_text(strip=True) if chapter_a else None

        if title and url:
            stories.append({
                "title": title,
                "url": url,
                "author": author_name,
                "chapter_info": chapter_text,
            })

    # Parse next page
    next_page_url = None
    # Lấy nút next (phải là trang tiếp)
    pag_a = soup.select_one(".pagination.pagination-sm li a[title*='Trang tiếp']")
    if pag_a and pag_a.has_attr("href"):
        next_page_url = pag_a['href']
        # Nếu link next là link tuyệt đối thì giữ nguyên, nếu là link tương đối thì join lại
        if not next_page_url.startswith("http"):#type: ignore
            next_page_url = urljoin(genre_page_url, next_page_url)#type: ignore

    return stories, next_page_url#type: ignore


async def get_all_stories_from_genre(
    genre_name: str, genre_url: str,
    max_pages_to_crawl: Optional[int] = MAX_STORIES_PER_GENRE_PAGE
) -> List[Dict[str, Any]]:
    all_stories, visited, page = [], set(), 0
    current_url = genre_url
    while current_url and current_url not in visited and (max_pages_to_crawl is None or page < max_pages_to_crawl):
        visited.add(current_url); page += 1
        logger.info(f"Trang {page} của '{genre_name}': {current_url}")
        sts, nxt = await get_stories_from_genre_page(current_url)
        for s in sts:
            if s['url'] not in {x['url'] for x in all_stories}:
                all_stories.append(s)
        if nxt and urlparse(nxt)._replace(fragment='') != urlparse(current_url)._replace(fragment=''):
            current_url = nxt
        else:
            break
    logger.info(f"Tổng có {len(all_stories)} truyện.")
    return all_stories

def get_input_value(soup, input_id, default=None):
    tag = soup.find("input", {"id": input_id})
    return tag["value"] if tag and tag.has_attr("value") else default

async def get_chapters_from_story(
    story_url: str, story_title: str,
    total_chapters_on_site: Optional[int] = None,  # Số chương thật sự trên site (từ metadata)
    site_key: str = "truyenfull"
) -> List[Dict[str, str]]:
    logger.info(f"Truyện '{story_title}': Lấy chương từ {story_url}")
    chapters: List[Dict[str, str]] = []

    headers = await get_random_headers(site_key)
    async with httpx.AsyncClient(headers=headers, timeout=20) as client:
        # 1. Request trang đầu để lấy truyen-id, truyen-ascii, total-page
        resp = await client.get(story_url)
        soup = BeautifulSoup(resp.text, "html.parser")
        def get_input(soup, key):
            tag = soup.find("input", {"id": key})
            return tag["value"] if tag and tag.has_attr("value") else None

        truyen_id = get_input(soup, "truyen-id")
        truyen_ascii = get_input(soup, "truyen-ascii")
        total_page = get_input(soup, "total-page")
        if not (truyen_id and truyen_ascii and total_page):
            logger.error("Không lấy được đủ thông tin (truyen-id, truyen-ascii, total-page)")
            return []

        truyen_name = story_title  # Bạn có thể dùng <title> nếu muốn, ở đây giữ nguyên title truyền vào
        total_page = int(total_page)

        # 2. Lặp từng page để lấy danh sách chương qua AJAX
        for page in range(1, total_page + 1):
            params = {
                "type": "list_chapter",
                "tid": truyen_id,
                "tascii": truyen_ascii,
                "tname": truyen_name,
                "page": page,
                "totalp": total_page
            }
            ajax_url = "https://truyenfull.vision/ajax.php"
            try:
                resp = await client.get(ajax_url, params=params)
                if resp.status_code != 200:
                    logger.warning(f"AJAX page {page}/{total_page} fail, status {resp.status_code}")
                    continue
                data = resp.json()
            except Exception as ex:
                logger.error(f"Lỗi khi gọi AJAX page {page}: {ex}")
                continue
            chap_html = data.get("chap_list")
            if not chap_html:
                logger.warning(f"Không có chap_list trong AJAX page {page}")
                continue
            soup_chap = BeautifulSoup(chap_html, "html.parser")
            for a in soup_chap.select("ul.list-chapter li a"):
                chapters.append({
                    "title": a.get("title") or a.get_text(strip=True),
                    "url": a["href"]
                })
            # Nếu đã lấy đủ số chương metadata thì dừng (tránh request thừa)
            if total_chapters_on_site and len(chapters) >= total_chapters_on_site:
                logger.info(f"Đã lấy đủ {total_chapters_on_site} chương, dừng crawl trang chương.")
                break
            await smart_delay()

    # 3. Lọc trùng và sort lại theo số chương
    uniq, seen = [], set()
    for ch in chapters:
        if ch['url'] not in seen:
            uniq.append(ch)
            seen.add(ch['url'])
    uniq.sort(key=lambda ch: float(re.search(r"(\d+)", ch['title']).group(1)) if re.search(r"(\d+)", ch['title']) else float('inf'))

    # Cảnh báo nếu lấy được ít hơn số chương metadata
    if total_chapters_on_site and len(uniq) < total_chapters_on_site:
        logger.warning(f"CHÚ Ý: Số chương lấy được ({len(uniq)}) < metadata ({total_chapters_on_site}) cho truyện '{story_title}'")

    logger.info(f"Tìm thấy {len(uniq)} chương.")
    return uniq

async def get_story_chapter_content(
    chapter_url: str, chapter_title: str,
    site_key
) -> Optional[str]:
    logger.info(f"Đang tải nội dung chương '{chapter_title}': {chapter_url}")
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(None, make_request, chapter_url)
    if not response or not getattr(response, 'text', None):
        logger.error(f"Chương '{chapter_title}': Không nhận được phản hồi từ {chapter_url}")
        return None
    html = response.text
    content = extract_chapter_content(html,site_key)
    if not content:
        logger.warning(f"Nội dung chương '{chapter_title}' trống sau khi clean header.")
        return None
    return content or None

async def get_all_stories_from_genre_with_page_check(genre_name, genre_url, max_pages=None):
    """
    Crawl tất cả truyện trong 1 thể loại (category) của truyenfull.vision
    Không phụ thuộc next_page_url, crawl từng page dựa vào pattern url và total_pages lấy từ phân trang.
    Dùng make_request (đồng bộ) để đồng bộ với các hàm cũ.
    """
    resp = make_request(genre_url)
    if not resp or not getattr(resp, 'text', None):
        logger.error(f"Không nhận được phản hồi từ {genre_url}")
        return [], 0, 0
    html = resp.text
    total_pages = get_total_pages_category(html)
    logger.info(f"Category {total_pages} có trang.")
    if max_pages:
        total_pages = min(total_pages, max_pages)

    all_stories = []
    seen_urls = set()
    pages_crawled = 0

    for page in range(1, total_pages + 1):
        page_url = genre_url if page == 1 else f"{genre_url.rstrip('/')}/trang-{page}/"
        logger.info(f"[Crawl] Page {page}: {page_url}")
        resp = make_request(page_url)
        if not resp or not getattr(resp, 'text', None):
            logger.warning(f"Không nhận được phản hồi từ {page_url}")
            break
        html = resp.text
        # Parse stories trên page này
        stories, _ = await get_stories_from_genre_page(page_url)
        if not stories:
            logger.warning(f"Không tìm thấy truyện nào ở {page_url}, dừng crawl category.")
            break
        for s in stories:
            if s['url'] not in seen_urls:
                all_stories.append(s)
                seen_urls.add(s['url'])
        pages_crawled += 1

    logger.info(f"Category {genre_name}: crawl được {len(all_stories)} truyện/{pages_crawled}/{total_pages} trang.")
    return all_stories, total_pages, pages_crawled
