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
    MAX_CHAPTER_PAGES_TO_CRAWL,
    get_random_headers,
)
import re
import asyncio
from typing import Dict, Any, List
from urllib.parse import urljoin
from bs4 import BeautifulSoup

async def get_story_details(self,story_url: str, story_title_for_log: str) -> Dict[str, Any]:
    logger.info(f"Truyen '{story_title_for_log}': Dang lay thong tin chi tiet tu {story_url}")
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
    response = await make_request(story_url, self.SITE_KEY)
    if not response or not getattr(response, 'text', None):
        logger.error(f"Truyen '{story_title_for_log}': Khong nhan duoc phan hoi hoac noi dung rong tu {story_url}")
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
    # Fallback cho mot so site co the nam o ngoai info-holder
    if not categories:
        for a_tag in soup.select('a[itemprop="genre"]'):
            if a_tag.has_attr('href'):
                categories.append({'name': a_tag.get_text(strip=True), 'url': a_tag['href']})
    details["categories"] = categories

    # 6. Status
    # Trang thai co the la text hoac trong span (vi du label label-success)
    status = None
    for el in soup.select('.info-holder, .info'):
        for tag in el.find_all(["span", "label", "div", "h4"], recursive=True):
            txt = tag.get_text(strip=True).lower()
            if "trang thai" in txt or "status" in txt:
                # Co the lay text phia sau, hoac trong strong, span ke tiep
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
    # (giu lai code lay label cu neu co)
    # Tim trong label, span, hoac input
    for label in soup.select('.info-holder .label-success, .info .label-success, span.label.label-success'):
        txt = label.get_text()
        if 'chuong' in txt.lower():
            match = re.search(r'(\d+)\s*[Cc]huong', txt)
            if match:
                num_chapters = int(match.group(1))
                break

    # Neu khong co hoac < 50 thi paginate de dem toan bo chuong
    if not num_chapters or num_chapters < 100:
        logger.warning(f"Khong tim thay tong so chuong hoac so nho, paginate de dem chinh xac so chuong!")
        from adapters.truyenfull_adapter import TruyenFullAdapter
        # Dung ham crawl chapter list (no se tu dong phan trang), LUU Y: truyen max_pages=None de lay het!
        chapters = await get_chapters_from_story(story_url,story_title_for_log,site_key="truyenfull")
        num_chapters = len(chapters) 
 
    details["total_chapters_on_site"] = num_chapters

    # 9. Sources (bo sung luon field cho metadata autofix)
    current_site = None
    for k, v in BASE_URLS.items():
        if v.rstrip('/') in story_url:
            current_site = k
            break
    if not current_site:
        current_site = story_url.split('/')[2]  # fallback lay domain neu chua match

    details["sources"] = [{
        "site": current_site,
        "url": story_url,
        "total_chapters": details.get("total_chapters_on_site", 0),
        "last_update": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }]

    # Fix None -> "" (chuan hoa tra ve cho autofix de merge)
    for k in list(details.keys()):
        if details[k] is None:
            details[k] = "" if not isinstance(details[k], list) else []

    return details


async def get_all_genres(self,homepage_url: str) -> List[Dict[str, str]]:
    logger.info(f"Dang lay danh sach the loai tu: {homepage_url}")
    response = await make_request(homepage_url, self.SITE_KEY)
    if not response or not getattr(response, 'text', None):
        logger.error(f"Khong nhan duoc phan hoi tu {homepage_url}")
        return []
    soup = BeautifulSoup(response.text, "html.parser")
    genres: List[Dict[str, str]] = []
    nav_menu = soup.find("ul", class_="control nav navbar-nav") or soup.find("div", class_="navbar-collapse")
    dropdown = None
    for a in nav_menu.find_all("a", class_=re.compile(r"dropdown-toggle|nav-link")):#type: ignore
        if "the loai" in a.get_text(strip=True).lower():
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
    logger.info(f"Tim thay {len(unique)} the loai.")
    return unique

async def get_stories_from_genre_page(genre_page_url: str, site_key) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    logger.info(f"Dang lay truyen tu trang: {genre_page_url}")
    response = await make_request(genre_page_url, site_key)
    if not response or not getattr(response, 'text', None):
        logger.error(f"Khong nhan duoc phan hoi tu {genre_page_url}")
        return [], None
    soup = BeautifulSoup(response.text, "html.parser")#type: ignore
    stories: List[Dict[str, Any]] = []

    # Parse tung truyen (moi .row trong .list.list-truyen.col-xs-12)
    for row in soup.select(".list.list-truyen.col-xs-12 > .row[itemscope][itemtype='https://schema.org/Book']"):
        # Lay title va url
        a = row.select_one("h3.truyen-title a[href]")
        title = a.get_text(strip=True) if a else None
        url = a['href'] if a else None
        # Lay author
        author = row.select_one("span.author")
        author_name = author.get_text(strip=True) if author else None
        # Lay so chuong (neu muon)
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
    # Lay nut next (phai la trang tiep)
    pag_a = soup.select_one(".pagination.pagination-sm li a[title*='Trang tiep']")
    if pag_a and pag_a.has_attr("href"):
        next_page_url = pag_a['href']
        # Neu link next la link tuyet doi thi giu nguyen, neu la link tuong doi thi join lai
        if not next_page_url.startswith("http"):#type: ignore
            next_page_url = urljoin(genre_page_url, next_page_url)#type: ignore

    # Fallback sang AI neu parse rong
    if not stories:
        try:
            from ai.selector_ai import ai_parse_category_fallback
            ai_stories, ai_next = await ai_parse_category_fallback(genre_page_url, response.text)
            if ai_stories:
                return ai_stories, ai_next
        except Exception as e:
            logger.warning(f"[AI-FALLBACK][GENRE] Parse fail: {e}")
    return stories, next_page_url#type: ignore


async def get_all_stories_from_genre(
    genre_name: str, genre_url: str,
    site_key: str = "truyenfull",
    max_pages_to_crawl: Optional[int] = MAX_STORIES_PER_GENRE_PAGE
) -> List[Dict[str, Any]]:
    all_stories, visited, page = [], set(), 0
    current_url = genre_url
    while current_url and current_url not in visited and (max_pages_to_crawl is None or page < max_pages_to_crawl):
        visited.add(current_url); page += 1
        logger.info(f"Trang {page} cua '{genre_name}': {current_url}")
        sts, nxt = await get_stories_from_genre_page(current_url, site_key)
        for s in sts:
            if s['url'] not in {x['url'] for x in all_stories}:
                all_stories.append(s)
        if nxt and urlparse(nxt)._replace(fragment='') != urlparse(current_url)._replace(fragment=''):
            current_url = nxt
        else:
            break
    logger.info(f"Tong co {len(all_stories)} truyen.")
    return all_stories

def get_input_value(soup, input_id, default=None):
    tag = soup.find("input", {"id": input_id})
    return tag["value"] if tag and tag.has_attr("value") else default

async def get_chapters_from_story(
    story_url: str, story_title: str,
    total_chapters_on_site: Optional[int] = None,  # So chuong that su tren site (tu metadata)
    site_key: str = "truyenfull"
) -> List[Dict[str, str]]:
    logger.info(f"Truyen '{story_title}': Lay chuong tu {story_url}")
    chapters: List[Dict[str, str]] = []

    headers = await get_random_headers(site_key)
    async with httpx.AsyncClient(headers=headers, timeout=20) as client:
        # 1. Request trang dau de lay truyen-id, truyen-ascii, total-page
        resp = await client.get(story_url)
        soup = BeautifulSoup(resp.text, "html.parser")
        def get_input(soup, key):
            tag = soup.find("input", {"id": key})
            return tag["value"] if tag and tag.has_attr("value") else None

        truyen_id = get_input(soup, "truyen-id")
        truyen_ascii = get_input(soup, "truyen-ascii")
        total_page = get_input(soup, "total-page")
        if not (truyen_id and truyen_ascii and total_page):
            logger.error("Khong lay duoc du thong tin (truyen-id, truyen-ascii, total-page)")
            try:
                from ai.selector_ai import ai_parse_chapter_list_fallback
                fb = await ai_parse_chapter_list_fallback(story_url, resp.text)
                if fb:
                    return fb
            except Exception as e:
                logger.warning(f"[AI-FALLBACK][CHAPTER-ID] Detect fail: {e}")
            return []

        truyen_name = story_title  # Ban co the dung <title> neu muon, o day giu nguyen title truyen vao
        total_page = int(total_page)#type: ignore
        # Gioi han de phong vong lap vo han khi site tra ve nhieu trang bat thuong
        limit_pages = total_page
        try:
            if MAX_CHAPTER_PAGES_TO_CRAWL:
                limit_pages = min(total_page, int(MAX_CHAPTER_PAGES_TO_CRAWL))
        except Exception:
            limit_pages = total_page

        seen_urls = set()
        no_new_pages = 0

        # 2. Lap tung page de lay danh sach chuong qua AJAX
        for page in range(1, limit_pages + 1):
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
                logger.error(f"Loi khi goi AJAX page {page}: {ex}")
                continue
            chap_html = data.get("chap_list")
            if not chap_html:
                logger.warning(f"Khong co chap_list trong AJAX page {page}")
                continue
            soup_chap = BeautifulSoup(chap_html, "html.parser")
            before_count = len(seen_urls)
            for a in soup_chap.select("ul.list-chapter li a"):
                href = a.get("href")
                if href and href not in seen_urls:
                    seen_urls.add(href)
                    chapters.append({
                        "title": a.get("title") or a.get_text(strip=True),
                        "url": href
                    })#type: ignore
            # Fallback AI neu trang AJAX khong them duoc chuong nao (markup thay doi)
            if len(seen_urls) == before_count:
                try:
                    from ai.selector_ai import ai_parse_chapter_list_fallback
                    ai_chaps = await ai_parse_chapter_list_fallback(story_url, chap_html)
                    for it in ai_chaps:
                        if it.get('url') and it['url'] not in seen_urls:
                            seen_urls.add(it['url'])
                            chapters.append(it)
                except Exception as e:
                    logger.warning(f"[AI-FALLBACK][CHAPTER-LIST] Parse fail: {e}")
            # Neu da lay du so chuong metadata thi dung (tranh request thua)
            if total_chapters_on_site and len(chapters) >= total_chapters_on_site:
                logger.info(f"Da lay du {total_chapters_on_site} chuong, dung crawl trang chuong.")
                break
            # Neu 2 trang lien tiep khong co chuong moi => dung de tranh lap vo han
            added = len(seen_urls) - before_count
            if added <= 0:
                no_new_pages += 1
            else:
                no_new_pages = 0
            if no_new_pages >= 2:
                logger.warning("Khong thay chuong moi trong 2 trang lien tiep, dung crawl danh sach chuong.")
                break
            await smart_delay()

    # 3. Loc trung va sort lai theo so chuong
    uniq, seen = [], set()
    for ch in chapters:
        if ch['url'] not in seen:
            uniq.append(ch)
            seen.add(ch['url'])
    uniq.sort(key=lambda ch: float(re.search(r"(\d+)", ch['title']).group(1)) if re.search(r"(\d+)", ch['title']) else float('inf')) #type: ignore

    # Canh bao neu lay duoc it hon so chuong metadata
    if total_chapters_on_site and len(uniq) < total_chapters_on_site:
        logger.warning(f"CHU Y: So chuong lay duoc ({len(uniq)}) < metadata ({total_chapters_on_site}) cho truyen '{story_title}'")

    logger.info(f"Tim thay {len(uniq)} chuong.")
    return uniq

async def get_story_chapter_content(
    chapter_url: str, chapter_title: str,
    site_key
) -> Optional[str]:
    logger.info(f"Dang tai noi dung chuong '{chapter_title}': {chapter_url}")
    loop = asyncio.get_event_loop()
    response = await make_request(chapter_url, site_key)
    if not response or not getattr(response, 'text', None):
        logger.error(f"Chuong '{chapter_title}': Khong nhan duoc phan hoi tu {chapter_url}")
        return None
    html = response.text
    content = extract_chapter_content(html, site_key, chapter_title)
    if not content:
        logger.warning(f"Noi dung chuong '{chapter_title}' trong sau khi clean header.")
        return None
    return content or None

async def get_all_stories_from_genre_with_page_check(genre_name, genre_url, site_key,max_pages=None):
    """
    Crawl tat ca truyen trong 1 the loai (category) cua truyenfull.vision
    Khong phu thuoc next_page_url, crawl tung page dua vao pattern url va total_pages lay tu phan trang.
    Dung make_request (dong bo) de dong bo voi cac ham cu.
    """
    resp = await make_request(genre_url, site_key)
    if not resp or not getattr(resp, 'text', None):
        logger.error(f"Khong nhan duoc phan hoi tu {genre_url}")
        return [], 0, 0
    html = resp.text
    total_pages = get_total_pages_category(html)
    logger.info(f"Category {total_pages} co trang.")
    if max_pages:
        total_pages = min(total_pages, max_pages)

    all_stories = []
    seen_urls = set()
    pages_crawled = 0
    repeat_count = 0
    last_first_url = None

    for page in range(1, total_pages + 1):
        page_url = genre_url if page == 1 else f"{genre_url.rstrip('/')}/trang-{page}/"
        logger.info(f"[Crawl] Page {page}: {page_url}")
        resp = await make_request(page_url, site_key)
        if not resp or not getattr(resp, 'text', None):
            logger.warning(f"Khong nhan duoc phan hoi tu {page_url}")
            break
        html = resp.text
        # Parse stories tren page nay
        stories, _ = await get_stories_from_genre_page(genre_page_url=page_url, site_key=site_key)
        if not stories:
            logger.warning(f"Khong tim thay truyen nao o {page_url}, dung crawl category.")
            break
        if last_first_url and stories[0]['url'] == last_first_url:
            repeat_count += 1
        else:
            repeat_count = 0
        last_first_url = stories[0]['url']
        if repeat_count >= 2:
            logger.error("Phat hien lap trang, dung crawl de tranh vong lap vo tan")
            break
        for s in stories:
            if s['url'] not in seen_urls:
                all_stories.append(s)
                seen_urls.add(s['url'])
        pages_crawled += 1

    logger.info(f"Category {genre_name}: crawl duoc {len(all_stories)} truyen/{pages_crawled}/{total_pages} trang.")
    return all_stories, total_pages, pages_crawled
