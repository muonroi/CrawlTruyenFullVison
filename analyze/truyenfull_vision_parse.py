import re
import asyncio
from typing import List, Tuple, Optional, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from utils.html_parser import extract_chapter_content
from utils.logger import logger
from scraper import make_request
from config.config import (
    MAX_STORIES_PER_GENRE_PAGE,
)
async def get_story_details(story_url: str, story_title_for_log: str) -> Dict[str, Any]:
    logger.info(f"Truyện '{story_title_for_log}': Đang lấy thông tin chi tiết từ {story_url}")
    details = {
        "description": None,
        "status": None,
        "source": None,
        "detailed_genres": [],
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

    # 1. Description
    desc_div = soup.find("div", class_="desc-text", itemprop="description")
    if desc_div:
        if (more := desc_div.find("div", class_="showmore")): more.decompose() #type: ignore
        for br in desc_div.find_all("br"): br.replace_with("\n")#type: ignore
        text = desc_div.get_text(separator="\n", strip=True)
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        details["description"] = "\n".join(lines)
    else:
        if (meta := soup.find("meta", attrs={"name": "description"})) and meta.get("content"):#type: ignore
            details["description"] = meta["content"].strip()#type: ignore

    # 2. Info div
    holder = soup.select_one("div.col-info-desc > div.info-holder > div.info") or soup.find("div", class_="info")
    if holder:
        for item in holder.find_all("div", recursive=False):#type: ignore
            if not (h3 := item.find("h3")): continue#type: ignore
            label = h3.get_text(strip=True).lower()#type: ignore
            node = h3.next_sibling#type: ignore
            parts: List[str] = []
            genres_tmp: List[Dict[str, str]] = []
            while node:
                if getattr(node, 'name', None) == 'a':
                    nm = node.get_text(strip=True); href = node.get('href')#type: ignore
                    if nm and href:
                        genres_tmp.append({"name": nm, "url": urljoin(story_url, href)})#type: ignore
                    elif nm:
                        parts.append(nm)
                elif getattr(node, 'name', None) == 'span':
                    parts.append(node.get_text(strip=True))
                elif isinstance(node, str) and node.strip():
                    parts.append(node.strip())
                nxt = getattr(node, 'next_sibling', None)
                if getattr(nxt, 'name', None) == 'h3': break
                node = nxt
            value = ", ".join(filter(None, parts)).rstrip(',')
            if "thể loại:" in label:
                details["detailed_genres"] = genres_tmp or [{"name": g.strip(), "url": None} for g in value.split(',')]
            elif "nguồn:" in label:
                details["source"] = value
            elif "trạng thái:" in label:
                span = item.find("span", class_=re.compile(r"text-primary|text-success|text-danger"))#type: ignore
                details["status"] = span.get_text(strip=True) if span else value
            elif "số chương:" in label:
                if m := re.search(r"(\d+)", value):
                    try:
                        details["total_chapters_on_site"] = int(m.group(1))
                    except ValueError:
                        logger.warning(f"Không thể chuyển đổi số chương '{m.group(1)}'.")

    # 3. Latest chapters fallback
    ul = soup.find("ul", class_="l-chapters")
    if not ul:
        cont = soup.find("div", id="list-chapter")
        if cont:
            ul = cont.find("ul", class_=re.compile(r"list-chapter"))#type: ignore
    if ul and (li := ul.find("li")) and (a := li.find("a")):#type: ignore
        txt, tit = a.get_text(strip=True), a.get('title','')#type: ignore
        m = re.search(r"Chương\s*(\d+)|^(\d+):", txt) or re.search(r"Chương\s*(\d+)|^(\d+):", tit)#type: ignore
        if m:
            num = int(m.group(1) or m.group(2))
            st = (details.get('status') or '').lower()
            tc = details.get('total_chapters_on_site')
            if tc is None or num > (tc or 0):
                details['total_chapters_on_site'] = num
                logger.info(f"Cập nhật tổng chương: {num}")

    # 4. Rating
    if (rd := soup.find("div", class_="rate-holder")) and rd.get('data-score'):#type: ignore
        details['rating_value'] = rd['data-score']#type: ignore
    if (rc := soup.select_one("div.small[itemprop='aggregateRating'] span[itemprop='ratingCount']")):
        details['rating_count'] = rc.get_text(strip=True)

    # 5. Fallback: Đếm số chương bằng cách đếm <li> của ul.list-chapter nếu vẫn chưa có số chương
    if details['total_chapters_on_site'] is None or details['total_chapters_on_site'] <= 1:
        cont = soup.find("div", id="list-chapter")
        chapter_lis = []
        total_pages = 1
        if cont:
            chapter_uls = cont.find_all("ul", class_=re.compile(r"list-chapter"))#type: ignore
            for ul in chapter_uls:
                chapter_lis += ul.find_all("li")#type: ignore
            total_page_input = cont.find("input", {"id": "total-page"})#type: ignore
            if total_page_input:
                try:
                    total_pages = int(total_page_input["value"])#type: ignore
                except Exception:
                    total_pages = 1
        if total_pages > 1:
            base_url = story_url.rstrip("/")
            # fetch tất cả các trang
            for i in range(2, total_pages+1):
                page_url = f"{base_url}/trang-{i}/"
                sub_resp = await loop.run_in_executor(None, make_request, page_url)
                if sub_resp and getattr(sub_resp, 'text', None):
                    sub_soup = BeautifulSoup(sub_resp.text, "html.parser")
                    sub_cont = sub_soup.find("div", id="list-chapter")
                    if sub_cont:
                        sub_uls = sub_cont.find_all("ul", class_=re.compile(r"list-chapter"))#type: ignore
                        for ul in sub_uls:
                            chapter_lis += ul.find_all("li")#type: ignore
        details["total_chapters_on_site"] = len(chapter_lis)
        logger.info(f"Đếm tổng số chương thực tế: {len(chapter_lis)}")

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

async def get_chapters_from_story(
    story_url: str, story_title: str,
    max_pages: Optional[int] = None,         # Không giới hạn mặc định
    total_chapters_on_site: Optional[int] = None  # Số chương thật sự trên site (từ metadata)
) -> List[Dict[str, str]]:
    logger.info(f"Truyện '{story_title}': Lấy chương từ {story_url}")
    loop = asyncio.get_event_loop()
    chapters: List[Dict[str, str]] = []

    # 1. Lấy trang đầu tiên
    response = await loop.run_in_executor(None, make_request, story_url)
    if not response or not getattr(response, 'text', None):
        logger.error(f"Không nhận được phản hồi từ {story_url}")
        return []
    soup = BeautifulSoup(response.text, "html.parser")
    cont = soup.find("div", id="list-chapter")
    if not cont:
        logger.warning("Không tìm thấy list-chapter")
        return []

    # 2. Lấy tổng số trang chương
    total_pages = 1
    total_page_input = cont.find("input", {"id": "total-page"}) #type: ignore
    if total_page_input:
        try:
            total_pages = int(total_page_input["value"])#type: ignore
        except Exception:
            total_pages = 1

    if max_pages:
        total_pages = min(total_pages, max_pages)

    # 3. Crawl từng trang chương (dừng khi đã lấy đủ số chương thực tế nếu truyền vào)
    for i in range(1, total_pages + 1):
        if i == 1:
            cur_soup = soup
        else:
            page_url = f"{story_url.rstrip('/')}/trang-{i}/"
            logger.info(f"Lấy chương trang {i}: {page_url}")
            resp = await loop.run_in_executor(None, make_request, page_url)
            if not resp or not getattr(resp, 'text', None):
                continue
            cur_soup = BeautifulSoup(resp.text, "html.parser")

        cur_cont = cur_soup.find("div", id="list-chapter")
        if cur_cont:
            for ul in cur_cont.find_all("ul", class_=re.compile(r"list-chapter")):#type: ignore
                for li in ul.find_all("li"):#type: ignore
                    a = li.find("a", href=True)#type: ignore
                    if a:
                        chapters.append({
                            "url": a["href"],#type: ignore
                            "title": a.get("title") or a.get_text(strip=True) #type: ignore
                        }) #type: ignore
        # Nếu đã lấy đủ số chương metadata thì dừng
        if total_chapters_on_site and len(chapters) >= total_chapters_on_site:
            logger.info(f"Đã lấy đủ {total_chapters_on_site} chương, dừng crawl trang chương.")
            break

    # 4. Xử lý trùng lặp và sắp xếp
    uniq, seen = [], set()
    for ch in chapters:
        if ch['url'] not in seen:
            uniq.append(ch)
            seen.add(ch['url'])

    uniq.sort(key=lambda ch: float(re.search(r"(\d+)", ch['title']).group(1)) if re.search(r"(\d+)", ch['title']) else float('inf')) #type: ignore

    # Cảnh báo nếu lấy được ít hơn số chương metadata
    if total_chapters_on_site and len(uniq) < total_chapters_on_site:
        logger.warning(f"CHÚ Ý: Số chương lấy được ({len(uniq)}) < metadata ({total_chapters_on_site}) cho truyện '{story_title}'")

    logger.info(f"Tìm thấy {len(uniq)} chương.")
    return uniq


async def get_story_chapter_content(
    chapter_url: str, chapter_title: str
) -> Optional[str]:
    logger.info(f"Đang tải nội dung chương '{chapter_title}': {chapter_url}")
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(None, make_request, chapter_url)
    if not response or not getattr(response, 'text', None):
        logger.error(f"Chương '{chapter_title}': Không nhận được phản hồi từ {chapter_url}")
        return None
    html = response.text
    content = extract_chapter_content(html)
    if not content:
        logger.warning(f"Nội dung chương '{chapter_title}' trống sau khi clean header.")
        return None
    return content or None

