import re
from bs4 import BeautifulSoup
from scraper import make_request
from config.config import BASE_URLS
import asyncio

def absolutize(url):
    if url.startswith("http"):
        return url
    return "https://vivutruyen.com" + url

async def get_all_genres(home_url):
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, home_url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    genres = []
    for ul in soup.select('.dropdown_columns ul'):
        for li in ul.find_all('li'):
            a = li.find('a')
            if a and a.has_attr('href'):
                genres.append({
                    'name': a.get_text(strip=True),
                    'url': absolutize(a['href'])
                })
    return genres

async def get_stories_from_genre(genre_url, max_pages=None):
    # Trang thể loại chỉ có 1 page, hoặc phân trang ? Nếu có phân trang thì cần thêm code!
    loop = asyncio.get_event_loop()
    stories = []
    page_num = 1
    while True:
        url = genre_url
        if page_num > 1:
            url = genre_url.rstrip("/") + f"/{page_num}"
        resp = await loop.run_in_executor(None, make_request, url)
        if not resp:
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        story_items = soup.select('.list-truyen-item-wrap')
        found_stories_on_page = False
        for item in story_items:
            a = item.find('a', href=True, title=True)
            if a:
                title = a['title'].strip()
                url = absolutize(a['href'])
                found_stories_on_page = True
                stories.append({'title': title, 'url': url})
        if not found_stories_on_page:
            break
        # Phân trang: tìm tiếp có trang sau không?
        pag = soup.select_one('ul.pagination')
        if not pag or not pag.find("a", string=str(page_num + 1)):
            break
        page_num += 1
        if max_pages and page_num > max_pages:
            break
    return stories

async def get_all_stories_from_genre_with_page_check(genre_name, genre_url, max_pages=None):
    stories = await get_stories_from_genre(genre_url, max_pages)
    # Không có total_pages rõ ràng
    return stories, 1, 1

async def get_story_details(story_url, story_title):
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, story_url)
    if not resp:
        return {}
    soup = BeautifulSoup(resp.text, "html.parser")
    info = soup.select_one('.book-info-top')
    title = author = cover = status = None
    total_chapters = 0
    categories = []
    description = ""
    if info:
        img = info.select_one('img')
        if img and img.has_attr('src'):
            cover = absolutize(img['src'])
        h1 = info.select_one('h1')
        if h1:
            title = h1.text.strip()
        uls = info.select_one('.book-info-text')
        if uls:
            for li in uls.find_all('li'):
                t = li.text.strip().lower()
                if 'tác giả' in t:
                    a = li.find('a')
                    author = a.text.strip() if a else li.text.replace("Tác giả :", "").strip()
                if 'thể loại' in t:
                    for cat in li.find_all('a'):
                        categories.append({
                            'name': cat.text.strip(),
                            'url': absolutize(cat['href'])
                        })
                if 'số chương' in t:
                    total_chapters = int(re.search(r'(\d+)', li.text).group(1)) if re.search(r'(\d+)', li.text) else 0
                if 'trạng thái' in t:
                    span = li.find('span')
                    status = span.text.strip() if span else li.text.replace("Trạng thái :", "").strip()
    # Lấy mô tả
    desc = soup.select_one('#gioithieu [itemprop="description"]')
    if desc:
        description = desc.get_text(separator="\n", strip=True)
    return {
        'title': title,
        'author': author,
        'cover': cover,
        'categories': categories,
        'description': description,
        'total_chapters_on_site': total_chapters,
        'status': status,
        'url': story_url
    }

async def get_chapters_from_story(story_url, max_pages=None):
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, story_url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    chapters = []
    chapter_list = soup.select('.book-info-chapter .chapter-list .chap-item a')
    for a in chapter_list:
        title = a.get_text(strip=True)
        url = absolutize(a['href'])
        chapters.append({'title': title, 'url': url})
    # Check phân trang chương
    pag = soup.select_one('.phan-trang ul')
    max_page = 1
    if pag:
        # Tìm số trang cuối cùng
        last_page = pag.select('li.page-grey a')
        if last_page:
            try:
                max_page = int(last_page[-1].get('data-ci-pagination-page', '1'))
            except Exception:
                pass
    # Nếu có nhiều trang, crawl từng trang
    for page in range(2, max_page+1):
        url = f"{story_url}/{page}"
        resp = await loop.run_in_executor(None, make_request, url)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        chapter_list = soup.select('.book-info-chapter .chapter-list .chap-item a')
        for a in chapter_list:
            title = a.get_text(strip=True)
            url = absolutize(a['href'])
            chapters.append({'title': title, 'url': url})
    return chapters

async def get_story_chapter_content(chapter_url, chapter_title):
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, chapter_url)
    if not resp:
        return ""
    soup = BeautifulSoup(resp.text, "html.parser")
    content_div = soup.select_one('.truyen')
    if not content_div:
        return ""
    # Loại bỏ <br>, giữ dòng
    text = content_div.get_text(separator="\n", strip=True)
    return text
