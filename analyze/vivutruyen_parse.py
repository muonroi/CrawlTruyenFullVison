import re
from bs4 import BeautifulSoup
import asyncio
from scraper import make_request

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
            a = li.find('a') #type:ignore
            if a and a.has_attr('href'): #type:ignore
                genres.append({
                    'name': a.get_text(strip=True), #type:ignore
                    'url': absolutize(a['href']) #type:ignore
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
                title = a['title'].strip() #type:ignore
                url = absolutize(a['href']) #type:ignore
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
                    a = li.find('a') #type:ignore
                    author = a.text.strip() if a else li.text.replace("Tác giả :", "").strip() #type:ignore
                if 'thể loại' in t:
                    for cat in li.find_all('a'): #type:ignore
                        categories.append({
                            'name': cat.text.strip(),
                            'url': absolutize(cat['href']) #type:ignore
                        })
                if 'số chương' in t:
                    total_chapters = int(re.search(r'(\d+)', li.text).group(1)) if re.search(r'(\d+)', li.text) else 0 #type:ignore
                if 'trạng thái' in t:
                    span = li.find('span') #type:ignore
                    status = span.text.strip() if span else li.text.replace("Trạng thái :", "").strip() #type:ignore
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

async def get_chapter_list(self, story_url, story_title, max_pages=None, total_chapters=None, site_key=None):
    import re
    from bs4 import BeautifulSoup
    from scraper import make_request
    import asyncio

    def absolutize(url):
        if url.startswith("http"):
            return url
        return "https://vivutruyen.com" + url

    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, make_request, story_url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")

    # Lấy list chương trang đầu
    all_chapters = []
    chapter_list = soup.select('.book-info-chapter .chapter-list .chap-item a')
    for a in chapter_list:
        title = a.get_text(strip=True)
        url = absolutize(a['href'])
        all_chapters.append({'title': title, 'url': url})

    # Phân trang: lấy số page cuối cùng
    total_pages = 1
    pag = soup.select_one('.phan-trang ul')
    if pag:
        page_links = pag.select('a[data-ci-pagination-page]')
        if page_links:
            try:
                total_pages = max(int(a['data-ci-pagination-page']) for a in page_links) #type:ignore
            except Exception:
                nums = [int(a.get_text()) for a in page_links if a.get_text().isdigit()]
                if nums:
                    total_pages = max(nums)
    # Parse story_id từ url
    match = re.search(r'/truyen/[^/]+/(\d+)', story_url)
    story_id = match.group(1) if match else None
    if not story_id:
        for a in pag.select('a[href]'): #type:ignore
            m = re.search(r'/truyen/[^/]+/(\d+)/', a['href']) #type:ignore
            if m:
                story_id = m.group(1)
                break
    if not story_id:
        print("[ERROR] Không lấy được story_id từ URL hoặc phân trang.")
        return all_chapters

    # Lặp các page còn lại
    for page in range(2, total_pages+1):
        page_url = f"{story_url}/{story_id}/{page}"
        resp = await loop.run_in_executor(None, make_request, page_url)
        if not resp:
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        chapter_list = soup.select('.book-info-chapter .chapter-list .chap-item a')
        for a in chapter_list:
            title = a.get_text(strip=True)
            url = absolutize(a['href'])
            all_chapters.append({'title': title, 'url': url})

    # Đảo ngược thứ tự
    all_chapters = list(reversed(all_chapters))
    # Debug số chương
    print(f"[vivutruyen] Lấy được tổng cộng {len(all_chapters)} chương")
    if all_chapters:
        print(f"  Chương đầu: {all_chapters[0]['title']}, chương cuối: {all_chapters[-1]['title']}")
    return all_chapters
