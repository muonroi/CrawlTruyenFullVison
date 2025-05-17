from config.config import PATTERN_FILE, load_blacklist_patterns
from scraper import make_request
from bs4 import BeautifulSoup
import re
PATTERNS, CONTAINS_LIST = load_blacklist_patterns(PATTERN_FILE)

def get_all_categories(self, home_url):
    resp = make_request(home_url)
    if not resp:
        return []
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    categories = []
    for ul_menu in soup.select('.dropdown-menu.multi-column ul.dropdown-menu'):
        for li_item in ul_menu.find_all('li'):
            a_tag = li_item.find('a') #type: ignore
            if a_tag and a_tag.has_attr('href'): #type: ignore
                categories.append({
                    'name': a_tag.get_text(strip=True), #type: ignore
                    'url': a_tag['href'] #type: ignore
                })
    return categories

def get_stories_from_category(self, category_url):
    stories = []
    page_num = 1
    while True:
        current_url = category_url
        if page_num > 1:
            current_url = category_url.rstrip('/') + f"/page/{page_num}"
        resp = make_request(current_url)
        if not resp:
            break
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
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

def get_story_metadata(self, story_url):
    resp = make_request(story_url)
    if not resp:
        return None
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.select_one('h1.title')
    title = title_tag.get_text(strip=True) if title_tag else ''
    author_tag = soup.select_one('.info-holder a[itemprop="author"]')
    author = author_tag.get_text(strip=True) if author_tag else None
    desc_tag = soup.select_one('.desc-text')
    description = desc_tag.get_text(strip=True) if desc_tag else None
    status = None
    for h4_tag in soup.select('.info-holder h4'):
        txt = h4_tag.get_text(strip=True)
        if "Trạng thái" in txt or "Status" in txt:
            strong_tag = h4_tag.find('strong')
            status = strong_tag.get_text(strip=True) if strong_tag else txt
            break
    categories = []
    for a_tag in soup.select('.info-holder h4 a[itemprop="genre"]'):
        if a_tag.has_attr('href'):
            categories.append({'name': a_tag.get_text(strip=True), 'url': a_tag['href']})
    num_chapters = 0
    for label in soup.select('.info-holder .label-success'):
        txt = label.get_text()
        if 'chương' in txt.lower():
            match = re.search(r'(\d+)\s*[Cc]hương', txt)
            if match:
                num_chapters = int(match.group(1))
                break
    img_tag = soup.select_one('.info-holder img[itemprop="image"]')
    cover = img_tag['src'] if img_tag and img_tag.has_attr('src') else None
    return {
        "title": title,
        "author": author,
        "description": description,
        "status": status,
        "categories": categories,
        "total_chapters_on_site": num_chapters,
        "cover": cover,
        "url": story_url
    }

def get_chapters_from_story(self, story_url):
    chapters = []
    page_num = 1
    while True:
        current_url = story_url
        if page_num > 1:
            current_url = story_url.rstrip('/') + f"/page/{page_num}"
        resp = make_request(current_url)
        if not resp:
            break
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
        found_chapters_on_page = False
        for li_item in soup.select('ul.list-chapter li, ul.l-chapters li'):
            a_tag = li_item.find('a')
            if a_tag and a_tag.has_attr('href') and 'chuong-' in a_tag['href']: #type: ignore
                chapter_title = a_tag.get_text(strip=True)
                href = a_tag['href'] #type: ignore
                chapters.append({
                    "title": chapter_title,
                    "url": href
                })
                found_chapters_on_page = True
        if not found_chapters_on_page:
            break
        pagination = soup.select_one('ul.pagination')
        if not pagination or not pagination.find("a", string=str(page_num + 1)):
            break
        page_num += 1
    return chapters

def filter_lines_by_patterns(lines, patterns, contains_list):
    filtered = []
    cutting = False
    for line in lines:
        lwr = line.lower()
        if any(key in lwr for key in contains_list):
            cutting = True
        if not cutting and not any(p.search(line) for p in patterns):
            filtered.append(line)
    return filtered

def extract_chapter_content(self, chapter_url):
    resp = make_request(chapter_url)
    if not resp:
        return ""
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    chapter_divs = soup.find_all("div", id="chapter-c")
    chapter_div = chapter_divs[-1] if chapter_divs else None
    if not chapter_div:
        return ""
    for ad_element in chapter_div.find_all(['div', 'ins', 'script', 'style', 'iframe']): #type: ignore
        ad_element.decompose()
    text = chapter_div.get_text(separator="\n")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    lines = filter_lines_by_patterns(lines, PATTERNS, CONTAINS_LIST)
    blacklist = [
        "Các bạn đang đọc truyện tại",
        "nhớ cho 1 đánh giá",
        "website có một số quảng cáo",
        "Để có kinh phí duy trì"
    ]

    content = "\n".join([line for line in lines if not any(bad_phrase in line for bad_phrase in blacklist)])
    return content.strip()
