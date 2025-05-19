import re
from typing import List
from bs4 import BeautifulSoup

from config.config import  HEADER_RE, PATTERN_FILE
from scraper import make_request
from utils.logger import logger
from utils.cleaner import clean_chapter_content
from utils.io_utils import filter_lines_by_patterns, load_patterns

BLACKLIST_PATTERNS = load_patterns(PATTERN_FILE)

def extract_chapter_content(html: str, patterns: List[re.Pattern]=BLACKLIST_PATTERNS) -> str:
    soup = BeautifulSoup(html, "html.parser")
    chapter_div = soup.find("div", id="chapter-c")
    if not chapter_div:
        with open('debug_empty_chapter.html', 'w', encoding='utf-8') as f:
            f.write(html)
        logger.warning(f"Không tìm thấy Div Nội dung chương trống, đã lưu response vào debug_empty_chapter.html")
        return ""
    
    # Đừng xóa thẳng tay nếu chưa chắc clean
    clean_chapter_content(chapter_div)

    # Log text sau clean_chapter_content
    text = chapter_div.get_text(separator="\n")

    lines = [line.strip() for line in text.splitlines() if line.strip()]

    cleaned_lines = filter_lines_by_patterns(lines, patterns)

    content = clean_header("\n".join(cleaned_lines)).strip()

    if not content:
        logger.warning("Nội dung chương trống sau khi lọc, đã lưu response vào debug_empty_chapter.html")
        with open('debug_empty_chapter.html', 'w', encoding='utf-8') as f:
            f.write(html)
        return ""
    return content

def clean_header(text: str):
    lines = text.splitlines()
    out = []
    skipping = True
    for line in lines:
        l = line.strip()
        if not l:
            continue
        if skipping and HEADER_RE.match(l):
            continue
        skipping = False
        out.append(line)
    return "\n".join(out).strip()

def get_total_pages_category(html: str) -> int:
    from bs4 import BeautifulSoup
    import re
    soup = BeautifulSoup(html, "html.parser")
    pag = soup.select_one('ul.pagination')
    if not pag:
        return 1
    max_page = 1
    for a in pag.find_all('a'):
        # Ưu tiên text "Cuối"
        if 'Cuối' in a.get_text():
            # Ưu tiên lấy số từ title nếu có
            title = a.get('title', '')#type: ignore
            m = re.search(r'trang[- ]?(\d+)', title, re.I)#type: ignore
            if m:
                num = int(m.group(1))
                if num > max_page:
                    max_page = num
            else:
                # Nếu không có title, lấy từ href
                href = a.get('href', '')#type: ignore
                m = re.search(r'/trang-(\d+)', href) #type: ignore
                if m:
                    num = int(m.group(1))
                    if num > max_page:
                        max_page = num
        # Ngoài ra, lấy số lớn nhất xuất hiện trong các thẻ <a> khác
        elif a.get_text().strip().isdigit():
            num = int(a.get_text().strip())
            if num > max_page:
                max_page = num
    return max_page


def parse_stories_from_category_page(html: str):
    soup = BeautifulSoup(html, "html.parser")
    stories = []
    for row in soup.select('div.row[itemtype="https://schema.org/Book"]'):
        a_tag = row.select_one('.truyen-title a')
        if a_tag and a_tag.has_attr('href'):
            title = a_tag.get_text(strip=True)
            href = a_tag['href']
            stories.append({
                "title": title,
                "url": href
            })
    return stories


def get_total_pages_metruyen_category(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    pag = soup.select_one('ul.pagination')
    if not pag:
        return 1
    max_page = 1
    for a in pag.find_all('a'):
        # Ưu tiên lấy số trang từ thuộc tính data-page
        data_page = a.get('data-page') #type: ignore
        if data_page and data_page.isdigit(): #type: ignore
            num = int(data_page) #type: ignore
            if num > max_page:
                max_page = num
        # Nếu là nút cuối (Cuối), lấy từ title hoặc href
        elif 'Cuối' in a.get_text() or 'Cuối' in a.get('title', ''): #type: ignore
            # Lấy số trang từ title hoặc href
            title = a.get('title') #type: ignore
            if title and title.isdigit(): #type: ignore
                num = int(title) #type: ignore
                if num > max_page:
                    max_page = num
            else:
                import re
                m = re.search(r'/page/(\d+)', a.get('href', '')) #type: ignore
                if m:
                    num = int(m.group(1))
                    if num > max_page:
                        max_page = num
        # Nếu là số trong text
        elif a.get_text().isdigit():
            num = int(a.get_text())
            if num > max_page:
                max_page = num
    return max_page