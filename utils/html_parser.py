import os
from bs4 import BeautifulSoup
from config.config import  HEADER_RE, PATTERN_FILE
from utils.chapter_utils import slugify_title
from config.config import PATTERN_FILE
from config.config import SITE_SELECTORS, PATTERN_FILE
from utils.cleaner import clean_chapter_content
from utils.io_utils import filter_lines_by_patterns, load_patterns
from utils.logger import logger
BLACKLIST_PATTERNS = load_patterns(PATTERN_FILE)
import chardet
def extract_chapter_content(
    html: str, 
    site_key: str, 
    chapter_title: str = None,#type: ignore
    patterns: list = None#type: ignore
) -> str:


    if patterns is None:
        patterns = load_patterns(PATTERN_FILE)

    debug_prefix = f"[DEBUG][{site_key}][{chapter_title}]"

    # --- Detect encoding nếu cần ---
    try:
        if isinstance(html, bytes):
            detected = chardet.detect(html)
            logger.info(f"{debug_prefix} Detected encoding: {detected}")
            html = html.decode(detected['encoding'] or 'utf-8', errors='replace')
    except Exception as e:
        logger.error(f"{debug_prefix} Lỗi khi detect/decode encoding: {e}")

    # --- Parse HTML và lấy DIV chương ---
    # Dùng "html5lib" để tránh lỗi parser (nếu vẫn fail đổi sang "lxml")
    soup = BeautifulSoup(html, "html5lib")
    chapter_div = None

    # Log rõ từng id có ký tự lạ
    for div in soup.find_all('div'):
        div_id = div.get('id')#type: ignore
        if div_id:
            logger.warning(f"[DEBUG][{site_key}][{chapter_title}] id: {repr(div_id)}")
            # Fallback mạnh: loại bỏ mọi ký tự trắng/ẩn
            id_clean = div_id.strip().lower().replace('\u200b', '').replace('\xa0', '').replace('\t', '').replace('\n', '') #type: ignore
            if 'chapter-c' in id_clean:
                chapter_div = div
                logger.warning(f"[DEBUG][{site_key}][{chapter_title}] FOUND id: {repr(div_id)} CLEAN: {repr(id_clean)}")
                # Dump ra file để bạn check thật sự div nào lấy được
                with open(f'debug_div_{slugify_title(chapter_title)}.html', 'w', encoding='utf-8') as f:
                    f.write(str(div))
                break

    # Fallback selector nếu vẫn chưa thấy (dùng SITE_SELECTORS cũ)
    if not chapter_div:
        selector_fn = SITE_SELECTORS.get(site_key)
        if selector_fn:
            try:
                chapter_div = selector_fn(soup)
            except Exception as e:
                logger.error(f"{debug_prefix} Lỗi khi gọi selector_fn: {e}")

    # Đúng ra phải kiểm tra lại chapter_div trước khi xử lý tiếp:
    if not chapter_div:
        fname = f"debug_empty_chapter_{slugify_title(chapter_title) or 'unknown'}.html"
        if not os.path.exists(fname):
            with open(fname, 'w', encoding='utf-8') as f:
                f.write(html)
        logger.error(f"{debug_prefix} Không tìm thấy selector DIV nội dung chương. Đã lưu HTML vào {fname}")
        return ""

    try:
        clean_chapter_content(chapter_div)
    except Exception as e:
        logger.error(f"{debug_prefix} Lỗi khi chạy clean_chapter_content: {e}")

    text = chapter_div.get_text(separator="\n")
    # --- Lọc dòng trắng, strip ---
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    logger.info(f"{debug_prefix} Số dòng sau strip: {len(lines)}")
    if len(lines) < 3:
        logger.warning(f"{debug_prefix} Sau clean còn rất ít dòng ({len(lines)}). Có thể mất nội dung.")

    # --- Lọc bằng blacklist patterns ---
    try:
        cleaned_lines = filter_lines_by_patterns(lines, patterns)
    except Exception as e:
        logger.error(f"{debug_prefix} Lỗi khi filter_lines_by_patterns: {e}")
        cleaned_lines = lines

    # --- Clean header cuối cùng ---
    content = clean_header("\n".join(cleaned_lines)).strip()
    # --- Kiểm tra kết quả cuối ---
    if not content:
        fname = f"debug_empty_chapter_{slugify_title(chapter_title) or 'unknown'}_after_filter.html"
        if not os.path.exists(fname):
            with open(fname, 'w', encoding='utf-8') as f:
                f.write(html)
        logger.error(f"{debug_prefix} Nội dung chương EMPTY sau khi filter/clean. Đã lưu HTML vào {fname}")
        return ""

    # --- Có nội dung ---
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