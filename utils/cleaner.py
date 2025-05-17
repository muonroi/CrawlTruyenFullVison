from bs4 import BeautifulSoup, Comment

from utils.logger import logger
from utils.io_utils import filter_lines_by_patterns

# Step 1: Clean HTML tags
def clean_chapter_content(chapter_div):
    for tag in chapter_div.find_all(['script', 'style']):
        tag.decompose()
    for tag in chapter_div.find_all(class_=lambda x: x and ('ads' in x or 'notice' in x or 'box-notice' in x)):
        tag.decompose()
    for element in chapter_div(text=lambda text: isinstance(text, Comment)):
        element.extract()
    return chapter_div

# Step 2: Clean lines by pattern (không hardcode)
def extract_chapter_content(html: str, patterns) -> str:
    soup = BeautifulSoup(html, "html.parser")
    chapter_div = soup.find("div", id="chapter-c")
    if not chapter_div:
        with open('debug_empty_chapter.html', 'w', encoding='utf-8') as f:
            f.write(html)
        logger.warning(f"Nội dung chương trống, đã lưu response vào debug_empty_chapter.html")
        return ""
    clean_chapter_content(chapter_div)
    text = chapter_div.get_text(separator="\n")
    lines = [line.strip() for line in text.splitlines()]
    # Dùng filter từ pattern file
    cleaned_lines = filter_lines_by_patterns(lines, patterns)
    return "\n".join(cleaned_lines).strip()