import re
from typing import List
from bs4 import BeautifulSoup

from config.config import  PATTERN_FILE
from utils.cleaner import clean_chapter_content
from utils.io_utils import filter_lines_by_patterns, load_patterns

BLACKLIST_PATTERNS = load_patterns(PATTERN_FILE)

def extract_chapter_content(html: str, patterns: List[re.Pattern]=BLACKLIST_PATTERNS) -> str:
    soup = BeautifulSoup(html, "html.parser")
    chapter_div = soup.find("div", id="chapter-c")
    if not chapter_div:
        return ""
    clean_chapter_content(chapter_div)
    text = chapter_div.get_text(separator="\n")
    lines = [line.strip() for line in text.splitlines()]
    # Lọc metadata/header ký tự thừa
    cleaned_lines = filter_lines_by_patterns(lines, patterns)
    return "\n".join(cleaned_lines).strip()
