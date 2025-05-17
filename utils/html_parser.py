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
    cleaned_lines = filter_lines_by_patterns(lines, patterns)
    content = clean_header("\n".join(cleaned_lines)).strip()
    return content

HEADER_PATTERNS = [
    r"^nguồn:",
    r"^truyện:",
    r"^thể loại:",
    r"^chương:",
]
HEADER_RE = re.compile("|".join(HEADER_PATTERNS), re.IGNORECASE)

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