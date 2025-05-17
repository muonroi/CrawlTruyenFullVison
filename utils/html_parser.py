import re
from typing import List
from bs4 import BeautifulSoup

from config.config import  HEADER_RE, PATTERN_FILE
from utils.cleaner import clean_chapter_content
from utils.io_utils import filter_lines_by_patterns, load_patterns

BLACKLIST_PATTERNS = load_patterns(PATTERN_FILE)

def extract_chapter_content(html: str, patterns: List[re.Pattern]=BLACKLIST_PATTERNS) -> str:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    chapter_div = soup.find("div", id="chapter-c")
    if not chapter_div:
        return ""
    clean_chapter_content(chapter_div)

    lines = []
    for p in chapter_div.find_all("p"): #type: ignore
        line = p.get_text("", strip=True) 
        if line:
            lines.append(line)

    cleaned_lines = filter_lines_by_patterns(lines, patterns)
    content = clean_header("\n".join(cleaned_lines)).strip()
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