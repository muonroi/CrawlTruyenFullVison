import base64
import re
import zlib
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup


_BASE64_PATTERN = re.compile(r'base64\s*=\s*"([^"]+)"', re.IGNORECASE)


def _clean_text_blocks(html_fragment: str) -> str:
    """Normalize HTML fragment into plain text paragraphs."""
    fragment_soup = BeautifulSoup(html_fragment, 'lxml')
    for tag in fragment_soup.select('script, style'):
        tag.decompose()
    # Replace <br> with newline for readability
    for br in fragment_soup.find_all('br'):
        br.replace_with('\n')
    text = fragment_soup.get_text(separator='\n')
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return '\n'.join(lines)


def parse_genres(html_content: str, base_url: str) -> List[Dict[str, str]]:
    """Parse homepage HTML to extract genre/category links."""
    soup = BeautifulSoup(html_content, 'lxml')
    genres: List[Dict[str, str]] = []
    seen: set[str] = set()

    menu = soup.select_one('li#menu-item-787939')
    anchors = menu.select('ul.sub-menu a[href]') if menu else soup.select('li.menu-item a[href*="/theloai/"]')

    for anchor in anchors:
        name = anchor.get_text(strip=True)
        href = anchor.get('href')
        if not name or not href:
            continue
        url = urljoin(base_url, href)
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        genres.append({'name': name, 'url': url})

    return genres


def parse_story_list(html_content: str, base_url: str) -> Tuple[List[Dict[str, str]], int]:
    """Parse a genre page to obtain stories and pagination info."""
    soup = BeautifulSoup(html_content, 'lxml')
    stories: List[Dict[str, str]] = []

    for item in soup.select('div.popular-item-wrap'):
        link = item.select_one('h5.widget-title a[href]')
        if not link:
            continue
        title = link.get('title') or link.get_text(strip=True)
        href = link.get('href')
        if not title or not href:
            continue
        stories.append({
            'title': title.strip(),
            'url': urljoin(base_url, href),
        })

    max_page = 1
    for a in soup.select('ul.pagination a.page-link[href]'):
        text = a.get_text(strip=True)
        href = a.get('href', '')
        page_num: Optional[int] = None
        if text.isdigit():
            page_num = int(text)
        elif 'page=' in href:
            try:
                page_num = int(href.split('page=')[-1])
            except ValueError:
                page_num = None
        if page_num:
            max_page = max(max_page, page_num)

    return stories, max_page


def _extract_post_id(body_classes: List[str]) -> Optional[str]:
    for cls in body_classes:
        if cls.startswith('postid-'):
            return cls.split('postid-')[-1]
    return None


def parse_story_info(html_content: str, base_url: str) -> Dict[str, Any]:
    """Parse story detail page for metadata and inline chapter list."""
    soup = BeautifulSoup(html_content, 'lxml')

    title_tag = soup.select_one('.post-title h1, h1.post-title, h1.entry-title')
    author_tag = soup.select_one('.author-content a, .author-name a')
    cover_tag = soup.select_one('.summary_image img, .tab-summary img, .summary_image a img')

    description_block = soup.select_one('.description-summary .summary__content')
    description = ''
    if description_block:
        for tag in description_block.select('script, style'):
            tag.decompose()
        description = _clean_text_blocks(str(description_block))

    genres = [
        {'name': a.get_text(strip=True), 'url': urljoin(base_url, a.get('href'))}
        for a in soup.select('.genres-content a[href]')
        if a.get_text(strip=True)
    ]

    status_block = soup.select_one('.post-status div, .summary-content div')
    status_text = status_block.get_text(strip=True) if status_block else None

    body = soup.body or soup
    post_id = _extract_post_id(body.get('class', [])) if hasattr(body, 'get') else None

    chapters = parse_chapter_list(html_content, base_url)
    total_chapters = len(chapters) or None

    return {
        'title': title_tag.get_text(strip=True) if title_tag else None,
        'author': author_tag.get_text(strip=True) if author_tag else None,
        'description': description,
        'post_id': post_id,
        'status': status_text,
        'categories': [g['name'] for g in genres],
        'genres_full': genres,
        'cover': cover_tag.get('src') if cover_tag and cover_tag.get('src') else None,
        'chapters': chapters,
        'total_chapters_on_site': total_chapters,
    }


def parse_chapter_list(html_content: str, base_url: str) -> List[Dict[str, str]]:
    """Parse chapter listing (either inline HTML or AJAX snippet)."""
    soup = BeautifulSoup(html_content, 'lxml')
    anchors = soup.select('ul.main li.wp-manga-chapter a[href]')
    if not anchors:
        anchors = soup.select('li.wp-manga-chapter a[href]')

    chapters: List[Dict[str, str]] = []
    for a in anchors:
        title = a.get_text(strip=True)
        href = a.get('href')
        if not title or not href:
            continue
        chapters.append({
            'title': title,
            'url': urljoin(base_url, href),
        })

    # Default order is newest-first, reverse to crawl older first
    chapters.reverse()
    return chapters


def parse_chapter_content(html_content: str) -> Optional[str]:
    """Extract readable chapter text, handling base64 + zlib payload."""
    soup = BeautifulSoup(html_content, 'lxml')
    script = soup.select_one('script#decompress-script')

    if script and script.string:
        match = _BASE64_PATTERN.search(script.string)
        if match:
            try:
                decoded = base64.b64decode(match.group(1))
                inflated = zlib.decompress(decoded)
                text = _clean_text_blocks(inflated.decode('utf-8', errors='ignore'))
                if text:
                    return text
            except Exception:
                pass

    content_div = soup.select_one('#chapter-reading-content')
    if content_div:
        return _clean_text_blocks(str(content_div))

    return None
