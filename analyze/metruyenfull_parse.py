"""Helpers for scraping the metruyenfull site."""
from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup


async def get_story_metadata(adapter, story_url):
    """Fetch and parse story level metadata."""

    response = await make_request(story_url, adapter)  # type: ignore[name-defined]
    soup = BeautifulSoup(response.text, 'html.parser')

    title_tag = soup.select_one('h1.title, h1[itemprop="name"], .title h1')
    title = title_tag.get_text(strip=True) if title_tag else ''

    author_tag = soup.select_one('[itemprop="author"]')
    if author_tag:
        author = author_tag.get_text(strip=True) or author_tag.get('title', '')
    else:
        author = ''

    description_tag = soup.select_one('.desc-text-full, .desc-text, [itemprop="description"]')
    description = description_tag.get_text(strip=True) if description_tag else ''

    categories = []
    for anchor in soup.select('[itemprop="genre"][href]'):
        name = anchor.get_text(strip=True)
        if not name:
            continue
        categories.append({'name': name, 'url': urljoin(story_url, anchor['href'])})

    total_chapters = None
    chapter_span = soup.select_one('.label-success, .total-chapters')
    if chapter_span:
        match = re.search(r'(\d+)', chapter_span.get_text())
        if match:
            total_chapters = int(match.group(1))

    if total_chapters is None:
        try:
            chapters = await get_chapters_from_story(adapter, story_url)  # type: ignore[name-defined]
            total_chapters = len(chapters)
        except Exception:
            total_chapters = None

    cover_tag = soup.select_one('img[itemprop="image"], .cover img')
    cover = urljoin(story_url, cover_tag['src']) if cover_tag and cover_tag.get('src') else ''

    return {
        'title': title,
        'author': author,
        'description': description,
        'categories': categories,
        'total_chapters_on_site': total_chapters or 0,
        'cover': cover,
    }
