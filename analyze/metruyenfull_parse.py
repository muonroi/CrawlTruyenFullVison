"""Parsing helpers for metruyenfull crawler."""
from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup


class _NullLogger:
    def warning(self, *args, **kwargs) -> None:  # pragma: no cover - fallback
        pass


async def _missing_make_request(*args, **kwargs):  # pragma: no cover - fallback
    raise RuntimeError("make_request is not configured")


async def _missing_get_chapters(*args, **kwargs) -> list:
    return []


logger = _NullLogger()
make_request = _missing_make_request
get_chapters_from_story = _missing_get_chapters


async def get_story_metadata(adapter, story_url: str) -> dict[str, object]:
    """Fetch a story page and extract key metadata fields."""
    response = await make_request(story_url, adapter.SITE_KEY)
    soup = BeautifulSoup(response.text, "html.parser")

    title_tag = soup.select_one("h1.title")
    title = title_tag.get_text(strip=True) if title_tag else None

    author_tag = soup.select_one("a[itemprop='author']")
    author = None
    if author_tag:
        author = author_tag.get("title") or author_tag.get_text(strip=True)

    desc_tag = soup.select_one(".desc-text")
    description = desc_tag.get_text(strip=True) if desc_tag else ""

    categories: list[dict[str, str]] = []
    for anchor in soup.select("a[itemprop='genre']"):
        name = anchor.get_text(strip=True)
        href = anchor.get("href")
        if not name or not href:
            continue
        categories.append({"name": name, "url": urljoin(story_url, href)})

    cover_tag = soup.select_one("img[itemprop='image']")
    cover = cover_tag.get("src") if cover_tag else None

    total_chapters: int | None = None
    badge = soup.select_one(".label-success")
    if badge:
        match = re.search(r"(\d+)", badge.get_text())
        if match:
            total_chapters = int(match.group(1))

    result = {
        "title": title,
        "author": author,
        "description": description,
        "categories": categories,
        "cover": cover,
        "total_chapters_on_site": total_chapters,
    }

    try:
        result["chapters"] = await get_chapters_from_story(adapter, story_url)
    except Exception:
        logger.warning("[metruyenfull] Failed to fetch chapters list", exc_info=True)
        result["chapters"] = []

    return result
