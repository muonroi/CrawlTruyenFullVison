import html
import json
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

_DEFAULT_PARSER = "html.parser"


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def _extract_first_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    match = re.search(r"(\d+)", text)
    return int(match.group(1)) if match else None


def parse_genres(html_content: str, base_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    results: List[Dict[str, str]] = []
    seen: set[str] = set()

    for anchor in soup.select("a[href*='/the-loai/']"):
        name = _normalize_text(html.unescape(anchor.get_text(" ", strip=True)))
        href = anchor.get("href")
        if not name or not href:
            continue
        url = urljoin(base_url, href)
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append({"name": name, "url": url})

    return results


def parse_story_list(html_content: str, base_url: str) -> Tuple[List[Dict[str, str]], int]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    stories: List[Dict[str, str]] = []
    seen: set[str] = set()

    def _extract_story_from_container(container) -> Optional[Dict[str, str]]:
        title_tag = None
        for selector in (
            "a.name[href]",
            "a.book-name[href]",
            "a.bookTitle[href]",
            "h3 a[href]",
            "h4 a[href]",
            "a[href]",
        ):
            candidate = container.select_one(selector)
            if not candidate:
                continue
            href_value = candidate.get("href") or ""
            if "/chuong-" in href_value.lower():
                continue
            title_tag = candidate
            break

        if not title_tag:
            return None

        title = _normalize_text(html.unescape(title_tag.get_text(" ", strip=True)))
        href = title_tag.get("href")
        if not title or not href:
            return None
        url = urljoin(base_url, href)
        if url in seen:
            return None

        story: Dict[str, str] = {"title": title, "url": url}

        latest_tag = None
        for selector in ("a.section[href]", "a.chapter[href]", "a[href]"):
            candidate = container.select_one(selector)
            if not candidate:
                continue
            href_value = (candidate.get("href") or "").lower()
            if "/chuong-" not in href_value:
                continue
            latest_tag = candidate
            break

        if latest_tag:
            story["latest_chapter"] = _normalize_text(
                html.unescape(latest_tag.get_text(" ", strip=True))
            )
            latest_href = latest_tag.get("href") or ""
            story["latest_chapter_url"] = urljoin(base_url, latest_href)

        author_tag = None
        for selector in ("a.author", "a.writer", "span.author", "p.author a", "p.author"):
            candidate = container.select_one(selector)
            if not candidate:
                continue
            author_text = _normalize_text(html.unescape(candidate.get_text(" ", strip=True)))
            if author_text:
                author_tag = author_text
                break

        if author_tag:
            story["author"] = author_tag

        seen.add(url)
        return story

    for row in soup.select("div.update-list table tbody tr"):
        story = _extract_story_from_container(row)
        if story:
            stories.append(story)

    if not stories:
        for item in soup.select("div.update-list div.book-wrap, div.update-list li, div.book-list li, div.update-list div.story-card"):
            story = _extract_story_from_container(item)
            if story:
                stories.append(story)

    if not stories:
        container = soup.select_one("div.update-list") or soup
        for anchor in container.select("a[href*='/doc-truyen/']"):
            href = anchor.get("href") or ""
            if not href or "/chuong-" in href.lower():
                continue
            parent_story = _extract_story_from_container(anchor.parent or container)
            if parent_story:
                stories.append(parent_story)

    max_page = 1
    for anchor in soup.select("a[href*='page=']"):
        href = anchor.get("href") or ""
        match = re.search(r"page=([0-9]+)", href)
        if match:
            try:
                max_page = max(max_page, int(match.group(1)))
            except ValueError:
                continue

    for option in soup.select("select option[value]"):
        value = option.get("value")
        if value and value.isdigit():
            max_page = max(max_page, int(value))

    for element in soup.select("[data-page]"):
        data_page = element.get("data-page")
        if data_page and data_page.isdigit():
            max_page = max(max_page, int(data_page))

    return stories, max_page


def parse_story_info(html_content: str, base_url: str = "") -> Dict[str, Any]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)

    title_tag = soup.select_one("div.book-info h1")
    title = html.unescape(title_tag.get_text(" ", strip=True)) if title_tag else None
    if title and " - " in title:
        head, tail = title.split(" - ", 1)
        if tail.strip() and set(tail.strip()) <= {"?", "-"}:
            title = head.strip()
    if title:
        title = _normalize_text(title)

    author_tag = soup.select_one("p.tag a.blue")
    author = _normalize_text(html.unescape(author_tag.get_text(" ", strip=True))) if author_tag else None

    status_tag = soup.select_one("p.tag span.blue")
    status = _normalize_text(html.unescape(status_tag.get_text(" ", strip=True))) if status_tag else None

    description_parts: List[str] = []
    for node in soup.select("div.book-info p.intro, p.intro"):
        text = _normalize_text(html.unescape(node.get_text(" ", strip=True)))
        if text:
            description_parts.append(text)
    if not description_parts:
        summary = soup.select_one("div#bookDetail, div.book-detail")
        if summary:
            text = _normalize_text(html.unescape(summary.get_text(" ", strip=True)))
            if text:
                description_parts.append(text)
    description = " ".join(description_parts)

    cover_tag = soup.select_one("div.book-img img[src]")
    cover = urljoin(base_url, cover_tag.get("src", "")) if cover_tag else None

    genre_tags = soup.select("p.tag a.red")
    if not genre_tags:
        genre_tags = soup.select("div.crumbs-nav a[href*='/the-loai/']")
    genres: List[Dict[str, str]] = []
    seen_genres: set[str] = set()
    for tag in genre_tags:
        name = _normalize_text(html.unescape(tag.get_text(" ", strip=True)))
        href = tag.get("href")
        if not name or not href:
            continue
        url = urljoin(base_url, href)
        key = url.lower()
        if key in seen_genres:
            continue
        seen_genres.add(key)
        genres.append({"name": name, "url": url})

    story_id = None
    story_meta = soup.find("meta", attrs={"name": "book_detail"})
    if story_meta and story_meta.get("content"):
        story_id = story_meta["content"].strip()
    if not story_id:
        hidden = soup.select_one("input#story_id_hidden[value]")
        if hidden and hidden.get("value"):
            story_id = hidden["value"].strip()

    path_meta = soup.find("meta", attrs={"name": "book_path"})
    story_path = path_meta.get("content").strip() if path_meta and path_meta.get("content") else None

    total_chapters = None
    catalog_link = soup.select_one("a#j-bookCatalogPage")
    if catalog_link:
        total_chapters = _extract_first_int(catalog_link.get_text(" ", strip=True))

    inline_chapters: List[Dict[str, str]] = []
    catalog_container = soup.select_one("div.catalog-content")
    if catalog_container:
        inline_html = catalog_container.decode_contents()
        if inline_html:
            inline_chapters = parse_chapter_list(inline_html, base_url)

    rating_value = None
    rating_count = None
    json_ld_tag = soup.find("script", type="application/ld+json")
    if json_ld_tag and json_ld_tag.string:
        try:
            payload = json.loads(json_ld_tag.string)
            rating = payload.get("aggregateRating")
            if isinstance(rating, dict):
                rating_value = rating.get("ratingValue")
                rating_count = rating.get("reviewCount") or rating.get("ratingCount")
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass

    return {
        "title": title,
        "author": author,
        "status": status,
        "description": description,
        "cover": cover,
        "categories": genres,
        "genres_full": genres,
        "chapters": inline_chapters,
        "total_chapters_on_site": total_chapters,
        "story_id": story_id,
        "manga_id": story_id,
        "story_path": story_path,
        "rating_value": rating_value,
        "rating_count": rating_count,
        "source": "tangthuvien.net",
        "ajax_nonce": None,
        "chapter_ranges": [],
    }


def parse_chapter_list(html_content: str, base_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    chapters: List[Dict[str, str]] = []
    seen: set[str] = set()

    anchor_candidates = soup.select("ul.cf li a[href]")
    if not anchor_candidates:
        anchor_candidates = soup.select("ul li a[href]")

    for anchor in anchor_candidates:
        parent_ul = anchor.find_parent("ul")
        if parent_ul and "pagination" in (parent_ul.get("class") or []):
            continue
        href = anchor.get("href")
        if not href or href.strip().startswith("javascript"):
            continue
        title = _normalize_text(html.unescape(anchor.get_text(" ", strip=True)))
        if not title:
            continue
        url = urljoin(base_url, href)
        if url in seen:
            continue
        seen.add(url)
        chapters.append({"title": title, "url": url})

    return chapters


def _chapters_to_html_paragraphs(parts: List[str]) -> Optional[str]:
    lines: List[str] = []
    for part in parts:
        for segment in re.split(r"\r?\n+", part):
            cleaned = segment.strip()
            if cleaned:
                lines.append(f"<p>{html.escape(cleaned)}</p>")
    return "".join(lines) if lines else None


def parse_chapter_content(html_content: str) -> Optional[Dict[str, Optional[str]]]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)

    title_tag = soup.select_one("div.chapter h2") or soup.select_one("h2") or soup.select_one("div.chapter h1")
    title = _normalize_text(html.unescape(title_tag.get_text(" ", strip=True))) if title_tag else None

    content_container = soup.select_one("div.chapter-c-content")
    if content_container:
        for selector in [
            ".left-control",
            ".panel-box",
            ".panel-setting",
            ".panel-catalog",
            ".chapter-comment",
            "script",
            "style",
        ]:
            for tag in content_container.select(selector):
                tag.decompose()

    parts: List[str] = []
    if content_container:
        for box in content_container.select("div.box-chap"):
            text = box.get_text("\n", strip=True)
            if text:
                parts.append(text)
        if not parts:
            text = content_container.get_text("\n", strip=True)
            if text:
                parts.append(text)

    content_html = _chapters_to_html_paragraphs(parts) if parts else None

    if not content_html:
        fallback = soup.select_one("div#chapter-content, div.chapter-content")
        if fallback:
            text = fallback.get_text("\n", strip=True)
            if text:
                content_html = _chapters_to_html_paragraphs([text])

    return {"title": title, "content": content_html}
