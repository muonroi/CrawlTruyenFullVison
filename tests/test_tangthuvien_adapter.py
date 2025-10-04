from pathlib import Path
from typing import List
from unittest.mock import AsyncMock

import pytest

from adapters.tangthuvien_adapter import TangThuVienAdapter

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "site_info" / "tangthuvien"


@pytest.mark.asyncio
async def test_get_chapter_content_returns_html(monkeypatch):
    adapter = TangThuVienAdapter()
    sample_html = (FIXTURE_DIR / "content_chapter.txt").read_text(encoding="utf-8")

    async def fake_fetch(self, url, wait_for_selector=None):
        return sample_html

    monkeypatch.setattr(TangThuVienAdapter, "_fetch_text", fake_fetch)

    content = await adapter.get_chapter_content(
        "https://example.com/chapter-1",
        "Chuong 151",
        "tangthuvien",
    )

    assert isinstance(content, str)
    assert "Vương Dục" in content
    assert "<p>" in content


@pytest.mark.asyncio
async def test_get_chapter_content_handles_missing_content(monkeypatch):
    adapter = TangThuVienAdapter()
    malformed_html = "<html><body><h1>Chapter Title</h1></body></html>"

    async def fake_fetch(self, url, wait_for_selector=None):
        return malformed_html

    monkeypatch.setattr(TangThuVienAdapter, "_fetch_text", fake_fetch)

    content = await adapter.get_chapter_content(
        "https://example.com/chapter-2",
        "Chuong 2",
        "tangthuvien",
    )

    assert content is None


@pytest.mark.asyncio
async def test_get_story_details_normalizes_alias_to_doc_truyen(monkeypatch):
    adapter = TangThuVienAdapter()
    sample_html = (FIXTURE_DIR / "detail_story.txt").read_text(encoding="utf-8")
    requested_urls: List[str] = []

    async def fake_fetch(self, url, wait_for_selector=None):
        requested_urls.append(url)
        return sample_html

    monkeypatch.setattr(TangThuVienAdapter, "_fetch_text", fake_fetch)
    monkeypatch.setattr(
        TangThuVienAdapter,
        "_fetch_chapters_via_api",
        AsyncMock(return_value=[]),
    )

    alias_url = "https://tangthuvien.net/tu-phe-linh-can-bat-dau-van-ma-tu-hanh-tong-phe-linh-can-khai-thuy-van-ma-tu-hanh"
    details = await adapter.get_story_details(alias_url, "Dummy title")

    assert requested_urls, "Expected the adapter to fetch story details"
    normalized = "https://tangthuvien.net/doc-truyen/tu-phe-linh-can-bat-dau-van-ma-tu-hanh-tong-phe-linh-can-khai-thuy-van-ma-tu-hanh"
    assert requested_urls[0] == normalized
    assert details is not None
    assert details["url"] == normalized
    assert details["sources"][0]["url"] == normalized


@pytest.mark.asyncio
async def test_get_genres_falls_back_to_desktop_domain(monkeypatch):
    adapter = TangThuVienAdapter()
    adapter.base_url = "https://m.tangthuvien.net"

    mobile_home = (FIXTURE_DIR / "home.debug.txt").read_text(encoding="utf-8")
    desktop_home = (FIXTURE_DIR / "home.txt").read_text(encoding="utf-8")
    requested_urls: List[str] = []

    async def fake_fetch(self, url, wait_for_selector=None):
        requested_urls.append(url)
        if "m.tangthuvien.net" in url:
            return mobile_home
        return desktop_home

    monkeypatch.setattr(TangThuVienAdapter, "_fetch_text", fake_fetch)

    genres = await adapter.get_genres()

    assert genres, "Expected fallback fetch to yield genres"
    assert len(requested_urls) >= 2
    assert any("tangthuvien.net" in url and "m.tangthuvien.net" not in url for url in requested_urls)
    assert any("/the-loai/" in genre["url"] for genre in genres)


@pytest.mark.asyncio
async def test_get_all_stories_handles_missing_total_pages(monkeypatch):
    adapter = TangThuVienAdapter()

    pages = {
        1: (
            [
                {"title": "Story 1", "url": "https://tangthuvien.net/doc-truyen/story-1"},
            ],
            1,
        ),
        2: (
            [
                {"title": "Story 2", "url": "https://tangthuvien.net/doc-truyen/story-2"},
            ],
            1,
        ),
        3: ([], 1),
    }

    async def fake_get(self, genre_url, page=1):
        return pages.get(page, ([], 0))

    monkeypatch.setattr(TangThuVienAdapter, "get_stories_in_genre", fake_get)

    stories, total_pages, crawled_pages = await adapter.get_all_stories_from_genre_with_page_check(
        genre_name="Tiên Hiệp",
        genre_url="https://tangthuvien.net/the-loai/tien-hiep",
        site_key="tangthuvien",
    )

    assert [story["title"] for story in stories] == ["Story 1", "Story 2"]
    assert total_pages >= 2
    assert crawled_pages == 3


@pytest.mark.asyncio
async def test_get_all_stories_stops_on_duplicates(monkeypatch):
    adapter = TangThuVienAdapter()

    pages = {
        1: (
            [
                {"title": "Story 1", "url": "https://tangthuvien.net/doc-truyen/story-1"},
            ],
            5,
        ),
        2: (
            [
                {"title": "Story 1", "url": "https://tangthuvien.net/doc-truyen/story-1"},
            ],
            5,
        ),
    }

    async def fake_get(self, genre_url, page=1):
        return pages.get(page, ([], 0))

    monkeypatch.setattr(TangThuVienAdapter, "get_stories_in_genre", fake_get)

    stories, total_pages, crawled_pages = await adapter.get_all_stories_from_genre_with_page_check(
        genre_name="Tiên Hiệp",
        genre_url="https://tangthuvien.net/the-loai/tien-hiep",
        site_key="tangthuvien",
    )

    assert len(stories) == 1
    assert total_pages >= 1
    assert crawled_pages == 2
