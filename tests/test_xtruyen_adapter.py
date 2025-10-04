from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from adapters.xtruyen_adapter import XTruyenAdapter


@pytest.mark.asyncio
async def test_get_chapter_content_returns_html(monkeypatch):
    adapter = XTruyenAdapter()
    sample_html = Path('tmp_data/xtruyen/content_chapter.txt').read_text(encoding='utf-8')

    async def fake_fetch(self, url, wait_for_selector=None):
        return sample_html

    monkeypatch.setattr(XTruyenAdapter, '_fetch_text', fake_fetch)

    content = await adapter.get_chapter_content('https://example.com/chapter-1', 'Chương 1', 'xtruyen')

    assert isinstance(content, str)
    assert 'Nửa đêm canh ba' in content
    assert '<p>' in content

@pytest.mark.asyncio
async def test_get_chapter_content_handles_missing_content_gracefully(monkeypatch):
    """Tests that get_chapter_content returns None when the content selector is missing."""
    adapter = XTruyenAdapter()
    # This HTML is valid but lacks the #chapter-reading-content div
    malformed_html = "<html><body><h1>Chapter Title</h1><p>Some unrelated text.</p></body></html>"

    async def fake_fetch(self, url, wait_for_selector=None):
        # We need to return a string, not a mock object with a .text attribute
        return malformed_html

    monkeypatch.setattr(XTruyenAdapter, '_fetch_text', fake_fetch)

    content = await adapter.get_chapter_content('https://example.com/chapter-2', 'Chương 2', 'xtruyen')

    assert content is None


@pytest.mark.asyncio
async def test_get_genres_and_stories_with_pagination(monkeypatch):
    adapter = XTruyenAdapter()

    home_html = Path('site_info/xtruyen/home.txt').read_text(encoding='utf-8')
    category_html = Path('site_info/xtruyen/category.txt').read_text(encoding='utf-8')
    extra_title = 'Truyện Trang Hai'
    page_two_html = f'''
    <html><body>
    <div class="popular-item-wrap">
        <h5 class="widget-title"><a href="/truyen/trang-hai" title="{extra_title}">{extra_title}</a></h5>
    </div>
    <ul class="pagination">
        <li><a class="page-link" href="?page=1">1</a></li>
        <li><a class="page-link" href="?page=2">2</a></li>
    </ul>
    </body></html>
    '''

    async def fake_fetch(self, url, wait_for_selector=None):
        if url == adapter.base_url:
            return home_html
        if 'page=2' in url or url.rstrip('/').endswith('/page/2'):
            return page_two_html
        return category_html

    monkeypatch.setattr(XTruyenAdapter, '_fetch_text', fake_fetch)
    sleep_mock = AsyncMock()
    monkeypatch.setattr('adapters.xtruyen_adapter.asyncio.sleep', sleep_mock)

    genres = await adapter.get_genres()
    assert genres, 'Expected at least one genre'
    assert any(g['name'] == 'Tiên Hiệp' for g in genres)

    genre_url = 'https://xtruyen.vn/the-loai/tien-hiep/'
    stories_page1, total_pages = await adapter.get_stories_in_genre(genre_url)
    assert stories_page1, 'Expected first page stories'
    assert total_pages >= 2

    all_stories, total, crawled = await adapter.get_all_stories_from_genre_with_page_check(
        'Tiên Hiệp',
        genre_url,
        site_key='xtruyen',
        max_pages=2,
    )

    assert total == total_pages
    assert crawled == 2
    assert len(all_stories) == len(stories_page1) + 1
    assert any(story['title'] == extra_title for story in all_stories)
    sleep_mock.assert_awaited()
