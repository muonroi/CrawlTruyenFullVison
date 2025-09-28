from pathlib import Path

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
