from pathlib import Path

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
