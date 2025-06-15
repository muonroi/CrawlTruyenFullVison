import asyncio
import json
from types import SimpleNamespace
import os
import sys
import pytest
import types

sys.modules.setdefault(
    "playwright.async_api",
    types.SimpleNamespace(async_playwright=None, Browser=None, BrowserContext=None),
)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import chapter_utils

@pytest.mark.asyncio
async def test_crawl_missing_retry_limit(tmp_path, monkeypatch):
    folder = tmp_path / "story"
    folder.mkdir()

    chapters = [
        {"title": "c1", "url": "u1"},
        {"title": "c2", "url": "u2"},
    ]
    meta = {"title": "S", "total_chapters_on_site": 2}

    async def fail_download(*args, **kwargs):
        raise Exception("fail")

    async def fake_mark(*args, **kwargs):
        pass

    monkeypatch.setattr(chapter_utils, "async_download_and_save_chapter", fail_download)
    monkeypatch.setattr(chapter_utils, "smart_delay", lambda *a, **k: asyncio.sleep(0))
    monkeypatch.setattr(chapter_utils, "mark_dead_chapter", fake_mark)
    monkeypatch.setattr(chapter_utils, "MAX_CHAPTER_RETRY", 1)

    await chapter_utils.crawl_missing_chapters_for_story(
        "dummy",
        None,
        chapters,
        meta,
        {},
        str(folder),
        {},
        num_batches=1,
        adapter=SimpleNamespace(),
    )

    report = folder / "missing_permanent.json"
    assert report.exists()
    data = json.loads(report.read_text(encoding="utf-8"))
    assert len(data) == 2
