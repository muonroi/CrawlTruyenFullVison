import sys, os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import asyncio
import json
import os
from types import SimpleNamespace
import pytest
from unittest.mock import AsyncMock, patch

from workers import crawler_single_missing_chapter, missing_chapter_worker


@pytest.mark.asyncio
async def test_crawl_missing_from_metadata_worker(tmp_path, monkeypatch):
    story_dir = tmp_path / "story"
    story_dir.mkdir()
    missing_path = story_dir / "missing_from_metadata.json"
    metadata_path = story_dir / "metadata.json"
    missing_path.write_text(
        json.dumps([{"title": "c1", "url": "u1"}]), encoding="utf-8"
    )
    metadata = {
        "title": "T",
        "site_key": "dummy",
        "categories": [{"name": "g"}],
        "url": "http://x",
    }
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    called = {}

    async def fake_crawl(*args, **kwargs):
        called["ok"] = True

    monkeypatch.setattr(
        missing_chapter_worker, "crawl_missing_chapters_for_story", fake_crawl
    )
    monkeypatch.setattr(missing_chapter_worker, "get_adapter", lambda sk: object())

    await missing_chapter_worker.crawl_missing_from_metadata_worker(
        str(story_dir), site_key="dummy"
    )

    assert called.get("ok")
    assert not missing_path.exists()


@pytest.mark.asyncio
async def test_crawl_single_story_worker(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    complete_dir = tmp_path / "complete"
    data_dir.mkdir()
    complete_dir.mkdir()

    monkeypatch.setattr(crawler_single_missing_chapter, "DATA_FOLDER", str(data_dir))
    monkeypatch.setattr(
        crawler_single_missing_chapter, "COMPLETED_FOLDER", str(complete_dir)
    )
    monkeypatch.setattr(
        crawler_single_missing_chapter, "PROXIES_FILE", str(tmp_path / "p.txt")
    )
    monkeypatch.setattr(
        crawler_single_missing_chapter, "PROXIES_FOLDER", str(tmp_path / "pf")
    )

    # Patch external async functions
    monkeypatch.setattr(
        crawler_single_missing_chapter,
        "create_proxy_template_if_not_exists",
        AsyncMock(),
    )
    monkeypatch.setattr(crawler_single_missing_chapter, "load_proxies", AsyncMock())
    monkeypatch.setattr(
        crawler_single_missing_chapter, "initialize_scraper", AsyncMock()
    )
    monkeypatch.setattr(
        crawler_single_missing_chapter,
        "get_missing_worker_state_file",
        lambda sk: str(tmp_path / "state.json"),
    )
    monkeypatch.setattr(
        crawler_single_missing_chapter, "load_crawl_state", AsyncMock(return_value={})
    )

    # Prepare story folder with metadata and one chapter file
    slug = "test-story"
    folder = data_dir / slug
    folder.mkdir()
    meta = {
        "title": "Test Story",
        "url": "http://example.com/test-story",
        "site_key": "dummy",
        "categories": [{"name": "c"}],
        "sources": [{"url": "http://example.com/test-story", "site_key": "dummy"}],
        "total_chapters_on_site": 2,
    }
    (folder / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    (folder / "0001_first.txt").write_text("a", encoding="utf-8")

    chapters = [
        {"title": "Chương 1", "url": "u1"},
        {"title": "Chương 2", "url": "u2"},
    ]

    class DummyAdapter:
        async def get_story_details(self, url, title):
            return {}

        async def get_chapter_list(self, url, title, site_key):
            return chapters

    monkeypatch.setattr(
        crawler_single_missing_chapter, "get_adapter", lambda sk: DummyAdapter()
    )

    async def fake_crawl(
        site_key,
        session,
        missing_chapters,
        metadata,
        current_category,
        story_folder,
        crawl_state,
        num_batches,
        state_file,
        adapter=None,
    ):
        for ch in missing_chapters:
            fname = crawler_single_missing_chapter.get_chapter_filename(
                ch["title"], ch["real_num"]
            )
            (folder / fname).write_text("x", encoding="utf-8")

    monkeypatch.setattr(
        crawler_single_missing_chapter, "crawl_missing_chapters_for_story", fake_crawl
    )

    await crawler_single_missing_chapter.crawl_single_story_worker(
        story_url="http://example.com/test-story"
    )

    # After crawl there should be two chapter files and folder moved to completed
    dest_folder = complete_dir / "c" / slug
    assert dest_folder.exists()
    files = list(dest_folder.glob("*.txt"))
    assert len(files) == 2
