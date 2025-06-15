import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import asyncio
import json
from types import SimpleNamespace
import types
import pytest
from unittest.mock import AsyncMock

# Patch heavy optional deps before importing workers modules
sys.modules.setdefault(
    "playwright.async_api",
    types.SimpleNamespace(async_playwright=None, Browser=None, BrowserContext=None),
)
sys.modules.setdefault("adapters.truyenfull_adapter", types.SimpleNamespace(TruyenFullAdapter=object))

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
    import config.config as cfg
    monkeypatch.setattr(cfg, "COMPLETED_FOLDER", str(complete_dir))
    import utils.io_utils as io_utils
    monkeypatch.setattr(io_utils, "COMPLETED_FOLDER", str(complete_dir))
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


@pytest.mark.asyncio
async def test_crawl_single_story_with_dead_chapter(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    complete_dir = tmp_path / "complete"
    data_dir.mkdir()
    complete_dir.mkdir()

    monkeypatch.setattr(crawler_single_missing_chapter, "DATA_FOLDER", str(data_dir))
    monkeypatch.setattr(crawler_single_missing_chapter, "COMPLETED_FOLDER", str(complete_dir))
    import config.config as cfg
    monkeypatch.setattr(cfg, "COMPLETED_FOLDER", str(complete_dir))
    import utils.io_utils as io_utils
    monkeypatch.setattr(io_utils, "COMPLETED_FOLDER", str(complete_dir))
    monkeypatch.setattr(crawler_single_missing_chapter, "PROXIES_FILE", str(tmp_path / "p.txt"))
    monkeypatch.setattr(crawler_single_missing_chapter, "PROXIES_FOLDER", str(tmp_path / "pf"))

    monkeypatch.setattr(crawler_single_missing_chapter, "create_proxy_template_if_not_exists", AsyncMock())
    monkeypatch.setattr(crawler_single_missing_chapter, "load_proxies", AsyncMock())
    monkeypatch.setattr(crawler_single_missing_chapter, "initialize_scraper", AsyncMock())
    monkeypatch.setattr(crawler_single_missing_chapter, "get_missing_worker_state_file", lambda sk: str(tmp_path / "state.json"))
    monkeypatch.setattr(crawler_single_missing_chapter, "load_crawl_state", AsyncMock(return_value={}))

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

    # Mark second chapter as dead
    dead = [{"index": 2, "title": "c2", "url": "u2"}]
    (folder / "dead_chapters.json").write_text(json.dumps(dead), encoding="utf-8")

    chapters = [
        {"title": "Chương 1", "url": "u1"},
        {"title": "Chương 2", "url": "u2"},
    ]

    class DummyAdapter:
        async def get_story_details(self, url, title):
            return {}

        async def get_chapter_list(self, url, title, site_key):
            return chapters

    monkeypatch.setattr(crawler_single_missing_chapter, "get_adapter", lambda sk: DummyAdapter())

    async def no_crawl(*args, **kwargs):
        pass

    monkeypatch.setattr(crawler_single_missing_chapter, "crawl_missing_chapters_for_story", no_crawl)

    await crawler_single_missing_chapter.crawl_single_story_worker(
        story_url="http://example.com/test-story"
    )

    dest_folder = complete_dir / "c" / slug
    assert dest_folder.exists()
    files = list(dest_folder.glob("*.txt"))
    assert len(files) == 1


def test_process_genre_item_batches(tmp_path, monkeypatch):
    import types, sys
    monkeypatch.setitem(sys.modules, "aiogram", types.SimpleNamespace(Router=object))
    import main

    genre = {"name": "g", "url": "http://g"}
    stories = [{"title": f"s{i}", "url": f"u{i}"} for i in range(5)]

    async def fake_get_all(name, url, sk, mp):
        return stories, 1, 1

    adapter = SimpleNamespace(
        get_all_stories_from_genre_with_page_check=fake_get_all,
        get_story_details=AsyncMock(return_value={}),
    )

    monkeypatch.setattr(main, "STORY_BATCH_SIZE", 2)
    monkeypatch.setattr(main, "DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(main, "slugify_title", lambda t: t)
    monkeypatch.setattr(main, "ensure_directory_exists", AsyncMock())
    monkeypatch.setattr(main, "save_story_metadata_file", AsyncMock())
    monkeypatch.setattr(main, "save_crawl_state", AsyncMock())
    monkeypatch.setattr(main, "clear_specific_state_keys", AsyncMock())

    called = []

    async def fake_process_story(session, story, g, folder, state, ad, sk):
        called.append(story["title"])
        return True

    monkeypatch.setattr(main, "process_story_item", fake_process_story)

    asyncio.run(main.process_genre_item(None, genre, {}, adapter, "dummy"))

    assert called == [f"s{i}" for i in range(5)]
