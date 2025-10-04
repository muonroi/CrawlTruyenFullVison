import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import main


@pytest.mark.asyncio
async def test_run_all_sites_invokes_each_site(monkeypatch):
    called = []

    monkeypatch.setattr(main.app_config, "BASE_URLS", {"site1": "http://s1", "site2": "http://s2"})

    async def fake_run_single_site(site_key, crawl_mode=None):
        called.append((site_key, crawl_mode))

    monkeypatch.setattr(main, "run_single_site", fake_run_single_site)

    await main.run_all_sites(crawl_mode="dummy_mode")

    assert sorted(called) == [("site1", "dummy_mode"), ("site2", "dummy_mode")]


@pytest.mark.asyncio
async def test_run_single_site_default_flow(monkeypatch):
    events = []

    monkeypatch.setattr(main.app_config, "GENRE_BATCH_SIZE", 2, raising=False)
    monkeypatch.setattr(main.app_config, "GENRE_ASYNC_LIMIT", 3, raising=False)
    monkeypatch.setattr(main.app_config, "PROXIES_FILE", "proxies.txt", raising=False)
    monkeypatch.setattr(main.app_config, "FAILED_GENRES_FILE", "failed.json", raising=False)
    monkeypatch.setattr(main.app_config, "RETRY_GENRE_ROUND_LIMIT", 1, raising=False)
    monkeypatch.setattr(main.app_config, "RETRY_SLEEP_SECONDS", 0, raising=False)

    monkeypatch.setattr(main, "load_skipped_stories", lambda: events.append("load_skipped"))
    monkeypatch.setattr(main, "merge_all_missing_workers_to_main", lambda site: events.append(f"merge:{site}"))

    async def fake_initialize(site_key):
        events.append(f"init:{site_key}")
        return "http://homepage/", {"existing": True}

    monkeypatch.setattr(main, "initialize_and_log_setup_with_state", fake_initialize)

    async def fake_start_bg():
        events.append("background:start")

    async def fake_stop_bg():
        events.append("background:stop")

    monkeypatch.setattr(main, "start_missing_background_loop", fake_start_bg)
    monkeypatch.setattr(main, "stop_missing_background_loop", fake_stop_bg)

    run_genres_calls = []

    async def fake_run_genres(site_key, settings, crawl_state):
        run_genres_calls.append((site_key, settings, crawl_state))
        events.append("run_genres")

    monkeypatch.setattr(main, "run_genres", fake_run_genres)

    retry_calls = []

    async def fake_retry_failed_genres(adapter, site_key, settings, shuffle_func):
        retry_calls.append((adapter, site_key, settings, shuffle_func))
        events.append("retry_failed")

    monkeypatch.setattr(main, "retry_failed_genres", fake_retry_failed_genres)

    crawl_missing_calls = []

    async def fake_crawl_all_missing_stories(site_key, homepage_url):
        crawl_missing_calls.append((site_key, homepage_url))
        events.append("crawl_missing")

    monkeypatch.setattr(main, "crawl_all_missing_stories", fake_crawl_all_missing_stories)

    run_retry_passes_mock = AsyncMock(side_effect=lambda site_key: events.append(f"retry_passes:{site_key}"))
    monkeypatch.setattr(main, "run_retry_passes", run_retry_passes_mock)

    monkeypatch.setattr(main, "get_adapter", lambda site_key: f"adapter:{site_key}")

    await main.run_single_site("demo")

    assert events[:3] == ["load_skipped", "merge:demo", "init:demo"]
    assert "background:start" in events
    assert "background:stop" in events
    assert events.index("background:start") < events.index("background:stop")

    assert run_genres_calls and run_genres_calls[0][0] == "demo"
    assert isinstance(run_genres_calls[0][1], main.WorkerSettings)
    assert run_genres_calls[0][2] == {"existing": True}

    assert retry_calls and retry_calls[0][0] == "adapter:demo"
    assert crawl_missing_calls == [("demo", "http://homepage/")]

    run_retry_passes_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_crawler_batches_all_genres(monkeypatch, tmp_path):
    monkeypatch.setattr(main.app_config, "get_state_file", lambda site_key: os.path.join(tmp_path, f"{site_key}.json"), raising=False)
    monkeypatch.setattr(main.app_config, "NUM_CHAPTER_BATCHES", 2, raising=False)

    load_crawl_state_mock = AsyncMock(return_value={"loaded": True})
    monkeypatch.setattr(main, "load_crawl_state", load_crawl_state_mock)

    process_calls = []

    async def fake_process_genre_with_limit(session, genre, crawl_state, adapter, site_key):
        process_calls.append((genre["name"], crawl_state.copy(), site_key))
        return True

    monkeypatch.setattr(main, "process_genre_with_limit", fake_process_genre_with_limit)

    async def fake_smart_delay():
        pass

    monkeypatch.setattr(main, "smart_delay", fake_smart_delay)

    class DummyClientSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(main, "aiohttp", SimpleNamespace(ClientSession=DummyClientSession))

    settings = main.WorkerSettings(
        genre_batch_size=2,
        genre_async_limit=3,
        proxies_file="proxies.txt",
        failed_genres_file="failed.json",
        retry_genre_round_limit=1,
        retry_sleep_seconds=0,
    )

    genres = [
        {"name": "genre1", "url": "http://g1"},
        {"name": "genre2", "url": "http://g2"},
        {"name": "genre3", "url": "http://g3"},
    ]

    await main.run_crawler(adapter="dummy", site_key="demo", genres=genres, settings=settings)

    assert len(process_calls) == len(genres)
    assert all(call[2] == "demo" for call in process_calls)
    assert process_calls[0][1] == {"loaded": True}


@pytest.mark.asyncio
async def test_run_missing_uses_adapter(monkeypatch):
    monkeypatch.setattr(main.app_config, "BASE_URLS", {"demo": "http://example.com"}, raising=False)

    initialize_scraper_mock = AsyncMock()
    monkeypatch.setattr(main, "initialize_scraper", initialize_scraper_mock)

    adapter_instance = object()
    monkeypatch.setattr(main, "get_adapter", lambda site_key: adapter_instance)

    crawl_missing_mock = AsyncMock()
    monkeypatch.setattr(main, "check_and_crawl_missing_all_stories", crawl_missing_mock)

    await main.run_missing("demo")

    initialize_scraper_mock.assert_awaited_once_with("demo")
    crawl_missing_mock.assert_awaited_once_with(adapter_instance, "http://example.com/", site_key="demo")


@pytest.mark.asyncio
async def test_run_single_story_delegates(monkeypatch):
    called = []

    async def fake_crawl(title, site_key, genre_name):
        called.append((title, site_key, genre_name))

    monkeypatch.setattr(main, "crawl_single_story_by_title", fake_crawl)

    await main.run_single_story("Story", site_key="demo", genre_name="action")

    assert called == [("Story", "demo", "action")]


@pytest.mark.asyncio
async def test_process_story_item_detects_missing_chapters(monkeypatch, tmp_path):
    story_folder = tmp_path / "story"
    story_folder.mkdir()

    metadata_content = {
        "title": "Sample Story",
        "url": "http://example.com/story",
        "site_key": "demo",
        "description": "desc",
        "status": "ongoing",
        "source": "source",
        "rating_value": 5,
        "rating_count": 10,
        "total_chapters_on_site": 50,
        "chapter_list": [{"title": f"Chapter {i+1}"} for i in range(50)],
    }

    metadata_file = story_folder / "metadata.json"
    metadata_file.write_text(
        json.dumps(metadata_content),
        encoding="utf-8",
    )

    monkeypatch.setattr(main, "ensure_directory_exists", AsyncMock())
    monkeypatch.setattr(main, "save_story_metadata_file", AsyncMock())
    monkeypatch.setattr(main, "save_crawl_state", AsyncMock())
    monkeypatch.setattr(main, "crawl_all_sources_until_full", AsyncMock(return_value=None))
    monkeypatch.setattr(main, "get_saved_chapters_files", MagicMock(return_value=["0001_Chapter_1.txt"]))
    monkeypatch.setattr(main, "get_real_total_chapters", AsyncMock(return_value=50))
    monkeypatch.setattr(main, "count_dead_chapters", MagicMock(return_value=0))
    monkeypatch.setattr(main, "backup_crawl_state", MagicMock())
    monkeypatch.setattr(main, "clear_specific_state_keys", AsyncMock())
    monkeypatch.setattr(main, "export_chapter_metadata_sync", MagicMock())
    monkeypatch.setattr(main, "send_job", AsyncMock())
    add_missing_mock = AsyncMock()
    monkeypatch.setattr(main, "add_missing_story", add_missing_mock)
    monkeypatch.setattr(main, "_normalize_story_sources", lambda *args, **kwargs: False)
    monkeypatch.setattr(main, "sanitize_filename", lambda title: title.replace(" ", "_"))

    monkeypatch.setattr(main.app_config, "NUM_CHAPTER_BATCHES", 1, raising=False)
    monkeypatch.setattr(main.app_config, "get_state_file", lambda site_key: os.path.join(tmp_path, f"state_{site_key}.json"), raising=False)

    story_data_item = {
        "title": "Sample Story",
        "url": "http://example.com/story",
        "site_key": "demo",
    }

    result = await main.process_story_item(
        session=None,
        story_data_item=story_data_item,
        current_discovery_genre_data={},
        story_global_folder_path=str(story_folder),
        crawl_state={},
        adapter=SimpleNamespace(get_story_details=AsyncMock(return_value={})),
        site_key="demo",
    )

    assert result is False
    add_missing_mock.assert_awaited_once_with("Sample Story", "http://example.com/story", 50, 1)

