import json
import os
import time
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest

import main
from core.crawl_planner import CategoryCrawlPlan, CrawlPlan
from core.category_store import SnapshotInfo


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

    async def fake_process_genre_with_limit(session, category, crawl_state, adapter, site_key, **kwargs):
        process_calls.append((category.name, crawl_state.copy(), site_key))
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

    send_job_mock = AsyncMock()
    monkeypatch.setattr(main, "send_job", send_job_mock)

    genres = [
        {"name": "genre1", "url": "http://g1"},
        {"name": "genre2", "url": "http://g2"},
        {"name": "genre3", "url": "http://g3"},
    ]

    crawl_plan = CrawlPlan(site_key="demo")
    for genre in genres:
        crawl_plan.add_category(
            CategoryCrawlPlan(
                name=genre["name"],
                url=genre["url"],
                stories=[{"title": f"{genre['name']} Story", "url": genre["url"] + "/story"}],
                raw_genre=dict(genre),
            )
        )

    async def fake_build_crawl_plan(adapter, *, genres=None, max_pages=None):
        return crawl_plan

    monkeypatch.setattr(main, "build_crawl_plan", fake_build_crawl_plan)

    class DummyStore:
        def __init__(self):
            self.calls = []

        def persist_snapshot(self, site_key, plan, *, version=None):
            self.calls.append((site_key, plan, version))
            return SnapshotInfo(id=1, site_key=site_key, version="v-test", created_at="2024-01-01 00:00:00")

    dummy_store = DummyStore()
    monkeypatch.setattr(main, "category_store", dummy_store)

    await main.run_crawler(adapter="dummy", site_key="demo", genres=genres, settings=settings)

    assert len(process_calls) == len(genres)
    assert all(call[2] == "demo" for call in process_calls)
    assert process_calls[0][0] == "genre1"
    first_state = process_calls[0][1]
    assert first_state["loaded"] is True
    assert "category_story_plan" in first_state
    assert first_state["category_story_plan"]["genre1"][0]["title"] == "genre1 Story"
    assert "category_change_detector" in first_state
    assert "demo" in first_state["category_change_detector"]
    assert "category_snapshots" in first_state
    assert first_state["category_snapshots"]["demo"]["version"] == "v-test"
    assert dummy_store.calls and dummy_store.calls[0][0] == "demo"
    send_job_mock.assert_not_called()


@pytest.mark.asyncio
async def test_run_crawler_schedules_refresh_on_large_change(monkeypatch, tmp_path):
    monkeypatch.setattr(main.app_config, "get_state_file", lambda site_key: os.path.join(tmp_path, f"{site_key}.json"), raising=False)
    monkeypatch.setattr(main.app_config, "NUM_CHAPTER_BATCHES", 2, raising=False)
    monkeypatch.setattr(main.app_config, "CATEGORY_CHANGE_REFRESH_RATIO", 0.1, raising=False)
    monkeypatch.setattr(main.app_config, "CATEGORY_CHANGE_REFRESH_ABSOLUTE", 1, raising=False)
    monkeypatch.setattr(main.app_config, "CATEGORY_CHANGE_MIN_STORIES", 1, raising=False)
    monkeypatch.setattr(main.app_config, "CATEGORY_REFRESH_BATCH_SIZE", 2, raising=False)

    baseline_stories = [
        {"title": "Story A", "url": "http://g1/story-a"},
        {"title": "Story B", "url": "http://g1/story-b"},
    ]

    from core.category_change_detector import CategoryChangeDetector

    detector = CategoryChangeDetector(ratio_threshold=0.1, absolute_threshold=1, min_story_count=1)
    baseline_result = detector.evaluate(baseline_stories, None)

    previous_state = {
        "loaded": True,
        "category_change_detector": {
            "demo": {
                "cat-1": {
                    "url_checksum": baseline_result.signature.url_checksum,
                    "content_signature": baseline_result.signature.content_signature,
                    "urls": baseline_result.urls,
                    "story_count": baseline_result.signature.story_count,
                }
            }
        },
        "category_story_plan": {"Genre": baseline_stories},
    }

    load_crawl_state_mock = AsyncMock(return_value=previous_state)
    monkeypatch.setattr(main, "load_crawl_state", load_crawl_state_mock)

    process_calls: List[tuple[str, Dict[str, Any], str]] = []

    async def fake_process_genre_with_limit(session, category, crawl_state, adapter, site_key, **kwargs):
        process_calls.append((category.name, crawl_state.copy(), site_key))
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

    changed_stories = [
        {"title": "Story C", "url": "http://g1/story-c"},
        {"title": "Story D", "url": "http://g1/story-d"},
        {"title": "Story E", "url": "http://g1/story-e"},
    ]

    crawl_plan = CrawlPlan(site_key="demo")
    crawl_plan.add_category(
        CategoryCrawlPlan(
            name="Genre",
            url="http://g1",
            stories=list(changed_stories),
            raw_genre={"name": "Genre", "url": "http://g1"},
            metadata={"category_id": "cat-1"},
        )
    )

    async def fake_build_crawl_plan(adapter, *, genres=None, max_pages=None):
        return crawl_plan

    monkeypatch.setattr(main, "build_crawl_plan", fake_build_crawl_plan)

    class DummyStore:
        def persist_snapshot(self, site_key, plan, *, version=None):
            return SnapshotInfo(id=2, site_key=site_key, version="v-test", created_at="2024-02-01 00:00:00")

    monkeypatch.setattr(main, "category_store", DummyStore())

    send_job_mock = AsyncMock()
    monkeypatch.setattr(main, "send_job", send_job_mock)

    save_crawl_state_mock = AsyncMock()
    monkeypatch.setattr(main, "save_crawl_state", save_crawl_state_mock)

    await main.run_crawler(adapter="dummy", site_key="demo", genres=[{"name": "Genre", "url": "http://g1"}], settings=settings)

    assert send_job_mock.await_count == 2
    for call in send_job_mock.await_args_list:
        payload = call.args[0]
        assert payload["type"] == "category_batch_refresh"
        assert payload["category_id"] == "cat-1"
    save_crawl_state_mock.assert_awaited()
    assert process_calls


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


@pytest.mark.asyncio
async def test_process_story_item_respects_story_cooldown(monkeypatch, tmp_path):
    story_folder = tmp_path / "story"
    story_folder.mkdir()

    monkeypatch.setattr(main, "ensure_directory_exists", AsyncMock())
    monkeypatch.setattr(main, "save_story_metadata_file", AsyncMock())
    monkeypatch.setattr(main, "save_crawl_state", AsyncMock())
    monkeypatch.setattr(main, "crawl_all_sources_until_full", AsyncMock())
    monkeypatch.setattr(main, "get_saved_chapters_files", MagicMock(return_value=set()))
    monkeypatch.setattr(main, "get_real_total_chapters", AsyncMock(return_value=0))
    monkeypatch.setattr(main, "count_dead_chapters", MagicMock(return_value=0))
    monkeypatch.setattr(main, "backup_crawl_state", MagicMock())
    monkeypatch.setattr(main, "clear_specific_state_keys", AsyncMock())
    monkeypatch.setattr(main, "export_chapter_metadata_sync", MagicMock())
    monkeypatch.setattr(main, "send_job", AsyncMock())
    monkeypatch.setattr(main, "_normalize_story_sources", lambda *args, **kwargs: False)

    future_time = time.time() + 120
    story_data_item = {
        "title": "Cool Story",
        "url": "http://example.com/story",
        "_cooldown_until": future_time,
    }

    crawl_state: Dict[str, Any] = {}

    result = await main.process_story_item(
        session=None,
        story_data_item=story_data_item,
        current_discovery_genre_data={},
        story_global_folder_path=str(story_folder),
        crawl_state=crawl_state,
        adapter=SimpleNamespace(get_story_details=AsyncMock(return_value={})),
        site_key="demo",
    )

    assert result is False
    cooldowns = crawl_state.get("story_cooldowns", {})
    assert pytest.approx(cooldowns.get("http://example.com/story"), rel=0.01) == future_time


@pytest.mark.asyncio
async def test_process_genre_item_retries_and_skips(monkeypatch, tmp_path, caplog):
    caplog.set_level("WARNING")

    monkeypatch.setattr(main.app_config, "MAX_STORIES_PER_GENRE_PAGE", 2, raising=False)
    monkeypatch.setattr(main.app_config, "MAX_STORIES_TOTAL_PER_GENRE", 0, raising=False)
    monkeypatch.setattr(main.app_config, "RETRY_STORY_ROUND_LIMIT", 2, raising=False)
    monkeypatch.setattr(
        main.app_config,
        "get_state_file",
        lambda site_key: str(tmp_path / f"{site_key}.json"),
        raising=False,
    )

    save_state_mock = AsyncMock()
    monkeypatch.setattr(main, "save_crawl_state", save_state_mock)

    smart_delay_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(main, "smart_delay", smart_delay_mock)

    ensure_dir_mock = AsyncMock()
    monkeypatch.setattr(main, "ensure_directory_exists", ensure_dir_mock)

    process_story_mock = AsyncMock(side_effect=[False, True])
    monkeypatch.setattr(main, "process_story_item", process_story_mock)

    monkeypatch.setattr(main, "load_skipped_stories", MagicMock())
    monkeypatch.setattr(
        main,
        "is_story_skipped",
        lambda story: story["title"] == "Skip me",
    )

    monkeypatch.setattr(main, "log_failed_genre", MagicMock())

    main.DATA_FOLDER = str(tmp_path)
    main.STORY_BATCH_SIZE = 2

    stories = [
        {"title": "Skip me", "url": "https://example.com/skip"},
        {"title": "Process me", "url": "https://example.com/process"},
    ]

    async def fake_get_all(*args, page_callback=None, collect=True, **kwargs):
        if page_callback:
            await page_callback(list(stories), 1, 1)
        return (list(stories) if collect else [], 1, 1)

    adapter = SimpleNamespace(
        get_all_stories_from_genre_with_page_check=AsyncMock(side_effect=fake_get_all),
        get_story_details=AsyncMock(return_value={}),
    )

    crawl_state = {}
    genre_data = {"name": "Tiên Hiệp", "url": "https://example.com/the-loai"}

    await main.process_genre_item(None, genre_data, crawl_state, adapter, "demo")

    assert adapter.get_all_stories_from_genre_with_page_check.await_count == 1
    assert smart_delay_mock.await_count >= 1

    assert any("[SKIP]" in rec.message for rec in caplog.records)

    assert process_story_mock.await_count == 2
    assert ensure_dir_mock.await_count >= 2

    assert sorted(crawl_state.get("globally_completed_story_urls", [])) == sorted(
        ["https://example.com/process", "https://example.com/skip"]
    )

    # save_crawl_state should be called at least twice: before fetching and after processing
    assert save_state_mock.await_count >= 2

