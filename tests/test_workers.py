
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import asyncio
import json
from types import SimpleNamespace
import pytest
from unittest.mock import AsyncMock, MagicMock

# Import workers after patching
from workers import (
    crawler_missing_chapter,
    crawler_single_missing_chapter,
    retry_failed_chapters,
)


@pytest.fixture
def mock_kafka_producer(monkeypatch):
    """Mocks the Kafka producer to prevent actual message sending."""
    mock_send = AsyncMock()
    monkeypatch.setattr("kafka.kafka_producer.send_job", mock_send)
    return mock_send

@pytest.mark.asyncio
async def test_retry_chapter_worker(monkeypatch):
    """Tests the new message-driven retry chapter worker."""
    # 1. Arrange
    mock_save = AsyncMock(return_value="new")
    monkeypatch.setattr(retry_failed_chapters, "async_save_chapter_with_hash_check", mock_save)
    monkeypatch.setattr(retry_failed_chapters, "initialize_scraper", AsyncMock())

    mock_adapter = MagicMock()
    mock_adapter.get_chapter_content = AsyncMock(return_value="Chapter content")
    monkeypatch.setattr(retry_failed_chapters, "get_adapter", lambda sk: mock_adapter)

    chapter_data = {
        "type": "retry_chapter", "site": "dummy", "chapter_url": "http://example.com/c1",
        "chapter_title": "Chapter 1", "story_title": "My Story",
        "filename": "/tmp/my-story/0001_chapter-1.txt",
        "story_data_item": { "categories": [{"name": "Action"}] }
    }

    # 2. Act
    await retry_failed_chapters.retry_single_chapter(chapter_data)

    # 3. Assert
    mock_adapter.get_chapter_content.assert_called_once_with("http://example.com/c1", "Chapter 1", "dummy")
    mock_save.assert_called_once()
    call_args = mock_save.call_args[0]
    assert call_args[0] == "/tmp/my-story/0001_chapter-1.txt"
    assert "Chapter content" in call_args[1]

@pytest.mark.asyncio
async def test_check_missing_chapters_worker(tmp_path, monkeypatch):
    """Tests the check_missing_chapters flow by calling the worker with a folder path."""
    data_dir = tmp_path / "data"
    complete_dir = tmp_path / "complete"
    data_dir.mkdir(); complete_dir.mkdir()

    monkeypatch.setattr(crawler_single_missing_chapter, "DATA_FOLDER", str(data_dir))
    monkeypatch.setattr(crawler_single_missing_chapter, "COMPLETED_FOLDER", str(complete_dir))
    import config.config as cfg
    monkeypatch.setattr(cfg, "COMPLETED_FOLDER", str(complete_dir))
    import utils.io_utils as io_utils
    monkeypatch.setattr(io_utils, "COMPLETED_FOLDER", str(complete_dir))
    monkeypatch.setattr(crawler_single_missing_chapter, "create_proxy_template_if_not_exists", AsyncMock())
    monkeypatch.setattr(crawler_single_missing_chapter, "load_proxies", AsyncMock())
    monkeypatch.setattr(crawler_single_missing_chapter, "initialize_scraper", AsyncMock())
    monkeypatch.setattr(crawler_single_missing_chapter, "cached_get_story_details", AsyncMock(return_value={}))
    monkeypatch.setattr(crawler_single_missing_chapter, "get_missing_worker_state_file", lambda sk: str(tmp_path / "state.json"))
    monkeypatch.setattr(crawler_single_missing_chapter, "load_crawl_state", AsyncMock(return_value={}))

    slug = "test-story"
    folder = data_dir / slug
    folder.mkdir()
    meta = {
        "title": "Test Story", "url": "http://example.com/test-story", "site_key": "dummy",
        "categories": [{"name": "c"}], "sources": [{"url": "http://example.com/test-story", "site_key": "dummy"}],
        "total_chapters_on_site": 2,
    }
    (folder / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    # Create the file with the name the code expects to avoid rename logic issues
    (folder / "0001_chuong-1-first.txt").write_text("Chapter 1 content", encoding="utf-8")

    chapters_from_web = [{"title": "Chương 1: first", "url": "u1"}, {"title": "Chương 2: second", "url": "u2"}]
    monkeypatch.setattr(crawler_single_missing_chapter, "cached_get_chapter_list", AsyncMock(return_value=chapters_from_web))
    monkeypatch.setattr(crawler_single_missing_chapter, "get_adapter", lambda sk: MagicMock())

    mock_crawl_missing = AsyncMock()
    monkeypatch.setattr(crawler_single_missing_chapter, "crawl_missing_chapters_for_story", mock_crawl_missing)

    await crawler_single_missing_chapter.crawl_single_story_worker(story_folder_path=str(folder))

    mock_crawl_missing.assert_called_once()
    missing_chapters_arg = mock_crawl_missing.call_args[0][2]
    assert len(missing_chapters_arg) == 1
    assert missing_chapters_arg[0]["title"] == "Chương 2: second"

@pytest.mark.asyncio
async def test_crawl_single_story_with_dead_chapter(tmp_path, monkeypatch):
    """Kept test: Verifies that dead chapters are correctly skipped."""
    data_dir = tmp_path / "data"; complete_dir = tmp_path / "complete"
    data_dir.mkdir(); complete_dir.mkdir()

    monkeypatch.setattr(crawler_single_missing_chapter, "DATA_FOLDER", str(data_dir))
    monkeypatch.setattr(crawler_single_missing_chapter, "COMPLETED_FOLDER", str(complete_dir))
    import config.config as cfg
    monkeypatch.setattr(cfg, "COMPLETED_FOLDER", str(complete_dir))
    import utils.io_utils as io_utils
    monkeypatch.setattr(io_utils, "COMPLETED_FOLDER", str(complete_dir))
    monkeypatch.setattr(crawler_single_missing_chapter, "create_proxy_template_if_not_exists", AsyncMock())
    monkeypatch.setattr(crawler_single_missing_chapter, "load_proxies", AsyncMock())
    monkeypatch.setattr(crawler_single_missing_chapter, "initialize_scraper", AsyncMock())
    monkeypatch.setattr(crawler_single_missing_chapter, "cached_get_story_details", AsyncMock(return_value={}))
    monkeypatch.setattr(crawler_single_missing_chapter, "get_missing_worker_state_file", lambda sk: str(tmp_path / "state.json"))
    monkeypatch.setattr(crawler_single_missing_chapter, "load_crawl_state", AsyncMock(return_value={}))

    slug = "test-story"
    folder = data_dir / slug
    folder.mkdir()
    meta = {
        "title": "Test Story", "url": "http://example.com/test-story", "site_key": "dummy",
        "categories": [{"name": "c"}], "sources": [{"url": "http://example.com/test-story", "site_key": "dummy"}],
        "total_chapters_on_site": 2,
    }
    (folder / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    # Create the file with the name the code expects
    (folder / "0001_chuong-1.txt").write_text("a", encoding="utf-8")
    (folder / "dead_chapters.json").write_text(json.dumps([{"index": 2, "title": "c2", "url": "u2"}]), encoding="utf-8")

    chapters = [{"title": "Chương 1", "url": "u1"}, {"title": "Chương 2", "url": "u2"}]
    monkeypatch.setattr(crawler_single_missing_chapter, "cached_get_chapter_list", AsyncMock(return_value=chapters))
    monkeypatch.setattr(crawler_single_missing_chapter, "get_adapter", lambda sk: MagicMock())

    mock_crawl_missing = AsyncMock()
    monkeypatch.setattr(crawler_single_missing_chapter, "crawl_missing_chapters_for_story", mock_crawl_missing)

    # Call with folder_path instead of url
    await crawler_single_missing_chapter.crawl_single_story_worker(story_folder_path=str(folder))

    # The story should be considered complete (1 downloaded + 1 dead)
    dest_folder = complete_dir / "c" / slug
    assert dest_folder.exists()
    files = list(dest_folder.glob("*.txt"))
    assert len(files) == 1
    # And the crawl function should not have been called because the dead chapter is skipped
    mock_crawl_missing.assert_not_called()


@pytest.mark.asyncio
async def test_missing_crawler_fetches_genres(monkeypatch, tmp_path):
    """Ensure the missing crawler queries genres instead of genre stories."""

    site_key = "demo"
    data_dir = tmp_path / "data"
    completed_dir = tmp_path / "completed"
    proxies_dir = tmp_path / "proxies"

    data_dir.mkdir()
    completed_dir.mkdir()
    proxies_dir.mkdir()

    monkeypatch.setattr(crawler_missing_chapter, "DATA_FOLDER", str(data_dir))
    monkeypatch.setattr(crawler_missing_chapter, "COMPLETED_FOLDER", str(completed_dir))
    monkeypatch.setattr(crawler_missing_chapter, "PROXIES_FOLDER", str(proxies_dir))
    monkeypatch.setattr(crawler_missing_chapter, "PROXIES_FILE", str(tmp_path / "proxies.txt"))
    monkeypatch.setattr(crawler_missing_chapter, "BASE_URLS", {site_key: "http://example.com"})
    monkeypatch.setattr(crawler_missing_chapter, "LOADED_PROXIES", [])

    monkeypatch.setattr(
        crawler_missing_chapter,
        "create_proxy_template_if_not_exists",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        crawler_missing_chapter, "load_proxies", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        crawler_missing_chapter, "initialize_scraper", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        crawler_missing_chapter, "cached_get_story_details", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        crawler_missing_chapter, "cached_get_chapter_list", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        crawler_missing_chapter, "get_missing_worker_state_file", lambda sk: str(tmp_path / "state.json")
    )
    monkeypatch.setattr(
        crawler_missing_chapter, "load_crawl_state", AsyncMock(return_value={})
    )
    monkeypatch.setattr(crawler_missing_chapter, "count_txt_files", lambda folder: 0)
    monkeypatch.setattr(crawler_missing_chapter, "count_dead_chapters", lambda folder: 0)
    monkeypatch.setattr(
        crawler_missing_chapter, "move_story_to_completed", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        crawler_missing_chapter, "get_real_total_chapters", AsyncMock(return_value=0)
    )
    monkeypatch.setattr(
        crawler_missing_chapter, "send_telegram_notify", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        crawler_missing_chapter, "get_missing_chapters", lambda folder, chapters, site_key: []
    )
    monkeypatch.setattr(
        crawler_missing_chapter,
        "crawl_missing_chapters_for_story",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        crawler_missing_chapter, "smart_delay", AsyncMock(return_value=None)
    )

    story_dir = data_dir / "demo-story"
    story_dir.mkdir()
    metadata = {
        "title": "Demo Story",
        "url": "http://example.com/demo-story",
        "site_key": site_key,
        "categories": [{"name": "Action"}],
        "sources": [],
        "total_chapters_on_site": 0,
    }
    (story_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

    class DummyAdapter:
        def __init__(self):
            self.get_genres_called = False

        async def get_genres(self):
            self.get_genres_called = True
            return [{"name": "Action", "url": "http://example.com/genre/action"}]

        async def get_all_stories_from_genre(self, genre_name, genre_url):
            return []

    adapter = DummyAdapter()
    monkeypatch.setattr(crawler_missing_chapter, "get_adapter", lambda sk: adapter)

    await crawler_missing_chapter.check_and_crawl_missing_all_stories(
        adapter=None,
        home_page_url="http://example.com",
        site_key=site_key,
    )

    assert adapter.get_genres_called is True

@pytest.mark.asyncio
async def test_main_sends_kafka_job(tmp_path, monkeypatch, mock_kafka_producer):
    """Tests that the main crawler sends a kafka job after processing a story."""
    # This is a simplified integration test for the producer side
    from main import process_story_item

    # Arrange: Mock dependencies of process_story_item individually
    monkeypatch.setattr("main.ensure_directory_exists", AsyncMock())
    monkeypatch.setattr("main.logger", MagicMock())
    monkeypatch.setattr("main.save_story_metadata_file", AsyncMock())
    monkeypatch.setattr("main.save_crawl_state", AsyncMock())
    monkeypatch.setattr("main.crawl_all_sources_until_full", AsyncMock())
    monkeypatch.setattr("main.get_saved_chapters_files", MagicMock(return_value={"file1.txt"}))
    real_total_mock = AsyncMock(return_value=1)
    monkeypatch.setattr("main.get_real_total_chapters", real_total_mock)
    monkeypatch.setattr("main.count_dead_chapters", MagicMock(return_value=0))
    monkeypatch.setattr("main.backup_crawl_state", MagicMock())
    monkeypatch.setattr("main.clear_specific_state_keys", AsyncMock())
    monkeypatch.setattr("main.export_chapter_metadata_sync", MagicMock())
    monkeypatch.setattr("main.send_job", mock_kafka_producer) # Use the mocked producer

    story_folder = str(tmp_path / "test-story")

    # Act
    await process_story_item(
        session=None,
        story_data_item={"title": "Test Story", "url": "http://example.com/story"},
        current_discovery_genre_data={},
        story_global_folder_path=story_folder,
        crawl_state={},
        adapter=SimpleNamespace(get_story_details=AsyncMock(return_value={})),
        site_key="dummy"
    )

    mock_kafka_producer.assert_awaited_once_with({
        "type": "check_missing_chapters",
        "story_folder_path": story_folder
    })

    # Khi chưa crawl đủ chương so với thực tế → không gửi job
    mock_kafka_producer.reset_mock()
    real_total_mock.return_value = 5

    await process_story_item(
        session=None,
        story_data_item={"title": "Test Story", "url": "http://example.com/story"},
        current_discovery_genre_data={},
        story_global_folder_path=story_folder,
        crawl_state={},
        adapter=SimpleNamespace(get_story_details=AsyncMock(return_value={})),
        site_key="dummy"
    )

    mock_kafka_producer.assert_not_called()

# Kept original test for main logic, as it's still relevant
@pytest.mark.asyncio
async def test_process_genre_item_batches(tmp_path, monkeypatch):
    from main import process_genre_item

    genre = {"name": "g", "url": "http://g"}
    stories = [{"title": f"s{i}", "url": f"u{i}"} for i in range(5)]

    adapter = SimpleNamespace(
        get_all_stories_from_genre_with_page_check=AsyncMock(return_value=(stories, 1, 1)),
        get_story_details=AsyncMock(return_value={}),
    )

    monkeypatch.setattr("main.STORY_BATCH_SIZE", 2)
    monkeypatch.setattr("main.DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr("main.slugify_title", lambda t: t)
    monkeypatch.setattr("main.ensure_directory_exists", AsyncMock())
    monkeypatch.setattr("main.save_crawl_state", AsyncMock())
    monkeypatch.setattr("main.clear_specific_state_keys", AsyncMock())

    called = []
    async def fake_process_story(session, story, g, folder, state, ad, sk):
        called.append(story["title"])
        return True
    monkeypatch.setattr("main.process_story_item", fake_process_story)

    await process_genre_item(None, genre, {}, adapter, "dummy")

    assert called == [f"s{i}" for i in range(5)]


@pytest.mark.asyncio
async def test_failed_download_sends_retry_job(monkeypatch, mock_kafka_producer):
    """Tests that a failed chapter download correctly sends a 'retry_chapter' job to Kafka."""
    # 1. Arrange
    from utils.chapter_utils import async_download_and_save_chapter

    # Mock the adapter to simulate a download failure
    mock_adapter = MagicMock()
    mock_adapter.get_chapter_content = AsyncMock(return_value=None)
    monkeypatch.setattr("utils.chapter_utils.log_error_chapter", AsyncMock()) # Mock logger to avoid file IO

    # Prepare dummy data for the function call
    chapter_info = {"url": "http://fail.com/c1", "title": "Failed Chapter"}
    story_data = {"title": "My Story", "url": "http://fail.com/story"}
    filename = "/tmp/story/0001_failed.txt"
    site_key = "dummy_site"

    # 2. Act
    await async_download_and_save_chapter(
        chapter_info=chapter_info,
        story_data_item=story_data,
        current_discovery_genre_data={},
        chapter_filename_full_path=filename,
        chapter_filename_only="0001_failed.txt",
        pass_description="test_pass",
        chapter_display_idx_log="1/1",
        crawl_state={},
        successfully_saved=set(),
        failed_list=[],
        site_key=site_key,
        adapter=mock_adapter
    )

    # 3. Assert
    # Verify that our mock Kafka producer was called
    mock_kafka_producer.assert_called_once()
    
    # Verify the content of the job
    call_args = mock_kafka_producer.call_args[0][0]
    assert call_args["type"] == "retry_chapter"
    assert call_args["chapter_url"] == "http://fail.com/c1"
    assert call_args["story_title"] == "My Story"
    assert call_args["site"] == site_key
