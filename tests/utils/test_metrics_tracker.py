import json
import time

from utils.metrics_tracker import CrawlMetricsTracker


def test_metrics_tracker_story_lifecycle(monkeypatch, tmp_path):
    dashboard = tmp_path / "dashboard.json"
    monkeypatch.setenv("STORYFLOW_DASHBOARD_FILE", str(dashboard))

    tracker = CrawlMetricsTracker()
    tracker.story_started(
        "story-1",
        "Truyện A",
        120,
        primary_site="site-a",
        genre_name="Tiên Hiệp",
        genre_url="https://example.com/genre/tien-hiep",
        genre_site_key="site-a",
    )
    tracker.update_story_progress(
        "story-1",
        crawled_chapters=10,
        missing_chapters=110,
        last_source="site-a",
    )
    cooldown_until = time.time() + 60
    tracker.story_on_cooldown("story-1", cooldown_until)
    tracker.story_completed("story-1")
    tracker.update_skipped_queue_size(3)
    tracker.update_site_health("site-a", success_delta=5)
    tracker.update_site_health("site-a", failure_delta=1, last_error="timeout")

    snapshot = tracker.get_snapshot()
    assert snapshot["aggregates"]["stories_completed"] == 1
    assert snapshot["aggregates"]["stories_in_progress"] == 0
    assert snapshot["aggregates"]["skipped_queue_size"] == 3
    assert snapshot["aggregates"]["total_missing_chapters"] == 0

    sites = snapshot["sites"]
    assert sites[0]["site_key"] == "site-a"
    assert sites[0]["success"] == 5
    assert sites[0]["failure"] == 1

    completed_story = snapshot["stories"]["completed"][0]
    assert completed_story["genre_name"] == "Tiên Hiệp"
    assert completed_story["genre_url"] == "https://example.com/genre/tien-hiep"
    assert completed_story["genre_site_key"] == "site-a"

    with open(dashboard, "r", encoding="utf-8") as f:
        persisted = json.load(f)
    assert persisted["aggregates"]["stories_completed"] == 1


def test_metrics_tracker_story_skipped(monkeypatch, tmp_path):
    dashboard = tmp_path / "dashboard_skipped.json"
    monkeypatch.setenv("STORYFLOW_DASHBOARD_FILE", str(dashboard))

    tracker = CrawlMetricsTracker()
    tracker.story_started(
        "story-2",
        "Truyện B",
        50,
        primary_site="site-b",
        genre_name="Huyền Ảo",
        genre_url="https://example.com/genre/huyen-ao",
        genre_site_key="site-b",
    )
    tracker.story_skipped("story-2", "Truyện B", "anti_bot")

    snapshot = tracker.get_snapshot()
    assert snapshot["aggregates"]["stories_skipped"] == 1
    skipped_story = snapshot["stories"]["skipped"][0]
    assert skipped_story["last_error"] == "anti_bot"
    assert skipped_story["genre_name"] == "Huyền Ảo"
    assert skipped_story["genre_url"] == "https://example.com/genre/huyen-ao"
    assert skipped_story["genre_site_key"] == "site-b"

    tracker.story_failed("story-2", "retry_limit")
    assert tracker.get_snapshot()["stories"]["skipped"][0]["last_error"] == "anti_bot"


def test_metrics_tracker_genre_tracking(monkeypatch, tmp_path):
    dashboard = tmp_path / "dashboard_genres.json"
    monkeypatch.setenv("STORYFLOW_DASHBOARD_FILE", str(dashboard))

    tracker = CrawlMetricsTracker()
    tracker.site_genres_initialized("site-a", 3)
    tracker.genre_started("site-a", "Tiên Hiệp", "https://example.com/genre/tien-hiep", position=1, total_genres=3)
    tracker.update_genre_pages("site-a", "https://example.com/genre/tien-hiep", crawled_pages=1, total_pages=5, current_page=1)
    tracker.set_genre_story_total("site-a", "https://example.com/genre/tien-hiep", 2)
    tracker.genre_story_started("site-a", "https://example.com/genre/tien-hiep", "Truyện A", story_page=2, story_position=3)
    snapshot_active = tracker.get_snapshot()["genres"]["in_progress"][0]
    assert snapshot_active["current_story_title"] == "Truyện A"
    assert snapshot_active["current_story_page"] == 2
    assert snapshot_active["current_story_position"] == 3
    assert snapshot_active["active_story_details"][0]["page"] == 2
    tracker.genre_story_finished("site-a", "https://example.com/genre/tien-hiep", "Truyện A", processed=True)
    tracker.genre_completed("site-a", "https://example.com/genre/tien-hiep")

    snapshot = tracker.get_snapshot()
    aggregates = snapshot["aggregates"]
    assert aggregates["genres_in_progress"] == 0
    assert aggregates["genres_completed"] == 1
    assert aggregates["genres_total_configured"] >= 3
    assert aggregates["genres_total_completed"] == 1

    site_genres = snapshot["site_genres"]
    assert site_genres[0]["site_key"] == "site-a"
    assert site_genres[0]["total_genres"] >= 3
    assert site_genres[0]["completed_genres"] == 1
    assert site_genres[0]["genres"][0]["stories"] == 1


def test_metrics_tracker_genre_progress_reset(monkeypatch, tmp_path):
    dashboard = tmp_path / "dashboard_genres_reset.json"
    monkeypatch.setenv("STORYFLOW_DASHBOARD_FILE", str(dashboard))

    tracker = CrawlMetricsTracker()
    site_key = "site-a"
    genre_url = "https://example.com/genre/tien-hiep"

    tracker.genre_started(site_key, "Tiên Hiệp", genre_url)
    tracker.update_genre_pages(
        site_key,
        genre_url,
        crawled_pages=50,
        total_pages=70,
        current_page=50,
    )

    snapshot = tracker.get_snapshot()
    active_genre = snapshot["genres"]["in_progress"][0]
    assert active_genre["crawled_pages"] == 50
    assert active_genre["current_page"] == 50

    tracker.genre_completed(site_key, genre_url)

    tracker.genre_started(site_key, "Tiên Hiệp", genre_url)
    tracker.update_genre_pages(
        site_key,
        genre_url,
        crawled_pages=1,
        total_pages=70,
        current_page=1,
    )

    refreshed_snapshot = tracker.get_snapshot()
    refreshed_genre = refreshed_snapshot["genres"]["in_progress"][0]
    assert refreshed_genre["crawled_pages"] == 1
    assert refreshed_genre["current_page"] == 1
