from utils.errors import CrawlError
from utils.health_monitor import SiteHealthMonitor
from utils.metrics_tracker import CrawlMetricsTracker


def test_site_health_monitor_alert(monkeypatch, tmp_path):
    dashboard = tmp_path / "health_dashboard.json"
    monkeypatch.setenv("STORYFLOW_DASHBOARD_FILE", str(dashboard))

    tracker = CrawlMetricsTracker()
    monkeypatch.setattr("utils.health_monitor.metrics_tracker", tracker)

    alerts = []

    def notifier(message, **kwargs):
        alerts.append((message, kwargs))

    monitor = SiteHealthMonitor(
        failure_threshold=0.5,
        min_attempts=4,
        alert_cooldown=900,
        notifier=notifier,
    )

    monitor.record_failure("site-a", CrawlError.TIMEOUT)
    monitor.record_failure("site-a", CrawlError.TIMEOUT)
    monitor.record_success("site-a")
    monitor.record_failure("site-a", CrawlError.TIMEOUT)

    assert len(alerts) == 1
    assert "site-a" in alerts[0][0]

    # Alert cooldown should suppress subsequent alerts immediately after
    monitor.record_failure("site-a", CrawlError.TIMEOUT)
    assert len(alerts) == 1

    snapshot = tracker.get_snapshot()
    site_stats = snapshot["sites"][0]
    assert site_stats["failure"] >= 4
    assert site_stats["last_error"] == CrawlError.TIMEOUT.value
