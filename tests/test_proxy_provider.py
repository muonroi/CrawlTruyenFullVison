import asyncio
import os
import pytest
import time
from config import proxy_provider


# Use monkeypatch to control the global state for each test
@pytest.fixture
def mock_proxy_state(monkeypatch):
    """Fixture to mock the global state of the proxy_provider module."""
    loaded_proxies = []
    cooldown_proxies = {}
    bad_proxy_counts = {}

    monkeypatch.setattr(proxy_provider, 'LOADED_PROXIES', loaded_proxies)
    monkeypatch.setattr(proxy_provider, 'COOLDOWN_PROXIES', cooldown_proxies)
    monkeypatch.setattr(proxy_provider, 'bad_proxy_counts', bad_proxy_counts)
    monkeypatch.setattr(proxy_provider, 'current_proxy_index', 0)
    monkeypatch.setattr(proxy_provider, 'USE_PROXY', True)  # Enable proxy logic for tests
    monkeypatch.setattr(proxy_provider, '_last_proxy_mtime', 0)
    monkeypatch.setattr(proxy_provider, 'PROXY_API_URL', None)
    monkeypatch.setattr(proxy_provider, 'PROXY_NOTIFIED', False)
    monkeypatch.setattr(proxy_provider, 'FAILED_PROXY_TIMES', [])
    monkeypatch.setattr(proxy_provider, 'MAX_FAIL_RATE', 10)

    return loaded_proxies, cooldown_proxies, bad_proxy_counts


@pytest.mark.asyncio
async def test_load_proxies_from_file(tmp_path, mock_proxy_state):
    """Tests loading proxies from a file."""
    loaded_proxies, _, _ = mock_proxy_state
    proxy_file = tmp_path / "proxies.txt"
    proxies_content = "http://proxy1:8080\nhttp://proxy2:8080"
    proxy_file.write_text(proxies_content)

    await proxy_provider.load_proxies(str(proxy_file))

    assert len(loaded_proxies) == 2
    assert "http://proxy1:8080" in loaded_proxies


def test_get_round_robin_proxy(mock_proxy_state, monkeypatch):
    """Tests the round-robin proxy selection logic."""
    loaded_proxies, _, _ = mock_proxy_state
    loaded_proxies.extend(["p1", "p2", "p3"])
    monkeypatch.setattr(proxy_provider, 'proxy_mode', 'round_robin')
    monkeypatch.setattr(proxy_provider, 'current_proxy_index', 0)  # Reset index

    assert proxy_provider.get_proxy_url() == "http://p1"
    assert proxy_provider.get_proxy_url() == "http://p2"
    assert proxy_provider.get_proxy_url() == "http://p3"
    assert proxy_provider.get_proxy_url() == "http://p1"  # Wraps around


def test_get_proxy_url_use_proxy_disabled(mock_proxy_state, monkeypatch):
    loaded_proxies, _, _ = mock_proxy_state
    loaded_proxies.extend(["p1"])

    monkeypatch.setattr(proxy_provider, 'USE_PROXY', False)
    proxy_provider.PROXY_NOTIFIED = False

    assert proxy_provider.get_proxy_url() is None
    assert proxy_provider.PROXY_NOTIFIED is False


def test_get_proxy_url_empty_pool_sets_notification_flag(mock_proxy_state, monkeypatch):
    loaded_proxies, _, _ = mock_proxy_state

    monkeypatch.setattr(proxy_provider.random, 'choice', lambda seq: seq[0])

    # No proxies loaded -> None and notification flag set once
    assert proxy_provider.get_proxy_url() is None
    assert proxy_provider.PROXY_NOTIFIED is True

    # Second call still returns None but keeps the notification flag
    assert proxy_provider.get_proxy_url() is None
    assert proxy_provider.PROXY_NOTIFIED is True

    loaded_proxies.append("proxy1:8000")

    # Once proxies appear, the flag should reset and a proxy URL should be returned
    assert proxy_provider.get_proxy_url() == "http://proxy1:8000"
    assert proxy_provider.PROXY_NOTIFIED is False


@pytest.mark.asyncio
async def test_mark_bad_proxy_cooldown(mock_proxy_state, monkeypatch):
    """Tests that a bad proxy is put into cooldown and recovers after time."""
    loaded_proxies, cooldown_proxies, _ = mock_proxy_state
    loaded_proxies.extend(["p1", "p2"])

    monkeypatch.setattr(proxy_provider, 'PROXY_COOLDOWN_SECONDS', 60)

    # Mark p1 as bad
    await proxy_provider.mark_bad_proxy("p1")

    assert "p1" in cooldown_proxies
    assert proxy_provider._get_available_proxies() == ["p2"]

    # Simulate time passing by advancing the clock
    current_time = time.time()
    monkeypatch.setattr(time, 'time', lambda: current_time + 61)

    # Trigger the cleanup by calling get_available_proxies
    available_proxies = proxy_provider._get_available_proxies()

    # Now the cooldown dict should be empty and p1 should be available
    assert "p1" not in cooldown_proxies
    assert "p1" in available_proxies
    assert len(available_proxies) == 2


def test_remove_bad_proxy(mock_proxy_state):
    """Tests the manual removal of a proxy via remove_bad_proxy."""
    loaded_proxies, _, _ = mock_proxy_state
    loaded_proxies.extend(["http://user:pass@1.1.1.1:8080", "http://2.2.2.2:9000"])

    # Test removing by full URL
    proxy_provider.remove_bad_proxy("http://user:pass@1.1.1.1:8080")

    assert len(loaded_proxies) == 1
    assert loaded_proxies[0] == "http://2.2.2.2:9000"

    # Test removing just by ip:port
    loaded_proxies.append("http://user:pass@1.1.1.1:8080")  # add it back
    proxy_provider.remove_bad_proxy("1.1.1.1:8080")
    assert len(loaded_proxies) == 1
    assert loaded_proxies[0] == "http://2.2.2.2:9000"


@pytest.mark.asyncio
async def test_reload_proxies_if_changed(tmp_path, mock_proxy_state, monkeypatch):
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("p1:8000\n")

    load_calls = []

    async def fake_load(filename=None):
        load_calls.append(filename)
        return []

    monkeypatch.setattr(proxy_provider, 'load_proxies', fake_load)

    await proxy_provider.reload_proxies_if_changed(str(proxy_file))
    assert load_calls == [str(proxy_file)]

    # No file change -> no reload
    await proxy_provider.reload_proxies_if_changed(str(proxy_file))
    assert load_calls == [str(proxy_file)]

    current_mtime = os.path.getmtime(proxy_file)
    os.utime(proxy_file, (current_mtime + 10, current_mtime + 10))

    await proxy_provider.reload_proxies_if_changed(str(proxy_file))
    assert load_calls == [str(proxy_file), str(proxy_file)]


@pytest.mark.asyncio
async def test_load_proxies_from_api(mock_proxy_state, monkeypatch):
    class DummyResponse:
        @staticmethod
        def json():
            return {"data": [{"ip": "1.2.3.4", "port": "8080"}]}

    class DummyAsyncClient:
        def __init__(self, timeout=None):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url):
            assert url == "http://proxy-api.local"
            return DummyResponse()

    monkeypatch.setattr(proxy_provider.httpx, 'AsyncClient', DummyAsyncClient)
    monkeypatch.setattr(proxy_provider, 'PROXY_API_URL', "http://proxy-api.local")

    loaded_proxies, _, _ = mock_proxy_state

    await proxy_provider.load_proxies()

    assert loaded_proxies == ["http://1.2.3.4:8080"]


def test_should_blacklist_proxy_logic():
    assert not proxy_provider.should_blacklist_proxy("proxy1:8000", ["proxy1:8000"])
    assert not proxy_provider.should_blacklist_proxy(
        "proxy-cheap.com:8000", ["proxy1:8000", "proxy2:9000"]
    )
    assert proxy_provider.should_blacklist_proxy(
        "regular-proxy.com:8000", ["proxy1:8000", "proxy2:9000"]
    )


def test_get_random_proxy_url_formats(monkeypatch, mock_proxy_state):
    proxies = [
        "http://proxy1:8000",
        "user:pass@proxy2:9000",
        "proxy3:9100",
    ]

    iterator = iter(proxies)

    def fake_choice(sequence):
        return next(iterator)

    monkeypatch.setattr(proxy_provider, '_get_available_proxies', lambda: proxies)
    monkeypatch.setattr(proxy_provider.random, 'choice', fake_choice)

    assert proxy_provider.get_random_proxy_url() == "http://proxy1:8000"
    assert proxy_provider.get_random_proxy_url() == "http://user:pass@proxy2:9000"
    assert (
        proxy_provider.get_random_proxy_url(username="user", password="pwd")
        == "http://user:pwd@proxy3:9100"
    )


@pytest.mark.asyncio
async def test_mark_bad_proxy_auto_ban_and_telegram_alert(mock_proxy_state, monkeypatch):
    loaded_proxies, _, bad_counts = mock_proxy_state

    proxies_to_ban = [f"http://proxy{i}:8000" for i in range(10)]
    loaded_proxies.extend(proxies_to_ban + ["http://spare:8000", "http://spare2:8000"])

    notifications = []
    created_tasks = []

    async def fake_send_telegram_notify(message, status=None):
        notifications.append((message, status))

    original_create_task = asyncio.create_task

    def fake_create_task(coro):
        created_tasks.append(coro)
        return original_create_task(coro)

    monkeypatch.setattr(proxy_provider, 'send_telegram_notify', fake_send_telegram_notify)
    monkeypatch.setattr(asyncio, 'create_task', fake_create_task)
    monkeypatch.setattr(proxy_provider.time, 'time', lambda: 1_000_000)

    for proxy in proxies_to_ban:
        for _ in range(3):
            await proxy_provider.mark_bad_proxy(proxy)
        assert proxy not in loaded_proxies
        assert bad_counts[proxy] >= 3

    # Allow any scheduled tasks to run
    await asyncio.sleep(0)

    assert len(notifications) == 1
    message, status = notifications[0]
    assert "Cảnh báo" in message
    assert status == "error"
    assert len(created_tasks) == 1
    assert proxy_provider.FAILED_PROXY_TIMES == []



def test_shuffle_proxies(monkeypatch, mock_proxy_state):
    loaded_proxies, _, _ = mock_proxy_state
    loaded_proxies.extend(["p1", "p2", "p3"])

    def fake_shuffle(sequence):
        assert sequence is loaded_proxies
        sequence[:] = ["p2", "p3", "p1"]

    monkeypatch.setattr(proxy_provider.random, 'shuffle', fake_shuffle)

    proxy_provider.shuffle_proxies()

    assert loaded_proxies == ["p2", "p3", "p1"]
