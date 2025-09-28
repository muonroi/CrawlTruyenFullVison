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
    monkeypatch.setattr(proxy_provider, 'USE_PROXY', True) # Enable proxy logic for tests

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
    monkeypatch.setattr(proxy_provider, 'current_proxy_index', 0) # Reset index

    assert proxy_provider.get_proxy_url() == "http://p1"
    assert proxy_provider.get_proxy_url() == "http://p2"
    assert proxy_provider.get_proxy_url() == "http://p3"
    assert proxy_provider.get_proxy_url() == "http://p1" # Wraps around

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
    loaded_proxies.append("http://user:pass@1.1.1.1:8080") # add it back
    proxy_provider.remove_bad_proxy("1.1.1.1:8080")
    assert len(loaded_proxies) == 1
    assert loaded_proxies[0] == "http://2.2.2.2:9000"