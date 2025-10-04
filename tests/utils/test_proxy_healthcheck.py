import pytest

from utils import proxy_healthcheck


@pytest.mark.asyncio
async def test_healthcheck_loop_marks_bad_proxies(monkeypatch):
    bad_proxy = "badproxy:8080"
    loaded_proxies = [bad_proxy]
    monkeypatch.setattr(proxy_healthcheck, "LOADED_PROXIES", loaded_proxies, raising=False)

    calls = {
        "mark_bad_proxy": [],
        "reload": 0,
    }

    async def fake_reload(path):
        calls["reload"] += 1

    async def fake_mark(proxy):
        calls["mark_bad_proxy"].append(proxy)

    async def fake_check(proxy):
        return False

    monkeypatch.setattr(proxy_healthcheck, "reload_proxies_if_changed", fake_reload)
    monkeypatch.setattr(proxy_healthcheck, "mark_bad_proxy", fake_mark)
    monkeypatch.setattr(proxy_healthcheck, "check_proxy", fake_check)

    await proxy_healthcheck.healthcheck_loop(interval=0, iterations=1)

    assert calls["reload"] == 1
    assert calls["mark_bad_proxy"] == ["http://badproxy:8080"]
