import importlib
import sys

import pytest

MODULE_NAME = "config.config"
FOLDER_ENV_KEYS = [
    "DATA_FOLDER",
    "COMPLETED_FOLDER",
    "BACKUP_FOLDER",
    "STATE_FOLDER",
    "LOG_FOLDER",
]


def _reload_config(monkeypatch: pytest.MonkeyPatch, tmp_path, base_value: str | None):
    # Ensure we import a fresh instance of config.config with controlled environment.
    sys.modules.pop(MODULE_NAME, None)

    for key in FOLDER_ENV_KEYS:
        monkeypatch.setenv(key, str(tmp_path / key.lower()))

    monkeypatch.setenv("BASE_XTRUYEN", "https://xtruyen.vn")
    if base_value is None:
        monkeypatch.delenv("BASE_TANGTHUVIEN", raising=False)
    else:
        monkeypatch.setenv("BASE_TANGTHUVIEN", base_value)

    module = importlib.import_module(MODULE_NAME)
    return module


def test_base_url_sanitizes_trailing_punctuation(monkeypatch: pytest.MonkeyPatch, tmp_path):
    module = _reload_config(monkeypatch, tmp_path, "https://tangthuvien.net/!")
    try:
        assert module.BASE_URLS["tangthuvien"] == "https://tangthuvien.net"
    finally:
        sys.modules.pop(MODULE_NAME, None)


def test_base_url_falls_back_without_scheme(monkeypatch: pytest.MonkeyPatch, tmp_path):
    module = _reload_config(monkeypatch, tmp_path, "tangthuvien.net")
    try:
        assert module.BASE_URLS["tangthuvien"] == "https://tangthuvien.net"
    finally:
        sys.modules.pop(MODULE_NAME, None)
