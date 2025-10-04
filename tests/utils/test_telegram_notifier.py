import httpx
import pytest

from utils import telegram_notifier


@pytest.mark.asyncio
async def test_send_message_missing_config(monkeypatch, caplog):
    monkeypatch.setattr(telegram_notifier, "TELEGRAM_BOT_TOKEN", None)
    monkeypatch.setattr(telegram_notifier, "TELEGRAM_CHAT_ID", None)

    caplog.set_level("WARNING")

    await telegram_notifier.send_telegram_message("hello")

    assert "chưa được cấu hình" in caplog.text


class DummyAsyncClient:
    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, *args, **kwargs):
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


def _patch_client(monkeypatch, response):
    monkeypatch.setattr(telegram_notifier, "httpx", httpx)

    def factory(*args, **kwargs):
        return DummyAsyncClient(response)

    monkeypatch.setattr(telegram_notifier, "httpx", httpx)
    monkeypatch.setattr(telegram_notifier.httpx, "AsyncClient", factory)


def _prepare_config(monkeypatch):
    monkeypatch.setattr(telegram_notifier, "TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setattr(telegram_notifier, "TELEGRAM_CHAT_ID", "chat")
    monkeypatch.setattr(telegram_notifier, "TELEGRAM_THREAD_ID", None)
    monkeypatch.setattr(telegram_notifier, "TELEGRAM_PARSE_MODE", None)
    monkeypatch.setattr(telegram_notifier, "TELEGRAM_DISABLE_WEB_PAGE_PREVIEW", None)


@pytest.mark.asyncio
async def test_send_message_success(monkeypatch, caplog):
    _prepare_config(monkeypatch)

    response = httpx.Response(200, request=httpx.Request("POST", "http://example.com"))
    _patch_client(monkeypatch, response)

    caplog.set_level("DEBUG")

    await telegram_notifier.send_telegram_message("hello")

    assert "Đã gửi tin nhắn thành công" in caplog.text


@pytest.mark.asyncio
async def test_send_message_http_error(monkeypatch, caplog):
    _prepare_config(monkeypatch)

    response = httpx.Response(
        400,
        request=httpx.Request("POST", "http://example.com"),
        text="Bad request",
    )
    _patch_client(monkeypatch, response)

    caplog.set_level("ERROR")

    await telegram_notifier.send_telegram_message("hello")

    assert "Status code: 400" in caplog.text


@pytest.mark.asyncio
async def test_send_message_request_error(monkeypatch, caplog):
    _prepare_config(monkeypatch)

    error = httpx.RequestError("network down", request=httpx.Request("POST", "http://example.com"))
    _patch_client(monkeypatch, error)

    caplog.set_level("ERROR")

    await telegram_notifier.send_telegram_message("hello")

    assert "Lỗi mạng" in caplog.text


@pytest.mark.asyncio
async def test_send_message_generic_exception(monkeypatch, caplog):
    _prepare_config(monkeypatch)

    _patch_client(monkeypatch, ValueError("boom"))

    caplog.set_level("ERROR")

    await telegram_notifier.send_telegram_message("hello")

    assert "Lỗi không xác định" in caplog.text
