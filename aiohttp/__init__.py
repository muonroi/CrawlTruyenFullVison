"""Small stub of :mod:`aiohttp` with only the APIs required by the tests."""

from __future__ import annotations


class _DummyResponse:
    def __init__(self, status: int = 200, text: str = "") -> None:
        self.status = status
        self._text = text

    async def text(self) -> str:
        return self._text


class ClientSession:
    def __init__(self, *args, **kwargs) -> None:  # pragma: no cover - simple stub
        pass

    async def __aenter__(self) -> "ClientSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def post(self, url: str, **kwargs) -> _DummyResponse:
        return _DummyResponse()

    async def get(self, url: str, **kwargs) -> _DummyResponse:
        return _DummyResponse()


__all__ = ["ClientSession"]
