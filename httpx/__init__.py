"""Tiny subset of the :mod:`httpx` API for running the unit tests.

Only the symbols touched by the tests are implemented.  Network requests are
not performed; instead the async client returns empty :class:`Response`
objects.  The behaviour is intentionally limited but keeps the code paths used
by the tests operational without the external dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import urlparse


class RequestError(Exception):
    """Base request error used by the real library."""


@dataclass
class Response:
    status_code: int = 200
    text: str = ""
    content: bytes = b""


class AsyncHTTPTransport:
    def __init__(self, proxy: Optional[str] = None) -> None:
        self.proxy = proxy


class URL:
    def __init__(self, value: str) -> None:
        parsed = urlparse(value)
        self.scheme = parsed.scheme
        self.host = parsed.hostname
        self.port = parsed.port
        self.username = parsed.username
        self.password = parsed.password


class AsyncClient:
    def __init__(self, *, headers: Optional[Dict[str, str]] = None, timeout: Any = None, mounts: Optional[Dict[str, Any]] = None, proxies: Any = None) -> None:
        self.headers = headers or {}
        self.timeout = timeout
        self.mounts = mounts or {}
        self.proxies = proxies

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def get(self, url: str, headers: Optional[Dict[str, str]] = None, timeout: Any = None) -> Response:
        return Response(status_code=599, text="", content=b"")


__all__ = [
    "AsyncClient",
    "AsyncHTTPTransport",
    "RequestError",
    "Response",
    "URL",
]
