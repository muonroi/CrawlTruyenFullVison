import asyncio

import httpx

from utils.errors import CrawlError, classify_crawl_exception, is_retryable_error


def test_classify_crawl_exception_http_status_not_found():
    response = httpx.Response(404, request=httpx.Request("GET", "https://example.com"))
    exc = httpx.HTTPStatusError("not found", request=response.request, response=response)
    assert classify_crawl_exception(exc) == CrawlError.NOT_FOUND


def test_classify_crawl_exception_rate_limit():
    response = httpx.Response(429, request=httpx.Request("GET", "https://example.com"))
    exc = httpx.HTTPStatusError("rate limited", request=response.request, response=response)
    assert classify_crawl_exception(exc) == CrawlError.RATE_LIMIT


def test_classify_crawl_exception_timeout_retryable():
    exc = asyncio.TimeoutError()
    assert classify_crawl_exception(exc) == CrawlError.TIMEOUT
    assert is_retryable_error(CrawlError.TIMEOUT) is True

