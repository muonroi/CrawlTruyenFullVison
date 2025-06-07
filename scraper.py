import asyncio
import random
import time
from typing import Dict, Optional

import cloudscraper
import httpx

from config.config import (
    GLOBAL_PROXY_PASSWORD,
    GLOBAL_PROXY_USERNAME,
    LOADED_PROXIES,
    USE_PROXY,
    get_random_headers,
)
from config.proxy_provider import (
    get_proxy_url,
    remove_bad_proxy,
    should_blacklist_proxy,
)
from utils.logger import logger

scraper: Optional[cloudscraper.CloudScraper] = None


async def initialize_scraper(
    site_key, override_headers: Optional[Dict[str, str]] = None
) -> None:
    """
    Khởi tạo cloudscraper instance với headers ngẫu nhiên (bất đồng bộ).
    """
    try:
        loop = asyncio.get_event_loop()
        # Tạo scraper trong executor
        scraper = await loop.run_in_executor(None, cloudscraper.create_scraper)

        # Lấy headers ngẫu nhiên
        current_headers = await get_random_headers(site_key)
        if override_headers:
            current_headers.update(override_headers)

        scraper.headers.update(current_headers)  # type: ignore
        logger.info(
            f"Cloudscraper initialized with User-Agent: {current_headers.get('User-Agent')}"
        )
        logger.debug(f"Full headers for session: {scraper.headers}")  # type: ignore
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo Cloudscraper: {e}")
        scraper = None


def get_proxy_mounts(proxy_url):
    return {
        "http://": httpx.AsyncHTTPTransport(proxy=proxy_url),
        "https://": httpx.AsyncHTTPTransport(proxy=proxy_url),
    }


async def make_request(url, site_key, timeout: int = 30, max_retries: int = 5):
    """Simple HTTP GET with retry & proxy support."""
    headers = await get_random_headers(site_key)
    last_exception = None

    for attempt in range(1, max_retries + 1):
        proxy_url = None
        if USE_PROXY:
            proxy_url = get_proxy_url(GLOBAL_PROXY_USERNAME, GLOBAL_PROXY_PASSWORD)
        mounts = get_proxy_mounts(proxy_url) if proxy_url else None

        try:
            logger.debug(f"[make_request] {attempt}/{max_retries} via {proxy_url}")
            async with httpx.AsyncClient(timeout=timeout, mounts=mounts) as client:
                resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp
        except Exception as ex:
            last_exception = ex
            logger.warning(f"[make_request] Lỗi lần {attempt} khi truy cập {url}: {ex}")
            if proxy_url and should_blacklist_proxy(proxy_url, LOADED_PROXIES):
                remove_bad_proxy(proxy_url)
        await asyncio.sleep(random.uniform(1, 2))

    logger.error(
        f"[make_request] Đã thử {max_retries} lần nhưng thất bại: {last_exception}"
    )
    return None
