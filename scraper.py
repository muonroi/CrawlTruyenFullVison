import asyncio
import time
import cloudscraper
from typing import Optional, Dict
import cloudscraper
import httpx
from config.proxy_provider import remove_bad_proxy, should_blacklist_proxy
from utils.logger import logger
from config.config import (
    USE_PROXY,
    get_random_headers)
import random
import time
import random
import cloudscraper
from utils.logger import logger
from config.config import LOADED_PROXIES

scraper: Optional[cloudscraper.CloudScraper] = None

async def initialize_scraper(site_key,override_headers: Optional[Dict[str, str]] = None) -> None:
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

        scraper.headers.update(current_headers) # type: ignore
        logger.info(f"Cloudscraper initialized with User-Agent: {current_headers.get('User-Agent')}")
        logger.debug(f"Full headers for session: {scraper.headers}") # type: ignore
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo Cloudscraper: {e}")
        scraper = None

def get_proxy_mounts(proxy_url):
    return {
        "http://": httpx.AsyncHTTPTransport(proxy=proxy_url),
        "https://": httpx.AsyncHTTPTransport(proxy=proxy_url)
    }

async def make_request(url, site_key, timeout=30, max_retries=5):
    headers = await get_random_headers(site_key)
    last_exception = None
    tried_proxies = set()
    for attempt in range(max_retries):
        if attempt > 0:
            await asyncio.sleep(random.uniform(1, 2))
        proxy_url = None
        if USE_PROXY:
            available_proxies = [p for p in LOADED_PROXIES if p not in tried_proxies]
            if not available_proxies:
                logger.warning("Đã thử hết proxy đang có, không còn proxy nào tốt.")
                break
            proxy_url = random.choice(available_proxies)
        mounts = get_proxy_mounts(proxy_url) if proxy_url else None
        try:
            print(f"[make_request] Đang sử dụng proxy: {proxy_url}")
            async with httpx.AsyncClient(timeout=timeout, mounts=mounts) as client:
                resp = await client.get(url, headers=headers)
            if resp.status_code == 403:
                # Xử lý như cũ
                pass
            resp.raise_for_status()
            return resp
        except Exception as ex:
            # Xử lý như cũ
            last_exception = ex
    logger.error(f"[make_request] Đã thử {max_retries} proxy nhưng vẫn lỗi: {last_exception}")
    return None

