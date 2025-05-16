import asyncio
import cloudscraper
from typing import Optional, Dict
import cloudscraper
from config.proxy_provider import get_proxy_url, remove_bad_proxy
from utils.utils import logger
from config.config import (
    GLOBAL_PROXY_PASSWORD,
    GLOBAL_PROXY_USERNAME,
    USE_PROXY,
    get_random_headers)

scraper: Optional[cloudscraper.CloudScraper] = None

async def initialize_scraper(override_headers: Optional[Dict[str, str]] = None) -> None:
    """
    Khởi tạo cloudscraper instance với headers ngẫu nhiên (bất đồng bộ).
    """
    global scraper
    try:
        loop = asyncio.get_event_loop()
        # Tạo scraper trong executor
        scraper = await loop.run_in_executor(None, cloudscraper.create_scraper)

        # Lấy headers ngẫu nhiên
        current_headers = await get_random_headers()
        if override_headers:
            current_headers.update(override_headers)

        scraper.headers.update(current_headers) # type: ignore
        logger.info(f"Cloudscraper initialized with User-Agent: {current_headers.get('User-Agent')}")
        logger.debug(f"Full headers for session: {scraper.headers}") # type: ignore
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo Cloudscraper: {e}")
        scraper = None

def make_request(url, headers_override=None, timeout=30):
    global scraper
    if scraper is None:
        scraper = cloudscraper.create_scraper()
    headers = scraper.headers.copy() # type: ignore
    if headers_override:
        headers.update(headers_override)

    # Lấy proxy random
    proxy_url = get_proxy_url(GLOBAL_PROXY_USERNAME, GLOBAL_PROXY_PASSWORD) if USE_PROXY else None
    proxies = {}
    if proxy_url:
        proxies = {
            "http": proxy_url,
            "https": proxy_url
        }
    try:
        print (f"Đang sử dụng proxy: {proxy_url}")
        resp = scraper.get(url, headers=headers, proxies=proxies, timeout=timeout)
        resp.raise_for_status()
        return resp
    except Exception as ex:
        logger.error(f"make_request lỗi: {ex}")
        remove_bad_proxy(proxy_url)
        return None
