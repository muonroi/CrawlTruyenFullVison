import asyncio
import time
import cloudscraper
from typing import Optional, Dict
import cloudscraper
from config.proxy_provider import remove_bad_proxy
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

async def initialize_scraper(adapter,override_headers: Optional[Dict[str, str]] = None) -> None:
    """
    Khởi tạo cloudscraper instance với headers ngẫu nhiên (bất đồng bộ).
    """
    try:
        site_key = getattr(adapter, 'SITE_KEY', None) or getattr(adapter, 'site_key', None) or 'unknown'
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


def make_request(url, headers_override=None, timeout=30, max_retries=5):
    global scraper
    if scraper is None:
        scraper = cloudscraper.create_scraper()
    headers = scraper.headers.copy()  # type: ignore
    if headers_override:
        headers.update(headers_override)

    last_exception = None
    tried_proxies = set()

    for attempt in range(max_retries):
        if attempt > 0:
            # Delay nhỏ tránh spam khi retry, chỉ delay nếu không phải lần đầu
            time.sleep(random.uniform(1, 2))

        proxy_url = None
        if USE_PROXY:
            available_proxies = [p for p in LOADED_PROXIES if p not in tried_proxies]
            if not available_proxies:
                logger.warning("Đã thử hết proxy đang có, không còn proxy nào tốt.")
                break
            proxy_url = random.choice(available_proxies)
        proxies = {}
        if proxy_url:
            proxies = {
                "http": proxy_url,
                "https": proxy_url
            }
        try:
            print(f"[make_request] Đang sử dụng proxy: {proxy_url}")
            resp = scraper.get(url, headers=headers, proxies=proxies, timeout=timeout)
            if resp.status_code == 403:
                logger.warning(f"Proxy {proxy_url} bị 403 Forbidden, sẽ thử proxy khác.")
                tried_proxies.add(proxy_url)
                remove_bad_proxy(proxy_url)
                continue
            resp.raise_for_status()
            return resp
        except Exception as ex:
            logger.error(f"[make_request] Lỗi với proxy {proxy_url}: {ex}")
            last_exception = ex
            if proxy_url:
                tried_proxies.add(proxy_url)
                remove_bad_proxy(proxy_url)
            # Thử lại với proxy khác

    logger.error(f"[make_request] Đã thử {max_retries} proxy nhưng vẫn lỗi: {last_exception}")
    return None
