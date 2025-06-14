import httpx
from config.config import (
    USE_PROXY,
    GLOBAL_PROXY_USERNAME,
    GLOBAL_PROXY_PASSWORD,
    TIMEOUT_REQUEST,
    REQUEST_DELAY,
    PROXIES_FILE,
    LOADED_PROXIES,
    get_random_headers,
)
from config.proxy_provider import (
    get_proxy_url,
    remove_bad_proxy,
    should_blacklist_proxy,
    reload_proxies_if_changed,
)
from utils.logger import logger
import asyncio
import random


async def fetch(url: str, site_key: str, timeout: int | None = None) -> httpx.Response | None:
    timeout = timeout or TIMEOUT_REQUEST
    await reload_proxies_if_changed(PROXIES_FILE)
    headers = await get_random_headers(site_key)
    proxy_url = get_proxy_url(GLOBAL_PROXY_USERNAME, GLOBAL_PROXY_PASSWORD)
    proxies = None
    if USE_PROXY and proxy_url:
        proxies = {
            "http://": proxy_url,
            "https://": proxy_url,
        }
    try:
        async with httpx.AsyncClient(headers=headers, proxies=proxies, timeout=timeout) as client:
            resp = await client.get(url)
            await asyncio.sleep(random.uniform(0, REQUEST_DELAY))
            return resp
    except Exception as e:
        logger.warning(f"[httpx] request error {e} for {url}")
        if proxy_url and should_blacklist_proxy(proxy_url, LOADED_PROXIES):
            remove_bad_proxy(proxy_url)
        return None

