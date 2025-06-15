import httpx
from config.config import (
    USE_PROXY,
    GLOBAL_PROXY_USERNAME,
    GLOBAL_PROXY_PASSWORD,
    TIMEOUT_REQUEST,
    REQUEST_DELAY,
    PROXIES_FILE,
    LOADED_PROXIES,
    RETRY_ATTEMPTS,
    DELAY_ON_RETRY,
    get_random_headers,
)
from config.proxy_provider import (
    get_proxy_url,
    remove_bad_proxy,
    should_blacklist_proxy,
    reload_proxies_if_changed,
    mark_bad_proxy,
)
from utils.logger import logger
from utils.anti_bot import is_anti_bot_content
import asyncio
import random

def get_async_client_for_proxy(proxy_url):
    return httpx.AsyncClient(
        mounts={
            "http://": httpx.AsyncHTTPTransport(proxy=proxy_url),
            "https://": httpx.AsyncHTTPTransport(proxy=proxy_url)
        }
    )
async def fetch(url: str, site_key: str, timeout: int | None = None) -> httpx.Response | None:
    timeout = timeout or TIMEOUT_REQUEST
    await reload_proxies_if_changed(PROXIES_FILE)

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        headers = await get_random_headers(site_key)
        proxy_url = get_proxy_url(GLOBAL_PROXY_USERNAME, GLOBAL_PROXY_PASSWORD)
        try:
            if USE_PROXY and proxy_url:
                async with get_async_client_for_proxy(proxy_url) as client:
                    resp = await client.get(url, headers=headers, timeout=timeout)
            else:
                async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
                    resp = await client.get(url)
            await asyncio.sleep(random.uniform(0, REQUEST_DELAY))
            if resp.status_code == 200 and resp.text and not is_anti_bot_content(resp.text):
                return resp
            logger.warning(f"[httpx] Potential anti-bot or bad status for {url}")
            if proxy_url and should_blacklist_proxy(proxy_url, LOADED_PROXIES):
                mark_bad_proxy(proxy_url)
        except Exception as e:
            logger.warning(f"[httpx] request error {e} for {url}")
            if proxy_url and should_blacklist_proxy(proxy_url, LOADED_PROXIES):
                remove_bad_proxy(proxy_url)
        await asyncio.sleep(DELAY_ON_RETRY * attempt)
    return None
