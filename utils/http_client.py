import asyncio
import random

import httpx
from playwright.async_api import async_playwright

from config.config import (
    DELAY_ON_RETRY,
    GLOBAL_PROXY_PASSWORD,
    GLOBAL_PROXY_USERNAME,
    LOADED_PROXIES,
    PROXIES_FILE,
    REQUEST_DELAY,
    RETRY_ATTEMPTS,
    TIMEOUT_REQUEST,
    USE_PROXY,
    get_random_headers,
)
from config.proxy_provider import (
    get_proxy_url,
    mark_bad_proxy,
    reload_proxies_if_changed,
    remove_bad_proxy,
    should_blacklist_proxy,
)
from utils.anti_bot import is_anti_bot_content
from utils.logger import logger


def get_async_client_for_proxy(proxy_url):
    return httpx.AsyncClient(
        mounts={
            "http://": httpx.AsyncHTTPTransport(proxy=proxy_url),
            "https://": httpx.AsyncHTTPTransport(proxy=proxy_url),
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
                await mark_bad_proxy(proxy_url)
        except Exception as e:
            logger.warning(f"[httpx] request error {e} for {url}")
            if proxy_url and should_blacklist_proxy(proxy_url, LOADED_PROXIES):
                remove_bad_proxy(proxy_url)
        await asyncio.sleep(DELAY_ON_RETRY * attempt)
    return None


async def fetch_with_playwright(url: str) -> str | None:
    """Fetches page content using Playwright routed through the project's proxy config."""
    headers = await get_random_headers('xtruyen')
    user_agent = headers.get('User-Agent')

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        proxy_url_str = get_proxy_url(GLOBAL_PROXY_USERNAME, GLOBAL_PROXY_PASSWORD)
        proxy_config = None
        if USE_PROXY and proxy_url_str:
            try:
                # Parse proxy string like 'http://user:pass@host:port'
                parsed_url = httpx.URL(proxy_url_str)
                proxy_config = {
                    "server": f"{parsed_url.scheme}://{parsed_url.host}:{parsed_url.port}",
                    "username": parsed_url.username,
                    "password": parsed_url.password
                }
                logger.info(f"[Playwright] Using proxy: {proxy_config['server']}")
            except Exception as e:
                logger.error(f"[Playwright] Failed to parse proxy URL: {e}")

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--disable-blink-features=AutomationControlled"],
                    proxy=proxy_config
                )
                context = await browser.new_context(user_agent=user_agent)
                page = await context.new_page()

                await page.goto(url, timeout=60000, wait_until='domcontentloaded')

                try:
                    await page.wait_for_selector('body', timeout=45000)
                except Exception:
                    logger.warning(f"[Playwright] Timed out waiting for selector on {url}.")

                content = await page.content()
                await browser.close()

                if "Just a moment..." not in content and not is_anti_bot_content(content):
                    logger.info(f"[Playwright] Successfully fetched content for {url}")
                    return content
                else:
                    logger.warning(f"[Playwright] Anti-bot content detected on {url} on attempt {attempt}")

        except Exception as e:
            logger.error(f"[Playwright] Error fetching {url} on attempt {attempt}: {e}")

        await asyncio.sleep(DELAY_ON_RETRY * attempt)

    logger.error(f"[Playwright] Failed to fetch {url} after {RETRY_ATTEMPTS} attempts.")
    return None
