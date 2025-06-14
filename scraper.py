import asyncio
import random
from typing import Dict, Optional

from playwright.async_api import async_playwright, Browser

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

browser: Optional[Browser] = None
playwright_obj = None


async def initialize_scraper(
    site_key, override_headers: Optional[Dict[str, str]] = None
) -> None:
    """Khởi tạo browser Playwright với proxy (nếu có)."""
    global browser, playwright_obj
    try:
        if playwright_obj is None:
            playwright_obj = await async_playwright().start()

        if browser:
            await browser.close()
        browser = await playwright_obj.chromium.launch(headless=True)
        logger.info("Playwright browser initialized")
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo Playwright: {e}")
        browser = None


async def make_request(url, site_key, timeout: int = 30, max_retries: int = 5):
    """Load trang bằng Playwright và trả về đối tượng có thuộc tính text."""
    global browser
    headers = await get_random_headers(site_key)
    last_exception = None

    for attempt in range(1, max_retries + 1):
        proxy_url = None
        try:
            if not browser:
                await initialize_scraper(site_key)
                if not browser:
                    raise RuntimeError("Playwright browser not initialized")

            proxy_settings = None
            if USE_PROXY:
                proxy_url = get_proxy_url(GLOBAL_PROXY_USERNAME, GLOBAL_PROXY_PASSWORD)
                if proxy_url:
                    from urllib.parse import urlparse
                    p = urlparse(proxy_url)
                    proxy_settings = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
                    if p.username:
                        proxy_settings["username"] = p.username
                    if p.password:
                        proxy_settings["password"] = p.password

            context = await browser.new_context(user_agent=headers.get("User-Agent"), proxy=proxy_settings)
            page = await context.new_page()
            logger.debug(f"[make_request] {attempt}/{max_retries} -> {url}")
            await page.goto(url, timeout=timeout * 1000)
            content = await page.content()
            await page.close()
            await context.close()

            class Resp:
                def __init__(self, text):
                    self.text = text

            return Resp(content)
        except Exception as ex:
            last_exception = ex
            logger.warning(f"[make_request] Lỗi lần {attempt} khi truy cập {url}: {ex}")
            if USE_PROXY and proxy_settings and should_blacklist_proxy(proxy_url, LOADED_PROXIES):
                remove_bad_proxy(proxy_url)
        await asyncio.sleep(random.uniform(1, 2))

    logger.error(f"[make_request] Đã thử {max_retries} lần nhưng thất bại: {last_exception}")
    return None
