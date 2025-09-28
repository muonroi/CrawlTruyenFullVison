import asyncio
import random
from typing import Any, Dict, Optional

from playwright.async_api import async_playwright, Browser, BrowserContext
from utils.http_client import fetch
from utils.anti_bot import is_anti_bot_content

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
    mark_bad_proxy,
)
from utils.logger import logger

browser: Optional[Browser] = None
playwright_obj = None
_init_lock = asyncio.Lock()
_context_pool: Dict[str, BrowserContext] = {}
_context_last_used: Dict[str, float] = {}
fallback_stats = {
    "httpx_success": {},
    "fallback_count": {},
}


async def initialize_scraper(
    site_key, override_headers: Optional[Dict[str, str]] = None
) -> None:
    """Khởi tạo browser Playwright một lần và tái sử dụng."""
    global browser, playwright_obj
    async with _init_lock:
        try:
            if playwright_obj is None:
                playwright_obj = await async_playwright().start()

            if browser is None:
                browser = await playwright_obj.chromium.launch(headless=True)
                logger.info("Playwright browser initialized")
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Playwright: {e}")
            browser = None

async def get_context(proxy_settings, headers) -> BrowserContext:
    key = str(proxy_settings)
    ctx = _context_pool.get(key)
    if ctx:
        _context_last_used[key] = asyncio.get_event_loop().time()
        return ctx
    assert browser, "Browser not initialized"
    ctx = await browser.new_context(user_agent=headers.get("User-Agent"), proxy=proxy_settings)
    _context_pool[key] = ctx
    _context_last_used[key] = asyncio.get_event_loop().time()
    return ctx

async def release_context(proxy_settings) -> None:
    key = str(proxy_settings)
    ctx = _context_pool.pop(key, None)
    _context_last_used.pop(key, None)
    if ctx:
        try:
            await ctx.close()
        except Exception:
            pass

async def close_playwright():
    global browser
    for ctx in list(_context_pool.values()):
        try:
            await ctx.close()
        except Exception:
            pass
    _context_pool.clear()
    _context_last_used.clear()
    if browser:
        try:
            await browser.close()
        except Exception:
            pass
        browser = None

async def recycle_idle_contexts(max_idle_seconds: float = 60):
    now = asyncio.get_event_loop().time()
    for key, last in list(_context_last_used.items()):
        if now - last > max_idle_seconds:
            ctx = _context_pool.pop(key, None)
            _context_last_used.pop(key, None)
            if ctx:
                try:
                    await ctx.close()
                except Exception:
                    pass


async def _make_request_playwright(
    url,
    site_key,
    timeout: int = 30,
    max_retries: int = 5,
    wait_for_selector: str | None = None,
):
    """Load trang bằng Playwright."""
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

            context = await get_context(proxy_settings, headers)
            page = await context.new_page()
            logger.debug(f"[make_request] {attempt}/{max_retries} -> {url}")
            content = None
            try:
                await page.goto(url, timeout=timeout * 1000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=timeout * 1000)
                except Exception:
                    pass
                if wait_for_selector:
                    try:
                        await page.wait_for_selector(wait_for_selector, timeout=timeout * 1000)
                    except Exception:
                        pass
                max_challenge_checks = 3
                for _ in range(max_challenge_checks):
                    content = await page.content()
                    if not is_anti_bot_content(content):
                        break
                    logger.warning("[playwright] Anti-bot detected, waiting for challenge to resolve")
                    await page.wait_for_timeout(4000)
                else:
                    logger.warning("[playwright] Anti-bot persisted after waits")
                    if USE_PROXY and proxy_settings and proxy_url:
                        await mark_bad_proxy(proxy_url)
                    await asyncio.sleep(random.uniform(2, 4))
                    if proxy_settings is not None:
                        await release_context(proxy_settings)
                    continue
            finally:
                try:
                    await page.close()
                except Exception:
                    pass
                await recycle_idle_contexts()

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

    logger.error(f"[playwright] Đã thử {max_retries} lần nhưng thất bại: {last_exception}")
    return None


async def make_request(
    url: str,
    site_key: str,
    timeout: int = 30,
    max_retries: int = 5,
    wait_for_selector: Optional[str] = None,
    method: str = 'GET',
    data: Optional[Dict[str, Any]] = None,
    extra_headers: Optional[Dict[str, str]] = None
):
    """Try httpx first then fallback to Playwright when blocked."""
    if method.upper() == 'POST':
        resp = await fetch(url, site_key, timeout, method=method, data=data, extra_headers=extra_headers)
        if resp and resp.status_code == 200:
            class R:
                def __init__(self, text):
                    self.text = text
            return R(resp.text)
        logger.error(f"[{site_key}] POST request to {url} failed with httpx and has no Playwright fallback.")
        return None

    resp = await fetch(url, site_key, timeout, extra_headers=extra_headers)
    fallback_stats["httpx_success"].setdefault(site_key, 0)
    fallback_stats["fallback_count"].setdefault(site_key, 0)
    if resp and resp.status_code == 200 and resp.text and not is_anti_bot_content(resp.text):
        class R:
            def __init__(self, text):
                self.text = text

        fallback_stats["httpx_success"][site_key] += 1
        return R(resp.text)
    logger.info("[request] Fallback to Playwright due to block or bad status")
    fallback_stats["fallback_count"][site_key] += 1
    return await _make_request_playwright(url, site_key, timeout, max_retries, wait_for_selector=wait_for_selector)
