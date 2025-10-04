import asyncio
from typing import Optional

import httpx
from config.config import GLOBAL_PROXY_USERNAME, GLOBAL_PROXY_PASSWORD, USE_PROXY, LOADED_PROXIES
from config.proxy_provider import get_proxy_url, mark_bad_proxy, reload_proxies_if_changed
from utils.logger import logger

TEST_URL = "http://httpbin.org/ip"

async def check_proxy(proxy: str) -> bool:
    proxies = {
        "http://": proxy,
        "https://": proxy,
    }
    try:
        async with httpx.AsyncClient(proxies=proxies, timeout=10) as client:
            await client.get(TEST_URL)
        return True
    except Exception:
        return False

async def healthcheck_loop(interval: int = 300, iterations: Optional[int] = None):
    loops_remaining = iterations
    while True:
        await reload_proxies_if_changed("proxies/proxies.txt")
        for proxy in list(LOADED_PROXIES):
            if "://" not in proxy:
                proxy_url = f"http://{proxy}"
            else:
                proxy_url = proxy
            ok = await check_proxy(proxy_url)
            if not ok:
                logger.warning(f"[HEALTHCHECK] Proxy {proxy_url} failed")
                await mark_bad_proxy(proxy_url)
        await asyncio.sleep(interval)
        if loops_remaining is not None:
            loops_remaining -= 1
            if loops_remaining <= 0:
                break

if __name__ == "__main__":
    asyncio.run(healthcheck_loop())
