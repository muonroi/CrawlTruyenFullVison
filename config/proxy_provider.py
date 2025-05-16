# proxy_provider.py
import random
import aiofiles
from typing import List, Optional
from utils.logger import logger

loaded_proxies: List[str] = []
proxy_mode = "random"
current_proxy_index: int = 0
bad_proxy_counts = {}

async def load_proxies(filename: str) -> List[str]:
    global loaded_proxies
    loaded_proxies = []
    try:
        async with aiofiles.open(filename, "r") as f:
            lines = await f.readlines()
        raw_entries = [line.strip() for line in lines if line.strip() and not line.startswith("#")]
        loaded_proxies = raw_entries
    except Exception as e:
        print(f"Proxy load error: {e}")
    return loaded_proxies

def mark_bad_proxy(proxy: str):
    bad_proxy_counts.setdefault(proxy, 0)
    bad_proxy_counts[proxy] += 1
    # Auto-ban nếu quá N lần lỗi
    if bad_proxy_counts[proxy] >= 3 and proxy in loaded_proxies:
        loaded_proxies.remove(proxy)
        print(f"Proxy '{proxy}' auto-banned do quá nhiều lỗi.")

def get_random_proxy_url(username: str = None, password: str = None) -> Optional[str]: # type: ignore
    if not loaded_proxies:
        return None
    proxy = random.choice(loaded_proxies)
    if "://" in proxy:
        return proxy
    if username and password:
        return f"http://{username}:{password}@{proxy}"
    return f"http://{proxy}"

def get_round_robin_proxy_url(username: str = None, password: str = None) -> Optional[str]: # type: ignore
    global current_proxy_index
    if not loaded_proxies:
        return None
    proxy = loaded_proxies[current_proxy_index]
    current_proxy_index = (current_proxy_index + 1) % len(loaded_proxies)
    if "://" in proxy:
        return proxy
    if username and password:
        return f"http://{username}:{password}@{proxy}"
    return f"http://{proxy}"

def get_proxy_url(username: str = None, password: str = None) -> Optional[str]: # type: ignore
    # Lựa chọn strategy, mặc định random
    if proxy_mode == "round_robin":
        return get_round_robin_proxy_url(username, password)
    return get_random_proxy_url(username, password)

def remove_bad_proxy(bad_proxy_url):
    global loaded_proxies
    if bad_proxy_url in loaded_proxies:
        loaded_proxies.remove(bad_proxy_url)
        logger.warning(f"Đã loại proxy lỗi: {bad_proxy_url} (bị 403 hoặc timeout)")