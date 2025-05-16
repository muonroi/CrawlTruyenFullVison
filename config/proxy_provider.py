# proxy_provider.py
import random
import aiofiles
from typing import List, Optional
from config.config import USE_PROXY
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
    if not USE_PROXY:
        return None
    
    if proxy_mode == "round_robin":
        return get_round_robin_proxy_url(username, password)
    return get_random_proxy_url(username, password)

def remove_bad_proxy(bad_proxy_url):
    if not bad_proxy_url:
        return
    
    global loaded_proxies
    # Chuẩn hóa về ip:port để remove chính xác
    import re
    match = re.search(r'(?:(?:http|https)://)?(?:[^@]+@)?(?P<ip>[\w\.\-:]+)', bad_proxy_url)
    if not match:
        logger.warning(f"Không tách được IP:PORT từ {bad_proxy_url}")
        return
    ip_port = match.group("ip")
    removed = False
    for proxy in loaded_proxies[:]:
        if ip_port in proxy:
            loaded_proxies.remove(proxy)
            logger.warning(f"Đã loại proxy lỗi: {proxy} (từ {bad_proxy_url})")
            removed = True
    if not removed:
        logger.warning(f"Không tìm thấy proxy {bad_proxy_url} để remove.")
