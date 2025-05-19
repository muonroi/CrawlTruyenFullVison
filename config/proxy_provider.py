# proxy_provider.py
import asyncio
import random
import sys
import time
import aiofiles
from typing import List, Optional
from config.config import USE_PROXY
from utils.logger import logger
from config.config import LOADED_PROXIES
from utils.notifier import send_telegram_notify


proxy_mode = "random"
current_proxy_index: int = 0
bad_proxy_counts = {}
PROXY_NOTIFIED = False
MAX_FAIL_RATE = 10 
FAILED_PROXY_TIMES = []

def should_blacklist_proxy(proxy_url, loaded_proxies):
    proxy_domain = proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url
    if len(loaded_proxies) <= 1:
        return False
    if any(key in proxy_domain for key in [
        "proxy-cheap.com"
    ]):
        return False
    return True


async def load_proxies(filename: str) -> List[str]:
    try:
        async with aiofiles.open(filename, "r") as f:
            lines = await f.readlines()
        raw_entries = [line.strip() for line in lines if line.strip() and not line.startswith("#")]
        LOADED_PROXIES.clear()
        LOADED_PROXIES.extend(raw_entries)
        if not LOADED_PROXIES:
            asyncio.create_task(send_telegram_notify("[Crawl Notify] Không có proxy nào được load vào hệ thống!"))
    except Exception as e:
        print(f"Proxy load error: {e}")
    return LOADED_PROXIES

def mark_bad_proxy(proxy: str):
    global FAILED_PROXY_TIMES
    if not should_blacklist_proxy(proxy, LOADED_PROXIES):
        logger.warning(f"[Proxy] Proxy xoay hoặc pool chỉ có 1 proxy, sẽ không blacklist: {proxy}. Chỉ sleep & retry.")
        time.sleep(10)  # Đợi IP backend đổi (proxy xoay)
        return

    bad_proxy_counts.setdefault(proxy, 0)
    bad_proxy_counts[proxy] += 1
    if bad_proxy_counts[proxy] >= 3 and proxy in LOADED_PROXIES:
        LOADED_PROXIES.remove(proxy)
        logger.warning(f"Proxy '{proxy}' auto-banned do quá nhiều lỗi.")
        FAILED_PROXY_TIMES.append(time.time())
        FAILED_PROXY_TIMES = [t for t in FAILED_PROXY_TIMES if time.time() - t < 60]
        if len(FAILED_PROXY_TIMES) >= MAX_FAIL_RATE:
            asyncio.create_task(send_telegram_notify("[Crawl Notify] Cảnh báo: Số lượng proxy bị ban liên tục vượt ngưỡng, hãy kiểm tra lại hệ thống/proxy!"))
            FAILED_PROXY_TIMES.clear()
 

def get_random_proxy_url(username: str = None, password: str = None) -> Optional[str]: # type: ignore
    if not LOADED_PROXIES:
        return None
    proxy = random.choice(LOADED_PROXIES)
    if "://" in proxy:
        return proxy
    if username and password:
        return f"http://{username}:{password}@{proxy}"
    return f"http://{proxy}"

def get_round_robin_proxy_url(username: str = None, password: str = None) -> Optional[str]: # type: ignore
    global current_proxy_index
    if not LOADED_PROXIES:
        return None
    proxy = LOADED_PROXIES[current_proxy_index]
    current_proxy_index = (current_proxy_index + 1) % len(LOADED_PROXIES)
    if "://" in proxy:
        return proxy
    if username and password:
        return f"http://{username}:{password}@{proxy}"
    return f"http://{proxy}"

def get_proxy_url(username: str = None, password: str = None) -> Optional[str]: # type: ignore
    if not USE_PROXY:
        return None
    global PROXY_NOTIFIED
    if not LOADED_PROXIES:
        if not PROXY_NOTIFIED:
            asyncio.create_task(send_telegram_notify("[Crawl Notify] Hết proxy usable!"))
            PROXY_NOTIFIED = True
        return None
    PROXY_NOTIFIED = False
    
    if proxy_mode == "round_robin":
        return get_round_robin_proxy_url(username, password)
    return get_random_proxy_url(username, password)

def remove_bad_proxy(bad_proxy_url):
    if not bad_proxy_url:
        return
    import re
    match = re.search(r'(?:(?:http|https)://)?(?:[^@]+@)?(?P<ip>[\w\.\-:]+)', bad_proxy_url)
    if not match:
        logger.warning(f"Không tách được IP:PORT từ {bad_proxy_url}")
        return
    ip_port = match.group("ip")
    removed = False
    for proxy in LOADED_PROXIES[:]:
        if ip_port in proxy:
            LOADED_PROXIES.remove(proxy)
            logger.warning(f"Đã loại proxy lỗi: {proxy} (từ {bad_proxy_url})")
            removed = True
    if not removed:
        logger.warning(f"Không tìm thấy proxy {bad_proxy_url} để remove.")
    if removed:
        with open("banned_proxies.log", "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {proxy} bị remove vì lỗi nhiều lần\n")

def shuffle_proxies():
    import random
    random.shuffle(LOADED_PROXIES)
    logger.info("[Proxy] Đã shuffle lại proxy pool!")
