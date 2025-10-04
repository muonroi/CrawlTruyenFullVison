import asyncio
from collections import defaultdict, deque
import random
import sys
import time
import aiofiles
import httpx
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse
import os

from config.config import (
    LOADED_PROXIES,
    PROXY_API_URL,
    PROXIES_FILE,
    USE_PROXY,
)
from utils.logger import logger
from utils.notifier import send_telegram_notify

proxy_mode = "random"
current_proxy_index: int = 0
bad_proxy_counts: Dict[str, int] = {}
COOLDOWN_PROXIES: Dict[str, float] = {}
PROXY_COOLDOWN_SECONDS = 60
PROXY_NOTIFIED = False
MAX_FAIL_RATE = 10
FAILED_PROXY_TIMES: List[float] = []
_last_proxy_mtime = 0
SITE_PROXY_BLACKLIST_TTL = 300
WEAK_PROXY_THRESHOLD = 5
WEAK_PROXY_RETENTION_SECONDS = 1800
SITE_PROXY_BLACKLIST: Dict[str, Dict[str, float]] = {}
_proxy_fail_history: Dict[str, deque[float]] = defaultdict(deque)

def _normalize_proxy_key(proxy: str) -> str:
    if not proxy:
        return ""
    candidate = proxy if "://" in proxy else f"http://{proxy}"
    try:
        parsed = urlparse(candidate)
        host = parsed.hostname or candidate
        port = parsed.port
        if host and port:
            return f"{host}:{port}".lower()
        if host:
            return host.lower()
    except Exception:
        pass
    return candidate.lower()


def _prune_site_blacklist(site_key: str, now: Optional[float] = None) -> Set[str]:
    if site_key not in SITE_PROXY_BLACKLIST:
        return set()

    now = now or time.time()
    site_map = SITE_PROXY_BLACKLIST[site_key]
    expired = [proxy for proxy, expiry in site_map.items() if expiry <= now]
    for proxy in expired:
        site_map.pop(proxy, None)

    if not site_map:
        SITE_PROXY_BLACKLIST.pop(site_key, None)
        return set()

    return set(site_map.keys())


def _remove_proxy_by_normalized(normalized_key: str) -> bool:
    removed = False
    for proxy in LOADED_PROXIES[:]:
        if _normalize_proxy_key(proxy) == normalized_key:
            LOADED_PROXIES.remove(proxy)
            COOLDOWN_PROXIES.pop(proxy, None)
            removed = True
    if removed:
        for site_map in SITE_PROXY_BLACKLIST.values():
            site_map.pop(normalized_key, None)
        bad_proxy_counts.pop(normalized_key, None)
        _proxy_fail_history.pop(normalized_key, None)
    return removed


def _record_proxy_failure(normalized_key: str) -> None:
    if not normalized_key:
        return

    now = time.time()
    history = _proxy_fail_history[normalized_key]
    history.append(now)
    cutoff = now - WEAK_PROXY_RETENTION_SECONDS
    while history and history[0] < cutoff:
        history.popleft()


async def cleanup_weak_proxies(
    retention_seconds: int = WEAK_PROXY_RETENTION_SECONDS,
    threshold: int = WEAK_PROXY_THRESHOLD,
) -> None:
    now = time.time()
    for proxy_key, history in list(_proxy_fail_history.items()):
        while history and history[0] < now - retention_seconds:
            history.popleft()
        if not history:
            _proxy_fail_history.pop(proxy_key, None)
            continue
        if len(history) >= threshold:
            if _remove_proxy_by_normalized(proxy_key):
                logger.warning(
                    "[Proxy] Proxy %s removed after %d failures in the last %d seconds.",
                    proxy_key,
                    len(history),
                    retention_seconds,
                )
            _proxy_fail_history.pop(proxy_key, None)


def _get_available_proxies(site_key: Optional[str] = None) -> List[str]:
    now = time.time()
    for p, t in list(COOLDOWN_PROXIES.items()):
        if t <= now:
            COOLDOWN_PROXIES.pop(p, None)

    proxies = [p for p in LOADED_PROXIES if COOLDOWN_PROXIES.get(p, 0) <= now]

    if not site_key:
        return proxies

    blacklist = _prune_site_blacklist(site_key, now)
    if not blacklist:
        return proxies

    return [p for p in proxies if _normalize_proxy_key(p) not in blacklist]

def should_blacklist_proxy(proxy_url, loaded_proxies):
    proxy_domain = proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url
    if len(loaded_proxies) <= 1:
        return False
    if any(key in proxy_domain for key in [
        "proxy-cheap.com"
    ]):
        return False
    return True

async def load_proxies(filename: Optional[str] = None) -> List[str]:
    """Load proxies từ file txt hoặc từ API nếu PROXY_API_URL có giá trị"""
    global LOADED_PROXIES

    if not USE_PROXY:
        # Khi không sử dụng proxy, đảm bảo danh sách rỗng và không log nhầm số lượng proxy.
        if LOADED_PROXIES:
            LOADED_PROXIES.clear()
        logger.info("[Proxy] USE_PROXY=false, bỏ qua việc load proxy.")
        return LOADED_PROXIES
    if PROXY_API_URL:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(PROXY_API_URL)
            # Đọc data, kiểm tra phải list
            data = resp.json()
            proxies_data = data.get("data")
            if not isinstance(proxies_data, list):
                proxies_data = []
            LOADED_PROXIES.clear()
            LOADED_PROXIES.extend([
                f"http://{item['ip']}:{item['port']}"
                for item in proxies_data
                if isinstance(item, dict) and 'ip' in item and 'port' in item
            ])
            if not LOADED_PROXIES:
                logger.info("[Crawl Notify] Không có proxy nào từ API!")
        except Exception as e:
            print(f"Proxy API load error: {e}")
    else:
        filename = filename or PROXIES_FILE
        try:
            async with aiofiles.open(filename, "r") as f:
                lines = await f.readlines()
            raw_entries = [line.strip() for line in lines if line.strip() and not line.startswith("#")]
            LOADED_PROXIES.clear()
            LOADED_PROXIES.extend(raw_entries)
            if not LOADED_PROXIES:
                asyncio.create_task(send_telegram_notify("[Crawl Notify] Không có proxy nào được load vào hệ thống!", status="warning"))
        except Exception as e:
            print(f"Proxy load error: {e}")
    return LOADED_PROXIES


async def reload_proxies_if_changed(filename: Optional[str] = None) -> None:
    """Reload proxies nếu file đổi (hoặc luôn reload nếu dùng API)"""
    global _last_proxy_mtime

    if not USE_PROXY:
        return
    if PROXY_API_URL:
        # Luôn reload mỗi lần gọi
        await load_proxies()
        logger.info("[Proxy] Reloaded proxy list from API")
        return
    # Trường hợp file txt như cũ
    filename = filename or PROXIES_FILE
    try:
        mtime = os.path.getmtime(filename)
    except FileNotFoundError:
        return
    if mtime > _last_proxy_mtime:
        await load_proxies(filename)
        _last_proxy_mtime = mtime
        logger.info("[Proxy] Reloaded proxy list from disk")

async def mark_bad_proxy(proxy: str, site_key: Optional[str] = None, *, permanent: bool = False):
    global FAILED_PROXY_TIMES

    normalized = _normalize_proxy_key(proxy)
    now = time.time()

    if site_key:
        SITE_PROXY_BLACKLIST.setdefault(site_key, {})[normalized] = now + SITE_PROXY_BLACKLIST_TTL
    else:
        COOLDOWN_PROXIES[proxy] = now + PROXY_COOLDOWN_SECONDS

    _record_proxy_failure(normalized)

    if not should_blacklist_proxy(proxy, LOADED_PROXIES) and not permanent:
        logger.warning(
            f"[Proxy] Proxy xoay hoặc pool chỉ có 1 proxy, sẽ không blacklist: {proxy}. Chỉ sleep & retry."
        )
        await asyncio.sleep(10)
        await cleanup_weak_proxies(
            retention_seconds=WEAK_PROXY_RETENTION_SECONDS,
            threshold=WEAK_PROXY_THRESHOLD,
        )
        return

    bad_proxy_counts.setdefault(normalized, 0)
    bad_proxy_counts[normalized] += 1

    if permanent or (bad_proxy_counts[normalized] >= 3 and LOADED_PROXIES):
        if _remove_proxy_by_normalized(normalized):
            logger.warning(f"Proxy '{proxy}' auto-banned do quá nhiều lỗi.")
            FAILED_PROXY_TIMES.append(time.time())
            FAILED_PROXY_TIMES[:] = [t for t in FAILED_PROXY_TIMES if time.time() - t < 60]
            if len(FAILED_PROXY_TIMES) >= MAX_FAIL_RATE:
                asyncio.create_task(
                    send_telegram_notify(
                        "[Crawl Notify] Cảnh báo: Số lượng proxy bị ban liên tục vượt ngưỡng, hãy kiểm tra lại hệ thống/proxy!",
                        status="error",
                    )
                )
                FAILED_PROXY_TIMES.clear()

    await cleanup_weak_proxies(
        retention_seconds=WEAK_PROXY_RETENTION_SECONDS,
        threshold=WEAK_PROXY_THRESHOLD,
    )

def get_random_proxy_url(
    username: str = None, password: str = None, site_key: Optional[str] = None
) -> Optional[str]:
    available = _get_available_proxies(site_key=site_key)
    if not available:
        return None
    proxy = random.choice(available)
    if "://" in proxy:
        return proxy
    if username and password:
        return f"http://{username}:{password}@{proxy}"
    return f"http://{proxy}"

def get_round_robin_proxy_url(
    username: str = None, password: str = None, site_key: Optional[str] = None
) -> Optional[str]:
    global current_proxy_index
    available = _get_available_proxies(site_key=site_key)
    if not available:
        return None
    proxy = available[current_proxy_index % len(available)]
    current_proxy_index = (current_proxy_index + 1) % len(available)
    if "://" in proxy:
        return proxy
    if username and password:
        return f"http://{username}:{password}@{proxy}"
    return f"http://{proxy}"

def get_proxy_url(
    username: str = None, password: str = None, site_key: Optional[str] = None
) -> Optional[str]:
    if not USE_PROXY:
        return None
    global PROXY_NOTIFIED
    available = _get_available_proxies(site_key=site_key)
    if not available:
        if not PROXY_NOTIFIED:
            PROXY_NOTIFIED = True
        return None
    PROXY_NOTIFIED = False

    if proxy_mode == "round_robin":
        return get_round_robin_proxy_url(username, password, site_key=site_key)
    return get_random_proxy_url(username, password, site_key=site_key)

def remove_bad_proxy(bad_proxy_url):
    if not bad_proxy_url:
        return
    normalized = _normalize_proxy_key(bad_proxy_url)
    if not normalized:
        logger.warning(f"Không tách được IP:PORT từ {bad_proxy_url}")
        return
    if _remove_proxy_by_normalized(normalized):
        logger.warning(f"Đã loại proxy lỗi: {bad_proxy_url}")
        with open("banned_proxies.log", "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {normalized} bị remove vì lỗi nhiều lần\n")
    else:
        logger.warning(f"Không tìm thấy proxy {bad_proxy_url} để remove.")

def shuffle_proxies():
    random.shuffle(LOADED_PROXIES)
    logger.info("[Proxy] Đã shuffle lại proxy pool!")
