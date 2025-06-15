import time
from typing import Any, Callable, Dict, Tuple

# Simple in-memory TTL cache for async functions

def _build_key(prefix: str, args: Tuple[Any, ...]) -> Tuple[str, Tuple[Any, ...]]:
    return (prefix, args)

class AsyncTTLCache:
    def __init__(self, ttl: float = 300.0):
        self.ttl = ttl
        self.cache: Dict[Tuple[str, Tuple[Any, ...]], Tuple[float, Any]] = {}

    def get(self, key: Tuple[str, Tuple[Any, ...]]):
        now = time.time()
        val = self.cache.get(key)
        if val and now - val[0] < self.ttl:
            return val[1]
        if val:
            self.cache.pop(key, None)
        return None

    def set(self, key: Tuple[str, Tuple[Any, ...]], value: Any):
        self.cache[key] = (time.time(), value)

cache = AsyncTTLCache()

async def cached_get_story_details(adapter, url: str, title: str, ttl: float = 300.0):
    key = _build_key("story", (getattr(adapter, "SITE_KEY", str(id(adapter))), url))
    val = cache.get(key)
    if val is not None:
        return val
    result = await adapter.get_story_details(url, title)
    cache.set(key, result)
    return result

async def cached_get_chapter_list(
    adapter,
    url: str,
    title: str,
    site_key: str,
    total_chapters: int | None = None,
    ttl: float = 300.0,
):
    key = _build_key("chapters", (site_key, url))
    val = cache.get(key)
    if val is not None:
        return val
    result = await adapter.get_chapter_list(url, title, site_key, total_chapters=total_chapters)
    cache.set(key, result)
    return result
