from urllib.parse import urlparse
from config.config import BASE_URLS
from adapters.factory import get_adapter

def resolve_site_key(src, fallback_url=None, default_site_key=None):
    if isinstance(src, dict):
        return src.get("site_key") or src.get("site") or (get_site_key_from_url(src.get("url")) if src.get("url") else default_site_key)
    elif isinstance(src, str):
        return get_site_key_from_url(src)
    elif fallback_url:
        return get_site_key_from_url(fallback_url)
    return default_site_key

def get_site_key_from_url(url):
    if not url:
        return None
    url_host = urlparse(url).netloc.lower()
    for key, base in BASE_URLS.items():
        base_host = urlparse(base).netloc.lower()
        if url_host == base_host:
            return key
    return None

def is_url_for_site(url, site_key):
    if not url or not site_key:
        return False
    url_host = urlparse(url).netloc.lower()
    base = BASE_URLS.get(site_key)
    if not base:
        return False
    base_host = urlparse(base).netloc.lower()
    return url_host == base_host

def get_adapter_from_url(url):
    key = get_site_key_from_url(url)
    if key:
        return get_adapter(key), key
    return None, None
