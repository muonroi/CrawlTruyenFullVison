"""Helpers for vivutruyen.com scraping logic."""
from __future__ import annotations

from urllib.parse import urljoin

def absolutize(url: str, base_url: str = "https://vivutruyen.com") -> str:
    """Return an absolute URL using the site's canonical base."""
    base = base_url or "https://vivutruyen.com"
    return urljoin(base, url)
