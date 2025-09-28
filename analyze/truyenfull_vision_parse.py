"""Utility helpers shared by truyenfull vision scraping."""
from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup


def get_input_value(soup: BeautifulSoup, input_id: str, default: str | None = None) -> str | None:
    """Return the ``value`` attribute of the input with ``input_id`` if present."""
    if soup is None:
        return default
    element = soup.find("input", id=input_id)
    if element is None:
        return default
    value: Any = element.get("value")
    return value if value is not None else default
