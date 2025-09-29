"""Utilities for parsing Vivutruyen content."""
from __future__ import annotations

from urllib.parse import urljoin


def absolutize(url: str | None, base_url: str | None = None) -> str:
    """Return an absolute URL for ``url``.

    Parameters
    ----------
    url:
        The input URL or path.  If ``None`` or an empty string is
        provided an empty string is returned.
    base_url:
        Base URL used for resolving relative paths.  When ``None`` the
        Vivutruyen domain is used.
    """

    if not url:
        return ""

    stripped = url.strip()
    if stripped.startswith(("http://", "https://")):
        return stripped

    base = (base_url or "https://vivutruyen.com").rstrip("/")
    return urljoin(base + '/', stripped.lstrip('/'))
