"""Minimal stub for :mod:`python-dotenv` used in configuration loading."""

from __future__ import annotations


def load_dotenv(*args, **kwargs) -> bool:
    """Pretend to load environment variables and return ``False``."""

    return False


__all__ = ["load_dotenv"]
