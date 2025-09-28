"""Lightweight stub mimicking :mod:`chardet`'s ``detect`` helper."""

from __future__ import annotations


def detect(data) -> dict:
    """Return a best-effort guess for text encoding.

    The implementation always reports UTF-8 which is sufficient for the unit
    tests that only need a dictionary with an ``encoding`` key.
    """

    return {"encoding": "utf-8"}


__all__ = ["detect"]
