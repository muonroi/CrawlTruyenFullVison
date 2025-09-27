"""Stub of :mod:`pydantic` providing a minimal :class:`BaseModel`."""

from __future__ import annotations


class BaseModel:  # pragma: no cover - minimal stand-in
    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


__all__ = ["BaseModel"]
