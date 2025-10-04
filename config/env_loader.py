"""Utility helpers for loading strongly-typed configuration from environment variables."""
from __future__ import annotations

import os
from typing import Callable, Iterable, List, Optional, Sequence, TypeVar

from dotenv import load_dotenv, find_dotenv

# Load environment variables from a .env file if available. If a concrete .env
# file is not present, fall back to .env.example so local development and tests
# still receive deterministic defaults while remaining overrideable.
_primary_env = find_dotenv(usecwd=True)
if _primary_env:
    load_dotenv(_primary_env, override=False)
else:  # pragma: no cover - defensive path for environments without .env
    _example_env = find_dotenv(".env.example", usecwd=True)
    if _example_env:
        load_dotenv(_example_env, override=False)

__all__ = [
    "EnvironmentConfigurationError",
    "get_str",
    "get_int",
    "get_float",
    "get_bool",
    "get_list",
]


class EnvironmentConfigurationError(RuntimeError):
    """Raised when the application configuration is invalid or incomplete."""


_T = TypeVar("_T")

_TRUE_VALUES = {"1", "true", "t", "yes", "y", "on"}
_FALSE_VALUES = {"0", "false", "f", "no", "n", "off"}


def _apply_cast(name: str, raw: str, caster: Callable[[str], _T]) -> _T:
    try:
        return caster(raw)
    except Exception as exc:  # pragma: no cover - defensive guard
        raise EnvironmentConfigurationError(
            f"Environment variable {name} has invalid value: {raw!r}"
        ) from exc


def _get_raw(name: str, *, required: bool = False, allow_empty: bool = False) -> Optional[str]:
    """Return the raw environment value while enforcing required/empty rules."""

    value = os.getenv(name)
    if value is None:
        if required:
            raise EnvironmentConfigurationError(
                f"Missing required environment variable: {name}"
            )
        return None

    if not allow_empty:
        value = value.strip()
        if not value:
            if required:
                raise EnvironmentConfigurationError(
                    f"Environment variable {name} must not be empty"
                )
            return None
    return value


def get_str(name: str, *, required: bool = False, allow_empty: bool = False) -> Optional[str]:
    """Return a string environment value or ``None`` when not provided."""

    return _get_raw(name, required=required, allow_empty=allow_empty)


def get_int(name: str, *, required: bool = False) -> Optional[int]:
    """Return an integer parsed from the environment."""

    raw = _get_raw(name, required=required)
    if raw is None:
        return None
    return _apply_cast(name, raw, int)


def get_float(name: str, *, required: bool = False) -> Optional[float]:
    """Return a float parsed from the environment."""

    raw = _get_raw(name, required=required)
    if raw is None:
        return None
    return _apply_cast(name, raw, float)


def get_bool(name: str, *, required: bool = False) -> Optional[bool]:
    """Return a boolean parsed from the environment."""

    raw = _get_raw(name, required=required)
    if raw is None:
        return None

    normalized = raw.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise EnvironmentConfigurationError(
        f"Environment variable {name} must be a boolean value (true/false), got {raw!r}"
    )


def get_list(
    name: str,
    *,
    required: bool = False,
    separator: str = ",",
    allow_empty: bool = False,
) -> Optional[List[str]]:
    """Return a list parsed from a separated environment value."""

    raw = _get_raw(name, required=required, allow_empty=allow_empty)
    if raw is None:
        return [] if required and allow_empty else None

    if not raw and not allow_empty:
        return []

    parts = [item.strip() for item in raw.split(separator)]
    return [item for item in parts if item]
