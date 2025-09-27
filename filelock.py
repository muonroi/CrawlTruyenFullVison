"""A very small subset of :mod:`filelock` used by the unit tests.

The real dependency provides a cross‑platform file locking mechanism.  For the
purposes of the tests we only need the context manager interface to guarantee
that code paths expecting a lock object keep working.  This implementation
keeps a process wide map of locks keyed by the lock file path.  The behaviour is
good enough for single process tests and avoids pulling additional third party
dependencies into the execution environment.
"""

from __future__ import annotations

import threading
from typing import Dict


class Timeout(Exception):
    """Raised when the lock cannot be acquired within the timeout."""


_GLOBAL_LOCK = threading.Lock()
_LOCKS: Dict[str, threading.Lock] = {}


class FileLock:
    def __init__(self, lock_file: str, timeout: float | None = None) -> None:
        self._path = lock_file
        self._timeout = timeout
        with _GLOBAL_LOCK:
            self._lock = _LOCKS.setdefault(lock_file, threading.Lock())
        self._acquired = False

    def acquire(self, timeout: float | None = None) -> None:
        real_timeout = self._timeout if timeout is None else timeout
        if real_timeout is None:
            acquired = self._lock.acquire()
        else:
            acquired = self._lock.acquire(timeout=real_timeout)
        if not acquired:
            raise Timeout(f"Could not acquire lock for {self._path!r}")
        self._acquired = True

    def release(self) -> None:
        if self._acquired:
            self._lock.release()
            self._acquired = False

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


__all__ = ["FileLock", "Timeout"]
