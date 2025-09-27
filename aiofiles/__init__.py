"""Very small asynchronous file helper used for the unit tests.

The functions mimic the subset of :mod:`aiofiles` utilised in the project.  The
implementation wraps synchronous file operations inside async functions so that
the rest of the code can ``await`` them without pulling the external
dependency.
"""

from __future__ import annotations

import io
from typing import Optional
import builtins


class _AsyncFile:
    def __init__(self, file_obj: io.TextIOBase | io.BufferedIOBase) -> None:
        self._file = file_obj

    async def __aenter__(self) -> "_AsyncFile":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._file.close()

    async def read(self, *args, **kwargs):
        return self._file.read(*args, **kwargs)

    async def write(self, data) -> int:
        written = self._file.write(data)
        self._file.flush()
        return written

    async def readline(self, *args, **kwargs):
        return self._file.readline(*args, **kwargs)

    async def readlines(self, *args, **kwargs):
        return self._file.readlines(*args, **kwargs)


def open(file: str, mode: str = "r", encoding: Optional[str] = None):
    file_obj = builtins.open(file, mode, encoding=encoding)
    return _AsyncFile(file_obj)


__all__ = ["open"]
