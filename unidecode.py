"""Minimal drop-in replacement for :mod:`Unidecode` used in tests.

The real library provides comprehensive transliteration.  For the unit tests we
only rely on accent stripping for Latin characters when building filenames.
This simplified implementation normalises the text using NFKD and removes
combining marks.  It intentionally leaves non-Latin characters untouched which
is sufficient for the code paths exercised by the tests.
"""

from __future__ import annotations

import unicodedata


def unidecode(text: str) -> str:
    if not isinstance(text, str):
        raise TypeError("unidecode expects a string input")
    replacements = str.maketrans({"đ": "d", "Đ": "D"})
    normalised = unicodedata.normalize("NFKD", text.translate(replacements))
    return "".join(ch for ch in normalised if not unicodedata.combining(ch))


__all__ = ["unidecode"]
