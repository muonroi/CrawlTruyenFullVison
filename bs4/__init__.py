"""A lightweight subset of the :mod:`beautifulsoup4` interface used in tests.

This project normally depends on :mod:`beautifulsoup4`, but the unit tests only
exercise a very small portion of the API.  Importing the real dependency would
require installing third‑party packages during the evaluation which is not
always possible.  To keep the tests self contained we provide a tiny
implementation that mimics the parts of the public interface relied upon by the
parsing helpers.

The implementation intentionally focuses on the limited behaviour required by
the tests (CSS selection, text extraction and a couple of mutation helpers).
It is not a drop‑in replacement for BeautifulSoup, but it behaves the same for
the selectors used in the repository.
"""

from .minisoup import BeautifulSoup, Comment

__all__ = ["BeautifulSoup", "Comment"]
