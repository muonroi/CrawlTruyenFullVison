"""Ensure vendored third-party dependencies are importable without pip install."""
from __future__ import annotations

import os
import sys

_VENDOR_DIR = os.path.join(os.path.dirname(__file__), "vendor")
if os.path.isdir(_VENDOR_DIR) and _VENDOR_DIR not in sys.path:
    sys.path.insert(0, _VENDOR_DIR)
