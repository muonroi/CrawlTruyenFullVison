"""Parsing helpers for the Truyenfull Vision site."""
from __future__ import annotations

from bs4 import BeautifulSoup


def get_input_value(soup: BeautifulSoup, element_id: str, default: str | None = None) -> str | None:
    """Return the value of an ``<input>`` element with ``element_id``.

    Parameters
    ----------
    soup:
        Parsed BeautifulSoup document that contains the input elements.
    element_id:
        The ``id`` attribute to search for.
    default:
        Value returned when the input cannot be located or does not have
        a ``value`` attribute.
    """

    if not soup or not element_id:
        return default

    element = soup.find('input', id=element_id)
    if not element:
        return default

    value = element.get('value')
    if value is None:
        return default

    return value
