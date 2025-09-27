"""Minimal HTML parsing helpers compatible with the project tests.

The goal of this module is to offer a very small subset of the behaviour
provided by :mod:`beautifulsoup4`.  Only the pieces that are required by the
unit tests are implemented: a DOM representation, CSS selectors for simple
queries, text extraction helpers and a couple of mutation helpers.

The implementation is intentionally tiny but battle tested by the repository's
tests.  It should be easy to extend in the future should other helpers require
additional behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Dict, Iterable, Iterator, List, Optional, Sequence


def _normalise_class(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item for item in value.split() if item]


@dataclass
class NavigableString:
    text: str
    parent: Optional["Tag"] = None

    def __str__(self) -> str:  # pragma: no cover - debugging helper
        return self.text

    def extract(self) -> "NavigableString":
        if self.parent is not None:
            try:
                self.parent.children.remove(self)
            except ValueError:
                pass
            self.parent = None
        return self


class Comment(NavigableString):
    pass


@dataclass
class Tag:
    name: Optional[str]
    attrs: Dict[str, object]
    parent: Optional["Tag"] = None
    children: List[object] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Tree helpers
    # ------------------------------------------------------------------
    def _iter_descendants(self) -> Iterator["Tag"]:
        for child in self.children:
            if isinstance(child, Tag):
                yield child
                yield from child._iter_descendants()

    # ------------------------------------------------------------------
    # Public API resembling BeautifulSoup
    # ------------------------------------------------------------------
    def select(self, selector: str) -> List["Tag"]:
        results: List[Tag] = []
        for part in selector.split(","):
            part = part.strip()
            if not part:
                continue
            results.extend(self._select_sequence(part))
        # Maintain document order but avoid duplicates
        seen: set[int] = set()
        ordered: List[Tag] = []
        for tag in results:
            ident = id(tag)
            if ident not in seen:
                seen.add(ident)
                ordered.append(tag)
        return ordered

    def select_one(self, selector: str) -> Optional["Tag"]:
        found = self.select(selector)
        return found[0] if found else None

    def __call__(self, name: Optional[str] = None, recursive: bool = True, text=None, **kwargs):
        if text is not None and name is None and not kwargs:
            return self._find_text_nodes(text, recursive)
        return self.find_all(name=name, recursive=recursive, **kwargs)

    def find_all(
        self,
        name: Optional[str] = None,
        recursive: bool = True,
        **kwargs,
    ) -> List["Tag"]:
        matches: List[Tag] = []

        def check(node: Tag) -> None:
            for child in node.children:
                if not isinstance(child, Tag):
                    continue
                if _matches_name(child, name) and _matches_kwargs(child, kwargs):
                    matches.append(child)
                if recursive:
                    check(child)

        check(self)
        return matches

    def _find_text_nodes(self, text_filter, recursive: bool) -> List[NavigableString]:
        matches: List[NavigableString] = []

        def check(node: "Tag") -> None:
            for child in node.children:
                if isinstance(child, NavigableString):
                    if _matches_text(child, text_filter):
                        matches.append(child)
                elif isinstance(child, Tag) and recursive:
                    check(child)

        check(self)
        return matches

    def get_text(self, separator: str | None = "", strip: bool = False) -> str:
        pieces: List[str] = []

        def gather(node: object) -> None:
            if isinstance(node, NavigableString):
                text = node.text
                if strip:
                    text = text.strip()
                if text:
                    pieces.append(text)
            elif isinstance(node, Tag):
                for child in node.children:
                    gather(child)

        for child in self.children:
            gather(child)

        if separator is None:
            separator = ""
        return separator.join(pieces)

    def get(self, key: str, default: object | None = None) -> object | None:
        key = "class" if key == "class_" else key
        return self.attrs.get(key, default)

    def __getitem__(self, key: str) -> object:
        key = "class" if key == "class_" else key
        if key not in self.attrs:
            raise KeyError(key)
        return self.attrs[key]

    def has_attr(self, key: str) -> bool:
        key = "class" if key == "class_" else key
        return key in self.attrs

    def decompose(self) -> None:
        if self.parent is None:
            return
        try:
            self.parent.children.remove(self)
        except ValueError:
            pass

    def replace_with(self, value: object) -> None:
        if self.parent is None:
            return
        replacement: object
        if isinstance(value, Tag):
            replacement = value
            value.parent = self.parent
        else:
            replacement = NavigableString(str(value), self.parent)
        for idx, child in enumerate(self.parent.children):
            if child is self:
                self.parent.children[idx] = replacement
                break

    @property
    def string(self) -> Optional[str]:
        texts = [child.text for child in self.children if isinstance(child, NavigableString)]
        if not texts:
            return None
        if any(isinstance(child, Tag) for child in self.children):
            return None
        return "".join(texts)

    def find(
        self,
        name: Optional[str] = None,
        recursive: bool = True,
        **kwargs,
    ) -> Optional["Tag"]:
        found = self.find_all(name=name, recursive=recursive, **kwargs)
        return found[0] if found else None

    # ------------------------------------------------------------------
    # CSS selection helpers
    # ------------------------------------------------------------------
    def _select_sequence(self, selector: str) -> List["Tag"]:
        tokens = [token for token in selector.split() if token]
        current: List[Tag] = [self]
        for token in tokens:
            current = self._descendant_match(current, token)
        return current

    def _descendant_match(self, elements: Sequence["Tag"], token: str) -> List["Tag"]:
        parsed = _parse_simple_selector(token)
        matches: List[Tag] = []
        for element in elements:
            iterable = element._iter_descendants()
            for child in iterable:
                if _matches_selector(child, parsed):
                    matches.append(child)
        return matches


# ----------------------------------------------------------------------
# CSS selector parsing
# ----------------------------------------------------------------------

@dataclass
class _AttrSelector:
    name: str
    operator: Optional[str] = None
    value: Optional[str] = None


@dataclass
class _ParsedSelector:
    tag: Optional[str] = None
    element_id: Optional[str] = None
    classes: List[str] = field(default_factory=list)
    attrs: List[_AttrSelector] = field(default_factory=list)


def _parse_simple_selector(token: str) -> _ParsedSelector:
    parsed = _ParsedSelector()
    buf: List[str] = []
    i = 0
    length = len(token)

    def flush_tag() -> None:
        if buf and parsed.tag is None:
            parsed.tag = "".join(buf)
        buf.clear()

    while i < length:
        ch = token[i]
        if ch in {".", "#", "["}:
            flush_tag()
        if ch == ".":
            i += 1
            start = i
            while i < length and token[i] not in ".#[":
                i += 1
            parsed.classes.append(token[start:i])
            continue
        if ch == "#":
            i += 1
            start = i
            while i < length and token[i] not in ".#[":
                i += 1
            parsed.element_id = token[start:i]
            continue
        if ch == "[":
            end = token.find("]", i)
            if end == -1:
                break
            content = token[i + 1 : end]
            operator = None
            value = None
            if "*=" in content:
                name, value = content.split("*=", 1)
                operator = "*="
            elif "=" in content:
                name, value = content.split("=", 1)
                operator = "="
            else:
                name = content
            name = name.strip()
            if value is not None:
                value = value.strip().strip('"\'')
            parsed.attrs.append(_AttrSelector(name=name, operator=operator, value=value))
            i = end + 1
            continue
        buf.append(ch)
        i += 1

    flush_tag()
    if parsed.tag:
        parsed.tag = parsed.tag.lower()
    return parsed


def _matches_selector(tag: Tag, selector: _ParsedSelector) -> bool:
    if selector.tag and tag.name != selector.tag:
        return False
    if selector.element_id:
        if str(tag.attrs.get("id", "")) != selector.element_id:
            return False
    if selector.classes:
        tag_classes = tag.attrs.get("class", [])
        if isinstance(tag_classes, str):
            tag_classes = _normalise_class(tag_classes)
        for cls in selector.classes:
            if cls not in tag_classes:
                return False
    for attr in selector.attrs:
        value = tag.attrs.get(attr.name)
        if value is None:
            return False
        if isinstance(value, list):
            candidate = " ".join(str(v) for v in value)
        else:
            candidate = str(value)
        if attr.operator == "=":
            if candidate != (attr.value or ""):
                return False
        elif attr.operator == "*=":
            if attr.value is None or attr.value not in candidate:
                return False
    return True


def _matches_name(tag: Tag, name: Optional[object]) -> bool:
    if name is None:
        return True
    if isinstance(name, str):
        return tag.name == name
    if isinstance(name, Iterable):
        return tag.name in name
    return False


def _matches_kwargs(tag: Tag, filters: Dict[str, object]) -> bool:
    for key, expected in filters.items():
        attr_name = "class" if key == "class_" else key
        value = tag.attrs.get(attr_name)
        if expected is True:
            if value is None:
                return False
            continue
        if expected is False:
            if value is not None:
                return False
            continue
        if value is None:
            return False
        if attr_name == "class":
            classes = value if isinstance(value, list) else _normalise_class(str(value))
            if isinstance(expected, (list, tuple, set)):
                if not set(expected).issubset(set(classes)):
                    return False
            else:
                if expected not in classes:
                    return False
        else:
            if str(value) != str(expected):
                return False
    return True


def _matches_text(node: NavigableString, test) -> bool:
    if callable(test):
        return bool(test(node))
    if test is None:
        return True
    return str(node) == str(test)


# ----------------------------------------------------------------------
# HTML parsing
# ----------------------------------------------------------------------


class _SoupHTMLParser(HTMLParser):
    def __init__(self, root: Tag) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: List[Tag] = [root]

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        attrs_dict: Dict[str, object] = {}
        for key, value in attrs:
            value = value or ""
            if key == "class":
                attrs_dict[key] = _normalise_class(value)
            else:
                attrs_dict[key] = value
        parent = self.stack[-1]
        element = Tag(tag.lower(), attrs_dict, parent)
        parent.children.append(element)
        self.stack.append(element)

    def handle_startendtag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        for idx in range(len(self.stack) - 1, 0, -1):
            if self.stack[idx].name == tag:
                del self.stack[idx:]
                break

    def handle_data(self, data: str) -> None:
        if not data:
            return
        parent = self.stack[-1]
        parent.children.append(NavigableString(data, parent))

    def handle_comment(self, data: str) -> None:
        parent = self.stack[-1]
        parent.children.append(Comment(data, parent))


class BeautifulSoup:
    def __init__(self, markup: str, _parser: str | None = None) -> None:
        self._root = Tag("[document]", {})
        parser = _SoupHTMLParser(self._root)
        parser.feed(markup)

    # Delegated public API -------------------------------------------------
    def select(self, selector: str) -> List[Tag]:
        return self._root.select(selector)

    def select_one(self, selector: str) -> Optional[Tag]:
        return self._root.select_one(selector)

    def find_all(self, name: Optional[str] = None) -> List[Tag]:
        return self._root.find_all(name)

    def get_text(self, separator: str | None = "", strip: bool = False) -> str:
        return self._root.get_text(separator=separator, strip=strip)

    def __iter__(self) -> Iterator[object]:  # pragma: no cover - convenience
        return iter(self._root.children)

    @property
    def body(self) -> Optional[Tag]:
        bodies = [tag for tag in self._root._iter_descendants() if tag.name == "body"]
        return bodies[0] if bodies else None

    def select_or_self(self, selector: str) -> List[Tag]:  # pragma: no cover - helper
        matches = self.select(selector)
        return matches or [self._root]

    def find(self, name: Optional[str] = None, recursive: bool = True, **kwargs) -> Optional[Tag]:
        return self._root.find(name=name, recursive=recursive, **kwargs)


__all__ = ["BeautifulSoup", "Tag", "NavigableString", "Comment"]
