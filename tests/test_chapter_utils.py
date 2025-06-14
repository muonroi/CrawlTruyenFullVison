import ast
import os
import re
import unicodedata
from pathlib import Path


def _load_functions():
    source = Path("utils/chapter_utils.py").read_text()
    module = ast.parse(source)
    namespace = {"re": re, "os": os}

    def _unidecode(text: str) -> str:
        text = text.replace("đ", "d").replace("Đ", "D")
        return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")

    namespace["unidecode"] = _unidecode

    wanted = {
        "slugify_title",
        "extract_real_chapter_number",
        "remove_chapter_number_from_title",
        "get_chapter_filename",
    }
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            exec(ast.get_source_segment(source, node), namespace)

    return (
        namespace["slugify_title"],
        namespace["extract_real_chapter_number"],
        namespace["remove_chapter_number_from_title"],
        namespace["get_chapter_filename"],
    )


slugify_title, extract_real_chapter_number, remove_title_number, get_chapter_filename = _load_functions()


def test_slugify_title_basic():
    assert slugify_title('Chương 1: Bắt đầu!') == 'chuong-1-bat-dau'
    assert slugify_title('Hello world!? 123') == 'hello-world-123'


def test_extract_real_chapter_number_variants():
    assert extract_real_chapter_number('Chương 12') == 12
    assert extract_real_chapter_number('Chapter 5') == 5
    assert extract_real_chapter_number('001. Title') == 1


def test_remove_number_and_filename():
    assert remove_title_number('Chương 10: ABC') == 'ABC'
    assert remove_title_number('Chapter 9 - XYZ') == 'XYZ'
    fname = get_chapter_filename('Tiêu đề', 3)
    assert fname == '0003_tieu-de.txt'
