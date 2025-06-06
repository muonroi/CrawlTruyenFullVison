import ast
import re
import unicodedata
from pathlib import Path


def _load_functions():
    source = Path('utils/chapter_utils.py').read_text()
    module = ast.parse(source)
    namespace = {'re': re}

    def _unidecode(text: str) -> str:
        text = text.replace('đ', 'd').replace('Đ', 'D')
        return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')

    namespace['unidecode'] = _unidecode

    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name in {'slugify_title', 'extract_real_chapter_number'}:
            exec(ast.get_source_segment(source, node), namespace)

    return namespace['slugify_title'], namespace['extract_real_chapter_number']


slugify_title, extract_real_chapter_number = _load_functions()


def test_slugify_title_basic():
    assert slugify_title('Chương 1: Bắt đầu!') == 'chuong-1-bat-dau'
    assert slugify_title('Hello world!? 123') == 'hello-world-123'


def test_extract_real_chapter_number_variants():
    assert extract_real_chapter_number('Chương 12') == 12
    assert extract_real_chapter_number('Chapter 5') == 5
    assert extract_real_chapter_number('001. Title') == 1
