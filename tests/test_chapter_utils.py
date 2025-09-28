
from utils.chapter_utils import (
    slugify_title,
    extract_real_chapter_number,
    remove_chapter_number_from_title,
    get_chapter_filename,
)


def test_slugify_title_basic():
    assert slugify_title('Chương 1: Bắt đầu!') == 'chuong-1-bat-dau'
    assert slugify_title('Hello world!? 123') == 'hello-world-123'


def test_extract_real_chapter_number_variants():
    assert extract_real_chapter_number('Chương 12') == 12
    assert extract_real_chapter_number('Chapter 5') == 5
    assert extract_real_chapter_number('001. Title') == 1


def test_remove_number_and_filename():
    assert remove_chapter_number_from_title('Chương 10: ABC') == 'ABC'
    assert remove_chapter_number_from_title('Chapter 9 - XYZ') == 'XYZ'
    fname = get_chapter_filename('Tiêu đề', 3)
    assert fname == '0003_tieu-de.txt'
