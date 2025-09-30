import pathlib

from analyze.xtruyen_parse import (
    parse_chapter_content,
    parse_chapter_list,
    parse_genres,
    parse_story_list,
)


FIXTURES_DIR = pathlib.Path(__file__).resolve().parent.parent / "site_info" / "xtruyen"
TMP_DIR = pathlib.Path(__file__).resolve().parent.parent / "tmp_data" / "xtruyen"


def _load_fixture(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def test_parse_genres_extracts_expected_links():
    html = _load_fixture(FIXTURES_DIR / "home.txt")
    genres = parse_genres(html, "https://xtruyen.vn")

    assert any(g["name"] == "Tiên Hiệp" for g in genres)
    tien_hiep = next(g for g in genres if g["name"] == "Tiên Hiệp")
    assert tien_hiep["url"].startswith("https://xtruyen.vn/theloai/tien-hiep/")


def test_parse_story_list_reads_cards():
    html = _load_fixture(FIXTURES_DIR / "category.txt")
    stories, max_pages = parse_story_list(html, "https://xtruyen.vn")

    assert stories, "Expected story list to be non-empty"
    first = stories[0]
    assert first["title"]
    assert first["url"].startswith("https://xtruyen.vn/truyen/")
    assert max_pages >= 1


def test_parse_chapter_list_keeps_natural_order():
    html = _load_fixture(FIXTURES_DIR / "load_list_chapter.txt")
    chapters = parse_chapter_list(html, "https://xtruyen.vn")

    assert chapters, "Expected chapters to be parsed"
    assert chapters[0]["title"].startswith("Chương 1")
    assert chapters[1]["title"].startswith("Chương 2")


def test_parse_chapter_content_decompresses_payload():
    html = _load_fixture(TMP_DIR / "content_chapter.txt")
    parsed = parse_chapter_content(html)

    assert parsed is not None
    assert parsed["title"].startswith("Chương 1")
    assert "Nửa đêm canh ba" in parsed["content"]
