import pathlib

from analyze.tangthuvien_parse import (
    find_genre_listing_url,
    parse_chapter_content,
    parse_chapter_list,
    parse_genres,
    parse_story_info,
    parse_story_list,
)

FIXTURES_DIR = pathlib.Path(__file__).resolve().parent.parent / "site_info" / "tangthuvien"


def _load(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def test_parse_genres_includes_tien_hiep():
    html = _load("home.txt")
    genres = parse_genres(html, "https://tangthuvien.net")

    assert genres, "Expected at least one genre"
    names = [genre["name"] for genre in genres]
    assert "Tiên Hiệp" in names
    tien_hiep = next(g for g in genres if g["name"] == "Tiên Hiệp")
    assert tien_hiep["url"].startswith("https://tangthuvien.net/the-loai/tien-hiep")


def test_parse_genres_strips_icon_and_counts():
    html = """
    <div class="nav-list">
        <a href="/the-loai/tien-hiep">
            <span class="iconfont">&#xe004;</span>
            Tiên Hiệp
            <span class="count">2115</span>
        </a>
    </div>
    """

    genres = parse_genres(html, "https://tangthuvien.net")

    assert genres == [
        {
            "name": "Tiên Hiệp",
            "url": "https://tangthuvien.net/the-loai/tien-hiep",
        }
    ]


def test_find_genre_listing_url_prefers_full_listing():
    html = _load("home.txt")
    listing_url = find_genre_listing_url(html, "https://tangthuvien.net")

    assert listing_url == "https://tangthuvien.net/tong-hop?tp=cv&ctg=1"


def test_parse_story_list_extracts_rows():
    html = _load("home.txt")
    stories, max_pages = parse_story_list(html, "https://tangthuvien.net")

    assert stories, "Expected story list to be non-empty"
    first = stories[0]
    assert first["title"]
    assert first["url"].startswith("https://tangthuvien.net/doc-truyen/")
    assert "latest_chapter" in first
    assert max_pages >= 1


def test_parse_story_list_handles_card_layout():
    html = _load("genre_modern.html")
    stories, max_pages = parse_story_list(html, "https://tangthuvien.net")

    assert len(stories) == 2
    assert stories[0]["title"] == "Linh Giới Chi Lữ"
    assert stories[0]["latest_chapter"] == "Chương 12"
    assert stories[0]["author"] == "Tác Giả Một"
    assert stories[1]["title"] == "Kiếm Hiệp Truyện"
    assert stories[1]["latest_chapter"] == "Chương 45"
    assert stories[1]["author"] == "Tác Giả Hai"
    assert max_pages == 3


def test_parse_story_info_captures_metadata():
    html = _load("detail_story.txt")
    info = parse_story_info(html, "https://tangthuvien.net")

    assert "Từ Phế Linh Căn" in info["title"]
    assert info["author"] and info["author"].startswith("Thủ Tàn")
    assert info["status"].lower().startswith("đang")
    assert info["story_id"] == "38472"
    assert info["total_chapters_on_site"] == 689
    assert isinstance(info.get("chapters"), list)
    assert any(g["name"] == "Tiên Hiệp" for g in info["categories"])
    assert info["stats"]["likes"] == 22
    assert info["stats"]["views"] == 370845
    assert "Tu chân" in info["tags"]
    latest = info["latest_chapters"]
    assert len(latest) >= 5
    assert latest[0]["url"].endswith("/chuong-689")


def test_parse_chapter_list_from_ajax_snippet():
    html = _load("detail_story_chapter_paging.txt")
    chapters = parse_chapter_list(html, "https://tangthuvien.net")

    assert chapters, "Expected chapters to be parsed"
    assert chapters[0]["title"].startswith("Chương 76")
    assert chapters[-1]["url"].startswith("https://tangthuvien.net/doc-truyen/")


def test_parse_chapter_content_normalizes_text():
    html = _load("content_chapter.txt")
    parsed = parse_chapter_content(html)

    assert parsed is not None
    assert parsed["title"].startswith("Chương 151")
    content = parsed["content"]
    assert isinstance(content, str)
    assert "<p>" in content
    assert "Vương Dục" in content
