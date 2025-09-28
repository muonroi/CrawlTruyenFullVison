"""Quick regression test for XTruyen chapter payload decoding."""
from pathlib import Path
import unicodedata

from analyze.xtruyen_parse import parse_chapter_content

FIXTURE = Path(__file__).resolve().parents[1] / "site_info" / "xtruyen" / "content_chapter.txt"


def _normalize(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")


def test_decompresses_chapter_payload_and_strips_ads():
    html = FIXTURE.read_text(encoding="utf-8")
    result = parse_chapter_content(html)

    title = result["title"] or ""
    normalized_title = _normalize(title).lower()
    assert "chuong 1" in normalized_title
    assert "gia tien su" in normalized_title

    content = result["content"]
    assert content, "Expected decompressed chapter content"
    assert "<p>" in content
    assert len(content) > 5000

    lowered = content.lower()
    assert "/hdsd/xaudio.php" not in lowered
    assert "data-cl-spot" not in lowered
    assert "div class=\"ads\"" not in lowered
    assert "canh ba" in lowered