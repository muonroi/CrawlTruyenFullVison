import json
from utils.chapter_utils import get_missing_chapters

def test_get_missing_chapters_detects_missing_numbers(tmp_path):
    story_dir = tmp_path / "demo"
    story_dir.mkdir()

    chapter_meta = [
        {"index": 1, "title": "Chương 1", "url": "u1", "file": "0001_chuong-1.txt"},
        {"index": 2, "title": "Chương 2", "url": "u2", "file": "0002_chuong-2.txt"},
        {"index": 3, "title": "Chương 3", "url": "u3", "file": "0003_chuong-3.txt"},
        {"index": 4, "title": "Chương 4", "url": "u4", "file": "0004_chuong-4.txt"},
    ]
    (story_dir / "chapter_metadata.json").write_text(
        json.dumps(chapter_meta, ensure_ascii=False), encoding="utf-8"
    )

    # Existing chapters: 1 and 3 only.
    (story_dir / "0001_chuong-1.txt").write_text("c1", encoding="utf-8")
    (story_dir / "0003_chuong-3.txt").write_text("c3", encoding="utf-8")

    # Index 4 is marked dead -> should not be considered missing.
    dead_data = [{"index": 4, "url": "u4"}]
    (story_dir / "dead_chapters.json").write_text(
        json.dumps(dead_data, ensure_ascii=False), encoding="utf-8"
    )

    chapters_from_web = [
        {"index": item["index"], "title": item["title"], "url": item["url"]}
        for item in chapter_meta
    ]

    missing = get_missing_chapters(str(story_dir), chapters_from_web, "test_site")

    assert len(missing) == 1
    assert missing[0]["index"] == 2
    assert missing[0]["url"] == "u2"
