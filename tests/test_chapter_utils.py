import json

import pytest

from utils import chapter_utils
from utils.chapter_utils import (
    slugify_title,
    extract_real_chapter_number,
    remove_chapter_number_from_title,
    get_chapter_filename,
    async_save_chapter_with_hash_check,
)


def test_slugify_title_basic():
    assert slugify_title('Chương 1: Bắt đầu!') == 'chuong-1-bat-dau'
    assert slugify_title('Hello world!? 123') == 'hello-world-123'


def test_slugify_title_handles_none():
    assert slugify_title(None) == ""


def test_extract_real_chapter_number_variants():
    assert extract_real_chapter_number('Chương 12') == 12
    assert extract_real_chapter_number('Chapter 5') == 5
    assert extract_real_chapter_number('001. Title') == 1


def test_remove_number_and_filename():
    assert remove_chapter_number_from_title('Chương 10: ABC') == 'ABC'
    assert remove_chapter_number_from_title('Chapter 9 - XYZ') == 'XYZ'
    fname = get_chapter_filename('Tiêu đề', 3)
    assert fname == '0003_tieu-de.txt'


def test_get_missing_chapters_skip_when_source_missing(tmp_path):
    story_folder = tmp_path / "story"
    story_folder.mkdir()

    metadata = [
        {
            "index": 1,
            "title": "Chương 1: Start",
            "url": "https://example.com/chap-1",
            "file": chapter_utils.get_chapter_filename("Chương 1: Start", 1),
        },
        {
            "index": 2,
            "title": "Chương 2: Missing",
            "url": "https://example.com/chap-2",
            "file": chapter_utils.get_chapter_filename("Chương 2: Missing", 2),
        },
        {
            "index": 3,
            "title": "Chương 3: Present",
            "url": "https://example.com/chap-3",
            "file": chapter_utils.get_chapter_filename("Chương 3: Present", 3),
        },
    ]

    meta_path = story_folder / "chapter_metadata.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    # Tạo sẵn file cho chương 1 và 3
    for item in (metadata[0], metadata[2]):
        file_path = story_folder / item["file"]
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("content")

    # Danh sách chương lấy từ web không có chương 2
    chapters_from_web = [
        {"title": "Chương 1: Start", "url": "https://example.com/chap-1"},
        {"title": "Chương 3: Present", "url": "https://example.com/chap-3"},
    ]

    missing = chapter_utils.get_missing_chapters(str(story_folder), chapters_from_web)

    # Không nên cố crawl lại chương 2 vì website không có link/chương này
    assert missing == []


def test_get_missing_chapters_detects_offset(tmp_path):
    story_folder = tmp_path / "story"
    story_folder.mkdir()

    metadata = [
        {
            "index": 1,
            "title": "Chương 1: Start",
            "url": "https://a/1",
            "file": chapter_utils.get_chapter_filename("Chương 1: Start", 1),
        },
        {
            "index": 2,
            "title": "Chương 2: Middle",
            "url": "https://a/2",
            "file": chapter_utils.get_chapter_filename("Chương 2: Middle", 2),
        },
        {
            "index": 3,
            "title": "Chương 3: End",
            "url": "https://a/3",
            "file": chapter_utils.get_chapter_filename("Chương 3: End", 3),
        },
    ]

    meta_path = story_folder / "chapter_metadata.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    # Đã có chương 1 và 2 ở local
    for item in metadata[:2]:
        with open(story_folder / item["file"], "w", encoding="utf-8") as f:
            f.write("content")

    chapters_from_web = [
        {"title": "Chương 2: Start", "url": "https://b/2"},
        {"title": "Chương 3: Middle", "url": "https://b/3"},
        {"title": "Chương 4: End", "url": "https://b/4"},
    ]

    missing = chapter_utils.get_missing_chapters(str(story_folder), chapters_from_web)

    assert [m["index"] for m in missing] == [3]
    assert chapters_from_web[-1]["aligned_index"] == 3


def test_get_missing_chapters_skip_inconsistent_source(tmp_path, caplog):
    story_folder = tmp_path / "story"
    story_folder.mkdir()

    metadata = [
        {
            "index": 1,
            "title": "Chương 1: Start",
            "url": "https://a/1",
            "file": chapter_utils.get_chapter_filename("Chương 1: Start", 1),
        },
    ]

    meta_path = story_folder / "chapter_metadata.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    # Có sẵn chương 1
    with open(story_folder / metadata[0]["file"], "w", encoding="utf-8") as f:
        f.write("content")

    inconsistent = [
        {"title": "Chương 10: Completely Different", "url": "https://b/10"},
        {"title": "Chương 11: Wrong", "url": "https://b/11"},
    ]

    with caplog.at_level("WARNING"):
        missing = chapter_utils.get_missing_chapters(str(story_folder), inconsistent)

    assert missing == []
    assert any("[MISMATCH]" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_async_save_chapter_with_hash_check_rejects_none(tmp_path):
    target = tmp_path / "chapter.txt"
    with pytest.raises(ValueError):
        await async_save_chapter_with_hash_check(str(target), None)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_async_save_chapter_with_hash_check_accepts_bytes(tmp_path):
    target = tmp_path / "chapter_bytes.txt"
    result = await async_save_chapter_with_hash_check(str(target), b"Noi dung thu nghiem")
    assert result == "new"
    assert target.exists()
    content = target.read_text(encoding="utf-8")
    assert "Noi dung thu nghiem" in content


@pytest.mark.asyncio
async def test_async_save_chapter_with_hash_check_rejects_non_string_like(tmp_path):
    target = tmp_path / "chapter_invalid.txt"
    with pytest.raises(TypeError):
        await async_save_chapter_with_hash_check(str(target), 12345)  # type: ignore[arg-type]

