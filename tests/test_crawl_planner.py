from typing import Any, Dict, List, Optional

import pytest

from typing import Any, Dict, List, Optional

import pytest

from adapters.base_site_adapter import BaseSiteAdapter
from core.crawl_planner import (
    CategoryCrawlPlan,
    CrawlPlan,
    build_category_plan,
    build_crawl_plan,
)


class _StubAdapter(BaseSiteAdapter):
    site_key = "stub"

    def __init__(self) -> None:
        self._captured_calls: List[Dict[str, Any]] = []

    async def get_genres(self) -> List[Dict[str, str]]:
        return [
            {"name": "Tiên Hiệp", "url": "https://example.com/tien-hiep"},
            {"name": "Kiếm Hiệp", "url": "https://example.com/kiem-hiep"},
            {"name": "Bỏ Qua"},  # Missing URL should be ignored
        ]

    async def get_stories_in_genre(self, genre_url: str, page: int = 1):  # pragma: no cover - not used
        raise NotImplementedError

    async def get_all_stories_from_genre(
        self,
        genre_name: str,
        genre_url: str,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:  # pragma: no cover - delegated to *_with_page_check
        return []

    async def get_story_details(self, story_url: str, story_title: str):  # pragma: no cover - not needed
        raise NotImplementedError

    async def get_chapter_list(
        self,
        story_url: str,
        story_title: str,
        site_key: str,
        max_pages: Optional[int] = None,
        total_chapters: Optional[int] = None,
    ):  # pragma: no cover - not needed
        raise NotImplementedError

    async def get_chapter_content(self, chapter_url: str, chapter_title: str, site_key: str):  # pragma: no cover - not needed
        raise NotImplementedError

    async def get_all_stories_from_genre_with_page_check(
        self,
        genre_name: str,
        genre_url: str,
        site_key: str,
        max_pages: Optional[int] = None,
        *,
        page_callback=None,
        collect: bool = True,
    ):
        self._captured_calls.append(
            {
                "genre_name": genre_name,
                "genre_url": genre_url,
                "site_key": site_key,
                "max_pages": max_pages,
            }
        )
        stories = [{"title": f"{genre_name} Story", "url": genre_url + "/story"}]
        if page_callback:
            await page_callback(list(stories), 1, 3)
        return (stories if collect else [], 3, 3)


@pytest.mark.asyncio
async def test_category_plan_to_dict_roundtrip():
    plan = CategoryCrawlPlan(
        name="Tiên Hiệp",
        url="https://example.com/tien-hiep",
        stories=[{"title": "Story A"}],
        total_pages=4,
        crawled_pages=2,
        metadata={"position": 1},
        raw_genre={"name": "Tiên Hiệp", "url": "https://example.com/tien-hiep", "slug": "tien-hiep"},
    )

    payload = plan.to_dict()
    assert payload["name"] == "Tiên Hiệp"
    assert payload["url"].endswith("tien-hiep")
    assert payload["total_pages"] == 4
    assert payload["crawled_pages"] == 2
    assert payload["stories"][0]["title"] == "Story A"
    assert payload["metadata"]["position"] == 1
    assert payload["raw_genre"]["slug"] == "tien-hiep"


@pytest.mark.asyncio
async def test_crawl_plan_generation_and_batches():
    adapter = _StubAdapter()

    selected_genres = [
        {"name": "Tiên Hiệp", "url": "https://example.com/tien-hiep"},
        {"name": "Kiếm Hiệp", "url": "https://example.com/kiem-hiep"},
    ]
    plan = await build_crawl_plan(
        adapter,
        genres=selected_genres,
        max_pages=2,
        extra_metadata={"source": "unit-test"},
    )

    assert isinstance(plan, CrawlPlan)
    assert plan.site_key == "stub"
    assert plan.total_categories == 2

    mapping = plan.as_mapping()
    assert set(mapping.keys()) == {"Tiên Hiệp", "Kiếm Hiệp"}
    assert mapping["Tiên Hiệp"][0]["title"] == "Tiên Hiệp Story"

    batches = plan.split_into_batches(batch_size=1)
    assert len(batches) == 2
    assert batches[0] == {"Tiên Hiệp": mapping["Tiên Hiệp"]}

    as_dict = plan.to_dict()
    assert as_dict["total_categories"] == 2
    first_meta = as_dict["categories"][0]["metadata"]
    assert first_meta["position"] == 1
    assert first_meta["total_genres"] == 2
    assert first_meta["source"] == "unit-test"
    assert as_dict["categories"][0]["raw_genre"]["name"] == "Tiên Hiệp"

    assert adapter._captured_calls == [
        {
            "genre_name": "Tiên Hiệp",
            "genre_url": "https://example.com/tien-hiep",
            "site_key": "stub",
            "max_pages": 2,
        },
        {
            "genre_name": "Kiếm Hiệp",
            "genre_url": "https://example.com/kiem-hiep",
            "site_key": "stub",
            "max_pages": 2,
        },
    ]


@pytest.mark.asyncio
async def test_build_category_plan_handles_metadata(monkeypatch):
    adapter = _StubAdapter()

    plan = await build_category_plan(
        adapter,
        {"name": "Tiên Hiệp", "url": "https://example.com/tien-hiep", "extra": 1},
        "stub",
        position=2,
        total_genres=5,
        max_pages=1,
        extra_metadata={"batch": "demo"},
    )

    assert isinstance(plan, CategoryCrawlPlan)
    assert plan.metadata["position"] == 2
    assert plan.metadata["total_genres"] == 5
    assert plan.metadata["batch"] == "demo"
    assert plan.raw_genre["extra"] == 1


def test_split_into_batches_rejects_invalid_size():
    plan = CrawlPlan(site_key="stub")
    with pytest.raises(ValueError):
        plan.split_into_batches(0)

