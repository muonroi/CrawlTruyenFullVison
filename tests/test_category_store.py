import sqlite3
from pathlib import Path

import pytest

from core.category_store import CategoryStore
from core.crawl_planner import CategoryCrawlPlan, CrawlPlan


@pytest.mark.parametrize("versions", [("v1", "v2"), (None, "v2")])
def test_category_store_persists_snapshots(tmp_path, versions):
    db_path = Path(tmp_path) / "store.sqlite3"
    store = CategoryStore(str(db_path))

    first_plan = CrawlPlan(site_key="demo")
    first_plan.add_category(
        CategoryCrawlPlan(
            name="Tiên hiệp",
            url="http://example.com/tien-hiep",
            stories=[{"title": "Story A", "url": "http://example.com/s1"}],
            metadata={"position": 1},
            raw_genre={"name": "Tiên hiệp", "url": "http://example.com/tien-hiep"},
        )
    )

    v1, v2 = versions
    info1 = store.persist_snapshot("demo", first_plan, version=v1)
    assert info1.site_key == "demo"
    assert info1.id > 0
    if v1:
        assert info1.version == v1

    second_plan = CrawlPlan(site_key="demo")
    second_plan.add_category(
        CategoryCrawlPlan(
            name="Tiên hiệp",
            url="http://example.com/tien-hiep",
            stories=[
                {"title": "Story B", "url": "http://example.com/s2"},
                {"title": "Story A", "url": "http://example.com/s1", "extra": True},
            ],
        )
    )

    info2 = store.persist_snapshot("demo", second_plan, version=v2)
    assert info2.id != info1.id
    if v2:
        assert info2.version == v2

    third_plan = CrawlPlan(site_key="demo")
    third_plan.add_category(
        CategoryCrawlPlan(
            name="Tiên hiệp",
            url="http://example.com/tien-hiep",
            stories=[{"title": "Story A", "url": "http://example.com/s1", "extra": True}],
        )
    )

    info3 = store.persist_snapshot("demo", third_plan, version="v3")
    assert info3.id != info2.id

    with sqlite3.connect(db_path) as conn:
        stories = conn.execute(
            "SELECT url, title FROM stories ORDER BY url"
        ).fetchall()
        assert [(row[0], row[1]) for row in stories] == [
            ("http://example.com/s1", "Story A"),
            ("http://example.com/s2", "Story B"),
        ]

        active = conn.execute(
            "SELECT COUNT(*) FROM category_story_membership WHERE ended_snapshot_id IS NULL"
        ).fetchone()[0]
        assert active == 1

        ended = conn.execute(
            "SELECT COUNT(*) FROM category_story_membership WHERE ended_snapshot_id IS NOT NULL"
        ).fetchone()[0]
        assert ended >= 1

        story_counts = conn.execute(
            "SELECT story_count, ended_snapshot_id FROM categories"
        ).fetchall()
        assert story_counts == [(1, None)]
