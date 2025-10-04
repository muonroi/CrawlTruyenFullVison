#!/usr/bin/env python3
"""Hiển thị trạng thái crawl hiện tại dựa trên ``logs/dashboard.json``."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict

DEFAULT_DASHBOARD_FILE = os.environ.get(
    "STORYFLOW_DASHBOARD_FILE",
    os.path.join("logs", "dashboard.json"),
)


def load_dashboard(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Không tìm thấy dashboard tại {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def print_dashboard(data: Dict[str, Any]) -> None:
    aggregates = data.get("aggregates", {})
    sites = data.get("sites", [])
    print("=== StoryFlow Crawl Dashboard ===")
    print(f"Cập nhật lúc: {data.get('updated_at', '-')}")
    print()
    print("Tổng quan:")
    print(f"  - Truyện đang crawl : {aggregates.get('stories_in_progress', 0)}")
    print(f"  - Truyện hoàn thành: {aggregates.get('stories_completed', 0)}")
    print(f"  - Truyện bị skip   : {aggregates.get('stories_skipped', 0)}")
    print(f"  - Tổng chương thiếu: {aggregates.get('total_missing_chapters', 0)}")
    print(f"  - Hàng đợi skip    : {aggregates.get('skipped_queue_size', 0)}")
    genres_total = aggregates.get("genres_total_configured")
    genres_done = aggregates.get("genres_total_completed", 0)
    if genres_total:
        print(f"  - Thể loại hoàn thành: {genres_done}/{genres_total}")
    else:
        print(f"  - Thể loại hoàn thành: {genres_done}")
    print()

    active = data.get("stories", {}).get("in_progress", [])
    if active:
        print("Đang crawl:")
        for story in active[:10]:  # show top 10
            print(
                f"  * {story.get('title')} — {story.get('crawled_chapters', 0)}/"
                f"{story.get('total_chapters', 0)} chương, còn thiếu {story.get('missing_chapters', 0)}"
            )
        if len(active) > 10:
            print(f"  ... và {len(active) - 10} truyện khác")
        print()

    active_genres = data.get("genres", {}).get("in_progress", [])
    if active_genres:
        print("Thể loại đang xử lý:")
        for genre in active_genres[:10]:
            total_pages = genre.get("total_pages") or "?"
            current_page = genre.get("current_page") or genre.get("crawled_pages") or 0
            total_stories = genre.get("total_stories") or "?"
            processed = genre.get("processed_stories", 0)
            position = genre.get("position")
            total_genres = genre.get("total_genres")
            prefix = f"[{genre.get('site_key')}] "
            if position and total_genres:
                prefix += f"({position}/{total_genres}) "
            print(
                f"  - {prefix}{genre.get('name')} — Trang {current_page}/{total_pages}, Truyện {processed}/{total_stories}"
            )
            active_stories = genre.get("active_stories") or []
            if active_stories:
                joined = ", ".join(active_stories[:5])
                print(f"      Đang crawl: {joined}")
        if len(active_genres) > 10:
            print(f"  ... và {len(active_genres) - 10} thể loại khác")
        print()

    if sites:
        print("Sức khỏe site:")
        for site in sorted(sites, key=lambda item: item.get("failure_rate", 0), reverse=True):
            failure_rate = site.get("failure_rate", 0.0)
            print(
                f"  - {site.get('site_key')}: {site.get('success', 0)} OK / {site.get('failure', 0)} lỗi"
                f" (tỷ lệ lỗi {failure_rate * 100:.2f}%)"
            )
        print()

    site_genres = data.get("site_genres", [])
    if site_genres:
        print("Tổng kết thể loại theo site:")
        for site in site_genres:
            total = site.get("total_genres") or 0
            completed = site.get("completed_genres") or 0
            print(
                f"  - {site.get('site_key')}: {completed}/{total} thể loại, cập nhật {site.get('updated_at', '-')}")
            for genre in site.get("genres", [])[:10]:
                status = genre.get("status", "completed")
                extra = f" ({status})" if status != "completed" else ""
                print(
                    f"      * {genre.get('name')} — {genre.get('stories', 0)} truyện{extra}"
                )
            if len(site.get("genres", [])) > 10:
                print(f"      ... và {len(site['genres']) - 10} thể loại khác")
        print()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Hiển thị dashboard crawl hiện tại")
    parser.add_argument(
        "--file",
        default=DEFAULT_DASHBOARD_FILE,
        help="Đường dẫn file dashboard.json (mặc định: logs/dashboard.json)",
    )
    args = parser.parse_args(argv)
    try:
        dashboard = load_dashboard(args.file)
    except FileNotFoundError as exc:
        print(exc)
        return 1
    except json.JSONDecodeError as exc:
        print(f"File dashboard hỏng: {exc}")
        return 1

    print_dashboard(dashboard)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main(sys.argv[1:]))
