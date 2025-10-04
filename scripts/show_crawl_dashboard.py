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

    if sites:
        print("Sức khỏe site:")
        for site in sorted(sites, key=lambda item: item.get("failure_rate", 0), reverse=True):
            failure_rate = site.get("failure_rate", 0.0)
            print(
                f"  - {site.get('site_key')}: {site.get('success', 0)} OK / {site.get('failure', 0)} lỗi"
                f" (tỷ lệ lỗi {failure_rate * 100:.2f}%)"
            )
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
