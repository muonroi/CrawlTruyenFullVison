#!/usr/bin/env python3
"""Hiển thị trạng thái crawl hiện tại dựa trên ``logs/dashboard.json``."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional

DEFAULT_DASHBOARD_FILE = os.environ.get(
    "STORYFLOW_DASHBOARD_FILE",
    os.path.join("logs", "dashboard.json"),
)

for stream in (sys.stdout, sys.stderr):
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8")



def load_dashboard(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Không tìm thấy dashboard tại {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def format_genre_summary(genre: Dict[str, Any]) -> List[str]:
    site_key = genre.get("site_key") or "-"
    name = genre.get("name") or "?"
    position = _coerce_int(genre.get("position"))
    total_genres = _coerce_int(genre.get("total_genres"))
    prefix = f"[{site_key}] "
    if position and total_genres:
        prefix += f"({position}/{total_genres}) "

    total_pages = _coerce_int(genre.get("total_pages"))
    crawled_pages = _coerce_int(genre.get("crawled_pages")) or 0
    completed_pages = max(crawled_pages, 0)
    if total_pages:
        completed_pages = min(completed_pages, total_pages)
        page_progress = f"{completed_pages}/{total_pages}"
    else:
        page_progress = str(completed_pages)
    current_page = _coerce_int(genre.get("current_page"))
    status = genre.get("status") or ""
    page_note = None
    if current_page and current_page > completed_pages:
        page_note = f"đang lấy trang {current_page}"
    elif status == "processing_stories" and total_pages:
        page_note = "đã quét đủ"

    processed = _coerce_int(genre.get("processed_stories")) or 0
    processed = max(processed, 0)
    total_stories = _coerce_int(genre.get("total_stories"))
    total_stories_display = "?" if total_stories is None else str(max(total_stories, 0))
    story_progress = f"{processed}/{total_stories_display}"

    active_details = genre.get("active_story_details") or []
    active_stories = genre.get("active_stories") or []
    active_count = len(active_details) or len(active_stories)

    headline = f"  - {prefix}{name} — Trang {page_progress}"
    if page_note:
        headline += f" ({page_note})"
    headline += f", Truyện {story_progress}"
    if active_count:
        headline += f" (đang mở {active_count} truyện)"
    lines_out: List[str] = [headline]

    if status:
        lines_out.append(f"      Trạng thái: {status.replace('_', ' ')}")

    current_story_title = genre.get("current_story_title")
    current_story_page = _coerce_int(genre.get("current_story_page"))
    current_story_position = _coerce_int(genre.get("current_story_position"))
    if current_story_title:
        detail_bits = []
        if current_story_page:
            detail_bits.append(f"trang {current_story_page}")
        if current_story_position:
            detail_bits.append(f"vị trí #{current_story_position}")
        suffix = f" ({', '.join(detail_bits)})" if detail_bits else ""
        lines_out.append(f"      Đang xử lý: {current_story_title}{suffix}")

    if active_details:
        rendered = []
        for item in active_details[:5]:
            title = item.get("title")
            if not title:
                continue
            bits = []
            page_val = _coerce_int(item.get("page"))
            if page_val:
                bits.append(f"trang {page_val}")
            pos_val = _coerce_int(item.get("position"))
            if pos_val:
                bits.append(f"#{pos_val}")
            note = " (" + ", ".join(bits) + ")" if bits else ""
            rendered.append(f"{title}{note}")
        if rendered:
            lines_out.append("      Hàng đợi truyện: " + "; ".join(rendered))
    elif active_stories:
        preview = ", ".join(active_stories[:5])
        lines_out.append(f"      Tiến độ truyện: {preview}")
        if len(active_stories) > 5:
            lines_out.append(f"      ... và {len(active_stories) - 5} truyện khác")

    last_error = genre.get("last_error")
    if last_error:
        lines_out.append(f"      Lỗi gần nhất: {last_error}")

    return lines_out

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
            for line in format_genre_summary(genre):
                print(line)
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
