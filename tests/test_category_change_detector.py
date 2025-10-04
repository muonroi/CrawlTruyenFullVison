from core.category_change_detector import CategoryChangeDetector


def test_category_change_detector_signature_order_invariant():
    detector = CategoryChangeDetector(ratio_threshold=0.5, absolute_threshold=5, min_story_count=1)
    stories_a = [
        {"title": "A", "url": "http://example.com/a"},
        {"title": "B", "url": "http://example.com/b"},
    ]
    stories_b = list(reversed(stories_a))

    first = detector.evaluate(stories_a, None)
    second = detector.evaluate(stories_b, None)

    assert first.signature.url_checksum == second.signature.url_checksum
    assert first.signature.content_signature == second.signature.content_signature


def test_category_change_detector_detects_large_changes():
    detector = CategoryChangeDetector(ratio_threshold=0.2, absolute_threshold=2, min_story_count=1)

    baseline = [
        {"title": "A", "url": "http://example.com/a"},
        {"title": "B", "url": "http://example.com/b"},
        {"title": "C", "url": "http://example.com/c"},
    ]

    baseline_result = detector.evaluate(baseline, None)

    changed = [
        {"title": "D", "url": "http://example.com/d"},
        {"title": "E", "url": "http://example.com/e"},
        {"title": "F", "url": "http://example.com/f"},
    ]

    result = detector.evaluate(changed, {
        "url_checksum": baseline_result.signature.url_checksum,
        "content_signature": baseline_result.signature.content_signature,
        "urls": baseline_result.urls,
        "story_count": baseline_result.signature.story_count,
    })

    assert result.requires_refresh is True
    assert result.change_count == 6


def test_category_change_detector_no_refresh_for_minor_updates():
    detector = CategoryChangeDetector(ratio_threshold=0.6, absolute_threshold=10, min_story_count=10)

    baseline = [
        {"title": f"Story {i}", "url": f"http://example.com/{i}"}
        for i in range(1, 21)
    ]

    baseline_result = detector.evaluate(baseline, None)

    changed = baseline[:-1] + [{"title": "New Story", "url": "http://example.com/new"}]

    result = detector.evaluate(
        changed,
        {
            "url_checksum": baseline_result.signature.url_checksum,
            "content_signature": baseline_result.signature.content_signature,
            "urls": baseline_result.urls,
            "story_count": baseline_result.signature.story_count,
        },
    )

    assert result.requires_refresh is False
