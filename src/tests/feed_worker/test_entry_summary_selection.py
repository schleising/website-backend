from __future__ import annotations

from typing import Any, cast
import unittest

from feeds.feeds import Feeds
from task_scheduler import TaskScheduler


SOURCE_URL = "https://example.com/feed.xml"


class _NoopScheduler:
    def schedule_task(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class FeedEntrySummarySelectionTests(unittest.TestCase):
    """Verify feed entry body selection keeps rich HTML formatting when available."""

    def setUp(self) -> None:
        self.worker = Feeds(cast(TaskScheduler, _NoopScheduler()))

    def test_prefers_html_content_block_over_first_plain_text_block(self) -> None:
        entry = {
            "link": "https://example.com/article",
            "title": "Article",
            "content": [
                {"value": "Plain text variant", "type": "text/plain"},
                {
                    "value": "<p><strong>Bold</strong> and <em>italic</em></p>",
                    "type": "text/html",
                },
            ],
            "summary": "Plain summary",
        }

        parsed = self.worker._parse_feed_entry(SOURCE_URL, entry)

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.summary_html, "<p><strong>Bold</strong> and <em>italic</em></p>")

    def test_prefers_summary_html_when_content_variant_is_plain_text(self) -> None:
        entry = {
            "link": "https://example.com/article",
            "title": "Article",
            "content": [
                {"value": "Plain text variant", "type": "text/plain"},
            ],
            "summary": "<p><b>Bold</b> <i>italic</i></p>",
        }

        parsed = self.worker._parse_feed_entry(SOURCE_URL, entry)

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.summary_html, "<p><b>Bold</b> <i>italic</i></p>")

    def test_detects_html_even_without_content_type_hint(self) -> None:
        entry = {
            "link": "https://example.com/article",
            "title": "Article",
            "content": [
                {"value": "<div><em>Rich body</em></div>"},
            ],
            "summary": "Plain summary",
        }

        parsed = self.worker._parse_feed_entry(SOURCE_URL, entry)

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.summary_html, "<div><em>Rich body</em></div>")


if __name__ == "__main__":
    unittest.main()
