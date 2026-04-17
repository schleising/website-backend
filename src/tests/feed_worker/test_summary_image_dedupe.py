from __future__ import annotations

import unittest

from feed_summary_images import strip_duplicate_summary_image


SOURCE_URL = "https://example.com/feed.xml"


class FeedSummaryImageDedupeTests(unittest.TestCase):
    """Verify duplicate summary images are removed when they match media_image_url."""

    def test_removes_matching_absolute_image_tag(self) -> None:
        summary = (
            "<p>Lead</p>"
            '<img src="https://cdn.example.com/hero.jpg" alt="hero">'
            "<p>Body</p>"
        )

        deduped = strip_duplicate_summary_image(
            summary,
            "https://cdn.example.com/hero.jpg",
            SOURCE_URL,
        )

        self.assertIsNotNone(deduped)
        assert deduped is not None
        self.assertNotIn("<img", deduped)
        self.assertIn("<p>Lead</p>", deduped)
        self.assertIn("<p>Body</p>", deduped)

    def test_removes_matching_relative_image_tag(self) -> None:
        summary = '<img src="/images/hero.jpg" alt="hero"><p>Body</p>'

        deduped = strip_duplicate_summary_image(
            summary,
            "https://example.com/images/hero.jpg",
            SOURCE_URL,
        )

        self.assertEqual(deduped, "<p>Body</p>")

    def test_keeps_non_matching_image_tag(self) -> None:
        summary = '<img src="https://cdn.example.com/other.jpg" alt="other"><p>Body</p>'

        deduped = strip_duplicate_summary_image(
            summary,
            "https://cdn.example.com/hero.jpg",
            SOURCE_URL,
        )

        self.assertEqual(deduped, summary)

    def test_returns_none_when_only_duplicate_image_remains(self) -> None:
        summary = '<img src="https://cdn.example.com/hero.jpg" alt="hero">'

        deduped = strip_duplicate_summary_image(
            summary,
            "https://cdn.example.com/hero.jpg",
            SOURCE_URL,
        )

        self.assertIsNone(deduped)

    def test_keeps_summary_when_media_url_missing(self) -> None:
        summary = "<p>Body only</p>"

        deduped = strip_duplicate_summary_image(summary, None, SOURCE_URL)

        self.assertEqual(deduped, summary)


if __name__ == "__main__":
    unittest.main()
