from __future__ import annotations

import unittest

from feeds.feed_summary_images import (
    extract_first_summary_image_url,
    strip_duplicate_summary_image,
)


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


class FeedSummaryImageFallbackTests(unittest.TestCase):
    """Verify fallback extraction of the first valid summary image URL."""

    def test_extracts_first_absolute_image_url(self) -> None:
        summary = (
            "<p>Lead</p>"
            '<img src="https://cdn.example.com/hero.jpg" alt="hero">'
            '<img src="https://cdn.example.com/secondary.jpg" alt="secondary">'
        )

        result = extract_first_summary_image_url(summary, SOURCE_URL)

        self.assertEqual(result, "https://cdn.example.com/hero.jpg")

    def test_extracts_first_relative_image_url_as_absolute(self) -> None:
        summary = '<img src="/images/hero.jpg" alt="hero"><p>Body</p>'

        result = extract_first_summary_image_url(summary, SOURCE_URL)

        self.assertEqual(result, "https://example.com/images/hero.jpg")

    def test_skips_invalid_url_and_uses_next_valid_image(self) -> None:
        summary = (
            '<img src="javascript:alert(1)" alt="bad">'
            '<img src="https://cdn.example.com/hero.jpg" alt="hero">'
        )

        result = extract_first_summary_image_url(summary, SOURCE_URL)

        self.assertEqual(result, "https://cdn.example.com/hero.jpg")

    def test_returns_none_when_no_valid_image_exists(self) -> None:
        summary = "<p>No image</p>"

        result = extract_first_summary_image_url(summary, SOURCE_URL)

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
