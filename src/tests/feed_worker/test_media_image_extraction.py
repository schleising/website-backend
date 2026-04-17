from __future__ import annotations

from feed_entry_media import extract_largest_media_image_url
import unittest


SOURCE_URL = "https://example.com/feed.xml"


class FeedMediaImageExtractionTests(unittest.TestCase):
    """Validate media-tag image extraction behavior for feed entries."""

    def test_extract_largest_media_image_prefers_biggest_area(self) -> None:
        """The largest image candidate from media_content should be selected."""

        entry = {
            "media_content": [
                {"url": "https://cdn.example.com/small.jpg", "width": "240", "height": "135"},
                {"url": "https://cdn.example.com/large.jpg", "width": "1280", "height": "720"},
                {"url": "https://cdn.example.com/medium.jpg", "width": "640", "height": "360"},
            ]
        }

        self.assertEqual(
            extract_largest_media_image_url(entry, SOURCE_URL),
            "https://cdn.example.com/large.jpg",
        )

    def test_extract_largest_media_image_resolves_relative_urls(self) -> None:
        """Relative media URLs should be resolved against the source URL."""

        entry = {
            "media_thumbnail": [
                {"url": "/images/thumb-640.jpg", "width": "640", "height": "360"}
            ]
        }

        self.assertEqual(
            extract_largest_media_image_url(entry, SOURCE_URL),
            "https://example.com/images/thumb-640.jpg",
        )

    def test_extract_largest_media_image_uses_image_enclosure_when_media_tags_absent(self) -> None:
        """Image enclosure links should be considered when media_content is missing."""

        entry = {
            "links": [
                {
                    "rel": "enclosure",
                    "type": "image/jpeg",
                    "href": "https://cdn.example.com/enclosure.jpg",
                }
            ]
        }

        self.assertEqual(
            extract_largest_media_image_url(entry, SOURCE_URL),
            "https://cdn.example.com/enclosure.jpg",
        )

    def test_extract_largest_media_image_rejects_non_http_urls(self) -> None:
        """Unsafe or unsupported URL schemes should be ignored."""

        entry = {
            "media_content": [
                {"url": "javascript:alert(1)", "width": "1024", "height": "768"},
                {"url": "data:image/png;base64,abcd", "width": "120", "height": "120"},
            ]
        }

        self.assertIsNone(extract_largest_media_image_url(entry, SOURCE_URL))


if __name__ == "__main__":
    unittest.main()
