from __future__ import annotations

from feeds.feeds import extract_meta_image_url, parse_retry_after_seconds
import unittest


ARTICLE_URL = "https://example.com/news/story"


class ArticleImageScrapeSafetyTests(unittest.TestCase):
    """Validate article meta-image extraction and retry header parsing."""

    def test_extract_meta_image_url_prefers_og_image_property(self) -> None:
        """The first valid og:image candidate should be selected."""

        html = """
        <html>
          <head>
            <meta property=\"og:image\" content=\"https://cdn.example.com/cover.jpg\" />
            <meta name=\"twitter:image\" content=\"https://cdn.example.com/twitter.jpg\" />
          </head>
        </html>
        """

        self.assertEqual(
            extract_meta_image_url(html, ARTICLE_URL),
            "https://cdn.example.com/cover.jpg",
        )

    def test_extract_meta_image_url_resolves_relative_paths(self) -> None:
        """Relative meta image URLs should resolve against article URL."""

        html = """
        <html>
          <head>
            <meta name=\"twitter:image\" content=\"/images/social-card.png\" />
          </head>
        </html>
        """

        self.assertEqual(
            extract_meta_image_url(html, ARTICLE_URL),
            "https://example.com/images/social-card.png",
        )

    def test_extract_meta_image_url_ignores_unsupported_schemes(self) -> None:
        """Unsafe URL schemes should not be accepted as image candidates."""

        html = """
        <html>
          <head>
            <meta property=\"og:image\" content=\"javascript:alert(1)\" />
            <meta name=\"twitter:image\" content=\"data:image/png;base64,abcd\" />
          </head>
        </html>
        """

        self.assertIsNone(extract_meta_image_url(html, ARTICLE_URL))

    def test_parse_retry_after_seconds_from_numeric_value(self) -> None:
        """Retry-After delay-seconds values should parse as float seconds."""

        self.assertEqual(parse_retry_after_seconds("120"), 120.0)

    def test_parse_retry_after_seconds_returns_none_for_invalid_header(self) -> None:
        """Malformed Retry-After values should be treated as absent."""

        self.assertIsNone(parse_retry_after_seconds("not-a-valid-header"))


if __name__ == "__main__":
    unittest.main()
