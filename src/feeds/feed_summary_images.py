from __future__ import annotations

import re
from html import unescape
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

IMG_TAG_RE = re.compile(r"<img\b[^>]*>", re.IGNORECASE)
SRC_ATTR_RE = re.compile(
    r"\bsrc\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|([^\s\"'=<>`]+))",
    re.IGNORECASE,
)
# Future/Tom's Guide CDN RSS media URLs append "-{width}-{height}" before the extension
# while inline article HTML uses the bare asset filename.
CDN_DIMENSION_SUFFIX_RE = re.compile(r"-\d+-\d+(\.[^./?#]+)$", re.IGNORECASE)


def _extract_img_src(tag: str) -> str | None:
    """Extract an img src attribute value from one <img> tag string."""

    match = SRC_ATTR_RE.search(tag)
    if not match:
        return None

    for candidate in match.groups():
        if isinstance(candidate, str) and candidate.strip() != "":
            return candidate.strip()

    return None


def normalize_summary_image_url(candidate: Any, source_url: str) -> str | None:
    """Normalize summary image URLs to comparable absolute HTTP(S) URLs."""

    if not isinstance(candidate, str):
        return None

    # Feed HTML often keeps entity-encoded query separators (&amp;) while
    # media:* attribute URLs are XML-decoded to bare (&).
    trimmed = unescape(candidate).strip()
    if trimmed == "":
        return None

    normalized = urljoin(source_url, trimmed)
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or parsed.netloc == "":
        return None

    # Ignore fragment differences when matching duplicate article images.
    return parsed._replace(fragment="").geturl()


def canonical_image_asset_path(path: str) -> str:
    """Normalize CDN asset paths for duplicate matching."""

    decoded_path = unquote(path or "")
    return CDN_DIMENSION_SUFFIX_RE.sub(r"\1", decoded_path)


def image_asset_identity(candidate: Any, source_url: str) -> str | None:
    """Return a CDN-stable identity (scheme/host/path) for duplicate matching."""

    normalized = normalize_summary_image_url(candidate, source_url)
    if normalized is None:
        return None

    parsed = urlparse(normalized)
    path = canonical_image_asset_path(parsed.path or "")
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"


def summary_image_urls_match(
    left: Any,
    right: Any,
    source_url: str,
) -> bool:
    """Return whether two image URLs refer to the same article asset."""

    left_normalized = normalize_summary_image_url(left, source_url)
    right_normalized = normalize_summary_image_url(right, source_url)
    if left_normalized is None or right_normalized is None:
        return False

    if left_normalized == right_normalized:
        return True

    # Same asset with different CDN transform query params (width/format/etc).
    return image_asset_identity(left, source_url) == image_asset_identity(right, source_url)


def extract_first_summary_image_url(summary_html: str | None, source_url: str) -> str | None:
    """Return the first valid summary <img> URL normalized to an absolute HTTP(S) URL."""

    if not isinstance(summary_html, str) or summary_html.strip() == "":
        return None

    for match in IMG_TAG_RE.finditer(summary_html):
        src_value = _extract_img_src(match.group(0))
        if src_value is None:
            continue

        normalized = normalize_summary_image_url(src_value, source_url)
        if normalized is not None:
            return normalized

    return None


def strip_duplicate_summary_image(
    summary_html: str | None,
    media_image_url: str | None,
    source_url: str,
) -> str | None:
    """Remove inline summary <img> tags that duplicate the primary media image URL."""

    if not isinstance(summary_html, str) or summary_html.strip() == "":
        return None

    canonical_media_url = normalize_summary_image_url(media_image_url, source_url)
    if canonical_media_url is None:
        return summary_html

    def _replace_if_duplicate(match: re.Match[str]) -> str:
        img_tag = match.group(0)
        src_value = _extract_img_src(img_tag)
        if src_value is None:
            return img_tag

        if summary_image_urls_match(src_value, media_image_url, source_url):
            return ""

        return img_tag

    deduped = IMG_TAG_RE.sub(_replace_if_duplicate, summary_html).strip()
    return deduped or None
