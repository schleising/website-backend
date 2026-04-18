from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin, urlparse

IMG_TAG_RE = re.compile(r"<img\b[^>]*>", re.IGNORECASE)
SRC_ATTR_RE = re.compile(
    r"\bsrc\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|([^\s\"'=<>`]+))",
    re.IGNORECASE,
)


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

    trimmed = candidate.strip()
    if trimmed == "":
        return None

    normalized = urljoin(source_url, trimmed)
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or parsed.netloc == "":
        return None

    # Ignore fragment differences when matching duplicate article images.
    return parsed._replace(fragment="").geturl()


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

        canonical_src = normalize_summary_image_url(src_value, source_url)
        if canonical_src == canonical_media_url:
            return ""

        return img_tag

    deduped = IMG_TAG_RE.sub(_replace_if_duplicate, summary_html).strip()
    return deduped or None
