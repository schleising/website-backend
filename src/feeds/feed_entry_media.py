from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

from .url_safety import is_public_http_url


def _iter_media_items(value: Any) -> list[dict[str, Any]]:
    """Return media candidate dictionaries from a feedparser value."""

    if isinstance(value, dict):
        return [value]

    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]

    return []


def _parse_positive_int(value: Any) -> int:
    """Parse numeric width/height hints to a positive integer when possible."""

    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return 0

    return parsed if parsed > 0 else 0


def _normalize_image_url(candidate: Any, source_url: str) -> str | None:
    """Normalize a candidate image URL and enforce HTTP(S)."""

    if not isinstance(candidate, str):
        return None

    raw_value = candidate.strip()
    if raw_value == "":
        return None

    absolute_url = urljoin(source_url, raw_value)
    if not is_public_http_url(absolute_url, require_dns_resolution=False):
        return None

    return absolute_url


def _add_media_candidate(
    candidates: list[tuple[int, int, int, str]],
    url_value: Any,
    source_url: str,
    width_value: Any,
    height_value: Any,
) -> None:
    """Append one normalized media candidate with a size-based score."""

    normalized_url = _normalize_image_url(url_value, source_url)
    if normalized_url is None:
        return

    width = _parse_positive_int(width_value)
    height = _parse_positive_int(height_value)
    area_score = width * height if width > 0 and height > 0 else max(width, height)

    candidates.append((area_score, width, height, normalized_url))


def extract_largest_media_image_url(entry: dict[str, Any], source_url: str) -> str | None:
    """Extract the largest image URL from media-tag style feed entry metadata."""

    candidates: list[tuple[int, int, int, str]] = []

    for media_item in _iter_media_items(entry.get("media_content")):
        _add_media_candidate(
            candidates,
            media_item.get("url") or media_item.get("href"),
            source_url,
            media_item.get("width"),
            media_item.get("height"),
        )

    for media_item in _iter_media_items(entry.get("media_thumbnail")):
        _add_media_candidate(
            candidates,
            media_item.get("url") or media_item.get("href"),
            source_url,
            media_item.get("width"),
            media_item.get("height"),
        )

    for media_group_item in _iter_media_items(entry.get("media_group")):
        for nested_item in _iter_media_items(media_group_item.get("media_content")):
            _add_media_candidate(
                candidates,
                nested_item.get("url") or nested_item.get("href"),
                source_url,
                nested_item.get("width"),
                nested_item.get("height"),
            )
        for nested_item in _iter_media_items(media_group_item.get("media_thumbnail")):
            _add_media_candidate(
                candidates,
                nested_item.get("url") or nested_item.get("href"),
                source_url,
                nested_item.get("width"),
                nested_item.get("height"),
            )

    for link_item in _iter_media_items(entry.get("links")):
        link_type = str(link_item.get("type", "")).strip().lower()
        link_rel = str(link_item.get("rel", "")).strip().lower()
        if not link_type.startswith("image/") and link_rel != "enclosure":
            continue

        _add_media_candidate(
            candidates,
            link_item.get("href") or link_item.get("url"),
            source_url,
            link_item.get("width"),
            link_item.get("height"),
        )

    image_entry = entry.get("image")
    if isinstance(image_entry, dict):
        _add_media_candidate(
            candidates,
            image_entry.get("href") or image_entry.get("url"),
            source_url,
            image_entry.get("width"),
            image_entry.get("height"),
        )

    if len(candidates) == 0:
        return None

    # Prefer larger dimensions first; tie-break with natural lexical URL stability.
    _, _, _, best_url = max(candidates, key=lambda item: (item[0], item[1], item[2], item[3]))
    return best_url
