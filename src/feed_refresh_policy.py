from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def _coerce_utc_datetime(value: Any) -> datetime | None:
    """Normalize a datetime value to timezone-aware UTC."""

    if not isinstance(value, datetime):
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def source_needs_fetch(
    source_doc: dict[str, Any],
    now: datetime,
    fetch_interval: timedelta,
) -> bool:
    """Return True when a feed source should be fetched now.

    Sources are fetched when they have never been fetched, when the regular
    interval has elapsed, or when an explicit force-refresh request is newer
    than the last successful fetch timestamp.
    """

    last_fetched_at = _coerce_utc_datetime(source_doc.get("last_fetched_at"))
    force_refresh_requested_at = _coerce_utc_datetime(
        source_doc.get("force_refresh_requested_at")
    )

    if force_refresh_requested_at is not None:
        if last_fetched_at is None or force_refresh_requested_at > last_fetched_at:
            return True

    next_retry_at = _coerce_utc_datetime(source_doc.get("next_retry_at"))
    if next_retry_at is not None and next_retry_at > now:
        return False

    if last_fetched_at is None:
        return True

    return last_fetched_at <= (now - fetch_interval)
