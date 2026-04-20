from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


MAX_REFRESH_INTERVAL = timedelta(minutes=30)
MIN_REFRESH_INTERVAL = timedelta(minutes=10)


def _coerce_utc_datetime(value: Any) -> datetime | None:
    """Normalize a datetime value to timezone-aware UTC."""

    if not isinstance(value, datetime):
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def _coerce_positive_int(value: Any) -> int | None:
    """Return a positive integer value when coercion succeeds."""

    if isinstance(value, bool):
        return None

    if isinstance(value, int):
        return value if value > 0 else None

    if isinstance(value, float):
        as_int = int(value)
        return as_int if as_int > 0 else None

    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None

        try:
            as_int = int(float(stripped))
        except ValueError:
            return None

        return as_int if as_int > 0 else None

    return None


def resolve_source_refresh_interval(
    source_doc: dict[str, Any],
    default_fetch_interval: timedelta,
) -> timedelta:
    """Resolve the effective source refresh interval from persisted metadata."""

    min_seconds = int(MIN_REFRESH_INTERVAL.total_seconds())
    max_seconds = int(MAX_REFRESH_INTERVAL.total_seconds())
    default_seconds = max(
        min_seconds,
        min(max_seconds, int(default_fetch_interval.total_seconds())),
    )
    persisted_seconds = _coerce_positive_int(source_doc.get("refresh_interval_seconds"))
    effective_seconds = persisted_seconds if persisted_seconds is not None else default_seconds
    return timedelta(seconds=max(min_seconds, min(max_seconds, effective_seconds)))


def source_needs_fetch(
    source_doc: dict[str, Any],
    now: datetime,
    default_fetch_interval: timedelta,
    max_refresh_lag: timedelta = timedelta(minutes=2),
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

    next_refresh_at = _coerce_utc_datetime(source_doc.get("next_refresh_at"))
    effective_interval = resolve_source_refresh_interval(source_doc, default_fetch_interval)
    bounded_lag = max(timedelta(0), max_refresh_lag)

    if last_fetched_at is None:
        if next_refresh_at is not None:
            return next_refresh_at <= now

        return True

    # Never fetch before the effective interval has elapsed, even if persisted
    # scheduling metadata is more aggressive from older policy versions.
    earliest_allowed_refresh = last_fetched_at + effective_interval
    if now < earliest_allowed_refresh:
        return False

    # Enforce the maximum allowed delay from the expected interval cadence.
    latest_allowed_refresh = earliest_allowed_refresh + bounded_lag
    if now >= latest_allowed_refresh:
        return True

    if next_refresh_at is not None:
        return next_refresh_at <= now

    return last_fetched_at <= (now - effective_interval)
