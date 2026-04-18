from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import hashlib
import importlib
import logging
import os
from time import mktime
from typing import Any
from urllib.parse import urljoin, urlparse

from bson import ObjectId
from pymongo.errors import AutoReconnect, NetworkTimeout, ServerSelectionTimeoutError
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from feed_entry_media import extract_largest_media_image_url
from feed_summary_images import strip_duplicate_summary_image

from feed_refresh_policy import resolve_source_refresh_interval, source_needs_fetch
from task_scheduler import TaskScheduler

from . import (
    FEED_ARTICLES_COLLECTION,
    FEED_SOURCES_COLLECTION,
    USER_ARTICLE_STATES_COLLECTION,
    USER_FEED_SUBSCRIPTIONS_COLLECTION,
)
from .models import FeedArticleDocument

FETCH_INTERVAL = timedelta(
    seconds=max(5, int(os.getenv("FEEDS_FETCH_INTERVAL_SECONDS", "900")))
)
CYCLE_INTERVAL = timedelta(
    seconds=max(5, int(os.getenv("FEEDS_CYCLE_INTERVAL_SECONDS", "15")))
)
MAX_SCHEDULE_LAG = timedelta(
    seconds=min(120, max(0, int(os.getenv("FEEDS_MAX_REFRESH_LAG_SECONDS", "120"))))
)
SOFT_DELETE_AFTER = timedelta(days=max(1, int(os.getenv("FEEDS_SOFT_DELETE_DAYS", "7"))))
HARD_DELETE_AFTER = timedelta(days=max(1, int(os.getenv("FEEDS_HARD_DELETE_DAYS", "30"))))
REQUEST_TIMEOUT_SECONDS = 20
MAX_SUMMARY_LENGTH = 60_000
FAILURE_MODE = os.getenv("FEEDS_FAILURE_MODE", "none").strip().lower()


@dataclass(slots=True)
class ParsedEntry:
    """Normalized article fields extracted from one feed entry."""

    dedupe_key: str
    title: str
    link: str
    author: str | None
    summary_html: str | None
    published_at: datetime | None
    media_image_url: str | None


class Feeds:
    """Background feed worker that fetches unique subscriptions and applies retention."""

    def __init__(self, scheduler: TaskScheduler) -> None:
        self.scheduler = scheduler
        self.requests_session = self._build_session()

        self.scheduler.schedule_task(
            datetime.now(timezone.utc),
            self.run_cycle,
            CYCLE_INTERVAL,
        )

    def _build_session(self) -> requests.Session:
        """Create a requests session with conservative retry behavior."""

        session = requests.Session()
        retry_policy = Retry(
            total=3,
            backoff_factor=0.4,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_policy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": "website3-feed-worker/1.0",
                "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9,*/*;q=0.8",
            }
        )
        return session

    def _resolve_refresh_interval(self, source_doc: dict[str, Any]) -> timedelta:
        """Return effective source refresh interval (default or persisted TTL-derived)."""

        return resolve_source_refresh_interval(source_doc, FETCH_INTERVAL)

    def _compute_next_refresh_at(
        self,
        source_id: ObjectId,
        last_refresh_at: datetime,
        refresh_interval: timedelta,
    ) -> datetime:
        """Compute next refresh time using bounded deterministic stagger."""

        normalized_interval = max(timedelta(seconds=5), refresh_interval)
        stagger_budget = min(MAX_SCHEDULE_LAG, normalized_interval)

        if stagger_budget <= timedelta(0):
            return last_refresh_at + normalized_interval

        seed = f"{source_id}:{int(normalized_interval.total_seconds())}"
        digest = hashlib.sha256(seed.encode("utf-8")).digest()
        stagger_seconds = int.from_bytes(digest[:2], "big") % (
            int(stagger_budget.total_seconds()) + 1
        )

        return last_refresh_at + normalized_interval + timedelta(seconds=stagger_seconds)

    def run_cycle(self) -> None:
        """Execute one ingestion/retention cycle."""

        if (
            FEED_SOURCES_COLLECTION is None
            or FEED_ARTICLES_COLLECTION is None
            or USER_FEED_SUBSCRIPTIONS_COLLECTION is None
            or USER_ARTICLE_STATES_COLLECTION is None
        ):
            logging.error("Feed collections are not configured; skipping cycle.")
            return

        try:
            sources = self._list_fetchable_sources()
            if len(sources) == 0:
                logging.debug("No subscribed feeds to fetch.")
            for source in sources:
                self._fetch_and_store_source(source)

            self._apply_retention()
        except (ServerSelectionTimeoutError, NetworkTimeout, AutoReconnect) as exc:
            logging.error(f"Feed cycle DB connectivity error: {exc}")
        except Exception as exc:
            logging.exception(f"Feed cycle failed unexpectedly: {exc}")

    def _list_fetchable_sources(self) -> list[dict[str, Any]]:
        """Return deduplicated source documents for currently subscribed feeds."""

        if FEED_SOURCES_COLLECTION is None or USER_FEED_SUBSCRIPTIONS_COLLECTION is None:
            return []

        feed_ids = USER_FEED_SUBSCRIPTIONS_COLLECTION.distinct("feed_id")
        normalized_feed_ids = [feed_id for feed_id in feed_ids if isinstance(feed_id, ObjectId)]

        if len(normalized_feed_ids) == 0:
            return []

        now = datetime.now(timezone.utc)
        cursor = FEED_SOURCES_COLLECTION.find({"_id": {"$in": normalized_feed_ids}})

        sources: list[dict[str, Any]] = []
        for source in cursor:
            if source_needs_fetch(source, now, FETCH_INTERVAL, MAX_SCHEDULE_LAG):
                sources.append(source)

        earliest = datetime.min.replace(tzinfo=timezone.utc)
        sources.sort(
            key=lambda source: (
                coerce_utc_datetime(source.get("next_refresh_at"))
                or coerce_utc_datetime(source.get("last_fetched_at"))
                or earliest
            )
        )

        return sources

    def _fetch_and_store_source(self, source_doc: dict[str, Any]) -> None:
        """Fetch one source and upsert parsed entries."""

        if FEED_SOURCES_COLLECTION is None:
            return

        source_id = source_doc.get("_id")
        source_url = str(source_doc.get("normalized_url", "")).strip()

        if not isinstance(source_id, ObjectId) or source_url == "":
            return

        if FAILURE_MODE == "timeout":
            self._record_fetch_failure(
                source_doc,
                source_id,
                "Simulated timeout failure mode.",
            )
            return

        if FAILURE_MODE == "http500":
            self._record_fetch_failure(
                source_doc,
                source_id,
                "Simulated upstream HTTP 500 failure mode.",
            )
            return

        request_headers: dict[str, str] = {}
        etag = source_doc.get("etag")
        if isinstance(etag, str) and etag.strip() != "":
            request_headers["If-None-Match"] = etag

        last_modified = source_doc.get("last_modified")
        if isinstance(last_modified, str) and last_modified.strip() != "":
            request_headers["If-Modified-Since"] = last_modified

        try:
            response = self.requests_session.get(
                source_url,
                headers=request_headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            self._record_fetch_failure(source_doc, source_id, f"Network error: {exc}")
            return

        now = datetime.now(timezone.utc)
        refresh_interval = self._resolve_refresh_interval(source_doc)

        if response.status_code == 304:
            next_refresh_at = self._compute_next_refresh_at(
                source_id,
                now,
                refresh_interval,
            )
            FEED_SOURCES_COLLECTION.update_one(
                {"_id": source_id},
                {
                    "$set": {
                        "fetch_status": "not_modified",
                        "last_error": None,
                        "next_retry_at": None,
                        "next_refresh_at": next_refresh_at,
                        "refresh_interval_seconds": int(refresh_interval.total_seconds()),
                        "force_refresh_requested_at": None,
                        "last_fetched_at": now,
                        "updated_at": now,
                    }
                },
            )
            return

        if response.status_code >= 400:
            self._record_fetch_failure(
                source_doc,
                source_id,
                f"HTTP {response.status_code}",
            )
            return

        feedparser_module = import_feedparser_module()
        if feedparser_module is None:
            self._record_fetch_failure(
                source_doc,
                source_id,
                "feedparser dependency is not installed.",
            )
            return

        payload = response.content
        if FAILURE_MODE == "malformed":
            payload = b"<rss><channel><title>Malformed"

        parsed = feedparser_module.parse(payload)
        entries = list(parsed.entries or [])

        if len(entries) == 0 and getattr(parsed, "bozo", False):
            bozo_exception = getattr(parsed, "bozo_exception", "Unknown parse error")
            self._record_fetch_failure(
                source_doc,
                source_id,
                f"Parse error: {bozo_exception}",
            )
            return

        ttl_interval = parse_feed_ttl_interval(parsed.feed)
        if ttl_interval is not None:
            refresh_interval = ttl_interval

        next_refresh_at = self._compute_next_refresh_at(
            source_id,
            now,
            refresh_interval,
        )

        feed_title = str(parsed.feed.get("title", "")).strip()
        if feed_title == "":
            feed_title = str(source_doc.get("title", source_url)).strip() or source_url

        feed_image_url = extract_feed_image_url(parsed.feed, source_url)
        if feed_image_url is None:
            existing_image_url = str(source_doc.get("image_url", "")).strip()
            if existing_image_url != "":
                feed_image_url = existing_image_url

        FEED_SOURCES_COLLECTION.update_one(
            {"_id": source_id},
            {
                "$set": {
                    "title": feed_title,
                    "image_url": feed_image_url,
                    "etag": response.headers.get("ETag"),
                    "last_modified": response.headers.get("Last-Modified"),
                    "fetch_status": "ok",
                    "last_error": None,
                    "next_retry_at": None,
                    "next_refresh_at": next_refresh_at,
                    "refresh_interval_seconds": int(refresh_interval.total_seconds()),
                    "force_refresh_requested_at": None,
                    "last_fetched_at": now,
                    "updated_at": now,
                }
            },
        )

        for entry in entries:
            normalized = self._parse_feed_entry(source_url, entry)
            if normalized is None:
                continue
            self._upsert_article(source_id, normalized)

    def _record_fetch_failure(
        self,
        source_doc: dict[str, Any],
        source_id: ObjectId,
        reason: str,
    ) -> None:
        """Persist source fetch failure metadata and the next scheduled refresh time."""

        if FEED_SOURCES_COLLECTION is None:
            return

        now = datetime.now(timezone.utc)
        refresh_interval = self._resolve_refresh_interval(source_doc)
        next_refresh_at = self._compute_next_refresh_at(
            source_id,
            now,
            refresh_interval,
        )

        FEED_SOURCES_COLLECTION.update_one(
            {"_id": source_id},
            {
                "$set": {
                    "fetch_status": "error",
                    "last_error": reason,
                    "next_retry_at": next_refresh_at,
                    "next_refresh_at": next_refresh_at,
                    "refresh_interval_seconds": int(refresh_interval.total_seconds()),
                    "force_refresh_requested_at": None,
                    "last_fetched_at": now,
                    "updated_at": now,
                }
            },
        )

    def _parse_feed_entry(self, source_url: str, entry: dict[str, Any]) -> ParsedEntry | None:
        """Normalize one feedparser entry into a stable write model."""

        link = str(entry.get("link", "")).strip()
        title = str(entry.get("title", "")).strip()

        if title == "":
            title = link or "Untitled"

        summary_html: str | None = None
        content_block = entry.get("content")
        if isinstance(content_block, list) and len(content_block) > 0:
            first_block = content_block[0]
            if isinstance(first_block, dict):
                summary_html = str(first_block.get("value", "")).strip() or None

        if summary_html is None:
            summary_html = str(entry.get("summary", "")).strip() or None

        media_image_url = extract_largest_media_image_url(entry, source_url)
        summary_html = strip_duplicate_summary_image(summary_html, media_image_url, source_url)

        if summary_html is not None and len(summary_html) > MAX_SUMMARY_LENGTH:
            summary_html = summary_html[:MAX_SUMMARY_LENGTH]

        published_at = parse_entry_published_at(entry)
        author = str(entry.get("author", "")).strip() or None

        dedupe_material = "|".join(
            [
                str(entry.get("id", "")).strip(),
                str(entry.get("guid", "")).strip(),
                link,
                title,
                published_at.isoformat() if published_at is not None else "",
                source_url,
            ]
        )
        dedupe_key = hashlib.sha256(dedupe_material.encode("utf-8")).hexdigest()

        return ParsedEntry(
            dedupe_key=dedupe_key,
            title=title,
            link=link or source_url,
            author=author,
            summary_html=summary_html,
            published_at=published_at,
            media_image_url=media_image_url,
        )

    def _upsert_article(self, feed_id: ObjectId, parsed_entry: ParsedEntry) -> None:
        """Upsert one article record keyed by feed_id and dedupe_key."""

        if FEED_ARTICLES_COLLECTION is None:
            return

        now = datetime.now(timezone.utc)

        article_document = FeedArticleDocument(
            feed_id=feed_id,
            dedupe_key=parsed_entry.dedupe_key,
            title=parsed_entry.title,
            link=parsed_entry.link,
            author=parsed_entry.author,
            summary_html=parsed_entry.summary_html,
            published_at=parsed_entry.published_at,
            media_image_url=parsed_entry.media_image_url,
            fetched_at=now,
            is_deleted=False,
            deleted_at=None,
        )

        FEED_ARTICLES_COLLECTION.update_one(
            {
                "feed_id": feed_id,
                "dedupe_key": parsed_entry.dedupe_key,
            },
            {
                "$set": article_document.model_dump(
                    by_alias=True,
                    exclude={"id", "is_deleted", "deleted_at"},
                ),
                "$setOnInsert": {
                    "created_at": now,
                    "is_deleted": False,
                    "deleted_at": None,
                },
            },
            upsert=True,
        )

    def _apply_retention(self) -> None:
        """Apply soft/hard retention while preserving unread user articles."""

        if (
            FEED_ARTICLES_COLLECTION is None
            or USER_FEED_SUBSCRIPTIONS_COLLECTION is None
            or USER_ARTICLE_STATES_COLLECTION is None
        ):
            return

        now = datetime.now(timezone.utc)
        soft_cutoff = now - SOFT_DELETE_AFTER
        hard_cutoff = now - HARD_DELETE_AFTER

        feed_user_map = build_feed_user_map()

        # Retention uses first-seen age (fetched_at), not publication date, so
        # freshly fetched back-catalog articles are not deleted immediately.
        soft_delete_query = {
            "is_deleted": False,
            "fetched_at": {"$lte": soft_cutoff},
        }

        soft_candidates = FEED_ARTICLES_COLLECTION.find(
            soft_delete_query,
            {"_id": 1, "feed_id": 1},
        )

        soft_delete_ids: list[ObjectId] = []
        soft_skipped_unread = 0
        for article_doc in soft_candidates:
            article_id = article_doc.get("_id")
            feed_id = article_doc.get("feed_id")
            if not isinstance(article_id, ObjectId):
                continue

            if article_has_unread_subscribers(article_id, feed_id, feed_user_map):
                soft_skipped_unread += 1
                continue

            soft_delete_ids.append(article_id)

        if len(soft_delete_ids) > 0:
            FEED_ARTICLES_COLLECTION.update_many(
                {"_id": {"$in": soft_delete_ids}},
                {
                    "$set": {
                        "is_deleted": True,
                        "deleted_at": now,
                    }
                },
            )

        logging.debug(
            "Retention soft-delete: marked=%d skipped_unread=%d",
            len(soft_delete_ids),
            soft_skipped_unread,
        )

        hard_candidates = FEED_ARTICLES_COLLECTION.find(
            {
                "is_deleted": True,
                "deleted_at": {"$lte": hard_cutoff},
            },
            {"_id": 1, "feed_id": 1},
        )

        hard_delete_ids: list[ObjectId] = []
        hard_skipped_unread = 0
        for article_doc in hard_candidates:
            article_id = article_doc.get("_id")
            feed_id = article_doc.get("feed_id")
            if not isinstance(article_id, ObjectId):
                continue

            if article_has_unread_subscribers(article_id, feed_id, feed_user_map):
                hard_skipped_unread += 1
                continue

            hard_delete_ids.append(article_id)

        if len(hard_delete_ids) > 0:
            FEED_ARTICLES_COLLECTION.delete_many({"_id": {"$in": hard_delete_ids}})
            USER_ARTICLE_STATES_COLLECTION.delete_many(
                {"article_id": {"$in": hard_delete_ids}}
            )

        logging.debug(
            "Retention hard-purge: purged=%d skipped_unread=%d",
            len(hard_delete_ids),
            hard_skipped_unread,
        )


def parse_entry_published_at(entry: dict[str, Any]) -> datetime | None:
    """Parse published/updated values from a feed entry into UTC datetime."""

    for parsed_key in ["published_parsed", "updated_parsed", "created_parsed"]:
        parsed_value = entry.get(parsed_key)
        if parsed_value is None:
            continue

        try:
            epoch = mktime(parsed_value)
            return datetime.fromtimestamp(epoch, tz=timezone.utc)
        except (TypeError, ValueError, OverflowError):
            continue

    for text_key in ["published", "updated", "created"]:
        text_value = str(entry.get(text_key, "")).strip()
        if text_value == "":
            continue

        try:
            parsed_datetime = parsedate_to_datetime(text_value)
        except (TypeError, ValueError):
            continue

        return coerce_utc_datetime(parsed_datetime)

    return None


def coerce_positive_int(value: Any) -> int | None:
    """Return a positive integer when the value can be coerced safely."""

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


def parse_feed_ttl_interval(feed_data: Any) -> timedelta | None:
    """Extract feed-provided TTL/syndication cadence as a refresh interval."""

    if not hasattr(feed_data, "get"):
        return None

    ttl_minutes = coerce_positive_int(feed_data.get("ttl"))
    if ttl_minutes is not None:
        return timedelta(seconds=max(5, ttl_minutes * 60))

    # Support RSS Syndication module metadata when TTL is absent.
    update_period = str(
        feed_data.get("sy_updateperiod")
        or feed_data.get("updateperiod")
        or ""
    ).strip().lower()
    update_frequency = coerce_positive_int(
        feed_data.get("sy_updatefrequency")
        or feed_data.get("updatefrequency")
        or 1
    )

    period_seconds = {
        "hourly": 3600,
        "daily": 86400,
        "weekly": 604800,
        "monthly": 2592000,
        "yearly": 31536000,
    }.get(update_period)

    if period_seconds is None or update_frequency is None:
        return None

    cadence_seconds = max(5, int(period_seconds / update_frequency))
    return timedelta(seconds=cadence_seconds)


def normalize_feed_asset_url(candidate: Any, source_url: str) -> str | None:
    """Normalize feed-level image/icon URLs to absolute HTTP(S) URLs."""

    if not isinstance(candidate, str):
        return None

    trimmed = candidate.strip()
    if trimmed == "":
        return None

    normalized = urljoin(source_url, trimmed)
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or parsed.netloc == "":
        return None

    return normalized


def extract_feed_image_url(feed_data: Any, source_url: str) -> str | None:
    """Extract a representative feed image/icon URL from parsed feed metadata."""

    if not hasattr(feed_data, "get"):
        return None

    image_block = feed_data.get("image")
    candidates: list[Any] = []
    if isinstance(image_block, dict):
        candidates.extend(
            [
                image_block.get("href"),
                image_block.get("url"),
                image_block.get("src"),
            ]
        )

    candidates.extend(
        [
            feed_data.get("icon"),
            feed_data.get("logo"),
            feed_data.get("image_href"),
            feed_data.get("image_url"),
        ]
    )

    for candidate in candidates:
        normalized = normalize_feed_asset_url(candidate, source_url)
        if normalized is not None:
            return normalized

    return None


def import_feedparser_module() -> Any | None:
    """Import feedparser lazily so worker startup remains robust in dev environments."""

    try:
        return importlib.import_module("feedparser")
    except ModuleNotFoundError:
        logging.error("feedparser module is not installed in the backend environment.")
        return None


def coerce_utc_datetime(value: Any) -> datetime | None:
    """Normalize a datetime value to timezone-aware UTC."""

    if not isinstance(value, datetime):
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def build_feed_user_map() -> dict[ObjectId, set[str]]:
    """Build a mapping of feed_id to subscribed usernames."""

    if USER_FEED_SUBSCRIPTIONS_COLLECTION is None:
        return {}

    mapping: dict[ObjectId, set[str]] = {}

    for sub in USER_FEED_SUBSCRIPTIONS_COLLECTION.find({}, {"feed_id": 1, "user_id": 1}):
        feed_id = sub.get("feed_id")
        user_id = sub.get("user_id")

        if not isinstance(feed_id, ObjectId) or not isinstance(user_id, str):
            continue

        mapping.setdefault(feed_id, set()).add(user_id)

    return mapping


def article_has_unread_subscribers(
    article_id: ObjectId,
    feed_id: Any,
    feed_user_map: dict[ObjectId, set[str]],
) -> bool:
    """Return True when any subscribed user has not read this article yet."""

    if USER_ARTICLE_STATES_COLLECTION is None:
        return False

    if not isinstance(feed_id, ObjectId):
        return False

    subscribed_users = feed_user_map.get(feed_id, set())
    if len(subscribed_users) == 0:
        return False

    read_count = USER_ARTICLE_STATES_COLLECTION.count_documents(
        {
            "article_id": article_id,
            "user_id": {"$in": list(subscribed_users)},
            "is_read": True,
        }
    )

    return read_count < len(subscribed_users)
