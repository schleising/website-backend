from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
import hashlib
import logging
import os
import re
from threading import Lock, Thread
from time import mktime, monotonic, sleep
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import feedparser
from bson import ObjectId
from pymongo.errors import AutoReconnect, DuplicateKeyError, NetworkTimeout, ServerSelectionTimeoutError
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from task_scheduler import TaskScheduler

from .feed_entry_media import extract_largest_media_image_url
from .feed_refresh_policy import (
    MIN_REFRESH_INTERVAL,
    MAX_REFRESH_INTERVAL,
    resolve_source_refresh_interval,
    source_needs_fetch,
)
from .feed_summary_images import extract_first_summary_image_url, strip_duplicate_summary_image
from .url_safety import is_public_http_url

from . import (
    FEED_ARTICLES_COLLECTION,
    FEED_SOURCES_COLLECTION,
    USER_ARTICLE_STATES_COLLECTION,
    USER_FEED_SUBSCRIPTIONS_COLLECTION,
)
from .models import FeedArticleDocument


def _read_env_positive_int(name: str, default: int, minimum: int = 1) -> int:
    """Read a positive integer env var with fallback defaults."""

    raw_value = str(os.getenv(name, str(default))).strip()
    try:
        parsed = int(raw_value)
    except ValueError:
        parsed = default

    return max(minimum, parsed)


def _read_env_non_negative_float(name: str, default: float) -> float:
    """Read a non-negative float env var with fallback defaults."""

    raw_value = str(os.getenv(name, str(default))).strip()
    try:
        parsed = float(raw_value)
    except ValueError:
        parsed = default

    return max(0.0, parsed)

FETCH_INTERVAL = timedelta(
    seconds=max(
        int(MIN_REFRESH_INTERVAL.total_seconds()),
        int(os.getenv("FEEDS_FETCH_INTERVAL_SECONDS", "900")),
    )
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
ARTICLE_IMAGE_REQUEST_TIMEOUT_SECONDS = _read_env_positive_int(
    "FEEDS_ARTICLE_IMAGE_REQUEST_TIMEOUT_SECONDS",
    default=8,
    minimum=2,
)
ARTICLE_IMAGE_SCRAPE_MIN_INTERVAL_SECONDS = max(
    0.25,
    _read_env_non_negative_float(
        "FEEDS_ARTICLE_IMAGE_SCRAPE_MIN_INTERVAL_SECONDS",
        default=1.0,
    ),
)
ARTICLE_IMAGE_HOST_MIN_INTERVAL_SECONDS = max(
    5.0,
    _read_env_non_negative_float(
        "FEEDS_ARTICLE_IMAGE_SCRAPE_HOST_MIN_INTERVAL_SECONDS",
        default=5.0,
    ),
)
ARTICLE_IMAGE_FAILURE_COOLDOWN_SECONDS = max(
    5.0,
    _read_env_non_negative_float(
        "FEEDS_ARTICLE_IMAGE_SCRAPE_FAILURE_COOLDOWN_SECONDS",
        default=300.0,
    ),
)
ARTICLE_IMAGE_RETRY_AFTER_MAX_SECONDS = _read_env_positive_int(
    "FEEDS_ARTICLE_IMAGE_SCRAPE_RETRY_AFTER_MAX_SECONDS",
    default=3600,
    minimum=30,
)
ARTICLE_IMAGE_MAX_REDIRECTS = _read_env_positive_int(
    "FEEDS_ARTICLE_IMAGE_SCRAPE_MAX_REDIRECTS",
    default=5,
    minimum=1,
)
SOURCE_FETCH_MAX_REDIRECTS = _read_env_positive_int(
    "FEEDS_SOURCE_FETCH_MAX_REDIRECTS",
    default=5,
    minimum=1,
)
ARTICLE_IMAGE_SCAN_MAX_CHARS = _read_env_positive_int(
    "FEEDS_ARTICLE_IMAGE_SCAN_MAX_CHARS",
    default=200_000,
    minimum=10_000,
)
ARTICLE_IMAGE_RESULT_CACHE_TTL_SECONDS = _read_env_non_negative_float(
    "FEEDS_ARTICLE_IMAGE_SCRAPE_RESULT_CACHE_TTL_SECONDS",
    default=3600.0,
)
ARTICLE_IMAGE_RESULT_CACHE_MAX_ENTRIES = _read_env_positive_int(
    "FEEDS_ARTICLE_IMAGE_SCRAPE_RESULT_CACHE_MAX_ENTRIES",
    default=5000,
    minimum=100,
)
MAX_SUMMARY_LENGTH = 60_000
FAILURE_MODE = os.getenv("FEEDS_FAILURE_MODE", "none").strip().lower()
HTML_TAG_RE = re.compile(r"<[a-zA-Z][^>]*>")
META_TAG_RE = re.compile(r"<meta\b[^>]*>", re.IGNORECASE)
META_ATTR_RE = re.compile(
    r"\b([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|([^\s\"'=<>`]+))",
    re.IGNORECASE,
)

META_IMAGE_PROPERTY_VALUES = {
    "og:image",
    "og:image:url",
    "og:image:secure_url",
}
META_IMAGE_NAME_VALUES = {
    "twitter:image",
    "twitter:image:src",
    "og:image",
    "og:image:url",
    "og:image:secure_url",
}
REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}


@dataclass(slots=True)
class ParsedEntry:
    """Normalized article fields extracted from one feed entry."""

    dedupe_key: str
    canonical_url: str | None
    external_id: str | None
    title: str
    link: str
    author: str | None
    summary_html: str | None
    published_at: datetime | None
    media_image_url: str | None


@dataclass(slots=True)
class ArticleImageScrapeJob:
    """Represents one deferred article page image scrape request."""

    article_id: ObjectId
    article_url: str


class Feeds:
    """Background feed worker that fetches unique subscriptions and applies retention."""

    def __init__(self, scheduler: TaskScheduler) -> None:
        self.scheduler = scheduler
        self.requests_session = self._build_session(enable_retries=True)
        self.article_scrape_session = self._build_session(enable_retries=False)
        self._next_article_image_scrape_at_monotonic = 0.0
        self._next_article_image_scrape_at_by_host_monotonic: dict[str, float] = {}
        self._article_image_scrape_host_backoff_until_monotonic: dict[str, float] = {}
        self._article_image_scrape_result_cache: dict[str, tuple[float, str | None]] = {}
        self._article_image_scrape_queue: deque[ArticleImageScrapeJob] = deque()
        self._article_image_scrape_thread: Thread | None = None
        self._article_image_scrape_lock = Lock()

        self.scheduler.schedule_task(
            datetime.now(timezone.utc),
            self.run_cycle,
            CYCLE_INTERVAL,
        )

    def _wait_for_article_image_scrape_slot(self, hostname: str | None) -> None:
        """Apply global + host-specific pace limits to article image scrape requests."""

        if (
            ARTICLE_IMAGE_SCRAPE_MIN_INTERVAL_SECONDS <= 0
            and ARTICLE_IMAGE_HOST_MIN_INTERVAL_SECONDS <= 0
        ):
            return

        now_monotonic = monotonic()
        next_allowed_at = self._next_article_image_scrape_at_monotonic
        if isinstance(hostname, str) and hostname != "":
            next_allowed_at = max(
                next_allowed_at,
                self._next_article_image_scrape_at_by_host_monotonic.get(hostname, 0.0),
            )

        wait_seconds = next_allowed_at - now_monotonic
        if wait_seconds > 0:
            sleep(wait_seconds)
            now_monotonic = monotonic()

        if ARTICLE_IMAGE_SCRAPE_MIN_INTERVAL_SECONDS > 0:
            self._next_article_image_scrape_at_monotonic = (
                now_monotonic + ARTICLE_IMAGE_SCRAPE_MIN_INTERVAL_SECONDS
            )

        if (
            isinstance(hostname, str)
            and hostname != ""
            and ARTICLE_IMAGE_HOST_MIN_INTERVAL_SECONDS > 0
        ):
            self._next_article_image_scrape_at_by_host_monotonic[hostname] = (
                now_monotonic + ARTICLE_IMAGE_HOST_MIN_INTERVAL_SECONDS
            )

    def _prune_article_image_result_cache(self, now_monotonic: float) -> None:
        """Keep result cache bounded and remove stale URL entries."""

        if ARTICLE_IMAGE_RESULT_CACHE_TTL_SECONDS <= 0:
            self._article_image_scrape_result_cache.clear()
            return

        if (
            len(self._article_image_scrape_result_cache)
            <= ARTICLE_IMAGE_RESULT_CACHE_MAX_ENTRIES
        ):
            return

        cutoff = now_monotonic - ARTICLE_IMAGE_RESULT_CACHE_TTL_SECONDS
        stale_urls = [
            article_url
            for article_url, (cached_at, _) in self._article_image_scrape_result_cache.items()
            if cached_at <= cutoff
        ]
        for article_url in stale_urls:
            self._article_image_scrape_result_cache.pop(article_url, None)

        overflow = (
            len(self._article_image_scrape_result_cache)
            - ARTICLE_IMAGE_RESULT_CACHE_MAX_ENTRIES
        )
        if overflow <= 0:
            return

        oldest_first_urls = sorted(
            self._article_image_scrape_result_cache.items(),
            key=lambda item: item[1][0],
        )
        for article_url, _ in oldest_first_urls[:overflow]:
            self._article_image_scrape_result_cache.pop(article_url, None)

    def _get_cached_article_meta_image_url(
        self,
        normalized_article_url: str,
        now_monotonic: float,
    ) -> tuple[bool, str | None]:
        """Return a cached scrape result for an article URL when still fresh."""

        if ARTICLE_IMAGE_RESULT_CACHE_TTL_SECONDS <= 0:
            return False, None

        cache_entry = self._article_image_scrape_result_cache.get(normalized_article_url)
        if cache_entry is None:
            return False, None

        cached_at, cached_media_image_url = cache_entry
        if (now_monotonic - cached_at) > ARTICLE_IMAGE_RESULT_CACHE_TTL_SECONDS:
            self._article_image_scrape_result_cache.pop(normalized_article_url, None)
            return False, None

        return True, cached_media_image_url

    def _cache_article_meta_image_url(
        self,
        normalized_article_url: str,
        media_image_url: str | None,
    ) -> None:
        """Store the scrape outcome for an article URL to avoid repeat requests."""

        if ARTICLE_IMAGE_RESULT_CACHE_TTL_SECONDS <= 0:
            return

        now_monotonic = monotonic()
        self._article_image_scrape_result_cache[normalized_article_url] = (
            now_monotonic,
            media_image_url,
        )
        self._prune_article_image_result_cache(now_monotonic)

    def _is_article_image_host_backed_off(
        self,
        hostname: str,
        now_monotonic: float,
    ) -> bool:
        """Check whether a host is currently in cooldown after failures or 429."""

        backed_off_until = self._article_image_scrape_host_backoff_until_monotonic.get(hostname)
        if backed_off_until is None:
            return False

        if now_monotonic >= backed_off_until:
            self._article_image_scrape_host_backoff_until_monotonic.pop(hostname, None)
            return False

        return True

    def _set_article_image_host_backoff(
        self,
        hostname: str,
        retry_after_header: str | None,
    ) -> None:
        """Back off a host after rate-limit/server errors to avoid hammering."""

        if hostname == "":
            return

        retry_after_seconds = parse_retry_after_seconds(retry_after_header)
        if retry_after_seconds is None or retry_after_seconds <= 0:
            retry_after_seconds = ARTICLE_IMAGE_FAILURE_COOLDOWN_SECONDS

        if retry_after_seconds <= 0:
            return

        bounded_backoff = min(
            float(ARTICLE_IMAGE_RETRY_AFTER_MAX_SECONDS),
            max(ARTICLE_IMAGE_HOST_MIN_INTERVAL_SECONDS, retry_after_seconds),
        )

        self._article_image_scrape_host_backoff_until_monotonic[hostname] = (
            monotonic() + bounded_backoff
        )

    def _mark_article_image_host_request(
        self,
        hostname: str,
        request_count: int = 1,
    ) -> None:
        """Record host request volume so redirect chains cannot amplify request rate."""

        if hostname == "" or ARTICLE_IMAGE_HOST_MIN_INTERVAL_SECONDS <= 0:
            return

        normalized_request_count = max(1, request_count)
        holdoff_seconds = ARTICLE_IMAGE_HOST_MIN_INTERVAL_SECONDS * normalized_request_count

        self._next_article_image_scrape_at_by_host_monotonic[hostname] = max(
            self._next_article_image_scrape_at_by_host_monotonic.get(hostname, 0.0),
            monotonic() + holdoff_seconds,
        )

    def _extract_article_meta_image_url(self, article_url: str) -> str | None:
        """Fetch article HTML and extract a representative meta-image URL."""

        normalized_article_url = normalize_feed_asset_url(article_url, article_url)
        if normalized_article_url is None:
            return None

        now_monotonic = monotonic()
        has_cached_result, cached_media_image_url = self._get_cached_article_meta_image_url(
            normalized_article_url,
            now_monotonic,
        )
        if has_cached_result:
            return cached_media_image_url

        requested_hostname = urlparse(normalized_article_url).netloc.strip().lower()
        if requested_hostname != "" and self._is_article_image_host_backed_off(
            requested_hostname,
            now_monotonic,
        ):
            return None

        self._wait_for_article_image_scrape_slot(requested_hostname)

        try:
            response = self._safe_get_with_redirects(
                session=self.article_scrape_session,
                initial_url=normalized_article_url,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                },
                timeout=ARTICLE_IMAGE_REQUEST_TIMEOUT_SECONDS,
                max_redirects=ARTICLE_IMAGE_MAX_REDIRECTS,
            )
        except requests.RequestException as exc:
            logging.debug(
                "Article meta-image fetch failed for %s: %s",
                normalized_article_url,
                exc,
            )
            if requested_hostname != "":
                self._set_article_image_host_backoff(requested_hostname, None)
            self._cache_article_meta_image_url(normalized_article_url, None)
            return None

        host_request_counts: Counter[str] = Counter()
        for hop_response in [*response.history, response]:
            hop_hostname = urlparse(str(hop_response.url).strip()).netloc.strip().lower()
            if hop_hostname != "":
                host_request_counts[hop_hostname] += 1

        for hop_hostname, hop_count in host_request_counts.items():
            self._mark_article_image_host_request(hop_hostname, request_count=hop_count)

        response_hostname = urlparse(str(response.url).strip()).netloc.strip().lower()

        if response.status_code == 429 or response.status_code >= 500:
            if requested_hostname != "":
                self._set_article_image_host_backoff(
                    requested_hostname,
                    response.headers.get("Retry-After"),
                )
            if response_hostname != "" and response_hostname != requested_hostname:
                self._set_article_image_host_backoff(
                    response_hostname,
                    response.headers.get("Retry-After"),
                )

        if response.status_code >= 400:
            self._cache_article_meta_image_url(normalized_article_url, None)
            return None

        page_url = str(response.url).strip() or normalized_article_url
        html_body = response.text
        if html_body.strip() == "":
            self._cache_article_meta_image_url(normalized_article_url, None)
            return None

        if len(html_body) > ARTICLE_IMAGE_SCAN_MAX_CHARS:
            html_body = html_body[:ARTICLE_IMAGE_SCAN_MAX_CHARS]

        media_image_url = extract_meta_image_url(html_body, page_url)
        self._cache_article_meta_image_url(normalized_article_url, media_image_url)
        return media_image_url

    def _enqueue_article_image_scrape_jobs(self, jobs: list[ArticleImageScrapeJob]) -> None:
        """Queue first-seen no-image articles for background meta-image scraping."""

        if len(jobs) == 0:
            return

        with self._article_image_scrape_lock:
            self._article_image_scrape_queue.extend(jobs)

        self._start_article_image_scrape_thread_if_idle()

    def _start_article_image_scrape_thread_if_idle(self) -> None:
        """Ensure at most one background article-image scrape thread is active."""

        thread_to_start: Thread | None = None

        with self._article_image_scrape_lock:
            active_thread = self._article_image_scrape_thread
            if active_thread is not None and active_thread.is_alive():
                return

            if len(self._article_image_scrape_queue) == 0:
                self._article_image_scrape_thread = None
                return

            thread_to_start = Thread(
                target=self._run_article_image_scrape_worker,
                name="feeds-article-image-scraper",
                daemon=True,
            )
            self._article_image_scrape_thread = thread_to_start

        if thread_to_start is not None:
            thread_to_start.start()

    def _run_article_image_scrape_worker(self) -> None:
        """Process queued article image scrape jobs sequentially."""

        while True:
            with self._article_image_scrape_lock:
                if len(self._article_image_scrape_queue) == 0:
                    self._article_image_scrape_thread = None
                    return

                job = self._article_image_scrape_queue.popleft()

            try:
                self._process_article_image_scrape_job(job)
            except Exception as exc:
                logging.exception("Article image scrape job failed unexpectedly: %s", exc)

    def _process_article_image_scrape_job(self, job: ArticleImageScrapeJob) -> None:
        """Fetch and persist one deferred article image when available."""

        if FEED_ARTICLES_COLLECTION is None:
            return

        media_image_url = self._extract_article_meta_image_url(job.article_url)
        if media_image_url is None:
            return

        FEED_ARTICLES_COLLECTION.update_one(
            {
                "_id": job.article_id,
                "$or": [
                    {"media_image_url": None},
                    {"media_image_url": ""},
                ],
            },
            {
                "$set": {
                    "media_image_url": media_image_url,
                }
            },
        )

    def _build_session(self, *, enable_retries: bool) -> requests.Session:
        """Create a requests session with conservative retry behavior."""

        session = requests.Session()

        if enable_retries:
            retry_policy: Retry | int = Retry(
                total=3,
                backoff_factor=0.4,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            )
        else:
            # Article scraping must never burst via transparent retry storms.
            retry_policy = 0

        adapter = HTTPAdapter(max_retries=retry_policy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update({"User-Agent": "website3-feed-worker/1.0"})
        if enable_retries:
            session.headers.update(
                {
                    "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9,*/*;q=0.8",
                }
            )

        return session

    def _safe_get_with_redirects(
        self,
        *,
        session: requests.Session,
        initial_url: str,
        headers: dict[str, str],
        timeout: int,
        max_redirects: int,
    ) -> requests.Response:
        """Fetch URL with bounded redirects while blocking non-public targets."""

        current_url = str(initial_url).strip()
        history: list[requests.Response] = []

        for _ in range(max_redirects + 1):
            if not is_public_http_url(current_url):
                raise requests.RequestException(f"Blocked non-public URL target: {current_url}")

            response = session.get(
                current_url,
                headers=headers,
                timeout=timeout,
                allow_redirects=False,
            )

            if response.status_code not in REDIRECT_STATUS_CODES:
                response.history = history
                return response

            redirect_location = str(response.headers.get("Location", "")).strip()
            if redirect_location == "":
                response.close()
                raise requests.RequestException(
                    f"Redirect response missing Location header: {current_url}"
                )

            next_url = urljoin(current_url, redirect_location)
            if not is_public_http_url(next_url):
                response.close()
                raise requests.RequestException(f"Blocked non-public redirect target: {next_url}")

            history.append(response)
            response.close()
            current_url = next_url

        raise requests.TooManyRedirects(
            f"Exceeded redirect limit ({max_redirects}) for URL: {initial_url}"
        )

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

        normalized_interval = min(
            MAX_REFRESH_INTERVAL,
            max(MIN_REFRESH_INTERVAL, refresh_interval),
        )
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
            pending_scrape_jobs: list[ArticleImageScrapeJob] = []

            sources = self._list_fetchable_sources()
            if len(sources) == 0:
                logging.debug("No subscribed feeds to fetch.")
            for source in sources:
                pending_scrape_jobs.extend(self._fetch_and_store_source(source))

            if len(pending_scrape_jobs) > 0:
                self._enqueue_article_image_scrape_jobs(pending_scrape_jobs)

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

    def _fetch_and_store_source(self, source_doc: dict[str, Any]) -> list[ArticleImageScrapeJob]:
        """Fetch one source and upsert parsed entries."""

        if FEED_SOURCES_COLLECTION is None:
            return []

        pending_scrape_jobs: list[ArticleImageScrapeJob] = []

        source_id = source_doc.get("_id")
        source_url = str(source_doc.get("normalized_url", "")).strip()

        if not isinstance(source_id, ObjectId) or source_url == "":
            return []

        if FAILURE_MODE == "timeout":
            self._record_fetch_failure(
                source_doc,
                source_id,
                "Simulated timeout failure mode.",
            )
            return []

        if FAILURE_MODE == "http500":
            self._record_fetch_failure(
                source_doc,
                source_id,
                "Simulated upstream HTTP 500 failure mode.",
            )
            return []

        request_headers: dict[str, str] = {}
        etag = source_doc.get("etag")
        if isinstance(etag, str) and etag.strip() != "":
            request_headers["If-None-Match"] = etag

        last_modified = source_doc.get("last_modified")
        if isinstance(last_modified, str) and last_modified.strip() != "":
            request_headers["If-Modified-Since"] = last_modified

        try:
            response = self._safe_get_with_redirects(
                session=self.requests_session,
                initial_url=source_url,
                headers=request_headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
                max_redirects=SOURCE_FETCH_MAX_REDIRECTS,
            )
        except requests.RequestException as exc:
            self._record_fetch_failure(source_doc, source_id, f"Network error: {exc}")
            return []

        effective_source_url = (
            normalize_feed_asset_url(str(response.url).strip(), source_url)
            or source_url
        )

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
            return []

        if response.status_code >= 400:
            self._record_fetch_failure(
                source_doc,
                source_id,
                f"HTTP {response.status_code}",
            )
            return []

        payload = response.content
        if FAILURE_MODE == "malformed":
            payload = b"<rss><channel><title>Malformed"

        parsed = feedparser.parse(payload)
        entries = list(parsed.entries or [])
        parsed_feed: Any = parsed.feed if hasattr(parsed, "feed") else {}
        if not hasattr(parsed_feed, "get"):
            parsed_feed = {}

        if len(entries) == 0 and getattr(parsed, "bozo", False):
            bozo_exception = getattr(parsed, "bozo_exception", "Unknown parse error")
            self._record_fetch_failure(
                source_doc,
                source_id,
                f"Parse error: {bozo_exception}",
            )
            return []

        ttl_interval = parse_feed_ttl_interval(parsed_feed)
        if ttl_interval is not None:
            refresh_interval = ttl_interval

        next_refresh_at = self._compute_next_refresh_at(
            source_id,
            now,
            refresh_interval,
        )

        fallback_source_title = (
            str(source_doc.get("title", effective_source_url)).strip() or effective_source_url
        )
        feed_title = resolve_source_feed_title(
            parsed_feed,
            effective_source_url,
            fallback_source_title,
        )

        feed_image_url = extract_feed_image_url(parsed_feed, effective_source_url)
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
            normalized = self._parse_feed_entry(effective_source_url, entry)
            if normalized is None:
                continue
            scrape_job = self._upsert_article(source_id, normalized)
            if scrape_job is not None:
                pending_scrape_jobs.append(scrape_job)

        return pending_scrape_jobs

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

        source_title = str(source_doc.get("title", source_doc.get("normalized_url", "Feed"))).strip() or "Feed"
        source_url = str(source_doc.get("normalized_url", "")).strip()
        logging.error(
            "Feed refresh failed | source_id=%s | title=%s | url=%s | reason=%s",
            source_id,
            source_title,
            source_url,
            reason,
        )

    def _parse_feed_entry(self, source_url: str, entry: dict[str, Any]) -> ParsedEntry | None:
        """Normalize one feedparser entry into a stable write model."""

        link = str(entry.get("link", "")).strip()
        canonical_url = normalize_article_identity_url(link, source_url)
        external_id = extract_entry_external_id(entry, source_url)
        title = str(entry.get("title", "")).strip()

        if title == "":
            title = canonical_url or link or "Untitled"

        summary_html = select_entry_summary_html(entry)

        media_image_url = extract_largest_media_image_url(entry, source_url)
        if media_image_url is None:
            media_image_url = extract_first_summary_image_url(summary_html, source_url)
        summary_html = strip_duplicate_summary_image(summary_html, media_image_url, source_url)

        if summary_html is not None and len(summary_html) > MAX_SUMMARY_LENGTH:
            summary_html = summary_html[:MAX_SUMMARY_LENGTH]

        published_at = parse_entry_published_at(entry)
        author = str(entry.get("author", "")).strip() or None

        dedupe_material = build_article_dedupe_material(
            canonical_url=canonical_url,
            external_id=external_id,
            source_url=source_url,
            title=title,
            published_at=published_at,
        )
        dedupe_key = hashlib.sha256(dedupe_material.encode("utf-8")).hexdigest()

        return ParsedEntry(
            dedupe_key=dedupe_key,
            canonical_url=canonical_url,
            external_id=external_id,
            title=title,
            link=canonical_url or source_url,
            author=author,
            summary_html=summary_html,
            published_at=published_at,
            media_image_url=media_image_url,
        )

    def _upsert_article(
        self,
        feed_id: ObjectId,
        parsed_entry: ParsedEntry,
    ) -> ArticleImageScrapeJob | None:
        """Upsert one article record keyed by canonical URL (fallback external_id/dedupe)."""

        if FEED_ARTICLES_COLLECTION is None:
            return None

        now = datetime.now(timezone.utc)
        dedupe_owner_doc = FEED_ARTICLES_COLLECTION.find_one(
            {
                "feed_id": feed_id,
                "dedupe_key": parsed_entry.dedupe_key,
            },
            {
                "_id": 1,
                "media_image_url": 1,
            },
        )

        if isinstance(dedupe_owner_doc, dict) and isinstance(
            dedupe_owner_doc.get("_id"),
            ObjectId,
        ):
            article_query: dict[str, Any] = {
                "_id": dedupe_owner_doc["_id"],
            }
            existing_doc = dedupe_owner_doc
        else:
            article_query = build_article_identity_query(feed_id, parsed_entry)
            existing_doc = FEED_ARTICLES_COLLECTION.find_one(
                article_query,
                {
                    "_id": 1,
                    "media_image_url": 1,
                },
            )

        existing_media_image_url: str | None = None
        if isinstance(existing_doc, dict):
            existing_media_candidate = str(existing_doc.get("media_image_url", "")).strip()
            if existing_media_candidate != "":
                existing_media_image_url = existing_media_candidate

        media_image_url = parsed_entry.media_image_url
        if media_image_url is None and existing_media_image_url is not None:
            media_image_url = existing_media_image_url

        article_document = FeedArticleDocument(
            feed_id=feed_id,
            dedupe_key=parsed_entry.dedupe_key,
            canonical_url=parsed_entry.canonical_url,
            external_id=parsed_entry.external_id,
            title=parsed_entry.title,
            link=parsed_entry.link,
            author=parsed_entry.author,
            summary_html=parsed_entry.summary_html,
            published_at=parsed_entry.published_at,
            media_image_url=media_image_url,
            fetched_at=now,
            is_deleted=False,
            deleted_at=None,
        )

        update_payload = {
            "$set": article_document.model_dump(
                by_alias=True,
                exclude={"id"},
            ),
            "$setOnInsert": {
                "created_at": now,
            },
        }

        resolved_article_id: ObjectId | None = None
        upserted_article_id: ObjectId | None = None

        try:
            upsert_result = FEED_ARTICLES_COLLECTION.update_one(
                article_query,
                update_payload,
                upsert=True,
            )
            possible_upserted_id = upsert_result.upserted_id
            if isinstance(possible_upserted_id, ObjectId):
                upserted_article_id = possible_upserted_id
        except DuplicateKeyError:
            # Legacy rows may share link/canonical identities while differing in
            # dedupe_key. Resolve conflicts by targeting the dedupe-key owner
            # directly, then optionally try weaker identity fallbacks.
            dedupe_owner_query = {
                "feed_id": feed_id,
                "dedupe_key": parsed_entry.dedupe_key,
            }
            dedupe_owner_result = FEED_ARTICLES_COLLECTION.update_one(
                dedupe_owner_query,
                update_payload,
                upsert=False,
            )

            if dedupe_owner_result.matched_count == 0:
                fallback_clauses: list[dict[str, Any]] = []
                if parsed_entry.external_id is not None:
                    fallback_clauses.append(
                        {
                            "feed_id": feed_id,
                            "external_id": parsed_entry.external_id,
                        }
                    )
                if parsed_entry.canonical_url is not None:
                    fallback_clauses.append(
                        {
                            "feed_id": feed_id,
                            "canonical_url": parsed_entry.canonical_url,
                        }
                    )
                    fallback_clauses.append(
                        {
                            "feed_id": feed_id,
                            "link": parsed_entry.canonical_url,
                        }
                    )

                if len(fallback_clauses) > 0:
                    FEED_ARTICLES_COLLECTION.update_one(
                        {"$or": fallback_clauses},
                        update_payload,
                        upsert=False,
                    )

            dedupe_owner = FEED_ARTICLES_COLLECTION.find_one(
                {
                    "feed_id": feed_id,
                    "dedupe_key": parsed_entry.dedupe_key,
                },
                {"_id": 1},
            )
            if isinstance(dedupe_owner, dict):
                possible_article_id = dedupe_owner.get("_id")
                if isinstance(possible_article_id, ObjectId):
                    resolved_article_id = possible_article_id

        should_enqueue_deferred_scrape = (
            existing_doc is None and media_image_url is None
        )
        if not should_enqueue_deferred_scrape:
            return None

        article_id = upserted_article_id
        if not isinstance(article_id, ObjectId) and isinstance(resolved_article_id, ObjectId):
            article_id = resolved_article_id
        if not isinstance(article_id, ObjectId):
            inserted_doc = FEED_ARTICLES_COLLECTION.find_one(article_query, {"_id": 1})
            if isinstance(inserted_doc, dict):
                possible_article_id = inserted_doc.get("_id")
                if isinstance(possible_article_id, ObjectId):
                    article_id = possible_article_id

        if not isinstance(article_id, ObjectId):
            dedupe_doc = FEED_ARTICLES_COLLECTION.find_one(
                {
                    "feed_id": feed_id,
                    "dedupe_key": parsed_entry.dedupe_key,
                },
                {"_id": 1},
            )
            if isinstance(dedupe_doc, dict):
                possible_article_id = dedupe_doc.get("_id")
                if isinstance(possible_article_id, ObjectId):
                    article_id = possible_article_id

        if not isinstance(article_id, ObjectId):
            return None

        return ArticleImageScrapeJob(
            article_id=article_id,
            article_url=parsed_entry.link,
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


def parse_retry_after_seconds(value: Any) -> float | None:
    """Parse Retry-After as delay-seconds or HTTP-date and return seconds."""

    if value is None:
        return None

    raw_value = str(value).strip()
    if raw_value == "":
        return None

    try:
        seconds = float(raw_value)
    except ValueError:
        seconds = -1.0

    if seconds >= 0:
        return seconds

    try:
        retry_after_datetime = parsedate_to_datetime(raw_value)
    except (TypeError, ValueError):
        return None

    retry_after_utc = coerce_utc_datetime(retry_after_datetime)
    if retry_after_utc is None:
        return None

    now_utc = datetime.now(timezone.utc)
    delta_seconds = (retry_after_utc - now_utc).total_seconds()
    return max(0.0, delta_seconds)


def _coerce_entry_text(value: Any) -> str | None:
    """Return a stripped non-empty string from feed entry metadata values."""

    if not isinstance(value, str):
        return None

    stripped = value.strip()
    return stripped or None


def _is_html_content_type(content_type: str | None) -> bool:
    """Check whether an entry content MIME type likely contains HTML markup."""

    if content_type is None:
        return False

    lowered = content_type.strip().lower()
    return "html" in lowered or "xhtml" in lowered


def _looks_like_html(value: str) -> bool:
    """Detect whether text contains HTML tags that should be preserved."""

    return HTML_TAG_RE.search(value) is not None


def select_entry_summary_html(entry: dict[str, Any]) -> str | None:
    """Choose the richest summary body, preferring HTML over plain-text variants."""

    plain_candidate: str | None = None

    content_block = entry.get("content")
    if isinstance(content_block, list):
        for block in content_block:
            if not isinstance(block, dict):
                continue

            value = _coerce_entry_text(block.get("value"))
            if value is None:
                continue

            block_type = _coerce_entry_text(block.get("type"))
            if _is_html_content_type(block_type) or _looks_like_html(value):
                return value

            if plain_candidate is None:
                plain_candidate = value

    summary_detail = entry.get("summary_detail")
    if isinstance(summary_detail, dict):
        detail_value = _coerce_entry_text(summary_detail.get("value"))
        detail_type = _coerce_entry_text(summary_detail.get("type"))

        if detail_value is not None:
            if _is_html_content_type(detail_type) or _looks_like_html(detail_value):
                return detail_value

            if plain_candidate is None:
                plain_candidate = detail_value

    summary_value = _coerce_entry_text(entry.get("summary"))
    if summary_value is not None:
        if _looks_like_html(summary_value):
            return summary_value

        if plain_candidate is None:
            plain_candidate = summary_value

    return plain_candidate


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
        return min(
            MAX_REFRESH_INTERVAL,
            timedelta(
                seconds=max(
                    int(MIN_REFRESH_INTERVAL.total_seconds()),
                    ttl_minutes * 60,
                )
            ),
        )

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

    cadence_seconds = max(
        int(MIN_REFRESH_INTERVAL.total_seconds()),
        int(period_seconds / update_frequency),
    )
    return min(
        MAX_REFRESH_INTERVAL,
        timedelta(seconds=cadence_seconds),
    )


def _coerce_feed_text(value: Any) -> str:
    """Return normalized feed-level text, or empty string when unavailable."""

    if not isinstance(value, str):
        return ""

    return value.strip()


def is_bbc_feed_source_url(source_url: str) -> bool:
    """Return True when the source URL points to BBC's feed host."""

    hostname = (urlparse(str(source_url).strip()).hostname or "").strip().lower()
    return hostname == "feeds.bbci.co.uk"


def resolve_source_feed_title(
    feed_data: Any,
    source_url: str,
    fallback_title: str,
) -> str:
    """Resolve persisted feed source title, preferring BBC description text."""

    title_value = ""
    description_value = ""

    if hasattr(feed_data, "get"):
        title_value = _coerce_feed_text(feed_data.get("title"))

        for description_key in ("description", "subtitle", "summary"):
            candidate_value = _coerce_feed_text(feed_data.get(description_key))
            if candidate_value != "":
                description_value = candidate_value
                break

    fallback_value = _coerce_feed_text(fallback_title)

    if is_bbc_feed_source_url(source_url) and description_value != "":
        return description_value

    if title_value != "":
        return title_value

    if description_value != "":
        return description_value

    return fallback_value


def normalize_article_identity_url(candidate: Any, source_url: str) -> str | None:
    """Normalize an entry URL for article identity/upsert matching."""

    if not isinstance(candidate, str):
        return None

    trimmed = candidate.strip()
    if trimmed == "" or trimmed.lower() in {"none", "null", "undefined"}:
        return None

    normalized = urljoin(source_url, trimmed)
    parsed = urlparse(normalized)

    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").strip().lower()
    if scheme not in {"http", "https"} or hostname == "":
        return None

    try:
        port = parsed.port
    except ValueError:
        return None

    if port is None or (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        netloc = hostname
    else:
        netloc = f"{hostname}:{port}"

    path = parsed.path or "/"

    return urlunparse((scheme, netloc, path, parsed.params, parsed.query, ""))


def extract_entry_external_id(entry: dict[str, Any], source_url: str) -> str | None:
    """Extract a stable feed entry external identifier when available."""

    for key in ("id", "guid"):
        raw_value = str(entry.get(key, "")).strip()
        if raw_value == "" or raw_value.lower() in {"none", "null", "undefined"}:
            continue

        normalized_url = normalize_article_identity_url(raw_value, source_url)
        return normalized_url or raw_value

    return None


def build_article_dedupe_material(
    *,
    canonical_url: str | None,
    external_id: str | None,
    source_url: str,
    title: str,
    published_at: datetime | None,
) -> str:
    """Build a secondary dedupe fingerprint material string."""

    if canonical_url is not None:
        return f"url|{canonical_url}"

    if external_id is not None:
        return f"external_id|{external_id}"

    return "|".join(
        [
            "fallback",
            title,
            published_at.isoformat() if published_at is not None else "",
            source_url,
        ]
    )


def build_article_identity_query(feed_id: ObjectId, parsed_entry: ParsedEntry) -> dict[str, Any]:
    """Return the best-available identity query for article upsert matching."""

    if parsed_entry.canonical_url is not None:
        identity_clauses: list[dict[str, Any]] = [
            {"canonical_url": parsed_entry.canonical_url},
            {"link": parsed_entry.canonical_url},
        ]

        if parsed_entry.external_id is not None:
            identity_clauses.append({"external_id": parsed_entry.external_id})
        else:
            identity_clauses.append({"dedupe_key": parsed_entry.dedupe_key})

        return {
            "feed_id": feed_id,
            "$or": identity_clauses,
        }

    if parsed_entry.external_id is not None:
        return {
            "feed_id": feed_id,
            "$or": [
                {"external_id": parsed_entry.external_id},
                {"dedupe_key": parsed_entry.dedupe_key},
            ],
        }

    return {
        "feed_id": feed_id,
        "dedupe_key": parsed_entry.dedupe_key,
    }


def normalize_feed_asset_url(candidate: Any, source_url: str) -> str | None:
    """Normalize feed-level image/icon URLs to absolute HTTP(S) URLs."""

    if not isinstance(candidate, str):
        return None

    trimmed = candidate.strip()
    if trimmed == "":
        return None

    normalized = urljoin(source_url, trimmed)
    if not is_public_http_url(normalized, require_dns_resolution=False):
        return None

    return normalized


def _extract_meta_attributes(meta_tag: str) -> dict[str, str]:
    """Parse one <meta> tag string into lowercase attribute mappings."""

    attributes: dict[str, str] = {}

    for match in META_ATTR_RE.finditer(meta_tag):
        attribute_name = str(match.group(1)).strip().lower()
        if attribute_name == "":
            continue

        raw_value = ""
        for group_value in match.groups()[1:]:
            if isinstance(group_value, str) and group_value.strip() != "":
                raw_value = group_value.strip()
                break

        if raw_value == "":
            continue

        attributes[attribute_name] = unescape(raw_value)

    return attributes


def extract_meta_image_url(page_html: str, article_url: str) -> str | None:
    """Extract the first valid og/twitter image candidate from article HTML."""

    if not isinstance(page_html, str) or page_html.strip() == "":
        return None

    for meta_match in META_TAG_RE.finditer(page_html):
        attributes = _extract_meta_attributes(meta_match.group(0))
        if len(attributes) == 0:
            continue

        content_value = str(attributes.get("content", "")).strip()
        if content_value == "":
            continue

        property_value = str(attributes.get("property", "")).strip().lower()
        name_value = str(attributes.get("name", "")).strip().lower()
        itemprop_value = str(attributes.get("itemprop", "")).strip().lower()

        if (
            property_value in META_IMAGE_PROPERTY_VALUES
            or name_value in META_IMAGE_NAME_VALUES
            or itemprop_value == "image"
        ):
            normalized = normalize_feed_asset_url(content_value, article_url)
            if normalized is not None:
                return normalized

    return None


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
