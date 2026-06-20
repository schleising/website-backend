import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from requests import Session, Response, status_codes
from requests.exceptions import (
    Timeout,
    ConnectionError,
    RequestException,
    HTTPError,
    TooManyRedirects,
)

FOOTBALL_DATA_API_HOST = "api.football-data.org"
FOOTBALL_DATA_HOST = "football-data.org"
FOOTBALL_API_MIN_INTERVAL = timedelta(seconds=4)
# Temporary — set False once football-data.org rate limiting is verified.
FOOTBALL_API_REQUEST_LOGGING = False
_DAILY_BACKOFF_SECONDS = (4, 8, 16, 32, 60)
_RATE_LIMIT_HEADER_PREFIXES = ("x-request", "x-ratelimit", "retry-after")


@dataclass(frozen=True)
class FootballApiFailure:
    retry_after: int | None = None
    was_rate_limited: bool = False


_last_football_api_failure: FootballApiFailure | None = None


def log_football_api_traffic(message: str, *args) -> None:
    """Log routine football-data.org traffic when FOOTBALL_API_REQUEST_LOGGING is enabled."""
    if FOOTBALL_API_REQUEST_LOGGING:
        logging.info(message, *args)


def log_live_poll_period_started(competition: str) -> None:
    logging.info("%s live match polling period started", competition)


def log_live_poll_period_finished(competition: str) -> None:
    logging.info("%s live match polling period finished", competition)


def log_live_poll_scheduled(competition: str, poll_at: datetime) -> None:
    logging.info(
        "%s live match polling period scheduled for %s",
        competition,
        poll_at.isoformat(),
    )


class LivePollPeriodTracker:
    """Track a live polling window from kickoff through full time (not each API request)."""

    def __init__(self, competition: str) -> None:
        self.competition = competition
        self._active = False

    @staticmethod
    def _aware(poll_at: datetime) -> datetime:
        if poll_at.tzinfo is None:
            return poll_at.replace(tzinfo=timezone.utc)
        return poll_at

    def log_poll_period_scheduled(
        self,
        poll_at: datetime,
        *,
        in_play: bool,
    ) -> None:
        """Log when the next polling period will begin — not each 4s in-play reschedule."""
        if in_play:
            return
        poll_at = self._aware(poll_at)
        now = datetime.now(timezone.utc)
        if poll_at <= now + FOOTBALL_API_MIN_INTERVAL:
            return
        log_live_poll_scheduled(self.competition, poll_at)

    def update(
        self,
        *,
        in_play: bool,
        upcoming: bool,
        next_poll_at: datetime | None,
    ) -> None:
        now = datetime.now(timezone.utc)
        poll_at = next_poll_at
        if poll_at is not None:
            poll_at = self._aware(poll_at)
        should_be_active = in_play or (
            upcoming
            and poll_at is not None
            and poll_at <= now + FOOTBALL_API_MIN_INTERVAL
        )
        if should_be_active:
            if not self._active:
                log_live_poll_period_started(self.competition)
                self._active = True
        elif self._active:
            log_live_poll_period_finished(self.competition)
            self._active = False

    def end_if_active(self) -> None:
        if self._active:
            log_live_poll_period_finished(self.competition)
            self._active = False


class FootballApiRateLimiter:
    """Enforce a minimum gap between football-data.org v4 API requests."""

    _lock = threading.Lock()
    _last_request_monotonic: float | None = None

    @classmethod
    def acquire(cls, url: str) -> None:
        with cls._lock:
            now = time.monotonic()
            if cls._last_request_monotonic is not None:
                wait_seconds = (
                    FOOTBALL_API_MIN_INTERVAL.total_seconds()
                    - (now - cls._last_request_monotonic)
                )
                if wait_seconds > 0:
                    log_football_api_traffic(
                        "Football API rate limit: waiting %.2fs before GET %s",
                        wait_seconds,
                        url,
                    )
                    time.sleep(wait_seconds)
            cls._last_request_monotonic = time.monotonic()


class DailyApiRetryScheduler:
    """Schedule daily football API tasks with exponential backoff until success."""

    def __init__(self, scheduler) -> None:
        self.scheduler = scheduler
        self._retry_scheduled: set[str] = set()
        self._attempts: dict[str, int] = {}

    def on_success(self, task_name: str) -> None:
        self._attempts[task_name] = 1
        self._retry_scheduled.discard(task_name)

    def schedule(
        self,
        task_name: str,
        callback: Callable[[], None],
        retry_after: int | None = None,
    ) -> None:
        if task_name in self._retry_scheduled:
            return

        attempt = self._attempts.get(task_name, 1)
        delay = daily_api_retry_delay(attempt, retry_after)
        self._retry_scheduled.add(task_name)
        self._attempts[task_name] = attempt + 1

        log_football_api_traffic(
            "scheduling %s retry attempt=%s delay=%ss",
            task_name,
            attempt,
            int(delay.total_seconds()),
        )

        def run_retry() -> None:
            self._retry_scheduled.discard(task_name)
            callback()

        self.scheduler.schedule_task(
            datetime.now(timezone.utc) + delay,
            run_retry,
        )


def is_football_data_url(url: str) -> bool:
    return FOOTBALL_DATA_HOST in url


def football_api_rate_limit_headers(headers) -> dict[str, str]:
    return {
        name: value
        for name, value in headers.items()
        if any(name.lower().startswith(prefix) for prefix in _RATE_LIMIT_HEADER_PREFIXES)
    }


def parse_retry_after(headers) -> int | None:
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def daily_api_retry_delay(attempt: int, retry_after: int | None = None) -> timedelta:
    index = min(max(attempt - 1, 0), len(_DAILY_BACKOFF_SECONDS) - 1)
    delay_seconds = _DAILY_BACKOFF_SECONDS[index]
    if retry_after is not None:
        delay_seconds = max(delay_seconds, retry_after)
    return timedelta(seconds=delay_seconds)


def get_last_football_api_failure() -> FootballApiFailure | None:
    return _last_football_api_failure


def get_request(url: str, session: Session) -> Response | None:
    """
    Perform a GET request to the specified URL using the provided session.
    Returns the response if successful, or None if an error occurs.
    """
    global _last_football_api_failure
    _last_football_api_failure = None

    is_football_api = is_football_data_url(url)
    if is_football_api:
        FootballApiRateLimiter.acquire(url)
        log_football_api_traffic("Football API request: GET %s", url)

    try:
        response = session.get(url, timeout=5)

        if response.status_code == status_codes.codes.ok:
            return response

        if response.status_code == status_codes.codes.too_many_requests:
            rate_headers = football_api_rate_limit_headers(response.headers)
            retry_after = parse_retry_after(response.headers)
            _last_football_api_failure = FootballApiFailure(
                retry_after=retry_after,
                was_rate_limited=True,
            )
            logging.warning(
                "Football API rate limited (429): GET %s Retry-After=%s headers=%s",
                url,
                response.headers.get("Retry-After", "unknown"),
                rate_headers or "n/a",
            )
        elif is_football_api:
            logging.error(
                "Football API request failed: GET %s -> %s headers=%s",
                url,
                response.status_code,
                football_api_rate_limit_headers(response.headers) or "n/a",
            )
        else:
            logging.error(
                "Request to %s failed with status code %s.",
                url,
                response.status_code,
            )
    except Timeout:
        if is_football_api:
            logging.error("Football API request timed out: GET %s", url)
        else:
            logging.error("Request to %s timed out.", url)
    except ConnectionError as error:
        if is_football_api:
            logging.error("Football API connection error: GET %s %s", url, error)
        else:
            logging.error("Connection error occurred while trying to reach %s: %s", url, error)
    except HTTPError as http_err:
        status = http_err.response.status_code if http_err.response is not None else "unknown"
        if is_football_api:
            logging.error("Football API HTTP error: GET %s status=%s", url, status)
        else:
            logging.error("HTTP error occurred: %s - Status Code: %s", http_err, status)
    except TooManyRedirects:
        logging.error("Too many redirects for URL: %s", url)
    except RequestException as req_err:
        if is_football_api:
            logging.error("Football API request error: GET %s %s", url, req_err)
        else:
            logging.error("An error occurred: %s", req_err)

    return None
