import logging

import requests
from requests import Session, Response, status_codes
from requests.exceptions import (
    Timeout,
    ConnectionError,
    RequestException,
    HTTPError,
    TooManyRedirects
)

FOOTBALL_DATA_API_HOST = "api.football-data.org"
FOOTBALL_DATA_HOST = "football-data.org"
# TODO: Re-enable football-data.org sync — set to True once rate-limit issue is resolved.
FOOTBALL_API_ENABLED = False
_RATE_LIMIT_HEADER_PREFIXES = ("x-request", "x-ratelimit", "retry-after")


def is_football_data_url(url: str) -> bool:
    return FOOTBALL_DATA_HOST in url


def football_api_rate_limit_headers(headers) -> dict[str, str]:
    return {
        name: value
        for name, value in headers.items()
        if any(name.lower().startswith(prefix) for prefix in _RATE_LIMIT_HEADER_PREFIXES)
    }


def get_request(url: str, session: Session) -> Response | None:
    """
    Perform a GET request to the specified URL using the provided session.
    Returns the response if successful, or None if an error occurs.
    """
    is_football_api = is_football_data_url(url)
    # TODO: Remove this block when FOOTBALL_API_ENABLED is True (see flag above).
    if is_football_api and not FOOTBALL_API_ENABLED:
        logging.warning("Football API disabled; skipping GET %s", url)
        return None

    if is_football_api:
        logging.info("Football API request: GET %s", url)

    try:
        response = session.get(url, timeout=5)

        if response.status_code == status_codes.codes.ok:
            return response

        if response.status_code == status_codes.codes.too_many_requests:
            rate_headers = football_api_rate_limit_headers(response.headers)
            logging.error(
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
    except ConnectionError:
        if is_football_api:
            logging.error("Football API connection error: GET %s", url)
        else:
            logging.error("Connection error occurred while trying to reach %s.", url)
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


def get_football_data_http(url: str, **kwargs) -> Response | None:
    """GET a football-data.org URL outside the shared session (e.g. crest assets)."""
    # TODO: Remove this block when FOOTBALL_API_ENABLED is True (see flag above).
    if is_football_data_url(url) and not FOOTBALL_API_ENABLED:
        logging.warning("Football API disabled; skipping GET %s", url)
        return None

    if is_football_data_url(url):
        logging.info("Football API request: GET %s", url)

    try:
        response = requests.get(url, **kwargs)
    except RequestException as error:
        if is_football_data_url(url):
            logging.error("Football API request error: GET %s %s", url, error)
        else:
            logging.error("Request error: GET %s %s", url, error)
        return None

    if is_football_data_url(url):
        logging.info(
            "Football API GET %s -> %s (%.0f ms) rate=%s",
            url,
            response.status_code,
            response.elapsed.total_seconds() * 1000 if response.elapsed else 0.0,
            football_api_rate_limit_headers(response.headers) or "n/a",
        )

    return response
