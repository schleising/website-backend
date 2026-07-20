import atexit
import logging
import sys
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from database import BackendDatabase
from utils.network_utils import football_api_rate_limit_headers, log_football_api_traffic

from .pl_season import (
    LIVE_PL_TABLE_COLLECTION,
    pl_matches_collection_name,
    pl_table_collection_name,
)

logger = logging.getLogger(__name__)

FOOTBALL_DATA_API_HOST = "api.football-data.org"
PL_DATABASE = "pl_database"
WC_DATABASE = "wc_database"
WEB_DATABASE = "web_database"


def _log_football_api_response(response: requests.Response, *_args, **_kwargs) -> requests.Response:
    if FOOTBALL_DATA_API_HOST not in response.url:
        return response

    elapsed_ms = response.elapsed.total_seconds() * 1000 if response.elapsed else 0.0
    rate_headers = football_api_rate_limit_headers(response.headers)
    method = response.request.method if response.request is not None else "GET"

    log_football_api_traffic(
        "Football API %s %s -> %s (%.0f ms) rate=%s",
        method,
        response.url,
        response.status_code,
        elapsed_ms,
        rate_headers or "n/a",
    )

    if response.status_code == requests.codes.too_many_requests:
        logger.warning(
            "Football API rate limited (429) Retry-After=%s headers=%s",
            response.headers.get("Retry-After", "unknown"),
            rate_headers or "n/a",
        )

    return response

mongo_db = BackendDatabase()

pl_match_collection = mongo_db.get_collection(
    pl_matches_collection_name(), db_name=PL_DATABASE
)
pl_table_collection = mongo_db.get_collection(
    pl_table_collection_name(), db_name=PL_DATABASE
)
live_pl_table_collection = mongo_db.get_collection(
    LIVE_PL_TABLE_COLLECTION, db_name=PL_DATABASE
)
wc_match_collection = mongo_db.get_collection(
    "wc_matches_2026", db_name=WC_DATABASE
)
wc_standings_collection = mongo_db.get_collection(
    "wc_standings_2026", db_name=WC_DATABASE
)
live_wc_standings_collection = mongo_db.get_collection(
    "live_wc_standings_2026", db_name=WC_DATABASE
)
football_push = mongo_db.get_collection(
    "football_push_subscriptions", db_name=WEB_DATABASE
)

# Try to read the api key from the secret file
try:
    with open(Path('src/secrets/football_api_token.txt'), 'r', encoding='utf-8') as secretFile:
        api_key = secretFile.read().strip()
except:
    # If this fails there's nothing we can do, so exit
    print('No football_api_token.txt file found')
    sys.exit()

# Create a requests session
requests_session = requests.Session()

# Retry TCP/TLS connect and read failures only — not HTTP status codes (429 handled in get_request).
_football_api_retry = Retry(
    total=3,
    connect=2,
    read=2,
    redirect=0,
    status=0,
    backoff_factor=0.5,
    allowed_methods=["GET"],
)
adapter = HTTPAdapter(max_retries=_football_api_retry)
requests_session.mount('https://', adapter)

# urllib3 logs each connect/read retry at WARNING; failures are handled by Retry or get_request().
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

# Add headers to the session
requests_session.headers.update({
    'X-Auth-Token': api_key,
    'X-Api-Version': 'v4.1',
})
requests_session.hooks['response'].append(_log_football_api_response)

# Function to close the session on exit
def close_requests_session() -> None:
    requests_session.close()

atexit.register(close_requests_session)

from .football import Football
from .football_main import football_loop

__all__ = [
    'Football',
    'football_loop',
]
