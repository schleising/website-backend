import atexit
import logging
import sys
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter

from database import BackendDatabase
from utils.network_utils import FOOTBALL_API_ENABLED, football_api_rate_limit_headers

logger = logging.getLogger(__name__)

FOOTBALL_DATA_API_HOST = "api.football-data.org"

if not FOOTBALL_API_ENABLED:
    # TODO: Remove when FOOTBALL_API_ENABLED is True in utils/network_utils.py.
    logger.warning("Football API calls are DISABLED (FOOTBALL_API_ENABLED=False)")


def _log_football_api_response(response: requests.Response, *_args, **_kwargs) -> requests.Response:
    if FOOTBALL_DATA_API_HOST not in response.url:
        return response

    elapsed_ms = response.elapsed.total_seconds() * 1000 if response.elapsed else 0.0
    rate_headers = football_api_rate_limit_headers(response.headers)
    method = response.request.method if response.request is not None else "GET"

    logger.info(
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

mongo_db.set_database('web_database')

pl_match_collection = mongo_db.get_collection('pl_matches_2025_2026')
pl_table_collection = mongo_db.get_collection('pl_table_2025_2026')
live_pl_table_collection = mongo_db.get_collection('live_pl_table')
wc_match_collection = mongo_db.get_collection('wc_matches_2026')
wc_standings_collection = mongo_db.get_collection('wc_standings_2026')
live_wc_standings_collection = mongo_db.get_collection('live_wc_standings_2026')
football_push = mongo_db.get_collection('football_push_subscriptions')

# Try to read the api key from the secret file
try:
    with open(Path('src/secrets/football_api_token.txt'), 'r', encoding='utf-8') as secretFile:
        api_key = secretFile.read().strip()
except:
    # If this fails there's nothing we can do, so exit
    print('No football_api_token.txt file found')
    sys.exit()

# Create a requests session
requests_session = requests.Session()

# Add a retry strategy to the session
adapter = HTTPAdapter(max_retries=3)
requests_session.mount('https://', adapter)

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
