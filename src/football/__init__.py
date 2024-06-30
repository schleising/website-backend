import sys
from pathlib import Path

from database import BackendDatabase

mongo_db = BackendDatabase()

mongo_db.set_database('web_database')

pl_match_collection = mongo_db.get_collection('pl_matches_2024_2025')
pl_table_collection = mongo_db.get_collection('pl_table_2024_2025')
live_pl_table_collection = mongo_db.get_collection('live_pl_table')
football_push = mongo_db.get_collection('football_push_subscriptions')

# Try to read the api key from the secret file
try:
    with open(Path('src/secrets/football_api_token.txt'), 'r', encoding='utf-8') as secretFile:
        api_key = secretFile.read().strip()
except:
    # If this fails there's nothing we can do, so exit
    print('No football_api_token.txt file found')
    sys.exit()

# Set the headers to include the api key
HEADERS = { 'X-Auth-Token': api_key }

from .football import Football
from .football_main import football_loop

__all__ = [
    'Football',
    'football_loop',
]
