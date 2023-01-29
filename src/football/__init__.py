import sys
from pathlib import Path

# Try to read the api key from the secret file
try:
    with open(Path('src/football/football_api_token.txt'), 'r', encoding='utf-8') as secretFile:
        api_key = secretFile.read()
except:
    # If this fails there's nothing we can do, so exit
    print('No football_api_token.txt file found')
    sys.exit()

# Set the headers to include the api key
HEADERS = { 'X-Auth-Token': api_key }

from .football import Football

__all__ = [
    'Football',
]
