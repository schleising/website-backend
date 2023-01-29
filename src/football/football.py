from threading import Event
import logging

import requests
from pymongo.operations import UpdateOne

from . import HEADERS, pl_match_collection

from .models import Table, Matches

class Football:
    def __init__(self) -> None:
        pass

    def get_matches(self, terminate_event: Event) -> None:
        logging.info('Getting Matches')

        response = requests.get('https://api.football-data.org/v4/competitions/PL/matches?dateFrom=2022-07-01&dateTo=2023-06-30', headers=HEADERS)

        if response.status_code == requests.status_codes.codes.ok:
            logging.info('Parsing Matches')
            matches = Matches.parse_raw(response.content)

            logging.info('Creating Operations')
            operations = [UpdateOne({'id': match.id}, { '$set': match.dict() }, upsert=True) for match in matches.matches]

            if pl_match_collection is not None:
                logging.info(f'Writing {len(operations)} Entries')

                pl_match_collection.bulk_write(operations)

                logging.info('Matches Added')
            else:
                logging.info('No Database Connection')
        else:
            logging.info(f'Download Error: {response.status_code}')

    def get_table(self, terminate_event: Event) -> None:
        response = requests.get('https://api.football-data.org/v4/competitions/PL/standings/', headers=HEADERS)

        if response.status_code == requests.status_codes.codes.ok:
            table = Table.parse_raw(response.content)

            for table_entry in table.standings[0].table:
                print(f'{table_entry.position:02} {table_entry.team.short_name:20} {table_entry.points}')
