from datetime import datetime, timedelta, timezone
import logging

import requests
from pymongo.operations import UpdateOne

from . import HEADERS, pl_match_collection

from .models import Table, Matches

from task_scheduler import TaskScheduler

class Football:
    def __init__(self, scheduler: TaskScheduler) -> None:
        self.scheduler = scheduler

        # Schedule the periodic task
        self.scheduler.schedule_task(datetime.now(timezone.utc) + timedelta(seconds=1), self.print_time, timedelta(seconds=1))

    def print_time(self) -> None:
        # Print the time on schedule
        logging.info(f'Time: {datetime.now(timezone.utc)}')

    def get_matches(self) -> None:
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

    def get_table(self) -> None:
        response = requests.get('https://api.football-data.org/v4/competitions/PL/standings/', headers=HEADERS)

        if response.status_code == requests.status_codes.codes.ok:
            table = Table.parse_raw(response.content)

            for table_entry in table.standings[0].table:
                print(f'{table_entry.position:02} {table_entry.team.short_name:20} {table_entry.points}')
