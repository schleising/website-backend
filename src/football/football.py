from datetime import datetime, time, timedelta, timezone
import logging

import requests
from pymongo.operations import UpdateOne

from . import HEADERS, pl_match_collection

from .models import Table, Matches, Match, MatchStatus

from task_scheduler import TaskScheduler

UPDATE_DELTA = timedelta(seconds=10)

class Football:
    def __init__(self, scheduler: TaskScheduler) -> None:
        self.scheduler = scheduler

        # Get the current date and time
        current_date_utc = datetime.now(timezone.utc).date()
        current_time_utc = datetime.now(timezone.utc).time()

        if current_time_utc < time(hour=1):
            # If it is before 1am today, set the update time to 1am today, both UTC
            next_match_update_time = datetime(current_date_utc.year, current_date_utc.month, current_date_utc.day, 1)
        else:
            # Otherwise set it to 1am tomorrow
            next_match_update_time = datetime(current_date_utc.year, current_date_utc.month, current_date_utc.day, 1) + timedelta(days=1)

        # Schedule the update of all matches
        self.scheduler.schedule_task(next_match_update_time, self.get_matches, timedelta(days=1))

        # Schedule the check for todays matches one minute later
        self.scheduler.schedule_task(next_match_update_time + timedelta(minutes=1), self.get_todays_matches, timedelta(days=1))

        # Get todays matches now
        self.get_todays_matches()

    def get_matches(self) -> None:
        self.get_matches_between_dates(datetime(2022, 7, 1), datetime(2023, 6, 30))

    def get_todays_matches(self) -> None:
        matches = self.get_matches_between_dates(datetime.now(timezone.utc), datetime.now(timezone.utc))

        for match in matches:
            logging.info(f'{match.home_team.short_name} {match.score.full_time.home} {match.score.full_time.away} {match.away_team.short_name} {match.status}')

        self.schedule_live_updates(matches)

    def get_matches_between_dates(self, from_date: datetime, to_date: datetime) -> list[Match]:
        logging.info('Getting Matches')

        match_list: list[Match] = []

        # Ensure times are in UTC and add one day to the end time as it is not inclusive
        from_date = from_date.astimezone(timezone.utc)
        to_date = to_date.astimezone(timezone.utc) + timedelta(days=1)

        response = requests.get(f'https://api.football-data.org/v4/competitions/PL/matches?dateFrom={from_date.date()}&dateTo={to_date.date()}', headers=HEADERS)

        if response.status_code == requests.status_codes.codes.ok:
            logging.info('Parsing Matches')
            matches = Matches.parse_raw(response.content)

            match_list = [match for match in matches.matches]

            logging.info('Creating Operations')
            operations = [UpdateOne({'id': match.id}, { '$set': match.dict() }, upsert=True) for match in match_list]

            if pl_match_collection is not None:
                logging.info(f'Writing {len(operations)} Entries')

                pl_match_collection.bulk_write(operations)

                logging.info('Matches Added')
            else:
                logging.info('No Database Connection')
        else:
            logging.info(f'Download Error: {response.status_code}')

        return match_list

    def schedule_live_updates(self, matches: list[Match]) -> None:
        if any(match.status in [MatchStatus.in_play, MatchStatus.paused, MatchStatus.suspended] for match in matches):
            logging.info('At least one match is in play')

            self.scheduler.schedule_task(datetime.now(timezone.utc) + UPDATE_DELTA, self.get_todays_matches)

        elif any(match.utc_date > datetime.now(timezone.utc) + timedelta(minutes=100) for match in matches):
            # Find the next match time
            next_match_utc = min(match.utc_date for match in matches if match.utc_date > datetime.now(timezone.utc) - timedelta(minutes=100))

            if next_match_utc < datetime.now(timezone.utc):
                next_match_utc = datetime.now(timezone.utc) + UPDATE_DELTA

            logging.info(f'Next match time {next_match_utc}')
            
            self.scheduler.schedule_task(next_match_utc, self.get_todays_matches)
        else:
            logging.info('No more matches today')

    def get_table(self) -> None:
        response = requests.get('https://api.football-data.org/v4/competitions/PL/standings/', headers=HEADERS)

        if response.status_code == requests.status_codes.codes.ok:
            table = Table.parse_raw(response.content)

            for table_entry in table.standings[0].table:
                print(f'{table_entry.position:02} {table_entry.team.short_name:20} {table_entry.points}')
