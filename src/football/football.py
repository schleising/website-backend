from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from enum import Enum, auto
import logging

import requests
from pymongo import ASCENDING
from pymongo.operations import UpdateOne

from . import HEADERS, pl_match_collection, pl_table_collection, live_pl_table_collection

from .models import Table, LiveTableItem, Matches, Match, MatchStatus

from task_scheduler import TaskScheduler

UPDATE_DELTA = timedelta(seconds=10)

class TeamStatus(Enum):
    winning = auto()
    losing = auto()
    drawing = auto()

@dataclass
class TableUpdate:
    match_status: MatchStatus
    goals_for: int
    goals_against: int

    @property
    def played(self) -> int:
        if self.match_status.has_started:
            return 1
        else:
            return 0

    @property
    def goal_difference(self) -> int:
        if self.match_status.has_started:
            return self.goals_for - self.goals_against
        else:
            return 0

    @property
    def team_status(self) -> TeamStatus:
        if self.match_status.has_started:
            gd = self.goal_difference

            if gd == 0:
                return TeamStatus.drawing
            elif gd > 0:
                return TeamStatus.winning
            else:
                return TeamStatus.losing
        else:
            return TeamStatus.drawing

    @property
    def won(self) -> int:
        if self.match_status.has_started:
            match self.team_status:
                case TeamStatus.winning:
                    return 1
                case TeamStatus.losing:
                    return 0
                case TeamStatus.drawing:
                    return 0
        else:
            return 0

    @property
    def draw(self) -> int:
        if self.match_status.has_started:
            match self.team_status:
                case TeamStatus.winning:
                    return 0
                case TeamStatus.losing:
                    return 0
                case TeamStatus.drawing:
                    return 1
        else:
            return 0

    @property
    def lost(self) -> int:
        if self.match_status.has_started:
            match self.team_status:
                case TeamStatus.winning:
                    return 0
                case TeamStatus.losing:
                    return 1
                case TeamStatus.drawing:
                    return 0
        else:
            return 0

    @property
    def points(self) -> int:
        if self.match_status.has_started:
            match self.team_status:
                case TeamStatus.winning:
                    return 3
                case TeamStatus.losing:
                    return 0
                case TeamStatus.drawing:
                    return 1
        else:
            return 0

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
        self.scheduler.schedule_task(next_match_update_time, self.get_season_matches, timedelta(days=1))

        # Schedule the table update 30 seconds later
        self.scheduler.schedule_task(next_match_update_time + timedelta(seconds=30), self.get_table, timedelta(days=1))

        # Schedule the check for todays matches one minute later
        self.scheduler.schedule_task(next_match_update_time + timedelta(minutes=1), self.get_todays_matches, timedelta(days=1))

        # Get the table now
        self.get_table()

        # Get todays matches now
        self.get_todays_matches()

    def get_season_matches(self) -> None:
        self.get_matches_between_dates(datetime(2022, 7, 1), datetime(2023, 6, 30))

    def get_todays_matches(self) -> None:
        matches = self.get_matches_between_dates(datetime.now(timezone.utc), datetime.now(timezone.utc))

        if matches is not None:
            for match in matches:
                logging.info(f'{match.home_team.short_name:14} {match.score.full_time.home if match.score.full_time.home is not None else "TBD":3} {match.score.full_time.away if match.score.full_time.away is not None else "TBD":3} {match.away_team.short_name:14} {match.status}')

        self.update_live_table(matches)

        self.schedule_live_updates(matches)

    def get_matches_between_dates(self, from_date: datetime, to_date: datetime) -> list[Match] | None:
        logging.info('Getting Matches')

        match_list: list[Match] = []

        # Ensure times are in UTC and add one day to the end time as it is not inclusive
        from_date = from_date.astimezone(timezone.utc)
        to_date = to_date.astimezone(timezone.utc)

        try:
            response = requests.get(f'https://api.football-data.org/v4/competitions/PL/matches?dateFrom={from_date.date()}&dateTo={to_date.date()}', headers=HEADERS, timeout=5)
        except requests.Timeout:
            logging.error('Request Timed Out')
            return None

        if response.status_code == requests.status_codes.codes.ok:
            logging.info('Parsing Matches')
            matches = Matches.parse_raw(response.content)

            match_list = [match for match in matches.matches]

            logging.info('Creating Operations')
            operations = [UpdateOne({'id': match.id}, { '$set': match.dict() }, upsert=True) for match in match_list]

            if pl_match_collection is None:
                logging.error('No Database Connection')
            elif not operations:
                logging.info('No Matches to Write')
            else:
                logging.info(f'Writing {len(operations)} Entries')

                try:
                    pl_match_collection.bulk_write(operations)
                except:
                    logging.error("Failed to Write Matches to DB")

                logging.info('Matches Added')
        else:
            logging.info(f'Download Error: {response.status_code}')
            return None

        return match_list

    def schedule_live_updates(self, matches: list[Match] | None) -> None:
        if matches is not None:
            if any(match.status in [MatchStatus.in_play, MatchStatus.paused, MatchStatus.suspended] for match in matches):
                logging.info('At least one match is in play')

                self.scheduler.schedule_task(datetime.now(timezone.utc) + UPDATE_DELTA, self.get_todays_matches)

            elif any(match.status in [MatchStatus.awarded, MatchStatus.scheduled, MatchStatus.timed] for match in matches):
                # Find the next match time
                next_match_utc = min(match.utc_date for match in matches if match.utc_date > datetime.now(timezone.utc) - timedelta(minutes=100))

                if next_match_utc < datetime.now(timezone.utc):
                    next_match_utc = datetime.now(timezone.utc) + UPDATE_DELTA

                logging.info(f'Next match time {next_match_utc}')
                
                self.scheduler.schedule_task(next_match_utc, self.get_todays_matches)
            else:
                logging.info('No more matches today')
        else:
            logging.info('Rescheduling Match Update Due to Error')
            self.scheduler.schedule_task(datetime.now(timezone.utc) + UPDATE_DELTA, self.get_todays_matches)

    def get_table(self) -> None:
        logging.info('Getting Table')
        try:
            response = requests.get(f'https://api.football-data.org/v4/competitions/PL/standings/?date={datetime.now(timezone.utc).date()}', headers=HEADERS, timeout=5)
        except requests.Timeout:
            logging.error('Table Download Timed Out')
        else:
            logging.info('Table Downloaded')

            if response.status_code == requests.status_codes.codes.ok:
                table = Table.parse_raw(response.content)

                # Update the database with the table
                if pl_table_collection is not None:
                    logging.info('Writing Table')
                    try:
                        logging.info('Creating Table Operations')
                        operations = [UpdateOne({'team.id': table_entry.team.id}, { '$set': table_entry.dict() }, upsert=True) for table_entry in table.standings[0].table]
                        pl_table_collection.bulk_write(operations)
                    except:
                        logging.error('Failed to Write Table to DB')
                    else:
                        logging.info('Table Written')

                for table_entry in table.standings[0].table:
                    logging.info(f'{table_entry.position:02} {table_entry.team.short_name:14} {table_entry.points}')

    def update_live_table(self, matches: list[Match] | None) -> None:
        table_dict: dict[str, LiveTableItem] = {}

        if pl_table_collection is not None:
            # Get the table from the DB
            table_cursor = pl_table_collection.find({}).sort('position', ASCENDING)

            table_list = [LiveTableItem(**table_item) for table_item in table_cursor]

            # Create a dict indexed by team name
            table_dict = {table_item.team.short_name: table_item for table_item in table_list}

            update_dict: dict[str, TableUpdate] = {}

            # Go through today's matches to see if any teams need an update
            if matches is not None:
                for match in matches:
                    if match.status.has_started and match.score.full_time.home is not None and match.score.full_time.away is not None:
                        home_update = TableUpdate(match.status, match.score.full_time.home, match.score.full_time.away)
                        update_dict[match.home_team.short_name] = home_update

                        away_update = TableUpdate(match.status, match.score.full_time.away, match.score.full_time.home)
                        update_dict[match.away_team.short_name] = away_update

            # Update the table entry for the teams that need an update
            for team_name, table_update in update_dict.items():
                table_dict[team_name].has_started = table_update.match_status.has_started
                table_dict[team_name].is_halftime = table_update.match_status.is_halftime
                table_dict[team_name].has_finished = table_update.match_status.has_finished
                table_dict[team_name].played_games += table_update.played
                table_dict[team_name].won += table_update.won
                table_dict[team_name].draw += table_update.draw
                table_dict[team_name].lost += table_update.lost
                table_dict[team_name].points += table_update.points
                table_dict[team_name].goals_for += table_update.goals_for
                table_dict[team_name].goals_against += table_update.goals_against
                table_dict[team_name].goal_difference += table_update.goal_difference

            # Calculate the new positions
            table_list = [table_item for table_item in table_dict.values()]
            table_list = self.update_live_positions(table_list)

            # Write the updated table to the DB
            if live_pl_table_collection is not None:
                logging.info('Writing Live Table')
                try:
                    logging.info('Creating Live Table Operations')
                    operations = [UpdateOne({'team.id': table_entry.team.id}, { '$set': table_entry.dict() }, upsert=True) for table_entry in table_list]
                    live_pl_table_collection.bulk_write(operations)
                except:
                    logging.error('Failed to Write Live Table to DB')
                else:
                    logging.info('Live Table Written')

            for table_entry in table_list:
                logging.info(f'Live: {table_entry.position:02} {table_entry.team.short_name:14} {table_entry.points}')

    def update_live_positions(self, table_list: list[LiveTableItem]) -> list[LiveTableItem]:
        # Sort by team name ascending
        table_list = sorted(table_list, key=lambda table_item: table_item.team.short_name)

        # Sort by goals for descending
        table_list = sorted(table_list, key=lambda table_item: table_item.goals_for, reverse=True)

        # Sort by goal difference descending
        table_list = sorted(table_list, key=lambda table_item: table_item.goal_difference, reverse=True)

        # Sort by points descending
        table_list = sorted(table_list, key=lambda table_item: table_item.points, reverse=True)

        # Add the new position value
        for position, table_item in enumerate(table_list):
            table_item.position = position + 1

        return table_list
