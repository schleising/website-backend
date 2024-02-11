from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from enum import Enum, auto
import logging
from zoneinfo import ZoneInfo
import json

from pydantic import ValidationError

import requests

from pymongo import ASCENDING
from pymongo.operations import UpdateOne

from pywebpush import webpush, WebPushException

from . import HEADERS, pl_match_collection, pl_table_collection, live_pl_table_collection, football_push

from .models import Table, LiveTableItem, FormItem, Matches, Match, MatchStatus

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

        # Get this seasons matches now
        self.get_season_matches()

        # Get todays matches now
        self.get_todays_matches()

    def get_season_matches(self) -> None:
        self.get_matches_between_dates(datetime(2023, 7, 1), datetime(2024, 6, 30))

    def get_todays_matches(self) -> None:
        matches = self.get_matches_between_dates(datetime.now(timezone.utc), datetime.now(timezone.utc))

        if matches is not None:
            for match in matches:
                logging.debug(f'{match.home_team.short_name:14} {match.score.full_time.home if match.score.full_time.home is not None else "-":>2} {match.score.full_time.away if match.score.full_time.away is not None else "-":>2} {match.away_team.short_name:14} {match.status}')

        self.update_live_table(matches)

        self.schedule_live_updates(matches)

    # Send a push notification for the change in match status
    def send_notification(self, title: str, message: str) -> None:
        # Get the subscriptions from the database
        if football_push is not None:
            subscriptions = football_push.find({})

            # Load the claims
            with open('/src/secrets/claims.json', 'r') as file:
                claims = json.load(file)

            # Send the push notifications
            for subscription in subscriptions:
                logging.debug(f'Sending notification to {subscription}')

                try:
                    webpush(
                        subscription_info=subscription,
                        data=json.dumps({
                            'title': title,
                            'body': message
                        }),
                        vapid_private_key='/src/secrets/private_key.pem',
                        vapid_claims=claims
                    )
                except WebPushException as ex:
                    logging.error(f'Error sending notification: {ex}')

                    if ex.response and ex.response.json():
                        extra = ex.response.json()
                        logging.error(f'Remote service replied with a {extra.code}:{extra.errno}, {extra.message}')
                else:
                    logging.debug('Notification sent successfully')

    def CompareMatchStates(self, previous_match: Match | None, current_match: Match) -> None:
        if previous_match is None:
            logging.error('No Previous Match')
            return

        if previous_match.status != current_match.status:
            logging.debug(f'Match Status Change: {previous_match.status} -> {current_match.status}')
            self.send_notification(
                title=str(current_match.status),
                message=f'{current_match.home_team.short_name} {current_match.score.full_time.home if current_match.score.full_time.home is not None else "-"} - {current_match.score.full_time.away if current_match.score.full_time.away is not None else "-"} {current_match.away_team.short_name}'
            )
        if previous_match.score.full_time.home != current_match.score.full_time.home or previous_match.score.full_time.away != current_match.score.full_time.away:
            logging.debug(f'Match Score Change: {previous_match.score.full_time.home} - {previous_match.score.full_time.away} -> {current_match.score.full_time.home} - {current_match.score.full_time.away}')
            self.send_notification(
                title=str(current_match.status),
                message=f'{current_match.home_team.short_name} {current_match.score.full_time.home if current_match.score.full_time.home is not None else "-"} - {current_match.score.full_time.away if current_match.score.full_time.away is not None else "-"} {current_match.away_team.short_name}'
            )

    def get_matches_between_dates(self, from_date: datetime, to_date: datetime) -> list[Match] | None:
        logging.debug('Getting Matches')

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
            logging.debug('Parsing Matches')

            try:
                matches = Matches.model_validate_json(response.content)
            except ValidationError as e:
                logging.error(f'Failed to Parse Matches: {response.content}')
                logging.error(e.json(indent=2))
                return None

            match_list = [match for match in matches.matches]

            # If a match utc time is midnight, set it to 3pm in the Europe/London timezone
            for match in match_list:
                # Check if the time is midnight
                if match.utc_date.time() == time(hour=0):
                    # Set the time to 3pm in the Europe/London timezone as timezone UTC
                    match.utc_date = datetime(match.utc_date.year, match.utc_date.month, match.utc_date.day, 15, tzinfo=ZoneInfo('Europe/London')).astimezone(timezone.utc)

                    # Log the change
                    logging.debug(f'Match Time Changed: {match.utc_date}')

                # Compare the current match state with the previous match state
                if pl_match_collection is not None:
                    previous_match = pl_match_collection.find_one({'id': match.id})

                    self.CompareMatchStates(previous_match, match)

            logging.debug('Creating Operations')
            operations = [UpdateOne({'id': match.id}, { '$set': match.model_dump() }, upsert=True) for match in match_list]

            if pl_match_collection is None:
                logging.error('No Database Connection')
            elif not operations:
                logging.debug('No Matches to Write')
            else:
                logging.debug(f'Writing {len(operations)} Entries')

                try:
                    pl_match_collection.bulk_write(operations)
                except:
                    logging.error("Failed to Write Matches to DB")

                logging.debug('Matches Added')
        else:
            logging.error(f'Download Error: {response.status_code}')
            return None

        return match_list

    def schedule_live_updates(self, matches: list[Match] | None) -> None:
        if matches is not None:
            if any(match.status in [MatchStatus.in_play, MatchStatus.paused, MatchStatus.suspended] for match in matches):
                logging.debug('At least one match is in play')

                self.scheduler.schedule_task(datetime.now(timezone.utc) + UPDATE_DELTA, self.get_todays_matches)

            elif any(match.status in [MatchStatus.awarded, MatchStatus.scheduled, MatchStatus.timed] for match in matches):
                # Remove postponed and cancelled matches from the calculation of next match time
                todays_matches = [match for match in matches if match.status not in [MatchStatus.postponed, MatchStatus.cancelled]]

                # Find the next match time
                next_match_utc = min(match.utc_date for match in todays_matches if match.utc_date > datetime.now(timezone.utc) - timedelta(minutes=100))

                if next_match_utc < datetime.now(timezone.utc):
                    next_match_utc = datetime.now(timezone.utc) + UPDATE_DELTA

                logging.debug(f'Next match time {next_match_utc}')
                
                self.scheduler.schedule_task(next_match_utc, self.get_todays_matches)
            else:
                logging.debug('No more matches today')
        else:
            logging.error('Rescheduling Match Update Due to Error')
            self.scheduler.schedule_task(datetime.now(timezone.utc) + UPDATE_DELTA, self.get_todays_matches)

    def get_table(self) -> None:
        logging.debug('Getting Table')

        # Get the date, if it is before the season starts, use the start date, otherwise use today's date
        if datetime.now(timezone.utc).date() < datetime(2023, 8, 15).date():
            table_date = datetime(2023, 8, 15).date()
        else:
            table_date = datetime.now(timezone.utc).date()

        try:
            response = requests.get(f'https://api.football-data.org/v4/competitions/PL/standings/?date={table_date}', headers=HEADERS, timeout=5)
        except requests.Timeout:
            logging.error('Table Download Timed Out')
        else:
            logging.debug('Table Downloaded')

            if response.status_code == requests.status_codes.codes.ok:
                table = Table.model_validate_json(response.content)

                logging.debug(f'Season Start: {table.season.start_date}')
                logging.debug(f'Season End  : {table.season.end_date}')

                # Update the database with the table
                if pl_table_collection is not None:
                    logging.debug('Writing Table')
                    try:
                        logging.debug('Creating Table Operations')
                        operations = [UpdateOne({'team.id': table_entry.team.id}, { '$set': table_entry.model_dump() }, upsert=True) for table_entry in table.standings[0].table]
                        pl_table_collection.bulk_write(operations)
                    except:
                        logging.error('Failed to Write Table to DB')
                    else:
                        logging.debug('Table Written')
                else:
                    logging.error('No Database Connection')

                for table_entry in table.standings[0].table:
                    logging.debug(f'{table_entry.position:02} {table_entry.team.short_name:14} {table_entry.points}')
            else:
                logging.error(f'Download Error: {response.status_code}')

    def update_live_table(self, matches: list[Match] | None) -> None:
        table_dict: dict[str, LiveTableItem] = {}

        if pl_table_collection is not None:
            # Get the table from the DB
            table_cursor = pl_table_collection.find({}).sort('position', ASCENDING)

            table_list = [LiveTableItem(**table_item) for table_item in table_cursor]

            # Create a dict indexed by team name
            table_dict = {table_item.team.short_name: table_item for table_item in table_list}

            # Add the form characters
            for table_item in table_dict.values():
                # Split out the W, D, L characters into a list
                form_list = table_item.form.split(',')

                # Reverse the list so the most recent game is last
                form_list.reverse()

                for form_item in form_list:
                    match form_item:
                        case 'W':
                            table_item.form_list.append(FormItem(character=form_item, css_class='form-win'))
                        case 'D':
                            table_item.form_list.append(FormItem(character=form_item, css_class='form-draw'))
                        case 'L':
                            table_item.form_list.append(FormItem(character=form_item, css_class='form-loss'))
                        case _:
                            # Will catch the form being empty at the start of the season
                            pass

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
                table_dict[team_name].score_string = f'{table_update.goals_for} - {table_update.goals_against}'

                match table_update.team_status:
                    case TeamStatus.winning:
                        table_dict[team_name].css_class = 'live-position winning'
                        if len(table_dict[team_name].form_list) >= 5:
                            table_dict[team_name].form_list.pop(0)
                        table_dict[team_name].form_list.append(FormItem(character='W', css_class='form-win'))
                    case TeamStatus.losing:
                        table_dict[team_name].css_class = 'live-position losing'
                        if len(table_dict[team_name].form_list) >= 5:
                            table_dict[team_name].form_list.pop(0)
                        table_dict[team_name].form_list.append(FormItem(character='L', css_class='form-loss'))
                    case TeamStatus.drawing:
                        table_dict[team_name].css_class = 'live-position drawing'
                        if len(table_dict[team_name].form_list) >= 5:
                            table_dict[team_name].form_list.pop(0)
                        table_dict[team_name].form_list.append(FormItem(character='D', css_class='form-draw'))

                if table_update.match_status.has_started and not table_update.match_status.has_finished:
                    table_dict[team_name].css_class = f'{table_dict[team_name].css_class} in-play'

                table_dict[team_name].played_games += table_update.played
                table_dict[team_name].won += table_update.won
                table_dict[team_name].draw += table_update.draw
                table_dict[team_name].lost += table_update.lost
                table_dict[team_name].points += table_update.points
                table_dict[team_name].goals_for += table_update.goals_for
                table_dict[team_name].goals_against += table_update.goals_against
                table_dict[team_name].goal_difference += table_update.goal_difference

            # Everton have been docked 10 points
            if 'Everton' in table_dict:
                table_dict['Everton'].points -= 10

            # Calculate the new positions
            table_list = [table_item for table_item in table_dict.values()]
            table_list = self.update_live_positions(table_list)

            # Write the updated table to the DB
            if live_pl_table_collection is not None:
                logging.debug('Writing Live Table')
                try:
                    logging.debug('Creating Live Table Operations')
                    operations = [UpdateOne({'team.id': table_entry.team.id}, { '$set': table_entry.model_dump() }, upsert=True) for table_entry in table_list]
                    live_pl_table_collection.bulk_write(operations)
                except:
                    logging.error('Failed to Write Live Table to DB')
                else:
                    logging.debug('Live Table Written')

            for table_entry in table_list:
                logging.debug(f'Live: {table_entry.position:02} {table_entry.team.short_name:14} {table_entry.points}')

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
