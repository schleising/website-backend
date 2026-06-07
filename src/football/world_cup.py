from __future__ import annotations

import logging
import os
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests
from pydantic import BaseModel, Field, ValidationError
from pymongo.operations import UpdateOne

from task_scheduler import TaskScheduler
from utils.network_utils import get_request

from . import (
    live_wc_standings_collection,
    requests_session,
    wc_match_collection,
    wc_standings_collection,
)
from .football import TableUpdate, TeamStatus
from .models import Match, Matches, MatchStatus, Table, TableItem, Team
from .push_notifications import (
    FOOTBALL_WEBAPP_ORIGIN,
    compare_match_states_and_notify,
)

WC_EDITION = "2026"
WC_API_BASE = "https://api.football-data.org/v4/competitions/WC"
WC_TOURNAMENT_START = datetime(2026, 6, 11, tzinfo=timezone.utc)
WC_TOURNAMENT_END = datetime(2026, 7, 19, 23, 59, 59, tzinfo=timezone.utc)
WC_GROUP_STAGE = "GROUP_STAGE"
WC_UPDATE_DELTA = timedelta(seconds=4)
WC_CREST_DIR = Path(os.environ.get("WC_CREST_DIR", "/crests/wc"))
WC_CREST_ALLOWED_HOSTS = frozenset({"crests.football-data.org"})


class CompetitionTeamsResponse(BaseModel):
    teams: list[Team] = Field(default_factory=list)


class WorldCup:
    def __init__(self, scheduler: TaskScheduler) -> None:
        self.scheduler = scheduler
        self.edition = WC_EDITION

        current_date_utc = datetime.now(timezone.utc).date()
        next_sync_time = datetime(
            current_date_utc.year,
            current_date_utc.month,
            current_date_utc.day,
            1,
            tzinfo=timezone.utc,
        )
        if datetime.now(timezone.utc).time() >= time(hour=1):
            next_sync_time += timedelta(days=1)

        self.scheduler.schedule_task(
            next_sync_time,
            self.sync_matches,
            timedelta(days=1),
        )
        self.scheduler.schedule_task(
            next_sync_time + timedelta(seconds=30),
            self.sync_standings,
            timedelta(days=1),
        )
        self.scheduler.schedule_task(
            next_sync_time + timedelta(minutes=1),
            self.get_todays_matches,
            timedelta(days=1),
        )

        self.sync_matches()
        self.sync_standings()
        self.sync_teams_and_crests()
        self.get_todays_matches()

    def sync_matches(self) -> None:
        logging.debug("Getting World Cup matches")

        response = get_request(
            (
                f"{WC_API_BASE}/matches"
                f"?dateFrom={WC_TOURNAMENT_START.date()}"
                f"&dateTo={WC_TOURNAMENT_END.date()}"
                f"&season={self.edition}"
            ),
            requests_session,
        )

        if response is None:
            logging.error("Failed to download World Cup matches")
            return

        try:
            matches = Matches.model_validate_json(response.content)
        except ValidationError as error:
            logging.error("Failed to parse World Cup matches: %s", error)
            return

        self._normalise_match_times(matches.matches)
        self._write_matches(matches.matches)

    def sync_standings(self) -> None:
        logging.debug("Getting World Cup standings")

        today = datetime.now(timezone.utc).date()
        if today < WC_TOURNAMENT_START.date():
            table_date = WC_TOURNAMENT_START.date()
        else:
            table_date = min(today, WC_TOURNAMENT_END.date())

        response = get_request(
            (
                f"{WC_API_BASE}/standings"
                f"?season={self.edition}&date={table_date}"
            ),
            requests_session,
        )

        if response is None:
            logging.error("Failed to download World Cup standings")
            return

        try:
            table = Table.model_validate_json(response.content)
        except ValidationError as error:
            logging.error("Failed to parse World Cup standings: %s", error)
            return

        if wc_standings_collection is None:
            logging.error("No World Cup standings collection configured")
            return

        operations: list[UpdateOne] = []
        for standing in table.standings:
            if standing.group is None:
                continue

            slug = standing.group.removeprefix("Group ").strip().lower()
            document = {
                "edition": self.edition,
                "group_slug": slug,
                "group_label": standing.group,
                "group_enum": f"GROUP_{slug.upper()}",
                "stage": standing.stage,
                "type": standing.type,
                "table": [row.model_dump() for row in standing.table],
            }
            operations.append(
                UpdateOne(
                    {"edition": self.edition, "group_slug": slug},
                    {"$set": document},
                    upsert=True,
                )
            )

        if len(operations) == 0:
            logging.debug("No World Cup standings to write")
            return

        wc_standings_collection.bulk_write(operations)
        logging.debug("Wrote %s World Cup group standings", len(operations))

    def sync_teams_and_crests(self) -> None:
        logging.debug("Getting World Cup teams")

        response = get_request(
            f"{WC_API_BASE}/teams?season={self.edition}",
            requests_session,
        )

        if response is None:
            logging.error("Failed to download World Cup teams")
            return

        try:
            teams_response = CompetitionTeamsResponse.model_validate_json(response.content)
        except ValidationError as error:
            logging.error("Failed to parse World Cup teams: %s", error)
            return

        WC_CREST_DIR.mkdir(parents=True, exist_ok=True)

        for team in teams_response.teams:
            if team.id is None:
                continue
            self._download_team_crest(team)

    def _download_team_crest(self, team: Team) -> None:
        crest_url = (team.crest or "").strip()
        if crest_url == "":
            return

        parsed = urlparse(crest_url)
        if parsed.scheme not in {"http", "https"} or parsed.hostname not in WC_CREST_ALLOWED_HOSTS:
            logging.warning(
                "Rejected World Cup crest URL for team %s: %s",
                team.id,
                crest_url,
            )
            return

        suffix = Path(parsed.path).suffix.lower() or ".png"
        if suffix not in {".png", ".svg"}:
            suffix = ".png"

        destination = WC_CREST_DIR / f"{team.id}{suffix}"
        if destination.exists():
            return

        try:
            crest_response = requests.get(crest_url, timeout=20)
            crest_response.raise_for_status()
            destination.write_bytes(crest_response.content)
            logging.debug("Saved World Cup crest for team %s", team.id)
        except Exception as error:
            logging.warning("Failed to download crest for team %s: %s", team.id, error)

    def _normalise_match_times(self, matches: list[Match]) -> None:
        for match in matches:
            if match.utc_date.time() == time(hour=0):
                match.utc_date = datetime(
                    match.utc_date.year,
                    match.utc_date.month,
                    match.utc_date.day,
                    15,
                    tzinfo=ZoneInfo("Europe/London"),
                ).astimezone(timezone.utc)

    def _world_cup_team_crest(self, team: Team) -> str:
        if team.id is None:
            return "/images/football/crests/unknown_team.svg"

        for suffix in (".png", ".svg"):
            if (WC_CREST_DIR / f"{team.id}{suffix}").exists():
                return f"/images/football/crests/wc/{team.id}{suffix}"

        return "/images/football/crests/unknown_team.svg"

    def _notify_match_updates(self, matches: list[Match]) -> None:
        if wc_match_collection is None:
            return

        page_url = "https://www.schleising.net/football/world-cup/"
        webapp_url = f"{FOOTBALL_WEBAPP_ORIGIN}/world-cup/"

        for match in matches:
            previous_document = wc_match_collection.find_one({"id": match.id})
            previous_match = None
            if previous_document is not None:
                previous_match = Match.model_validate(previous_document)

            compare_match_states_and_notify(
                previous_match,
                match,
                crest_for_team=self._world_cup_team_crest,
                page_url=page_url,
                webapp_url=webapp_url,
            )

    def _write_matches(self, matches: list[Match]) -> None:
        if wc_match_collection is None:
            logging.error("No World Cup match collection configured")
            return

        operations = [
            UpdateOne({"id": match.id}, {"$set": match.model_dump()}, upsert=True)
            for match in matches
        ]

        if len(operations) == 0:
            logging.debug("No World Cup matches to write")
            return

        wc_match_collection.bulk_write(operations)
        logging.debug("Wrote %s World Cup matches", len(operations))

    def get_todays_matches(self) -> None:
        now = datetime.now(timezone.utc)
        today = now.date()

        if today < WC_TOURNAMENT_START.date() or today > WC_TOURNAMENT_END.date():
            self._schedule_next_tournament_poll(now)
            return

        response = get_request(
            (
                f"{WC_API_BASE}/matches"
                f"?dateFrom={today}&dateTo={today}&season={self.edition}"
            ),
            requests_session,
        )

        if response is None:
            logging.error("Failed to download today's World Cup matches")
            self.update_live_standings(None)
            self.schedule_live_updates(None)
            return

        try:
            matches = Matches.model_validate_json(response.content)
        except ValidationError as error:
            logging.error("Failed to parse today's World Cup matches: %s", error)
            self.update_live_standings(None)
            self.schedule_live_updates(None)
            return

        self._normalise_match_times(matches.matches)
        self._notify_match_updates(matches.matches)
        self._write_matches(matches.matches)
        self.update_live_standings(matches.matches)

        self.schedule_live_updates(matches.matches)

    def update_live_standings(self, matches: list[Match] | None) -> None:
        if wc_standings_collection is None or live_wc_standings_collection is None:
            logging.error("No World Cup standings collections configured")
            return

        group_documents = list(
            wc_standings_collection.find({"edition": self.edition})
        )
        if len(group_documents) == 0:
            logging.debug("No World Cup group standings in DB to update live")
            return

        todays_group_matches: list[Match] = []
        if matches is not None:
            todays_group_matches = [
                match
                for match in matches
                if match.stage == WC_GROUP_STAGE and match.group is not None
            ]

        operations: list[UpdateOne] = []

        for document in group_documents:
            group_enum = document.get("group_enum")
            if not isinstance(group_enum, str):
                continue

            table_rows = [
                TableItem.model_validate(row) for row in document.get("table", [])
            ]
            table_dict = {
                table_item.team.display_name: table_item for table_item in table_rows
            }

            update_dict: dict[str, TableUpdate] = {}
            for match in todays_group_matches:
                if match.group != group_enum:
                    continue

                if (
                    match.status.has_started
                    and match.score.full_time.home is not None
                    and match.score.full_time.away is not None
                ):
                    home_update = TableUpdate(
                        match.status,
                        match.score.full_time.home,
                        match.score.full_time.away,
                    )
                    update_dict[match.home_team.display_name] = home_update

                    away_update = TableUpdate(
                        match.status,
                        match.score.full_time.away,
                        match.score.full_time.home,
                    )
                    update_dict[match.away_team.display_name] = away_update

            for team_name, table_update in update_dict.items():
                if team_name not in table_dict:
                    continue

                table_dict[team_name].played_games += table_update.played
                table_dict[team_name].won += table_update.won
                table_dict[team_name].draw += table_update.draw
                table_dict[team_name].lost += table_update.lost
                table_dict[team_name].points += table_update.points
                table_dict[team_name].goals_for += table_update.goals_for
                table_dict[team_name].goals_against += table_update.goals_against
                table_dict[team_name].goal_difference += table_update.goal_difference

            table_list = self._update_group_positions(list(table_dict.values()))
            group_slug = document.get("group_slug")
            if not isinstance(group_slug, str):
                continue

            live_document = {
                "edition": self.edition,
                "group_slug": group_slug,
                "group_label": document.get("group_label"),
                "group_enum": group_enum,
                "stage": document.get("stage"),
                "type": document.get("type"),
                "table": [row.model_dump() for row in table_list],
            }
            operations.append(
                UpdateOne(
                    {"edition": self.edition, "group_slug": group_slug},
                    {"$set": live_document},
                    upsert=True,
                )
            )

        if len(operations) == 0:
            logging.debug("No World Cup live standings to write")
            return

        try:
            live_wc_standings_collection.bulk_write(operations)
        except Exception:
            logging.error("Failed to write World Cup live standings to DB")
        else:
            logging.debug("Wrote %s World Cup live group standings", len(operations))

    def _update_group_positions(
        self, table_list: list[TableItem]
    ) -> list[TableItem]:
        table_list = sorted(
            table_list, key=lambda table_item: table_item.team.display_name.casefold()
        )
        table_list = sorted(
            table_list, key=lambda table_item: table_item.goals_for, reverse=True
        )
        table_list = sorted(
            table_list,
            key=lambda table_item: table_item.goal_difference,
            reverse=True,
        )
        table_list = sorted(
            table_list, key=lambda table_item: table_item.points, reverse=True
        )

        for position, table_item in enumerate(table_list, start=1):
            table_item.position = position

        return table_list

    def _schedule_next_tournament_poll(self, now: datetime) -> None:
        if now.date() < WC_TOURNAMENT_START.date():
            next_poll = WC_TOURNAMENT_START
        else:
            next_day = now.date() + timedelta(days=1)
            next_poll = datetime(
                next_day.year,
                next_day.month,
                next_day.day,
                tzinfo=timezone.utc,
            )

        logging.debug("Next World Cup live poll scheduled for %s", next_poll)
        self.scheduler.schedule_task(next_poll, self.get_todays_matches)

    def schedule_live_updates(self, matches: list[Match] | None) -> None:
        if matches is not None:
            if any(
                match.status
                in [
                    MatchStatus.in_play,
                    MatchStatus.paused,
                    MatchStatus.suspended,
                ]
                for match in matches
            ):
                logging.debug("At least one World Cup match is in play")
                self.scheduler.schedule_task(
                    datetime.now(timezone.utc) + WC_UPDATE_DELTA,
                    self.get_todays_matches,
                )
                return

            if any(
                match.status
                in [
                    MatchStatus.awarded,
                    MatchStatus.scheduled,
                    MatchStatus.timed,
                ]
                for match in matches
            ):
                todays_matches = [
                    match
                    for match in matches
                    if match.status
                    not in [MatchStatus.postponed, MatchStatus.cancelled]
                ]

                if len(todays_matches) == 0:
                    logging.debug("No more World Cup matches today")
                    return

                next_match_utc = min(
                    match.utc_date
                    for match in todays_matches
                    if match.utc_date
                    > datetime.now(timezone.utc) - timedelta(minutes=100)
                )

                if next_match_utc < datetime.now(timezone.utc):
                    next_match_utc = datetime.now(timezone.utc) + WC_UPDATE_DELTA

                logging.debug("Next World Cup match time %s", next_match_utc)
                self.scheduler.schedule_task(next_match_utc, self.get_todays_matches)
                return

            logging.debug("No more World Cup matches today")
            return

        logging.error("Rescheduling World Cup match update due to error")
        self.scheduler.schedule_task(
            datetime.now(timezone.utc) + WC_UPDATE_DELTA,
            self.get_todays_matches,
        )
