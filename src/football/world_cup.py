from __future__ import annotations

import logging
import os
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, ValidationError
from pymongo.operations import UpdateOne

from task_scheduler import TaskScheduler
from utils.network_utils import get_request

from . import requests_session, wc_match_collection, wc_standings_collection
from .models import Match, Matches, MatchStatus, Table, Team

WC_EDITION = "2026"
WC_API_BASE = "https://api.football-data.org/v4/competitions/WC"
WC_TOURNAMENT_START = datetime(2026, 6, 11, tzinfo=timezone.utc)
WC_TOURNAMENT_END = datetime(2026, 7, 19, 23, 59, 59, tzinfo=timezone.utc)
WC_GROUP_STAGE = "GROUP_STAGE"
WC_UPDATE_DELTA = timedelta(seconds=4)
WC_CREST_DIR = Path(os.environ.get("WC_CREST_DIR", "/crests/wc"))


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
            self.sync_tournament,
            timedelta(days=1),
        )

        self.sync_tournament()
        self.get_todays_matches()

    def sync_tournament(self) -> None:
        self.sync_matches()
        self.sync_standings()
        self.sync_teams_and_crests()

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

        response = get_request(
            f"{WC_API_BASE}/standings?season={self.edition}",
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
        suffix = Path(parsed.path).suffix.lower() or ".png"
        if suffix not in {".png", ".svg"}:
            suffix = ".png"

        destination = WC_CREST_DIR / f"{team.id}{suffix}"
        if destination.exists():
            return

        try:
            crest_response = requests_session.get(crest_url, timeout=20)
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
            self.schedule_live_updates(None)
            return

        try:
            matches = Matches.model_validate_json(response.content)
        except ValidationError as error:
            logging.error("Failed to parse today's World Cup matches: %s", error)
            self.schedule_live_updates(None)
            return

        self._normalise_match_times(matches.matches)
        self._write_matches(matches.matches)

        if any(match.stage == WC_GROUP_STAGE for match in matches.matches):
            self.sync_standings()

        self.schedule_live_updates(matches.matches)

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
