from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
from typing import TypeVar
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
from .models import LiveTableItem, Match, Matches, MatchStatus, Table, TableItem, Team

TableRowT = TypeVar("TableRowT", bound=TableItem)
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
WC_TOURNAMENT_TZ = ZoneInfo("America/Los_Angeles")
WC_CREST_DIR = Path(os.environ.get("WC_CREST_DIR", "/crests/wc"))
WC_CREST_ALLOWED_HOSTS = frozenset({"crests.football-data.org"})


def _wc_tournament_today(*, now: datetime | None = None) -> date:
    current = now or datetime.now(timezone.utc)
    return current.astimezone(WC_TOURNAMENT_TZ).date()


def _match_on_wc_tournament_day(match: Match, day: date | None = None) -> bool:
    tournament_day = day or _wc_tournament_today()
    kickoff = match.utc_date
    if kickoff.tzinfo is None:
        kickoff = kickoff.replace(tzinfo=timezone.utc)
    return kickoff.astimezone(WC_TOURNAMENT_TZ).date() == tournament_day


def _wc_tournament_day_bounds(day: date | None = None) -> tuple[datetime, datetime]:
    tournament_day = day or _wc_tournament_today()
    day_start = datetime(
        tournament_day.year,
        tournament_day.month,
        tournament_day.day,
        tzinfo=WC_TOURNAMENT_TZ,
    ).astimezone(timezone.utc)
    next_day_start = day_start + timedelta(days=1)
    return day_start, next_day_start - timedelta(microseconds=1)


def _wc_tournament_day_api_dates(day: date | None = None) -> tuple[date, date]:
    """Calendar dates to pass to football-data dateFrom/dateTo for one tournament day."""
    day_start, day_end = _wc_tournament_day_bounds(day)
    return day_start.date(), day_end.date()


def _next_wc_tournament_midnight_utc(*, now: datetime | None = None) -> datetime:
    current = now or datetime.now(timezone.utc)
    local_now = current.astimezone(WC_TOURNAMENT_TZ)
    next_midnight_local = datetime(
        local_now.year,
        local_now.month,
        local_now.day,
        tzinfo=WC_TOURNAMENT_TZ,
    ) + timedelta(days=1)
    return next_midnight_local.astimezone(timezone.utc)


class CompetitionTeamsResponse(BaseModel):
    teams: list[Team] = Field(default_factory=list)


class WorldCup:
    def __init__(self, scheduler: TaskScheduler) -> None:
        self.scheduler = scheduler
        self.edition = WC_EDITION

        next_sync_time = _next_wc_tournament_midnight_utc()

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
        self._notify_match_updates(matches.matches)
        self._write_matches(matches.matches)

    def _matches_on_tournament_today(self, matches: list[Match]) -> list[Match]:
        tournament_today = _wc_tournament_today()
        return [
            match
            for match in matches
            if _match_on_wc_tournament_day(match, tournament_today)
        ]

    def sync_standings(self) -> None:
        logging.debug("Getting World Cup standings")

        today = _wc_tournament_today()
        if today < WC_TOURNAMENT_START.astimezone(WC_TOURNAMENT_TZ).date():
            table_date = WC_TOURNAMENT_START.astimezone(WC_TOURNAMENT_TZ).date()
        else:
            tournament_end = WC_TOURNAMENT_END.astimezone(WC_TOURNAMENT_TZ).date()
            table_date = min(today, tournament_end)

        response = get_request(
            (
                f"{WC_API_BASE}/standings"
                f"?season={self.edition}&date={table_date}"
            ),
            requests_session,
        )

        if wc_standings_collection is None:
            logging.error("No World Cup standings collection configured")
            return

        if response is None:
            logging.error("Failed to download World Cup standings")
            self._write_standings_operations(self._build_standings_operations_from_matches())
            return

        try:
            table = Table.model_validate_json(response.content)
        except ValidationError as error:
            logging.error("Failed to parse World Cup standings: %s", error)
            self._write_standings_operations(self._build_standings_operations_from_matches())
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
            operations = self._build_standings_operations_from_matches()

        self._write_standings_operations(operations)

    def _write_standings_operations(self, operations: list[UpdateOne]) -> None:
        if len(operations) == 0:
            logging.debug("No World Cup standings to write")
        elif wc_standings_collection is None:
            logging.error("No World Cup standings collection configured")
        else:
            wc_standings_collection.bulk_write(operations)
            logging.debug("Wrote %s World Cup group standings", len(operations))

        self.update_live_standings(None)

    def _build_standings_operations_from_matches(self) -> list[UpdateOne]:
        if wc_match_collection is None or wc_standings_collection is None:
            return []

        group_matches: dict[str, list[Match]] = {}
        for item in wc_match_collection.find(
            {"stage": WC_GROUP_STAGE, "group": {"$ne": None}}
        ):
            try:
                match = Match.model_validate(item)
            except ValidationError:
                continue

            if not isinstance(match.group, str):
                continue

            group_matches.setdefault(match.group, []).append(match)

        operations: list[UpdateOne] = []
        for group_enum, matches in sorted(group_matches.items()):
            slug = group_enum.removeprefix("GROUP_").lower()
            teams_by_id: dict[int, Team] = {}
            for match in matches:
                for team in (match.home_team, match.away_team):
                    if team.id is not None:
                        teams_by_id[team.id] = team

            if len(teams_by_id) == 0:
                continue

            table_rows = [
                TableItem(
                    position=index,
                    team=team,
                    played_games=0,
                    won=0,
                    draw=0,
                    lost=0,
                    points=0,
                    goals_for=0,
                    goals_against=0,
                    goal_difference=0,
                )
                for index, team in enumerate(
                    sorted(
                        teams_by_id.values(),
                        key=lambda value: value.display_name.casefold(),
                    ),
                    start=1,
                )
            ]
            table_dict = {
                row.team.id: row for row in table_rows if row.team.id is not None
            }

            for match in matches:
                if not match.status.has_finished:
                    continue
                home_score = match.score.full_time.home
                away_score = match.score.full_time.away
                if home_score is None or away_score is None:
                    continue

                for team, goals_for, goals_against in (
                    (match.home_team, home_score, away_score),
                    (match.away_team, away_score, home_score),
                ):
                    if team.id is None or team.id not in table_dict:
                        continue
                    row = table_dict[team.id]
                    row.played_games += 1
                    row.goals_for += goals_for
                    row.goals_against += goals_against
                    row.goal_difference += goals_for - goals_against
                    if goals_for > goals_against:
                        row.won += 1
                        row.points += 3
                    elif goals_for == goals_against:
                        row.draw += 1
                        row.points += 1
                    else:
                        row.lost += 1

            sorted_rows = self._update_group_positions(list(table_dict.values()))
            operations.append(
                UpdateOne(
                    {"edition": self.edition, "group_slug": slug},
                    {
                        "$set": {
                            "edition": self.edition,
                            "group_slug": slug,
                            "group_label": f"Group {slug.upper()}",
                            "group_enum": group_enum,
                            "stage": WC_GROUP_STAGE,
                            "type": "TOTAL",
                            "table": [row.model_dump() for row in sorted_rows],
                        }
                    },
                    upsert=True,
                )
            )

        return operations

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
        tournament_today = _wc_tournament_today(now=now)
        tournament_start = WC_TOURNAMENT_START.astimezone(WC_TOURNAMENT_TZ).date()
        tournament_end = WC_TOURNAMENT_END.astimezone(WC_TOURNAMENT_TZ).date()

        if tournament_today < tournament_start or tournament_today > tournament_end:
            self._schedule_next_tournament_poll(now)
            return

        api_date_from, api_date_to = _wc_tournament_day_api_dates(tournament_today)

        response = get_request(
            (
                f"{WC_API_BASE}/matches"
                f"?dateFrom={api_date_from}&dateTo={api_date_to}"
                f"&season={self.edition}"
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

        todays_group_matches = [
            match
            for match in matches.matches
            if match.stage == WC_GROUP_STAGE
            and match.group is not None
            and _match_on_wc_tournament_day(match, tournament_today)
        ]
        if any(match.status.has_finished for match in todays_group_matches):
            self.sync_standings()

        self.update_live_standings(matches.matches)

        self.schedule_live_updates(
            self._matches_on_tournament_today(matches.matches)
        )

    def _todays_started_group_matches(
        self, matches: list[Match] | None
    ) -> list[Match]:
        tournament_today = _wc_tournament_today()

        if matches is not None:
            candidate_matches = matches
        elif wc_match_collection is None:
            return []
        else:
            day_start, day_end = _wc_tournament_day_bounds(tournament_today)
            candidate_matches = []
            for item in wc_match_collection.find(
                {
                    "stage": WC_GROUP_STAGE,
                    "group": {"$ne": None},
                    "utc_date": {"$gte": day_start, "$lte": day_end},
                }
            ):
                try:
                    candidate_matches.append(Match.model_validate(item))
                except ValidationError:
                    continue

        return [
            match
            for match in candidate_matches
            if match.stage == WC_GROUP_STAGE
            and match.group is not None
            and _match_on_wc_tournament_day(match, tournament_today)
            and match.status.has_started
        ]

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

        todays_group_matches = self._todays_started_group_matches(matches)

        operations: list[UpdateOne] = []

        for document in group_documents:
            group_enum = document.get("group_enum")
            if not isinstance(group_enum, str):
                continue

            table_rows = [
                LiveTableItem.model_validate(row) for row in document.get("table", [])
            ]
            table_dict: dict[int, LiveTableItem] = {}
            for table_item in table_rows:
                if table_item.team.id is not None:
                    table_dict[table_item.team.id] = table_item

            update_dict: dict[int, TableUpdate] = {}
            for match in todays_group_matches:
                if match.group != group_enum:
                    continue

                home_score, away_score = match.score.display_scoreline()
                if home_score is None or away_score is None:
                    continue

                if match.home_team.id is not None:
                    update_dict[match.home_team.id] = TableUpdate(
                        match.status,
                        home_score,
                        away_score,
                    )

                if match.away_team.id is not None:
                    update_dict[match.away_team.id] = TableUpdate(
                        match.status,
                        away_score,
                        home_score,
                    )

            for table_item in table_dict.values():
                table_item.has_started = False
                table_item.is_halftime = False
                table_item.has_finished = False
                table_item.score_string = None
                table_item.css_class = None

            for team_id, table_update in update_dict.items():
                if team_id not in table_dict:
                    continue

                table_dict[team_id].has_started = (
                    table_update.match_status.has_started
                )
                table_dict[team_id].is_halftime = (
                    table_update.match_status.is_halftime
                )
                table_dict[team_id].has_finished = (
                    table_update.match_status.has_finished
                )
                table_dict[team_id].score_string = (
                    f"{table_update.goals_for} - {table_update.goals_against}"
                )

                match table_update.team_status:
                    case TeamStatus.winning:
                        table_dict[team_id].css_class = "live-position winning"
                    case TeamStatus.losing:
                        table_dict[team_id].css_class = "live-position losing"
                    case TeamStatus.drawing:
                        table_dict[team_id].css_class = "live-position drawing"

                if (
                    table_update.match_status.has_started
                    and not table_update.match_status.has_finished
                ):
                    table_dict[team_id].css_class = (
                        f"{table_dict[team_id].css_class} in-play"
                    )

                if table_update.match_status.has_finished:
                    continue

                table_dict[team_id].played_games += table_update.played
                table_dict[team_id].won += table_update.won
                table_dict[team_id].draw += table_update.draw
                table_dict[team_id].lost += table_update.lost
                table_dict[team_id].points += table_update.points
                table_dict[team_id].goals_for += table_update.goals_for
                table_dict[team_id].goals_against += table_update.goals_against
                table_dict[team_id].goal_difference += table_update.goal_difference

            table_list = self._update_group_positions(table_rows)
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
        self, table_list: Sequence[TableRowT]
    ) -> list[TableRowT]:
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
        tournament_start = WC_TOURNAMENT_START.astimezone(WC_TOURNAMENT_TZ).date()
        if _wc_tournament_today(now=now) < tournament_start:
            next_poll = WC_TOURNAMENT_START
        else:
            next_poll = _next_wc_tournament_midnight_utc(now=now)

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
                    self._schedule_next_tournament_poll(datetime.now(timezone.utc))
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
            self._schedule_next_tournament_poll(datetime.now(timezone.utc))
            return

        logging.error("Rescheduling World Cup match update due to error")
        self.scheduler.schedule_task(
            datetime.now(timezone.utc) + WC_UPDATE_DELTA,
            self.get_todays_matches,
        )
