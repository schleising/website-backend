from __future__ import annotations
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class ShortName(str, Enum):
    arsenal = "Arsenal"
    aston_villa = "Aston Villa"
    bournemouth = "Bournemouth"
    brentford = "Brentford"
    brighton = "Brighton Hove"
    burnley = "Burnley"
    chelsea = "Chelsea"
    crystal_palace = "Crystal Palace"
    everton = "Everton"
    fulham = "Fulham"
    ipswich = "Ipswich Town"
    leeds_utd = "Leeds United"
    leicester = "Leicester City"
    liverpool = "Liverpool"
    luton = "Luton Town"
    man_city = "Man City"
    man_utd = "Man United"
    newcastle = "Newcastle"
    nottingham = "Nottingham"
    sheffield_utd = "Sheffield Utd"
    southampton = "Southampton"
    sunderland = "Sunderland"
    tottentham = "Tottenham"
    west_ham = "West Ham"
    wolves = "Wolverhampton"

    def __str__(self) -> str:
        match self.value:
            case self.brighton:
                return "Brighton"
            case self.wolves:
                return "Wolves"
            case self.nottingham:
                return "Notts Forest"
            case _:
                return str(self.value)


class MatchStatus(str, Enum):
    scheduled = "SCHEDULED"
    timed = "TIMED"
    in_play = "IN_PLAY"
    paused = "PAUSED"
    finished = "FINISHED"
    suspended = "SUSPENDED"
    postponed = "POSTPONED"
    cancelled = "CANCELLED"
    awarded = "AWARDED"

    def __str__(self) -> str:
        match self:
            case self.scheduled | self.timed | self.awarded:
                return "Not Started"
            case self.in_play:
                return "In Play"
            case self.paused:
                return "Half Time"
            case self.finished:
                return "Full Time"
            case self.suspended:
                return "Suspended"
            case self.postponed:
                return "Postponed"
            case self.cancelled:
                return "Cancelled"
            case _:
                return "Error"

    @property
    def has_started(self) -> bool:
        match self:
            case (
                MatchStatus.scheduled
                | MatchStatus.timed
                | MatchStatus.postponed
                | MatchStatus.cancelled
            ):
                return False
            case (
                MatchStatus.in_play
                | MatchStatus.paused
                | MatchStatus.finished
                | MatchStatus.suspended
                | MatchStatus.awarded
            ):
                return True

    @property
    def is_halftime(self) -> bool:
        if self == MatchStatus.paused:
            return True
        else:
            return False

    @property
    def has_finished(self) -> bool:
        if self == MatchStatus.finished:
            return True
        else:
            return False

    @property
    def is_live(self) -> bool:
        if self in [MatchStatus.in_play, MatchStatus.paused]:
            return True
        else:
            return False


class Filters(BaseModel):
    season: str


class Area(BaseModel):
    id: int
    name: str
    code: str
    flag: str


class Competition(BaseModel):
    id: int
    name: str
    code: str
    type: str
    emblem: str


class Team(BaseModel):
    id: int
    name: str
    short_name: ShortName = Field(..., alias="shortName")
    tla: str
    crest: str

    class Config:
        populate_by_name = True

    @property
    def local_crest(self) -> str:
        return (
            f'/images/football/crests/{self.crest.split('/')[-1].replace("svg", "png")}'
        )


class Season(BaseModel):
    id: int
    start_date: str = Field(..., alias="startDate")
    end_date: str = Field(..., alias="endDate")
    current_matchday: int = Field(..., alias="currentMatchday")
    winner: Team | None = None

    class Config:
        populate_by_name = True


class TableItem(BaseModel):
    position: int
    team: Team
    played_games: int = Field(..., alias="playedGames")
    form: str
    won: int
    draw: int
    lost: int
    points: int
    goals_for: int = Field(..., alias="goalsFor")
    goals_against: int = Field(..., alias="goalsAgainst")
    goal_difference: int = Field(..., alias="goalDifference")

    class Config:
        populate_by_name = True


class FormItem(BaseModel):
    character: str
    css_class: str


class LiveTableItem(TableItem):
    has_started: bool = False
    is_halftime: bool = False
    has_finished: bool = False
    score_string: str | None = None
    css_class: str | None = None
    form_list: list[FormItem] = []


class Standing(BaseModel):
    stage: str
    type: str
    group: str | None = None
    table: list[TableItem]


class Table(BaseModel):
    filters: Filters
    area: Area
    competition: Competition
    season: Season
    standings: list[Standing]


class ResultSet(BaseModel):
    count: int
    first: str | None = None
    last: str | None = None
    played: int | None = None


class FullTime(BaseModel):
    home: int | None = None
    away: int | None = None


class HalfTime(BaseModel):
    home: int | None = None
    away: int | None = None


class Score(BaseModel):
    winner: str | None = None
    duration: str
    full_time: FullTime = Field(..., alias="fullTime")
    half_time: HalfTime = Field(..., alias="halfTime")

    class Config:
        populate_by_name = True


class Odds(BaseModel):
    msg: str


class Referee(BaseModel):
    id: int
    name: str
    type: str
    nationality: str | None = None


class Match(BaseModel):
    area: Area
    competition: Competition
    season: Season
    id: int
    utc_date: datetime = Field(..., alias="utcDate")
    local_date: datetime | None = None
    status: MatchStatus
    minute: int | None = None
    injury_time: int | None = Field(default=None, alias="injuryTime")
    matchday: int
    stage: str
    group: str | None = None
    last_updated: datetime = Field(..., alias="lastUpdated")
    home_team: Team = Field(..., alias="homeTeam")
    away_team: Team = Field(..., alias="awayTeam")
    score: Score
    odds: Odds
    referees: list[Referee]

    class Config:
        populate_by_name = True

    def team_points(self, team_name: str) -> int | None:
        if (
            self.score.full_time.home is not None
            and self.score.full_time.away is not None
        ):
            if self.home_team.name == team_name:
                if self.score.full_time.home > self.score.full_time.away:
                    return 3
                elif self.score.full_time.home == self.score.full_time.away:
                    return 1
                else:
                    return 0
            elif self.away_team.name == team_name:
                if self.score.full_time.away > self.score.full_time.home:
                    return 3
                elif self.score.full_time.away == self.score.full_time.home:
                    return 1
                else:
                    return 0
            else:
                return None
        else:
            return None


class Matches(BaseModel):
    filters: Filters
    result_set: ResultSet = Field(..., alias="resultSet")
    competition: Competition
    matches: list[Match]

    class Config:
        populate_by_name = True


class MatchList(BaseModel):
    matches: list[Match]


# Simplified match type for gpt4
class SimplifiedMatch(BaseModel):
    status: str
    start_time_iso: str
    home_team: str
    home_team_score: int | None = None
    away_team: str
    away_team_score: int | None = None


# Simplified table row for gpt4
class SimplifiedTableRow(BaseModel):
    position: int
    team: str
    played: int
    won: int
    drawn: int
    lost: int
    goals_for: int
    goals_against: int
    goal_difference: int
    points: int


# Simplified football data for gpt4
class SimplifiedFootballData(BaseModel):
    matches: list[SimplifiedMatch]
    table: list[SimplifiedTableRow]


# Data for the football bet page
class FootballBetData(BaseModel):
    team_name: str
    name: str
    points: int
    owea: str
    amounta: int
    oweb: str
    amountb: int
    best_case: int
    worst_case: int
    balance: str
    balance_amount: int


# Ordered list of bet data
class FootballBetList(BaseModel):
    bets: list[FootballBetData]
