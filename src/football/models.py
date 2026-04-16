from datetime import datetime
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, model_validator


class ShortName(str, Enum):
    accrington_fc = "Accrington F.C."
    arsenal = "Arsenal"
    aston_villa = "Aston Villa"
    barnsley = "Barnsley"
    birmingham_city = "Birmingham City"
    blackburn_rovers = "Blackburn Rovers"
    blackpool = "Blackpool"
    bolton_wanderers = "Bolton Wanderers"
    bournemouth = "Bournemouth"
    bradford = "Bradford"
    bradford_park_avenue = "Bradford Park Avenue"
    brentford = "Brentford"
    brighton = "Brighton Hove"
    bristol_city = "Bristol City"
    burnley = "Burnley"
    bury = "Bury"
    cardiff_city = "Cardiff City"
    carlisle_united = "Carlisle United"
    charlton_athletic = "Charlton Athletic"
    chelsea = "Chelsea"
    coventry = "Coventry"
    crystal_palace = "Crystal Palace"
    darwen = "Darwen"
    derby = "Derby"
    everton = "Everton"
    fulham = "Fulham"
    glossop_north_end = "Glossop North End"
    grimsby_town = "Grimsby Town"
    huddersfield_town = "Huddersfield Town"
    hull_city = "Hull City"
    ipswich = "Ipswich Town"
    leeds_utd = "Leeds United"
    leicester = "Leicester City"
    leyton_orient = "Leyton Orient"
    liverpool = "Liverpool"
    luton = "Luton Town"
    man_city = "Man City"
    man_utd = "Man United"
    middlesbrough = "Middlesbrough"
    millwall = "Millwall"
    newcastle = "Newcastle"
    northampton_town = "Northampton Town"
    norwich_city = "Norwich City"
    nottingham = "Nottingham"
    notts_county = "Notts County"
    oldham_athletic = "Oldham Athletic"
    oxford_united = "Oxford United"
    portsmouth = "Portsmouth"
    preston_north_end = "Preston North End"
    queens_park_rangers = "Queens Park Rangers"
    reading = "Reading"
    sheffield_utd = "Sheffield Utd"
    sheffield_wed = "Sheffield Wed"
    southampton = "Southampton"
    stoke_city = "Stoke City"
    sunderland = "Sunderland"
    swansea_city = "Swansea City"
    swindon_town = "Swindon Town"
    tottentham = "Tottenham"
    watford = "Watford"
    west_bromwich_albion = "West Bromwich Albion"
    west_ham = "West Ham"
    wigan_athletic = "Wigan Athletic"
    wimbledon = "Wimbledon"
    wolves = "Wolverhampton"

    def __str__(self) -> str:
        match self:
            case self.brighton:
                return "Brighton"
            case self.wolves:
                return "Wolves"
            case self.nottingham:
                return "Notts Forest"
            case _:
                return self.value


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
        fallback_path = "/images/football/crests/unknown_team.svg"
        crest_value = (self.crest or "").strip()
        if crest_value == "":
            return fallback_path

        filename = crest_value.split("/")[-1].strip()
        if filename == "":
            return fallback_path

        suffix = Path(filename).suffix.lower()
        if suffix == ".svg":
            return f"/images/football/crests/{Path(filename).stem}.png"

        return f"/images/football/crests/{filename}"


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
            if self.home_team.short_name == team_name:
                if self.score.full_time.home > self.score.full_time.away:
                    return 3
                elif self.score.full_time.home == self.score.full_time.away:
                    return 1
                else:
                    return 0
            elif self.away_team.short_name == team_name:
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


class PushSubscriptionKeys(BaseModel):
    p256dh: str
    auth: str


class PushSubscription(BaseModel):
    endpoint: str
    expiration_time: int | None = Field(default=None, alias="expirationTime")
    keys: PushSubscriptionKeys

    class Config:
        populate_by_name = True


class PushSubscriptionDocument(BaseModel):
    subscription: PushSubscription
    team_ids: list[int] = Field(default_factory=list)
    username: str = "Anonymous User"

    @model_validator(mode="before")
    @classmethod
    def normalise_legacy_shape(cls, data):
        if not isinstance(data, dict):
            return data

        if "subscription" in data:
            return data

        if "endpoint" in data and "keys" in data:
            return {
                "subscription": {
                    "endpoint": data.get("endpoint"),
                    "expirationTime": data.get("expirationTime"),
                    "keys": data.get("keys", {}),
                },
                "team_ids": data.get("team_ids", []),
                "username": data.get("username", "Anonymous User"),
            }

        return data


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
    played: int
    points: int
    owea: str
    amounta: int
    oweb: str
    amountb: int
    best_case: int
    worst_case: int
    balance: str
    balance_amount: int
    live: bool
    home_team: str | None = None
    away_team: str | None = None
    home_team_score: int | None = None
    away_team_score: int | None = None


# Ordered list of bet data
class FootballBetList(BaseModel):
    bets: list[FootballBetData]
