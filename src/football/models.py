from __future__ import annotations
from datetime import date, datetime

from pydantic import BaseModel, Field

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

class Season(BaseModel):
    id: int
    start_date: str = Field(..., alias='startDate')
    end_date: str = Field(..., alias='endDate')
    current_matchday: int = Field(..., alias='currentMatchday')
    winner: str | None

    class Config:
        allow_population_by_field_name = True

class Team(BaseModel):
    id: int
    name: str
    short_name: str = Field(..., alias='shortName')
    tla: str
    crest: str

    class Config:
        allow_population_by_field_name = True

class TableItem(BaseModel):
    position: int
    team: Team
    played_games: int = Field(..., alias='playedGames')
    form: str
    won: int
    draw: int
    lost: int
    points: int
    goals_for: int = Field(..., alias='goalsFor')
    goals_against: int = Field(..., alias='goalsAgainst')
    goal_difference: int = Field(..., alias='goalDifference')

    class Config:
        allow_population_by_field_name = True

class Standing(BaseModel):
    stage: str
    type: str
    group: str | None
    table: list[TableItem]

class Table(BaseModel):
    filters: Filters
    area: Area
    competition: Competition
    season: Season
    standings: list[Standing]

class ResultSet(BaseModel):
    count: int
    first: str
    last: str
    played: int

class FullTime(BaseModel):
    home: int | None
    away: int | None


class HalfTime(BaseModel):
    home: int | None
    away: int | None


class Score(BaseModel):
    winner: str | None
    duration: str
    full_time: FullTime = Field(..., alias='fullTime')
    half_time: HalfTime = Field(..., alias='halfTime')

    class Config:
        allow_population_by_field_name = True

class Odds(BaseModel):
    msg: str


class Referee(BaseModel):
    id: int
    name: str
    type: str
    nationality: str


class Match(BaseModel):
    area: Area
    competition: Competition
    season: Season
    id: int
    utc_date: datetime = Field(..., alias='utcDate')
    status: str
    matchday: int
    stage: str
    group: str | None
    last_updated: datetime = Field(..., alias='lastUpdated')
    home_team: Team = Field(..., alias='homeTeam')
    away_team: Team = Field(..., alias='awayTeam')
    score: Score
    odds: Odds
    referees: list[Referee]

    class Config:
        allow_population_by_field_name = True

class Matches(BaseModel):
    filters: Filters
    result_set: ResultSet = Field(..., alias='resultSet')
    competition: Competition
    matches: list[Match]

    class Config:
        allow_population_by_field_name = True
