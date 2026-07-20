"""Current Premier League season constants for the football worker.

Keep in sync with ``website/football/db_names.py`` (``CURRENT_PL_SEASON``) and
``bet/src/database.go``. Next rollover: edit ``CURRENT_PL_SEASON`` and
``CURRENT_PL_STANDINGS_CLAMP_DATE``.
"""

from __future__ import annotations

from datetime import date, datetime

# Live Premier League season key (YYYY_YYYY).
CURRENT_PL_SEASON = "2026_2027"

# Standings ``date=`` clamp used before that calendar day arrives.
# MUST be one day AFTER every club has played their first match of the new
# season (not merely first kickoff / season.startDate). Earlier dates can make
# football-data.org return the previous season's table.
# 2026/27: last MD1 games 2026-08-24 (Chelsea, Fulham) → clamp 2026-08-25.
CURRENT_PL_STANDINGS_CLAMP_DATE = date(2026, 8, 25)

LIVE_PL_TABLE_COLLECTION = "live_pl_table"


def pl_matches_collection_name(season_key: str = CURRENT_PL_SEASON) -> str:
    return f"pl_matches_{season_key}"


def pl_table_collection_name(season_key: str = CURRENT_PL_SEASON) -> str:
    return f"pl_table_{season_key}"


def current_pl_match_window() -> tuple[datetime, datetime]:
    """July–June UTC window used for the full-season matches pull."""
    start_year_str, end_year_str = CURRENT_PL_SEASON.split("_", 1)
    start_year = int(start_year_str)
    end_year = int(end_year_str)
    return datetime(start_year, 7, 1), datetime(end_year, 6, 30)
