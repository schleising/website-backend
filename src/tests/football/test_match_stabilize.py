"""Tests for football-data.org match update stabilization."""

from __future__ import annotations

import copy
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

SRC = Path(__file__).resolve().parents[2]


def _load_football_module(module_name: str):
    filepath = SRC / "football" / f"{module_name}.py"
    full_name = f"football.{module_name}"
    spec = importlib.util.spec_from_file_location(full_name, filepath)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {full_name}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[full_name] = module
    if "football" not in sys.modules:
        package = types.ModuleType("football")
        package.__path__ = [str(SRC / "football")]
        sys.modules["football"] = package
    setattr(sys.modules["football"], module_name, module)
    module.__package__ = "football"
    spec.loader.exec_module(module)
    return module


_models = _load_football_module("models")
_stabilize = _load_football_module("match_stabilize")
Match = _models.Match
stabilize_match_update = _stabilize.stabilize_match_update
is_match_status_regression = _stabilize.is_match_status_regression
reset_active_blip_tracking = _stabilize.reset_active_blip_tracking
MatchStatus = _models.MatchStatus

_MATCH_TEMPLATE: dict = {
    "area": {"id": 1, "name": "World", "code": "INT", "flag": None},
    "competition": {
        "id": 2000,
        "name": "FIFA World Cup",
        "code": "WC",
        "type": "CUP",
        "emblem": "",
    },
    "season": {
        "id": 1,
        "startDate": "2026-06-11",
        "endDate": "2026-07-19",
        "currentMatchday": 1,
        "winner": None,
    },
    "minute": None,
    "injuryTime": None,
    "matchday": None,
    "stage": "LAST_32",
    "lastUpdated": "2026-06-30T22:05:00Z",
    "homeTeam": {"id": 100, "name": "France", "shortName": "FRA"},
    "awayTeam": {"id": 200, "name": "Sweden", "shortName": "SWE"},
    "odds": {"msg": ""},
    "referees": [],
}


def _match(
    *,
    match_id: int,
    status: str,
    home_score: int | None,
    away_score: int | None,
    minute: int | None = None,
    injury_time: int | None = None,
    duration: str = "REGULAR",
) -> Match:
    payload = copy.deepcopy(_MATCH_TEMPLATE)
    payload.update(
        {
            "id": match_id,
            "status": status,
            "utcDate": "2026-06-30T19:00:00Z",
            "minute": minute,
            "injuryTime": injury_time,
            "score": {
                "winner": None,
                "duration": duration,
                "fullTime": {"home": home_score, "away": away_score},
                "halfTime": {"home": None, "away": None},
            },
        }
    )
    return Match.model_validate(payload)


class MatchStabilizeTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_active_blip_tracking()

    def test_keeps_finished_status_when_api_blips_to_timed(self) -> None:
        previous = _match(
            match_id=1,
            status="FINISHED",
            home_score=1,
            away_score=2,
        )
        incoming = _match(
            match_id=1,
            status="TIMED",
            home_score=None,
            away_score=None,
        )

        stabilized = stabilize_match_update(previous, incoming)

        self.assertEqual(stabilized.status.value, "FINISHED")
        self.assertEqual(stabilized.score.full_time.home, 1)
        self.assertEqual(stabilized.score.full_time.away, 2)

    def test_keeps_in_play_status_when_api_blips_to_timed(self) -> None:
        previous = _match(
            match_id=2,
            status="IN_PLAY",
            home_score=0,
            away_score=0,
            minute=4,
        )
        incoming = _match(
            match_id=2,
            status="TIMED",
            home_score=None,
            away_score=None,
            minute=None,
        )

        stabilized = stabilize_match_update(previous, incoming)

        self.assertEqual(stabilized.status.value, "IN_PLAY")
        self.assertEqual(stabilized.score.full_time.home, 0)
        self.assertEqual(stabilized.score.full_time.away, 0)
        self.assertEqual(stabilized.minute, 4)

    def test_allows_forward_status_progression(self) -> None:
        previous = _match(
            match_id=3,
            status="TIMED",
            home_score=None,
            away_score=None,
        )
        incoming = _match(
            match_id=3,
            status="IN_PLAY",
            home_score=0,
            away_score=0,
            minute=1,
        )

        stabilized = stabilize_match_update(previous, incoming)

        self.assertEqual(stabilized.status.value, "IN_PLAY")
        self.assertEqual(stabilized.minute, 1)

    def test_detects_status_regression(self) -> None:
        self.assertTrue(
            is_match_status_regression(
                MatchStatus.finished,
                MatchStatus.timed,
            )
        )
        self.assertFalse(
            is_match_status_regression(
                MatchStatus.timed,
                MatchStatus.in_play,
            )
        )

    def test_logs_blip_cleared_when_api_recovers(self) -> None:
        stored = _match(
            match_id=1,
            status="FINISHED",
            home_score=1,
            away_score=2,
            minute=90,
        )
        blip = _match(
            match_id=1,
            status="TIMED",
            home_score=None,
            away_score=None,
        )
        recovered = _match(
            match_id=1,
            status="FINISHED",
            home_score=1,
            away_score=2,
            minute=90,
        )

        with patch.object(_stabilize.logger, "info") as mock_info:
            stabilized = stabilize_match_update(stored, blip)
            stabilize_match_update(stabilized, recovered)

        cleared_messages = [
            str(call)
            for call in mock_info.call_args_list
            if "API blip cleared" in str(call)
        ]
        self.assertEqual(len(cleared_messages), 1)

    def test_clears_injury_time_at_half_time(self) -> None:
        previous = _match(
            match_id=4,
            status="IN_PLAY",
            home_score=0,
            away_score=0,
            minute=45,
            injury_time=4,
        )
        half_time = _match(
            match_id=4,
            status="PAUSED",
            home_score=0,
            away_score=0,
            minute=45,
            injury_time=None,
        )

        with patch.object(_stabilize.logger, "info") as mock_info:
            stabilized = stabilize_match_update(previous, half_time)
            for _ in range(3):
                stabilized = stabilize_match_update(stabilized, half_time)

        self.assertEqual(stabilized.status.value, "PAUSED")
        self.assertIsNone(stabilized.injury_time)
        mock_info.assert_not_called()

    def test_keeps_injury_time_during_in_play_when_api_omits_it(self) -> None:
        previous = _match(
            match_id=5,
            status="IN_PLAY",
            home_score=0,
            away_score=0,
            minute=45,
            injury_time=4,
        )
        incoming = _match(
            match_id=5,
            status="IN_PLAY",
            home_score=0,
            away_score=0,
            minute=45,
            injury_time=None,
        )

        stabilized = stabilize_match_update(previous, incoming)

        self.assertEqual(stabilized.injury_time, 4)

    def test_clears_injury_time_when_clock_advances_into_extra_time(self) -> None:
        previous = _match(
            match_id=6,
            status="IN_PLAY",
            home_score=1,
            away_score=1,
            minute=90,
            injury_time=6,
        )
        incoming = _match(
            match_id=6,
            status="IN_PLAY",
            home_score=1,
            away_score=1,
            minute=93,
            injury_time=None,
        )

        with patch.object(_stabilize.logger, "info") as mock_info:
            stabilized = stabilize_match_update(previous, incoming)

        self.assertEqual(stabilized.minute, 93)
        self.assertIsNone(stabilized.injury_time)
        self.assertFalse(
            any("injury_time kept" in str(call) for call in mock_info.call_args_list)
        )

    def test_clears_stale_injury_time_already_written_in_extra_time(self) -> None:
        previous = _match(
            match_id=7,
            status="IN_PLAY",
            home_score=1,
            away_score=1,
            minute=93,
            injury_time=6,
        )
        incoming = _match(
            match_id=7,
            status="IN_PLAY",
            home_score=1,
            away_score=1,
            minute=93,
            injury_time=None,
        )

        stabilized = stabilize_match_update(previous, incoming)

        self.assertEqual(stabilized.minute, 93)
        self.assertIsNone(stabilized.injury_time)

    def test_clears_clock_when_match_moves_to_penalty_shootout(self) -> None:
        previous = _match(
            match_id=8,
            status="IN_PLAY",
            home_score=1,
            away_score=1,
            minute=120,
            injury_time=3,
            duration="EXTRA_TIME",
        )
        incoming = _match(
            match_id=8,
            status="IN_PLAY",
            home_score=1,
            away_score=1,
            minute=None,
            injury_time=None,
            duration="PENALTY_SHOOTOUT",
        )

        with patch.object(_stabilize.logger, "info") as mock_info:
            stabilized = stabilize_match_update(previous, incoming)

        self.assertEqual(stabilized.score.duration, "PENALTY_SHOOTOUT")
        self.assertIsNone(stabilized.minute)
        self.assertIsNone(stabilized.injury_time)
        self.assertFalse(
            any("injury_time kept" in str(call) for call in mock_info.call_args_list)
        )

    def test_clears_stale_clock_already_written_during_penalties(self) -> None:
        previous = _match(
            match_id=9,
            status="IN_PLAY",
            home_score=1,
            away_score=1,
            minute=120,
            injury_time=3,
            duration="PENALTY_SHOOTOUT",
        )
        incoming = _match(
            match_id=9,
            status="IN_PLAY",
            home_score=1,
            away_score=1,
            minute=None,
            injury_time=None,
            duration="PENALTY_SHOOTOUT",
        )

        stabilized = stabilize_match_update(previous, incoming)

        self.assertEqual(stabilized.score.duration, "PENALTY_SHOOTOUT")
        self.assertIsNone(stabilized.minute)
        self.assertIsNone(stabilized.injury_time)


if __name__ == "__main__":
    unittest.main()
