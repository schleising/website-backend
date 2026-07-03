"""Merge football-data.org match payloads without regressing on transient API blips."""

from __future__ import annotations

import logging

from .models import FullTime, HalfTime, Match, MatchStatus, Score

logger = logging.getLogger(__name__)

_matches_with_active_blips: set[int] = set()

_STATUS_RANK: dict[MatchStatus, int] = {
    MatchStatus.postponed: 0,
    MatchStatus.cancelled: 0,
    MatchStatus.scheduled: 1,
    MatchStatus.timed: 1,
    MatchStatus.awarded: 1,
    MatchStatus.in_play: 2,
    MatchStatus.paused: 2,
    MatchStatus.suspended: 2,
    MatchStatus.finished: 3,
}

_DURATION_RANK: dict[str, int] = {
    "REGULAR": 0,
    "EXTRA_TIME": 1,
    "PENALTY_SHOOTOUT": 2,
}

_INJURY_TIME_ANCHOR_MINUTES = frozenset({45, 90, 105, 120})


def _status_rank(status: MatchStatus) -> int:
    return _STATUS_RANK.get(status, 0)


def is_match_status_regression(
    previous: MatchStatus,
    current: MatchStatus,
) -> bool:
    """True when current status is less progressed than previous (likely API blip)."""
    return _status_rank(current) < _status_rank(previous)


def _duration_rank(duration: str) -> int:
    return _DURATION_RANK.get(duration, 0)


def _prefer_status(previous: MatchStatus, incoming: MatchStatus) -> MatchStatus:
    if _status_rank(previous) > _status_rank(incoming):
        return previous
    return incoming


def _prefer_duration(previous: str, incoming: str) -> str:
    if _duration_rank(previous) > _duration_rank(incoming):
        return previous
    return incoming


def _merge_optional_int(incoming: int | None, previous: int | None) -> int | None:
    if incoming is not None:
        return incoming
    return previous


def _merge_injury_time(
    incoming: Match,
    previous: Match,
    status: MatchStatus,
) -> int | None:
    """Preserve injury time only while the live clock is still in that stoppage period."""
    if status == MatchStatus.paused:
        return incoming.injury_time
    if incoming.injury_time is not None:
        return incoming.injury_time
    if status in (MatchStatus.in_play, MatchStatus.suspended):
        if (
            previous.minute in _INJURY_TIME_ANCHOR_MINUTES
            and (incoming.minute is None or incoming.minute == previous.minute)
        ):
            return previous.injury_time
    return incoming.injury_time


def _merge_full_time(incoming: FullTime, previous: FullTime | None) -> FullTime:
    if previous is None:
        return incoming
    return FullTime(
        home=_merge_optional_int(incoming.home, previous.home),
        away=_merge_optional_int(incoming.away, previous.away),
    )


def _merge_half_time(incoming: HalfTime, previous: HalfTime | None) -> HalfTime:
    if previous is None:
        return incoming
    return HalfTime(
        home=_merge_optional_int(incoming.home, previous.home),
        away=_merge_optional_int(incoming.away, previous.away),
    )


def _merge_optional_full_time(
    incoming: FullTime | None,
    previous: FullTime | None,
) -> FullTime | None:
    if incoming is None:
        return previous
    if previous is None:
        return incoming
    return _merge_full_time(incoming, previous)


def _merge_score(previous: Score, incoming: Score) -> Score:
    return incoming.model_copy(
        update={
            "winner": incoming.winner or previous.winner,
            "duration": _prefer_duration(previous.duration, incoming.duration),
            "full_time": _merge_full_time(incoming.full_time, previous.full_time),
            "half_time": _merge_half_time(incoming.half_time, previous.half_time),
            "regular_time": _merge_optional_full_time(
                incoming.regular_time,
                previous.regular_time,
            ),
            "extra_time": _merge_optional_full_time(
                incoming.extra_time,
                previous.extra_time,
            ),
            "penalties": _merge_optional_full_time(
                incoming.penalties,
                previous.penalties,
            ),
        }
    )


def _format_score(full_time: FullTime) -> str:
    home = full_time.home
    away = full_time.away
    if home is None or away is None:
        return "-"
    return f"{home}-{away}"


def _blip_corrections(incoming: Match, stabilized: Match) -> list[str]:
    corrections: list[str] = []

    if incoming.status != stabilized.status:
        corrections.append(
            f"status {incoming.status.value}->{stabilized.status.value}"
        )

    incoming_score = incoming.score.full_time
    stabilized_score = stabilized.score.full_time
    if (
        incoming_score.home != stabilized_score.home
        or incoming_score.away != stabilized_score.away
    ):
        corrections.append(
            "score "
            f"{_format_score(incoming_score)}->{_format_score(stabilized_score)}"
        )

    if incoming.score.duration != stabilized.score.duration:
        corrections.append(
            f"duration {incoming.score.duration}->{stabilized.score.duration}"
        )

    if incoming.minute != stabilized.minute and stabilized.minute is not None:
        if incoming.minute is None:
            corrections.append(f"minute kept at {stabilized.minute}")
        else:
            corrections.append(
                f"minute {incoming.minute}->{stabilized.minute}"
            )

    if (
        stabilized.status == MatchStatus.in_play
        and incoming.injury_time != stabilized.injury_time
        and stabilized.injury_time is not None
        and incoming.injury_time is None
    ):
        corrections.append(f"injury_time kept at {stabilized.injury_time}")

    return corrections


def _blip_recovery_summary(stabilized: Match) -> str:
    parts = [
        f"status {stabilized.status.value}",
        f"score {_format_score(stabilized.score.full_time)}",
    ]
    if stabilized.minute is not None:
        minute = f"{stabilized.minute}'"
        if stabilized.injury_time is not None:
            minute = f"{stabilized.minute}+{stabilized.injury_time}'"
        parts.append(f"minute {minute}")
    return "; ".join(parts)


def reset_active_blip_tracking() -> None:
    """Clear in-memory blip tracking (for tests)."""
    _matches_with_active_blips.clear()


def stabilize_match_update(previous: Match | None, incoming: Match) -> Match:
    """Keep the furthest-progressed match state when the API sends stale data."""
    if previous is None:
        return incoming

    status = _prefer_status(previous.status, incoming.status)
    score = _merge_score(previous.score, incoming.score)

    stabilized = incoming.model_copy(
        update={
            "status": status,
            "score": score,
            "minute": _merge_optional_int(incoming.minute, previous.minute),
            "injury_time": _merge_injury_time(incoming, previous, status),
        }
    )

    corrections = _blip_corrections(incoming, stabilized)
    if corrections:
        _matches_with_active_blips.add(incoming.id)
        logger.info(
            "API blip corrected for match %s (%s vs %s): %s",
            incoming.id,
            incoming.home_team.display_name,
            incoming.away_team.display_name,
            "; ".join(corrections),
        )
    elif incoming.id in _matches_with_active_blips:
        _matches_with_active_blips.discard(incoming.id)
        logger.info(
            "API blip cleared for match %s (%s vs %s): %s",
            incoming.id,
            incoming.home_team.display_name,
            incoming.away_team.display_name,
            _blip_recovery_summary(stabilized),
        )

    return stabilized


def stabilize_match_updates(
    incoming_matches: list[Match],
    previous_by_id: dict[int, Match],
) -> list[Match]:
    return [
        stabilize_match_update(previous_by_id.get(match.id), match)
        for match in incoming_matches
    ]
