from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from pydantic import BaseModel, ValidationError
from pywebpush import WebPushException, webpush
from requests import status_codes

from . import football_push
from .match_stabilize import is_match_status_regression
from .models import Match, MatchStatus, PushSubscriptionDocument

if TYPE_CHECKING:
    from .models import Team

FOOTBALL_WEBAPP_ORIGIN = "https://football.schleising.net"
FOOTBALL_PUSH_ASSET_VERSION = "1.0.4"
FOOTBALL_PUSH_DEFAULT_ICON_PATH = (
    f"/icons/football/webapp/android-chrome-192x192.png?v{FOOTBALL_PUSH_ASSET_VERSION}"
)
FOOTBALL_PUSH_BADGE_PATH = (
    f"/icons/football/badge-192x192.png?v{FOOTBALL_PUSH_ASSET_VERSION}"
)


class MatchNotification(BaseModel):
    title: str
    message: str
    crest_url: str | None = None


def team_notification_label(team: Team) -> str:
    if team.short_name is not None:
        return str(team.short_name)
    if team.name is not None:
        return team.name
    if team.tla is not None:
        return team.tla
    return "Team"


def send_push_notification(
    title: str,
    message: str,
    team_ids: list[int] | list[int | None],
    icon: str | None = None,
    *,
    page_url: str = "https://www.schleising.net/football/",
    webapp_url: str = f"{FOOTBALL_WEBAPP_ORIGIN}/",
) -> None:
    valid_team_ids = sorted(
        {team_id for team_id in team_ids if isinstance(team_id, int) and team_id > 0}
    )
    if len(valid_team_ids) == 0:
        logging.debug("Skipping notification because no team IDs were provided")
        return

    if football_push is None:
        logging.error("No football push subscription collection configured")
        return

    subscriptions = football_push.find({"team_ids": {"$in": valid_team_ids}})

    with open("src/secrets/claims.json", "r", encoding="utf-8") as file:
        claims = json.load(file)

    if icon is None:
        notification_icon = FOOTBALL_PUSH_DEFAULT_ICON_PATH
    else:
        separator = "&" if "?" in icon else "?"
        notification_icon = f"{icon}{separator}v{FOOTBALL_PUSH_ASSET_VERSION}"

    notification_badge = FOOTBALL_PUSH_BADGE_PATH
    logging.info("Sending Notification: %s - %s", title, message)

    sent_endpoints: set[str] = set()

    for subscription_data in subscriptions:
        try:
            subscription_doc = PushSubscriptionDocument.model_validate(subscription_data)
        except ValidationError as ex:
            logging.error("Invalid subscription document: %s", ex)
            continue

        endpoint = subscription_doc.subscription.endpoint
        if endpoint in sent_endpoints:
            continue

        sent_endpoints.add(endpoint)
        logging.debug("Sending notification to endpoint %s", endpoint)

        try:
            webpush(
                subscription_info=subscription_doc.subscription.model_dump(
                    by_alias=True,
                    exclude_none=True,
                ),
                data=json.dumps(
                    {
                        "title": title,
                        "body": message,
                        "icon": notification_icon,
                        "badge": notification_badge,
                        "url": page_url,
                        "webapp_url": webapp_url,
                        "requireInteraction": True,
                    }
                ),
                headers={"Urgency": "normal"},
                ttl=60 * 60 * 24 * 7,
                vapid_private_key="/src/secrets/private_key.pem",
                vapid_claims=claims,
            )
        except WebPushException as ex:
            logging.error("Error sending notification: %s", ex)

            if ex.response is not None:
                logging.error("Status code: %s", ex.response.status_code)
                logging.error("Reason: %s", ex.response.reason)
                logging.error("Content: %s", ex.response.text.strip())

                if ex.response.status_code in [
                    status_codes.codes.not_found,
                    status_codes.codes.gone,
                ]:
                    logging.error(
                        "Subscription is no longer valid, removing from database"
                    )
                    football_push.delete_one(
                        {
                            "$or": [
                                {"subscription.endpoint": endpoint},
                                {"endpoint": endpoint},
                            ]
                        }
                    )
        else:
            logging.debug("Notification sent successfully")


def compare_match_states_and_notify(
    previous_match: Match | None,
    current_match: Match,
    *,
    crest_for_team: Callable[[Team], str],
    page_url: str = "https://www.schleising.net/football/",
    webapp_url: str = f"{FOOTBALL_WEBAPP_ORIGIN}/",
) -> None:
    if previous_match is None:
        logging.debug("No previous match state for match %s", current_match.id)
        return

    notification: MatchNotification | None = None
    home_label = team_notification_label(current_match.home_team)
    away_label = team_notification_label(current_match.away_team)

    if (
        previous_match.status != current_match.status
        and previous_match.status != MatchStatus.paused
        and current_match.status != MatchStatus.paused
        and not is_match_status_regression(
            previous_match.status,
            current_match.status,
        )
    ):
        if current_match.status == MatchStatus.in_play:
            title = "Kickoff"
        else:
            title = str(current_match.status)

        logging.debug(
            "Match status change: %s -> %s",
            previous_match.status,
            current_match.status,
        )
        notification = MatchNotification(
            title=title,
            message=(
                f'{home_label} '
                f'{current_match.score.full_time.home if current_match.score.full_time.home is not None else "0"} - '
                f'{current_match.score.full_time.away if current_match.score.full_time.away is not None else "0"} '
                f"{away_label}"
            ),
        )

    if (
        previous_match.score.full_time.home is not None
        and previous_match.score.full_time.away is not None
        and current_match.score.full_time.home is not None
        and current_match.score.full_time.away is not None
    ):
        if current_match.minute is not None:
            time_string = f" - {current_match.minute}'"
            if current_match.injury_time is not None:
                time_string = f"{time_string[0:-1]}+{current_match.injury_time}'"
        else:
            time_string = ""

        if previous_match.score.full_time.home < current_match.score.full_time.home:
            notification = MatchNotification(
                title=f"{home_label} Goal{time_string}",
                message=(
                    f"{home_label} {current_match.score.full_time.home} - "
                    f"{current_match.score.full_time.away} {away_label}"
                ),
                crest_url=crest_for_team(current_match.home_team),
            )
        elif previous_match.score.full_time.away < current_match.score.full_time.away:
            notification = MatchNotification(
                title=f"{away_label} Goal{time_string}",
                message=(
                    f"{home_label} {current_match.score.full_time.home} - "
                    f"{current_match.score.full_time.away} {away_label}"
                ),
                crest_url=crest_for_team(current_match.away_team),
            )
        elif previous_match.score.full_time.home > current_match.score.full_time.home:
            notification = MatchNotification(
                title=f"{home_label} VAR Correction",
                message=(
                    f"{home_label} {current_match.score.full_time.home} - "
                    f"{current_match.score.full_time.away} {away_label}"
                ),
                crest_url=crest_for_team(current_match.home_team),
            )
        elif previous_match.score.full_time.away > current_match.score.full_time.away:
            notification = MatchNotification(
                title=f"{away_label} VAR Correction",
                message=(
                    f"{home_label} {current_match.score.full_time.home} - "
                    f"{current_match.score.full_time.away} {away_label}"
                ),
                crest_url=crest_for_team(current_match.away_team),
            )

    if notification is not None:
        send_push_notification(
            notification.title,
            notification.message,
            [current_match.home_team.id, current_match.away_team.id],
            notification.crest_url,
            page_url=page_url,
            webapp_url=webapp_url,
        )
