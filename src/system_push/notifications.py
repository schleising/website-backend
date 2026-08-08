from __future__ import annotations

import json
import logging

from pydantic import ValidationError
from pywebpush import WebPushException, webpush
from requests import status_codes

from . import system_push_subscriptions
from .models import SystemPushSubscriptionDocument

SYSTEM_NOTIFICATIONS_PAGE_URL = "https://www.schleising.net/account/notifications/"
SYSTEM_PUSH_DEFAULT_ICON = "/icons/units/android-chrome-192x192.png"


def send_system_push(
    topic_id: str,
    title: str,
    message: str,
    *,
    page_url: str = SYSTEM_NOTIFICATIONS_PAGE_URL,
    icon: str | None = None,
) -> None:
    topic = topic_id.strip()
    if topic == "":
        logging.debug("Skipping system push because topic_id was empty")
        return

    if system_push_subscriptions is None:
        logging.error("No system push subscription collection configured")
        return

    subscriptions = system_push_subscriptions.find({"topics": topic})

    try:
        with open("src/secrets/claims.json", "r", encoding="utf-8") as file:
            claims = json.load(file)
    except FileNotFoundError:
        logging.error("VAPID claims file not found; cannot send system push")
        return

    notification_icon = icon or SYSTEM_PUSH_DEFAULT_ICON
    logging.info("Sending system notification [%s]: %s - %s", topic, title, message)

    sent_endpoints: set[str] = set()

    for subscription_data in subscriptions:
        try:
            subscription_doc = SystemPushSubscriptionDocument.model_validate(
                subscription_data
            )
        except ValidationError as ex:
            logging.error("Invalid system push subscription document: %s", ex)
            continue

        endpoint = subscription_doc.subscription.endpoint
        if endpoint in sent_endpoints:
            continue

        sent_endpoints.add(endpoint)
        logging.debug("Sending system notification to endpoint %s", endpoint)

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
                        "url": page_url,
                        "requireInteraction": True,
                    }
                ),
                headers={"Urgency": "normal"},
                ttl=60 * 60 * 24 * 7,
                vapid_private_key="/src/secrets/private_key.pem",
                vapid_claims=claims,
            )
        except WebPushException as ex:
            logging.error("Error sending system notification: %s", ex)

            if ex.response is not None:
                logging.error("Status code: %s", ex.response.status_code)
                logging.error("Reason: %s", ex.response.reason)
                logging.error("Content: %s", ex.response.text.strip())

                if ex.response.status_code in [
                    status_codes.codes.not_found,
                    status_codes.codes.gone,
                ]:
                    logging.error(
                        "System subscription is no longer valid, removing from database"
                    )
                    system_push_subscriptions.delete_one(
                        {
                            "$or": [
                                {"subscription.endpoint": endpoint},
                                {"endpoint": endpoint},
                            ]
                        }
                    )
        else:
            logging.debug("System notification sent successfully")
