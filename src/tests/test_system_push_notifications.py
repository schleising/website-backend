from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, mock_open, patch

BACKEND_SRC = Path(__file__).resolve().parents[1]
if str(BACKEND_SRC) not in sys.path:
    sys.path.insert(0, str(BACKEND_SRC))


def _load_system_push_notifications():
    for module_name in list(sys.modules):
        if module_name == "system_push" or module_name.startswith("system_push."):
            del sys.modules[module_name]

    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_db.get_collection.return_value = mock_collection

    with patch("database.BackendDatabase", return_value=mock_db):
        import system_push.notifications as notifications_module

    return notifications_module, mock_collection


class SystemPushNotificationsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.notifications, self.collection = _load_system_push_notifications()

    def test_send_system_push_noops_when_collection_missing(self) -> None:
        with (
            patch.object(self.notifications, "system_push_subscriptions", None),
            patch.object(self.notifications, "webpush") as webpush_mock,
        ):
            self.notifications.send_system_push("dyn_dns_success", "Title", "Body")

        webpush_mock.assert_not_called()

    def test_send_system_push_filters_by_topic_and_dedupes_endpoints(self) -> None:
        self.collection.find.return_value = [
            {
                "subscription": {
                    "endpoint": "https://push.example/a",
                    "keys": {"p256dh": "p1", "auth": "a1"},
                },
                "topics": ["dyn_dns_success"],
                "username": "steve",
            },
            {
                "subscription": {
                    "endpoint": "https://push.example/a",
                    "keys": {"p256dh": "p1", "auth": "a1"},
                },
                "topics": ["dyn_dns_success"],
                "username": "steve",
            },
            {
                "subscription": {
                    "endpoint": "https://push.example/b",
                    "keys": {"p256dh": "p2", "auth": "a2"},
                },
                "topics": ["dyn_dns_success"],
                "username": "steve",
            },
        ]

        with (
            patch(
                "builtins.open",
                mock_open(read_data=json.dumps({"sub": "mailto:test@example.com"})),
            ),
            patch.object(self.notifications, "webpush") as webpush_mock,
        ):
            self.notifications.send_system_push(
                "dyn_dns_success",
                "DNS Update Succeeded",
                "IP changed",
            )

        self.collection.find.assert_called_once_with({"topics": "dyn_dns_success"})
        self.assertEqual(webpush_mock.call_count, 2)

    def test_send_system_push_removes_gone_subscriptions(self) -> None:
        from pywebpush import WebPushException

        self.collection.find.return_value = [
            {
                "subscription": {
                    "endpoint": "https://push.example/gone",
                    "keys": {"p256dh": "p1", "auth": "a1"},
                },
                "topics": ["dyn_dns_failure"],
                "username": "steve",
            }
        ]

        response = SimpleNamespace(status_code=410, reason="Gone", text="gone")
        exception = WebPushException("gone", response=response)

        with (
            patch(
                "builtins.open",
                mock_open(read_data=json.dumps({"sub": "mailto:test@example.com"})),
            ),
            patch.object(self.notifications, "webpush", side_effect=exception),
        ):
            self.notifications.send_system_push(
                "dyn_dns_failure",
                "DNS Update Failed",
                "Update failed",
            )

        self.collection.delete_one.assert_called_once()


if __name__ == "__main__":
    unittest.main()
