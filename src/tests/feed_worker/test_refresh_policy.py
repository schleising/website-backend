from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from feed_refresh_policy import resolve_source_refresh_interval, source_needs_fetch


class SourceRefreshPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.fetch_interval = timedelta(minutes=5)

    def test_source_needs_fetch_when_never_fetched(self) -> None:
        self.assertTrue(source_needs_fetch({}, self.now, self.fetch_interval))

    def test_source_uses_persisted_refresh_interval_when_available(self) -> None:
        source_doc = {
            "refresh_interval_seconds": 1800,
        }

        self.assertEqual(
            resolve_source_refresh_interval(source_doc, self.fetch_interval),
            timedelta(minutes=30),
        )

    def test_source_caps_persisted_refresh_interval_to_thirty_minutes(self) -> None:
        source_doc = {
            "refresh_interval_seconds": 7200,
        }

        self.assertEqual(
            resolve_source_refresh_interval(source_doc, self.fetch_interval),
            timedelta(minutes=30),
        )

    def test_source_caps_default_refresh_interval_to_thirty_minutes(self) -> None:
        self.assertEqual(
            resolve_source_refresh_interval({}, timedelta(hours=2)),
            timedelta(minutes=30),
        )

    def test_source_does_not_fetch_before_interval_without_force(self) -> None:
        source_doc = {
            "last_fetched_at": self.now - self.fetch_interval + timedelta(seconds=45),
        }

        self.assertFalse(source_needs_fetch(source_doc, self.now, self.fetch_interval))

    def test_source_fetches_after_interval_elapsed(self) -> None:
        source_doc = {
            "last_fetched_at": self.now - self.fetch_interval - timedelta(seconds=1),
        }

        self.assertTrue(source_needs_fetch(source_doc, self.now, self.fetch_interval))

    def test_source_respects_next_refresh_when_present(self) -> None:
        source_doc = {
            "last_fetched_at": self.now - self.fetch_interval,
            "next_refresh_at": self.now + timedelta(seconds=10),
        }

        self.assertFalse(source_needs_fetch(source_doc, self.now, self.fetch_interval))

    def test_source_fetches_when_next_refresh_has_elapsed(self) -> None:
        source_doc = {
            "last_fetched_at": self.now - self.fetch_interval,
            "next_refresh_at": self.now - timedelta(seconds=1),
        }

        self.assertTrue(source_needs_fetch(source_doc, self.now, self.fetch_interval))

    def test_source_fetches_when_next_refresh_is_later_than_max_allowed_lag(self) -> None:
        source_doc = {
            "last_fetched_at": self.now - self.fetch_interval - timedelta(minutes=2, seconds=1),
            "next_refresh_at": self.now + timedelta(minutes=5),
        }

        self.assertTrue(source_needs_fetch(source_doc, self.now, self.fetch_interval))

    def test_force_refresh_overrides_recent_fetch_and_retry_window(self) -> None:
        source_doc = {
            "last_fetched_at": self.now - timedelta(seconds=30),
            "next_retry_at": self.now + timedelta(minutes=10),
            "force_refresh_requested_at": self.now,
        }

        self.assertTrue(source_needs_fetch(source_doc, self.now, self.fetch_interval))

    def test_stale_force_refresh_marker_does_not_trigger_fetch(self) -> None:
        source_doc = {
            "last_fetched_at": self.now,
            "force_refresh_requested_at": self.now - timedelta(seconds=1),
        }

        self.assertFalse(source_needs_fetch(source_doc, self.now, self.fetch_interval))

    def test_future_retry_window_blocks_non_forced_fetch(self) -> None:
        source_doc = {
            "last_fetched_at": self.now - self.fetch_interval - timedelta(minutes=1),
            "next_retry_at": self.now + timedelta(minutes=2),
        }

        self.assertFalse(source_needs_fetch(source_doc, self.now, self.fetch_interval))


if __name__ == "__main__":
    unittest.main()
