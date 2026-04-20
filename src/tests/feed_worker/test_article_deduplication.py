from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast
import unittest

from bson import ObjectId

import feeds.feeds as feeds_module
from feeds.feeds import Feeds, normalize_article_identity_url
from task_scheduler import TaskScheduler


SOURCE_URL = "https://example.com/feed.xml"


class _NoopScheduler:
    def schedule_task(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class _FakeUpdateResult:
    def __init__(self, upserted_id: ObjectId | None) -> None:
        self.upserted_id = upserted_id


class _FakeFeedArticlesCollection:
    def __init__(self) -> None:
        self.docs: list[dict[str, Any]] = []

    def _matches(self, doc: dict[str, Any], query: dict[str, Any]) -> bool:
        for key, value in query.items():
            if key == "$or":
                if not isinstance(value, list):
                    return False
                if not any(self._matches(doc, cast(dict[str, Any], subquery)) for subquery in value):
                    return False
                continue

            if doc.get(key) != value:
                return False

        return True

    def find_one(
        self,
        query: dict[str, Any],
        projection: dict[str, int] | None = None,
    ) -> dict[str, Any] | None:
        for doc in self.docs:
            if not self._matches(doc, query):
                continue

            if projection is None:
                return dict(doc)

            projected: dict[str, Any] = {}
            for field, include in projection.items():
                if include and field in doc:
                    projected[field] = doc[field]

            return projected

        return None

    def update_one(
        self,
        query: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
    ) -> _FakeUpdateResult:
        for index, doc in enumerate(self.docs):
            if not self._matches(doc, query):
                continue

            updated = dict(doc)
            updated.update(cast(dict[str, Any], update.get("$set", {})))
            self.docs[index] = updated
            return _FakeUpdateResult(upserted_id=None)

        if not upsert:
            return _FakeUpdateResult(upserted_id=None)

        inserted: dict[str, Any] = {}
        inserted.update(cast(dict[str, Any], update.get("$setOnInsert", {})))
        inserted.update(cast(dict[str, Any], update.get("$set", {})))
        inserted["_id"] = ObjectId()
        self.docs.append(inserted)
        return _FakeUpdateResult(upserted_id=cast(ObjectId, inserted["_id"]))


class FeedArticleDeduplicationTests(unittest.TestCase):
    """Verify article identity prefers URL/external ID over mutable text fields."""

    def setUp(self) -> None:
        self.original_articles_collection = feeds_module.FEED_ARTICLES_COLLECTION
        self.fake_articles_collection = _FakeFeedArticlesCollection()
        feeds_module.FEED_ARTICLES_COLLECTION = self.fake_articles_collection

        self.worker = Feeds(cast(TaskScheduler, _NoopScheduler()))
        self.feed_id = ObjectId()

    def tearDown(self) -> None:
        feeds_module.FEED_ARTICLES_COLLECTION = self.original_articles_collection

    def test_same_url_with_title_correction_updates_single_article(self) -> None:
        original_entry = {
            "link": "https://www.bbc.com/sport/formula1/articles/c75kg9vrd3xo?at_medium=RSS&at_campaign=rss",
            "title": "Title with typo",
        }
        corrected_entry = {
            "link": "https://www.bbc.com/sport/formula1/articles/c75kg9vrd3xo?at_medium=RSS&at_campaign=rss",
            "title": "Title corrected",
        }

        parsed_original = self.worker._parse_feed_entry(SOURCE_URL, original_entry)
        parsed_corrected = self.worker._parse_feed_entry(SOURCE_URL, corrected_entry)

        self.assertIsNotNone(parsed_original)
        self.assertIsNotNone(parsed_corrected)
        assert parsed_original is not None
        assert parsed_corrected is not None

        self.assertEqual(parsed_original.canonical_url, parsed_corrected.canonical_url)
        self.assertEqual(parsed_original.dedupe_key, parsed_corrected.dedupe_key)

        self.worker._upsert_article(self.feed_id, parsed_original)
        self.worker._upsert_article(self.feed_id, parsed_corrected)

        self.assertEqual(len(self.fake_articles_collection.docs), 1)
        self.assertEqual(self.fake_articles_collection.docs[0].get("title"), "Title corrected")

    def test_upsert_revives_soft_deleted_article_when_same_url_reappears(self) -> None:
        entry = {
            "link": "https://www.bbc.com/sport/formula1/articles/c75kg9vrd3xo?at_medium=RSS&at_campaign=rss",
            "title": "Original",
        }
        parsed = self.worker._parse_feed_entry(SOURCE_URL, entry)

        self.assertIsNotNone(parsed)
        assert parsed is not None

        self.worker._upsert_article(self.feed_id, parsed)
        self.assertEqual(len(self.fake_articles_collection.docs), 1)

        self.fake_articles_collection.docs[0]["is_deleted"] = True
        self.fake_articles_collection.docs[0]["deleted_at"] = datetime.now(timezone.utc)

        corrected = self.worker._parse_feed_entry(
            SOURCE_URL,
            {
                "link": entry["link"],
                "title": "Corrected",
            },
        )
        self.assertIsNotNone(corrected)
        assert corrected is not None

        self.worker._upsert_article(self.feed_id, corrected)

        self.assertFalse(bool(self.fake_articles_collection.docs[0].get("is_deleted")))
        self.assertIsNone(self.fake_articles_collection.docs[0].get("deleted_at"))
        self.assertEqual(self.fake_articles_collection.docs[0].get("title"), "Corrected")

    def test_external_id_fallback_updates_single_article_when_link_missing(self) -> None:
        first = {
            "id": "urn:uuid:feeds-example-article-1",
            "title": "First title",
        }
        second = {
            "id": "urn:uuid:feeds-example-article-1",
            "title": "Revised title",
        }

        parsed_first = self.worker._parse_feed_entry(SOURCE_URL, first)
        parsed_second = self.worker._parse_feed_entry(SOURCE_URL, second)

        self.assertIsNotNone(parsed_first)
        self.assertIsNotNone(parsed_second)
        assert parsed_first is not None
        assert parsed_second is not None

        self.assertIsNone(parsed_first.canonical_url)
        self.assertEqual(parsed_first.external_id, "urn:uuid:feeds-example-article-1")
        self.assertEqual(parsed_first.dedupe_key, parsed_second.dedupe_key)

        self.worker._upsert_article(self.feed_id, parsed_first)
        self.worker._upsert_article(self.feed_id, parsed_second)

        self.assertEqual(len(self.fake_articles_collection.docs), 1)
        self.assertEqual(self.fake_articles_collection.docs[0].get("title"), "Revised title")

    def test_normalize_article_identity_url_preserves_query_and_drops_fragment(self) -> None:
        normalized = normalize_article_identity_url(
            "HTTPS://WWW.BBC.COM/sport/formula1/articles/c75kg9vrd3xo?at_medium=RSS&at_campaign=rss#fragment",
            SOURCE_URL,
        )

        self.assertEqual(
            normalized,
            "https://www.bbc.com/sport/formula1/articles/c75kg9vrd3xo?at_medium=RSS&at_campaign=rss",
        )


if __name__ == "__main__":
    unittest.main()
