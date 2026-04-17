import logging
import re
from typing import Any

from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

from .database import BackendDatabase


SEASON_MATCH_PATTERN = re.compile(r"^pl_matches_\d{4}_\d{4}$")
SEASON_TABLE_PATTERN = re.compile(r"^pl_table_\d{4}_\d{4}$")


def _index_matches(
    index_meta: dict[str, Any],
    keys: list[tuple[str, int]],
    unique: bool,
) -> bool:
    existing_keys = index_meta.get("key")
    existing_unique = bool(index_meta.get("unique", False))

    return existing_keys == keys and existing_unique == unique


def _keys_match(index_meta: dict[str, Any], keys: list[tuple[str, int]]) -> bool:
    return index_meta.get("key") == keys


def _ensure_index(
    collection: Collection,
    keys: list[tuple[str, int]],
    *,
    unique: bool = False,
) -> None:
    # Reuse equivalent existing indexes regardless of name to avoid name conflicts.
    existing_indexes = collection.index_information()

    for index_meta in existing_indexes.values():
        if _index_matches(index_meta, keys, unique):
            return

    # If an index already exists on the same key pattern with different options,
    # treat startup ensure as satisfied and keep the process non-fatal.
    for index_name, index_meta in existing_indexes.items():
        if not _keys_match(index_meta, keys):
            continue

        existing_unique = bool(index_meta.get("unique", False))

        if unique and not existing_unique:
            logging.warning(
                f"Index {collection.full_name}.{index_name} matches keys {keys} but is not unique; requested unique index ensure skipped"
            )
        elif not unique and existing_unique:
            logging.info(
                f"Index {collection.full_name}.{index_name} matches keys {keys} and is unique; reusing for non-unique ensure"
            )

        return

    try:
        collection.create_index(keys, unique=unique)
    except OperationFailure as ex:
        # If an equivalent index already exists under another name, keep startup non-fatal.
        if ex.code in (85, 86):
            logging.warning(
                f"Skipping create_index for {collection.full_name} on {keys}: {ex}"
            )
            return

        raise


def ensure_backend_indexes() -> None:
    """Create/ensure required indexes for backend and website query paths.

    This function is intentionally additive-only and must be safe to run at startup.
    """
    database = BackendDatabase()

    # web_database indexes
    database.set_database("web_database")

    football_push = database.get_collection("football_push_subscriptions")
    if football_push is not None:
        _ensure_index(football_push, [("subscription.endpoint", ASCENDING)], unique=True)
        _ensure_index(football_push, [("team_ids", ASCENDING)])

    if database.current_db is not None:
        collection_names = database.current_db.list_collection_names()

        for collection_name in collection_names:
            if SEASON_MATCH_PATTERN.match(collection_name):
                season_matches = database.get_collection(collection_name)
                if season_matches is None:
                    continue

                _ensure_index(season_matches, [("id", ASCENDING)], unique=True)
                _ensure_index(season_matches, [("utc_date", ASCENDING)])
                _ensure_index(
                    season_matches,
                    [("home_team.id", ASCENDING), ("utc_date", ASCENDING)],
                )
                _ensure_index(
                    season_matches,
                    [("away_team.id", ASCENDING), ("utc_date", ASCENDING)],
                )

            if SEASON_TABLE_PATTERN.match(collection_name):
                season_table = database.get_collection(collection_name)
                if season_table is None:
                    continue

                _ensure_index(season_table, [("team.id", ASCENDING)], unique=True)
                _ensure_index(season_table, [("position", ASCENDING)])

    live_pl_table = database.get_collection("live_pl_table")
    if live_pl_table is not None:
        _ensure_index(live_pl_table, [("team.id", ASCENDING)], unique=True)
        _ensure_index(live_pl_table, [("position", ASCENDING)])

    team_primary_colours = database.get_collection("pl_team_primary_colours")
    if team_primary_colours is not None:
        _ensure_index(team_primary_colours, [("team_id", ASCENDING)], unique=True)

    sensors_collection = database.get_collection("sensors_collection")
    if sensors_collection is not None:
        _ensure_index(sensors_collection, [("device", ASCENDING)], unique=True)

    sensor_data = database.get_collection("sensor_data")
    if sensor_data is not None:
        _ensure_index(sensor_data, [("device_name", ASCENDING), ("timestamp", DESCENDING)])
        _ensure_index(sensor_data, [("device_name", ASCENDING), ("timestamp", ASCENDING)])

    # feeds_database indexes
    database.set_database("feeds_database")

    feed_sources = database.get_collection("feed_sources")
    if feed_sources is not None:
        _ensure_index(feed_sources, [("normalized_url", ASCENDING)], unique=True)
        _ensure_index(feed_sources, [("next_retry_at", ASCENDING)])

    feed_articles = database.get_collection("feed_articles")
    if feed_articles is not None:
        _ensure_index(
            feed_articles,
            [("feed_id", ASCENDING), ("dedupe_key", ASCENDING)],
            unique=True,
        )
        _ensure_index(feed_articles, [("feed_id", ASCENDING), ("published_at", ASCENDING)])
        _ensure_index(feed_articles, [("is_deleted", ASCENDING), ("deleted_at", ASCENDING)])
        _ensure_index(feed_articles, [("_id", ASCENDING), ("feed_id", ASCENDING)])

    user_feed_subscriptions = database.get_collection("user_feed_subscriptions")
    if user_feed_subscriptions is not None:
        _ensure_index(
            user_feed_subscriptions,
            [("user_id", ASCENDING), ("feed_id", ASCENDING)],
            unique=True,
        )
        _ensure_index(
            user_feed_subscriptions,
            [("user_id", ASCENDING), ("category_id", ASCENDING)],
        )
        _ensure_index(user_feed_subscriptions, [("feed_id", ASCENDING)])

    feed_categories = database.get_collection("feed_categories")
    if feed_categories is not None:
        _ensure_index(
            feed_categories,
            [("user_id", ASCENDING), ("name", ASCENDING)],
            unique=True,
        )
        _ensure_index(
            feed_categories,
            [("user_id", ASCENDING), ("muted", ASCENDING), ("sort_order", ASCENDING)],
        )
        _ensure_index(feed_categories, [("user_id", ASCENDING), ("color_hex", ASCENDING)])

    user_article_states = database.get_collection("user_article_states")
    if user_article_states is not None:
        _ensure_index(
            user_article_states,
            [("user_id", ASCENDING), ("article_id", ASCENDING)],
            unique=True,
        )
        _ensure_index(
            user_article_states,
            [("user_id", ASCENDING), ("is_read", ASCENDING), ("read_at", DESCENDING)],
        )
        _ensure_index(user_article_states, [("article_id", ASCENDING), ("is_read", ASCENDING)])

    # media indexes
    database.set_database("media")

    media_collection = database.get_collection("media_collection")
    if media_collection is not None:
        _ensure_index(
            media_collection,
            [
                ("conversion_required", ASCENDING),
                ("converted", ASCENDING),
                ("conversion_error", ASCENDING),
                ("deleted", ASCENDING),
                ("end_conversion_time", DESCENDING),
            ],
        )
        _ensure_index(
            media_collection,
            [
                ("conversion_required", ASCENDING),
                ("converted", ASCENDING),
                ("converting", ASCENDING),
                ("conversion_error", ASCENDING),
                ("deleted", ASCENDING),
                ("video_information.format.bit_rate", DESCENDING),
            ],
        )
        _ensure_index(media_collection, [("converting", ASCENDING), ("deleted", ASCENDING)])
        _ensure_index(media_collection, [("copying", ASCENDING), ("deleted", ASCENDING)])
        _ensure_index(media_collection, [("conversion_error", ASCENDING), ("deleted", ASCENDING)])
        _ensure_index(media_collection, [("converted", ASCENDING), ("deleted", ASCENDING)])
        _ensure_index(
            media_collection,
            [("backend_name", ASCENDING), ("converted", ASCENDING), ("deleted", ASCENDING)],
        )

    logging.info("Index ensure completed")


def run_manual_legacy_index_cleanup() -> None:
    """Drop known legacy/redundant indexes.

    This is intentionally NOT called by startup. Run manually when required.
    """
    database = BackendDatabase()

    def _drop_known_indexes(db_name: str, collection_name: str, index_names: list[str]) -> None:
        database.set_database(db_name)
        collection = database.get_collection(collection_name)

        if collection is None:
            return

        existing_names = set(collection.index_information().keys())

        for index_name in index_names:
            if index_name not in existing_names:
                continue

            try:
                collection.drop_index(index_name)
                logging.info(
                    f"Dropped legacy index {index_name} on {db_name}.{collection_name}"
                )
            except Exception as ex:
                logging.error(
                    f"Failed to drop index {index_name} on {db_name}.{collection_name}: {ex}"
                )

    _drop_known_indexes(
        "web_database",
        "football_push_subscriptions",
        [
            "subscription.endpoint_1",
            "endpoint_1",
        ],
    )

    _drop_known_indexes(
        "media",
        "media_collection",
        [
            "converting_1",
            "copying_1",
            "converted_1",
            "conversion_error_1",
            "deleted_1",
            "backend_name_1",
        ],
    )

    logging.info("Manual legacy index cleanup completed")
