from __future__ import annotations

from database import BackendDatabase

DATABASE = BackendDatabase()
DATABASE.set_database("feeds_database")

FEED_SOURCES_COLLECTION = DATABASE.get_collection("feed_sources")
FEED_ARTICLES_COLLECTION = DATABASE.get_collection("feed_articles")
USER_FEED_SUBSCRIPTIONS_COLLECTION = DATABASE.get_collection("user_feed_subscriptions")
FEED_CATEGORIES_COLLECTION = DATABASE.get_collection("feed_categories")
USER_ARTICLE_STATES_COLLECTION = DATABASE.get_collection("user_article_states")

from .feeds_main import feeds_loop

__all__ = [
    "feeds_loop",
    "FEED_SOURCES_COLLECTION",
    "FEED_ARTICLES_COLLECTION",
    "USER_FEED_SUBSCRIPTIONS_COLLECTION",
    "FEED_CATEGORIES_COLLECTION",
    "USER_ARTICLE_STATES_COLLECTION",
]
