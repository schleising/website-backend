from __future__ import annotations

from datetime import datetime, timezone

from bson import ObjectId
from pydantic import BaseModel, ConfigDict, Field


class MongoDocumentModel(BaseModel):
    """Base model for MongoDB documents that include ObjectId fields."""

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)


class FeedSourceDocument(MongoDocumentModel):
    """Represents a deduplicated feed source tracked by the backend worker."""

    id: ObjectId | None = Field(default=None, alias="_id")
    normalized_url: str
    title: str = ""
    image_url: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    last_fetched_at: datetime | None = None
    fetch_status: str = "new"
    last_error: str | None = None
    next_retry_at: datetime | None = None
    force_refresh_requested_at: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class FeedArticleDocument(MongoDocumentModel):
    """Represents one normalized article entry from a feed source."""

    id: ObjectId | None = Field(default=None, alias="_id")
    feed_id: ObjectId
    dedupe_key: str
    title: str
    link: str
    author: str | None = None
    summary_html: str | None = None
    media_image_url: str | None = None
    published_at: datetime | None = None
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    is_deleted: bool = False
    deleted_at: datetime | None = None
