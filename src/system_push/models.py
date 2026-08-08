from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator


class PushSubscriptionKeys(BaseModel):
    p256dh: str
    auth: str


class PushSubscription(BaseModel):
    endpoint: str
    expiration_time: int | None = Field(
        default=None,
        validation_alias=AliasChoices("expiration_time", "expirationTime"),
        serialization_alias="expirationTime",
    )
    keys: PushSubscriptionKeys

    model_config = ConfigDict(populate_by_name=True)


class SystemPushSubscriptionDocument(BaseModel):
    subscription: PushSubscription
    topics: list[str] = Field(default_factory=list)
    username: str = "Anonymous User"
    client_id: str | None = None

    @model_validator(mode="before")
    @classmethod
    def normalise_legacy_shape(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        if "subscription" in data:
            return data

        if "endpoint" in data and "keys" in data:
            return {
                "subscription": {
                    "endpoint": data.get("endpoint"),
                    "expirationTime": data.get("expirationTime"),
                    "keys": data.get("keys", {}),
                },
                "topics": data.get("topics", []),
                "username": data.get("username", "Anonymous User"),
                "client_id": data.get("client_id"),
            }

        return data
