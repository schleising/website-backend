from database import BackendDatabase

WEB_DATABASE = "web_database"

mongo_db = BackendDatabase()
system_push_subscriptions = mongo_db.get_collection(
    "system_push_subscriptions", db_name=WEB_DATABASE
)

from .notifications import send_system_push

__all__ = [
    "send_system_push",
    "system_push_subscriptions",
]
