from typing import Any

from pymongo import MongoClient
from pymongo.collection import Collection

from shared.config.database import mongodb

client: MongoClient | None = None
db = None
jira_tickets_collection: Collection | None = None

if mongodb.enabled:
    kwargs: dict[str, Any] = {"serverSelectionTimeoutMS": mongodb.server_selection_timeout_ms}
    if mongodb.replica_set:
        kwargs["replicaSet"] = mongodb.replica_set

    client = MongoClient(mongodb.url, **kwargs)
    db = client[mongodb.database]
    jira_tickets_collection = db["jira_tickets"]
