from pymongo import MongoClient
from pymongo.collection import Collection

from shared.config.database import mongodb

client: MongoClient | None = None
db = None
jira_tickets_collection: Collection | None = None

if mongodb.enabled:
    client = MongoClient(
        mongodb.url,
        serverSelectionTimeoutMS=mongodb.server_selection_timeout_ms,
    )
    db = client[mongodb.database]
    jira_tickets_collection = db["jira_tickets"]
