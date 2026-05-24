from pymongo import MongoClient
from pymongo.collection import Collection

from app.config.database import mongodb

client: MongoClient = MongoClient(
    mongodb.url,
    serverSelectionTimeoutMS=mongodb.server_selection_timeout_ms,
)
db = client[mongodb.database]

jira_tickets_collection: Collection = db["jira_tickets"]
