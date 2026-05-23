from motor.motor_asyncio import AsyncIOMotorClient

from app.core.config import settings

client = AsyncIOMotorClient(settings.mongodb_url)
db = client[settings.mongodb_database]

jira_tickets_collection = db["jira_tickets"]
