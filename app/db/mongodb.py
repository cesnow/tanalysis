from motor.motor_asyncio import AsyncIOMotorClient

from app.config import settings

client = AsyncIOMotorClient(settings.mongodb_url)
db = client[settings.mongodb_database]

jira_tickets_collection = db["jira_tickets"]
