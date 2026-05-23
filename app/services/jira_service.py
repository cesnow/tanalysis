from datetime import datetime

import httpx

from app.core.config import settings
from app.db.mongodb import jira_tickets_collection


async def fetch_jira_tickets(jql: str | None = None, max_results: int = 50) -> list[dict]:
    """Fetch tickets from Jira REST API and save raw JSON to MongoDB."""
    if not jql:
        jql = f"project = {settings.jira_project_key} ORDER BY updated DESC"

    url = f"{settings.jira_base_url}/rest/api/3/search"
    params = {
        "jql": jql,
        "maxResults": max_results,
        "fields": "summary,status,issuetype,priority,assignee,reporter,project,created,updated,resolutiondate,description,labels",
    }
    auth = (settings.jira_email, settings.jira_api_token)

    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, auth=auth, timeout=30)
        response.raise_for_status()
        data = response.json()

    issues = data.get("issues", [])

    for issue in issues:
        issue["_fetched_at"] = datetime.utcnow().isoformat()
        await jira_tickets_collection.update_one(
            {"key": issue["key"]},
            {"$set": issue},
            upsert=True,
        )

    return issues


async def get_tickets_from_mongo(project_key: str | None = None, limit: int = 50) -> list[dict]:
    """Read cached tickets from MongoDB."""
    query = {}
    if project_key:
        query["fields.project.key"] = project_key

    cursor = jira_tickets_collection.find(query, {"_id": 0}).sort("_fetched_at", -1).limit(limit)
    return await cursor.to_list(length=limit)
