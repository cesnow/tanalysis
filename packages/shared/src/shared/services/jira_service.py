import asyncio

import aiohttp

from shared.config.settings import settings
from shared.db.mongodb import jira_tickets_collection
from shared.repositories import jira_mongo_repo


async def _fetch_jira_tickets_async(jql: str, max_results: int) -> list[dict]:
    auth = aiohttp.BasicAuth(settings.jira_email, settings.jira_api_token)

    async with aiohttp.ClientSession(auth=auth) as session:
        # 1. Fetch custom fields first
        field_url = f"{settings.jira_base_url}/rest/api/latest/field"
        async with session.get(field_url) as resp:
            resp.raise_for_status()
            fields_data = await resp.json()

        field_ids = [f["id"] for f in fields_data]
        fields_param = ",".join(field_ids)

        # 2. Pass fields to the search API
        search_url = f"{settings.jira_base_url}/rest/api/latest/search"
        params = {
            "jql": jql,
            "maxResults": max_results,
            "fields": fields_param,
        }
        async with session.get(search_url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

        return data.get("issues", [])


def fetch_jira_tickets(jql: str | None = None, max_results: int = 50) -> list[dict]:
    """Fetch tickets from Jira REST API and save raw JSON to MongoDB."""
    if not jql:
        jql = f"project = {settings.jira_project_key} ORDER BY updated DESC"

    issues = asyncio.run(_fetch_jira_tickets_async(jql, max_results))
    jira_mongo_repo.upsert_many(jira_tickets_collection, issues)
    return issues


def get_tickets_from_mongo(project_key: str | None = None, limit: int = 50) -> list[dict]:
    """Read cached tickets from MongoDB."""
    return jira_mongo_repo.find_all(jira_tickets_collection, project_key, limit)


async def _fetch_jira_fields_async() -> list[dict]:
    auth = aiohttp.BasicAuth(settings.jira_email, settings.jira_api_token)
    url = f"{settings.jira_base_url}/rest/api/latest/field"

    async with aiohttp.ClientSession(auth=auth) as session, session.get(url) as resp:
        resp.raise_for_status()
        return await resp.json()


def fetch_jira_fields() -> list[dict]:
    """Fetch custom fields from Jira Data Center REST API."""
    return asyncio.run(_fetch_jira_fields_async())
