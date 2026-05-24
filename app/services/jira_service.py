import httpx

from app.config.settings import settings
from app.db.mongodb import jira_tickets_collection
from app.repositories import jira_mongo_repo


def fetch_jira_tickets(jql: str | None = None, max_results: int = 50) -> list[dict]:
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

    with httpx.Client() as client:
        response = client.get(url, params=params, auth=auth, timeout=30)
        response.raise_for_status()
        data = response.json()

    issues = data.get("issues", [])
    jira_mongo_repo.upsert_many(jira_tickets_collection, issues)
    return issues


def get_tickets_from_mongo(project_key: str | None = None, limit: int = 50) -> list[dict]:
    """Read cached tickets from MongoDB."""
    return jira_mongo_repo.find_all(jira_tickets_collection, project_key, limit)
