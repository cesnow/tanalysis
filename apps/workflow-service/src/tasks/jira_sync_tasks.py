import base64

import requests
from prefect import get_run_logger, task

from shared.config.settings import settings
from shared.core.constants import JIRA_ISSUE_TYPES
from shared.db.mongodb import jira_tickets_collection
from shared.repositories import jira_mongo_repo


def _jira_headers() -> dict:
    token = base64.b64encode(f"{settings.jira_email}:{settings.jira_api_token}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
    }


def _build_jql(base_jql: str) -> str:
    """Append issue type filter to the product's base JQL."""
    types_str = ", ".join(f'"{t}"' for t in JIRA_ISSUE_TYPES)
    return f"({base_jql}) AND issuetype in ({types_str}) ORDER BY updated DESC"


@task(name="fetch_jira_issues_by_jql", retries=2, retry_delay_seconds=30)
def fetch_jira_issues_by_jql(product_name: str, jql: str) -> list[dict]:
    """Call the Jira REST API and paginate through all matching issues using JQL."""
    logger = get_run_logger()
    url = f"{settings.jira_base_url}/rest/api/3/search"
    headers = _jira_headers()
    full_jql = _build_jql(jql)
    logger.info(f"[{product_name}] JQL: {full_jql}")

    all_issues: list[dict] = []
    start_at = 0
    max_results = 100

    while True:
        resp = requests.get(
            url,
            headers=headers,
            params={
                "jql": full_jql,
                "startAt": start_at,
                "maxResults": max_results,
                "fields": (
                    "summary,status,issuetype,priority,assignee,reporter,project,created,updated,resolutiondate,description,labels"
                ),
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        issues = data.get("issues", [])
        all_issues.extend(issues)
        total = data.get("total", 0)
        logger.info(f"[{product_name}] fetched {len(all_issues)}/{total} issues")

        if start_at + max_results >= total:
            break
        start_at += max_results

    return all_issues


@task(name="upsert_jira_issues_to_mongodb")
def upsert_jira_issues_to_mongodb(product_name: str, product_id: int, issues: list[dict]) -> int:
    """Upsert fetched Jira issues into the MongoDB jira_tickets collection."""
    logger = get_run_logger()
    count = jira_mongo_repo.upsert_many(jira_tickets_collection, issues, product_id, product_name)
    logger.info(f"[{product_name}] Upserted {count} issues into MongoDB")
    return count
