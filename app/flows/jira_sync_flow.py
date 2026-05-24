"""Prefect flow: Fetch all Jira issues via JQL for each enabled Product and upsert them into MongoDB.

Schedule: runs once per hour (configured via Prefect deployment with interval=3600).
After completion, automatically triggers jira_clean_flow to clean and write data into MariaDB.
"""

import base64

import requests
from prefect import flow, get_run_logger, task

from app.config.settings import settings
from app.core.constants import JIRA_ISSUE_TYPES
from app.db.base import Base
from app.db.database import SessionLocal, engine
from app.db.mongodb import jira_tickets_collection
from app.flows.jira_clean_flow import jira_product_clean_flow
from app.repositories import jira_mongo_repo, product_repo

# ---------- helpers ----------


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


# ---------- tasks ----------


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
                    "summary,status,issuetype,priority,assignee,reporter,"
                    "project,created,updated,resolutiondate,description,labels"
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


# ---------- per-product sub-flow ----------


@flow(name="jira_product_sync_flow", log_prints=True)
def jira_product_sync_flow(product_id: int, product_name: str, jql: str) -> dict:
    """Fetch Jira issues and write them into MongoDB for a single product."""
    issues = fetch_jira_issues_by_jql(product_name, jql)
    count = upsert_jira_issues_to_mongodb(product_name, product_id, issues)
    return {"product": product_name, "product_id": product_id, "fetched": len(issues), "upserted": count}


# ---------- main flow (hourly schedule) ----------


@flow(name="jira_sync_flow", log_prints=True)
def jira_sync_flow() -> list[dict]:
    """Main flow: load all enabled products and fetch issues from Jira API via JQL, upserting into MongoDB.
    After each product completes, automatically trigger jira_clean_flow to clean and write data into MariaDB.

    Recommended Prefect deployment setting:
        interval = 3600  # every hour
    """
    logger = get_run_logger()

    Base.metadata.create_all(engine)
    with SessionLocal() as db:
        products = product_repo.list_enabled(db)
        product_list = [{"id": p.id, "name": p.name, "jql": p.jql} for p in products]

    if not product_list:
        logger.info("No enabled products found, skipping.")
        return []

    logger.info(f"Found {len(product_list)} enabled product(s): {[p['name'] for p in product_list]}")

    results = []
    for p in product_list:
        # Step 1: sync Jira → MongoDB
        sync_result = jira_product_sync_flow(
            product_id=p["id"],
            product_name=p["name"],
            jql=p["jql"],
        )
        # Step 2: after sync, trigger clean MongoDB → MariaDB
        clean_result = jira_product_clean_flow(
            product_id=p["id"],
            product_name=p["name"],
        )
        results.append({**sync_result, "cleaned": clean_result.get("cleaned", 0)})

    return results


if __name__ == "__main__":
    jira_sync_flow()
