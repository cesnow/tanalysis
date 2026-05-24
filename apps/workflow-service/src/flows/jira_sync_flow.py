"""Prefect flow: Fetch all Jira issues via JQL for each enabled Product and upsert them into MongoDB.

Schedule: runs once per hour (configured via Prefect deployment with interval=3600).
After completion, automatically triggers jira_clean_flow to clean and write data into MariaDB.
"""

from prefect import flow, get_run_logger
from tasks.jira_sync_tasks import fetch_jira_issues_by_jql, upsert_jira_issues_to_mongodb

from flows.jira_clean_flow import jira_product_clean_flow
from shared.db.mariadb import SessionLocal
from shared.repositories import product_repo

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
