"""Prefect flow: Read Jira raw data from MongoDB, clean it, and upsert into MariaDB.

This flow is automatically triggered by jira_sync_flow after each product sync completes.
It can also be triggered manually via an API endpoint.
"""

from prefect import flow, get_run_logger
from tasks.jira_clean_tasks import clean_and_load_jira_to_mariadb, extract_jira_from_mongodb

from shared.db.mariadb import SessionLocal
from shared.repositories import product_repo

# ---------- per-product sub-flow ----------


@flow(name="jira_product_clean_flow", log_prints=True)
def jira_product_clean_flow(product_id: int, product_name: str) -> dict:
    """Run MongoDB → MariaDB data cleaning for a single product."""
    docs = extract_jira_from_mongodb(product_id, product_name)
    count = clean_and_load_jira_to_mariadb(product_name, docs)
    return {"product": product_name, "product_id": product_id, "cleaned": count}


# ---------- main flow (can be triggered manually to clean all) ----------


@flow(name="jira_clean_flow", log_prints=True)
def jira_clean_flow() -> list[dict]:
    """Run MongoDB → MariaDB data cleaning for all enabled products.
    Normally triggered automatically by jira_sync_flow, but can also be called manually.
    """
    logger = get_run_logger()

    with SessionLocal() as db:
        products = product_repo.list_enabled(db)
        product_list = [{"id": p.id, "name": p.name} for p in products]

    if not product_list:
        logger.info("No enabled products found, skipping.")
        return []

    results = []
    for p in product_list:
        result = jira_product_clean_flow(
            product_id=p["id"],
            product_name=p["name"],
        )
        results.append(result)

    return results


if __name__ == "__main__":
    jira_clean_flow()
