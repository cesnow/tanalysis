"""Prefect flow: Read Jira raw data from MongoDB, clean it, and upsert into MariaDB.

This flow is automatically triggered by jira_sync_flow after each product sync completes.
It can also be triggered manually via an API endpoint.
"""

from datetime import UTC, datetime

from prefect import flow, get_run_logger, task

from app.db.mariadb import SessionLocal, engine
from app.db.mongodb import jira_tickets_collection
from app.repositories import jira_mongo_repo, jira_ticket_repo, product_repo

# ---------- helpers ----------


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except (ValueError, AttributeError):
        return None


def _safe(fields: dict, *keys, default=None):
    val = fields
    for k in keys:
        if isinstance(val, dict):
            val = val.get(k)
        else:
            return default
    return val if val is not None else default


# ---------- tasks ----------


@task(name="extract_jira_from_mongodb")
def extract_jira_from_mongodb(product_id: int, product_name: str) -> list[dict]:
    """Read raw issues for the specified product from the MongoDB jira_tickets collection."""
    logger = get_run_logger()
    docs = jira_mongo_repo.find_by_product(jira_tickets_collection, product_id)
    logger.info(f"[{product_name}] Extracted {len(docs)} issues from MongoDB")
    return docs


@task(name="clean_and_load_jira_to_mariadb")
def clean_and_load_jira_to_mariadb(product_name: str, docs: list[dict]) -> int:
    """Clean MongoDB raw issues and upsert them into the MariaDB jira_tickets table."""
    logger = get_run_logger()

    if not docs:
        logger.info(f"[{product_name}] No issues to clean and load")
        return 0

    rows = []
    for issue in docs:
        fields = issue.get("fields", {})
        rows.append(
            {
                "ticket_key": issue.get("key"),
                "summary": _safe(fields, "summary") or "",
                "status": _safe(fields, "status", "name") or "",
                "issue_type": _safe(fields, "issuetype", "name") or "",
                "priority": _safe(fields, "priority", "name") or "",
                "assignee": _safe(fields, "assignee", "displayName") or "",
                "reporter": _safe(fields, "reporter", "displayName") or "",
                "project_key": _safe(fields, "project", "key") or "",
                "created_at": _parse_dt(fields.get("created")),
                "updated_at": _parse_dt(fields.get("updated")),
                "resolved_at": _parse_dt(fields.get("resolutiondate")),
                "description": str(_safe(fields, "description") or ""),
                "labels": ",".join(fields.get("labels", [])),
                "synced_at": datetime.now(UTC).replace(tzinfo=None),
            }
        )

    rows = [r for r in rows if r.get("ticket_key")]

    if not rows:
        logger.info(f"[{product_name}] No valid rows after cleaning")
        return 0

    count = jira_ticket_repo.upsert_many(engine, rows)
    logger.info(f"[{product_name}] Cleaned and loaded {count} issues into MariaDB")
    return count


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
