from datetime import UTC, datetime

from prefect import get_run_logger, task

from shared.db.mariadb import engine
from shared.db.mongodb import jira_tickets_collection
from shared.repositories import jira_mongo_repo, jira_ticket_repo


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
