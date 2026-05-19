"""Prefect flow: Extract Jira tickets from MongoDB, transform, and load into MariaDB."""
from datetime import datetime
import pandas as pd
from prefect import flow, task, get_run_logger
from pymongo import MongoClient
from sqlalchemy.dialects.mysql import insert as mysql_upsert
from app.config import settings
from app.db.mariadb import engine
from app.models.jira_ticket import JiraTicket, Base


@task(name="extract_from_mongodb")
def extract_from_mongodb(limit: int = 500) -> list[dict]:
    """Read raw Jira tickets from MongoDB that are newer than the latest synced_at in MariaDB."""
    logger = get_run_logger()

    # Determine the latest synced_at already in MariaDB to do incremental extraction
    last_synced = None
    try:
        with engine.connect() as conn:
            result = conn.execute(
                JiraTicket.__table__.select().with_only_columns(
                    JiraTicket.__table__.c.synced_at
                ).order_by(JiraTicket.__table__.c.synced_at.desc()).limit(1)
            ).fetchone()
            if result:
                last_synced = result[0]
    except Exception:
        pass  # table may not exist yet

    client = MongoClient(settings.mongodb_url)
    db = client[settings.mongodb_database]
    collection = db["jira_tickets"]

    query = {}
    if last_synced:
        query["_fetched_at"] = {"$gt": last_synced.isoformat()}
        logger.info(f"Incremental extract: fetching tickets newer than {last_synced}")
    else:
        logger.info("Full extract: no previous sync found")

    docs = list(collection.find(query, {"_id": 0}).sort("_fetched_at", -1).limit(limit))
    logger.info(f"Extracted {len(docs)} tickets from MongoDB")
    client.close()
    return docs


def _safe_get(fields: dict, *keys, default=None):
    """Safely traverse nested dicts."""
    val = fields
    for k in keys:
        if isinstance(val, dict):
            val = val.get(k)
        else:
            return default
    return val if val is not None else default


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


@task(name="transform_tickets")
def transform_tickets(raw_tickets: list[dict]) -> pd.DataFrame:
    """Transform raw Jira JSON into a pandas DataFrame for MariaDB."""
    logger = get_run_logger()

    rows = []
    for ticket in raw_tickets:
        fields = ticket.get("fields", {})
        rows.append(
            {
                "ticket_key": ticket.get("key"),
                "summary": _safe_get(fields, "summary"),
                "status": _safe_get(fields, "status", "name"),
                "issue_type": _safe_get(fields, "issuetype", "name"),
                "priority": _safe_get(fields, "priority", "name"),
                "assignee": _safe_get(fields, "assignee", "displayName"),
                "reporter": _safe_get(fields, "reporter", "displayName"),
                "project_key": _safe_get(fields, "project", "key"),
                "created_at": _parse_dt(fields.get("created")),
                "updated_at": _parse_dt(fields.get("updated")),
                "resolved_at": _parse_dt(fields.get("resolutiondate")),
                "description": str(_safe_get(fields, "description") or ""),
                "labels": ",".join(fields.get("labels", [])),
                "synced_at": datetime.utcnow(),
            }
        )

    df = pd.DataFrame(rows)

    if df.empty:
        logger.info("No tickets to transform")
        return df

    # Drop rows missing the primary key
    df = df.dropna(subset=["ticket_key"])

    # Deduplicate by ticket_key, keeping the latest entry
    df = df.drop_duplicates(subset=["ticket_key"], keep="first")

    # Ensure datetime columns are proper dtype
    for col in ("created_at", "updated_at", "resolved_at", "synced_at"):
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    # Strip timezone info so MariaDB DATETIME columns accept the values
    for col in ("created_at", "updated_at", "resolved_at", "synced_at"):
        df[col] = df[col].dt.tz_localize(None)

    # Fill string NaN values with empty string
    str_cols = ["summary", "status", "issue_type", "priority", "assignee",
                "reporter", "project_key", "description", "labels"]
    df[str_cols] = df[str_cols].fillna("")

    logger.info(f"Transformed {len(df)} tickets into DataFrame")
    return df


@task(name="load_to_mariadb")
def load_to_mariadb(df: pd.DataFrame) -> int:
    """Bulk upsert transformed tickets DataFrame into MariaDB."""
    logger = get_run_logger()

    if df.empty:
        logger.info("No rows to load")
        return 0

    Base.metadata.create_all(engine)

    rows = df.to_dict(orient="records")
    # Bulk upsert: single statement with all rows instead of row-by-row
    with engine.begin() as conn:
        stmt = mysql_upsert(JiraTicket.__table__).values(rows)
        update_cols = {c.name: c for c in stmt.inserted if c.name != "ticket_key"}
        stmt = stmt.on_duplicate_key_update(**update_cols)
        conn.execute(stmt)

    logger.info(f"Loaded {len(rows)} tickets into MariaDB")
    return len(rows)


@flow(name="jira_etl_flow", log_prints=True)
def jira_etl_flow(limit: int = 500) -> dict:
    """ETL: MongoDB (raw Jira JSON) → transform → MariaDB."""
    raw = extract_from_mongodb(limit=limit)
    df = transform_tickets(raw)
    count = load_to_mariadb(df)
    return {"extracted": len(raw), "loaded": count}


if __name__ == "__main__":
    jira_etl_flow()
