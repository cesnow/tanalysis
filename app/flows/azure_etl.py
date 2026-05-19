"""Prefect flow: Extract Azure DevOps work items from MongoDB, transform, and load into MariaDB."""
from datetime import datetime
import pandas as pd
from prefect import flow, task, get_run_logger
from pymongo import MongoClient
from sqlalchemy.dialects.mysql import insert as mysql_upsert
from app.config import settings
from app.db.mariadb import engine, Base
from app.models.azure_ticket import AzureTicket


@task(name="extract_azure_from_mongodb")
def extract_azure_from_mongodb(limit: int = 500) -> list[dict]:
    """Read raw Azure work items from MongoDB, incrementally since last sync."""
    logger = get_run_logger()

    last_synced = None
    try:
        with engine.connect() as conn:
            result = conn.execute(
                AzureTicket.__table__.select().with_only_columns(
                    AzureTicket.__table__.c.synced_at
                ).order_by(AzureTicket.__table__.c.synced_at.desc()).limit(1)
            ).fetchone()
            if result:
                last_synced = result[0]
    except Exception:
        pass  # table may not exist yet

    client = MongoClient(settings.mongodb_url)
    db = client[settings.mongodb_database]
    collection = db["azure_tickets"]

    query: dict = {}
    if last_synced:
        query["_fetched_at"] = {"$gt": last_synced.isoformat()}
        logger.info(f"Incremental extract: fetching Azure tickets newer than {last_synced}")
    else:
        logger.info("Full extract: no previous Azure sync found")

    docs = list(collection.find(query, {"_id": 0}).sort("_fetched_at", -1).limit(limit))
    logger.info(f"Extracted {len(docs)} Azure work items from MongoDB")
    client.close()
    return docs


def _safe_field(fields: dict, *keys, default=None):
    """Safely get a nested value from mapped fields dict."""
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


@task(name="transform_azure_tickets")
def transform_azure_tickets(raw_docs: list[dict]) -> pd.DataFrame:
    """Transform raw Azure work item JSON into a pandas DataFrame for MariaDB."""
    logger = get_run_logger()

    rows = []
    for doc in raw_docs:
        # Use mapped (display name) fields; fall back to raw fields if needed
        fields: dict = doc.get("fields", doc.get("_raw_fields", {}))

        # Priority may be an int or a dict with a name
        priority_val = fields.get("Priority")
        if isinstance(priority_val, dict):
            priority_val = priority_val.get("id")
        try:
            priority_int = int(priority_val) if priority_val is not None else None
        except (ValueError, TypeError):
            priority_int = None

        assigned_to = fields.get("Assigned To") or fields.get("System.AssignedTo")
        if isinstance(assigned_to, dict):
            assigned_to = assigned_to.get("displayName")

        created_by = fields.get("Created By") or fields.get("System.CreatedBy")
        if isinstance(created_by, dict):
            created_by = created_by.get("displayName")

        rows.append({
            "work_item_id": doc.get("id"),
            "title": fields.get("Title") or fields.get("System.Title"),
            "work_item_type": fields.get("Work Item Type") or fields.get("System.WorkItemType"),
            "state": fields.get("State") or fields.get("System.State"),
            "priority": priority_int,
            "assigned_to": str(assigned_to or ""),
            "created_by": str(created_by or ""),
            "area_path": fields.get("Area Path") or fields.get("System.AreaPath") or "",
            "iteration_path": fields.get("Iteration Path") or fields.get("System.IterationPath") or "",
            "project": fields.get("Team Project") or fields.get("System.TeamProject") or "",
            "tags": fields.get("Tags") or fields.get("System.Tags") or "",
            "description": str(fields.get("Description") or fields.get("System.Description") or ""),
            "created_at": _parse_dt(fields.get("Created Date") or fields.get("System.CreatedDate")),
            "updated_at": _parse_dt(fields.get("Changed Date") or fields.get("System.ChangedDate")),
            "resolved_at": _parse_dt(fields.get("Resolved Date") or fields.get("Microsoft.VSTS.Common.ResolvedDate")),
            "synced_at": datetime.utcnow(),
        })

    df = pd.DataFrame(rows)

    if df.empty:
        logger.info("No Azure work items to transform")
        return df

    df = df.dropna(subset=["work_item_id"])
    df = df.drop_duplicates(subset=["work_item_id"], keep="first")

    for col in ("created_at", "updated_at", "resolved_at", "synced_at"):
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
        df[col] = df[col].dt.tz_localize(None)

    str_cols = ["title", "work_item_type", "state", "assigned_to", "created_by",
                "area_path", "iteration_path", "project", "tags", "description"]
    df[str_cols] = df[str_cols].fillna("")

    logger.info(f"Transformed {len(df)} Azure work items into DataFrame")
    return df


@task(name="load_azure_to_mariadb")
def load_azure_to_mariadb(df: pd.DataFrame) -> int:
    """Bulk upsert transformed Azure work items DataFrame into MariaDB."""
    logger = get_run_logger()

    if df.empty:
        logger.info("No Azure rows to load")
        return 0

    Base.metadata.create_all(engine)

    rows = df.to_dict(orient="records")
    with engine.begin() as conn:
        stmt = mysql_upsert(AzureTicket.__table__).values(rows)
        update_cols = {c.name: c for c in stmt.inserted if c.name != "work_item_id"}
        stmt = stmt.on_duplicate_key_update(**update_cols)
        conn.execute(stmt)

    logger.info(f"Loaded {len(rows)} Azure work items into MariaDB")
    return len(rows)


@flow(name="azure_etl_flow", log_prints=True)
def azure_etl_flow(limit: int = 500) -> dict:
    """ETL: MongoDB (raw Azure JSON) → transform → MariaDB."""
    raw = extract_azure_from_mongodb(limit=limit)
    df = transform_azure_tickets(raw)
    count = load_azure_to_mariadb(df)
    return {"extracted": len(raw), "loaded": count}


if __name__ == "__main__":
    azure_etl_flow()
