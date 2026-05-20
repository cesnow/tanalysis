"""Prefect flow: 從 MongoDB 讀取 Jira raw data，清洗後 upsert 進 MariaDB。

此 flow 由 jira_sync_flow 在每個 product 同步完成後自動觸發。
亦可透過 API endpoint 手動觸發。
"""
from datetime import datetime

from prefect import flow, task, get_run_logger
from pymongo import MongoClient
from sqlalchemy.dialects.mysql import insert as mysql_upsert

from app.config import settings
from app.db.mariadb import Base, engine
from app.models.jira_ticket import JiraTicket


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
    """從 MongoDB jira_tickets collection 讀取指定 product 的 raw issues。"""
    logger = get_run_logger()

    client = MongoClient(settings.mongodb_url)
    db = client[settings.mongodb_database]
    collection = db["jira_tickets"]

    docs = list(collection.find(
        {"_product_id": product_id},
        {"_id": 0},
    ))
    client.close()

    logger.info(f"[{product_name}] Extracted {len(docs)} issues from MongoDB")
    return docs


@task(name="clean_and_load_jira_to_mariadb")
def clean_and_load_jira_to_mariadb(product_name: str, docs: list[dict]) -> int:
    """將 MongoDB raw issues 清洗後 upsert 進 MariaDB jira_tickets 表。"""
    logger = get_run_logger()

    if not docs:
        logger.info(f"[{product_name}] No issues to clean and load")
        return 0

    Base.metadata.create_all(engine)

    rows = []
    for issue in docs:
        fields = issue.get("fields", {})
        rows.append({
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
            "synced_at": datetime.utcnow(),
        })

    # 過濾掉 ticket_key 為空的資料
    rows = [r for r in rows if r.get("ticket_key")]

    if not rows:
        logger.info(f"[{product_name}] No valid rows after cleaning")
        return 0

    with engine.begin() as conn:
        stmt = mysql_upsert(JiraTicket.__table__).values(rows)
        update_cols = {c.name: c for c in stmt.inserted if c.name != "ticket_key"}
        stmt = stmt.on_duplicate_key_update(**update_cols)
        conn.execute(stmt)

    logger.info(f"[{product_name}] Cleaned and loaded {len(rows)} issues into MariaDB")
    return len(rows)


# ---------- per-product sub-flow ----------

@flow(name="jira_product_clean_flow", log_prints=True)
def jira_product_clean_flow(product_id: int, product_name: str) -> dict:
    """針對單一 product 執行 MongoDB → MariaDB 資料清洗。"""
    docs = extract_jira_from_mongodb(product_id, product_name)
    count = clean_and_load_jira_to_mariadb(product_name, docs)
    return {"product": product_name, "product_id": product_id, "cleaned": count}


# ---------- main flow（可手動觸發全部清洗） ----------

@flow(name="jira_clean_flow", log_prints=True)
def jira_clean_flow() -> list[dict]:
    """對所有 enabled products 執行 MongoDB → MariaDB 資料清洗。
    通常由 jira_sync_flow 自動觸發，亦可手動呼叫。
    """
    from app.db.mariadb import SessionLocal
    from app.models.product import Product

    logger = get_run_logger()

    Base.metadata.create_all(engine)
    db = SessionLocal()
    try:
        products = db.query(Product).filter(Product.enabled == True).all()  # noqa: E712
        product_list = [{"id": p.id, "name": p.name} for p in products]
    finally:
        db.close()

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
