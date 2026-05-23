"""Prefect flow: 針對每個 enabled Product 用 JQL 從 Jira API 抓取所有 issues，upsert 進 MongoDB。

排程：每小時執行一次（由 Prefect deployment 設定 interval=3600）。
完成後自動觸發 jira_clean_flow 進行資料清洗寫入 MariaDB。
"""
import base64
from datetime import datetime

import requests
from prefect import flow, get_run_logger, task
from pymongo import MongoClient

from app.config import settings
from app.db.mariadb import Base, SessionLocal, engine
from app.models.product import Product

ISSUE_TYPES = ["Bug", "Epic", "Task", "Sub-task"]


# ---------- helpers ----------

def _jira_headers() -> dict:
    token = base64.b64encode(
        f"{settings.jira_email}:{settings.jira_api_token}".encode()
    ).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
    }


def _build_jql(base_jql: str) -> str:
    """在 product JQL 基礎上加入 issue type 過濾。"""
    types_str = ", ".join(f'"{t}"' for t in ISSUE_TYPES)
    return f"({base_jql}) AND issuetype in ({types_str}) ORDER BY updated DESC"


# ---------- tasks ----------

@task(name="fetch_jira_issues_by_jql", retries=2, retry_delay_seconds=30)
def fetch_jira_issues_by_jql(product_name: str, jql: str) -> list[dict]:
    """呼叫 Jira REST API，用 JQL 分頁抓取所有符合的 issues。"""
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
    """將抓到的 Jira issues upsert 進 MongoDB jira_tickets collection。"""
    logger = get_run_logger()

    if not issues:
        logger.info(f"[{product_name}] No issues to upsert")
        return 0

    fetched_at = datetime.utcnow().isoformat()
    client = MongoClient(settings.mongodb_url)
    db = client[settings.mongodb_database]
    collection = db["jira_tickets"]

    upserted = 0
    for issue in issues:
        issue["_fetched_at"] = fetched_at
        issue["_product_id"] = product_id
        issue["_product_name"] = product_name
        collection.update_one(
            {"key": issue["key"]},
            {"$set": issue},
            upsert=True,
        )
        upserted += 1

    client.close()
    logger.info(f"[{product_name}] Upserted {upserted} issues into MongoDB")
    return upserted


# ---------- per-product sub-flow ----------

@flow(name="jira_product_sync_flow", log_prints=True)
def jira_product_sync_flow(product_id: int, product_name: str, jql: str) -> dict:
    """針對單一 product 執行 Jira 抓取並寫入 MongoDB。"""
    issues = fetch_jira_issues_by_jql(product_name, jql)
    count = upsert_jira_issues_to_mongodb(product_name, product_id, issues)
    return {"product": product_name, "product_id": product_id, "fetched": len(issues), "upserted": count}


# ---------- main flow（每小時排程） ----------

@flow(name="jira_sync_flow", log_prints=True)
def jira_sync_flow() -> list[dict]:
    """主 flow：讀取所有 enabled products，逐一用 JQL 從 Jira API 抓取並 upsert 進 MongoDB。
    每個 product 完成後，自動觸發 jira_clean_flow 進行資料清洗寫入 MariaDB。

    Prefect deployment 建議設定：
        interval = 3600  # 每小時
    """
    from app.flows.jira_clean_flow import jira_product_clean_flow

    logger = get_run_logger()

    Base.metadata.create_all(engine)
    db = SessionLocal()
    try:
        products = db.query(Product).filter(Product.enabled == True).all()  # noqa: E712
        product_list = [{"id": p.id, "name": p.name, "jql": p.jql} for p in products]
    finally:
        db.close()

    if not product_list:
        logger.info("No enabled products found, skipping.")
        return []

    logger.info(f"Found {len(product_list)} enabled product(s): {[p['name'] for p in product_list]}")

    results = []
    for p in product_list:
        # Step 1: 同步 Jira → MongoDB
        sync_result = jira_product_sync_flow(
            product_id=p["id"],
            product_name=p["name"],
            jql=p["jql"],
        )
        # Step 2: 同步完成後，觸發清洗 MongoDB → MariaDB
        clean_result = jira_product_clean_flow(
            product_id=p["id"],
            product_name=p["name"],
        )
        results.append({**sync_result, "cleaned": clean_result.get("cleaned", 0)})

    return results


if __name__ == "__main__":
    jira_sync_flow()
