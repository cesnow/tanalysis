from fastapi import APIRouter, Query

from app.flows.jira_clean_flow import jira_clean_flow, jira_product_clean_flow
from app.flows.jira_sync_flow import jira_product_sync_flow, jira_sync_flow

router = APIRouter(prefix="/flows", tags=["Flows"])


@router.post("/jira-sync")
def trigger_jira_sync():
    """Trigger Jira sync flow for all enabled products (Jira API → MongoDB), then automatically trigger the clean flow to write into MariaDB. Also runs on the hourly schedule."""
    results = jira_sync_flow()
    return {"status": "completed", "results": results}


@router.post("/jira-sync/{product_id}")
def trigger_jira_product_sync(product_id: int, product_name: str = Query(...), jql: str = Query(...)):
    """Manually trigger the Jira sync flow for a single product (Jira API → MongoDB)."""
    result = jira_product_sync_flow(product_id=product_id, product_name=product_name, jql=jql)
    return {"status": "completed", "result": result}


@router.post("/jira-clean")
def trigger_jira_clean():
    """Manually trigger the Jira clean flow for all enabled products (MongoDB → MariaDB)."""
    results = jira_clean_flow()
    return {"status": "completed", "results": results}


@router.post("/jira-clean/{product_id}")
def trigger_jira_product_clean(product_id: int, product_name: str = Query(...)):
    """Manually trigger the Jira clean flow for a single product (MongoDB → MariaDB)."""
    result = jira_product_clean_flow(product_id=product_id, product_name=product_name)
    return {"status": "completed", "result": result}
