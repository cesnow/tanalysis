from fastapi import APIRouter, HTTPException, Query

from app.schemas.jira import ListTicketsResponse, SyncTicketsResponse
from app.services.jira_service import fetch_jira_tickets, get_tickets_from_mongo

router = APIRouter(prefix="/jira", tags=["Jira"])


@router.post("/tickets/sync", response_model=SyncTicketsResponse)
def sync_jira_tickets(
    jql: str | None = Query(None, description="JQL query to filter tickets"),
    max_results: int = Query(50, ge=1, le=1000),
):
    """Fetch tickets from Jira API and save raw JSON to MongoDB."""
    try:
        issues = fetch_jira_tickets(jql=jql, max_results=max_results)
        return {"synced": len(issues), "tickets": [i["key"] for i in issues]}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Jira API error: {e}") from e


@router.get("/tickets", response_model=ListTicketsResponse)
def list_jira_tickets(
    project_key: str | None = Query(None),
    limit: int = Query(50, ge=1, le=1000),
):
    """List cached Jira tickets from MongoDB."""
    tickets = get_tickets_from_mongo(project_key=project_key, limit=limit)
    return {"count": len(tickets), "tickets": tickets}
