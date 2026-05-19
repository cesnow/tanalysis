"""FastAPI router for Azure DevOps endpoints."""
from fastapi import APIRouter, HTTPException, Query
from app.services.azure_service import (
    fetch_azure_custom_fields,
    fetch_azure_tickets,
    get_azure_tickets_from_mongo,
)

router = APIRouter(prefix="/azure", tags=["Azure"])


@router.post("/fields/sync")
async def sync_azure_fields():
    """Fetch Azure DevOps custom fields and cache in MongoDB."""
    try:
        field_map = await fetch_azure_custom_fields()
        return {"synced_fields": len(field_map)}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Azure API error: {e}")


@router.post("/tickets/sync")
async def sync_azure_tickets(
    wiql: str | None = Query(None, description="WIQL query to filter work items"),
    max_results: int = Query(50, ge=1, le=1000),
):
    """Fetch Azure DevOps work items (custom fields first, then tickets) and save to MongoDB."""
    try:
        items = await fetch_azure_tickets(wiql=wiql, max_results=max_results)
        return {"synced": len(items), "work_item_ids": [i["id"] for i in items]}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Azure API error: {e}")


@router.get("/tickets")
async def list_azure_tickets(
    project: str | None = Query(None),
    limit: int = Query(50, ge=1, le=1000),
):
    """List cached Azure work items from MongoDB."""
    tickets = await get_azure_tickets_from_mongo(project=project, limit=limit)
    return {"count": len(tickets), "tickets": tickets}
