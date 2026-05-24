from fastapi import APIRouter, HTTPException, Query
from prefect.client.orchestration import get_client

router = APIRouter(prefix="/flows", tags=["Flows"])


async def _trigger_deployment(name: str, parameters: dict | None = None):
    """Helper to asynchronously trigger a Prefect deployment by name without blocking."""
    async with get_client() as client:
        try:
            deployment = await client.read_deployment_by_name(name)
            await client.create_flow_run_from_deployment(deployment_id=deployment.id, parameters=parameters)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to trigger deployment {name}: {e!s}") from e


@router.post("/jira-sync")
async def trigger_jira_sync():
    """Trigger Jira sync flow for all enabled products."""
    await _trigger_deployment("jira_sync_flow/jira-sync")
    return {"status": "triggered"}


@router.post("/jira-sync/{product_id}")
async def trigger_jira_product_sync(product_id: int, product_name: str = Query(...), jql: str = Query(...)):
    """Manually trigger the Jira sync flow for a single product."""
    await _trigger_deployment(
        "jira_product_sync_flow/jira-product-sync",
        parameters={"product_id": product_id, "product_name": product_name, "jql": jql},
    )
    return {"status": "triggered"}


@router.post("/jira-clean")
async def trigger_jira_clean():
    """Manually trigger the Jira clean flow for all enabled products."""
    await _trigger_deployment("jira_clean_flow/jira-clean")
    return {"status": "triggered"}


@router.post("/jira-clean/{product_id}")
async def trigger_jira_product_clean(product_id: int, product_name: str = Query(...)):
    """Manually trigger the Jira clean flow for a single product."""
    await _trigger_deployment(
        "jira_product_clean_flow/jira-product-clean",
        parameters={"product_id": product_id, "product_name": product_name},
    )
    return {"status": "triggered"}
