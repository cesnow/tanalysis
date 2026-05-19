from fastapi import FastAPI, Query

from app.routers.jira import router as jira_router
from app.flows.jira_etl import jira_etl_flow

app = FastAPI(title="TAnalysis", description="Jira Analytics Pipeline")

app.include_router(jira_router)


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/flows/jira-etl", tags=["Flows"])
async def trigger_jira_etl(limit: int = Query(500, ge=1, le=10000)):
    """Trigger the Prefect Jira ETL flow (MongoDB → MariaDB)."""
    result = jira_etl_flow(limit=limit)
    return {"status": "completed", "result": result}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
