from fastapi import FastAPI, Query

from app.flows.jira_clean_flow import jira_clean_flow, jira_product_clean_flow
from app.flows.jira_sync_flow import jira_product_sync_flow, jira_sync_flow
from app.routers.jira import router as jira_router
from app.routers.product import router as product_router

app = FastAPI(title="TAnalysis", description="Jira Analytics Pipeline")

app.include_router(jira_router)
app.include_router(product_router)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/flows/jira-sync", tags=["Flows"])
async def trigger_jira_sync():
    """觸發所有 enabled products 的 Jira 同步 flow（Jira API → MongoDB），完成後自動觸發清洗寫入 MariaDB。每小時排程亦執行此 flow。"""
    results = jira_sync_flow()
    return {"status": "completed", "results": results}


@app.post("/flows/jira-sync/{product_id}", tags=["Flows"])
async def trigger_jira_product_sync(product_id: int, product_name: str = Query(...), jql: str = Query(...)):
    """手動觸發單一 product 的 Jira 同步 flow（Jira API → MongoDB）。"""
    result = jira_product_sync_flow(product_id=product_id, product_name=product_name, jql=jql)
    return {"status": "completed", "result": result}


@app.post("/flows/jira-clean", tags=["Flows"])
async def trigger_jira_clean():
    """手動觸發所有 enabled products 的 Jira 清洗 flow（MongoDB → MariaDB）。"""
    results = jira_clean_flow()
    return {"status": "completed", "results": results}


@app.post("/flows/jira-clean/{product_id}", tags=["Flows"])
async def trigger_jira_product_clean(product_id: int, product_name: str = Query(...)):
    """手動觸發單一 product 的 Jira 清洗 flow（MongoDB → MariaDB）。"""
    result = jira_product_clean_flow(product_id=product_id, product_name=product_name)
    return {"status": "completed", "result": result}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
