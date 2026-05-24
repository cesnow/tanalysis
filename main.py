from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.api.flows import router as flows_router
from app.api.health import router as health_router
from app.api.jira import router as jira_router
from app.api.product import router as product_router
from app.config.logging import configure_logging
from app.config.observability import METRICS_ENABLED
from app.core.lifespan import lifespan

configure_logging()

app = FastAPI(title="TAnalysis", description="Jira Analytics Pipeline", lifespan=lifespan)

app.include_router(health_router)
app.include_router(jira_router)
app.include_router(product_router)
app.include_router(flows_router)

if METRICS_ENABLED:
    Instrumentator().instrument(app).expose(app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
