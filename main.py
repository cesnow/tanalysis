from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.api.routers.flows import router as flows_router
from app.api.routers.health import router as health_router
from app.api.routers.jira import router as jira_router
from app.api.routers.product import router as product_router
from app.db.base import Base
from app.db.database import engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup - run DB migrations / create tables
    Base.metadata.create_all(engine)
    yield
    # Teardown
    pass


app = FastAPI(title="TAnalysis", description="Jira Analytics Pipeline", lifespan=lifespan)

app.include_router(health_router)
app.include_router(jira_router)
app.include_router(product_router)
app.include_router(flows_router)

# Enable Prometheus Metrics
Instrumentator().instrument(app).expose(app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
