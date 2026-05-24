"""FastAPI application lifespan — startup and shutdown logic."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db.base import Base
from app.db.database import engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create DB tables on startup (idempotent). Run Alembic for schema migrations."""
    Base.metadata.create_all(bind=engine)
    yield
    # Teardown: close connection pool
    engine.dispose()
