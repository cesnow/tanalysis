"""FastAPI application lifespan — startup and shutdown logic."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db.mariadb import engine, init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create DB tables on startup (idempotent). Run Alembic for schema migrations."""
    init_db()
    yield
    # Teardown: close connection pool
    engine.dispose()
