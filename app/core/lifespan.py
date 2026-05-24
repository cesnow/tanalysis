"""FastAPI application lifespan — startup and shutdown logic."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db.mariadb import engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: manages teardown of DB connections."""
    yield
    # Teardown: close connection pool
    engine.dispose()
