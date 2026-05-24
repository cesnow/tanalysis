from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config.database import mariadb

engine = create_engine(mariadb.url, echo=mariadb.echo, pool_recycle=mariadb.pool_recycle)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def init_db() -> None:
    """Create all tables in the database if they don't exist."""
    import app.models  # noqa: F401 - Ensures all ORM models are registered
    from app.db.base import DatabaseModel

    DatabaseModel.metadata.create_all(bind=engine)


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency: yields a SQLAlchemy session and closes it after the request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
