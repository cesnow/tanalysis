from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from app.config.database import mariadb

engine = create_engine(mariadb.url, echo=mariadb.echo, pool_recycle=mariadb.pool_recycle)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
DatabaseModel = declarative_base()
