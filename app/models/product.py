from datetime import UTC, datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text

from app.db.base import Base


class Product(Base):
    """Product dimension table: defines a product and its Jira JQL scope."""

    __tablename__ = "products"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    # JQL defines the Jira scope this product can fetch, e.g.:
    # project = "MYPROJ" AND area = "backend"
    jql = Column(Text, nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC).replace(tzinfo=None))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(UTC).replace(tzinfo=None),
        onupdate=lambda: datetime.now(UTC).replace(tzinfo=None),
    )
