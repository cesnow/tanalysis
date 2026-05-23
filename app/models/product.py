from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text

from app.db.mariadb import Base


class Product(Base):
    """Product dimension table: defines a product and its Jira JQL scope."""
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    # JQL 定義此 product 可抓取的 Jira 範圍，例如：
    # project = "MYPROJ" AND area = "backend"
    jql = Column(Text, nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
