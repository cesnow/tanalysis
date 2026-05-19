"""SQLAlchemy model for Azure DevOps work items stored in MariaDB."""
from datetime import datetime
from sqlalchemy import Column, String, Text, DateTime, Integer
from app.db.mariadb import Base


class AzureTicket(Base):
    """Fact table for Azure DevOps work items stored in MariaDB."""

    __tablename__ = "azure_tickets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    work_item_id = Column(Integer, unique=True, nullable=False, index=True)
    title = Column(String(512), nullable=True)
    work_item_type = Column(String(64), nullable=True)
    state = Column(String(64), nullable=True)
    priority = Column(Integer, nullable=True)
    assigned_to = Column(String(256), nullable=True)
    created_by = Column(String(256), nullable=True)
    area_path = Column(String(512), nullable=True)
    iteration_path = Column(String(512), nullable=True)
    project = Column(String(256), nullable=True)
    tags = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    resolved_at = Column(DateTime, nullable=True)
    synced_at = Column(DateTime, default=datetime.utcnow)
