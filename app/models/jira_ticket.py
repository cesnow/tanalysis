from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Integer, String, Text

from app.db.base import Base


class JiraTicket(Base):
    """Fact table for Jira tickets stored in MariaDB."""

    __tablename__ = "jira_tickets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticket_key = Column(String(64), unique=True, nullable=False, index=True)
    summary = Column(String(512), nullable=True)
    status = Column(String(64), nullable=True)
    issue_type = Column(String(64), nullable=True)
    priority = Column(String(64), nullable=True)
    assignee = Column(String(256), nullable=True)
    reporter = Column(String(256), nullable=True)
    project_key = Column(String(64), nullable=True)
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    resolved_at = Column(DateTime, nullable=True)
    description = Column(Text, nullable=True)
    labels = Column(Text, nullable=True)
    synced_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
