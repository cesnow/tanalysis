"""Repository for JiraTicket (MariaDB / SQLAlchemy)."""

from sqlalchemy import Engine
from sqlalchemy.dialects.mysql import insert as mysql_upsert

from shared.models.jira_ticket import JiraTicket


def upsert_many(engine: Engine, rows: list[dict]) -> int:
    """Bulk-upsert cleaned JiraTicket rows using ON DUPLICATE KEY UPDATE."""
    if not rows:
        return 0
    with engine.begin() as conn:
        stmt = mysql_upsert(JiraTicket).values(rows)
        update_cols = {c.name: c for c in stmt.inserted if c.name not in ("id", "ticket_key")}
        stmt = stmt.on_duplicate_key_update(**update_cols)
        conn.execute(stmt)
    return len(rows)
