# Importing all ORM models here ensures they are registered with Base.metadata.
# This is required for Alembic autogenerate to detect all tables.
from app.models.jira_ticket import JiraTicket as JiraTicket
from app.models.product import Product as Product
