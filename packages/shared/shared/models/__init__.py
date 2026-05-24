# Importing all ORM models here ensures they are registered with DatabaseModel.metadata.
# This is required for Alembic autogenerate to detect all tables.
from shared.models.jira_ticket import JiraTicket as JiraTicket
from shared.models.product import Product as Product
