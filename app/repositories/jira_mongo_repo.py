"""Repository for Jira raw tickets (MongoDB / pymongo)."""

from datetime import datetime, timezone

from pymongo.collection import Collection


def upsert_many(
    collection: Collection,
    issues: list[dict],
    product_id: int | None = None,
    product_name: str | None = None,
) -> int:
    """Upsert Jira issues into MongoDB, optionally tagging with product metadata."""
    if not issues:
        return 0
    fetched_at = datetime.now(timezone.utc).isoformat()
    for issue in issues:
        issue["_fetched_at"] = fetched_at
        if product_id is not None:
            issue["_product_id"] = product_id
        if product_name is not None:
            issue["_product_name"] = product_name
        collection.update_one({"key": issue["key"]}, {"$set": issue}, upsert=True)
    return len(issues)


def find_by_product(collection: Collection, product_id: int) -> list[dict]:
    """Return all raw issues for a given product."""
    return list(collection.find({"_product_id": product_id}, {"_id": 0}))


def find_all(
    collection: Collection,
    project_key: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return cached issues, optionally filtered by Jira project key."""
    query: dict = {}
    if project_key:
        query["fields.project.key"] = project_key
    return list(collection.find(query, {"_id": 0}).sort("_fetched_at", -1).limit(limit))
