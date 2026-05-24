"""Repository for Jira raw tickets (MongoDB / pymongo)."""

from datetime import UTC, datetime

from pymongo.collection import Collection


def _require_collection(collection: Collection | None) -> Collection:
    if collection is None:
        raise RuntimeError("MongoDB is disabled or the jira_tickets collection is unavailable")
    return collection


def upsert_many(
    collection: Collection | None,
    issues: list[dict],
    product_id: int | None = None,
    product_name: str | None = None,
) -> int:
    """Upsert Jira issues into MongoDB, optionally tagging with product metadata."""
    if not issues:
        return 0
    collection = _require_collection(collection)
    fetched_at = datetime.now(UTC).isoformat()
    for issue in issues:
        issue["_fetched_at"] = fetched_at
        if product_id is not None:
            issue["_product_id"] = product_id
        if product_name is not None:
            issue["_product_name"] = product_name
        collection.update_one({"key": issue["key"]}, {"$set": issue}, upsert=True)
    return len(issues)


def find_by_product(collection: Collection | None, product_id: int) -> list[dict]:
    """Return all raw issues for a given product."""
    collection = _require_collection(collection)
    return list(collection.find({"_product_id": product_id}, {"_id": 0}))


def find_all(
    collection: Collection | None,
    project_key: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return cached issues, optionally filtered by Jira project key."""
    collection = _require_collection(collection)
    query: dict = {}
    if project_key:
        query["fields.project.key"] = project_key
    return list(collection.find(query, {"_id": 0}).sort("_fetched_at", -1).limit(limit))
