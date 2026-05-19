"""Azure DevOps service: fetch custom fields then work items, store raw JSON in MongoDB."""
from datetime import datetime
import httpx
from app.config import settings
from app.db.mongodb import db as mongo_db

azure_tickets_collection = mongo_db["azure_tickets"]
azure_fields_collection = mongo_db["azure_fields"]


def _auth_header() -> dict:
    import base64
    token = base64.b64encode(f":{settings.azure_pat}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


async def fetch_azure_custom_fields() -> dict:
    """Fetch all work item fields from Azure DevOps and cache in MongoDB.

    Returns a mapping of field reference name → display name.
    """
    url = (
        f"{settings.azure_base_url}/{settings.azure_organization}"
        f"/{settings.azure_project}/_apis/wit/fields?api-version=7.1"
    )
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=_auth_header(), timeout=30)
        response.raise_for_status()
        data = response.json()

    fields = data.get("value", [])
    field_map: dict[str, str] = {f["referenceName"]: f["name"] for f in fields}

    # Cache in MongoDB (upsert by referenceName)
    for field in fields:
        await azure_fields_collection.update_one(
            {"referenceName": field["referenceName"]},
            {"$set": {**field, "_fetched_at": datetime.utcnow().isoformat()}},
            upsert=True,
        )

    return field_map


async def _get_cached_field_map() -> dict:
    """Load field map from MongoDB cache; fetch from API if empty."""
    count = await azure_fields_collection.count_documents({})
    if count == 0:
        return await fetch_azure_custom_fields()
    cursor = azure_fields_collection.find({}, {"_id": 0, "referenceName": 1, "name": 1})
    docs = await cursor.to_list(length=None)
    return {d["referenceName"]: d["name"] for d in docs}


async def fetch_azure_tickets(
    wiql: str | None = None, max_results: int = 50
) -> list[dict]:
    """Fetch work items from Azure DevOps, map custom fields, and store in MongoDB.

    Steps:
      1. Ensure field map is available (fetch if needed).
      2. Run WIQL query to get work item IDs.
      3. Batch-fetch work item details.
      4. Map field reference names to display names.
      5. Upsert into MongoDB.
    """
    field_map = await _get_cached_field_map()

    # Step 1: WIQL query to get IDs
    if not wiql:
        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.TeamProject] = '{settings.azure_project}' "
            f"ORDER BY [System.ChangedDate] DESC"
        )
    wiql_url = (
        f"{settings.azure_base_url}/{settings.azure_organization}"
        f"/{settings.azure_project}/_apis/wit/wiql?api-version=7.1"
    )
    async with httpx.AsyncClient() as client:
        wiql_resp = await client.post(
            wiql_url,
            headers={**_auth_header(), "Content-Type": "application/json"},
            json={"query": wiql},
            timeout=30,
        )
        wiql_resp.raise_for_status()
        work_item_refs = wiql_resp.json().get("workItems", [])[:max_results]

    if not work_item_refs:
        return []

    ids = [str(ref["id"]) for ref in work_item_refs]

    # Step 2: Batch fetch work item details (max 200 per request)
    batch_size = 200
    all_items: list[dict] = []
    async with httpx.AsyncClient() as client:
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i: i + batch_size]
            detail_url = (
                f"{settings.azure_base_url}/{settings.azure_organization}"
                f"/_apis/wit/workitems?ids={','.join(batch_ids)}"
                f"&$expand=all&api-version=7.1"
            )
            detail_resp = await client.get(
                detail_url, headers=_auth_header(), timeout=60
            )
            detail_resp.raise_for_status()
            all_items.extend(detail_resp.json().get("value", []))

    # Step 3: Map field reference names → display names and upsert into MongoDB
    mapped_items: list[dict] = []
    for item in all_items:
        raw_fields: dict = item.get("fields", {})
        mapped_fields: dict = {
            field_map.get(ref, ref): value for ref, value in raw_fields.items()
        }
        doc = {
            "id": item["id"],
            "url": item.get("url"),
            "fields": mapped_fields,
            "_raw_fields": raw_fields,
            "_fetched_at": datetime.utcnow().isoformat(),
        }
        await azure_tickets_collection.update_one(
            {"id": item["id"]},
            {"$set": doc},
            upsert=True,
        )
        mapped_items.append(doc)

    return mapped_items


async def get_azure_tickets_from_mongo(
    project: str | None = None, limit: int = 50
) -> list[dict]:
    """Read cached Azure work items from MongoDB."""
    query: dict = {}
    if project:
        query["fields.System.TeamProject"] = project
    cursor = azure_tickets_collection.find(query, {"_id": 0}).sort(
        "_fetched_at", -1
    ).limit(limit)
    return await cursor.to_list(length=limit)
