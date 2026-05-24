from typing import cast

import pytest
from pymongo.collection import Collection

from shared.repositories import jira_mongo_repo


class _FakeCursor:
    def __init__(self, docs: list[dict]) -> None:
        self.docs = docs
        self.sort_args = None
        self.limit_value = None

    def sort(self, *args):
        self.sort_args = args
        return self

    def limit(self, value: int):
        self.limit_value = value
        return self

    def __iter__(self):
        return iter(self.docs)


class _FakeCollection:
    def __init__(self) -> None:
        self.updates: list[tuple[dict, dict, bool]] = []
        self.find_calls: list[tuple[dict, dict]] = []
        self.cursor = _FakeCursor([{"key": "TAN-1"}])

    def update_one(self, query: dict, update: dict, upsert: bool = False) -> None:
        self.updates.append((query, update, upsert))

    def find(self, query: dict, projection: dict):
        self.find_calls.append((query, projection))
        return self.cursor


def test_upsert_many_adds_product_metadata() -> None:
    collection = _FakeCollection()
    issues = [{"key": "TAN-1", "fields": {}}]

    count = jira_mongo_repo.upsert_many(cast(Collection, collection), issues, product_id=7, product_name="Core")

    assert count == 1
    assert issues[0]["_product_id"] == 7
    assert issues[0]["_product_name"] == "Core"
    assert "_fetched_at" in issues[0]
    assert collection.updates == [({"key": "TAN-1"}, {"$set": issues[0]}, True)]


def test_find_all_filters_by_project_and_limits_results() -> None:
    collection = _FakeCollection()

    docs = jira_mongo_repo.find_all(cast(Collection, collection), project_key="TAN", limit=25)

    assert docs == [{"key": "TAN-1"}]
    assert collection.find_calls == [({"fields.project.key": "TAN"}, {"_id": 0})]
    assert collection.cursor.sort_args == ("_fetched_at", -1)
    assert collection.cursor.limit_value == 25


def test_mongo_repository_raises_clear_error_when_collection_is_missing() -> None:
    with pytest.raises(RuntimeError, match="MongoDB is disabled"):
        jira_mongo_repo.find_all(None)
