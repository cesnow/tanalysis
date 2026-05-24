import importlib
import sys


def _reload_module(name: str):
    module = sys.modules.get(name)
    if module is None:
        return importlib.import_module(name)
    return importlib.reload(module)


def _reload_config_and_db_modules() -> None:
    _reload_module("shared.config.settings")
    _reload_module("shared.config.database")


def test_mongodb_client_is_none_when_disabled(monkeypatch) -> None:
    monkeypatch.setenv("MONGODB_ENABLED", "false")
    _reload_config_and_db_modules()

    mongodb = _reload_module("shared.db.mongodb")

    assert mongodb.client is None
    assert mongodb.db is None
    assert mongodb.jira_tickets_collection is None


def test_redis_clients_are_none_when_disabled(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_ENABLED", "false")
    _reload_config_and_db_modules()

    redis = _reload_module("shared.db.redis")

    assert redis.client_sync is None
    assert redis.client_async is None
