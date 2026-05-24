"""Database connection configuration — typed config objects consumed by app/db/."""

from dataclasses import dataclass

from shared.config.settings import settings


@dataclass(frozen=True)
class _MariaDBConfig:
    url: str
    echo: bool = False
    pool_recycle: int = 1800  # reconnect after 30 min to prevent stale connections


@dataclass(frozen=True)
class _MongoDBConfig:
    enabled: bool
    url: str
    database: str
    server_selection_timeout_ms: int = 5_000
    replica_set: str | None = None


@dataclass(frozen=True)
class _RedisConfig:
    enabled: bool
    url: str


mariadb = _MariaDBConfig(url=settings.mariadb_url)
mongodb = _MongoDBConfig(
    enabled=settings.mongodb_enabled,
    url=settings.mongodb_url,
    database=settings.mongodb_database,
    replica_set=settings.mongodb_replica_set,
)
redis = _RedisConfig(enabled=settings.redis_enabled, url=settings.redis_url)
