import redis as redis_sync
import redis.asyncio as redis_async

from shared.config.database import redis as redis_config

client_sync: redis_sync.Redis | None = None
client_async: redis_async.Redis | None = None

if redis_config.enabled:
    client_sync = redis_sync.from_url(redis_config.url, decode_responses=True)
    client_async = redis_async.from_url(redis_config.url, decode_responses=True)
