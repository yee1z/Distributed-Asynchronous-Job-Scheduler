from __future__ import annotations

import json

import redis
import redis.asyncio as aioredis

from backend.common.config import get_settings

_settings = get_settings()


def get_sync_redis() -> redis.Redis:
    """Synchronous client used by API and scheduler to enqueue work."""
    return redis.Redis.from_url(_settings.redis_url, decode_responses=True)


def get_async_redis() -> aioredis.Redis:
    """Async client used by the worker consumer loop."""
    return aioredis.Redis.from_url(_settings.redis_url, decode_responses=True)


def ensure_group(client: redis.Redis) -> None:
    """Create the consumer group (idempotent)."""
    try:
        client.xgroup_create(
            name=_settings.stream_key,
            groupname=_settings.consumer_group,
            id="0",
            mkstream=True,
        )
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def ensure_group_async(client: aioredis.Redis) -> None:
    try:
        await client.xgroup_create(
            name=_settings.stream_key,
            groupname=_settings.consumer_group,
            id="0",
            mkstream=True,
        )
    except aioredis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def enqueue_run(client: redis.Redis, run_id: int) -> str:
    """Add a run to the dispatch stream. Returns the stream message id."""
    return client.xadd(_settings.stream_key, {"run_id": str(run_id)})


def encode_payload(data: dict) -> dict[str, str]:
    return {"payload": json.dumps(data)}
