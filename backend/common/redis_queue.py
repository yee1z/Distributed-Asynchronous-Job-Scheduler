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


def schedule_delayed(client: redis.Redis, run_id: int, ready_at_epoch: float) -> None:
    """Register a run to be promoted onto the stream once ``ready_at_epoch`` passes."""
    client.zadd(_settings.delayed_set_key, {str(run_id): ready_at_epoch})


async def enqueue_run_async(client: aioredis.Redis, run_id: int) -> str:
    return await client.xadd(_settings.stream_key, {"run_id": str(run_id)})


async def schedule_delayed_async(
    client: aioredis.Redis, run_id: int, ready_at_epoch: float
) -> None:
    await client.zadd(_settings.delayed_set_key, {str(run_id): ready_at_epoch})


# Atomically pop every due member from the delayed set and push it onto the stream.
# Atomicity matters so the scheduler leader never double-promotes a retry.
_PROMOTE_LUA = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, member in ipairs(due) do
  redis.call('ZREM', KEYS[1], member)
  redis.call('XADD', KEYS[2], '*', 'run_id', member)
end
return #due
"""


def promote_due_delayed(client: redis.Redis, now_epoch: float) -> int:
    """Move all delayed runs whose time has come onto the dispatch stream."""
    count = client.eval(_PROMOTE_LUA, 2, _settings.delayed_set_key, _settings.stream_key, now_epoch)
    return int(count)


def pending_count(client: redis.Redis) -> int:
    """Number of delivered-but-unacked messages in the consumer group."""
    try:
        summary = client.xpending(_settings.stream_key, _settings.consumer_group)
    except redis.ResponseError:
        return 0
    if isinstance(summary, dict):
        return int(summary.get("pending", 0))
    return int(summary[0]) if summary else 0


def encode_payload(data: dict) -> dict[str, str]:
    return {"payload": json.dumps(data)}
