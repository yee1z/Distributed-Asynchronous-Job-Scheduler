from __future__ import annotations

import json
import time

import redis
import redis.asyncio as aioredis

from backend.common.config import get_settings
from backend.common.metrics import (
    REDIS_COMMAND_DURATION,
    REDIS_COMMANDS,
    REDIS_DELAYED_RUNS,
    REDIS_STREAM_LENGTH,
    observe_redis_command,
)

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
        with observe_redis_command("xgroup_create"):
            client.xgroup_create(
                name=_settings.stream_key,
                groupname=_settings.consumer_group,
                id="0",
                mkstream=True,
            )
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def _observe_async(operation: str, awaitable):
    start = time.perf_counter()
    status = "success"
    try:
        return await awaitable
    except Exception:
        status = "error"
        raise
    finally:
        REDIS_COMMAND_DURATION.labels(operation=operation).observe(time.perf_counter() - start)
        REDIS_COMMANDS.labels(operation=operation, status=status).inc()


async def ensure_group_async(client: aioredis.Redis) -> None:
    try:
        await _observe_async(
            "xgroup_create",
            client.xgroup_create(
                name=_settings.stream_key,
                groupname=_settings.consumer_group,
                id="0",
                mkstream=True,
            ),
        )
    except aioredis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def enqueue_run(client: redis.Redis, run_id: int) -> str:
    """Add a run to the dispatch stream. Returns the stream message id."""
    with observe_redis_command("xadd"):
        message_id = client.xadd(_settings.stream_key, {"run_id": str(run_id)})
    update_queue_gauges(client)
    return message_id


def schedule_delayed(client: redis.Redis, run_id: int, ready_at_epoch: float) -> None:
    """Register a run to be promoted onto the stream once ``ready_at_epoch`` passes."""
    with observe_redis_command("zadd"):
        client.zadd(_settings.delayed_set_key, {str(run_id): ready_at_epoch})
    update_queue_gauges(client)


async def enqueue_run_async(client: aioredis.Redis, run_id: int) -> str:
    message_id = await _observe_async("xadd", client.xadd(_settings.stream_key, {"run_id": str(run_id)}))
    await update_queue_gauges_async(client)
    return message_id


async def schedule_delayed_async(
    client: aioredis.Redis, run_id: int, ready_at_epoch: float
) -> None:
    await _observe_async("zadd", client.zadd(_settings.delayed_set_key, {str(run_id): ready_at_epoch}))
    await update_queue_gauges_async(client)


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
    with observe_redis_command("eval"):
        count = client.eval(_PROMOTE_LUA, 2, _settings.delayed_set_key, _settings.stream_key, now_epoch)
    update_queue_gauges(client)
    return int(count)


def update_queue_gauges(client: redis.Redis) -> None:
    try:
        with observe_redis_command("xlen"):
            REDIS_STREAM_LENGTH.set(client.xlen(_settings.stream_key))
    except redis.ResponseError:
        REDIS_STREAM_LENGTH.set(0)
    try:
        with observe_redis_command("zcard"):
            REDIS_DELAYED_RUNS.set(client.zcard(_settings.delayed_set_key))
    except redis.ResponseError:
        REDIS_DELAYED_RUNS.set(0)


async def update_queue_gauges_async(client: aioredis.Redis) -> None:
    try:
        REDIS_STREAM_LENGTH.set(await _observe_async("xlen", client.xlen(_settings.stream_key)))
    except aioredis.ResponseError:
        REDIS_STREAM_LENGTH.set(0)
    try:
        REDIS_DELAYED_RUNS.set(await _observe_async("zcard", client.zcard(_settings.delayed_set_key)))
    except aioredis.ResponseError:
        REDIS_DELAYED_RUNS.set(0)


def pending_count(client: redis.Redis) -> int:
    """Number of delivered-but-unacked messages in the consumer group."""
    try:
        with observe_redis_command("xpending"):
            summary = client.xpending(_settings.stream_key, _settings.consumer_group)
    except redis.ResponseError:
        return 0
    if isinstance(summary, dict):
        return int(summary.get("pending", 0))
    return int(summary[0]) if summary else 0


def encode_payload(data: dict) -> dict[str, str]:
    return {"payload": json.dumps(data)}
