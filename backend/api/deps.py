from __future__ import annotations

from collections.abc import Iterator

import redis

from backend.common.redis_queue import get_sync_redis


def get_redis() -> Iterator[redis.Redis]:
    """FastAPI dependency yielding a synchronous Redis client."""
    client = get_sync_redis()
    try:
        yield client
    finally:
        client.close()
