"""Worker process.

Consumes the dispatch stream as part of a Redis consumer group and executes
runs concurrently (asyncio, bounded by ``WORKER_CONCURRENCY``).

Reliability properties:
  * Horizontal scaling — run N replicas in one consumer group; Redis load-balances.
  * Failover — ``XAUTOCLAIM`` reclaims messages whose owner has been idle past
    ``CLAIM_MIN_IDLE_MS`` (i.e. crashed mid-run), so no run is lost.
  * No double execution — a run is only acked after it reaches a terminal state,
    and the DB claim is a conditional UPDATE so duplicate deliveries are no-ops.
  * Graceful shutdown — on SIGTERM we stop reading and let in-flight runs finish.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import time
import uuid

from prometheus_client import start_http_server

from backend.common.config import get_settings
from backend.common.logging import get_logger
from backend.common.metrics import (
    INFLIGHT,
    REDIS_COMMAND_DURATION,
    REDIS_COMMANDS,
    RUNS_RECLAIMED,
)
from backend.common.redis_queue import ensure_group_async, get_async_redis
from backend.worker.runner import process_run

logger = get_logger(__name__)
settings = get_settings()


def _worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


async def _observe_redis(operation: str, awaitable):
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


def _parse_run_id(fields: dict) -> int | None:
    try:
        return int(fields["run_id"])
    except (KeyError, TypeError, ValueError):
        return None


async def run() -> None:
    start_http_server(settings.metrics_port)
    aredis = get_async_redis()
    await ensure_group_async(aredis)

    worker_id = _worker_id()
    stream, group = settings.stream_key, settings.consumer_group
    sem = asyncio.Semaphore(settings.worker_concurrency)
    inflight: set[asyncio.Task] = set()
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    logger.info("worker %s started: concurrency=%s, metrics on :%s",
                worker_id, settings.worker_concurrency, settings.metrics_port)

    async def handle(message_id: str, run_id: int, *, allow_running: bool) -> None:
        async with sem:
            INFLIGHT.inc()
            try:
                await process_run(aredis, run_id, allow_running=allow_running, worker_id=worker_id)
            except Exception:  # noqa: BLE001
                logger.exception("worker: unhandled error processing run_id=%s", run_id)
            finally:
                INFLIGHT.dec()
                # Ack only after reaching a terminal state, then drop from the stream.
                await _observe_redis("xack", aredis.xack(stream, group, message_id))
                await _observe_redis("xdel", aredis.xdel(stream, message_id))

    def spawn(message_id: str, run_id: int, *, allow_running: bool) -> None:
        task = asyncio.create_task(handle(message_id, run_id, allow_running=allow_running))
        inflight.add(task)
        task.add_done_callback(inflight.discard)

    while not stop.is_set():
        capacity = settings.worker_concurrency - len(inflight)
        if capacity <= 0:
            await asyncio.sleep(0.1)
            continue
        count = min(capacity, settings.worker_read_batch)

        await _reclaim_stale(aredis, stream, group, worker_id, count, spawn)

        try:
            resp = await _observe_redis(
                "xreadgroup",
                aredis.xreadgroup(
                    group, worker_id, {stream: ">"}, count=count, block=settings.worker_block_ms
                ),
            )
        except Exception:  # noqa: BLE001
            logger.exception("worker: xreadgroup failed")
            await asyncio.sleep(1)
            continue

        for _stream_name, messages in resp or []:
            for message_id, fields in messages:
                run_id = _parse_run_id(fields)
                if run_id is None:
                    await _observe_redis("xack", aredis.xack(stream, group, message_id))
                    await _observe_redis("xdel", aredis.xdel(stream, message_id))
                    continue
                spawn(message_id, run_id, allow_running=False)

    logger.info("worker %s draining %s in-flight run(s)", worker_id, len(inflight))
    if inflight:
        await asyncio.gather(*inflight, return_exceptions=True)
    await aredis.aclose()
    logger.info("worker %s stopped", worker_id)


async def _reclaim_stale(aredis, stream, group, worker_id, count, spawn) -> None:
    """Reclaim messages abandoned by dead/stalled workers (failover)."""
    try:
        reply = await _observe_redis(
            "xautoclaim",
            aredis.xautoclaim(
                stream, group, worker_id,
                min_idle_time=settings.claim_min_idle_ms, start_id="0-0", count=count,
            ),
        )
    except Exception:  # noqa: BLE001 - group may not exist yet, or older server
        return
    # redis-py returns (cursor, messages) or (cursor, messages, deleted) by version.
    messages = reply[1] if len(reply) >= 2 else []
    for message_id, fields in messages:
        run_id = _parse_run_id(fields)
        if run_id is None:
            await _observe_redis("xack", aredis.xack(stream, group, message_id))
            await _observe_redis("xdel", aredis.xdel(stream, message_id))
            continue
        RUNS_RECLAIMED.inc()
        logger.info("worker: reclaimed stale message %s (run_id=%s)", message_id, run_id)
        spawn(message_id, run_id, allow_running=True)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
