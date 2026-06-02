"""Execute a single claimed run end-to-end.

Ties together: idempotent claim -> executor dispatch -> log streaming ->
terminal state -> retry scheduling (delayed) and dependency fan-out.
"""

from __future__ import annotations

import asyncio
import time

import redis.asyncio as aioredis

from backend.common.constants import RunStatus
from backend.common.logging import get_logger
from backend.common.metrics import RUN_DURATION, RUNS_PROCESSED
from backend.common.redis_queue import enqueue_run_async, schedule_delayed_async
from backend.worker import store
from backend.worker.executors import ExecResult, get_executor
from backend.worker.executors.retry import backoff_seconds, should_retry

logger = get_logger(__name__)

_FLUSH_EVERY = 25
_RETRYABLE = {RunStatus.FAILED, RunStatus.TIMED_OUT}


class LogBuffer:
    """Buffers log lines and flushes them to the DB in batches."""

    def __init__(self, run_id: int) -> None:
        self._run_id = run_id
        self._buf: list[tuple[str, str]] = []

    async def write(self, stream: str, line: str) -> None:
        self._buf.append((stream, line))
        if len(self._buf) >= _FLUSH_EVERY:
            await self.flush()

    async def flush(self) -> None:
        if not self._buf:
            return
        batch, self._buf = self._buf, []
        await asyncio.to_thread(store.append_logs, self._run_id, batch)


async def process_run(
    aredis: aioredis.Redis, run_id: int, *, allow_running: bool, worker_id: str
) -> None:
    claimed = await asyncio.to_thread(
        store.claim_run, run_id, worker_id, allow_running=allow_running
    )
    if claimed is None:
        logger.debug("run_id=%s already claimed/terminal, skipping", run_id)
        return

    task_type = claimed["task_type"]
    logs = LogBuffer(run_id)
    started = time.monotonic()

    try:
        executor = get_executor(task_type)
        result = await executor(
            claimed["task_spec"], timeout_sec=claimed["timeout_sec"], log=logs.write
        )
    except asyncio.CancelledError:
        await logs.write("system", "run canceled; stopping executor")
        await logs.flush()
        RUN_DURATION.labels(task_type=task_type).observe(time.monotonic() - started)
        RUNS_PROCESSED.labels(status=RunStatus.CANCELED, task_type=task_type).inc()
        raise
    except Exception as exc:  # noqa: BLE001 - executor must never crash the worker
        logger.exception("run_id=%s executor crashed", run_id)
        await logs.write("system", f"executor crashed: {exc}")
        result = ExecResult.failed(f"executor crashed: {exc}")

    await logs.flush()
    RUN_DURATION.labels(task_type=task_type).observe(time.monotonic() - started)

    if result.succeeded:
        await _on_success(aredis, claimed, result)
    else:
        await _on_failure(aredis, claimed, result)

    RUNS_PROCESSED.labels(status=result.status, task_type=task_type).inc()


async def _on_success(aredis: aioredis.Redis, claimed: dict, result: ExecResult) -> None:
    run_id, job_id = claimed["run_id"], claimed["job_id"]
    if await asyncio.to_thread(store.is_run_canceled, run_id):
        logger.info("run_id=%s was canceled; skipping success side effects", run_id)
        return
    await asyncio.to_thread(
        store.finish_run, run_id, RunStatus.SUCCEEDED,
        exit_code=result.exit_code, result=result.result,
    )
    logger.info("run_id=%s job_id=%s succeeded", run_id, job_id)

    dependents = await asyncio.to_thread(store.dependents_ready, job_id)
    for dep_job_id in dependents:
        new_id = await asyncio.to_thread(store.create_dependency_run, dep_job_id)
        if new_id is not None:
            await enqueue_run_async(aredis, new_id)
            logger.info("run_id=%s triggered dependent job_id=%s as run_id=%s",
                        run_id, dep_job_id, new_id)


async def _on_failure(aredis: aioredis.Redis, claimed: dict, result: ExecResult) -> None:
    run_id, job_id = claimed["run_id"], claimed["job_id"]
    if await asyncio.to_thread(store.is_run_canceled, run_id):
        logger.info("run_id=%s was canceled; skipping failure side effects", run_id)
        return
    attempt, max_retries = claimed["attempt"], claimed["max_retries"]

    if result.status in _RETRYABLE and should_retry(attempt, max_retries):
        delay = backoff_seconds(attempt, claimed["retry_backoff_sec"])
        new_id = await asyncio.to_thread(store.create_retry_run, job_id, attempt + 1)
        await schedule_delayed_async(aredis, new_id, time.time() + delay)
        await asyncio.to_thread(
            store.finish_run, run_id, result.status,
            exit_code=result.exit_code, result=result.result,
            error=f"{result.error} (retry {attempt + 1}/{max_retries} scheduled in {delay}s "
                  f"as run {new_id})",
        )
        logger.warning("run_id=%s failed (%s); retry %s scheduled in %ss as run_id=%s",
                       run_id, result.status, attempt + 1, delay, new_id)
    else:
        await asyncio.to_thread(
            store.finish_run, run_id, result.status,
            exit_code=result.exit_code, result=result.result, error=result.error,
        )
        logger.warning("run_id=%s job_id=%s final status=%s: %s",
                       run_id, job_id, result.status, result.error)
