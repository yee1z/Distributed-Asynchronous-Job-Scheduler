"""Scheduler process.

Responsibilities (only while holding leadership):
  1. Promote due delayed runs (retry backoff) onto the dispatch stream.
  2. Enqueue cron / interval jobs whose next tick has arrived.
  3. Re-enqueue runs stuck in ``queued`` (safety net for a lost enqueue).

Multiple replicas can run; a Postgres advisory lock ensures a single active
leader, and standbys take over automatically if the leader dies.
"""

from __future__ import annotations

import signal
import time
from datetime import datetime, timedelta, timezone

from prometheus_client import start_http_server
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.common.config import get_settings
from backend.common.constants import RunStatus, ScheduleType, TriggerType
from backend.common.db import SessionLocal, engine
from backend.common.logging import get_logger
from backend.common.metrics import QUEUE_DEPTH, SCHEDULER_IS_LEADER
from backend.common.models import Job, JobRun
from backend.common.redis_queue import (
    enqueue_run,
    ensure_group,
    get_sync_redis,
    pending_count,
    promote_due_delayed,
)
from backend.scheduler.cron import due_tick
from backend.scheduler.leader import Leader

logger = get_logger(__name__)
settings = get_settings()

_stop = False


def _handle_signal(signum, _frame) -> None:
    global _stop
    logger.info("scheduler: received signal %s, shutting down", signum)
    _stop = True


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def schedule_due_jobs(session: Session, now: datetime) -> list[int]:
    """Create runs for every cron/interval job whose next tick is due. Returns run ids."""
    enqueued: list[int] = []
    jobs = session.execute(
        select(Job).where(
            Job.enabled.is_(True),
            Job.schedule_type.in_([ScheduleType.CRON, ScheduleType.INTERVAL]),
        )
    ).scalars()

    for job in jobs:
        # Anchor on the last tick we scheduled, or the job's creation time the
        # first time we see it (using ``now`` here would push the next tick
        # perpetually into the future and the job would never fire).
        base = job.last_scheduled_for or job.created_at
        tick = due_tick(job.schedule_type, job.schedule_expr, job.timezone, base, now)
        if tick is None:
            continue

        savepoint = session.begin_nested()
        try:
            run = JobRun(
                job_id=job.id,
                trigger_type=TriggerType.SCHEDULED,
                status=RunStatus.QUEUED,
                scheduled_for=tick,
            )
            session.add(run)
            session.flush()  # assign id + hit the unique(job_id, scheduled_for) guard
            job.last_scheduled_for = tick
            savepoint.commit()
            enqueued.append(run.id)
        except IntegrityError:
            # Another leader/restart already created this exact tick — not an error.
            savepoint.rollback()
            job.last_scheduled_for = tick

    return enqueued


def requeue_orphans(session: Session, now: datetime) -> list[int]:
    """Runs left in 'queued' beyond the threshold likely lost their stream message."""
    cutoff = now - timedelta(seconds=settings.orphan_requeue_after_sec)
    rows = session.execute(
        select(JobRun.id).where(JobRun.status == RunStatus.QUEUED, JobRun.created_at < cutoff)
    ).scalars()
    return list(rows)


def tick(redis_client) -> None:
    now = _utcnow()

    promoted = promote_due_delayed(redis_client, now.timestamp())
    if promoted:
        logger.info("scheduler: promoted %s delayed run(s)", promoted)

    with SessionLocal() as session:
        try:
            scheduled = schedule_due_jobs(session, now)
            orphans = requeue_orphans(session, now)
            session.commit()
        except Exception:
            session.rollback()
            raise

    for run_id in scheduled:
        enqueue_run(redis_client, run_id)
    if scheduled:
        logger.info("scheduler: enqueued %s scheduled run(s): %s", len(scheduled), scheduled)

    for run_id in orphans:
        enqueue_run(redis_client, run_id)
    if orphans:
        logger.warning("scheduler: re-enqueued %s orphaned run(s): %s", len(orphans), orphans)

    QUEUE_DEPTH.set(pending_count(redis_client))


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    start_http_server(settings.metrics_port)
    redis_client = get_sync_redis()
    ensure_group(redis_client)

    leader = Leader(engine, settings.scheduler_lock_key)
    logger.info("scheduler: started, poll=%.1fs, metrics on :%s",
                settings.scheduler_poll_interval_sec, settings.metrics_port)

    while not _stop:
        try:
            if leader.acquire():
                SCHEDULER_IS_LEADER.set(1)
                tick(redis_client)
            else:
                SCHEDULER_IS_LEADER.set(0)
        except Exception:  # noqa: BLE001 - never let one bad tick kill the loop
            logger.exception("scheduler: tick failed")
        time.sleep(settings.scheduler_poll_interval_sec)

    leader.release()
    SCHEDULER_IS_LEADER.set(0)
    logger.info("scheduler: stopped")


if __name__ == "__main__":
    main()
