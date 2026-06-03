from __future__ import annotations

from datetime import datetime

import redis
from sqlalchemy.orm import Session

from backend.common.constants import RunStatus, TriggerType
from backend.common.logging import get_logger
from backend.common.metrics import JOBS_ENQUEUED
from backend.common.models import JobRun
from backend.common.redis_queue import enqueue_run

logger = get_logger(__name__)


def create_run(
    session: Session,
    job_id: int,
    trigger_type: str,
    *,
    scheduled_for: datetime | None = None,
    attempt: int = 1,
    status: str = RunStatus.QUEUED,
) -> JobRun:
    """Create a queued run row. Caller is responsible for committing."""
    run = JobRun(
        job_id=job_id,
        trigger_type=trigger_type,
        status=status,
        attempt=attempt,
        scheduled_for=scheduled_for,
    )
    session.add(run)
    session.flush()  # assign run.id
    return run


def publish(client: redis.Redis, run: JobRun) -> None:
    """Push an already-persisted run onto the dispatch stream."""
    enqueue_run(client, run.id)
    JOBS_ENQUEUED.labels(trigger_type=run.trigger_type).inc()
    logger.info("enqueued run_id=%s job_id=%s trigger=%s", run.id, run.job_id, run.trigger_type)


def create_and_publish(
    session: Session,
    client: redis.Redis,
    job_id: int,
    trigger_type: str = TriggerType.MANUAL,
    *,
    attempt: int = 1,
) -> JobRun:
    run = create_run(session, job_id, trigger_type, attempt=attempt)
    session.commit()
    publish(client, run)
    return run
