"""Synchronous database operations used by the worker.

These run inside ``asyncio.to_thread`` from the async worker loop so blocking
SQLAlchemy calls never stall the event loop. Each function owns a short
transaction and returns plain data (never live ORM objects) to stay
thread-safe.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import insert, select, update
from sqlalchemy.orm import Session

from backend.common.constants import RunStatus, TriggerType
from backend.common.db import session_scope
from backend.common.models import Job, JobDependency, JobRun, JobRunLog

_ACTIVE = (RunStatus.PENDING, RunStatus.QUEUED, RunStatus.RUNNING)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def claim_run(run_id: int, worker_id: str, *, allow_running: bool) -> dict | None:
    """Atomically transition a run to RUNNING and return everything needed to execute it.

    Returns ``None`` if the run no longer exists or was already claimed by
    someone else / finished. The conditional UPDATE is the core idempotency
    guard: only one worker can move a given run out of the queued state.

    ``allow_running=True`` (used for reclaimed/stale messages) also re-claims
    runs stuck in RUNNING, so a run whose worker died still completes.
    """
    allowed = list(_ACTIVE) if allow_running else [RunStatus.PENDING, RunStatus.QUEUED]
    with session_scope() as session:
        result = session.execute(
            update(JobRun)
            .where(JobRun.id == run_id, JobRun.status.in_(allowed))
            .values(status=RunStatus.RUNNING, started_at=_now(), worker_id=worker_id)
        )
        if result.rowcount == 0:
            return None

        run = session.get(JobRun, run_id)
        job = session.get(Job, run.job_id)
        if job is None:
            return None
        return {
            "run_id": run.id,
            "job_id": run.job_id,
            "attempt": run.attempt,
            "task_type": job.task_type,
            "task_spec": dict(job.task_spec or {}),
            "timeout_sec": job.timeout_sec,
            "max_retries": job.max_retries,
            "retry_backoff_sec": job.retry_backoff_sec,
            "job_name": job.name,
        }


def append_logs(run_id: int, entries: list[tuple[str, str]]) -> None:
    if not entries:
        return
    rows = [
        {"run_id": run_id, "ts": _now(), "stream": stream, "line": line}
        for stream, line in entries
    ]
    with session_scope() as session:
        session.execute(insert(JobRunLog), rows)


def finish_run(
    run_id: int,
    status: str,
    *,
    exit_code: int | None = None,
    result: dict | None = None,
    error: str | None = None,
) -> None:
    with session_scope() as session:
        session.execute(
            update(JobRun)
            .where(JobRun.id == run_id)
            .values(
                status=status,
                finished_at=_now(),
                exit_code=exit_code,
                result=result,
                error=error,
            )
        )


def create_retry_run(job_id: int, attempt: int) -> int:
    with session_scope() as session:
        run = JobRun(
            job_id=job_id,
            trigger_type=TriggerType.RETRY,
            status=RunStatus.QUEUED,
            attempt=attempt,
        )
        session.add(run)
        session.flush()
        return run.id


def dependents_ready(job_id: int) -> list[int]:
    """Jobs that depend on ``job_id`` and are now ready to run.

    Ready means: enabled, every one of its dependencies has at least one
    succeeded run, and it has no run currently active (best-effort de-dupe).
    """
    ready: list[int] = []
    with session_scope() as session:
        dependent_ids = session.execute(
            select(JobDependency.job_id).where(JobDependency.depends_on_job_id == job_id)
        ).scalars().all()

        for dep_job_id in dict.fromkeys(dependent_ids):
            job = session.get(Job, dep_job_id)
            if job is None or not job.enabled:
                continue
            if _has_active_run(session, dep_job_id):
                continue
            if _all_dependencies_succeeded(session, dep_job_id):
                ready.append(dep_job_id)
    return ready


def create_dependency_run(job_id: int) -> int | None:
    """Create a dependency-triggered run unless one is already active (race guard)."""
    with session_scope() as session:
        if _has_active_run(session, job_id):
            return None
        run = JobRun(
            job_id=job_id,
            trigger_type=TriggerType.DEPENDENCY,
            status=RunStatus.QUEUED,
        )
        session.add(run)
        session.flush()
        return run.id


def _has_active_run(session: Session, job_id: int) -> bool:
    return session.execute(
        select(JobRun.id).where(JobRun.job_id == job_id, JobRun.status.in_(_ACTIVE)).limit(1)
    ).first() is not None


def _all_dependencies_succeeded(session: Session, job_id: int) -> bool:
    dep_ids = session.execute(
        select(JobDependency.depends_on_job_id).where(JobDependency.job_id == job_id)
    ).scalars().all()
    dep_ids = list(dict.fromkeys(dep_ids))
    if not dep_ids:
        return True
    succeeded = session.execute(
        select(JobRun.job_id)
        .where(JobRun.job_id.in_(dep_ids), JobRun.status == RunStatus.SUCCEEDED)
        .distinct()
    ).scalars().all()
    return set(dep_ids) == set(succeeded)
