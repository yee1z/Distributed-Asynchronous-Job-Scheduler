from __future__ import annotations

import redis
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.api.deps import get_redis
from backend.common.constants import RunStatus, TriggerType
from backend.common.db import get_session
from backend.common.dispatch import create_run, publish
from backend.common.models import Job, JobRun, JobRunLog
from backend.common.schemas import JobRunLogOut, JobRunOut

router = APIRouter(prefix="/api/v1/runs", tags=["runs"])


@router.get("", response_model=list[JobRunOut])
def list_runs(
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> list[JobRun]:
    stmt = select(JobRun)
    if status_filter is not None:
        stmt = stmt.where(JobRun.status == status_filter)
    stmt = stmt.order_by(JobRun.created_at.desc()).limit(limit).offset(offset)
    return list(session.execute(stmt).scalars())


@router.get("/{run_id}", response_model=JobRunOut)
def get_run(run_id: int, session: Session = Depends(get_session)) -> JobRun:
    run = session.get(JobRun, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="run not found")
    return run


@router.get("/{run_id}/logs", response_model=list[JobRunLogOut])
def get_run_logs(
    run_id: int,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> list[JobRunLog]:
    run = session.get(JobRun, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="run not found")
    stmt = (
        select(JobRunLog)
        .where(JobRunLog.run_id == run_id)
        .order_by(JobRunLog.ts, JobRunLog.id)
        .limit(limit)
        .offset(offset)
    )
    return list(session.execute(stmt).scalars())


@router.post("/{run_id}/retry", response_model=JobRunOut, status_code=status.HTTP_202_ACCEPTED)
def retry_run(
    run_id: int,
    session: Session = Depends(get_session),
    client: redis.Redis = Depends(get_redis),
) -> JobRun:
    """Create a new run that retries a previous (terminal) run of the same job."""
    run = session.get(JobRun, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="run not found")
    if run.status not in RunStatus.TERMINAL:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"run is not in a terminal state (current: {run.status})",
        )
    job = session.get(Job, run.job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")

    new_run = create_run(session, run.job_id, TriggerType.RETRY, attempt=run.attempt + 1)
    session.commit()
    session.refresh(new_run)
    publish(client, new_run)
    return new_run
