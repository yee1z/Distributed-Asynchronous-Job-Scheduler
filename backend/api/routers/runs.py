from __future__ import annotations

import redis
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.api.deps import get_current_user, get_redis
from backend.common.constants import RunStatus, TriggerType
from backend.common.db import get_session
from backend.common.dispatch import create_run, publish
from backend.common.models import Job, JobRun, JobRunLog, User
from backend.common.schemas import JobRunLogOut, JobRunOut
from backend.worker import store

router = APIRouter(prefix="/api/v1/runs", tags=["runs"])


@router.get("", response_model=list[JobRunOut])
def list_runs(
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> list[JobRun]:
    stmt = select(JobRun).join(Job).where(Job.owner_user_id == current_user.id)
    if status_filter is not None:
        stmt = stmt.where(JobRun.status == status_filter)
    stmt = stmt.order_by(JobRun.created_at.desc()).limit(limit).offset(offset)
    return list(session.execute(stmt).scalars())


@router.get("/{run_id}", response_model=JobRunOut)
def get_run(
    run_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> JobRun:
    run = _get_owned_run(session, run_id, current_user.id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="run not found")
    return run


@router.get("/{run_id}/logs", response_model=list[JobRunLogOut])
def get_run_logs(
    run_id: int,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> list[JobRunLog]:
    run = _get_owned_run(session, run_id, current_user.id)
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
    current_user: User = Depends(get_current_user),
) -> JobRun:
    """Create a new run that retries a previous (terminal) run of the same job."""
    run = _get_owned_run(session, run_id, current_user.id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="run not found")
    if run.status not in RunStatus.TERMINAL:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"run is not in a terminal state (current: {run.status})",
        )
    new_run = create_run(session, run.job_id, TriggerType.RETRY, attempt=run.attempt + 1)
    session.commit()
    session.refresh(new_run)
    publish(client, new_run)
    return new_run


@router.post("/{run_id}/cancel", response_model=JobRunOut)
def cancel_run(
    run_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> JobRun:
    """Best-effort cancellation for queued/pending/running runs."""
    run = _get_owned_run(session, run_id, current_user.id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="run not found")
    if run.status in RunStatus.TERMINAL:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"run is already terminal (current: {run.status})",
        )
    if not store.cancel_run(run_id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="run could not be canceled",
        )
    session.expire_all()
    canceled = _get_owned_run(session, run_id, current_user.id)
    if canceled is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="run not found")
    return canceled


def _get_owned_run(session: Session, run_id: int, owner_user_id: int) -> JobRun | None:
    return session.execute(
        select(JobRun).join(Job).where(JobRun.id == run_id, Job.owner_user_id == owner_user_id)
    ).scalar_one_or_none()
