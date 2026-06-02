from __future__ import annotations

import redis
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.api import crud
from backend.api.deps import get_current_user, get_redis
from backend.common.constants import TriggerType
from backend.common.db import get_session
from backend.common.dispatch import create_run, publish
from backend.common.models import JobRun, JobRunLog, User
from backend.common.schemas import JobCreate, JobOut, JobRunLogOut, JobRunOut, JobUpdate

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])


@router.post("", response_model=JobOut, status_code=status.HTTP_201_CREATED)
def create_job(
    payload: JobCreate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> dict:
    try:
        job = crud.create_job(session, payload, owner_user_id=current_user.id)
    except crud.CrudError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return crud.job_to_dict(session, job)


@router.get("", response_model=list[JobOut])
def list_jobs(
    enabled: bool | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> list[dict]:
    jobs = crud.list_jobs(
        session, owner_user_id=current_user.id, enabled=enabled, limit=limit, offset=offset
    )
    return [crud.job_to_dict(session, j) for j in jobs]


@router.get("/{job_id}", response_model=JobOut)
def get_job(
    job_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> dict:
    job = crud.get_job(session, job_id, owner_user_id=current_user.id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    return crud.job_to_dict(session, job)


@router.put("/{job_id}", response_model=JobOut)
def update_job(
    job_id: int,
    payload: JobUpdate,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> dict:
    job = crud.get_job(session, job_id, owner_user_id=current_user.id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    try:
        job = crud.update_job(session, job, payload)
    except crud.CrudError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return crud.job_to_dict(session, job)


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_job(
    job_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> None:
    job = crud.get_job(session, job_id, owner_user_id=current_user.id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    crud.delete_job(session, job)


@router.post("/{job_id}/trigger", response_model=JobRunOut, status_code=status.HTTP_202_ACCEPTED)
def trigger_job(
    job_id: int,
    session: Session = Depends(get_session),
    client: redis.Redis = Depends(get_redis),
    current_user: User = Depends(get_current_user),
) -> JobRun:
    """Manually create and enqueue an immediate run for a job."""
    job = crud.get_job(session, job_id, owner_user_id=current_user.id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    run = create_run(session, job_id, TriggerType.MANUAL)
    session.commit()
    session.refresh(run)
    publish(client, run)
    return run


@router.get("/{job_id}/latest-run", response_model=JobRunOut | None)
def get_latest_job_run(
    job_id: int,
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> JobRun | None:
    job = crud.get_job(session, job_id, owner_user_id=current_user.id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    return session.execute(
        select(JobRun).where(JobRun.job_id == job_id).order_by(JobRun.created_at.desc()).limit(1)
    ).scalar_one_or_none()


@router.get("/{job_id}/logs", response_model=list[JobRunLogOut])
def get_latest_job_run_logs(
    job_id: int,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> list[JobRunLog]:
    latest = get_latest_job_run(job_id, session=session, current_user=current_user)
    if latest is None:
        return []
    stmt = (
        select(JobRunLog)
        .where(JobRunLog.run_id == latest.id)
        .order_by(JobRunLog.ts, JobRunLog.id)
        .limit(limit)
        .offset(offset)
    )
    return list(session.execute(stmt).scalars())


@router.get("/{job_id}/runs", response_model=list[JobRunOut])
def list_job_runs(
    job_id: int,
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> list[JobRun]:
    job = crud.get_job(session, job_id, owner_user_id=current_user.id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    stmt = select(JobRun).where(JobRun.job_id == job_id)
    if status_filter is not None:
        stmt = stmt.where(JobRun.status == status_filter)
    stmt = stmt.order_by(JobRun.created_at.desc()).limit(limit).offset(offset)
    return list(session.execute(stmt).scalars())
