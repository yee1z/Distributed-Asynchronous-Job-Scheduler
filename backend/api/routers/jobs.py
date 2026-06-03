from __future__ import annotations

from pathlib import Path

import redis
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.api import crud
from backend.api.deps import get_current_user, get_redis
from backend.common.constants import RunStatus, TriggerType
from backend.common.db import get_session
from backend.common.dispatch import create_run, publish
from backend.common.models import Job, JobDependency, JobRun, JobRunLog, User
from backend.common.schemas import (
    JobCreate,
    JobDraftOut,
    JobOut,
    JobRecentOut,
    JobRunLogOut,
    JobRunOut,
    JobUpdate,
)

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])


_MAX_DRAFT_FILE_BYTES = 128 * 1024


def _job_name_from_filename(filename: str | None) -> str:
    stem = Path(filename or "uploaded-task").stem.strip()
    return stem[:255] or "uploaded-task"


def _decode_text_file(data: bytes) -> str:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="uploaded file must be UTF-8 text",
        ) from exc


def _draft_from_text(filename: str | None, content: str) -> JobDraftOut:
    return JobDraftOut(
        name=_job_name_from_filename(filename),
        description=f"Draft imported from {filename}" if filename else "Draft imported from text file",
        category=None,
        task_type="shell",
        task_spec={"command": "sh", "args": ["-c", content]},
        source_filename=filename,
        file_content=content,
    )


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


@router.get("/recent", response_model=list[JobRecentOut])
def list_recent_jobs(
    limit: int = Query(default=10, ge=1, le=50),
    session: Session = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> list[JobRecentOut]:
    jobs = list(
        session.execute(
            select(Job)
            .where(Job.owner_user_id == current_user.id)
            .order_by(Job.created_at.desc())
            .limit(limit)
        ).scalars()
    )
    recent: list[JobRecentOut] = []
    for job in jobs:
        latest_run = session.execute(
            select(JobRun)
            .where(JobRun.job_id == job.id)
            .order_by(JobRun.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        recent.append(
            JobRecentOut(
                job=JobOut.model_validate(crud.job_to_dict(session, job)),
                latest_run=JobRunOut.model_validate(latest_run) if latest_run is not None else None,
            )
        )
    return recent


@router.post("/drafts/from-file", response_model=JobDraftOut)
async def create_job_draft_from_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
) -> JobDraftOut:
    # current_user is intentionally required so uploaded scripts are never accepted anonymously.
    _ = current_user
    data = await file.read(_MAX_DRAFT_FILE_BYTES + 1)
    if len(data) > _MAX_DRAFT_FILE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"uploaded file must be <= {_MAX_DRAFT_FILE_BYTES} bytes",
        )
    content = _decode_text_file(data)
    return _draft_from_text(file.filename, content)


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
    """Manually trigger a job, queuing unmet dependencies first when needed."""
    job = crud.get_job(session, job_id, owner_user_id=current_user.id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    runs_to_publish: list[JobRun] = []
    run = _prepare_trigger_run(session, job_id, TriggerType.MANUAL, runs_to_publish, [])
    session.commit()
    session.refresh(run)
    for queued_run in runs_to_publish:
        publish(client, queued_run)
    return run


def _prepare_trigger_run(
    session: Session,
    job_id: int,
    trigger_type: str,
    runs_to_publish: list[JobRun],
    stack: list[int],
) -> JobRun:
    if job_id in stack:
        cycle = " -> ".join(str(node) for node in [*stack, job_id])
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"job dependency cycle detected: {cycle}",
        )

    dependency_ids = _dependency_ids(session, job_id)
    if not dependency_ids:
        run = create_run(session, job_id, trigger_type)
        runs_to_publish.append(run)
        return run

    for dep_id in dependency_ids:
        if _active_run(session, dep_id) is None:
            _prepare_trigger_run(
                session, dep_id, TriggerType.DEPENDENCY, runs_to_publish, [*stack, job_id]
            )

    existing = _active_run(session, job_id)
    if existing is not None:
        return existing
    return create_run(session, job_id, trigger_type, status=RunStatus.PENDING)


def _dependency_ids(session: Session, job_id: int) -> list[int]:
    return list(
        session.execute(
            select(JobDependency.depends_on_job_id).where(JobDependency.job_id == job_id)
        ).scalars()
    )


def _active_run(session: Session, job_id: int) -> JobRun | None:
    return session.execute(
        select(JobRun)
        .where(
            JobRun.job_id == job_id,
            JobRun.status.in_([RunStatus.PENDING, RunStatus.QUEUED, RunStatus.RUNNING]),
        )
        .order_by(JobRun.created_at, JobRun.id)
        .limit(1)
    ).scalar_one_or_none()


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
