from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.common.models import Job, JobDependency
from backend.common.schemas import JobBase, JobCreate, JobUpdate


class CrudError(Exception):
    """Raised on validation/business-rule violations in CRUD operations."""


def _dependency_ids(session: Session, job_id: int) -> list[int]:
    rows = session.execute(
        select(JobDependency.depends_on_job_id).where(JobDependency.job_id == job_id)
    ).scalars()
    return list(rows)


def _validate_dependencies(
    session: Session, job_id: int | None, owner_user_id: int, depends_on: list[int]
) -> None:
    unique = set(depends_on)
    if job_id is not None and job_id in unique:
        raise CrudError("a job cannot depend on itself")
    if unique:
        found = session.execute(
            select(Job.id).where(Job.id.in_(unique), Job.owner_user_id == owner_user_id)
        ).scalars().all()
        missing = unique - set(found)
        if missing:
            raise CrudError(f"unknown dependency job ids: {sorted(missing)}")


def _set_dependencies(session: Session, job: Job, depends_on: list[int]) -> None:
    session.query(JobDependency).filter(JobDependency.job_id == job.id).delete()
    for dep_id in dict.fromkeys(depends_on):  # de-dupe, keep order
        session.add(JobDependency(job_id=job.id, depends_on_job_id=dep_id))


def job_to_dict(session: Session, job: Job) -> dict:
    data = {
        "id": job.id,
        "owner_user_id": job.owner_user_id,
        "name": job.name,
        "description": job.description,
        "task_type": job.task_type,
        "task_spec": job.task_spec,
        "schedule_type": job.schedule_type,
        "schedule_expr": job.schedule_expr,
        "timezone": job.timezone,
        "enabled": job.enabled,
        "max_retries": job.max_retries,
        "retry_backoff_sec": job.retry_backoff_sec,
        "timeout_sec": job.timeout_sec,
        "depends_on": _dependency_ids(session, job.id),
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }
    return data


def create_job(session: Session, payload: JobCreate, *, owner_user_id: int) -> Job:
    existing = session.execute(
        select(Job).where(Job.owner_user_id == owner_user_id, Job.name == payload.name)
    ).scalar_one_or_none()
    if existing is not None:
        raise CrudError(f"job name '{payload.name}' already exists")
    _validate_dependencies(session, None, owner_user_id, payload.depends_on)

    job = Job(
        owner_user_id=owner_user_id,
        name=payload.name,
        description=payload.description,
        task_type=payload.task_type,
        task_spec=payload.task_spec,
        schedule_type=payload.schedule_type,
        schedule_expr=payload.schedule_expr,
        timezone=payload.timezone,
        enabled=payload.enabled,
        max_retries=payload.max_retries,
        retry_backoff_sec=payload.retry_backoff_sec,
        timeout_sec=payload.timeout_sec,
    )
    session.add(job)
    session.flush()
    _set_dependencies(session, job, payload.depends_on)
    session.commit()
    session.refresh(job)
    return job


def get_job(session: Session, job_id: int, *, owner_user_id: int) -> Job | None:
    return session.execute(
        select(Job).where(Job.id == job_id, Job.owner_user_id == owner_user_id)
    ).scalar_one_or_none()


def list_jobs(
    session: Session, *, owner_user_id: int, enabled: bool | None, limit: int, offset: int
) -> list[Job]:
    stmt = select(Job).where(Job.owner_user_id == owner_user_id).order_by(Job.id)
    if enabled is not None:
        stmt = stmt.where(Job.enabled == enabled)
    stmt = stmt.limit(limit).offset(offset)
    return list(session.execute(stmt).scalars())


def update_job(session: Session, job: Job, payload: JobUpdate) -> Job:
    data = payload.model_dump(exclude_unset=True)
    depends_on = data.pop("depends_on", None)
    if depends_on is not None:
        _validate_dependencies(session, job.id, job.owner_user_id, depends_on)

    merged = {
        "name": job.name,
        "description": job.description,
        "task_type": job.task_type,
        "task_spec": job.task_spec,
        "schedule_type": job.schedule_type,
        "schedule_expr": job.schedule_expr,
        "timezone": job.timezone,
        "enabled": job.enabled,
        "max_retries": job.max_retries,
        "retry_backoff_sec": job.retry_backoff_sec,
        "timeout_sec": job.timeout_sec,
    }
    merged.update({k: v for k, v in data.items() if k in merged})
    # Re-run the cross-field validation (task_spec vs task_type, schedule_expr, ...).
    try:
        JobBase(**merged, depends_on=depends_on or [])
    except ValueError as exc:
        raise CrudError(str(exc)) from exc

    for key, value in data.items():
        setattr(job, key, value)
    session.flush()
    if depends_on is not None:
        _set_dependencies(session, job, depends_on)
    session.commit()
    session.refresh(job)
    return job


def delete_job(session: Session, job: Job) -> None:
    session.delete(job)
    session.commit()
