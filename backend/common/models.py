from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    email: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    owner_user_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # http | shell | http_async
    task_type: Mapped[str] = mapped_column(String(32), nullable=False)
    # Free-form spec; shape depends on task_type (url/method/headers/body or command/args).
    task_spec: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # cron | interval | manual
    schedule_type: Mapped[str] = mapped_column(String(16), nullable=False, default="manual")
    # cron expression (5-field) or interval seconds (as string) depending on schedule_type
    schedule_expr: Mapped[str | None] = mapped_column(String(255), nullable=True)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, default="UTC")

    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    retry_backoff_sec: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    timeout_sec: Mapped[int] = mapped_column(Integer, nullable=False, default=300)

    # Bookkeeping for the scheduler so it does not re-enqueue the same tick.
    last_scheduled_for: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )

    owner: Mapped[User | None] = relationship()

    runs: Mapped[list["JobRun"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )
    dependencies: Mapped[list["JobDependency"]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
        foreign_keys="JobDependency.job_id",
    )


class JobDependency(Base):
    """job_id runs only after depends_on_job_id has succeeded."""

    __tablename__ = "job_dependencies"
    __table_args__ = (
        UniqueConstraint("job_id", "depends_on_job_id", name="uq_job_dependency"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    depends_on_job_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )

    job: Mapped["Job"] = relationship(back_populates="dependencies", foreign_keys=[job_id])


class JobRun(Base):
    __tablename__ = "job_runs"
    __table_args__ = (
        # Guarantees a single scheduled tick cannot be enqueued twice.
        UniqueConstraint("job_id", "scheduled_for", name="uq_job_run_tick"),
        Index("ix_job_runs_status", "status"),
        Index("ix_job_runs_job_id_created", "job_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # scheduled | manual | dependency | retry
    trigger_type: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    # The logical tick this run belongs to (null for manual/dependency runs).
    scheduled_for: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    exit_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    worker_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )

    job: Mapped["Job"] = relationship(back_populates="runs")
    logs: Mapped[list["JobRunLog"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class JobRunLog(Base):
    __tablename__ = "job_run_logs"
    __table_args__ = (Index("ix_job_run_logs_run_id_ts", "run_id", "ts"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("job_runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    stream: Mapped[str] = mapped_column(String(16), nullable=False, default="stdout")
    line: Mapped[str] = mapped_column(Text, nullable=False)

    run: Mapped["JobRun"] = relationship(back_populates="logs")
