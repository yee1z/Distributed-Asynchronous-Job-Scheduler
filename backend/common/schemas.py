from __future__ import annotations

from datetime import datetime
from typing import Any

from croniter import croniter
from pydantic import BaseModel, Field, field_validator, model_validator

from backend.common.constants import ScheduleType, TaskType


class JobBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    task_type: str
    task_spec: dict[str, Any] = Field(default_factory=dict)
    schedule_type: str = ScheduleType.MANUAL
    schedule_expr: str | None = None
    timezone: str = "UTC"
    enabled: bool = True
    max_retries: int = Field(default=0, ge=0, le=100)
    retry_backoff_sec: int = Field(default=30, ge=0, le=86400)
    timeout_sec: int = Field(default=300, ge=1, le=86400)
    depends_on: list[int] = Field(default_factory=list)

    @field_validator("task_type")
    @classmethod
    def _validate_task_type(cls, v: str) -> str:
        if v not in TaskType.ALL:
            raise ValueError(f"task_type must be one of {sorted(TaskType.ALL)}")
        return v

    @field_validator("schedule_type")
    @classmethod
    def _validate_schedule_type(cls, v: str) -> str:
        if v not in ScheduleType.ALL:
            raise ValueError(f"schedule_type must be one of {sorted(ScheduleType.ALL)}")
        return v

    @model_validator(mode="after")
    def _validate_schedule_expr(self) -> "JobBase":
        if self.schedule_type == ScheduleType.CRON:
            if not self.schedule_expr or not croniter.is_valid(self.schedule_expr):
                raise ValueError("schedule_expr must be a valid cron expression for cron schedules")
        elif self.schedule_type == ScheduleType.INTERVAL:
            if not self.schedule_expr or not self.schedule_expr.isdigit() or int(self.schedule_expr) <= 0:
                raise ValueError("schedule_expr must be a positive integer (seconds) for interval schedules")
        self._validate_task_spec()
        return self

    def _validate_task_spec(self) -> None:
        spec = self.task_spec or {}
        if self.task_type in (TaskType.HTTP, TaskType.HTTP_ASYNC):
            if not spec.get("url"):
                raise ValueError("task_spec.url is required for http/http_async tasks")
        elif self.task_type == TaskType.SHELL:
            if not spec.get("command"):
                raise ValueError("task_spec.command is required for shell tasks")


class JobCreate(JobBase):
    pass


class JobUpdate(BaseModel):
    description: str | None = None
    task_type: str | None = None
    task_spec: dict[str, Any] | None = None
    schedule_type: str | None = None
    schedule_expr: str | None = None
    timezone: str | None = None
    enabled: bool | None = None
    max_retries: int | None = Field(default=None, ge=0, le=100)
    retry_backoff_sec: int | None = Field(default=None, ge=0, le=86400)
    timeout_sec: int | None = Field(default=None, ge=1, le=86400)
    depends_on: list[int] | None = None


class JobOut(BaseModel):
    id: int
    name: str
    description: str | None
    task_type: str
    task_spec: dict[str, Any]
    schedule_type: str
    schedule_expr: str | None
    timezone: str
    enabled: bool
    max_retries: int
    retry_backoff_sec: int
    timeout_sec: int
    depends_on: list[int] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class JobRunOut(BaseModel):
    id: int
    job_id: int
    trigger_type: str
    status: str
    attempt: int
    scheduled_for: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    exit_code: int | None
    result: dict[str, Any] | None
    error: str | None
    worker_id: str | None
    created_at: datetime

    model_config = {"from_attributes": True}


class JobRunLogOut(BaseModel):
    id: int
    run_id: int
    ts: datetime
    stream: str
    line: str

    model_config = {"from_attributes": True}
