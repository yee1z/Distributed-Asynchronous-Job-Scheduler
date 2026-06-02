from __future__ import annotations

import pytest
from pydantic import ValidationError

from datetime import datetime, timezone

from backend.common.schemas import JobCreate, JobOut, JobRecentOut


def test_valid_shell_job():
    job = JobCreate(name="s", task_type="shell", task_spec={"command": "echo"})
    assert job.task_type == "shell"


def test_valid_http_cron_job():
    job = JobCreate(
        name="h",
        task_type="http",
        task_spec={"url": "http://x"},
        schedule_type="cron",
        schedule_expr="*/5 * * * *",
    )
    assert job.schedule_expr == "*/5 * * * *"


def test_valid_interval_job():
    job = JobCreate(
        name="i",
        task_type="shell",
        task_spec={"command": "echo"},
        schedule_type="interval",
        schedule_expr="30",
    )
    assert job.schedule_type == "interval"


def test_reject_unknown_task_type():
    with pytest.raises(ValidationError):
        JobCreate(name="x", task_type="ftp", task_spec={})


def test_reject_unknown_schedule_type():
    with pytest.raises(ValidationError):
        JobCreate(name="x", task_type="shell", task_spec={"command": "echo"},
                  schedule_type="weekly")


def test_cron_requires_valid_expression():
    with pytest.raises(ValidationError):
        JobCreate(name="x", task_type="shell", task_spec={"command": "echo"},
                  schedule_type="cron", schedule_expr="not-a-cron")


def test_interval_requires_positive_integer():
    with pytest.raises(ValidationError):
        JobCreate(name="x", task_type="shell", task_spec={"command": "echo"},
                  schedule_type="interval", schedule_expr="0")


def test_http_requires_url():
    with pytest.raises(ValidationError):
        JobCreate(name="x", task_type="http", task_spec={})


def test_shell_requires_command():
    with pytest.raises(ValidationError):
        JobCreate(name="x", task_type="shell", task_spec={})


def test_recent_job_schema_allows_missing_latest_run():
    now = datetime.now(timezone.utc)
    job = JobOut(
        id=1,
        owner_user_id=1,
        name="recent",
        description=None,
        category=None,
        task_type="shell",
        task_spec={"command": "echo"},
        schedule_type="manual",
        schedule_expr=None,
        timezone="UTC",
        enabled=True,
        max_retries=0,
        retry_backoff_sec=30,
        timeout_sec=300,
        depends_on=[],
        created_at=now,
        updated_at=now,
    )

    recent = JobRecentOut(job=job)

    assert recent.job.name == "recent"
    assert recent.latest_run is None
