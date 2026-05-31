"""Black-box integration tests against a running stack.

Run the full stack first, then:
    docker compose --profile full up --build      # in another shell
    PYTHONPATH=.deps pytest tests/integration -m integration

Tests skip automatically if the API is not reachable (see ``require_stack``).
"""

from __future__ import annotations

import time
import uuid

import pytest

from tests.conftest import HTTP_TARGET

pytestmark = pytest.mark.integration

TERMINAL = {"succeeded", "failed", "timed_out", "canceled"}


def _name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _poll(client, run_id: int, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    run = {}
    while time.time() < deadline:
        run = client.get(f"/runs/{run_id}").json()
        if run["status"] in TERMINAL:
            return run
        time.sleep(0.5)
    raise AssertionError(f"run {run_id} did not finish in {timeout}s (last={run.get('status')})")


def test_shell_job_runs_and_logs(client, require_stack):
    job = client.post("/jobs", json={
        "name": _name("it-shell"), "task_type": "shell",
        "task_spec": {"command": "echo", "args": ["hello-itest"]},
    }).json()
    run = client.post(f"/jobs/{job['id']}/trigger").json()
    finished = _poll(client, run["id"])
    assert finished["status"] == "succeeded"
    assert finished["exit_code"] == 0

    logs = client.get(f"/runs/{run['id']}/logs").json()
    lines = [entry["line"] for entry in logs]
    assert any("hello-itest" in line for line in lines)


def test_http_job(client, require_stack):
    job = client.post("/jobs", json={
        "name": _name("it-http"), "task_type": "http",
        "task_spec": {"url": HTTP_TARGET, "method": "GET"},
    }).json()
    run = client.post(f"/jobs/{job['id']}/trigger").json()
    finished = _poll(client, run["id"])
    assert finished["status"] == "succeeded"
    assert finished["result"]["status_code"] == 200


def test_automatic_retry(client, require_stack):
    job = client.post("/jobs", json={
        "name": _name("it-retry"), "task_type": "shell",
        "task_spec": {"command": "sh", "args": ["-c", "exit 3"]},
        "max_retries": 1, "retry_backoff_sec": 1,
    }).json()
    first = client.post(f"/jobs/{job['id']}/trigger").json()
    assert _poll(client, first["id"])["status"] == "failed"

    # The failed run should schedule exactly one retry (attempt 2), then stop.
    deadline = time.time() + 15
    runs = []
    while time.time() < deadline:
        runs = client.get(f"/jobs/{job['id']}/runs").json()
        if len(runs) >= 2 and all(r["status"] in TERMINAL for r in runs):
            break
        time.sleep(1)
    attempts = sorted(r["attempt"] for r in runs)
    assert attempts == [1, 2], runs
    assert any(r["trigger_type"] == "retry" for r in runs)


def test_manual_retry_endpoint(client, require_stack):
    job = client.post("/jobs", json={
        "name": _name("it-manual-retry"), "task_type": "shell",
        "task_spec": {"command": "sh", "args": ["-c", "exit 1"]},
    }).json()
    first = client.post(f"/jobs/{job['id']}/trigger").json()
    assert _poll(client, first["id"])["status"] == "failed"

    retry = client.post(f"/runs/{first['id']}/retry")
    assert retry.status_code == 202
    retry_run = retry.json()
    assert retry_run["id"] != first["id"]
    assert retry_run["trigger_type"] == "retry"
    assert _poll(client, retry_run["id"])["status"] == "failed"


def test_dependency_chain(client, require_stack):
    a = client.post("/jobs", json={
        "name": _name("it-dag-a"), "task_type": "shell",
        "task_spec": {"command": "echo", "args": ["upstream"]},
    }).json()
    b = client.post("/jobs", json={
        "name": _name("it-dag-b"), "task_type": "shell",
        "task_spec": {"command": "echo", "args": ["downstream"]},
        "depends_on": [a["id"]],
    }).json()

    run_a = client.post(f"/jobs/{a['id']}/trigger").json()
    assert _poll(client, run_a["id"])["status"] == "succeeded"

    deadline = time.time() + 15
    b_runs = []
    while time.time() < deadline:
        b_runs = client.get(f"/jobs/{b['id']}/runs").json()
        if b_runs:
            break
        time.sleep(1)
    assert b_runs, "downstream job B was never triggered"
    dep_run = b_runs[0]
    assert dep_run["trigger_type"] == "dependency"
    assert _poll(client, dep_run["id"])["status"] == "succeeded"


def test_interval_scheduling(client, require_stack):
    job = client.post("/jobs", json={
        "name": _name("it-interval"), "task_type": "shell",
        "task_spec": {"command": "echo", "args": ["tick"]},
        "schedule_type": "interval", "schedule_expr": "5",
    }).json()
    try:
        deadline = time.time() + 20
        scheduled = []
        while time.time() < deadline:
            runs = client.get(f"/jobs/{job['id']}/runs").json()
            scheduled = [r for r in runs if r["trigger_type"] == "scheduled"]
            if scheduled:
                break
            time.sleep(1)
        assert scheduled, "interval job never produced a scheduled run"
    finally:
        # Stop the recurring schedule so it doesn't keep firing after the test.
        client.put(f"/jobs/{job['id']}", json={"enabled": False})
