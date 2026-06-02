from __future__ import annotations

import os
import uuid

import httpx
import pytest

API_BASE = os.environ.get("API_BASE", "http://localhost:8000")
# Target URL the *worker* uses for http-task tests. In docker compose the worker
# reaches the API via the service name; override for other topologies.
HTTP_TARGET = os.environ.get("HTTP_TARGET", "http://api:8000/healthz")


@pytest.fixture(scope="session")
def api_base() -> str:
    return API_BASE


@pytest.fixture()
def client(api_base: str, require_stack):
    with httpx.Client(base_url=f"{api_base}/api/v1", timeout=10) as c:
        username = f"itest-{uuid.uuid4().hex[:12]}"
        register = c.post(
            "/auth/register",
            json={"username": username, "password": "integration-password"},
        )
        register.raise_for_status()
        token = register.json()["access_token"]
        c.headers.update({"Authorization": f"Bearer {token}"})
        yield c


@pytest.fixture()
def require_stack(api_base: str):
    """Skip the test unless the API (and its dependencies) are reachable."""
    try:
        r = httpx.get(f"{api_base}/readyz", timeout=3)
        if r.status_code != 200:
            pytest.skip(f"stack not ready: {r.text}")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"stack not reachable at {api_base}: {exc}")
