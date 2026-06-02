from __future__ import annotations

from fastapi.testclient import TestClient

from backend.api.deps import get_current_user
from backend.api.main import app


class _User:
    id = 1
    username = "draft-user"
    is_active = True


def test_create_job_draft_from_uploaded_text_file():
    app.dependency_overrides[get_current_user] = lambda: _User()
    try:
        client = TestClient(app)
        response = client.post(
            "/api/v1/jobs/drafts/from-file",
            files={"file": ("nightly.sh", b"echo hello\n", "text/x-shellscript")},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["name"] == "nightly"
    assert payload["source_filename"] == "nightly.sh"
    assert payload["file_content"] == "echo hello\n"
    assert payload["task_type"] == "shell"
    assert payload["task_spec"] == {"command": "sh", "args": ["-c", "echo hello\n"]}


def test_create_job_draft_rejects_binary_file():
    app.dependency_overrides[get_current_user] = lambda: _User()
    try:
        client = TestClient(app)
        response = client.post(
            "/api/v1/jobs/drafts/from-file",
            files={"file": ("bad.bin", b"\xff\xfe", "application/octet-stream")},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 400
    assert response.json()["detail"] == "uploaded file must be UTF-8 text"
