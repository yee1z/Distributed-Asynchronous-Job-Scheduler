"""Unit tests for the http_async submit-then-poll executor.

The executor fires a submission request, discovers a status URL, then polls it
until the remote task reports success/failure or the job's ``timeout_sec``
elapses. Transient poll errors are tolerated (logged and retried) until the
deadline.

No real network: ``httpx.AsyncClient`` is swapped for one wired to an
``httpx.MockTransport`` that replays a scripted sequence of responses. Polls use
a tiny ``poll_interval_sec`` so the loop never really sleeps.
"""

from __future__ import annotations

import httpx
import pytest

from backend.common.constants import RunStatus
from backend.worker.executors import http_async

SUBMIT_URL = "https://remote.test/submit"
STATUS_URL = "https://remote.test/status/1"


class FakeRemote:
    """Routes submit vs. poll by path and replays a scripted poll sequence.

    ``status_script`` is a list of ``(status_code, json_payload)``; the last
    entry repeats once exhausted (so an always-pending script drives a timeout).
    """

    def __init__(self, *, status_script, submit_status=200, submit_json=None, submit_text=None):
        self.status_script = list(status_script)
        self.submit_status = submit_status
        self.submit_text = submit_text  # if set, the submit body is non-JSON
        self.submit_json = {"status_url": STATUS_URL} if submit_json is None else submit_json
        self.submit_calls = 0
        self.status_calls = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/submit":
            self.submit_calls += 1
            if self.submit_text is not None:
                return httpx.Response(self.submit_status, text=self.submit_text)
            return httpx.Response(self.submit_status, json=self.submit_json)
        self.status_calls += 1
        idx = min(self.status_calls - 1, len(self.status_script) - 1)
        code, payload = self.status_script[idx]
        return httpx.Response(code, json=payload)


@pytest.fixture()
def wire_remote(monkeypatch):
    """Return an installer that points the executor's httpx at a FakeRemote."""
    real_async_client = httpx.AsyncClient

    def install(remote: FakeRemote) -> FakeRemote:
        def factory(*args, **kwargs):
            return real_async_client(transport=httpx.MockTransport(remote.handler), **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", factory)
        return remote

    return install


class LogSink:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []

    async def __call__(self, stream: str, line: str) -> None:
        self.entries.append((stream, line))

    def lines(self, stream: str | None = None) -> list[str]:
        return [line for s, line in self.entries if stream is None or s == stream]


def _spec(**overrides):
    spec = {"url": SUBMIT_URL, "poll_interval_sec": 0.0}
    spec.update(overrides)
    return spec


async def _run(spec, *, timeout_sec=30):
    log = LogSink()
    result = await http_async.execute(spec, timeout_sec=timeout_sec, log=log)
    return result, log


async def test_pending_then_succeeded(wire_remote):
    remote = wire_remote(FakeRemote(status_script=[
        (200, {"status": "pending"}),
        (200, {"status": "running"}),
        (200, {"status": "succeeded"}),
    ]))

    result, log = await _run(_spec())

    assert result.succeeded
    assert result.status == RunStatus.SUCCEEDED
    assert result.result == {"status": "succeeded"}
    assert remote.submit_calls == 1
    assert remote.status_calls == 3
    assert "status=succeeded" in log.lines("stdout")


async def test_status_value_is_case_insensitive(wire_remote):
    wire_remote(FakeRemote(status_script=[(200, {"status": "SUCCEEDED"})]))

    result, _ = await _run(_spec())

    assert result.succeeded
    assert result.result == {"status": "succeeded"}


async def test_remote_failure_marks_failed(wire_remote):
    wire_remote(FakeRemote(status_script=[
        (200, {"status": "pending"}),
        (200, {"status": "failed"}),
    ]))

    result, _ = await _run(_spec())

    assert result.status == RunStatus.FAILED
    assert "remote task reported 'failed'" in result.error
    assert result.result == {"status": "failed"}


async def test_result_field_is_copied_on_success(wire_remote):
    wire_remote(FakeRemote(status_script=[(200, {"status": "done", "output": {"rows": 7}})]))

    result, _ = await _run(_spec(success_values=["done"], result_field="output"))

    assert result.succeeded
    assert result.result == {"status": "done", "output": {"rows": 7}}


async def test_transient_poll_error_is_retried_then_succeeds(wire_remote):
    remote = wire_remote(FakeRemote(status_script=[
        (500, {"oops": True}),            # transient server error -> tolerated
        (200, {"status": "pending"}),
        (200, {"status": "succeeded"}),
    ]))

    result, log = await _run(_spec())

    assert result.succeeded
    assert remote.status_calls == 3
    # The 500 is logged as a retryable poll error, not a failure.
    assert any("poll error" in line for line in log.lines("stderr"))


async def test_explicit_status_url_skips_reading_from_response(wire_remote):
    # Submit response has NO status_url; spec provides it directly.
    remote = wire_remote(FakeRemote(
        status_script=[(200, {"status": "succeeded"})],
        submit_json={"accepted": True},
    ))

    result, _ = await _run(_spec(status_url=STATUS_URL))

    assert result.succeeded
    assert remote.submit_calls == 1


async def test_submission_http_error_fails_fast(wire_remote):
    remote = wire_remote(FakeRemote(status_script=[], submit_status=500))

    result, _ = await _run(_spec())

    assert result.status == RunStatus.FAILED
    assert "submission failed" in result.error
    assert remote.status_calls == 0  # never got to polling


async def test_missing_status_url_fails(wire_remote):
    # 200 submit, but neither the response nor the spec carries a status_url.
    wire_remote(FakeRemote(
        status_script=[(200, {"status": "succeeded"})],
        submit_json={"accepted": True},
    ))

    result, _ = await _run(_spec())  # no status_url in spec

    assert result.status == RunStatus.FAILED
    assert "status_url" in result.error


async def test_non_json_submit_response_without_status_url_fails(wire_remote):
    # Submit returns a non-JSON body and the spec carries no status_url, so the
    # executor cannot discover where to poll.
    wire_remote(FakeRemote(status_script=[], submit_text="<html>accepted</html>"))

    result, _ = await _run(_spec())

    assert result.status == RunStatus.FAILED
    assert "status_url" in result.error


async def test_deadline_exceeded_times_out(wire_remote):
    # Always pending -> the only possible terminal outcome is a timeout.
    remote = wire_remote(FakeRemote(status_script=[(200, {"status": "pending"})]))

    result, _ = await _run(_spec(poll_interval_sec=0.02), timeout_sec=0.2)

    assert result.status == RunStatus.TIMED_OUT
    assert result.result == {"status_url": STATUS_URL}
    assert remote.status_calls >= 1
