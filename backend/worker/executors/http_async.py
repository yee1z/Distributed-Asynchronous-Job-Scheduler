"""Long-running HTTP task executor (submit + poll).

Designed for jobs that kick off remote work and only finish much later. The
worker submits the request, then *polls* a status URL with ``asyncio.sleep``
between checks — so a single worker can supervise many long tasks concurrently
without tying up a thread or CPU while waiting.

task_spec:
  url               (required)   submission endpoint
  method            (default "POST")
  headers, body     (optional)
  status_url        (optional)   poll target; if omitted it is read from the
                                 submission response JSON at ``status_url_field``
  status_url_field  (default "status_url")
  status_field      (default "status")    field in the poll response
  success_values    (default ["succeeded","success","completed","done"])
  failure_values    (default ["failed","error","canceled","cancelled"])
  poll_interval_sec (default 10)
  result_field      (optional)   field copied into the run result on success

The overall deadline is the job's ``timeout_sec``.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from backend.worker.executors.base import ExecResult, LogFn

_DEFAULT_SUCCESS = ["succeeded", "success", "completed", "done"]
_DEFAULT_FAILURE = ["failed", "error", "canceled", "cancelled"]


async def execute(spec: dict[str, Any], *, timeout_sec: int, log: LogFn) -> ExecResult:
    url = spec["url"]
    method = str(spec.get("method", "POST")).upper()
    headers = spec.get("headers") or {}
    body = spec.get("body")
    status_url = spec.get("status_url")
    status_url_field = spec.get("status_url_field", "status_url")
    status_field = spec.get("status_field", "status")
    success_values = {s.lower() for s in spec.get("success_values", _DEFAULT_SUCCESS)}
    failure_values = {s.lower() for s in spec.get("failure_values", _DEFAULT_FAILURE)}
    poll_interval = float(spec.get("poll_interval_sec", 10))
    result_field = spec.get("result_field")

    deadline = time.monotonic() + timeout_sec
    # Per-request timeout stays short; the long wait happens between polls.
    request_timeout = min(30, timeout_sec)

    async with httpx.AsyncClient(timeout=request_timeout) as client:
        await log("system", f"submit {method} {url}")
        try:
            resp = await client.request(
                method, url, headers=headers, json=body if body is not None else None
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            return ExecResult.failed(f"submission failed: {exc}")

        if not status_url:
            try:
                status_url = resp.json().get(status_url_field)
            except ValueError:
                status_url = None
            if not status_url:
                return ExecResult.failed(
                    f"no status_url provided and response had no '{status_url_field}'"
                )
        await log("system", f"polling {status_url} every {poll_interval}s")

        while True:
            if time.monotonic() >= deadline:
                return ExecResult.timed_out(
                    f"polling exceeded timeout of {timeout_sec}s", result={"status_url": status_url}
                )
            try:
                poll = await client.get(status_url, headers=headers)
                poll.raise_for_status()
                payload = poll.json()
            except (httpx.HTTPError, ValueError) as exc:
                await log("stderr", f"poll error (will retry): {exc}")
                payload = {}

            state = str(payload.get(status_field, "")).lower()
            if state:
                await log("stdout", f"status={state}")
            if state in success_values:
                result = {"status": state}
                if result_field and result_field in payload:
                    result[result_field] = payload[result_field]
                return ExecResult.ok(exit_code=0, result=result)
            if state in failure_values:
                return ExecResult.failed(f"remote task reported '{state}'", result={"status": state})

            # Sleep without blocking the event loop; cap so we honour the deadline.
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                continue
            await asyncio.sleep(min(poll_interval, remaining))
