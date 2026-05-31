"""Single HTTP request executor.

task_spec:
  url        (required)         target URL
  method     (default "GET")
  headers    (dict, optional)
  body       (json-serialisable, optional)  sent as JSON
  expect     (list[int], optional)           status codes treated as success
                                             (default: any 2xx)
"""

from __future__ import annotations

from typing import Any

import httpx

from backend.worker.executors.base import ExecResult, LogFn


def _is_success(status_code: int, expect: list[int] | None) -> bool:
    if expect:
        return status_code in expect
    return 200 <= status_code < 300


async def execute(spec: dict[str, Any], *, timeout_sec: int, log: LogFn) -> ExecResult:
    url = spec["url"]
    method = str(spec.get("method", "GET")).upper()
    headers = spec.get("headers") or {}
    body = spec.get("body")
    expect = spec.get("expect")

    await log("system", f"HTTP {method} {url}")
    try:
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            resp = await client.request(
                method, url, headers=headers, json=body if body is not None else None
            )
    except httpx.TimeoutException:
        return ExecResult.timed_out(f"request to {url} timed out after {timeout_sec}s")
    except httpx.HTTPError as exc:
        return ExecResult.failed(f"request error: {exc}")

    snippet = resp.text[:2000]
    await log("stdout", f"status={resp.status_code}")
    if snippet:
        await log("stdout", snippet)

    result = {"status_code": resp.status_code, "body": snippet}
    if _is_success(resp.status_code, expect):
        return ExecResult.ok(exit_code=0, result=result)
    return ExecResult.failed(
        f"unexpected status {resp.status_code}", exit_code=resp.status_code, result=result
    )
