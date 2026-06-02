"""Shell command executor.

task_spec:
  command  (required)  the program to run
  args     (list[str], optional)
  env      (dict[str,str], optional)  extra environment variables
  cwd      (str, optional)            working directory

stdout/stderr are streamed line-by-line into job_run_logs. On timeout the
process group is terminated.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from backend.worker.executors.base import ExecResult, LogFn


async def _pump(stream: asyncio.StreamReader, name: str, log: LogFn) -> None:
    while True:
        raw = await stream.readline()
        if not raw:
            break
        await log(name, raw.decode(errors="replace").rstrip("\n"))


async def execute(spec: dict[str, Any], *, timeout_sec: int, log: LogFn) -> ExecResult:
    command = spec["command"]
    args = [str(a) for a in (spec.get("args") or [])]
    env = {**os.environ, **{str(k): str(v) for k, v in (spec.get("env") or {}).items()}}
    cwd = spec.get("cwd")

    await log("system", f"$ {command} {' '.join(args)}".rstrip())
    try:
        proc = await asyncio.create_subprocess_exec(
            command, *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=cwd,
            start_new_session=True,  # isolate in its own process group for clean kill
        )
    except (FileNotFoundError, PermissionError, OSError) as exc:
        return ExecResult.failed(f"failed to start process: {exc}")

    pumps = asyncio.gather(
        _pump(proc.stdout, "stdout", log),
        _pump(proc.stderr, "stderr", log),
    )
    try:
        await asyncio.wait_for(proc.wait(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        _terminate(proc)
        await asyncio.gather(pumps, return_exceptions=True)
        return ExecResult.timed_out(
            f"command exceeded timeout of {timeout_sec}s", exit_code=proc.returncode
        )
    except asyncio.CancelledError:
        await log("system", "cancellation requested; terminating process")
        _terminate(proc)
        await asyncio.gather(pumps, return_exceptions=True)
        raise
    finally:
        if not pumps.done():
            await asyncio.gather(pumps, return_exceptions=True)

    code = proc.returncode
    if code == 0:
        return ExecResult.ok(exit_code=0, result={"exit_code": 0})
    return ExecResult.failed(f"command exited with code {code}", exit_code=code,
                             result={"exit_code": code})


def _terminate(proc: asyncio.subprocess.Process) -> None:
    try:
        os.killpg(os.getpgid(proc.pid), 9)
    except (ProcessLookupError, PermissionError):
        try:
            proc.kill()
        except ProcessLookupError:
            pass
