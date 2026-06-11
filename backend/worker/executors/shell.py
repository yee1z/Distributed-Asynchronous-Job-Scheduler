"""Shell command executor.

task_spec:
  command  (required)  the program to run
  args     (list[str], optional)
  env      (dict[str,str], optional)  extra environment variables
  cwd      (str, optional)            working directory
  source_filename/file_content optional uploaded script payload to materialize

stdout/stderr are streamed line-by-line into job_run_logs. On timeout the
process group is terminated.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import shlex
import tempfile
from typing import Any

from backend.worker.executors.base import ExecResult, LogFn


async def _pump(stream: asyncio.StreamReader, name: str, log: LogFn) -> None:
    while True:
        raw = await stream.readline()
        if not raw:
            break
        await log(name, raw.decode(errors="replace").rstrip("\n"))


async def execute(spec: dict[str, Any], *, timeout_sec: int, log: LogFn) -> ExecResult:
    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    try:
        argv = _build_argv(spec)
        temp_dir, argv = _materialize_uploaded_script(spec, argv)
    except ValueError as exc:
        return ExecResult.failed(f"failed to parse command: {exc}")

    env = {**os.environ, **{str(k): str(v) for k, v in (spec.get("env") or {}).items()}}
    cwd = spec.get("cwd")

    await log("system", f"$ {shlex.join(argv)}")
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=cwd,
            start_new_session=True,  # isolate in its own process group for clean kill
        )
    except (FileNotFoundError, PermissionError, OSError) as exc:
        if temp_dir is not None:
            temp_dir.cleanup()
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
        if temp_dir is not None:
            temp_dir.cleanup()

    code = proc.returncode
    if code == 0:
        return ExecResult.ok(exit_code=0, result={"exit_code": 0})
    return ExecResult.failed(
        f"command exited with code {code}", exit_code=code, result={"exit_code": code}
    )


def _build_argv(spec: dict[str, Any]) -> list[str]:
    command = str(spec["command"])
    args = [str(a) for a in (spec.get("args") or [])]
    argv = [*shlex.split(command), *args]
    if not argv:
        raise ValueError("command is empty")
    return argv


def _materialize_uploaded_script(
    spec: dict[str, Any], argv: list[str]
) -> tuple[tempfile.TemporaryDirectory[str] | None, list[str]]:
    content = spec.get("file_content")
    source_filename = spec.get("source_filename")
    if content is None or source_filename is None:
        return None, argv

    filename = Path(str(source_filename)).name
    if not filename or filename in {".", ".."}:
        raise ValueError("source_filename is invalid")

    temp_dir = tempfile.TemporaryDirectory(prefix="job-scheduler-script-")
    script_path = Path(temp_dir.name) / filename
    script_path.write_text(str(content), encoding="utf-8")

    replaced = False
    rewritten: list[str] = []
    for arg in argv:
        if Path(arg).name == filename:
            rewritten.append(str(script_path))
            replaced = True
        else:
            rewritten.append(arg)

    if not replaced:
        rewritten.append(str(script_path))
    return temp_dir, rewritten


def _terminate(proc: asyncio.subprocess.Process) -> None:
    try:
        os.killpg(os.getpgid(proc.pid), 9)
    except (ProcessLookupError, PermissionError):
        try:
            proc.kill()
        except ProcessLookupError:
            pass
