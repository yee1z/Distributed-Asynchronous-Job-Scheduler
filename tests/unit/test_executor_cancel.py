from __future__ import annotations

import asyncio

import pytest

from backend.worker.executors import shell


@pytest.mark.asyncio
async def test_shell_executor_cancellation_terminates_process_quickly():
    logs: list[tuple[str, str]] = []

    async def log(stream: str, line: str) -> None:
        logs.append((stream, line))

    task = asyncio.create_task(
        shell.execute(
            {"command": "sh", "args": ["-c", "echo started; sleep 30; echo done"]},
            timeout_sec=60,
            log=log,
        )
    )
    await asyncio.sleep(0.2)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=3)

    assert ("system", "cancellation requested; terminating process") in logs
    assert not any(line == "done" for stream, line in logs if stream == "stdout")
