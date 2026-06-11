from __future__ import annotations

import asyncio

import pytest

from backend.worker.executors import shell


@pytest.mark.asyncio
async def test_shell_executor_splits_quoted_command_string():
    logs: list[tuple[str, str]] = []

    async def log(stream: str, line: str) -> None:
        logs.append((stream, line))

    result = await shell.execute({"command": "echo \"HI\""}, timeout_sec=5, log=log)

    assert result.succeeded
    assert ("system", "$ echo HI") in logs
    assert ("stdout", "HI") in logs


@pytest.mark.asyncio
async def test_shell_executor_appends_args_after_split_command():
    logs: list[tuple[str, str]] = []

    async def log(stream: str, line: str) -> None:
        logs.append((stream, line))

    result = await shell.execute({"command": "echo base", "args": ["tail"]}, timeout_sec=5, log=log)

    assert result.succeeded
    assert ("stdout", "base tail") in logs


@pytest.mark.asyncio
async def test_shell_executor_materializes_uploaded_python_script():
    logs: list[tuple[str, str]] = []

    async def log(stream: str, line: str) -> None:
        logs.append((stream, line))

    result = await shell.execute(
        {
            "command": "python3",
            "args": ["beautiful.py"],
            "source_filename": "beautiful.py",
            "file_content": "print('beautiful')\n",
        },
        timeout_sec=5,
        log=log,
    )

    assert result.succeeded
    assert any(stream == "system" and "beautiful.py" in line for stream, line in logs)
    assert ("stdout", "beautiful") in logs


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
