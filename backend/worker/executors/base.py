"""Executor contract shared by all task types."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from backend.common.constants import RunStatus

# Async sink the executor uses to stream a single log line (stream, line).
LogFn = Callable[[str, str], Awaitable[None]]


@dataclass
class ExecResult:
    status: str  # RunStatus terminal value
    exit_code: int | None = None
    result: dict[str, Any] | None = None
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status == RunStatus.SUCCEEDED

    @classmethod
    def ok(cls, **kwargs: Any) -> "ExecResult":
        return cls(status=RunStatus.SUCCEEDED, **kwargs)

    @classmethod
    def failed(cls, error: str, **kwargs: Any) -> "ExecResult":
        return cls(status=RunStatus.FAILED, error=error, **kwargs)

    @classmethod
    def timed_out(cls, error: str = "execution timed out", **kwargs: Any) -> "ExecResult":
        return cls(status=RunStatus.TIMED_OUT, error=error, **kwargs)


class Executor(Protocol):
    async def __call__(
        self, spec: dict[str, Any], *, timeout_sec: int, log: LogFn
    ) -> ExecResult: ...
