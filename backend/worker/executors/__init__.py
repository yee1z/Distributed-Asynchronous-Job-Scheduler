"""Executor registry keyed by task_type."""

from __future__ import annotations

from backend.common.constants import TaskType
from backend.worker.executors import http, http_async, shell
from backend.worker.executors.base import ExecResult, LogFn

REGISTRY = {
    TaskType.HTTP: http.execute,
    TaskType.SHELL: shell.execute,
    TaskType.HTTP_ASYNC: http_async.execute,
}


def get_executor(task_type: str):
    executor = REGISTRY.get(task_type)
    if executor is None:
        raise ValueError(f"no executor registered for task_type '{task_type}'")
    return executor


__all__ = ["REGISTRY", "get_executor", "ExecResult", "LogFn"]
