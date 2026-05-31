from __future__ import annotations


class TaskType:
    HTTP = "http"
    SHELL = "shell"
    # Fire a request, obtain a handle, then poll a status URL until done.
    HTTP_ASYNC = "http_async"

    ALL = {HTTP, SHELL, HTTP_ASYNC}


class ScheduleType:
    CRON = "cron"
    INTERVAL = "interval"
    MANUAL = "manual"

    ALL = {CRON, INTERVAL, MANUAL}


class TriggerType:
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    DEPENDENCY = "dependency"
    RETRY = "retry"


class RunStatus:
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELED = "canceled"

    TERMINAL = {SUCCEEDED, FAILED, TIMED_OUT, CANCELED}


class LogStream:
    STDOUT = "stdout"
    STDERR = "stderr"
    SYSTEM = "system"
