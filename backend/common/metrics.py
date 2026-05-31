from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# Shared metric definitions. Each process exposes its own /metrics endpoint or
# pushes via the HTTP server started in worker/scheduler.

JOBS_ENQUEUED = Counter(
    "scheduler_jobs_enqueued_total",
    "Number of job runs enqueued to the dispatch stream",
    ["trigger_type"],
)

RUNS_PROCESSED = Counter(
    "worker_runs_processed_total",
    "Number of job runs processed by workers",
    ["status", "task_type"],
)

RUNS_RECLAIMED = Counter(
    "worker_runs_reclaimed_total",
    "Number of stream messages reclaimed from dead/stalled workers",
)

RUN_DURATION = Histogram(
    "worker_run_duration_seconds",
    "Execution duration of job runs",
    ["task_type"],
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 300, 1800, 3600),
)

INFLIGHT = Gauge(
    "worker_inflight_runs",
    "Number of job runs currently being executed by this worker",
)

QUEUE_DEPTH = Gauge(
    "scheduler_stream_pending",
    "Pending (unacked) messages in the dispatch consumer group",
)

SCHEDULER_IS_LEADER = Gauge(
    "scheduler_is_leader",
    "1 if this scheduler instance currently holds the leader lock, else 0",
)
