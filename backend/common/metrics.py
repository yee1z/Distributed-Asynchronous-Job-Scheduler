from __future__ import annotations

from contextlib import contextmanager
import time
from collections.abc import Iterator

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

HTTP_REQUESTS = Counter(
    "api_http_requests_total",
    "Number of HTTP requests handled by the API",
    ["method", "path", "status_code"],
)

HTTP_REQUEST_DURATION = Histogram(
    "api_http_request_duration_seconds",
    "API HTTP request duration in seconds",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)

HTTP_INFLIGHT = Gauge(
    "api_http_inflight_requests",
    "Number of API HTTP requests currently in progress",
)

DB_QUERIES = Counter(
    "db_queries_total",
    "Number of SQL statements executed",
    ["operation", "status"],
)

DB_QUERY_DURATION = Histogram(
    "db_query_duration_seconds",
    "SQL statement execution duration in seconds",
    ["operation"],
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)

DB_POOL_CHECKED_OUT = Gauge(
    "db_pool_checked_out_connections",
    "Number of database connections currently checked out from the SQLAlchemy pool",
)

DB_POOL_SIZE = Gauge(
    "db_pool_size_connections",
    "Configured SQLAlchemy database connection pool size",
)

DB_POOL_OVERFLOW = Gauge(
    "db_pool_overflow_connections",
    "Number of SQLAlchemy database connections above the configured pool size",
)

REDIS_COMMANDS = Counter(
    "redis_commands_total",
    "Number of Redis commands executed by the application",
    ["operation", "status"],
)

REDIS_COMMAND_DURATION = Histogram(
    "redis_command_duration_seconds",
    "Redis command duration in seconds",
    ["operation"],
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)

REDIS_STREAM_LENGTH = Gauge(
    "redis_stream_length",
    "Number of entries currently in the Redis dispatch stream",
)

REDIS_DELAYED_RUNS = Gauge(
    "redis_delayed_runs",
    "Number of runs currently waiting in the Redis delayed retry set",
)


def sql_operation(statement: str | None) -> str:
    if not statement:
        return "unknown"
    first = statement.lstrip().split(None, 1)
    return first[0].lower() if first else "unknown"


@contextmanager
def observe_redis_command(operation: str) -> Iterator[None]:
    start = time.perf_counter()
    status = "success"
    try:
        yield
    except Exception:
        status = "error"
        raise
    finally:
        REDIS_COMMAND_DURATION.labels(operation=operation).observe(time.perf_counter() - start)
        REDIS_COMMANDS.labels(operation=operation, status=status).inc()


def set_db_pool_gauges(pool) -> None:
    for metric, attr in (
        (DB_POOL_CHECKED_OUT, "checkedout"),
        (DB_POOL_SIZE, "size"),
        (DB_POOL_OVERFLOW, "overflow"),
    ):
        try:
            metric.set(getattr(pool, attr)())
        except Exception:
            # Some SQLAlchemy pools do not expose QueuePool-style counters.
            pass
