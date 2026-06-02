from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://scheduler:scheduler@localhost:5432/scheduler"

    redis_url: str = "redis://localhost:6379/0"
    stream_key: str = "jobs:stream"
    consumer_group: str = "workers"
    # Sorted set holding runs that should be (re)enqueued at a future time (retry backoff).
    delayed_set_key: str = "jobs:delayed"
    cancel_channel: str = "jobs:cancel"

    scheduler_poll_interval_sec: float = 5.0
    # Arbitrary but fixed key used for the Postgres advisory lock (leader election).
    scheduler_lock_key: int = 42
    # Re-enqueue runs that have sat in 'queued' longer than this (lost-enqueue safety net).
    orphan_requeue_after_sec: int = 60

    worker_concurrency: int = 10
    worker_block_ms: int = 5000
    # How many new stream messages to pull per read.
    worker_read_batch: int = 20
    # Reclaim messages whose owner has been idle longer than this (failover).
    claim_min_idle_ms: int = 30000
    # Upper bound for exponential retry backoff.
    retry_backoff_max_sec: int = 3600

    # Port for the Prometheus /metrics endpoint exposed by scheduler/worker processes.
    metrics_port: int = 9100

    auth_secret_key: str = "dev-change-me"
    auth_token_expire_minutes: int = 1440

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
