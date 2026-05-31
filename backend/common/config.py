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

    scheduler_poll_interval_sec: float = 5.0
    # Arbitrary but fixed key used for the Postgres advisory lock (leader election).
    scheduler_lock_key: int = 42

    worker_concurrency: int = 10
    worker_block_ms: int = 5000
    claim_min_idle_ms: int = 30000

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
