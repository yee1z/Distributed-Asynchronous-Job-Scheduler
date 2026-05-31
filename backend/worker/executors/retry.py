"""Retry backoff policy.

Exponential backoff based on the attempt that just failed:
    delay = base * 2 ** (attempt - 1)
capped at ``retry_backoff_max_sec``.
"""

from __future__ import annotations

from backend.common.config import get_settings


def should_retry(attempt: int, max_retries: int) -> bool:
    """``attempt`` is the 1-based number of the attempt that just failed."""
    return attempt <= max_retries


def backoff_seconds(attempt: int, base_sec: int) -> int:
    cap = get_settings().retry_backoff_max_sec
    if base_sec <= 0:
        return 0
    delay = base_sec * (2 ** max(0, attempt - 1))
    return int(min(delay, cap))
