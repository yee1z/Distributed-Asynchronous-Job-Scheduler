from __future__ import annotations

import pytest

from backend.worker.executors.retry import backoff_seconds, should_retry


@pytest.mark.parametrize(
    "attempt,base,expected",
    [
        (1, 30, 30),   # 30 * 2^0
        (2, 30, 60),   # 30 * 2^1
        (3, 30, 120),  # 30 * 2^2
        (1, 0, 0),     # zero base disables delay
    ],
)
def test_backoff_exponential(attempt, base, expected):
    assert backoff_seconds(attempt, base) == expected


def test_backoff_capped(monkeypatch):
    # Large attempt should be clamped to the configured maximum.
    assert backoff_seconds(20, 30) <= 3600


@pytest.mark.parametrize(
    "attempt,max_retries,expected",
    [(1, 0, False), (1, 3, True), (3, 3, True), (4, 3, False)],
)
def test_should_retry(attempt, max_retries, expected):
    assert should_retry(attempt, max_retries) is expected
