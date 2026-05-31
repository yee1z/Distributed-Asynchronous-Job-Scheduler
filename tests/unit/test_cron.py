from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from backend.scheduler.cron import due_tick, next_run_time

UTC = timezone.utc


def dt(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def test_cron_next_every_5_minutes():
    assert next_run_time("cron", "*/5 * * * *", "UTC", dt(2026, 1, 1, 0, 0, 0)) == dt(
        2026, 1, 1, 0, 5, 0
    )


def test_cron_respects_timezone():
    # 02:00 in Asia/Taipei (UTC+8) == 18:00 UTC the previous day.
    assert next_run_time("cron", "0 2 * * *", "Asia/Taipei", dt(2026, 1, 1, 0, 0, 0)) == dt(
        2026, 1, 1, 18, 0, 0
    )


def test_interval_next():
    assert next_run_time("interval", "60", "UTC", dt(2026, 1, 1, 0, 0, 0)) == dt(
        2026, 1, 1, 0, 1, 0
    )


@pytest.mark.parametrize("expr", [None, "0", "-5", "abc"])
def test_interval_invalid_returns_none(expr):
    assert next_run_time("interval", expr, "UTC", dt(2026, 1, 1)) is None


def test_manual_has_no_next():
    assert next_run_time("manual", None, "UTC", dt(2026, 1, 1)) is None


def test_due_tick_not_due_yet():
    now = dt(2026, 1, 1, 0, 0, 0)
    # interval anchored at now -> next fire is now+5 which is in the future
    assert due_tick("interval", "5", "UTC", now, now) is None


def test_due_tick_fires_after_anchor():
    base = dt(2026, 1, 1, 0, 0, 0)
    now = dt(2026, 1, 1, 0, 0, 7)
    assert due_tick("interval", "5", "UTC", base, now) == dt(2026, 1, 1, 0, 0, 5)


def test_due_tick_collapses_missed_ticks():
    # Idle for an hour -> only the most recent due tick is returned, not hundreds.
    now = dt(2026, 1, 1, 1, 0, 0)
    base = now - timedelta(hours=1)
    tick = due_tick("interval", "5", "UTC", base, now)
    assert tick == now  # last 5s boundary <= now
