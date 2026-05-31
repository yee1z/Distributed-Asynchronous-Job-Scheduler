"""Next-run-time computation for cron and interval schedules.

All inputs/outputs are timezone-aware ``datetime`` objects in UTC. Cron
expressions are evaluated in the job's configured timezone so that, e.g.,
"every day at 02:00" means 02:00 local time even across DST boundaries.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

from backend.common.constants import ScheduleType


def _zone(tz_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError, KeyError):
        return ZoneInfo("UTC")


def next_run_time(
    schedule_type: str,
    schedule_expr: str | None,
    tz_name: str,
    after: datetime,
) -> datetime | None:
    """Return the next fire time strictly after ``after`` (UTC), or None for manual."""
    if after.tzinfo is None:
        after = after.replace(tzinfo=timezone.utc)

    if schedule_type == ScheduleType.CRON:
        if not schedule_expr:
            return None
        local_after = after.astimezone(_zone(tz_name))
        itr = croniter(schedule_expr, local_after)
        nxt: datetime = itr.get_next(datetime)
        return nxt.astimezone(timezone.utc)

    if schedule_type == ScheduleType.INTERVAL:
        if not schedule_expr or not schedule_expr.isdigit():
            return None
        seconds = int(schedule_expr)
        if seconds <= 0:
            return None
        return after.astimezone(timezone.utc) + timedelta(seconds=seconds)

    return None
