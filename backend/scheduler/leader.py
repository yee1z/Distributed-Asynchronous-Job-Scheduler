"""Leader election via a PostgreSQL session-level advisory lock.

Every scheduler replica calls :meth:`Leader.acquire` each tick. Exactly one
holds ``pg_advisory_lock(key)`` at a time; the lock is bound to that database
*session*, so if the leader pod dies its connection drops and the lock is
released automatically, letting a standby take over (failover).
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from backend.common.logging import get_logger

logger = get_logger(__name__)


class Leader:
    def __init__(self, engine: Engine, lock_key: int) -> None:
        self._engine = engine
        self._lock_key = lock_key
        self._conn: Connection | None = None
        self.is_leader = False

    def acquire(self) -> bool:
        """Try to become (or confirm we remain) the leader. Returns leadership state."""
        if self.is_leader and self._connection_alive():
            return True

        # Lost / never had the lock: (re)connect and attempt to grab it.
        self._reset()
        try:
            self._conn = self._engine.connect()
            acquired = self._conn.execute(
                text("SELECT pg_try_advisory_lock(:k)"), {"k": self._lock_key}
            ).scalar()
        except Exception:  # noqa: BLE001 - any DB error => not leader this tick
            logger.warning("leader: lock attempt failed", exc_info=True)
            self._reset()
            return False

        self.is_leader = bool(acquired)
        if self.is_leader:
            logger.info("leader: acquired advisory lock %s", self._lock_key)
        else:
            # Someone else holds it; drop our spare connection while we stand by.
            self._reset()
        return self.is_leader

    def _connection_alive(self) -> bool:
        if self._conn is None:
            return False
        try:
            self._conn.execute(text("SELECT 1"))
            return True
        except Exception:  # noqa: BLE001
            logger.warning("leader: connection lost, will re-elect", exc_info=True)
            return False

    def _reset(self) -> None:
        self.is_leader = False
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
        self._conn = None

    def release(self) -> None:
        if self._conn is not None and self.is_leader:
            try:
                self._conn.execute(
                    text("SELECT pg_advisory_unlock(:k)"), {"k": self._lock_key}
                )
            except Exception:  # noqa: BLE001
                pass
        self._reset()
