"""Unit tests for scheduler leader election (Postgres advisory lock).

``Leader`` keeps one database *session* open and holds
``pg_try_advisory_lock`` on it; exactly one replica wins, and when a leader's
session drops the lock frees automatically so a standby takes over.

The advisory-lock functions are Postgres-only, so these tests drive ``Leader``
against a fake engine whose connections share a tiny in-memory lock server that
models session-scoped locking: a lock is held by the connection that grabbed
it, and closing/killing that connection releases it — exactly Postgres's
behaviour. This exercises the election state machine (acquire, fast-path,
re-election, failover, release) deterministically without a database.
"""

from __future__ import annotations

import pytest

from backend.scheduler.leader import Leader

LOCK_KEY = 42


class FakeDBError(Exception):
    """Stand-in for a dropped/failed connection."""


class FakeLockServer:
    """Models Postgres session-scoped advisory locks across connections."""

    def __init__(self) -> None:
        self.held: dict[int, FakeConnection] = {}

    def try_lock(self, key: int, conn: "FakeConnection") -> bool:
        holder = self.held.get(key)
        if holder is None or holder is conn:
            self.held[key] = conn
            return True
        return False

    def unlock(self, key: int, conn: "FakeConnection") -> None:
        if self.held.get(key) is conn:
            del self.held[key]

    def drop_connection(self, conn: "FakeConnection") -> None:
        # A dropped session releases every advisory lock it held.
        for key, holder in list(self.held.items()):
            if holder is conn:
                del self.held[key]

    def holder_of(self, key: int) -> "FakeConnection | None":
        return self.held.get(key)


class FakeResult:
    def __init__(self, value) -> None:
        self._value = value

    def scalar(self):
        return self._value


class FakeConnection:
    def __init__(self, server: FakeLockServer) -> None:
        self.server = server
        self.alive = True
        self.statements: list[str] = []
        self.raise_on_close = False

    def execute(self, clause, params=None):
        sql = " ".join(str(clause).split())
        self.statements.append(sql)
        if not self.alive:
            raise FakeDBError("connection is dead")
        if "pg_try_advisory_lock" in sql:
            return FakeResult(self.server.try_lock(params["k"], self))
        if "pg_advisory_unlock" in sql:
            self.server.unlock(params["k"], self)
            return FakeResult(True)
        if "SELECT 1" in sql:
            return FakeResult(1)
        return FakeResult(None)

    def close(self) -> None:
        if self.raise_on_close:
            raise FakeDBError("close failed")
        self.alive = False
        self.server.drop_connection(self)

    def kill(self) -> None:
        """Simulate the leader's pod dying: socket broken, session lock freed."""
        self.alive = False
        self.server.drop_connection(self)


class FakeEngine:
    def __init__(self, server: FakeLockServer) -> None:
        self.server = server
        self.connections: list[FakeConnection] = []
        self.connect_error: Exception | None = None

    def connect(self) -> FakeConnection:
        if self.connect_error is not None:
            raise self.connect_error
        conn = FakeConnection(self.server)
        self.connections.append(conn)
        return conn


@pytest.fixture()
def server() -> FakeLockServer:
    return FakeLockServer()


@pytest.fixture()
def engine(server: FakeLockServer) -> FakeEngine:
    return FakeEngine(server)


def test_acquires_lock_when_free(engine, server):
    leader = Leader(engine, LOCK_KEY)

    assert leader.acquire() is True
    assert leader.is_leader is True
    assert server.holder_of(LOCK_KEY) is engine.connections[0]


def test_not_leader_when_lock_held_elsewhere(engine, server):
    # Someone else already holds the lock.
    other = FakeConnection(server)
    assert server.try_lock(LOCK_KEY, other) is True

    leader = Leader(engine, LOCK_KEY)

    assert leader.acquire() is False
    assert leader.is_leader is False
    # The spare connection it opened to try is dropped while standing by.
    assert leader._conn is None


def test_two_replicas_elect_single_leader(engine, server):
    l1 = Leader(engine, LOCK_KEY)
    l2 = Leader(engine, LOCK_KEY)

    assert l1.acquire() is True
    assert l2.acquire() is False
    assert l1.is_leader is True
    assert l2.is_leader is False


def test_fast_path_keeps_leadership_without_reconnecting(engine):
    leader = Leader(engine, LOCK_KEY)
    assert leader.acquire() is True
    assert len(engine.connections) == 1

    # Still leader on the next tick: confirm via a liveness check, no reconnect,
    # and no second lock attempt.
    assert leader.acquire() is True
    assert len(engine.connections) == 1
    assert engine.connections[0].statements.count("SELECT 1") == 1
    assert sum("pg_try_advisory_lock" in s for s in engine.connections[0].statements) == 1


def test_failover_standby_takes_over_when_leader_dies(engine, server):
    l1 = Leader(engine, LOCK_KEY)
    l2 = Leader(engine, LOCK_KEY)
    assert l1.acquire() is True
    assert l2.acquire() is False  # standby waits

    # l1's pod dies: its session drops and the advisory lock frees.
    l1._conn.kill()

    # The standby now wins on its next tick.
    assert l2.acquire() is True
    assert l2.is_leader is True
    assert server.holder_of(LOCK_KEY) is l2._conn

    # The dead leader notices on its next tick (liveness check fails -> re-elect)
    # and steps down because the lock is taken.
    assert l1.acquire() is False
    assert l1.is_leader is False


def test_reelects_after_its_own_connection_drops(engine, server):
    leader = Leader(engine, LOCK_KEY)
    assert leader.acquire() is True
    first_conn = leader._conn

    # Transient blip: the connection dies (lock freed), nobody else grabs it.
    first_conn.kill()

    # Next tick: liveness check fails, it reconnects and re-grabs its own lock.
    assert leader.acquire() is True
    assert leader.is_leader is True
    assert leader._conn is not first_conn
    assert server.holder_of(LOCK_KEY) is leader._conn


def test_connect_failure_returns_false(engine):
    engine.connect_error = FakeDBError("cannot reach database")
    leader = Leader(engine, LOCK_KEY)

    assert leader.acquire() is False
    assert leader.is_leader is False
    assert leader._conn is None


def test_release_unlocks_and_lets_another_acquire(engine, server):
    leader = Leader(engine, LOCK_KEY)
    assert leader.acquire() is True
    leader_conn = leader._conn

    leader.release()

    assert leader.is_leader is False
    assert leader._conn is None
    assert server.holder_of(LOCK_KEY) is None
    assert any("pg_advisory_unlock" in s for s in leader_conn.statements)

    # The freed lock can now be taken by a fresh replica.
    other = Leader(engine, LOCK_KEY)
    assert other.acquire() is True


def test_release_when_not_leader_is_noop(engine, server):
    other = FakeConnection(server)
    server.try_lock(LOCK_KEY, other)  # someone else holds it
    leader = Leader(engine, LOCK_KEY)
    assert leader.acquire() is False

    # Must not raise and must not unlock a lock it never held.
    leader.release()

    assert leader.is_leader is False
    assert server.holder_of(LOCK_KEY) is other


def test_release_tolerates_unlock_failure(engine):
    leader = Leader(engine, LOCK_KEY)
    assert leader.acquire() is True

    # Connection died just before shutdown: the unlock query will raise.
    leader._conn.kill()
    leader.release()  # must swallow the error and still step down cleanly

    assert leader.is_leader is False
    assert leader._conn is None


def test_reset_tolerates_close_failure(engine):
    leader = Leader(engine, LOCK_KEY)
    assert leader.acquire() is True
    leader._conn.raise_on_close = True

    leader.release()  # unlock succeeds, then close() raises and is swallowed

    assert leader.is_leader is False
    assert leader._conn is None


def test_liveness_check_handles_missing_connection(engine):
    leader = Leader(engine, LOCK_KEY)
    # Inconsistent state: marked leader but holding no connection. The liveness
    # check must report not-alive and trigger a clean re-election.
    leader.is_leader = True
    leader._conn = None

    assert leader.acquire() is True
    assert leader._conn is engine.connections[0]
