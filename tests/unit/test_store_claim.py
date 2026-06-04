"""Unit tests for the worker's idempotent run claim.

``store.claim_run`` is the core "no double execution" guard: a conditional
``UPDATE ... WHERE status IN (...)`` so that only one worker can move a given
run out of its claimable state. Duplicate stream deliveries must therefore be
no-ops, and a RUNNING run must only be re-claimable on failover.

These tests run ``store`` against a throwaway SQLite database (no Postgres
needed): ``store`` calls ``session_scope()``, which reads ``SessionLocal`` from
``backend.common.db`` at call time, so swapping that binding redirects every
query to SQLite.
"""

from __future__ import annotations

import threading

import pytest
from sqlalchemy import BigInteger, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

import backend.common.db as db_module
from backend.common.constants import RunStatus, TriggerType
from backend.common.models import Base, Job, JobRun
from backend.worker import store


# The ORM metadata uses Postgres-specific column types. Teach SQLite how to
# render them so ``create_all`` can build the schema in-memory: BigInteger
# primary keys need INTEGER to auto-increment via SQLite's rowid, and JSONB has
# no SQLite equivalent. Both only affect the "sqlite" dialect, never Postgres.
@compiles(BigInteger, "sqlite")
def _compile_bigint_sqlite(element, compiler, **kw):  # noqa: ANN001, ANN202
    return "INTEGER"


@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(element, compiler, **kw):  # noqa: ANN001, ANN202
    return "JSON"


@pytest.fixture()
def sqlite_session(tmp_path, monkeypatch):
    """A SQLite-backed SessionLocal swapped in for ``store``'s queries.

    File-backed (not ``:memory:``) so each session gets its own connection and
    the concurrency test exercises a genuine multi-connection race.
    """
    url = f"sqlite:///{tmp_path / 'claim.db'}"
    engine = create_engine(url, future=True, connect_args={"timeout": 30})
    Base.metadata.create_all(engine)
    test_session = sessionmaker(
        bind=engine, autoflush=False, expire_on_commit=False, future=True
    )
    monkeypatch.setattr(db_module, "SessionLocal", test_session)
    yield test_session
    engine.dispose()


def _seed_run(test_session, *, status: str = RunStatus.QUEUED, attempt: int = 1) -> int:
    """Insert a job + one run in ``status`` and return the run id."""
    with test_session() as session:
        job = Job(
            name="claim-test",
            task_type="shell",
            task_spec={"command": "echo", "args": ["hi"]},
            timeout_sec=60,
            max_retries=2,
            retry_backoff_sec=5,
        )
        session.add(job)
        session.flush()
        run = JobRun(
            job_id=job.id,
            trigger_type=TriggerType.MANUAL,
            status=status,
            attempt=attempt,
        )
        session.add(run)
        session.commit()
        return run.id


def _status_of(test_session, run_id: int) -> JobRun:
    with test_session() as session:
        return session.get(JobRun, run_id)


def test_first_claim_returns_payload_and_marks_running(sqlite_session):
    run_id = _seed_run(sqlite_session)

    claimed = store.claim_run(run_id, "worker-1", allow_running=False)

    assert claimed is not None
    assert claimed["run_id"] == run_id
    assert claimed["attempt"] == 1
    assert claimed["task_type"] == "shell"
    assert claimed["task_spec"] == {"command": "echo", "args": ["hi"]}
    assert claimed["timeout_sec"] == 60
    assert claimed["max_retries"] == 2

    run = _status_of(sqlite_session, run_id)
    assert run.status == RunStatus.RUNNING
    assert run.worker_id == "worker-1"
    assert run.started_at is not None


def test_duplicate_delivery_is_noop(sqlite_session):
    run_id = _seed_run(sqlite_session)

    first = store.claim_run(run_id, "worker-1", allow_running=False)
    # Same run delivered again to a different worker: the conditional UPDATE
    # finds nothing still in (pending, queued), so this is a no-op.
    second = store.claim_run(run_id, "worker-2", allow_running=False)

    assert first is not None
    assert second is None
    # The original owner is preserved; the duplicate did not steal the run.
    assert _status_of(sqlite_session, run_id).worker_id == "worker-1"


def test_running_run_only_reclaimable_on_failover(sqlite_session):
    run_id = _seed_run(sqlite_session, status=RunStatus.RUNNING)

    # A normal (>) delivery must never touch a run already RUNNING.
    assert store.claim_run(run_id, "w-normal", allow_running=False) is None
    # A reclaimed/stale message (XAUTOCLAIM) may re-claim it so a crashed
    # worker's run still completes.
    reclaimed = store.claim_run(run_id, "w-failover", allow_running=True)

    assert reclaimed is not None
    assert _status_of(sqlite_session, run_id).worker_id == "w-failover"


@pytest.mark.parametrize(
    "terminal", [RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.TIMED_OUT, RunStatus.CANCELED]
)
def test_terminal_run_is_never_claimed(sqlite_session, terminal):
    run_id = _seed_run(sqlite_session, status=terminal)

    # Even failover must not resurrect a run that already reached a terminal state.
    assert store.claim_run(run_id, "w", allow_running=True) is None
    assert _status_of(sqlite_session, run_id).status == terminal


def test_missing_run_returns_none(sqlite_session):
    assert store.claim_run(999_999, "w", allow_running=False) is None


def test_concurrent_claims_have_a_single_winner(sqlite_session):
    """Eight workers race for one queued run; exactly one may win."""
    run_id = _seed_run(sqlite_session)
    n = 8
    barrier = threading.Barrier(n)
    lock = threading.Lock()
    results: list[dict | None] = []

    def claim(i: int) -> None:
        barrier.wait()  # release all threads as simultaneously as possible
        claimed = store.claim_run(run_id, f"worker-{i}", allow_running=False)
        with lock:
            results.append(claimed)

    threads = [threading.Thread(target=claim, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    winners = [r for r in results if r is not None]
    assert len(winners) == 1, f"expected exactly one winner, got {len(winners)}"
    assert len(results) == n
    # The run ended up RUNNING, owned by exactly one worker.
    run = _status_of(sqlite_session, run_id)
    assert run.status == RunStatus.RUNNING
    assert run.worker_id is not None
