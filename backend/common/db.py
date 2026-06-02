from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import time

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from backend.common.config import get_settings
from backend.common.metrics import (
    DB_QUERIES,
    DB_QUERY_DURATION,
    set_db_pool_gauges,
    sql_operation,
)

_settings = get_settings()

engine = create_engine(
    _settings.database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)


@event.listens_for(engine, "before_cursor_execute")
def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany) -> None:
    context._query_start_time = time.perf_counter()
    context._query_operation = sql_operation(statement)


@event.listens_for(engine, "after_cursor_execute")
def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany) -> None:
    operation = getattr(context, "_query_operation", sql_operation(statement))
    start = getattr(context, "_query_start_time", None)
    if start is not None:
        DB_QUERY_DURATION.labels(operation=operation).observe(time.perf_counter() - start)
    DB_QUERIES.labels(operation=operation, status="success").inc()
    set_db_pool_gauges(engine.pool)


@event.listens_for(engine, "handle_error")
def _handle_db_error(exception_context) -> None:
    operation = sql_operation(getattr(exception_context, "statement", None))
    DB_QUERIES.labels(operation=operation, status="error").inc()
    set_db_pool_gauges(engine.pool)


@contextmanager
def session_scope() -> Iterator[Session]:
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_session() -> Iterator[Session]:
    """FastAPI dependency that yields a session and commits on success."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
