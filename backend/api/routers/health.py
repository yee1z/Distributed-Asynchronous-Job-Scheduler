from __future__ import annotations

from fastapi import APIRouter, Response
from sqlalchemy import text

from backend.common.db import engine
from backend.common.redis_queue import get_sync_redis

router = APIRouter(tags=["health"])


@router.get("/healthz")
def healthz() -> dict:
    """Liveness probe: process is up."""
    return {"status": "ok"}


@router.get("/readyz")
def readyz(response: Response) -> dict:
    """Readiness probe: dependencies (Postgres, Redis) are reachable."""
    checks: dict[str, str] = {}
    ok = True

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["database"] = f"error: {exc}"
        ok = False

    client = get_sync_redis()
    try:
        client.ping()
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = f"error: {exc}"
        ok = False
    finally:
        client.close()

    if not ok:
        response.status_code = 503
    return {"status": "ok" if ok else "degraded", "checks": checks}
