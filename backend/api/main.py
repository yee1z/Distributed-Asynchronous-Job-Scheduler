from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.requests import Request
from starlette.responses import Response

from backend.api.routers import auth, health, jobs, runs
from backend.common.logging import configure_logging, get_logger

logger = get_logger(__name__)

configure_logging()

app = FastAPI(
    title="Distributed Asynchronous Job Scheduler API",
    version="0.1.0",
    description=(
        "RESTful API for registering jobs, dispatching runs, querying results/logs, "
        "and manually triggering or retrying jobs."
    ),
)

# CORS is open by default so the frontend (served from any origin during dev) can call the API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(auth.router)
app.include_router(jobs.router)
app.include_router(runs.router)


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/", include_in_schema=False)
def root() -> dict:
    return {
        "service": "job-scheduler-api",
        "version": app.version,
        "docs": "/docs",
        "openapi": "/openapi.json",
    }


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={"detail": "internal server error"})
