# syntax=docker/dockerfile:1

# --- builder: install dependencies into an isolated venv -------------------
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app
RUN python -m venv "$VIRTUAL_ENV"

COPY requirements.txt ./
RUN pip install -r requirements.txt

# --- runtime: slim image with the venv + application code ------------------
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY backend ./backend
COPY alembic.ini ./alembic.ini
COPY migrations ./migrations

# Run as an unprivileged user.
RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8000

# Default command runs the API. Scheduler / worker override `command` in k8s
# (e.g. `python -m backend.scheduler.main`, `python -m backend.worker.main`).
CMD ["uvicorn", "backend.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
