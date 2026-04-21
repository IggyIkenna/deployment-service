# Dockerfile for Deployment Monitoring API + UI
# Builds both API and UI, serves UI static files from FastAPI
#
# Production-ready configuration:
# - Multi-stage build for smaller image
# - Non-root user for security
# - Gunicorn with uvicorn workers for performance
# - Health checks for Cloud Run
# - Configurable worker count

# ARG must be before any FROM to use in base image
ARG PROJECT_ID

# ============================================
# Stage 1: Build UI (React/Vite)
# ============================================
FROM node:20-slim AS ui-builder

WORKDIR /app/ui

# Copy package files first for better caching
COPY ui/package*.json ./
RUN npm ci --prefer-offline

# Copy UI source and build for production
COPY ui/ ./
RUN npm run build

# ============================================
# Stage 2: Python API (Production)
# ============================================
# Use unified-trading-library base image for cloud abstraction
FROM --platform=linux/amd64 asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-trading-library/unified-trading-library:latest AS base

FROM base AS api

# All GCP/app configuration comes from .env at runtime (via docker-compose env_file)
# Only set Python & performance knobs that never change between environments.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PORT=8080 \
    GRPC_POLL_STRATEGY=epoll1 \
    GRPC_DNS_RESOLVER=native

WORKDIR /app

# Install additional system dependencies for dashboard
RUN apt-get update && apt-get install -y --no-install-recommends \
    ripgrep \
    tini \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user for security (if not already exists from base)
RUN id -u appuser >/dev/null 2>&1 || useradd --create-home --uid 1000 --shell /bin/bash appuser

# Copy and install Python dependencies first (better caching)
COPY pyproject.toml uv.lock ./

# Install dependencies from lockfile
# unified-trading-library already installed in base image
RUN uv sync --frozen --no-dev --system \
    && uv pip install --system --no-cache-dir gunicorn[gevent] gevent

# Copy application code
COPY deployment_service/ ./deployment_service/
COPY api/ ./api/
COPY backends/ ./backends/
COPY deployment/ ./deployment/
COPY configs/ ./configs/

# Copy built UI from previous stage
COPY --from=ui-builder /app/ui/dist ./ui/dist

# Change ownership to non-root user
RUN chown -R appuser:appuser /app

USER appuser

# Expose port
EXPOSE 8080

# Health check - Cloud Run uses this for readiness
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/api/health || exit 1

# Use tini as init system for proper signal handling
ENTRYPOINT ["/usr/bin/tini", "--"]

# Run with gunicorn for production (manages uvicorn workers)
CMD ["gunicorn", "api.main:app", "-c", "/app/api/gunicorn.conf.py"]

# ============================================
# Stage 3: api-dev (for Cloud Build quality gates - test-in-image)
# ============================================
FROM api AS api-dev
USER root
COPY scripts/ ./scripts/
COPY tests/ ./tests/
RUN uv sync --frozen --no-dev --system \
    && chown -R appuser:appuser /app
USER appuser

# ============================================
# Stage 4: sports-scheduler (Cloud Run Job)
# ============================================
# Short-lived job image — Cloud Scheduler fires this on a 5-minute cron via
# Cloud Run Jobs. Each invocation runs one evaluation cycle
# (`sports-trigger run --one-shot`) and exits. State (last_run per tier)
# persists to gs://deployment-scripts-<project>/sports_scheduler_state/
# between runs, so cadence is preserved across short-lived containers.
#
# Uses the same Python + deps as the `api` stage (co-located code in
# deployment_service/) — only the CMD differs. No gunicorn, no HTTP port,
# no healthcheck: Cloud Run Jobs track exit code, not liveness.
FROM api AS sports-scheduler

# Jobs do not serve HTTP — clear the API HEALTHCHECK inherited from `api`.
HEALTHCHECK NONE

# Keep tini as PID 1 for signal handling (terminate cleanly on SIGTERM).
ENTRYPOINT ["/usr/bin/tini", "--"]

# One-shot evaluation cycle — Cloud Scheduler drives cadence externally.
CMD ["python", "-m", "deployment_service", "sports-trigger", "run", "--one-shot"]
