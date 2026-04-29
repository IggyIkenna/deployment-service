# Dockerfile for Deployment Orchestration API (deployment-service)
# Serves the deployment-dashboard FastAPI; UI is hosted as a sibling Cloud Run service (deployment-ui).
#
# Production-ready configuration:
# - Multi-stage build (api → api-dev → sports-scheduler)
# - Non-root user for security
# - Gunicorn with uvicorn workers for performance
# - Health checks for Cloud Run
# - Configurable worker count

# ARG must be before any FROM to use in base image
ARG PROJECT_ID

# ============================================
# Stage 1: Python API (Production)
# ============================================
# Use unified-trading-library base image for cloud abstraction
# UI co-serving dropped — deployment-ui is a sibling Cloud Run service.
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

# Copy package source first — hatchling needs the package dir present at install time
# to determine wheel contents (no explicit `tool.hatch.build.targets.wheel.packages`
# declaration in pyproject.toml; it relies on the project-name → directory heuristic).
# Repo layout: deployment_service/{api,backends,deployment}/ subpackages + configs/ at repo root.
# (No top-level ui/api/backends/deployment dirs — those never existed.)
COPY pyproject.toml uv.lock README.md ./
COPY deployment_service/ ./deployment_service/
COPY configs/ ./configs/

# Install dependencies (UTL base image already has transitive deps + UAC + UTL pre-installed).
# uv >= 0.11 removed --system from `uv sync`; mirror features-sports-service pattern instead.
RUN uv pip install --system --no-deps -e . \
    && uv pip install --system --no-cache-dir gunicorn[gevent] gevent

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

# Run with gunicorn for production (manages uvicorn workers).
# Module path matches actual package layout: deployment_service/api/main.py defines `app = create_app()`.
CMD ["gunicorn", "deployment_service.api.main:app", "-c", "/app/deployment_service/api/gunicorn.conf.py"]

# ============================================
# Stage 2: api-dev (for Cloud Build quality gates - test-in-image)
# ============================================
FROM api AS api-dev
USER root
COPY scripts/ ./scripts/
COPY tests/ ./tests/
RUN uv pip install --system --no-deps -e . \
    && chown -R appuser:appuser /app
USER appuser

# ============================================
# Stage 3: sports-scheduler (Cloud Run Job)
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
