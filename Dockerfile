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
# Digest-pinned UTL base image (QG STEP 5.79 -- reproducible builds + UTL/UAC provenance).
# Refreshed by the dependency-update fan-out (update-dependency-version.yml) on base-image
# republish; cloudbuild may override at build time: --build-arg BASE_IMAGE_DIGEST=sha256:...
ARG BASE_IMAGE_DIGEST=sha256:3c1e032fbfeaee64b85f6e5db38b165e2d151907f2d0ded28c56d3137b014abf
FROM --platform=linux/amd64 asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-trading-library/unified-trading-library@${BASE_IMAGE_DIGEST} AS base

FROM base AS api

# cloudbuild passes the git-tag-derived version as --build-arg SETUPTOOLS_SCM_PRETEND_VERSION.
# Export it as an ENV so hatch-vcs (pyproject `[tool.hatch.version] source = "vcs"`) resolves a
# version during `uv pip install -e .` below — inside the docker build `.git` is absent (.dockerignore'd
# + COPY . . excludes it), so setuptools-scm cannot detect a version on its own. Without this, the
# `-e .` install fails: "setuptools-scm was unable to detect version". The api-dev / sports-scheduler /
# maintenance-jobs stages all build FROM api, so this ENV carries through to their installs too.
ARG SETUPTOOLS_SCM_PRETEND_VERSION
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

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
# scripts/ holds the maintenance Cloud Run Job entrypoints (vm/cleanup_old_tarballs.py,
# vm/vm_log_archival_cron.py, …). Those jobs (terraform/gcp/{tarball_cleanup,vm_log_archival}_scheduler.tf)
# override the CMD, so the scripts must be present in the published `api` image — not just the
# api-dev test stage. WORKDIR is /app, so they resolve as `scripts/...` (file) / `scripts.vm....` (module).
COPY scripts/ ./scripts/

# Install dependencies (UTL base image already has transitive deps + UAC + UTL pre-installed).
# uv >= 0.11 removed --system from `uv sync`; mirror features-sports-service pattern instead.
#
# `--no-deps` installs ONLY the deployment_service package and relies on the UTL base for
# transitive runtime deps. The base image dropped `uvicorn` + `jinja2` (UTL 0.13.0 digest
# refresh 2026-06-19) — they are DECLARED deps of deployment-service (pyproject `uvicorn[standard]`,
# `jinja2`) but were never installed under `--no-deps`, so the `api` image crashed at gunicorn
# startup: the `uvicorn.workers.UvicornWorker` worker_class was unresolvable AND the eager backends
# import chain (deployment_service/__init__ -> live_deployment -> backends -> vm/services/vm_config)
# hit `from jinja2 import Template`. Install both explicitly so the image is self-contained and
# does not depend on incidental base-image transitives.
RUN uv pip install --system --no-deps -e . \
    && uv pip install --system --no-cache-dir gunicorn[gevent] gevent "uvicorn[standard]" jinja2

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

# ============================================
# Stage 4: maintenance-jobs (Cloud Run Jobs that import the full deployment_service package)
# ============================================
# The `api` stage installs deployment_service with --no-deps (the UTL base provides the
# API server's runtime needs). Maintenance jobs (scripts/vm/vm_log_archival_cron.py, …)
# import the full backends chain (deployment_service/__init__ -> live_deployment ->
# backends.provider_factory -> {aws,gcp,vm} backends), which needs deployment-service's
# DECLARED deps (google-cloud-run, botocore, …) that are NOT in the UTL base. Install them
# here (WITH deps) so the eager package import resolves. Kept as a separate stage so the
# api/dashboard image is unchanged.
# The `api` stage already installs deployment_service (--no-deps); we cannot re-run
# `uv pip install -e .` WITH deps here because the lockfile pins workspace path-deps
# (unified-trading-library, …) that are absent as source in the image. So we install ONLY
# the third-party packages the eager backends-import chain needs that are NOT in the UTL
# base — verified by probing the image: jinja2 (backends/services/vm_config.py), flask +
# functions-framework (cloud-functions backend imports). All other backend deps
# (google-cloud-run/compute, botocore, web3, …) are already present in the base.
# TODO(P3): declare jinja2/flask/functions-framework in deployment-service pyproject.
FROM api AS maintenance-jobs
USER root
RUN uv pip install --system --no-cache-dir jinja2 flask functions-framework \
    && chown -R appuser:appuser /app
USER appuser
HEALTHCHECK NONE
ENTRYPOINT ["/usr/bin/tini", "--"]
# CMD is overridden per-job by the Cloud Run Job definition (terraform/gcp/*_scheduler.tf).
CMD ["python", "-c", "import deployment_service; print('deployment-service maintenance-jobs image OK')"]
