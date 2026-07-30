# SCHEMA_PROVENANCE_EXEMPT: Service-internal dispatch backends — not cross-repo contracts.
"""Local-subprocess and Cloud Run dispatch backends for the sports trigger scheduler.

Extracted from ``sports_trigger_scheduler.py`` to keep that module under the
930-line codex file-size cap (lint-codex STEP file-size gate). The scheduler
keeps thin wrapper methods (``_dispatch_local``, ``_get_cloud_run_backend``,
``_strip_python_module_prefix``) that delegate here — several unit tests
monkeypatch those methods directly on a ``SportsTriggerScheduler`` instance,
so the wrapper signatures on the scheduler are unchanged by this extraction.
"""

from __future__ import annotations

import logging
import shlex
import subprocess
from pathlib import Path

from .backends.cloud_run import CloudRunBackend

logger = logging.getLogger(__name__)


def strip_python_module_prefix(cmd: str) -> list[str]:
    """Strip the ``python -m <module>`` prefix ``_build_cli_cmd`` always emits.

    Cloud Run Jobs V2 execution overrides can only replace a job's
    ``args``, never its ``command``/entrypoint — every real target job
    here (verified via ``gcloud run jobs describe
    uts-prod-instruments-service-t1-recon``: ``command: None``, ``args:
    ['--operation=instruments', ...]``) bakes the module invocation into
    the image's own ENTRYPOINT, so the override must be CLI flags only.
    """
    all_tokens = shlex.split(cmd)
    # _build_cli_cmd always starts with ["python", "-m", "<module>", ...]
    return all_tokens[3:] if len(all_tokens) > 3 else all_tokens


def get_cloud_run_backend(
    job_name: str,
    cloud_run_config: dict[str, str],
    cache: dict[str, CloudRunBackend],
) -> CloudRunBackend | None:
    """Return a cached CloudRunBackend for the given Cloud Run job name.

    Returns None and logs a warning if cloud_run_config is missing required keys.
    Backends are cached per job_name (via the caller-owned ``cache`` dict) so
    repeated calls share one client instance.
    """
    if job_name in cache:
        return cache[job_name]
    required = ("project_id", "region", "service_account_email")
    missing = [k for k in required if not cloud_run_config.get(k)]
    if missing:
        logger.warning(
            "cloud_run_config missing keys %s — cannot create backend for job %s",
            missing,
            job_name,
        )
        return None
    try:
        backend = CloudRunBackend(
            project_id=cloud_run_config["project_id"],
            region=cloud_run_config["region"],
            service_account_email=cloud_run_config["service_account_email"],
            job_name=job_name,
        )
        cache[job_name] = backend
        return backend
    except Exception as exc:
        logger.warning("Failed to create CloudRunBackend for job %s: %s", job_name, exc)
        return None


def dispatch_local(
    cmd: str,
    service: str,
    trigger_name: str,
    fixture_id: str,
    workspace_root: str,
) -> bool:
    """Dispatch a CLI command as a local subprocess.

    Uses the service repo's .venv python if ``workspace_root`` is set,
    otherwise falls back to the system python on PATH.

    Returns True on success, False on failure. Never raises — shard-level
    failure isolation ensures one service failure does not block others.
    """
    # Resolve service repo directory and venv python
    if workspace_root:
        service_dir = Path(workspace_root) / service
        venv_python = service_dir / ".venv" / "bin" / "python"

        if not service_dir.is_dir():
            logger.warning(
                "Service repo not found at %s — skipping %s for %s:%s",
                service_dir,
                service,
                trigger_name,
                fixture_id,
            )
            return False

        # Use repo venv python instead of generic "python -m ..."
        # shlex.split the full command, then replace python path
        raw_tokens = shlex.split(cmd)
        # raw_tokens[0] is "python", replace with venv python
        raw_tokens[0] = str(venv_python)
        cmd_tokens = raw_tokens
        cwd = str(service_dir)
    else:
        # No workspace_root: assumes `service` is importable in THIS
        # process's own environment. Loud WARNING (not silent) — a
        # deployment-service-only container (e.g. the Cloud Run Job
        # image) hits FileNotFoundError below on every call otherwise
        # (the 2026-07-08 silent-zero-dispatch root cause).
        logger.warning(
            "Local dispatch for %s with no workspace_root — assumes %s is "
            "importable here; use --backend cloud in a container that "
            "only ships deployment-service.",
            service,
            service,
        )
        cmd_tokens = shlex.split(cmd)
        cwd = None

    logger.info(
        "Dispatching local subprocess: %s (cwd=%s)",
        " ".join(cmd_tokens),
        cwd or "<inherited>",
    )

    process: subprocess.Popen[bytes] | None = None
    try:
        process = subprocess.Popen(
            cmd_tokens,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        _, stderr_bytes = process.communicate(timeout=3600)

        if process.returncode != 0:
            stderr_text = stderr_bytes.decode("utf-8", errors="replace")[:500]
            logger.warning(
                "Trigger dispatch failed for %s (trigger=%s fixture=%s) exit_code=%d stderr=%s",
                service,
                trigger_name,
                fixture_id,
                process.returncode,
                stderr_text,
            )
            return False

        logger.info(
            "Trigger dispatch succeeded for %s (trigger=%s fixture=%s pid=%d)",
            service,
            trigger_name,
            fixture_id,
            process.pid,
        )
        return True

    except subprocess.TimeoutExpired:
        logger.warning(
            "Trigger dispatch timed out for %s (trigger=%s fixture=%s) — killing",
            service,
            trigger_name,
            fixture_id,
        )
        if process is not None:
            process.kill()
            process.wait(timeout=10)
        return False
    except (FileNotFoundError, PermissionError, OSError):
        logger.warning(
            "Executable not found for %s (trigger=%s fixture=%s) — cmd=%s",
            service,
            trigger_name,
            fixture_id,
            " ".join(cmd_tokens),
        )
        return False
