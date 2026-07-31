# Epic: observability_master
# Lifecycle: permanent
# Delete-when: NA
"""Self-healing actuator — re-execute a crashed manifest-consolidator Cloud Run Job.

Runbook: RB-MANIFEST-001 (DP-MANIFEST-001 / ``CONSOLIDATOR_DOWN``).

A ``CONSOLIDATOR_DOWN`` finding (DP-MANIFEST-001 — the consolidator is not
running / the ``_index`` went stale while per-VM shards exist) is, per the
``autonomous-recovery-matrix.md`` "relaunch a crashed safety/monitoring job is
in-scope-autonomous" rule, a deterministic re-execute: kick the per-asset_group
``manifest-consolidator-{ag}`` Cloud Run Job once more. The consolidator is
idempotent (it re-derives ``_index`` from the per-VM shards), so a single
re-execute is safe.

This mirrors the shipped ``refetch_feed`` Layer-0 actuator pattern — idempotent,
dry-runnable, cloud-agnostic (the GCP SDK is reached ONLY through the sanctioned
``deployment_service.backends._gcp_sdk`` boundary), and it emits a lifecycle
event (``CONSOLIDATOR_RECOVERED`` on a successful relaunch). It is NOT a
``Layer0Script`` subclass: that base hard-binds a UAC ``ActionType`` +
``RecoveryScriptRegistry`` entry (cross-repo), and these DP actuators live wholly
inside deployment-service, invoked from
``data_pipeline_monitors/escalation.py::route_finding``'s ``auto_recover`` tier.

Storm guard (idempotency)
-------------------------
A persistently-down consolidator must NOT spawn a relaunch storm. A per-AG file
sentinel in ``tempfile.gettempdir()`` records the last relaunch time; a relaunch
inside the cooldown window is skipped (``relaunch_skipped=cooldown``). Bounded
``_MAX_RELAUNCHES_PER_WINDOW`` (=1) per asset_group per window — exactly the
``refetch_feed`` 120s-cooldown idiom.

AUTO-ESCALATE safety net (operator idea 2026-06-24)
-----------------------------------------------------
A plain re-execute is the wrong fix for an OOM signature (terminal exit
137/signal-9 in the persisted ``run.log`` AND the ``_index`` mtime did NOT
advance) — a same-tier relaunch just re-OOMs. ``relaunch_with_escalation``
bumps the job to a GIVEN (cpu, memory, duckdb_memory) tier via
``gcloud run jobs update`` (reached through the sanctioned GCP SDK boundary),
moving the DuckDB memory ceiling in lockstep, THEN executes. This actuator
does NOT itself decide which tier to escalate TO — mirroring
``relaunch_backfill_vm``'s ``MACHINE_TYPE``-via-``launcher_env`` shape, the
ladder-climbing decision (mirrors VM ``lifecycle_class`` + the
autonomous-recovery-matrix ``auto_cooldown`` idiom — capped at the top rung,
page rather than loop forever) lives in
``data_pipeline_monitors/escalation.py::_recover_consolidator`` via the
canonical ``launch_budget_registry.CLOUD_RUN_MEMORY_TIER_LADDER``
(``[16Gi/cpu4 -> 32Gi/cpu8 -> 64Gi/cpu16]``), keeping ONE registry the launcher
and the OOM actuator both climb so they can never drift. This is the safety
net UNDER the bounded-canonical design (the per-VM-shard purge), NOT a
substitute for it — Cloud Run "autoscaling" is parallelism across executions,
not per-execution RAM, so it can never bump memory on its own.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from unified_trading_library import (
    UnifiedCloudConfig,
    log_event,
)
from unified_trading_library.events import (  # noqa: qg-deep-import
    CONSOLIDATOR_DOWN,
    CONSOLIDATOR_RECOVERED,
)

logger = logging.getLogger("recovery.relaunch_consolidator")

# Per-AG relaunch cooldown (seconds). Mirrors refetch_feed's auto_cooldown idiom:
# a persistently-down consolidator gets at most one active relaunch per window
# before the alerting ladder / page takes over. 120s is the same floor.
_COOLDOWN_SECONDS: int = 120
_MAX_RELAUNCHES_PER_WINDOW: int = 1

_VALID_ASSET_GROUPS: frozenset[str] = frozenset(("cefi", "defi", "tradfi", "sports", "prediction"))

# DuckDB's own memory_limit env var — must move in lockstep with the container
# --memory bump (2026-07-14 gotcha; see the module docstring's AUTO-ESCALATE
# section + manifest-consolidator-ssot.md).
_DUCKDB_MEMORY_ENV: str = "CONSOLIDATOR_DUCKDB_MEMORY_LIMIT"


def _default_cooldown_dir() -> Path:
    return Path(tempfile.gettempdir()) / "uts_consolidator_relaunch_cooldown"


def _resolve_project_id() -> str:
    """GCP project id via UnifiedCloudConfig (never ``os.getenv``)."""
    try:
        cfg = UnifiedCloudConfig()
    except (ValueError, RuntimeError, OSError):
        return ""
    return getattr(cfg, "gcp_project_id", "") or ""


def _default_run_job(job_name: str, *, project_id: str, region: str) -> str:
    """Re-execute a Cloud Run Job via the sanctioned GCP SDK boundary.

    Returns the started execution's resource name. Raises on SDK failure (the
    caller maps that to a FAILED verdict → file_issue fallthrough).
    """
    from deployment_service.backends import _gcp_sdk as _gcp_sdk_mod  # noqa: qg-deep-import

    run_v2 = _gcp_sdk_mod.run_v2
    client = run_v2.JobsClient()
    name = f"projects/{project_id}/locations/{region}/jobs/{job_name}"
    operation = client.run_job(name=name)
    execution: object = operation.metadata
    return str(getattr(execution, "name", name))


def _default_update_job(
    job_name: str, *, project_id: str, region: str, cpu: str, memory: str, duckdb_memory: str
) -> None:
    """Bump a Cloud Run Job's cpu/memory + DuckDB memory ceiling, then block until it lands.

    Cloud Run Job resource sizing (unlike a Service) lives on the job's task
    template, not on ``RunJobRequest``'s per-execution overrides — it MUST be
    set via ``update_job`` before the next ``run_job`` picks it up. Fetches the
    live ``Job`` first (never hand-builds one — that would silently drop every
    other field, e.g. the image digest / other env vars) and mutates only the
    resource limits + the DuckDB memory-limit env var (moved in lockstep — see
    the module docstring's AUTO-ESCALATE section). Raises on SDK failure (the
    caller maps that to a FAILED verdict).
    """
    from deployment_service.backends import _gcp_sdk as _gcp_sdk_mod  # noqa: qg-deep-import

    run_v2 = _gcp_sdk_mod.run_v2
    client = run_v2.JobsClient()
    name = f"projects/{project_id}/locations/{region}/jobs/{job_name}"
    job = client.get_job(name=name)
    container = job.template.template.containers[0]
    container.resources = run_v2.ResourceRequirements(limits={"cpu": cpu, "memory": memory})
    container.env = [
        *(e for e in container.env if getattr(e, "name", "") != _DUCKDB_MEMORY_ENV),
        run_v2.EnvVar(name=_DUCKDB_MEMORY_ENV, value=duckdb_memory),
    ]
    operation = client.update_job(job=job)
    operation.result(timeout=120)


class RelaunchConsolidator:
    """Re-execute the ``manifest-consolidator-{ag}`` Cloud Run Job once per window."""

    def __init__(
        self,
        *,
        cooldown_dir: Path | None = None,
        cooldown_seconds: int = _COOLDOWN_SECONDS,
        max_per_window: int = _MAX_RELAUNCHES_PER_WINDOW,
        region: str = "asia-northeast1",
        project_id: str | None = None,
        now: Callable[[], datetime] | None = None,
        run_job: Callable[..., str] | None = None,
        update_job: Callable[..., None] | None = None,
    ) -> None:
        self._cooldown_dir = cooldown_dir or _default_cooldown_dir()
        self._cooldown_seconds = cooldown_seconds
        self._max_per_window = max_per_window
        self._region = region
        self._project_id = project_id if project_id is not None else _resolve_project_id()
        self._now = now or (lambda: datetime.now(UTC))
        self._run_job = run_job or _default_run_job
        self._update_job = update_job or _default_update_job

    def job_name(self, asset_group: str) -> str:
        return f"manifest-consolidator-{asset_group}"

    def dry_run_plan(self, asset_group: str) -> dict[str, str]:
        return {
            "action": "relaunch_consolidator",
            "asset_group": asset_group,
            "job_name": self.job_name(asset_group),
            "region": self._region,
            "effect": "re-execute the manifest-consolidator Cloud Run Job (re-derives _index)",
        }

    def relaunch(self, asset_group: str, *, dry_run: bool = False) -> dict[str, str | bool | int | None]:
        """Re-execute the per-AG consolidator job, bounded by the per-AG cooldown.

        Returns a structured result dict. Never raises — an SDK failure is
        captured as ``{"status": "FAILED", ...}`` so the caller falls through to
        ``file_issue`` instead of crashing the escalation hop.
        """
        if asset_group not in _VALID_ASSET_GROUPS:
            return {
                "status": "FAILED",
                "asset_group": asset_group,
                "detail": f"unknown asset_group {asset_group!r}; valid: {sorted(_VALID_ASSET_GROUPS)}",
            }

        if dry_run:
            plan = self.dry_run_plan(asset_group)
            return {"status": "DRY_RUN", **plan}

        skip_reason = self._cooldown_skip_reason(asset_group)
        if skip_reason is not None:
            return {
                "status": "SUCCEEDED",
                "asset_group": asset_group,
                "relaunch_skipped": skip_reason,
                "cooldown_seconds": self._cooldown_seconds,
                "detail": f"consolidator relaunch rate-limited ({skip_reason}) — no relaunch issued",
            }

        if not self._project_id:
            return {
                "status": "FAILED",
                "asset_group": asset_group,
                "detail": "no GCP project id resolvable (UnifiedCloudConfig.gcp_project_id empty)",
            }

        job = self.job_name(asset_group)
        self._stamp_relaunch(asset_group)
        try:
            execution_name = self._run_job(job, project_id=self._project_id, region=self._region)
        except Exception as exc:  # SDK failure → FAILED (file_issue fallthrough)
            logger.warning("relaunch_consolidator: run_job(%s) failed: %s", job, exc)
            return {
                "status": "FAILED",
                "asset_group": asset_group,
                "job_name": job,
                "detail": f"run_job failed: {exc!r}"[:500],
            }

        log_event(
            CONSOLIDATOR_RECOVERED,
            severity="INFO",
            details={
                "asset_group": asset_group,
                "job_name": job,
                "execution": execution_name,
                "recovery_action": "relaunch_consolidator",
                "triggered_by": CONSOLIDATOR_DOWN,
            },
        )
        return {
            "status": "SUCCEEDED",
            "asset_group": asset_group,
            "job_name": job,
            "execution": execution_name,
        }

    def relaunch_with_escalation(
        self,
        asset_group: str,
        *,
        cpu: str,
        memory: str,
        duckdb_memory: str,
        dry_run: bool = False,
    ) -> dict[str, str | bool | int | None]:
        """OOM-signature relaunch: bump the job to the GIVEN tier, then re-execute.

        The caller (``escalation.py::_recover_consolidator``) has already
        confirmed the OOM signature and resolved the next ladder rung to
        escalate to (or self-emitted CRITICAL + returned a PAGE verdict without
        calling this at all, when already at the top rung) — this method only
        actuates the given tier, it does not walk the ladder itself. Never
        raises: an ``update_job`` SDK failure is captured as a FAILED verdict
        (file_issue fallthrough) rather than crashing the escalation hop.
        """
        if asset_group not in _VALID_ASSET_GROUPS:
            return {
                "status": "FAILED",
                "asset_group": asset_group,
                "detail": f"unknown asset_group {asset_group!r}; valid: {sorted(_VALID_ASSET_GROUPS)}",
            }

        if dry_run:
            plan = self.dry_run_plan(asset_group)
            return {
                "status": "DRY_RUN",
                **plan,
                "escalate_to_cpu": cpu,
                "escalate_to_memory": memory,
                "escalate_to_duckdb_memory": duckdb_memory,
            }

        if not self._project_id:
            return {
                "status": "FAILED",
                "asset_group": asset_group,
                "detail": "no GCP project id resolvable (UnifiedCloudConfig.gcp_project_id empty)",
            }

        job = self.job_name(asset_group)
        try:
            self._update_job(
                job,
                project_id=self._project_id,
                region=self._region,
                cpu=cpu,
                memory=memory,
                duckdb_memory=duckdb_memory,
            )
        except Exception as exc:  # SDK failure → FAILED (file_issue fallthrough)
            logger.warning("relaunch_consolidator: update_job(%s) failed: %s", job, exc)
            return {
                "status": "FAILED",
                "asset_group": asset_group,
                "job_name": job,
                "detail": f"update_job failed: {exc!r}"[:500],
            }

        result = self.relaunch(asset_group, dry_run=False)
        return {**result, "escalated_cpu": cpu, "escalated_memory": memory, "escalated_duckdb_memory": duckdb_memory}

    # ── cooldown sentinel (mirrors refetch_feed) ───────────────────────────

    def _cooldown_path(self, asset_group: str) -> Path:
        return self._cooldown_dir / f"{asset_group}.json"

    def _cooldown_skip_reason(self, asset_group: str) -> str | None:
        path = self._cooldown_path(asset_group)
        if not path.exists():
            return None
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        last_ts = float(state.get("last_relaunch_epoch", 0.0))
        count = int(state.get("count_in_window", 0))
        elapsed = self._now().timestamp() - last_ts
        if elapsed >= self._cooldown_seconds:
            return None
        if count >= self._max_per_window:
            return "window_cap" if self._max_per_window > 1 else "cooldown"
        return None

    def _stamp_relaunch(self, asset_group: str) -> None:
        self._cooldown_dir.mkdir(parents=True, exist_ok=True)
        path = self._cooldown_path(asset_group)
        now_epoch = self._now().timestamp()
        count = 1
        if path.exists():
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
                last_ts = float(state.get("last_relaunch_epoch", 0.0))
                if now_epoch - last_ts < self._cooldown_seconds:
                    count = int(state.get("count_in_window", 0)) + 1
            except (OSError, json.JSONDecodeError):
                count = 1
        path.write_text(
            json.dumps({"last_relaunch_epoch": now_epoch, "count_in_window": count}),
            encoding="utf-8",
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Relaunch a crashed manifest-consolidator Cloud Run Job.")
    parser.add_argument("--asset-group", required=True, dest="asset_group", choices=sorted(_VALID_ASSET_GROUPS))
    parser.add_argument("--region", default="asia-northeast1")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    actuator = RelaunchConsolidator(region=args.region)
    result = actuator.relaunch(args.asset_group, dry_run=args.dry_run)
    logger.info("relaunch_consolidator result=%s", result)
    return 0 if result.get("status") in ("SUCCEEDED", "DRY_RUN") else 1


if __name__ == "__main__":
    sys.exit(main())
