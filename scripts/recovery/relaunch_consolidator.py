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

OOM AUTO-ESCALATE safety net (operator idea 2026-06-24)
---------------------------------------------------------
``cefi_hl_aster_batch_data_gaps_2026_06_22.md``'s deadman/consolidator-watchdog
todo: a same-tier relaunch after an OOM (exit 137/signal-9) just re-OOMs. Cloud
Run "autoscaling" is PARALLELISM (more concurrent executions), not per-execution
RAM, so the only way to give a retried execution more headroom is to bump the
Job's container resource limits before re-executing it (mirrors
``data_pipeline_monitors/escalation.py``'s ``_escalated_machine_type`` KEY #4
pattern for VM backfills, adapted to Cloud Run's discrete cpu/memory limits
instead of GCE machine types). ``relaunch(asset_group, oom=True)`` walks
``_MACHINE_TIER_REGISTRY`` one rung up per call (persisted per-AG so repeated
OOMs keep climbing, never reset by a plain non-OOM relaunch) and re-executes at
the new tier; once the top rung is reached, it PAGES (``CONSOLIDATOR_DOWN``
CRITICAL) instead of looping forever at the ceiling.
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

# Cloud Run machine-size ladder for an OOM-triggered relaunch — ascending
# (cpu, memory) pairs, matching the plan todo's literal registry
# ``[16Gi/cpu4 -> 32Gi/cpu8 -> 64Gi/cpu16]``. Values are the exact strings
# ``run_v2`` resource-limit fields expect (``cpu`` as a stringified core count,
# ``memory`` as a `<N>Gi` string).
_MACHINE_TIER_REGISTRY: tuple[tuple[str, str], ...] = (
    ("4", "16Gi"),
    ("8", "32Gi"),
    ("16", "64Gi"),
)
_TOP_TIER_INDEX: int = len(_MACHINE_TIER_REGISTRY) - 1


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


def _default_update_job_resources(job_name: str, *, project_id: str, region: str, cpu: str, memory: str) -> None:
    """Bump a Cloud Run Job's per-execution cpu/memory limits (the OOM-escalation step).

    Mirrors ``_default_run_job``'s sanctioned GCP SDK boundary. Cloud Run
    "autoscaling" is parallelism, not per-execution RAM — the only way to give a
    retried execution more headroom is to mutate the Job's container resource
    limits before re-executing it (``gcloud run jobs update --memory --cpu``,
    done here via the SDK). Raises on SDK failure; the caller maps that to a
    FAILED verdict (never partially escalates — the tier-index bump only
    persists after this call succeeds).
    """
    from deployment_service.backends import _gcp_sdk as _gcp_sdk_mod  # noqa: qg-deep-import

    run_v2 = _gcp_sdk_mod.run_v2
    client = run_v2.JobsClient()
    name = f"projects/{project_id}/locations/{region}/jobs/{job_name}"
    job = client.get_job(name=name)
    job.template.template.containers[0].resources.limits["cpu"] = cpu
    job.template.template.containers[0].resources.limits["memory"] = memory
    client.update_job(job=job)


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
        update_job_resources: Callable[..., None] | None = None,
    ) -> None:
        self._cooldown_dir = cooldown_dir or _default_cooldown_dir()
        self._cooldown_seconds = cooldown_seconds
        self._max_per_window = max_per_window
        self._region = region
        self._project_id = project_id if project_id is not None else _resolve_project_id()
        self._now = now or (lambda: datetime.now(UTC))
        self._run_job = run_job or _default_run_job
        self._update_job_resources = update_job_resources or _default_update_job_resources

    def job_name(self, asset_group: str) -> str:
        return f"manifest-consolidator-{asset_group}"

    # ── OOM machine-tier escalation state (per-AG, sticky — never auto-resets;
    #    a plain non-OOM relaunch does not touch it) ────────────────────────

    def _tier_state_path(self, asset_group: str) -> Path:
        return self._cooldown_dir / f"{asset_group}.tier.json"

    def current_tier_index(self, asset_group: str) -> int:
        """Persisted machine-tier index for ``asset_group`` (0 = the baseline tier)."""
        path = self._tier_state_path(asset_group)
        if not path.exists():
            return 0
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
            return min(max(int(state.get("tier_index", 0)), 0), _TOP_TIER_INDEX)
        except (OSError, json.JSONDecodeError, ValueError, TypeError):
            return 0

    def _write_tier_index(self, asset_group: str, tier_index: int) -> None:
        self._cooldown_dir.mkdir(parents=True, exist_ok=True)
        self._tier_state_path(asset_group).write_text(json.dumps({"tier_index": tier_index}), encoding="utf-8")

    def machine_tier(self, tier_index: int) -> tuple[str, str]:
        """``(cpu, memory)`` for a tier index, clamped into the registry's bounds."""
        return _MACHINE_TIER_REGISTRY[min(max(tier_index, 0), _TOP_TIER_INDEX)]

    def dry_run_plan(self, asset_group: str, *, oom: bool = False) -> dict[str, str]:
        plan = {
            "action": "relaunch_consolidator",
            "asset_group": asset_group,
            "job_name": self.job_name(asset_group),
            "region": self._region,
            "effect": "re-execute the manifest-consolidator Cloud Run Job (re-derives _index)",
        }
        if oom:
            next_index = self.current_tier_index(asset_group) + 1
            if next_index > _TOP_TIER_INDEX:
                plan["oom_effect"] = "escalation exhausted at the top machine tier — would PAGE, not relaunch"
            else:
                cpu, memory = _MACHINE_TIER_REGISTRY[next_index]
                plan["oom_effect"] = f"would bump the job to cpu={cpu} memory={memory} before re-executing"
        return plan

    def relaunch(
        self, asset_group: str, *, dry_run: bool = False, oom: bool = False
    ) -> dict[str, str | bool | int | None]:
        """Re-execute the per-AG consolidator job, bounded by the per-AG cooldown.

        ``oom=True`` is the AUTO-ESCALATE safety net (operator idea 2026-06-24):
        the caller has diagnosed an OOM signature (terminal exit 137/signal-9 with
        the manifest index un-advanced) on the LAST run, so a same-tier relaunch
        would just re-OOM. Before re-executing, this walks ``_MACHINE_TIER_REGISTRY``
        one rung up (persisted per-AG, sticky across calls) and bumps the Job's
        cpu/memory limits to match. Once the top rung is already active, this does
        NOT relaunch at all — it PAGES (``CONSOLIDATOR_DOWN`` CRITICAL) so a
        genuinely-oversized workload escalates to a human instead of looping
        forever re-executing at the ceiling.

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
            plan = self.dry_run_plan(asset_group, oom=oom)
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
        escalated: tuple[str, str] | None = None

        if oom:
            next_index = self.current_tier_index(asset_group) + 1
            if next_index > _TOP_TIER_INDEX:
                top_cpu, top_memory = _MACHINE_TIER_REGISTRY[_TOP_TIER_INDEX]
                log_event(
                    CONSOLIDATOR_DOWN,
                    severity="CRITICAL",
                    details={
                        "asset_group": asset_group,
                        "job_name": job,
                        "reason": "oom_escalation_exhausted",
                        "top_tier_cpu": top_cpu,
                        "top_tier_memory": top_memory,
                        "detail": (
                            f"consolidator OOM'd again at the top machine tier "
                            f"(cpu={top_cpu}, memory={top_memory}) — no further "
                            "escalation available; paging rather than relaunch-looping"
                        ),
                    },
                )
                return {
                    "status": "PAGED",
                    "asset_group": asset_group,
                    "job_name": job,
                    "detail": "OOM at top machine tier — escalation exhausted, paged (no relaunch)",
                }
            cpu, memory = _MACHINE_TIER_REGISTRY[next_index]
            try:
                self._update_job_resources(
                    job, project_id=self._project_id, region=self._region, cpu=cpu, memory=memory
                )
            except Exception as exc:  # SDK failure → FAILED (file_issue fallthrough)
                logger.warning("relaunch_consolidator: update_job_resources(%s) failed: %s", job, exc)
                return {
                    "status": "FAILED",
                    "asset_group": asset_group,
                    "job_name": job,
                    "detail": f"update_job_resources failed: {exc!r}"[:500],
                }
            self._write_tier_index(asset_group, next_index)
            escalated = (cpu, memory)

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
                **(
                    {"escalated_tier_cpu": escalated[0], "escalated_tier_memory": escalated[1]}
                    if escalated is not None
                    else {}
                ),
            },
        )
        return {
            "status": "SUCCEEDED",
            "asset_group": asset_group,
            "job_name": job,
            **(
                {"escalated_tier_cpu": escalated[0], "escalated_tier_memory": escalated[1]}
                if escalated is not None
                else {}
            ),
            "execution": execution_name,
        }

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
    parser.add_argument(
        "--oom",
        action="store_true",
        help="escalate to the next machine tier before relaunching (OOM AUTO-ESCALATE safety net)",
    )
    args = parser.parse_args(argv)
    actuator = RelaunchConsolidator(region=args.region)
    result = actuator.relaunch(args.asset_group, dry_run=args.dry_run, oom=args.oom)
    logger.info("relaunch_consolidator result=%s", result)
    return 0 if result.get("status") in ("SUCCEEDED", "DRY_RUN") else 1


if __name__ == "__main__":
    sys.exit(main())
