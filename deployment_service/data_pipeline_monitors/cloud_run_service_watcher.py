# Epic: observability_master
# Lifecycle: permanent
"""DP-VM-012 — Cloud Run Service liveness + OOM detector.

Gap: infra_health_audit_alert_coverage_gaps_2026_08_07.md finding A — the entire
DP-VM registry (DP-VM-001..011) covers GCE VMs and the manifest-consolidator Cloud
Run Job family only; Cloud Run **Services** (long-running HTTP endpoints) had zero
detector coverage. Three services were silently broken for 9.5-19 months:

  - ``market-data-query-service``  — crash-loop (wrong bucket, since 2025-10-20)
  - ``central-market-data-tardis-loader`` — never-started 19mo (minScale=2 boot fail)
  - ``uts-prod-data-status-rollup-svc`` — OOM at 32Gi/8vCPU ceiling (ongoing)

For each service in ``cloud_run_service_registry.CLOUD_RUN_SERVICES``:

  1. Reads the service's ``terminal_condition`` via ``run_v2.ServicesClient.get_service()``.
  2. Fires ``DP_CLOUD_RUN_SERVICE_DOWN`` (CRITICAL, PAGE_OPERATOR, DP-VM-012) when
     ``terminal_condition.state == CONDITION_FAILED`` or the condition message/reason
     contains a known OOM signature.
  3. Resets the miss counter when the service is healthy (CONDITION_SUCCEEDED).

MissTracker gating: a single-sweep transient does not page; the condition must hold for
``min_consecutive`` consecutive sweeps (same pattern as consolidator_oom_watcher /
stale_image_watcher).

SDK import: ``google.cloud.run_v2`` is deferred (credential-bound boundary).  An import
failure returns ``None`` per service and silences the watcher for the sweep — fail toward
no false alert, same as the sibling watchers.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable

from unified_trading_library.events import DP_CLOUD_RUN_SERVICE_DOWN  # noqa: qg-deep-import

from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding
from deployment_service.data_pipeline_monitors.meta_watchers import (
    DEFAULT_MIN_CONSECUTIVE_MISSES,
    MissTracker,
    emit_finding,
)

logger = logging.getLogger(__name__)

# Known OOM-signature substrings in Cloud Run Service condition messages/reasons.
# Same set as consolidator_oom_watcher._OOM_SIGNATURE_SUBSTRINGS (kept local to avoid
# cross-module coupling — both are narrowly scoped to the OOM-detection use case).
_OOM_SIGNATURE_SUBSTRINGS: tuple[str, ...] = (
    "signal: 9",
    "signal 9",
    "sigkill",
    "exit code 137",
    "exit_code=137",
    "out of memory",
    "oomkill",
    "oom killed",
    "memory limit exceeded",
    "memorylimit exceeded",
    "container killed",
)

# The Cloud Run v2 CONDITION_FAILED state string (after stripping the enum-class prefix
# that the proto serialiser sometimes prepends, e.g. "State.CONDITION_FAILED").
_CONDITION_FAILED = "CONDITION_FAILED"

# Callable: (project_id, location, service_name) -> (state, reason, message) | None.
# None = service not found / API error → skip this sweep for that service.
ServiceConditionReader = Callable[[str, str, str], tuple[str, str, str] | None]


def _miss_key(service_name: str) -> str:
    return f"DP_CLOUD_RUN_SERVICE_DOWN::{service_name}"


def check_cloud_run_service_liveness(
    *,
    service_names: Iterable[str],
    condition_reader: ServiceConditionReader,
    project_id: str,
    location: str = "asia-northeast1",
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> dict[str, dict[str, object]]:
    """DP-VM-012 — detect Cloud Run Services in CONDITION_FAILED state or OOM.

    For each service name, reads ``terminal_condition`` via the injected
    ``condition_reader`` and emits ``DP_CLOUD_RUN_SERVICE_DOWN`` (CRITICAL,
    PAGE_OPERATOR) when the state is CONDITION_FAILED or the message/reason contains
    an OOM signature. Resets the miss counter when the service is healthy.

    Returns ``{service_name: finding_details | {}}`` (empty dict = no finding for
    that service this sweep).
    """
    findings: dict[str, dict[str, object]] = {}
    for svc in service_names:
        miss_key = _miss_key(svc)
        result = condition_reader(project_id, location, svc)

        if result is None:
            # API unavailable or service not found — skip; do NOT reset the miss
            # counter so a genuine outage isn't suppressed by API unavailability.
            findings[svc] = {}
            continue

        state, reason, message = result

        # Determine failure class.
        is_failed = state == _CONDITION_FAILED
        combined = f"{reason} {message}".lower()
        oom_reason = next((sig for sig in _OOM_SIGNATURE_SUBSTRINGS if sig in combined), "")
        is_oom = bool(oom_reason)

        if not (is_failed or is_oom):
            # Service is healthy — reset the miss counter.
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[svc] = {}
            continue

        # Service is unhealthy — apply miss-tracker gating.
        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "cloud_run_service_watcher: DP_CLOUD_RUN_SERVICE_DOWN for '%s' below "
                    "consecutive-miss threshold (%d/%d) — not paging yet",
                    svc,
                    misses,
                    min_consecutive,
                )
                findings[svc] = {}
                continue

        failure_kind = "OOM" if is_oom and not is_failed else ("CONDITION_FAILED+OOM" if is_oom else "CONDITION_FAILED")
        summary = (
            f"Cloud Run Service '{svc}' is DOWN ({failure_kind}): "
            f"state={state!r} reason={reason!r} message={message[:200]!r}"
        )
        details: dict[str, object] = {
            "service_name": svc,
            "state": state,
            "reason": reason,
            "message": message[:500],
            "failure_kind": failure_kind,
            "oom_reason": oom_reason,
        }
        emit_finding(
            PipelineFinding(
                event=DP_CLOUD_RUN_SERVICE_DOWN,
                severity="CRITICAL",
                tier=EscalationTier.PAGE_OPERATOR,
                summary=summary,
                details=details,
                registry_id="DP-VM-012",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
        findings[svc] = details

    return findings


def make_service_condition_reader() -> ServiceConditionReader:
    """Return ``(project_id, location, service_name) -> (state, reason, message) | None``.

    Reads the Cloud Run Service's ``terminal_condition`` via ``run_v2.ServicesClient``.
    Returns ``None`` when the service is not found, the API errors, or the SDK import
    fails — fail toward silence (no false alert on a transient API blip).

    ``state`` is normalised: the proto serialiser sometimes returns
    ``"State.CONDITION_FAILED"``; the dot-suffix is stripped to plain
    ``"CONDITION_FAILED"`` so downstream comparisons work reliably.

    Deferred-import of ``google.cloud.run_v2`` (credential-bound SDK boundary, same
    pattern as ``consolidator_oom_watcher.make_consolidator_execution_oom_reader``).
    """
    import importlib  # noqa: imports-inside-functions

    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions
        services_client = run_mod.ServicesClient()  # pyright: ignore[reportAny]
    except Exception as exc:
        logger.info("cloud-run-service reader unavailable (DP-VM-012 off this sweep): %s", exc)
        return lambda _proj, _loc, _svc: None

    def _read(project_id: str, location: str, service_name: str) -> tuple[str, str, str] | None:
        if not project_id:
            return None
        name = f"projects/{project_id}/locations/{location}/services/{service_name}"
        try:
            service = services_client.get_service(name=name)  # pyright: ignore[reportAny]
            condition = getattr(service, "terminal_condition", None)  # pyright: ignore[reportAny]
            if condition is None:
                return None
            raw_state = str(getattr(condition, "state", "") or "")  # pyright: ignore[reportAny]
            # Normalise "State.CONDITION_FAILED" → "CONDITION_FAILED".
            state = raw_state.rsplit(".", 1)[-1] if "." in raw_state else raw_state
            reason = str(getattr(condition, "reason", "") or "")  # pyright: ignore[reportAny]
            message = str(getattr(condition, "message", "") or "")  # pyright: ignore[reportAny]
            return state, reason, message
        except Exception as exc:
            logger.info("cloud-run-service lookup for %s -> unknown: %s", service_name, exc)
            return None

    return _read


def service_names_from_registry() -> list[str]:
    """Return service names from ``cloud_run_service_registry.CLOUD_RUN_SERVICES``."""
    from deployment_service.cloud_run_service_registry import CLOUD_RUN_SERVICES  # noqa: imports-inside-functions

    return [t.name for t in CLOUD_RUN_SERVICES]


__all__ = [
    "ServiceConditionReader",
    "check_cloud_run_service_liveness",
    "make_service_condition_reader",
    "service_names_from_registry",
]
