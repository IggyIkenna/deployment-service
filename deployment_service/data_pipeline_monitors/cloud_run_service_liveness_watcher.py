# Epic: observability_master
# Lifecycle: permanent
"""DP-VM-012 — Cloud Run Service liveness and OOM detector.

Closes the alert-coverage gap identified in the 2026-08-07 infra-health audit:
the entire DP-VM detector family is GCE-VM-specific or narrowly wired to ONE
Cloud Run *Job* family (``manifest-consolidator-{ag}``). No registry, rule, or
GCP monitoring policy exists for Cloud Run *Services* outside the 5 hand-picked
``critical_service_uptime.tf`` targets.

**What this watcher detects**, for every :class:`CloudRunServiceTarget` in
``cloud_run_service_registry.CLOUD_RUN_SERVICES``:

  1. **Terminal condition FAILED** — the service's ``terminal_condition.state``
     is not SUCCEEDED (typically a crash-loop, wrong config, or OOM at startup).
  2. **OOM signature** — the service or its latest revision's conditions carry
     a known OOM indicator (``signal: 9`` / ``exit_code=137`` / ``out of memory``
     / ``OOMKilled``).
  3. **Zero ready instances when min_scale > 0** — the service reports fewer
     ready instances than its registered ``min_scale``. Covers services that
     never started (``central-market-data-tardis-loader`` pattern).

The watcher emits ``DP_CLOUD_RUN_SERVICE_DOWN`` (DP-VM-012, CRITICAL,
PAGE_OPERATOR) through the standard ``emit_finding`` → ``route_finding`` spine,
gated on ``MissTracker`` so a transient blip (a single failed health-check
during a deploy) does not page. Default consecutive-miss threshold = 2.

**Injectable callables** keep the pure check credential-free for unit tests.
The real Cloud Run SDK reader (``make_cloud_run_service_reader``) is a
deferred import of ``google.cloud.run_v2`` so it doesn't crash the monitor on
runtimes where the SDK is absent.

SSOT:
``plans/active/issues/infra_health_audit_alert_coverage_gaps_2026_08_07.md``
todo 1.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from unified_trading_library.events import DP_CLOUD_RUN_SERVICE_DOWN  # noqa: qg-deep-import

from deployment_service.cloud_run_service_registry import CloudRunServiceTarget
from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding
from deployment_service.data_pipeline_monitors.meta_watchers import (
    DEFAULT_MIN_CONSECUTIVE_MISSES,
    MissTracker,
    emit_finding,
)

logger = logging.getLogger(__name__)

# The event constant string — used in PipelineFinding. The import above makes
# the UAC-exported name available for static analysis; for log_event we pass
# the string (consistent with how other local-string-constant events work).
_EVENT = DP_CLOUD_RUN_SERVICE_DOWN

# Canonical OOM-signature substrings (lowercased) — mirrored from consolidator_oom_watcher
# to avoid a cross-module private import (that would be a reportPrivateUsage violation).
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
    "oom",
)


# ---------------------------------------------------------------------------
# Injectable callable type aliases (for credential-free unit tests).
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ServiceHealthSnapshot:
    """Snapshot of a Cloud Run Service's health state.

    Returned by :class:`CloudRunServiceReader` per target. ``None`` fields
    mean the information was unavailable (API error / service not found).

    Attributes:
        name: full service name as read from the Cloud Run API.
        terminal_condition_state: the service's top-level condition state
            string (e.g. ``"CONDITION_SUCCEEDED"``, ``"CONDITION_FAILED"``).
        condition_messages: concatenated condition reason + message strings
            (lowercased) for OOM-signature scanning.
        ready_instance_count: number of READY instances (``None`` = unknown).
        found: False when the service does not exist in the project/location
            (API returned NOT_FOUND — skip rather than alert).
    """

    name: str
    terminal_condition_state: str | None
    condition_messages: str
    ready_instance_count: int | None
    found: bool


# ``(project_id, env_prefix, targets) -> {target.name: snapshot | None}``
# ``None`` per target = API error, not a service-not-found (which is ``found=False``).
CloudRunServiceReader = Callable[
    [str, str, Iterable[CloudRunServiceTarget]],
    dict[str, ServiceHealthSnapshot | None],
]


# ---------------------------------------------------------------------------
# OOM / crash-loop detection helpers.
# ---------------------------------------------------------------------------

def _is_terminal_failed(state: str | None) -> bool:
    """True when the service's terminal condition is in a non-succeeded state.

    Cloud Run terminal condition states that indicate the service is NOT
    healthy: ``CONDITION_FAILED``, ``CONDITION_UNKNOWN`` (pending or broken),
    ``CONDITION_PENDING`` (still trying — alert if sustained across sweeps).
    We treat anything except explicit ``CONDITION_SUCCEEDED`` as unhealthy so
    a brand-new state value in a future SDK version is flagged, not silently
    accepted as healthy.
    """
    if state is None:
        return False  # no information — skip (fail toward no false alert)
    return state.upper() != "CONDITION_SUCCEEDED"


def _has_oom_signature(condition_messages: str) -> str:
    """Return the matched OOM-signature substring, or ``""`` if none found.

    Scans the lowercased concatenation of all condition reason+message strings
    for the same known OOM signatures used by ``consolidator_oom_watcher``.
    """
    text = condition_messages.lower()
    for sig in _OOM_SIGNATURE_SUBSTRINGS:
        if sig in text:
            return sig
    return ""


def _miss_key(target: CloudRunServiceTarget) -> str:
    return f"CR_SERVICE_DOWN::{target.name}"


# ---------------------------------------------------------------------------
# Main check function.
# ---------------------------------------------------------------------------

def check_cloud_run_service_liveness(
    *,
    targets: Iterable[CloudRunServiceTarget],
    service_reader: CloudRunServiceReader,
    project_id: str,
    env_prefix: str = "",
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> dict[str, dict[str, object]]:
    """DP-VM-012 — detect Cloud Run Service liveness failures and OOM crashes.

    For each :class:`CloudRunServiceTarget` in ``targets``:

    1. Reads the service health snapshot via the injected ``service_reader``.
    2. Skips if ``found=False`` (service doesn't exist — not an alert condition).
    3. Flags DOWN when EITHER the terminal condition is not SUCCEEDED, OR when
       ``ready_instance_count < target.min_scale``.
    4. Classifies OOM when the conditions carry an OOM-signature substring.
    5. Gates on ``MissTracker`` (``min_consecutive`` sweeps) to suppress
       transient blips during deploys.
    6. Emits ``DP_CLOUD_RUN_SERVICE_DOWN`` (CRITICAL, PAGE_OPERATOR) via
       ``emit_finding`` → ``route_finding`` for persistent failures.

    Returns ``{target.name: finding_details}`` — a non-empty sub-dict means
    the finding fired for that service (useful for testing / logging).
    """
    targets_list = list(targets)
    snapshots = service_reader(project_id, env_prefix, targets_list)

    findings: dict[str, dict[str, object]] = {}
    for target in targets_list:
        snapshot = snapshots.get(target.name)
        miss_key = _miss_key(target)

        if snapshot is None:
            # API error reading this service — skip (fail toward no false alert).
            logger.info("cloud_run_service_liveness_watcher: %s unreadable (API error) — skipping", target.name)
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[target.name] = {}
            continue

        if not snapshot.found:
            # Service does not exist in this project/region — skip.
            logger.info("cloud_run_service_liveness_watcher: %s not found — skipping", target.name)
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[target.name] = {}
            continue

        # --- Determine whether the service is down. -----------------------
        terminal_failed = _is_terminal_failed(snapshot.terminal_condition_state)
        below_min_scale = (
            snapshot.ready_instance_count is not None
            and snapshot.ready_instance_count < target.min_scale
        )
        is_down = terminal_failed or below_min_scale

        if not is_down:
            if miss_tracker is not None:
                miss_tracker.register(miss_key, stale=False)
            findings[target.name] = {}
            continue

        # --- Classify the failure mode. -----------------------------------
        oom_signature = _has_oom_signature(snapshot.condition_messages)

        # --- Consecutive-miss gating. -------------------------------------
        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "cloud_run_service_liveness_watcher: %s DOWN below consecutive-miss "
                    "threshold (%d/%d) — not paging yet",
                    target.name,
                    misses,
                    min_consecutive,
                )
                findings[target.name] = {}
                continue

        # --- Build and emit the finding. ----------------------------------
        failure_reason = (
            "OOM" if oom_signature
            else "terminal_condition_failed" if terminal_failed
            else f"ready_instances={snapshot.ready_instance_count}<min_scale={target.min_scale}"
        )
        summary = (
            f"{target.name} DOWN: {failure_reason}"
            + (f" (oom_signature={oom_signature!r})" if oom_signature else "")
        )
        details: dict[str, object] = {
            "service_name": target.name,
            "description": target.description,
            "terminal_condition_state": snapshot.terminal_condition_state or "unknown",
            "ready_instance_count": snapshot.ready_instance_count,
            "min_scale": target.min_scale,
            "oom": bool(oom_signature),
            "oom_signature": oom_signature,
            "failure_reason": failure_reason,
        }
        emit_finding(
            PipelineFinding(
                event=_EVENT,
                severity="CRITICAL",
                tier=EscalationTier.PAGE_OPERATOR,
                summary=summary,
                details=details,
                registry_id="DP-VM-012",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
        findings[target.name] = details

    return findings


# ---------------------------------------------------------------------------
# Real Cloud Run SDK reader (credential-bound; deferred import).
# ---------------------------------------------------------------------------

def make_cloud_run_service_reader() -> CloudRunServiceReader:
    """Return a Cloud Run Services SDK reader for production use.

    Deferred-import of ``google.cloud.run_v2`` (credential-bound SDK boundary).
    On SDK import failure (e.g. missing dependency in the runtime image) returns
    a reader that returns ``None`` for every target — fail toward silence on a
    transient API unavailability rather than crashing the sweep.

    The reader calls ``run_v2.ServicesClient().get_service(name=...)`` per target
    and translates the proto response into a :class:`ServiceHealthSnapshot`:

    - ``terminal_condition.state`` → ``terminal_condition_state``
    - All ``conditions[].reason + " " + conditions[].message`` → ``condition_messages``
    - ``reconciling`` ignored (covered by terminal_condition already)
    - ``scaling.min_instance_count`` is NOT re-read here — we trust the registry's
      declared ``min_scale`` (the source of truth; avoids a second API call).
    """
    import importlib  # noqa: imports-inside-functions

    try:
        run_mod = importlib.import_module("google.cloud.run_v2")  # noqa: imports-inside-functions
        services_client = run_mod.ServicesClient()  # pyright: ignore[reportAny]
    except Exception as exc:
        logger.info(
            "cloud_run_service_liveness_watcher: run_v2 SDK unavailable "
            "(DP-VM-012 off this sweep): %s",
            exc,
        )
        return lambda _project, _prefix, _targets: {}

    def _read(
        project_id: str,
        env_prefix: str,
        targets: Iterable[CloudRunServiceTarget],
    ) -> dict[str, ServiceHealthSnapshot | None]:
        if not project_id:
            return {}
        result: dict[str, ServiceHealthSnapshot | None] = {}
        for target in targets:
            # Cloud Run service names in terraform carry an ``${local.env_prefix}-``
            # template prefix that resolves to e.g. ``prd-`` at deploy time.
            # Prepend it when an env_prefix is provided.
            full_name = f"{env_prefix}{target.name}" if env_prefix else target.name
            resource = f"projects/{project_id}/locations/{target.location}/services/{full_name}"
            try:
                svc = services_client.get_service(name=resource)  # pyright: ignore[reportAny]
            except Exception as exc:
                exc_str = str(exc).lower()
                if "not found" in exc_str or "404" in exc_str:
                    result[target.name] = ServiceHealthSnapshot(
                        name=full_name,
                        terminal_condition_state=None,
                        condition_messages="",
                        ready_instance_count=None,
                        found=False,
                    )
                else:
                    logger.info(
                        "cloud_run_service_liveness_watcher: get_service(%s) failed: %s",
                        full_name,
                        exc,
                    )
                    result[target.name] = None
                continue

            # --- terminal_condition ----------------------------------------
            tc = getattr(svc, "terminal_condition", None)  # pyright: ignore[reportAny]
            tc_state: str | None = None
            if tc is not None:
                state_val = getattr(tc, "state", None)  # pyright: ignore[reportAny]
                if state_val is not None:
                    tc_state = (  # pyright: ignore[reportAny]
                        str(state_val.name)  # pyright: ignore[reportAny]
                        if hasattr(state_val, "name")
                        else str(state_val)  # pyright: ignore[reportAny]
                    )

            # --- conditions (for OOM scanning) ----------------------------
            cond_parts: list[str] = []
            for cond in getattr(svc, "conditions", []) or []:  # pyright: ignore[reportAny]
                reason = str(getattr(cond, "reason", "") or "")  # pyright: ignore[reportAny]
                message = str(getattr(cond, "message", "") or "")  # pyright: ignore[reportAny]
                if reason or message:
                    cond_parts.append(f"{reason} {message}")
            # Also scan the terminal condition itself.
            if tc is not None:
                tc_reason = str(getattr(tc, "reason", "") or "")  # pyright: ignore[reportAny]
                tc_message = str(getattr(tc, "message", "") or "")  # pyright: ignore[reportAny]
                if tc_reason or tc_message:
                    cond_parts.append(f"{tc_reason} {tc_message}")

            # --- ready instances ------------------------------------------
            # ``observed_generation`` + ``traffic[*].percent`` doesn't directly
            # give us a ready-instance count. Cloud Run surfaces ready instances
            # via ``Service.reconciling`` (bool) and via monitoring metrics, but
            # not a ready_instance_count field in the service proto itself.
            # We approximate via the terminal condition state: a service in
            # CONDITION_FAILED / CONDITION_PENDING has 0 effective ready
            # instances, so a None here (unknown) is safe — the terminal-
            # condition check already flags it.
            # For min_scale=2 services (central-market-data-tardis-loader) the
            # terminal condition FAILED is the right signal — we pass None so
            # the terminal-condition check fires and the below_min_scale path
            # doesn't add noise from an imprecise count.
            ready: int | None = None

            result[target.name] = ServiceHealthSnapshot(
                name=full_name,
                terminal_condition_state=tc_state,
                condition_messages=" ".join(cond_parts).lower(),
                ready_instance_count=ready,
                found=True,
            )
        return result

    return _read


__all__ = [
    "CloudRunServiceReader",
    "ServiceHealthSnapshot",
    "check_cloud_run_service_liveness",
    "make_cloud_run_service_reader",
]
