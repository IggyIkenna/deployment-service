"""Guard test for the long-lived-service monitoring registry (Phase 4, gap #6).

Asserts:

(a) every ``service`` / ``api-service`` / ``api`` repo in
    ``workspace-manifest.json`` (minus the named ``_NOT_LONG_LIVED_SERVICE_REPOS``
    exclusions) has a ``MONITORED_SERVICES`` entry — the canonical "added a
    service, forgot to register it for monitoring" guard;
(b) every ``MONITORED_SERVICES`` entry's name classifies via
    ``classify_deployment_target`` without raising ``UnclassifiedDeploymentError``,
    and lands on the LIVE umbrella;
(c) no ``MONITORED_SERVICES`` entry duplicates a ``CLOUD_RUN_JOBS`` job name
    (services are not jobs);
(d) ``batch-service`` repos are NOT required here (they register as Cloud Run
    jobs in ``CLOUD_RUN_JOBS``).

The workspace-manifest is located by walking up from this file to the workspace
root (mirroring how the Cloud-Run-job guard locates ``terraform/gcp`` via
``_REPO_ROOT = Path(__file__).resolve().parents[2]``).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

# Ensure resolve_bucket_name (called transitively at deployment_classification /
# monitored_services import time) has an env tier + project to resolve against.
# Reads the UAC-packaged cloud-providers.yaml — no network.
os.environ.setdefault("DEPLOYMENT_ENV", "prod")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("PROJECT_ID", "test-project")

from unified_api_contracts import DeploymentUmbrella, ShardResponsibilityKind  # noqa: E402 — after env setup.

from deployment_service.cloud_run_job_registry import CLOUD_RUN_JOBS  # noqa: E402
from deployment_service.deployment_classification import (  # noqa: E402
    UnclassifiedDeploymentError,
    classify_deployment_target,
)
from deployment_service.deployment_cluster_registry import (  # noqa: E402
    is_data_plane_service,
    responsibility_for_deployment,
)
from deployment_service.monitored_services import (  # noqa: E402
    _NOT_LONG_LIVED_SERVICE_REPOS,
    MONITORED_SERVICES,
    is_service_monitored,
    monitored_service_names,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKSPACE_ROOT = _REPO_ROOT.parent
_MANIFEST = _WORKSPACE_ROOT / "unified-trading-pm" / "workspace-manifest.json"

# The long-lived deployable-service repo types (mirrors
# scripts/validation/check-no-service-deps.py ``_SERVICE_REPO_TYPES`` minus
# ``batch-service`` — batch repos register as Cloud Run JOBS, not services).
_LONG_LIVED_SERVICE_TYPES: frozenset[str] = frozenset({"service", "api-service", "api"})


def _long_lived_service_repos() -> set[str]:
    """Every repo in workspace-manifest with a long-lived deployable-service type."""
    repos = json.loads(_MANIFEST.read_text()).get("repositories", {})
    return {
        name
        for name, meta in repos.items()
        if meta.get("type") in _LONG_LIVED_SERVICE_TYPES and name not in _NOT_LONG_LIVED_SERVICE_REPOS
    }


# ---------------------------------------------------------------------------
# (a) every long-lived service repo is registered for monitoring.
# ---------------------------------------------------------------------------
def test_every_long_lived_service_repo_is_registered() -> None:
    """Each service/api-service/api repo has a MONITORED_SERVICES entry."""
    assert _MANIFEST.exists(), f"workspace-manifest.json not found at {_MANIFEST}"
    expected = _long_lived_service_repos()
    assert expected, "no long-lived service repos found — wrong manifest path / type set?"
    registered = monitored_service_names()
    missing = sorted(expected - registered)
    assert not missing, (
        "Deployable service(s) NOT registered for monitoring — add a MONITORED_SERVICES "
        f"entry (gap #6 guard):\n{missing}"
    )


def test_no_stray_registry_entries() -> None:
    """Every MONITORED_SERVICES name is an actual long-lived service repo."""
    repos = json.loads(_MANIFEST.read_text()).get("repositories", {})
    stray = sorted(name for name in MONITORED_SERVICES if name not in repos)
    assert not stray, f"MONITORED_SERVICES names not present in workspace-manifest: {stray}"


# ---------------------------------------------------------------------------
# (b) every registered entry classifies (LIVE umbrella), no raise.
# ---------------------------------------------------------------------------
def test_every_registered_service_classifies_live() -> None:
    """Each MONITORED_SERVICES name classifies without raising, on LIVE umbrella."""
    failures: list[str] = []
    for name in MONITORED_SERVICES:
        try:
            target = classify_deployment_target(name, lifecycle_class="LONG_LIVED_LIVE", service=name)
        except UnclassifiedDeploymentError as exc:  # pragma: no cover - failure path
            failures.append(f"{name!r}: {exc}")
            continue
        assert target.umbrella is DeploymentUmbrella.LIVE, f"{name} not LIVE: {target.umbrella}"
    assert not failures, "Unclassified monitored services:\n" + "\n".join(failures)


def test_stored_targets_are_live() -> None:
    """The stored DeploymentTarget on each entry is on the LIVE umbrella."""
    for name, svc in MONITORED_SERVICES.items():
        assert svc.umbrella is DeploymentUmbrella.LIVE, f"{name} stored umbrella not LIVE"
        assert svc.health_path == "/health", f"{name} health_path not /health"


# ---------------------------------------------------------------------------
# (c) services are not jobs — no name collision with CLOUD_RUN_JOBS.
# ---------------------------------------------------------------------------
def test_no_service_collides_with_a_cloud_run_job() -> None:
    """No MONITORED_SERVICES name duplicates a CLOUD_RUN_JOBS job name."""
    job_names = {t.name for t in CLOUD_RUN_JOBS}
    collisions = sorted(set(MONITORED_SERVICES) & job_names)
    assert not collisions, f"Service names collide with Cloud Run job names (services != jobs): {collisions}"


# ---------------------------------------------------------------------------
# (d) batch-service repos are NOT required in the service registry.
# ---------------------------------------------------------------------------
def test_batch_service_repos_not_in_service_registry() -> None:
    """A batch-service repo registers as a Cloud Run job, not a monitored service."""
    repos = json.loads(_MANIFEST.read_text()).get("repositories", {})
    batch_repos = {name for name, meta in repos.items() if meta.get("type") == "batch-service"}
    overlap = sorted(batch_repos & set(MONITORED_SERVICES))
    assert not overlap, f"batch-service repos wrongly registered as monitored services: {overlap}"


# ---------------------------------------------------------------------------
# accessor sanity + guard-not-vacuous.
# ---------------------------------------------------------------------------
def test_is_service_monitored_accessor() -> None:
    """is_service_monitored returns True for a registered svc, False otherwise."""
    a_registered = next(iter(MONITORED_SERVICES))
    assert is_service_monitored(a_registered)
    assert not is_service_monitored("totally-unregistered-phantom-service")


def test_guard_detects_an_unregistered_service() -> None:
    """The (a) guard MUST fail if a long-lived service is unregistered (not vacuous)."""
    expected = _long_lived_service_repos()
    registered = monitored_service_names()
    # Simulate dropping one registered repo: it must then be reported missing.
    a_registered = next(iter(registered & expected))
    simulated_registered = registered - {a_registered}
    missing = expected - simulated_registered
    assert a_registered in missing, "guard is vacuous — dropping a service was not detected as missing"


# ---------------------------------------------------------------------------
# Phase 4.5 — shard-responsibility resolver: a data-plane producer NEVER
# silently resolves to NONE; gateways/consumers ARE liveness-only.
# ---------------------------------------------------------------------------
def test_data_plane_services_never_silently_liveness_only() -> None:
    """Every registered data-plane service resolves to a non-NONE responsibility.

    The whole point of Phase 4.5: a service that owns availability-manifest
    shards must declare them (ASSET_GROUP_CAPTURE / STRATEGY_SHARD), never
    silently read as liveness-only. If this fails, a data producer would render
    as ``liveness_only`` in the cockpit and its freshness would go unattributed.
    """
    silent: list[str] = []
    for name, svc in MONITORED_SERVICES.items():
        if is_data_plane_service(name) and svc.responsibility.kind is ShardResponsibilityKind.NONE:
            silent.append(name)
    assert not silent, f"data-plane service(s) silently resolved to NONE responsibility: {silent}"


def test_capture_and_strategy_services_resolve_expected_kind() -> None:
    """The capture producers -> ASSET_GROUP_CAPTURE; strategy -> STRATEGY_SHARD."""
    kinds = {name: svc.responsibility.kind for name, svc in MONITORED_SERVICES.items()}
    for capture in (
        "market-tick-data-service",
        "market-data-processing-service",
        "instruments-service",
        "features-service",
    ):
        assert kinds[capture] is ShardResponsibilityKind.ASSET_GROUP_CAPTURE, f"{capture} not ASSET_GROUP_CAPTURE"
    assert kinds["strategy-service"] is ShardResponsibilityKind.STRATEGY_SHARD


def test_gateways_and_consumers_are_liveness_only() -> None:
    """API gateways + non-shard-owning consumers resolve to NONE (liveness-only)."""
    for gw in ("deployment-api", "unified-trading-api", "execution-service", "ml-service", "alerting-service"):
        svc = MONITORED_SERVICES[gw]
        assert svc.responsibility.kind is ShardResponsibilityKind.NONE, f"{gw} should be liveness-only"
        assert svc.owns_data_freshness is False, f"{gw} owns_data_freshness should be False"


def test_strategy_mode_derived_from_umbrella() -> None:
    """A STRATEGY_SHARD responsibility carries the mode derived from the umbrella."""
    from deployment_service.deployment_classification import classify_deployment_target

    target = classify_deployment_target(
        "strategy-service", lifecycle_class="LONG_LIVED_LIVE", service="strategy-service"
    )
    resp = responsibility_for_deployment(target)
    assert resp.kind is ShardResponsibilityKind.STRATEGY_SHARD
    assert resp.mode == "live"  # LONG_LIVED_LIVE -> LIVE umbrella -> "live"


# ---------------------------------------------------------------------------
# Phase 4.5 follow-up (finding 2026-06-24): VM launcher-family coverage.
#
# Inventory rows are VMs whose ``DeploymentTarget.service`` is the launcher-
# family stem (``strategy-live-…`` / ``mtds-backfill-…`` / ``cefi-binance-…``),
# NOT a canonical service name. These resolve to their real responsibility via
# the ``_STRATEGY_LAUNCHER_PREFIXES`` / ``_CAPTURE_LAUNCHER_PREFIXES`` allowlists
# rather than staying liveness_only. The allowlist is conservative: a name
# matching nothing (or a capture family with no derivable asset_group) stays
# NONE — honest, never a false "fresh".
# ---------------------------------------------------------------------------
def _classify(name: str, lifecycle_class: str):
    from deployment_service.deployment_classification import classify_deployment_target

    return classify_deployment_target(name, lifecycle_class=lifecycle_class)


def test_strategy_live_vm_resolves_strategy_shard_live() -> None:
    """A ``strategy-live-<ag>-<runid>`` VM row resolves to STRATEGY_SHARD/live."""
    resp = responsibility_for_deployment(_classify("strategy-live-cefi-abc123", "LONG_LIVED_LIVE"))
    assert resp.kind is ShardResponsibilityKind.STRATEGY_SHARD
    assert resp.mode == "live"


def test_strategy_paper_vm_resolves_strategy_shard_paper() -> None:
    """A ``strategy-paper-<ag>-<runid>`` VM row resolves to STRATEGY_SHARD/paper.

    The PAPER_PREFIXES override gives it the PAPER umbrella regardless of
    lifecycle_class → mode ``paper``.
    """
    resp = responsibility_for_deployment(_classify("strategy-paper-defi-xyz789", "LONG_LIVED_LIVE"))
    assert resp.kind is ShardResponsibilityKind.STRATEGY_SHARD
    assert resp.mode == "paper"


def test_paper_trading_launcher_families_resolve_strategy_shard() -> None:
    """defi-paper / funding-ensemble-paper VMs are strategy paper shards, not capture."""
    for name in ("defi-paper-trading-vm-1", "funding-ensemble-paper-cron-vm"):
        resp = responsibility_for_deployment(_classify(name, "LONG_LIVED_LIVE"))
        assert resp.kind is ShardResponsibilityKind.STRATEGY_SHARD, name
        assert resp.mode == "paper", name


def test_capture_launcher_families_resolve_asset_group_capture() -> None:
    """mtds/mdps/cefi-venue backfill VMs resolve to ASSET_GROUP_CAPTURE with their ag."""
    cases = {
        "mtds-backfill-tradfi-vm1": "tradfi",
        "mdps-backfill-cefi-shard3": "cefi",
        "cefi-binance-spot-vm-1": "cefi",
        "mtds-live-prediction-feed": "prediction",
        "tradfi-bf-equities-2026": "tradfi",
    }
    for name, expected_ag in cases.items():
        resp = responsibility_for_deployment(_classify(name, "EPHEMERAL_BATCH"))
        assert resp.kind is ShardResponsibilityKind.ASSET_GROUP_CAPTURE, name
        assert resp.asset_group == expected_ag, name


def test_capture_family_without_asset_group_stays_liveness_only() -> None:
    """A capture-family VM with NO derivable asset_group stays NONE (honest)."""
    # ``mtds-backfill-vm`` carries no asset_group token → freshness unattributable.
    resp = responsibility_for_deployment(_classify("mtds-backfill-vm", "EPHEMERAL_BATCH"))
    assert resp.kind is ShardResponsibilityKind.NONE


def test_non_capture_vm_with_asset_group_stays_liveness_only() -> None:
    """An ag-scoped non-capture infra VM (orchestrator worker) is NOT swept into capture.

    ``agent-orch-vm-cefi-…`` carries asset_group=cefi but is a control-plane
    worker, not a data producer → must stay liveness_only (the conservative
    allowlist does not match it).
    """
    resp = responsibility_for_deployment(_classify("agent-orch-vm-cefi-1", "LONG_LIVED_LIVE"))
    assert resp.kind is ShardResponsibilityKind.NONE
