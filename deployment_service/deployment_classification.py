"""Deployment classification spine — the umbrella SSOT every observability surface reads.

Every compute unit (a **VM** or a **Cloud Run job/execution**) classifies to exactly
one ``DeploymentTarget`` value-object: ``umbrella`` x ``cloud`` x ``service`` x
``asset_group``. The umbrella (``live`` / ``batch`` / ``paper`` / ``experiment``)
derives from the VM ``lifecycle_class`` (UAC ``UMBRELLA_FOR_LIFECYCLE_CLASS``) with two
overrides:

* the **paper** launcher prefixes (``defi-paper-`` / ``funding-ensemble-paper-`` /
  ``strategy-paper-``) classify ``paper`` regardless of their lifecycle_class
  (they are SCHEDULED_RECURRING / LONG_LIVED by lifecycle but ``paper`` by umbrella);
* an explicit ``is_paper=True`` from the caller forces ``paper``.

There is **no silent default** — an unresolvable name raises
``UnclassifiedDeploymentError`` (the "added a launcher / Cloud Run job, forgot to
register it" guard, extended from VM prefixes to umbrellas).

SSOT: ``plans/active/deployment_observability_parity_live_batch_paper_2026_06_22.md``
Phase 0.
"""

from __future__ import annotations

from collections.abc import Mapping

from unified_api_contracts import (
    UMBRELLA_FOR_LIFECYCLE_CLASS,
    DeploymentCloud,
    DeploymentKind,
    DeploymentTarget,
    DeploymentUmbrella,
)

# ---------------------------------------------------------------------------
# Paper-launcher prefixes (umbrella override). These VMs are SCHEDULED_RECURRING /
# LONG_LIVED by lifecycle_class but PAPER by umbrella, so they must be resolved
# BEFORE the lifecycle_class lookup. SSOT: the launcher set in the plan grounding +
# vm_zombie_watchdog.VM_PREFIX_TO_BUCKET.
# ---------------------------------------------------------------------------
PAPER_PREFIXES: tuple[str, ...] = (
    "defi-paper-",
    "funding-ensemble-paper-",
    "strategy-paper-",
)

# Asset-group tokens that may appear anywhere in a VM/job name segment. Order does
# not matter (each token is unique). cefi/defi/tradfi/sports/prediction are the
# canonical lowercase asset_group keys (CLAUDE.md § asset-group vocabulary).
_ASSET_GROUP_TOKENS: tuple[str, ...] = ("cefi", "defi", "tradfi", "sports", "prediction")

# Explicit name-segment → asset_group aliases for prefixes that name a venue/source
# rather than the asset_group token directly (e.g. sports source launchers).
_SERVICE_ALIASES: dict[str, str] = {
    "api-football": "sports",
    "footystats": "sports",
    "fss": "sports",
    "sfi": "sports",
    "tm": "sports",
    "af": "sports",
    "fs": "sports",
    "weather": "sports",
    "us": "sports",
}


class UnclassifiedDeploymentError(ValueError):
    """Raised when a deployment name resolves to no umbrella / asset_group.

    The canonical "added a launcher / Cloud Run job, forgot to classify it" guard.
    Carries the offending ``name`` so the guard test + alerting can name the gap.
    """

    def __init__(self, name: str, *, reason: str = "") -> None:
        self.deployment_name = name
        suffix = f" ({reason})" if reason else ""
        super().__init__(f"Cannot classify deployment target {name!r}{suffix}")


def _resolve_umbrella(
    name: str,
    *,
    lifecycle_class: str | None,
    is_paper: bool | None,
) -> DeploymentUmbrella:
    """Resolve the umbrella from is_paper → paper-prefix → lifecycle_class.

    Raises ``UnclassifiedDeploymentError`` when neither the paper signal nor a known
    lifecycle_class resolves — never a silent default.
    """
    if is_paper or name.startswith(PAPER_PREFIXES):
        return DeploymentUmbrella.PAPER
    if lifecycle_class is None:
        raise UnclassifiedDeploymentError(name, reason="no lifecycle_class and not paper")
    umbrella = UMBRELLA_FOR_LIFECYCLE_CLASS.get(lifecycle_class)
    if umbrella is None:
        raise UnclassifiedDeploymentError(name, reason=f"unknown lifecycle_class {lifecycle_class!r}")
    return umbrella


def _derive_asset_group(name: str) -> str:
    """Derive the asset_group from a VM/job name's segments.

    Cross-asset / asset-group-agnostic targets (audits, consolidator coordinators,
    monitors) return ``""`` — they are not scoped to one asset_group. Raises only via
    the caller when neither a token nor an alias matches AND the caller requires one.
    """
    segments = name.split("-")
    for seg in segments:
        if seg in _ASSET_GROUP_TOKENS:
            return seg
    # alias match on a leading multi-segment token (e.g. "api-football-...").
    for alias, ag in _SERVICE_ALIASES.items():
        if name.startswith(f"{alias}-"):
            return ag
    return ""


def _derive_service(name: str) -> str:
    """Derive the owning service stem from a VM/job name (the leading non-ag stem).

    Strips a trailing ``-`` and any ``-{asset_group}`` segment, returning the launcher
    family (e.g. ``mtds-backfill``, ``manifest-consolidator``, ``strategy-paper``).
    """
    stem = name.rstrip("-")
    for token in (*_ASSET_GROUP_TOKENS,):
        stem = stem.replace(f"-{token}-", "-").removesuffix(f"-{token}")
    return stem or name.rstrip("-")


def classify_deployment_target(
    name: str,
    *,
    lifecycle_class: str | None = None,
    cloud: DeploymentCloud = DeploymentCloud.GCP,
    kind: DeploymentKind = DeploymentKind.VM,
    is_paper: bool | None = None,
    asset_group: str | None = None,
    service: str | None = None,
) -> DeploymentTarget:
    """Classify a VM-prefix or Cloud-Run-job name into a ``DeploymentTarget``.

    The single resolver both the zombie watchdog and deployment-api call.

    Umbrella resolution (in order): ``is_paper`` OR a PAPER_PREFIXES match → ``paper``;
    else ``UMBRELLA_FOR_LIFECYCLE_CLASS[lifecycle_class]``. A ``None`` / unknown
    lifecycle_class that is not paper raises ``UnclassifiedDeploymentError``.

    ``asset_group`` / ``service`` default to a derivation from the name segments; a
    caller (the Cloud Run job registry) may pass them explicitly. Cross-asset targets
    carry ``asset_group=""``.

    Raises ``UnclassifiedDeploymentError`` when nothing resolves — never a silent
    default.
    """
    if not name:
        raise UnclassifiedDeploymentError(name, reason="empty name")

    umbrella = _resolve_umbrella(name, lifecycle_class=lifecycle_class, is_paper=is_paper)
    resolved_ag = asset_group if asset_group is not None else _derive_asset_group(name)
    resolved_service = service if service is not None else _derive_service(name)

    return DeploymentTarget(
        name=name,
        kind=kind,
        umbrella=umbrella,
        cloud=cloud,
        service=resolved_service,
        asset_group=resolved_ag,
        lifecycle_class=(lifecycle_class if lifecycle_class is not None else umbrella.value),
    )


def umbrella_for_vm_name(
    vm_name: str,
    prefix_registry: Mapping[str, object | None],
) -> DeploymentUmbrella:
    """Resolve a VM name's deployment umbrella via longest-prefix-match on the registry.

    The single resolver the deployment **emitters** (the VM-side
    ``deployment_heartbeat`` DEPLOYMENT_* events + the exit-code fleet monitor's
    DP_VM_* findings) call to STAMP the umbrella on each alert payload, so the
    alerting-service router can split LIVE → ``#uts-live-alerts`` vs BATCH →
    ``#data-pipeline-alerts`` (operator 2026-06-23 routing contract).

    Resolution: longest-prefix-match `vm_name` against ``prefix_registry`` (typically
    ``vm_zombie_watchdog.VM_PREFIX_TO_BUCKET``), then dispatch through
    ``classify_deployment_target`` so the PAPER override + the lifecycle→umbrella map
    are applied identically to every other surface (no re-derivation). The spec's own
    ``umbrella`` override (paper prefixes) wins when present.

    Raises ``UnclassifiedDeploymentError`` when no prefix matches OR the matched spec's
    lifecycle_class is unknown — never a silent default (the same "register the prefix"
    guard every other classification surface enforces).
    """
    if not vm_name:
        raise UnclassifiedDeploymentError(vm_name, reason="empty vm_name")
    best_match: str | None = None
    for prefix in prefix_registry:
        if vm_name.startswith(prefix) and (best_match is None or len(prefix) > len(best_match)):
            best_match = prefix
    if best_match is None:
        raise UnclassifiedDeploymentError(
            vm_name, reason="no registered prefix; add a VmPrefixSpec to VM_PREFIX_TO_BUCKET"
        )
    spec = prefix_registry[best_match]
    if spec is None:
        raise UnclassifiedDeploymentError(vm_name, reason=f"prefix {best_match!r} maps to None (no spec)")
    # An explicit per-spec umbrella override (the paper prefixes) wins outright.
    spec_umbrella: object = getattr(spec, "umbrella", None)
    if isinstance(spec_umbrella, DeploymentUmbrella):
        return spec_umbrella
    lifecycle: object = getattr(spec, "lifecycle_class", None)
    lifecycle_str: str | None = None
    if isinstance(lifecycle, str):
        # LifecycleClass is a StrEnum → str(value) is the canonical key.
        lifecycle_str = str(lifecycle)
    elif lifecycle is not None:
        lifecycle_str = str(lifecycle)
    return classify_deployment_target(vm_name, lifecycle_class=lifecycle_str).umbrella
