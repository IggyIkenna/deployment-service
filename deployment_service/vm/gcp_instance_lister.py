"""GCE instance lister — SSOT for "what VMs are currently RUNNING in the project".

Thin wrapper around the UTL Compute Engine client
(``unified_trading_library.cloud_interface.get_compute_engine_client`` →
``ComputeEngineClient.aggregated_list_instances``) used exclusively by
``DeploymentsRegistry.reap_stale()`` to cross-reference GCS-backed
``deployments/active/`` entries against live GCE VM state. Routing through UTL
(rather than importing ``google.cloud.compute_v1`` directly) keeps the cloud
SDK confined to ``unified_cloud_interface/providers`` per the cloud-interface
SSOT (QG STEP 5.10).

Read-only, failure-isolated: any API error returns an empty set and logs a
WARN — callers treat "API failed" identically to "no VMs running" which is
the safe default for the reaper (fall-back to heartbeat-age only, never
archive something while in doubt).

The public return type is a ``set[str]`` of VM names (not self-links or
fully-qualified instance resources) because that is the form
``DeploymentRegistryEntry.vm_name`` carries.
"""

from __future__ import annotations

import logging

from unified_trading_library.cloud_interface import get_compute_engine_client  # noqa: qg-deep-import

logger = logging.getLogger(__name__)


def list_running_instances_strict(project_id: str) -> list[dict[str, object]]:
    """Return the RUNNING instance ROWS in ``project_id``, RAISING on API failure.

    Unlike :func:`list_running_vm_names_strict` this preserves each row whole —
    critically its ``metadata``, which is where the five pinning launchers
    actually record ``*_TARBALL_SHA``. Reducing to bare names here is what made
    the first attempt at pin-aware retention structurally blind: the pins never
    survived to the consumer. See ``deployment_service.vm.tarball_pins``.
    """
    client = get_compute_engine_client(provider="gcp", project_id=project_id)
    # Empty-fallback rationale: a row lacking status/name is not a RUNNING VM; "" is correctly
    # falsy and the row is filtered out. This narrows the set (never widens it),
    # so it cannot cause a pin to be missed and then deleted — the consumer's
    # fail-closed gate treats an unobservable VM as a blocker, not as absent.
    rows = [
        inst
        for inst in client.aggregated_list_instances(project_id, filter_str="")
        if str(inst.get("status", "")) == "RUNNING"  # noqa: qg-empty-fallback
        and str(inst.get("name", ""))  # noqa: qg-empty-fallback
    ]
    with_metadata = sum(1 for inst in rows if "metadata" in inst)
    logger.info(
        "list_running_instances(%s): %d RUNNING VMs (%d carrying metadata)",
        project_id,
        len(rows),
        with_metadata,
    )
    return rows


def list_running_vm_names_strict(project_id: str) -> set[str]:
    """Return the RUNNING VM names in ``project_id``, RAISING on API failure.

    The fail-closed variant. Callers that make a DESTRUCTIVE decision from this
    set (``scripts/vm/cleanup_old_tarballs.py`` deleting SHA-pinned code
    tarballs) must be able to tell "the API failed" apart from "nothing is
    running" — collapsing the two is exactly how a live pin gets reaped while
    its fleet is still using it (the 2026-07-20 cefi-migration outage). Read-only
    consumers that genuinely want the lenient default keep using
    :func:`list_running_vm_names`.
    """
    client = get_compute_engine_client(provider="gcp", project_id=project_id)
    running: set[str] = set()
    for inst in client.aggregated_list_instances(project_id, filter_str=""):
        status = str(inst.get("status", ""))
        name = str(inst.get("name", ""))
        if status == "RUNNING" and name:
            running.add(name)
    logger.info("list_running_vm_names(%s): %d RUNNING VMs", project_id, len(running))
    return running


def list_running_vm_names(project_id: str) -> set[str]:
    """Return the set of VM names currently in ``RUNNING`` state in ``project_id``.

    Uses the UTL Compute Engine client's ``aggregated_list_instances`` (one API
    call covers every zone). On failure returns an empty set + logs a warning —
    the reaper falls back to heartbeat-age-only classification in that case,
    which is strictly safer (may under-reap, never over-reaps).

    NOTE that "safer on failure" holds for the reaper, NOT for a deleter — see
    :func:`list_running_vm_names_strict`.
    """
    try:
        return list_running_vm_names_strict(project_id)
    except Exception as exc:
        logger.warning("list_running_vm_names(%s) failed: %s", project_id, exc)
        return set()


__all__ = ["list_running_instances_strict", "list_running_vm_names", "list_running_vm_names_strict"]
