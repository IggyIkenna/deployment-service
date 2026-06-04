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


def list_running_vm_names(project_id: str) -> set[str]:
    """Return the set of VM names currently in ``RUNNING`` state in ``project_id``.

    Uses the UTL Compute Engine client's ``aggregated_list_instances`` (one API
    call covers every zone). On failure returns an empty set + logs a warning —
    the reaper falls back to heartbeat-age-only classification in that case,
    which is strictly safer (may under-reap, never over-reaps).
    """
    try:
        client = get_compute_engine_client(provider="gcp", project_id=project_id)
        running: set[str] = set()
        for inst in client.aggregated_list_instances(project_id, filter_str=""):
            status = str(inst.get("status", ""))
            name = str(inst.get("name", ""))
            if status == "RUNNING" and name:
                running.add(name)
        logger.info("list_running_vm_names(%s): %d RUNNING VMs", project_id, len(running))
        return running
    except Exception as exc:
        logger.warning("list_running_vm_names(%s) failed: %s", project_id, exc)
        return set()


__all__ = ["list_running_vm_names"]
