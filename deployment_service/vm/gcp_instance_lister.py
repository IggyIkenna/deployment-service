"""GCE instance lister — SSOT for "what VMs are currently RUNNING in the project".

Thin wrapper around ``google.cloud.compute_v1.InstancesClient.aggregated_list``
used exclusively by ``DeploymentsRegistry.reap_stale()`` to cross-reference
GCS-backed ``deployments/active/`` entries against live GCE VM state.

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

from google.cloud import compute_v1

logger = logging.getLogger(__name__)


def list_running_vm_names(project_id: str) -> set[str]:
    """Return the set of VM names currently in ``RUNNING`` state in ``project_id``.

    Uses ``aggregated_list`` so one API call covers every zone. On failure
    returns an empty set + logs a warning — the reaper falls back to
    heartbeat-age-only classification in that case, which is strictly
    safer (may under-reap, never over-reaps).
    """
    try:
        client = compute_v1.InstancesClient()
        request = compute_v1.AggregatedListInstancesRequest(project=project_id)
        running: set[str] = set()
        for _zone, scoped_list in client.aggregated_list(request=request):
            instances = getattr(scoped_list, "instances", None)
            if not instances:
                continue
            for inst in instances:
                status = str(getattr(inst, "status", ""))
                name = str(getattr(inst, "name", ""))
                if status == "RUNNING" and name:
                    running.add(name)
        logger.info("list_running_vm_names(%s): %d RUNNING VMs", project_id, len(running))
        return running
    except Exception as exc:  # noqa: BLE001 — shard-level isolation; reaper must never raise
        logger.warning("list_running_vm_names(%s) failed: %s", project_id, exc)
        return set()


__all__ = ["list_running_vm_names"]
