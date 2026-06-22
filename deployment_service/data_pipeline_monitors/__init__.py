"""Data-pipeline fleet monitors (Wave 4a of data_pipeline_hardening_self_monitoring_2026_06_22).

Cross-cutting watchers that make a running batch/live data-pipeline VM, a watcher,
or a daily audit *incapable of failing silently*. Each module emits a typed
``DP_*`` event (UTL ``unified_trading_library.events``) which the alerting-service
router (``rules/data_pipeline_rules.py``) mirrors to ``#data-pipeline-alerts``.

Modules
-------
``exit_code_fleet_monitor``
    Closes DP-VM-001/002 — the self-delete-masks-OOM blind spot (CLAUDE.md
    2026-06-22). Per recently-terminated VM, reads the persisted GCS ``run.log``
    terminal ``exit_code`` (survives ``VM_SHUTDOWN_ON_COMPLETION`` self-delete)
    AND cross-checks whether the manifest ``captured`` count climbed. Emits
    ``DP_VM_EXIT_NONZERO`` on ``exit_code != 0`` (incl. 137 OOM) and
    ``DP_VM_GONE_NO_CAPTURE`` when a VM drained but captured stayed flat. The
    wake condition is ``exit_code != 0 OR captured did not climb`` — NEVER
    "VM gone ⇒ success".

``heartbeat_stall_watcher``
    Closes DP-VM-003/004 — consumes ``PIPELINE_HEARTBEAT`` (emitted by running
    VMs via UTL ``emit_pipeline_heartbeat``); if a registered RUNNING VM's last
    heartbeat / progress is older than the stall threshold, emits ``DP_VM_STALL``
    (and ``DP_EVENT_LOOP_STARVED`` when the silence is total).

``meta_watchers``
    Closes DP-CATALOG-001 / DP-WATCHER-001/002 — freshness probes for the
    instrument-catalogue refresh, the zombie-VM watchdog itself, and scheduled
    audit/consolidator/digest crons (the "is the checker itself alive" gap).

``escalation``
    The escalation hop mirroring ``ci_failure_watcher.py``'s
    auto-recover-vs-escalate: routes a finding through ``auto_recover`` /
    ``file_issue`` (writes ``plans/active/issues/<slug>_<date>.md`` in the PM
    clone + pings the orchestrator inbox) / ``page_operator`` per its registry
    ``escalation`` tier.

Shared GCS read helpers live in ``_gcs`` (cloud-agnostic — ``get_storage_client``,
never ``google.cloud`` directly).
"""

from __future__ import annotations

from deployment_service.data_pipeline_monitors.escalation import (
    EscalationTier,
    PipelineFinding,
    route_finding,
)

__all__ = [
    "EscalationTier",
    "PipelineFinding",
    "route_finding",
]
