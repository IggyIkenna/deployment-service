"""Compute Operations API fallback for the exit-code fleet monitor's preemption check.

Separated from ``cli.py`` (the credential-bound wiring entry point) purely to keep
that file's line budget clear — this module owns the ONE call site that resolves
a live ``ComputeEngineClient`` for the preemption-op-checker.
"""

from __future__ import annotations

from collections.abc import Callable

from unified_trading_library.cloud_interface import get_compute_engine_client  # noqa: qg-deep-import


def make_preemption_op_checker(project_id: str) -> Callable[[str], bool]:
    """``vm_name -> was-actually-preempted`` via the Compute Operations API.

    Fallback for ``exit_code_fleet_monitor.sweep``'s ``GONE_NO_CAPTURE`` candidate
    path — see that function's ``preemption_op_checker`` docstring for the race
    this closes (a VM reclaimed within seconds of insert can be preempted before
    its in-guest shutdown-script — the primary GCS ``PREEMPTED`` marker — ever
    runs). Never raises — a query failure returns ``False``.
    """

    def _check(vm_name: str) -> bool:
        try:
            client = get_compute_engine_client(provider="gcp", project_id=project_id)
            return bool(client.was_instance_preempted(project_id, vm_name))
        except Exception:
            return False

    return _check
