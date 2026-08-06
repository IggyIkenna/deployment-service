"""Compute Engine API checkers for the exit-code fleet monitor's preemption verdict.

Separated from ``cli.py`` (the credential-bound wiring entry point) purely to keep
that file's line budget clear — this module owns the call sites that resolve a
live ``ComputeEngineClient`` for (1) the Operations-API preemption fallback and
(2) the scheduling-provisioning-model veto cross-check.
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


def make_scheduling_model_checker(project_id: str) -> Callable[[str], str | None]:
    """``vm_name -> scheduling.provisioning_model`` (``"STANDARD"``/``"SPOT"``/...), or
    ``None`` when unresolvable (already deleted / API error / field unreadable).

    Cross-check for the preemption-verdict veto in ``exit_code_fleet_monitor.
    classify_terminated_vm``'s ``is_spot`` parameter — closes
    cefi_fwd_vm_preempted_false_positive_standard_provisioning_2026_08_06.md, where
    a GCE STANDARD (on-demand, structurally un-preemptible) ``cefi-fwd-*`` VM was
    deliberately ``instances.stop``'d by our own system yet still classified
    ``PREEMPTED`` — neither existing preemption signal (the in-guest GCS marker
    ``_gcs.is_vm_preempted``, nor the ``was_instance_preempted`` Operations-API
    fallback above) is cross-checked against the instance's OWN scheduling
    config, so a wrong signal from either source was trusted blindly. This
    reads the instance's ACTUAL ``scheduling.provisioning_model`` so a
    ``preempted=True`` verdict can be vetoed once it is confirmed non-SPOT.
    Never raises — a query failure (or an already-deleted instance) returns
    ``None`` (unknown), which the caller treats as "trust the preempted signal
    as-is", never as a suppression signal on its own.
    """

    def _check(vm_name: str) -> str | None:
        try:
            client = get_compute_engine_client(provider="gcp", project_id=project_id)
            rows = client.aggregated_list_instances(project_id, f'name="{vm_name}"')
            for row in rows:
                if row.get("name") == vm_name:
                    model = row.get("scheduling_provisioning_model")
                    return str(model) if model else None
        except Exception:
            return None
        return None

    return _check
