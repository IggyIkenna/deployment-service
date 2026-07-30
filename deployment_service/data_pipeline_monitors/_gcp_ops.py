"""GCE Compute Operations API — preemption fallback for the exit-code fleet monitor.

``_gcs.is_vm_preempted`` depends on the VM's OWN shutdown-script successfully
writing a durable GCS blob within GCE's SPOT-preemption shutdown grace window.
That write can race a VM preempted seconds after boot — confirmed 2026-07-30 on
``tradfi-bf-nasdaq-ohlcv-1m-2024-d02-20260730-210525``: its
``uts-preemption-signal.service`` was installed + active by boot+~16s (long
before the preemption at boot+~180s), yet no ``PREEMPTED`` blob was ever
written, and the exit-code fleet monitor fired a CRITICAL
``DP_VM_GONE_NO_CAPTURE`` page for what
``gcloud compute operations list --filter="targetLink~<vm>"`` proved was a
routine SPOT reclaim (``operationType=compute.instances.preempted``, ``DONE``)
— the SAME manual check CLAUDE.md already tells operators to run by hand when
triaging a stalled SPOT VM ("Checking a stalled SPOT VM: verify
compute.instances.preempted via gcloud compute operations list FIRST — one-off
VMs aren't wired into the fleet monitor, check it yourself"). This module wires
the equivalent read into the monitor itself as a FALLBACK (the cheap
guest-written blob stays the primary, first-checked signal) so the next
occurrence of the same early-boot race auto-recovers instead of paging.

Host-side and authoritative: the Compute Engine control plane records this
systemEvent regardless of what the guest OS managed to do in its shutdown
grace window, so it is strictly more reliable than the guest-written blob for
an early-boot preemption. The GCP SDK stays confined to
``deployment_service.backends._gcp_sdk`` (the approved boundary — see
``deadman_poster.py``'s ``_default_monitoring_reader`` for the same
lazy-import-inside-function pattern, required because the GCP SDK must stay
OFF this package's module-load path; see the 2026-06-23 packaging incident in
``data-pipeline-alerts.md``). This module never imports ``google.cloud``
directly, and never at module level.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

logger = logging.getLogger(__name__)

DEFAULT_OPERATIONS_API_TIMEOUT_SECONDS = 30.0

_operations_client: object | None = None


def _bounded_call[T](fn: Callable[[], T], *, timeout_seconds: float, op_label: str) -> T:
    """Run ``fn`` on a daemon thread, bounded to ``timeout_seconds``.

    Same bounded-wait / abandon-and-log mechanism as ``_gcs._call_with_timeout``
    (mirrored rather than imported, to keep this module's only coupling to
    ``_gcs`` at zero — the two modules classify DIFFERENT failure signals and
    stay independently readable). Raises ``TimeoutError`` on expiry (logged as
    a distinct WARNING first); re-raises whatever ``fn`` itself raised on a
    normal failure. The caller wraps this in a broad ``except Exception`` so
    both paths conservatively resolve to "not confirmed preempted".
    """
    result: list[T] = []
    error: list[Exception] = []

    def _runner() -> None:
        try:
            result.append(fn())
        except Exception as exc:
            error.append(exc)

    thread = threading.Thread(target=_runner, name=f"ops-timeout:{op_label}", daemon=True)
    thread.start()
    thread.join(timeout_seconds)
    if thread.is_alive():
        logger.warning(
            "_gcp_ops: %s exceeded the %.0fs bounded-call timeout — abandoning the stalled "
            "Compute Operations API call and treating this check as failed",
            op_label,
            timeout_seconds,
        )
        raise TimeoutError(f"{op_label} exceeded {timeout_seconds:.0f}s")
    if error:
        raise error[0]
    return result[0]


def _get_operations_client() -> object:
    """Lazily construct a singleton ``compute_v1.GlobalOperationsClient``.

    Function-local import (mirrors ``deadman_poster._default_monitoring_reader``)
    — the GCP SDK must stay off this package's module-load path.
    """
    global _operations_client
    if _operations_client is None:
        from deployment_service.backends import _gcp_sdk

        credentials, _project = _gcp_sdk.google_auth_default(  # pyright: ignore[reportUnknownMemberType]
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        session = _gcp_sdk.google_auth_requests.AuthorizedSession(credentials)
        _operations_client = _gcp_sdk.compute_v1.GlobalOperationsClient(  # pyright: ignore[reportUnknownMemberType]
            credentials=session.credentials
        )
    return _operations_client


def was_vm_preempted_via_operations_api(
    project_id: str,
    vm_name: str,
    *,
    timeout_seconds: float = DEFAULT_OPERATIONS_API_TIMEOUT_SECONDS,
    client: object | None = None,
) -> bool:
    """True when a ``compute.instances.preempted`` systemEvent targets ``vm_name``.

    Aggregated across every zone in ``project_id`` in one call — no zone
    tracking needed (the exit-code fleet monitor's census does not retain a
    terminated VM's zone, and this check only runs for a VM already gone from
    the running set). Never raises: any API error or timeout conservatively
    returns ``False``, so a caller falls through to its existing (blob-based)
    classification, unchanged from today's behavior on failure — this is a
    fallback signal, not a replacement for the blob check.

    ``client`` is an injection point for tests (a duck-typed stand-in exposing
    ``aggregated_list(request=...) -> Iterable[tuple[str, object]]`` where each
    scoped list carries an ``operations`` attribute) — production callers never
    pass it, and it is only ever consulted, never constructed, when absent.
    """
    try:
        from deployment_service.backends import _gcp_sdk

        resolved_client = client if client is not None else _get_operations_client()

        def _fetch() -> bool:
            request = _gcp_sdk.compute_v1.AggregatedListGlobalOperationsRequest(  # pyright: ignore[reportUnknownMemberType]
                project=project_id,
                filter=(f'(targetLink~"instances/{vm_name}$") AND (operationType="compute.instances.preempted")'),
            )
            for _scope, ops_list in resolved_client.aggregated_list(request=request):  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
                if ops_list.operations:
                    return True
            return False

        return _bounded_call(
            _fetch,
            timeout_seconds=timeout_seconds,
            op_label=f"compute.operations.aggregatedList(preempted:{vm_name})",
        )
    except Exception as exc:
        logger.warning("was_vm_preempted_via_operations_api(%s) failed: %s", vm_name, exc)
        return False


__all__ = ["DEFAULT_OPERATIONS_API_TIMEOUT_SECONDS", "was_vm_preempted_via_operations_api"]
