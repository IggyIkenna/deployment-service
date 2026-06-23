"""Exit_code-aware fleet monitor (DP-VM-001 / DP-VM-002).

Closes the self-delete-masks-OOM blind spot (CLAUDE.md 2026-06-22): a batch/live
VM launched with ``VM_SHUTDOWN_ON_COMPLETION=true`` self-deletes on exit whether
it SUCCEEDED (exit 0) or CRASHED (exit 137=OOM / non-zero=error). A monitor that
only watches the RUNNING set + treats "VM gone ⇒ drained" is BLIND to mass
failures (incident 2026-06-22: 3 sports backfills OOM-died exit 137, self-deleted,
the drain-only monitor read 14→1 as healthy completion → no wake; coverage was 0%
with 75k+ attempted_failed rows).

This monitor is census-diffing + durable-signal-reading:

  1. Snapshot the RUNNING VM census each tick to a GCS census blob, recording per
     VM its observed cumulative captured count.
  2. A VM present in the PRIOR census but GONE this tick has *terminated*. For
     each such VM, read the persisted GCS ``run.log`` terminal ``exit_code``
     (survives self-delete via ``read_terminal_exit_code``) AND cross-check
     whether its manifest ``captured`` count climbed during the run.
  3. Emit, per terminated VM:
       - ``DP_VM_EXIT_NONZERO`` (CRITICAL) when ``exit_code != 0`` (incl. 137 OOM).
       - ``DP_VM_GONE_NO_CAPTURE`` (CRITICAL) when the VM drained but captured did
         NOT climb (``exit_code == 0`` but flat captured — the silent-zero class).

The wake/alert condition is ``exit_code != 0 OR captured did not climb`` — NEVER
"VM gone ⇒ success".

Pure-function core (``classify_terminated_vm``) is unit-tested with fixtures; the
GCS/compute I/O is injected so the sweep is credential-free + block-network safe.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import cast

from unified_trading_library import StorageClient
from unified_trading_library.events import DP_VM_EXIT_NONZERO, DP_VM_GONE_NO_CAPTURE  # noqa: qg-deep-import

from deployment_service.data_pipeline_monitors import _gcs
from deployment_service.data_pipeline_monitors.escalation import (
    EscalationTier,
    PipelineFinding,
    route_finding,
)

logger = logging.getLogger(__name__)

# The census blob lives in the heartbeat/log bucket so it is durable + cheap.
CENSUS_BLOB = "vm-census/exit-code-fleet-census.json"


class TerminationVerdict(StrEnum):
    """Per terminated-VM classification outcome."""

    EXIT_NONZERO = "exit_nonzero"  # DP-VM-001
    GONE_NO_CAPTURE = "gone_no_capture"  # DP-VM-002
    CLEAN = "clean"  # exit 0 + captured climbed → healthy completion
    UNKNOWN = "unknown"  # no durable exit code AND no captured signal


@dataclass(frozen=True)
class TerminationResult:
    """The result of classifying one terminated VM."""

    vm_name: str
    verdict: TerminationVerdict
    exit_code: int | None
    captured_before: int
    captured_after: int

    @property
    def captured_climbed(self) -> bool:
        return self.captured_after > self.captured_before


def classify_terminated_vm(
    vm_name: str,
    *,
    exit_code: int | None,
    captured_before: int,
    captured_after: int,
) -> TerminationResult:
    """Pure classification of a terminated VM. No I/O.

    Verdict precedence (most severe first):
      - ``exit_code != 0`` (incl. 137 OOM)         → EXIT_NONZERO  (DP-VM-001)
      - ``exit_code == 0`` but captured did NOT climb → GONE_NO_CAPTURE (DP-VM-002)
      - ``exit_code == 0`` AND captured climbed     → CLEAN
      - ``exit_code is None`` AND captured did NOT climb → GONE_NO_CAPTURE
        (no durable success proof + no work done = treat as silent failure, the
        fail-safe direction — never infer success from "the VM is gone")
      - ``exit_code is None`` but captured climbed  → CLEAN (work landed; the
        durable code blob was just never written)
    """
    climbed = captured_after > captured_before
    if exit_code is not None and exit_code != 0:
        verdict = TerminationVerdict.EXIT_NONZERO
    elif climbed:
        verdict = TerminationVerdict.CLEAN
    elif exit_code == 0:
        verdict = TerminationVerdict.GONE_NO_CAPTURE
    elif exit_code is None:
        # No durable exit code AND no captured climb → fail-safe to GONE_NO_CAPTURE.
        verdict = TerminationVerdict.GONE_NO_CAPTURE
    else:  # pragma: no cover - exhaustive guard
        verdict = TerminationVerdict.UNKNOWN
    return TerminationResult(
        vm_name=vm_name,
        verdict=verdict,
        exit_code=exit_code,
        captured_before=captured_before,
        captured_after=captured_after,
    )


def _finding_for(result: TerminationResult, *, asset_group: str, relaunch_launcher: str = "") -> PipelineFinding | None:
    """Build the escalation finding for a non-clean termination (None when clean).

    An **OOM** exit (exit_code 137) routes to the ``auto_recover`` tier — the
    ``relaunch_backfill_vm`` actuator re-launches it (≤2/day per vm-prefix then
    page). The binding is the ``relaunch_launcher`` script name (resolved by the
    sweep's ``launcher_for_vm``); when no launcher binding is available the
    actuator skips and the finding falls through to ``file_issue`` (the
    escalation hop guarantees no auto_recover finding is lost). A non-OOM
    non-zero exit stays ``page_operator``.
    """
    base_details: dict[str, object] = {
        "vm_name": result.vm_name,
        "asset_group": asset_group,
        "exit_code": result.exit_code,
        "captured_before": result.captured_before,
        "captured_after": result.captured_after,
    }
    if result.verdict is TerminationVerdict.EXIT_NONZERO:
        oom = result.exit_code == 137
        summary = (
            f"VM {result.vm_name} terminated with exit_code={result.exit_code}"
            f"{' (OOM)' if oom else ''} — captured did not complete cleanly"
        )
        oom_details: dict[str, object] = {**base_details, "oom": oom}
        if oom and relaunch_launcher:
            oom_details["relaunch_launcher"] = relaunch_launcher
        return PipelineFinding(
            event=DP_VM_EXIT_NONZERO,
            severity="CRITICAL",
            tier=EscalationTier.AUTO_RECOVER if oom else EscalationTier.PAGE_OPERATOR,
            summary=summary,
            details=oom_details,
            registry_id="DP-VM-001",
        )
    if result.verdict is TerminationVerdict.GONE_NO_CAPTURE:
        return PipelineFinding(
            event=DP_VM_GONE_NO_CAPTURE,
            severity="CRITICAL",
            tier=EscalationTier.PAGE_OPERATOR,
            summary=(
                f"VM {result.vm_name} drained but manifest captured did not climb "
                f"({result.captured_before} → {result.captured_after}) — self-delete "
                "masked a zero-row run"
            ),
            details=base_details,
            registry_id="DP-VM-002",
        )
    return None


def load_census(storage_client: StorageClient, log_bucket: str) -> dict[str, int]:
    """Load the prior RUNNING-VM census (vm_name → observed captured_cum)."""
    raw = _gcs.read_text(storage_client, log_bucket, CENSUS_BLOB)
    if not raw:
        return {}
    try:
        loaded = cast("object", json.loads(raw))
    except (json.JSONDecodeError, ValueError):
        return {}
    if not isinstance(loaded, dict):
        return {}
    data = cast("dict[str, object]", loaded)
    vms_obj = data.get("vms", {})
    if not isinstance(vms_obj, dict):
        return {}
    vms = cast("dict[str, object]", vms_obj)
    out: dict[str, int] = {}
    for key, value in vms.items():
        if isinstance(value, (int, float, str)):
            try:
                out[str(key)] = int(value)
            except (TypeError, ValueError):
                continue
    return out


def write_census(storage_client: StorageClient, log_bucket: str, census: dict[str, int]) -> None:
    """Persist the current RUNNING-VM census. Best-effort (never raises the sweep)."""
    payload = json.dumps(
        {"updated_at": datetime.now(UTC).isoformat(), "vms": census},
        sort_keys=True,
    ).encode("utf-8")
    try:
        storage_client.upload_bytes(log_bucket, CENSUS_BLOB, payload, content_type="application/json")
    except Exception as exc:
        logger.warning("exit_code_fleet_monitor: failed to persist census: %s", exc)


def sweep(
    *,
    storage_client: StorageClient,
    log_bucket: str,
    running_vms: Iterable[tuple[str, str]],
    captured_reader: Callable[[str], int],
    asset_group_for_vm: Callable[[str], str],
    launcher_for_vm: Callable[[str], str] | None = None,
    pm_repo_path: str | None = None,
    dry_run: bool = False,
) -> list[TerminationResult]:
    """Run one exit-code-aware fleet sweep.

    Args:
        storage_client: cloud-agnostic storage client.
        log_bucket: the durable log/heartbeat bucket (``deployment-scripts-{pid}``).
        running_vms: iterable of ``(vm_name, zone)`` for currently-RUNNING VMs.
        captured_reader: ``vm_name -> captured_cum`` (per-VM manifest shard count).
        asset_group_for_vm: ``vm_name -> asset_group`` for the alert detail.
        launcher_for_vm: optional ``vm_name -> launcher-script-name`` resolver. When
            supplied, an OOM (exit-137) finding carries a ``relaunch_launcher``
            binding so the ``relaunch_backfill_vm`` auto_recover actuator can
            re-launch it; absent it, the OOM finding falls through to file_issue.
        pm_repo_path: optional PM clone path for the file_issue tier.
        dry_run: when True, classify + return but emit nothing / persist nothing.

    Returns the list of TerminationResult for VMs that terminated since last tick.
    """
    prior = load_census(storage_client, log_bucket)
    running = dict(running_vms)

    # Update census for currently-running VMs (record their latest captured_cum).
    current_census: dict[str, int] = {}
    for name in running:
        try:
            current_census[name] = max(captured_reader(name), prior.get(name, 0))
        except Exception:
            current_census[name] = prior.get(name, 0)

    # Terminated = in prior census, gone now.
    terminated = [name for name in prior if name not in running]

    results: list[TerminationResult] = []
    for name in terminated:
        exit_code = _gcs.read_terminal_exit_code(storage_client, log_bucket, name)
        captured_before = prior.get(name, 0)
        try:
            captured_after = max(captured_reader(name), captured_before)
        except Exception:
            captured_after = captured_before
        result = classify_terminated_vm(
            name,
            exit_code=exit_code,
            captured_before=captured_before,
            captured_after=captured_after,
        )
        results.append(result)

        launcher = launcher_for_vm(name) if launcher_for_vm is not None else ""
        finding = _finding_for(result, asset_group=asset_group_for_vm(name), relaunch_launcher=launcher)
        if finding is not None and not dry_run:
            route_finding(finding, pm_repo_path=pm_repo_path)
        if finding is not None:
            logger.warning(
                "exit_code_fleet_monitor: %s verdict=%s exit_code=%s captured=%d->%d",
                name,
                result.verdict,
                exit_code,
                captured_before,
                result.captured_after,
            )

    if not dry_run:
        write_census(storage_client, log_bucket, current_census)

    return results
