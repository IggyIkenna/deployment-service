"""Guard test — every SPOT launcher must be able to emit the ``PREEMPTED`` signal.

The whole SPOT preemption auto-recovery chain is triggered by ONE artifact: the
durable ``vm-logs/{vm}/PREEMPTED`` blob. Without it a reclaimed VM is classified
``EXIT_NONZERO``/``GONE_NO_CAPTURE`` and PAGES a human instead of auto-relaunching:

    PREEMPTED blob
      -> _gcs.is_vm_preempted
      -> exit_code_fleet_monitor.classify_terminated_vm(preempted=True)
      -> DP_VM_PREEMPTED (AUTO_RECOVER)
      -> escalation._recover_preempted_vm
      -> RelaunchPreemptedVm (replays LAUNCH_PARAMS.json, resuming START_DATE from
         the monotonic PROGRESS.json checkpoint)

SSOT: ``codex/05-infrastructure/spot-vms-for-backfill.md``.

WHY THIS TEST EXISTS (measured 2026-07-20): that codex section claimed the signal was
"wired fleet-wide via ``scripts/vm/lib/launcher_common.sh``", but
``lc_write_preemption_signal_file`` was only ever CALLED by
``launch-cefi-sharded-backfill.sh`` (+8 launchers carrying an inline copy). 56
launchers passed ``--provisioning-model=SPOT`` and 47 of them wrote no PREEMPTED blob
at all — i.e. the documented fleet-wide guarantee did not exist. The fix installs the
signal in the SHARED VM-side seam (``setup-data-pipeline-vm.sh``, the
``startup-script-url`` every one of those 47 launchers uses), mirroring how the
PROGRESS.json writer was made fleet-wide via ``vm-exec-with-gcs-tee.sh``. This test
pins that invariant so a new SPOT launcher cannot silently reopen the gap.
"""

from __future__ import annotations

from pathlib import Path

from deployment_service.data_pipeline_monitors import _gcs
from deployment_service.data_pipeline_monitors.launcher_registry import (
    resolve_launcher_for_vm,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS_VM = _REPO_ROOT / "scripts" / "vm"
_SETUP_SCRIPT = _SCRIPTS_VM / "setup-data-pipeline-vm.sh"

# The metadata gate + blob write that make a shutdown a *preemption* signal rather
# than a generic shutdown hook. Any implementation must carry both.
_PREEMPT_METADATA_GATE = "instance/preempted"
_PREEMPT_BLOB_SEGMENT = "/PREEMPTED"

# The shared startup-script seam. A launcher that points its VMs at this script
# inherits the signal without needing its own shutdown-script.
_SHARED_SETUP_SEAM = "setup-data-pipeline-vm.sh"

_SPOT_FLAG = "--provisioning-model=SPOT"


def _launcher_scripts() -> list[Path]:
    return sorted(_SCRIPTS_VM.glob("launch-*.sh"))


def _emits_preemption_signal(text: str) -> bool:
    """True when the launcher attaches its OWN preemption shutdown-script."""
    return _PREEMPT_METADATA_GATE in text or "lc_write_preemption_signal_file" in text


def test_shared_setup_script_installs_the_preemption_signal() -> None:
    """``setup-data-pipeline-vm.sh`` is the fleet-wide seam — it MUST install the signal.

    This is the single edit that covers the 47 SPOT launchers which have no
    shutdown-script of their own. If it regresses, every one of them silently loses
    auto-recovery, so it is asserted directly rather than inferred.
    """
    text = _SETUP_SCRIPT.read_text(encoding="utf-8")
    assert _PREEMPT_METADATA_GATE in text, (
        f"{_SETUP_SCRIPT.name} must gate on the GCE metadata preempted flag — without it "
        "the shutdown hook would mark EVERY shutdown (incl. a clean self-delete) as a "
        "preemption and relaunch completed work"
    )
    assert _PREEMPT_BLOB_SEGMENT in text, (
        f"{_SETUP_SCRIPT.name} must write the PREEMPTED blob — it is the ONLY trigger for RelaunchPreemptedVm"
    )
    assert "uts-preemption-signal.service" in text, (
        f"{_SETUP_SCRIPT.name} must install the preemption-signal systemd unit"
    )


def test_signal_blob_path_matches_the_reader_contract() -> None:
    """The writer's path must be the exact path ``_gcs.is_vm_preempted`` reads.

    A drift here fails OPEN and SILENT: the blob is written, nobody reads it, and the
    preemption looks like an unexplained crash.
    """
    expected = _gcs.PREEMPTED_BLOB.format(vm="VM")
    assert expected == "vm-logs/VM/PREEMPTED"
    text = _SETUP_SCRIPT.read_text(encoding="utf-8")
    assert "vm-logs/${VM_NAME}/PREEMPTED" in text


def test_every_spot_launcher_can_emit_the_preemption_signal() -> None:
    """No SPOT launcher may be preemption-blind.

    A launcher satisfies the invariant either by attaching its own preemption
    shutdown-script, or by booting its VMs from the shared ``setup-data-pipeline-vm.sh``
    seam (which installs the signal for it).
    """
    blind: list[str] = []
    spot_launchers: list[str] = []
    for path in _launcher_scripts():
        text = path.read_text(encoding="utf-8")
        if _SPOT_FLAG not in text:
            continue
        spot_launchers.append(path.name)
        if _emits_preemption_signal(text) or _SHARED_SETUP_SEAM in text:
            continue
        blind.append(path.name)

    assert spot_launchers, "no SPOT launchers found — the scan is broken, not the fleet"
    assert not blind, (
        "SPOT launcher(s) with NO preemption signal — a reclaimed VM will PAGE instead of "
        "auto-relaunching. Either boot the VM from setup-data-pipeline-vm.sh (preferred — "
        "the signal is installed there fleet-wide) or call lc_write_preemption_signal_file "
        f"and pass --metadata-from-file=shutdown-script=. Offenders: {sorted(blind)}"
    )


def test_mdps_sharded_sports_shard_has_a_relaunch_binding() -> None:
    """``launch-mdps-sharded-backfill.sh`` emits ``mdps-sports-{year}-{ts}``.

    Its default asset-group set includes sports, but the prefix was registered in
    NEITHER registry until 2026-07-20 — so a preempted sports MDPS shard had no
    relaunch binding and fell through to file_issue. The launcher->emitted-name
    direction is what the prefix-parity test cannot see (both registries agreed with
    each other while both omitted the name the launcher actually produces).
    """
    launcher = resolve_launcher_for_vm("mdps-sports-2024-20260720-120000")
    assert launcher == "launch-mdps-sharded-backfill.sh"


def test_mdps_sports_bucket_prefix_still_wins_longest_prefix_match() -> None:
    """Adding ``mdps-sports-`` must not steal ``mdps-sports-bucket-``'s VMs.

    They are different launchers; longest-prefix match keeps them apart.
    """
    assert resolve_launcher_for_vm("mdps-sports-bucket-20260720-120000") == "launch-mdps-sports-bucket-vm.sh"
