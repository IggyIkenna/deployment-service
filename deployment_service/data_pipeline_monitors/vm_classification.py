"""VM-name -> asset_group / is-a-data-VM classifiers for the fleet monitors.

Split out of ``cli.py`` (2026-08-04, codex-compliance file-size ceiling — same
reason ``meta_targets.py`` was split out 2026-07-13) so ``cli.py`` stays under the
930-line ceiling. Pure, credential-free, no cloud SDK — ``cli.py`` imports the
public names below aliased to their original underscore-prefixed names so existing
call sites (incl. tests) keep working.
"""

from __future__ import annotations

from deployment_service.data_pipeline_monitors.meta_targets import ASSET_GROUPS

# VM-name prefixes that ARE data-pipeline backfill/live-capture VMs (emit PIPELINE_HEARTBEAT + a
# per-VM manifest shard) — sweeps SKIP infra VMs (zombie-watchdog, …) or false EVENT_LOOP_STARVED
# fires (2026-06-22 BUG2). A missing prefix ALSO drops the VM from exit-code PREEMPTED
# classification (RelaunchPreemptedVm never fires — af_backfill_preemption_auto_recovery_not_firing_2026_08_04.md).
# 29 more prefixes had the exact same gap (real relaunch launcher, no asset_group
# substring in the name) — full audit + why-prefix-not-substring rationale in that
# doc + test_data_vm_prefixes_cover_every_relaunchable_launcher (test_data_pipeline_
# monitors_cli.py), which guards every current/future LAUNCHER_FOR_VM_PREFIX entry.
DATA_VM_PREFIXES = (
    "mtds-",
    "tm-backfill",
    "tm-forward-poll-",
    "fs-backfill",
    "fts-backfill",
    "instruments-",
    "tradfi-bf",
    "tradfi-fwd",
    "cefi-",
    "defi-",
    "sports-",
    "prediction-",
    "weather-backfill",
    "solana-",
    "af-backfill-",
    "af-audit-",
    "af-recover-",
    "aster-fwd-",
    "blank-reason-recon-",
    "deribit-opts-fwd-",
    "dvol-deribit-",
    "expected-universe-v2-",
    "feat-orph-",
    "features-",
    "fill-missing-player-stats-",
    "footystats-fwd-",
    "fss-backfill-vm-",
    "governance-backfill-",
    "instr-backfill-pred",
    "jito-solana-backfill-",
    "marinade-backfill-",
    "ml-orph-",
    "opt-cboe-",
    "opt-cme-",
    "opt-deribit-",
    "opt-okx-",
    "pyth-lst-backfill-",
    "replay-",
    "scenario-matrix-",
    "sfi-backfill-",
    "sfi-fwd-",
    "strat-orph-",
    "us-backfill-",
    "us-forward-poll-",
)


def asset_group_for_vm(vm_name: str) -> str:
    """Best-effort asset_group from the VM-name segment (cefi/defi/tradfi/...)."""
    lowered = vm_name.lower()
    for ag in ASSET_GROUPS:
        if ag in lowered:
            return ag
    return "unknown"


def is_data_vm(vm_name: str) -> bool:
    """True when ``vm_name`` is a data-pipeline VM (heartbeats + per-VM shard).

    Filters the RUNNING census down to the data VMs the heartbeat/exit-code
    sweeps apply to. An AG segment in the name (cefi/defi/tradfi/sports/
    prediction) OR a known data-VM prefix qualifies; everything else (infra /
    orchestrator / watchdog VMs) is skipped so they never false-alert.
    """
    lowered = vm_name.lower()
    if asset_group_for_vm(vm_name) != "unknown":
        return True
    return any(lowered.startswith(p) for p in DATA_VM_PREFIXES)
