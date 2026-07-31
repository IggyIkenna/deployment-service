"""Guard test for the ``vm_name -> launcher`` relaunch registry.

Asserts (mirrors ``test_cloud_run_job_registry_guard`` / ``test_validate_vm_prefix_mapping``):

(a) every ``vm_zombie_watchdog.VM_PREFIX_TO_BUCKET`` prefix is present in
    ``LAUNCHER_FOR_VM_PREFIX`` — so "added a watchdog prefix, forgot the relaunch
    registry" fails CI (the self-heal actuator would otherwise silently fall through
    to file_issue with no relaunch);
(b) every NON-None launcher value resolves to an existing ``scripts/vm/launch-*.sh``
    file — a typo / renamed-launcher fails CI;
(c) the registry adds NO extra prefixes the watchdog doesn't know about (bidirectional
    parity — a registry-only prefix is a stale entry);
(d) ``resolve_launcher_for_vm`` does longest-prefix match + the fail-safe direction
    (unknown vm -> None -> file_issue), and a tradfi-ohlcv stall VM resolves to a
    NON-None launcher so its finding carries a ``relaunch_launcher`` binding.

The watchdog module calls ``resolve_bucket_name`` at import (reads the UAC-packaged
cloud-providers.yaml — no network), so the env tier + project are set BEFORE import,
exactly as the cloud-run-job guard does.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Env tier + project for resolve_bucket_name (called at vm_zombie_watchdog import).
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS_VM = _REPO_ROOT / "scripts" / "vm"

os.environ.setdefault("DEPLOYMENT_ENV", "prod")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("PROJECT_ID", "test-project")
if str(_SCRIPTS_VM) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_VM))

import vm_zombie_watchdog  # noqa: E402 — load after env/path setup above.

from deployment_service.data_pipeline_monitors.launcher_registry import (  # noqa: E402
    LAUNCHER_FOR_VM_PREFIX,
    launcher_path,
    resolve_launcher_for_vm,
)


# ---------------------------------------------------------------------------
# (a) every watchdog prefix is in the relaunch registry.
# ---------------------------------------------------------------------------
def test_every_watchdog_prefix_has_a_registry_entry() -> None:
    """Every VM_PREFIX_TO_BUCKET prefix appears in LAUNCHER_FOR_VM_PREFIX.

    A watchdog prefix missing here means a killed/OOM'd VM with that prefix would
    resolve to no launcher binding → the self-heal finding silently falls through to
    file_issue with no relaunch. Adding the prefix to the watchdog WITHOUT a registry
    entry (a launcher filename or an explicit ``None`` + reason) must fail CI.
    """
    watchdog_prefixes = set(vm_zombie_watchdog.VM_PREFIX_TO_BUCKET)
    registry_prefixes = set(LAUNCHER_FOR_VM_PREFIX)
    missing = watchdog_prefixes - registry_prefixes
    assert not missing, (
        "VM_PREFIX_TO_BUCKET prefixes with NO launcher_registry entry — add a "
        "scripts/vm/launch-*.sh filename OR an explicit None + reason:\n" + "\n".join(sorted(missing))
    )


# ---------------------------------------------------------------------------
# (b) every non-None launcher value is a real scripts/vm/launch-*.sh file.
# ---------------------------------------------------------------------------
def test_every_non_none_launcher_file_exists() -> None:
    """Each mapped launcher filename resolves to an existing scripts/vm/ file."""
    broken: list[str] = []
    for prefix, launcher in LAUNCHER_FOR_VM_PREFIX.items():
        if launcher is None:
            continue
        if not launcher.startswith("launch-") or not launcher.endswith(".sh"):
            broken.append(f"{prefix!r} -> {launcher!r} (not a launch-*.sh name)")
            continue
        if not launcher_path(launcher).exists():
            broken.append(f"{prefix!r} -> {launcher!r} (file does not exist)")
    assert not broken, "launcher_registry entries pointing at non-existent launchers:\n" + "\n".join(broken)


# ---------------------------------------------------------------------------
# (c) bidirectional parity — no stale registry-only prefix.
# ---------------------------------------------------------------------------
def test_no_extra_registry_prefixes() -> None:
    """The registry adds no prefixes the watchdog doesn't know about (stale entry)."""
    extra = set(LAUNCHER_FOR_VM_PREFIX) - set(vm_zombie_watchdog.VM_PREFIX_TO_BUCKET)
    assert not extra, (
        "launcher_registry prefixes absent from VM_PREFIX_TO_BUCKET (stale — remove "
        "or add to the watchdog):\n" + "\n".join(sorted(extra))
    )


# ---------------------------------------------------------------------------
# (d) resolution behaviour: longest-prefix, fail-safe None, tradfi non-None.
# ---------------------------------------------------------------------------
def test_unknown_vm_resolves_none() -> None:
    """An unmatched vm_name resolves to None (fail-safe → file_issue, never wrong relaunch)."""
    assert resolve_launcher_for_vm("totally-unknown-vm-name-xyz-123") is None


def test_explicit_none_prefix_resolves_none() -> None:
    """A vm under an explicitly-None prefix resolves to None (not a relaunch target)."""
    # ml- is mapped to None (model artefacts, not a relaunchable backfill).
    assert resolve_launcher_for_vm("ml-train-20260101") is None


def test_tradfi_ohlcv_stall_carries_a_launcher() -> None:
    """A tradfi-bf OHLCV stall VM resolves to its OWN dedicated launcher, not the generic one.

    Regression guard for a live 2026-07-31 DP-VM-002 finding (agt-d6540c,
    ``tradfi_bf_ohlcv_launchers_missing_registry_entry_2026_07_31.md``): before the
    dedicated ``tradfi-bf-cme-ohlcv-1m-`` entry existed, this VM name resolved to the
    generic ``tradfi-bf-`` -> ``launch-tradfi-backfill-vm.sh`` (a DIFFERENT sharding
    scheme — quarterly/monthly ES/BTC/ETH tiers, not root-group year-shard OHLCV) —
    a relaunch would have silently launched the wrong shape. Mirrors the FRED fix below.
    """
    launcher = resolve_launcher_for_vm("tradfi-bf-cme-ohlcv-1m-g01-es-es-2020-20260731-000118")
    assert launcher is not None
    assert launcher == "launch-tradfi-bf-cme-ohlcv-1m.sh"
    assert launcher_path(launcher).exists()


def test_tradfi_ohlcv_venues_resolve_to_their_own_dedicated_launchers() -> None:
    """Every per-venue OHLCV launcher family resolves to ITS launcher, not the generic one.

    Same class of bug as the CME case above and the FRED case below — each of these
    families shares the ``tradfi-bf-`` prefix with the generic CME/BTC/ETH launcher and
    needs its own longest-prefix-winning entry.
    """
    cases = {
        "tradfi-bf-ice-ohlcv-1m-brn-2020-20260731-000000": "launch-tradfi-bf-ice-ohlcv-1m.sh",
        "tradfi-bf-nasdaq-ohlcv-1m-2020-a-20260731-000000": "launch-tradfi-bf-nasdaq-ohlcv-1m.sh",
        "tradfi-bf-nyse-ohlcv-1m-2020-a-20260731-000000": "launch-tradfi-bf-nyse-ohlcv-1m.sh",
        "tradfi-bf-cboe-ohlcv-1m-vx-2020-20260731-000000": "launch-tradfi-bf-cboe-ohlcv-1m.sh",
        "tradfi-bf-cfe-ohlcv-1m-2020-20260731-000000": "launch-tradfi-bf-cfe-ohlcv-1m.sh",
        "tradfi-bf-fx-ohlcv-24h-2020-20260731-000000": "launch-tradfi-bf-fx-ohlcv-24h.sh",
        "tradfi-bf-krx-eq-ohlcv-24h-2020-20260731-000000": "launch-tradfi-bf-krx-equities-ohlcv-24h.sh",
        "tradfi-bf-cboe-idx-ohlcv-24h-2020-20260731-000000": "launch-tradfi-bf-cboe-indices-ohlcv-24h.sh",
    }
    for vm_name, expected_launcher in cases.items():
        launcher = resolve_launcher_for_vm(vm_name)
        assert launcher == expected_launcher, f"{vm_name} -> {launcher}, expected {expected_launcher}"
        assert launcher_path(launcher).exists()


def test_tradfi_bf_fred_resolves_to_its_own_dedicated_launcher() -> None:
    """A FRED macro-backfill VM resolves to launch-tradfi-bf-fred.sh, NOT the generic launcher.

    Regression guard for a live 2026-07-30 DP-VM-001 finding: tradfi-bf-fred- had no registry
    entry, so longest-prefix match fell back to the generic ``tradfi-bf-`` -> CME/BTC/ETH
    launcher (which defaults --root-symbol=ES and has no FRED root at all) instead of FRED's
    own single-VM launcher — a relaunch would have silently backfilled the wrong venue.
    """
    launcher = resolve_launcher_for_vm("tradfi-bf-fred-full-20260730-064542")
    assert launcher == "launch-tradfi-bf-fred.sh"
    assert launcher_path(launcher).exists()

    year_smoke_launcher = resolve_launcher_for_vm("tradfi-bf-fred-2024-20260730-014447")
    assert year_smoke_launcher == "launch-tradfi-bf-fred.sh"


def test_longest_prefix_wins() -> None:
    """A more-specific prefix wins over its parent (longest-prefix match).

    ``cefi-fwd-daily-cron-`` (a cron HOST, None) must win over ``cefi-fwd-``
    (forward-poll worker, a real launcher) — never the other way round.
    """
    assert resolve_launcher_for_vm("cefi-fwd-daily-cron-20260101") is None
    assert resolve_launcher_for_vm("cefi-fwd-binance-20260101") == "launch-cefi-forward-poll.sh"
