# Epic: observability_master
# Lifecycle: permanent
# Delete-when: NA
"""Self-healing actuator — re-launch a watchdog-KILLED stalled backfill VM.

Runbook: RB-INFRA-002 (DP-VM-003 / ``DP_VM_STALL``).

The heartbeat-stall watcher (``data_pipeline_monitors/heartbeat_stall_watcher.py``)
fires ``DP_VM_STALL`` (WARN, auto_recover tier) when a RUNNING data VM's worker
heartbeat goes stale OR its ``run.log`` freezes past the hung-process threshold
(the ``tradfi-bf-cme`` that hung after ``ApiKeyReloader`` is the canonical case).
The zombie watchdog beside it KILLS the hung VM — but the WORK is incomplete, so
the deterministic in-band recovery is to RE-LAUNCH the same backfill via its
``deployment-service/scripts/vm/launch-*`` launcher (which streams durable GCS
logs + registers a deployment heartbeat — so the relaunch is NEVER
fire-and-forget).

This differs from ``relaunch_backfill_vm`` (the OOM/exit-137 case) on the TRIGGER
only: a STALL is not an exit code. The watchdog already terminated the VM, so the
relaunch is unconditional on exit code — the stall verdict IS the trigger. A
repeated stall means a genuinely-wedged shard (network partition / unbounded HTTP
hang), so the relaunch is **budget-bounded**: at most ``_MAX_RELAUNCHES_PER_DAY``
(=2) per (vm-prefix, day) before the actuator pages the operator
(``escalation.py`` then falls the finding through to ``file_issue`` for a slot to
diagnose the hang's root cause — almost always an outbound call lacking a
``timeout=`` per the CLAUDE.md hung-process rule).

Mirrors the shipped ``refetch_feed`` / ``relaunch_backfill_vm`` actuator pattern —
idempotent, dry-runnable, cloud-agnostic (no direct ``google.cloud``; the relaunch
is the existing launcher subprocess, which itself uses the sanctioned SDK
boundary), and it emits a lifecycle event. It is NOT a ``Layer0Script`` subclass;
these DP actuators live wholly in deployment-service, invoked from
``escalation.py::route_finding``'s ``auto_recover`` tier.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from unified_trading_library import log_event
from unified_trading_library.events import DP_VM_STALL  # noqa: qg-deep-import

logger = logging.getLogger("recovery.relaunch_stalled_vm")

# Relaunch budget per (vm-prefix, day). A 3rd stall in a day = a genuinely-wedged
# shard (the hang root cause must be fixed in code — usually an unbounded HTTP
# call) → page the operator. Keyed on a calendar day (a backfill VM lifetime is
# hours, not the 120s feed cadence). Mirrors ``relaunch_backfill_vm``'s budget.
_MAX_RELAUNCHES_PER_DAY: int = 2

# The deployment-service VM launcher dir (launchers stream durable logs +
# register a heartbeat → never fire-and-forget).
_LAUNCHER_DIR = Path(__file__).resolve().parent.parent / "vm"


def _default_budget_dir() -> Path:
    return Path(tempfile.gettempdir()) / "uts_stalled_relaunch_budget"


def vm_prefix(vm_name: str) -> str:
    """The launcher-keying prefix = the VM name's first hyphen segment.

    Matches ``vm_zombie_watchdog.VM_PREFIX_TO_BUCKET`` keying — the budget is
    per launcher-family, not per ephemeral VM instance (each stalled VM the
    watchdog kills self-deletes with a fresh timestamped name).
    """
    return vm_name.split("-", 1)[0] if "-" in vm_name else vm_name


def _default_run_launcher(launcher: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Invoke the existing VM launcher script (streams durable logs + registers)."""
    path = _LAUNCHER_DIR / launcher
    if not path.exists():
        raise FileNotFoundError(f"launcher {launcher!r} not found under {_LAUNCHER_DIR}")
    full_env = {**_os_environ_snapshot(), **env}
    return subprocess.run(
        ["bash", str(path)],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
        env=full_env,
    )


def _os_environ_snapshot() -> dict[str, str]:
    """Inherit the ambient launcher env (PATH, ADC creds, project) for the subprocess.

    This is process-env passthrough for a launcher subprocess, NOT app config —
    the launcher resolves its own GCP project via the standard env the cron job
    already exports.
    """
    import os

    return dict(os.environ)


class RelaunchStalledVm:
    """Re-launch a watchdog-killed stalled backfill VM, bounded to ``_MAX_RELAUNCHES_PER_DAY``."""

    def __init__(
        self,
        *,
        budget_dir: Path | None = None,
        max_per_day: int = _MAX_RELAUNCHES_PER_DAY,
        now: Callable[[], datetime] | None = None,
        run_launcher: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self._budget_dir = budget_dir or _default_budget_dir()
        self._max_per_day = max_per_day
        self._now = now or (lambda: datetime.now(UTC))
        self._run_launcher = run_launcher or _default_run_launcher

    def dry_run_plan(self, vm_name: str, *, launcher: str) -> dict[str, str]:
        return {
            "action": "relaunch_stalled_vm",
            "vm_name": vm_name,
            "vm_prefix": vm_prefix(vm_name),
            "launcher": launcher,
            "effect": "re-run the backfill launcher (streams durable logs + registers heartbeat)",
        }

    def relaunch(
        self,
        vm_name: str,
        *,
        launcher: str,
        asset_group: str = "",
        launcher_env: dict[str, str] | None = None,
        dry_run: bool = False,
    ) -> dict[str, str | bool | int | None]:
        """Re-launch the stalled backfill, budget-bounded. Never raises.

        - no ``launcher`` binding → ``status=SKIPPED`` (``reason=no_launcher_binding``);
          the caller falls through to file_issue (cannot relaunch deterministically).
        - budget exhausted → ``status=PAGE`` (``reason=budget_exceeded``) — the
          caller escalates to page_operator / file_issue (the hang root cause is
          the human/worker call: fix the unbounded call).
        - SDK/launcher failure → ``status=FAILED`` (caller falls to file_issue).
        - else → ``status=SUCCEEDED`` (relaunched).
        """
        prefix = vm_prefix(vm_name)
        base: dict[str, str | bool | int | None] = {
            "vm_name": vm_name,
            "vm_prefix": prefix,
            "asset_group": asset_group,
        }

        if not launcher:
            # No launcher binding in the finding → cannot relaunch deterministically.
            return {
                **base,
                "status": "SKIPPED",
                "reason": "no_launcher_binding",
                "detail": "DP_VM_STALL finding carried no relaunch_launcher — file_issue owns it",
            }

        if dry_run:
            return {**base, "status": "DRY_RUN", **self.dry_run_plan(vm_name, launcher=launcher)}

        count_today = self._relaunches_today(prefix)
        if count_today >= self._max_per_day:
            log_event(
                DP_VM_STALL,
                severity="CRITICAL",
                details={
                    **base,
                    "recovery_action": "relaunch_stalled_vm",
                    "relaunch_budget_exceeded": True,
                    "relaunches_today": count_today,
                    "max_per_day": self._max_per_day,
                    "detail": "stall relaunch budget spent — page operator (fix the hung-process root cause)",
                },
            )
            return {
                **base,
                "status": "PAGE",
                "reason": "budget_exceeded",
                "relaunches_today": count_today,
                "max_per_day": self._max_per_day,
            }

        self._stamp_relaunch(prefix)
        env = launcher_env or {}
        try:
            result = self._run_launcher(launcher, env=env)
        except Exception as exc:  # launcher/SDK failure → file_issue fallthrough
            logger.warning("relaunch_stalled_vm: launcher %s failed: %s", launcher, exc)
            return {**base, "status": "FAILED", "launcher": launcher, "detail": f"launcher failed: {exc!r}"[:500]}

        if result.returncode != 0:
            return {
                **base,
                "status": "FAILED",
                "launcher": launcher,
                "returncode": result.returncode,
                "stderr": (result.stderr or "")[-500:],
            }

        log_event(
            DP_VM_STALL,
            severity="WARN",
            details={
                **base,
                "recovery_action": "relaunch_stalled_vm",
                "relaunched": True,
                "launcher": launcher,
                "relaunches_today": count_today + 1,
                "max_per_day": self._max_per_day,
            },
        )
        return {
            **base,
            "status": "SUCCEEDED",
            "launcher": launcher,
            "relaunches_today": count_today + 1,
            "stdout_tail": (result.stdout or "")[-300:],
        }

    # ── per-(prefix, day) budget sentinel ──────────────────────────────────

    def _budget_path(self, prefix: str) -> Path:
        return self._budget_dir / f"{prefix}.json"

    def _day_key(self) -> str:
        return self._now().strftime("%Y-%m-%d")

    def _relaunches_today(self, prefix: str) -> int:
        path = self._budget_path(prefix)
        if not path.exists():
            return 0
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return 0
        if state.get("day") != self._day_key():
            return 0  # a new day resets the budget
        return int(state.get("count", 0))

    def _stamp_relaunch(self, prefix: str) -> None:
        self._budget_dir.mkdir(parents=True, exist_ok=True)
        path = self._budget_path(prefix)
        count = self._relaunches_today(prefix) + 1
        path.write_text(
            json.dumps({"day": self._day_key(), "count": count}),
            encoding="utf-8",
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Re-launch a watchdog-killed stalled backfill VM, budget-bounded.")
    parser.add_argument("--vm-name", required=True, dest="vm_name")
    parser.add_argument("--launcher", required=True, help="launcher script name under scripts/vm/")
    parser.add_argument("--asset-group", default="", dest="asset_group")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    actuator = RelaunchStalledVm()
    result = actuator.relaunch(
        args.vm_name,
        launcher=args.launcher,
        asset_group=args.asset_group,
        dry_run=args.dry_run,
    )
    logger.info("relaunch_stalled_vm result=%s", result)
    return 0 if result.get("status") in ("SUCCEEDED", "DRY_RUN", "SKIPPED") else 1


if __name__ == "__main__":
    sys.exit(main())
