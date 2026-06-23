# Epic: observability_master
# Lifecycle: permanent
"""Self-healing actuator — re-launch an OOM-killed (exit-137) backfill VM.

Runbook: RB-INFRA-001 (DP-VM-001 / ``DP_VM_EXIT_NONZERO`` with ``exit_code=137``).

A backfill VM launched with ``VM_SHUTDOWN_ON_COMPLETION=true`` self-deletes on
exit. When it exits 137 (OOM) the run is INCOMPLETE — the exit-code fleet monitor
surfaces ``DP_VM_EXIT_NONZERO`` (oom=True). For the OOM subcase the deterministic
in-band recovery is to re-launch the same backfill (via its
``deployment-service/scripts/vm/launch-*`` launcher, which streams durable GCS
logs + registers a deployment heartbeat — so the relaunch is NEVER
fire-and-forget). A repeated OOM means a too-small machine, so the relaunch is
**budget-bounded**: at most ``_MAX_RELAUNCHES_PER_DAY`` (=2) per (vm-prefix, day)
before the actuator pages the operator (resize is the human call —
``resize_machine_after_oom`` Layer-0 script).

This mirrors the shipped ``refetch_feed`` actuator pattern — idempotent,
dry-runnable, cloud-agnostic (no direct ``google.cloud``; the relaunch is the
existing launcher subprocess, which itself uses the sanctioned SDK boundary), and
it emits a lifecycle event. It is NOT a ``Layer0Script`` subclass (that base
hard-binds a UAC ``ActionType`` + ``RecoveryScriptRegistry`` entry, cross-repo);
these DP actuators live wholly in deployment-service, invoked from
``escalation.py::route_finding``'s ``auto_recover`` tier.

Only ``exit_code == 137`` (OOM) is relaunched here; any other non-zero exit
returns a ``status=SKIPPED`` (not_oom) so the page-tier owns it.
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
from unified_trading_library.events import DP_VM_EXIT_NONZERO  # noqa: qg-deep-import

logger = logging.getLogger("recovery.relaunch_backfill_vm")

# Relaunch budget per (vm-prefix, day). A 3rd OOM in a day = the machine is too
# small → page the operator (resize is the human call). Mirrors the bounded
# refetch_feed/auto_cooldown idiom but keyed on a calendar day (a backfill VM
# lifetime is hours, not the 120s feed cadence).
_MAX_RELAUNCHES_PER_DAY: int = 2
_OOM_EXIT_CODE: int = 137

# The deployment-service VM launcher dir (launchers stream durable logs +
# register a heartbeat → never fire-and-forget).
_LAUNCHER_DIR = Path(__file__).resolve().parent.parent / "vm"


def _default_budget_dir() -> Path:
    return Path(tempfile.gettempdir()) / "uts_backfill_relaunch_budget"


def vm_prefix(vm_name: str) -> str:
    """The launcher-keying prefix = the VM name's first hyphen segment.

    Matches ``vm_zombie_watchdog.VM_PREFIX_TO_BUCKET`` keying — the budget is
    per launcher-family, not per ephemeral VM instance (each OOM self-deletes
    with a fresh timestamped name).
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


class RelaunchBackfillVm:
    """Re-launch an OOM-killed backfill VM, bounded to ``_MAX_RELAUNCHES_PER_DAY``."""

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
            "action": "relaunch_backfill_vm",
            "vm_name": vm_name,
            "vm_prefix": vm_prefix(vm_name),
            "launcher": launcher,
            "effect": "re-run the backfill launcher (streams durable logs + registers heartbeat)",
        }

    def relaunch(
        self,
        vm_name: str,
        *,
        exit_code: int | None,
        launcher: str,
        asset_group: str = "",
        launcher_env: dict[str, str] | None = None,
        dry_run: bool = False,
    ) -> dict[str, str | bool | int | None]:
        """Re-launch the OOM'd backfill, budget-bounded. Never raises.

        - ``exit_code != 137`` → ``status=SKIPPED`` (``reason=not_oom``); the
          page tier owns a non-OOM crash.
        - budget exhausted → ``status=PAGE`` (``reason=budget_exceeded``) — the
          caller escalates to page_operator (resize is the human call).
        - SDK/launcher failure → ``status=FAILED`` (caller falls to file_issue).
        """
        prefix = vm_prefix(vm_name)
        base: dict[str, str | bool | int | None] = {
            "vm_name": vm_name,
            "vm_prefix": prefix,
            "asset_group": asset_group,
            "exit_code": exit_code,
        }

        if exit_code != _OOM_EXIT_CODE:
            return {
                **base,
                "status": "SKIPPED",
                "reason": "not_oom",
                "detail": f"exit_code={exit_code} is not 137 (OOM) — page tier owns it",
            }

        if dry_run:
            return {**base, "status": "DRY_RUN", **self.dry_run_plan(vm_name, launcher=launcher)}

        count_today = self._relaunches_today(prefix)
        if count_today >= self._max_per_day:
            log_event(
                DP_VM_EXIT_NONZERO,
                severity="CRITICAL",
                details={
                    **base,
                    "recovery_action": "relaunch_backfill_vm",
                    "relaunch_budget_exceeded": True,
                    "relaunches_today": count_today,
                    "max_per_day": self._max_per_day,
                    "detail": "OOM relaunch budget spent — page operator (resize the machine)",
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
            logger.warning("relaunch_backfill_vm: launcher %s failed: %s", launcher, exc)
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
            DP_VM_EXIT_NONZERO,
            severity="WARN",
            details={
                **base,
                "recovery_action": "relaunch_backfill_vm",
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
    parser = argparse.ArgumentParser(description="Re-launch an OOM-killed (exit-137) backfill VM, budget-bounded.")
    parser.add_argument("--vm-name", required=True, dest="vm_name")
    parser.add_argument("--exit-code", type=int, required=True, dest="exit_code")
    parser.add_argument("--launcher", required=True, help="launcher script name under scripts/vm/")
    parser.add_argument("--asset-group", default="", dest="asset_group")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    actuator = RelaunchBackfillVm()
    result = actuator.relaunch(
        args.vm_name,
        exit_code=args.exit_code,
        launcher=args.launcher,
        asset_group=args.asset_group,
        dry_run=args.dry_run,
    )
    logger.info("relaunch_backfill_vm result=%s", result)
    return 0 if result.get("status") in ("SUCCEEDED", "DRY_RUN", "SKIPPED") else 1


if __name__ == "__main__":
    sys.exit(main())
