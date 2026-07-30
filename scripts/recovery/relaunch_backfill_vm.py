# Epic: observability_master
# Lifecycle: permanent
# Delete-when: NA
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

Also home to ``RelaunchPreemptedVm`` (Runbook RB-INFRA-001b, DP-VM-008 /
``DP_VM_PREEMPTED``, resolved-bookend DP-VM-011 / ``DP_VM_PREEMPTED_RECOVERED``)
— the preemption-aware relauncher for
``cefi_completion_program_2026_07_15.md`` P0 "Close the SPOT-preemption
relaunch gap". A DIFFERENT trigger (unconditional on exit code — the durable
``PREEMPTED`` signal blob IS the trigger, not an exit code) and a DIFFERENT
failure contract (every failure path self-emits a CRITICAL
``DP_VM_PREEMPTED_NO_RELAUNCH`` — see that class's docstring) than the OOM
relauncher above, so it is its own class with its own budget namespace, not a
second code path bolted onto ``RelaunchBackfillVm``.
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
from typing import cast

from unified_trading_library import UnifiedCloudConfig, get_storage_client, log_event
from unified_trading_library.events import DP_VM_EXIT_NONZERO  # noqa: qg-deep-import

from deployment_service.vm.tarball_pins import (
    TARBALL_SHA_ENV_TO_NAME,
    collect_pins_for_vm,
    resolve_available_pin,
)

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


# Deliberately NOT UTL-exported constants (unlike DP_VM_EXIT_NONZERO above) — this
# fix (cefi_completion_program_2026_07_15.md P0 "Close the SPOT-preemption
# relaunch gap") is scoped to deployment-service only; ``log_event`` takes a plain
# string, no Python type validates it (all three ARE registered in UAC
# DATA_PIPELINE_ALERT_RULES as DP-VM-008/009/011 — see
# vm_fleet_preemption_autorecovery_gap_2026_07_23.md item 5).
_EVENT_VM_PREEMPTED = "DP_VM_PREEMPTED"
_EVENT_VM_PREEMPTED_NO_RELAUNCH = "DP_VM_PREEMPTED_NO_RELAUNCH"
# Resolved-bookend for a SUCCESSFUL relaunch (DP-VM-011) — deliberately a
# DISTINCT event name from _EVENT_VM_PREEMPTED (the open-alert detection emitted
# by exit_code_fleet_monitor), not a reuse of it: reusing the same name made the
# "preempted" and "recovered" cases indistinguishable in #data-pipeline-alerts
# (both rendered as a bare "DP_VM_PREEMPTED" line; a reader had to parse
# `details.relaunched` to tell them apart). vm_name/asset_group appear in both
# events' details so a human can still correlate open->resolved by content (no
# alerting-service threading exists to do it automatically — webhook-only,
# opened-at-ts correlation per the AO-alerts bookend convention).
_EVENT_VM_PREEMPTED_RECOVERED = "DP_VM_PREEMPTED_RECOVERED"

# Separate budget namespace from the OOM relauncher above (own budget dir + own
# per-(vm-prefix, day) cap) — a SPOT VM legitimately preempts far more often than
# an OOM crash-loops (measured: as often as ~hourly under the operator's cap-1
# regime), so its budget is deliberately generous rather than sharing OOM's ≤2/day
# (which would defeat the whole point of this actuator the first afternoon).
_MAX_PREEMPTION_RELAUNCHES_PER_DAY: int = 48


# Rate-budget keys are DERIVED at launch from the fleet size + live daily quota, so
# replaying them is replaying a stale share. A VM preempted when it was ALONE carries
# a full-key budget; relaunching it into a now-crowded fleet re-applies that budget and
# oversubscribes the shared key — the same "replay stale launch params" root cause as
# START_DATE (see the force-replay guard above). Measured 2026-07-18: five concurrent
# api-football VMs each holding a full-budget share produced 61 `rateLimit` FALSE
# attempted_failed rows in ~30min.
#
# Dropping them makes the launcher RE-DERIVE on replay: it now counts the running
# af-backfill-*/af-audit-* fleet and divides by count+1 (measured, not assumed).
# REMAINING_DAILY_QUOTA is dropped too — it is a point-in-time reading of the live
# /status quota and is meaningless hours later.
_STALE_ON_REPLAY: tuple[str, ...] = (
    "SPORTS_ADAPTER_RATE_RPM",
    "SPORTS_ADAPTER_CONCURRENCY",
    "FLEET_VMS",
    "REMAINING_DAILY_QUOTA",
)


def _drop_stale_rate_budget(env: dict[str, str]) -> dict[str, str]:
    """Strip launch-time-derived rate-budget keys so the launcher recomputes them."""
    return {k: v for k, v in env.items() if k not in _STALE_ON_REPLAY}


# A code-tarball SHA pin is the OPPOSITE of a stale-on-replay key: it must SURVIVE
# the replay verbatim, and be re-resolved ONLY when the pinned object is proven
# missing. Never add ``*_TARBALL_SHA`` to ``_STALE_ON_REPLAY`` — dropping it makes
# the launcher's ``[[ -n ... ]]`` guard fall through to a FLOATING pull, i.e. the
# exact silent code-identity degrade this re-pin path exists to prevent.
_EVENT_VM_TARBALL_REPINNED = "DP_VM_TARBALL_REPINNED"


def _default_resolve_pin(tarball_name: str, requested_sha: str) -> str | None:
    """Resolve a pin against the live code bucket (see ``tarball_pins``)."""
    project = getattr(UnifiedCloudConfig(), "gcp_project_id", "") or ""
    if not project:
        return None
    return resolve_available_pin(get_storage_client(), f"deployment-scripts-{project}", tarball_name, requested_sha)


def _default_load_vm_pins(vm_name: str) -> dict[str, str]:
    """Read ``vm_name``'s recorded tarball pins from the authoritative registry.

    This exists because ``launch_env`` (``LAUNCH_PARAMS.json``) is NOT where the
    pins are. The launchers that pin and the launcher that writes LAUNCH_PARAMS
    are disjoint sets, so re-resolving pins from ``launch_env`` alone re-resolved
    an empty dict every time — the actuator looked implemented and did nothing.
    ``collect_pins_for_vm`` reads the same durable ``TARBALL_PINS.json`` registry
    the retention sweep protects against, which is the one source that still
    exists once the instance (and its metadata) is gone — precisely the state a
    preemption relaunch operates in.
    """
    project = getattr(UnifiedCloudConfig(), "gcp_project_id", "") or ""
    if not project:
        return {}
    name_to_env = {name: key for key, name in TARBALL_SHA_ENV_TO_NAME.items()}
    pins = collect_pins_for_vm(get_storage_client(), f"deployment-scripts-{project}", vm_name)
    return {name_to_env[pin.tarball_name]: pin.sha for pin in pins if pin.tarball_name in name_to_env}


def _default_preemption_budget_dir() -> Path:
    return Path(tempfile.gettempdir()) / "uts_preempted_relaunch_budget"


class RelaunchPreemptedVm:
    """Re-launch a SPOT-preempted backfill VM, replaying its exact launch params.

    Differs from ``RelaunchBackfillVm`` (the OOM/exit-137 case) on BOTH the
    trigger and the failure-silence contract:

    - **Trigger**: unconditional on exit code — a preemption's exit code is
      whatever SIGTERM produced, not a signal of the failure mode. The
      ``PREEMPTED`` verdict itself (the durable GCS ``PREEMPTED`` blob) is the
      trigger, mirroring ``RelaunchStalledVm``.
    - **Exact replay**: ``launch_env`` (read from the ``LAUNCH_PARAMS.json``
      blob ``lc_write_launch_params`` writes at VM-creation time) is passed
      through to the launcher subprocess UNCHANGED, so the relaunch reproduces
      the SAME venues/START_DATE/concurrency/lease the preempted VM was
      running — never a blind relaunch onto the launcher's bare defaults
      (which for ``launch-cefi-sharded-backfill.sh`` would mean the FULL
      genesis-to-now venue universe instead of the narrow tail that died).
      ``launch_env`` absent/empty (an older VM, or a launcher that doesn't call
      the helper yet) falls back to just the ambient env — the launcher's own
      ``tardis_concurrency_guard`` still gates it either way.
    - **Failure is never silent**: this is the ONE difference from every other
      actuator in this module. A preempted backfill silently vanishing with
      zero operator signal was the exact 2026-07-16 finding this closes
      (``exit_code_fleet_monitor`` logged an INFO line claiming a relaunch that
      did not exist). So EVERY non-success path here — no launcher bound,
      budget exhausted, the launcher subprocess erroring, OR the launcher's own
      guard REFUSING (e.g. a live/forward VM holds the single Tardis slot) —
      self-emits a CRITICAL ``DP_VM_PREEMPTED_NO_RELAUNCH``, not just the
      quiet ``status=SKIPPED``/``FAILED`` dict the OOM/STALL actuators return
      (those rely solely on ``route_finding``'s generic file_issue fallthrough;
      this one ALSO pages directly, because "the relaunch didn't happen" is
      the alert Fix 2 exists to guarantee).
    """

    def __init__(
        self,
        *,
        budget_dir: Path | None = None,
        max_per_day: int = _MAX_PREEMPTION_RELAUNCHES_PER_DAY,
        now: Callable[[], datetime] | None = None,
        run_launcher: Callable[..., subprocess.CompletedProcess[str]] | None = None,
        resolve_pin: Callable[[str, str], str | None] | None = None,
        load_vm_pins: Callable[[str], dict[str, str]] | None = None,
    ) -> None:
        self._budget_dir = budget_dir or _default_preemption_budget_dir()
        self._max_per_day = max_per_day
        self._now = now or (lambda: datetime.now(UTC))
        self._run_launcher = run_launcher or _default_run_launcher
        self._resolve_pin = resolve_pin or _default_resolve_pin
        self._load_vm_pins = load_vm_pins or _default_load_vm_pins

    def _merge_recorded_pins(self, vm_name: str, env: dict[str, str]) -> dict[str, str]:
        """Fill in ``*_TARBALL_SHA`` keys the captured launch env does not carry.

        ``launch_env`` wins where it HAS a value (it is the more specific record);
        the registry supplies everything it lacks — which, for all five pinning
        launchers, is every pin there is.
        """
        merged = dict(env)
        try:
            recorded = self._load_vm_pins(vm_name)
        except Exception as exc:
            logger.warning("could not read recorded tarball pins for %s: %s", vm_name, exc)
            return merged
        for env_key, sha in recorded.items():
            if not merged.get(env_key, "").strip():
                merged[env_key] = sha
        if recorded:
            logger.info("merged %d recorded tarball pin(s) for %s from the durable registry", len(recorded), vm_name)
        return merged

    def _resolve_tarball_pins(self, env: dict[str, str]) -> tuple[dict[str, str], list[dict[str, str]], list[str]]:
        """Re-resolve every ``*_TARBALL_SHA`` pin against what actually EXISTS.

        Returns ``(env, repins, unresolved)``. A pin whose object is intact is
        left untouched. A pin whose ``.tar.gz`` is gone is re-pointed at the
        newest COMPLETE pair so the caller can re-pin LOUDLY. A pin with no
        surviving candidate lands in ``unresolved`` — the caller PAGEs and must
        NOT unset the key: an empty value is indistinguishable from an absent one
        to the launcher, and both mean a silent floating pull.
        """
        updated = dict(env)
        repins: list[dict[str, str]] = []
        unresolved: list[str] = []
        for env_key, tarball_name in TARBALL_SHA_ENV_TO_NAME.items():
            requested = env.get(env_key, "").strip()
            if not requested:
                continue
            try:
                available = self._resolve_pin(tarball_name, requested)
            except Exception as exc:
                logger.warning("tarball pin resolution failed for %s: %s", tarball_name, exc)
                unresolved.append(env_key)
                continue
            if available == requested:
                continue
            if not available:
                unresolved.append(env_key)
                continue
            updated[env_key] = available
            repins.append(
                {"env_key": env_key, "tarball_name": tarball_name, "old_sha": requested, "new_sha": available}
            )
        return updated, repins, unresolved

    def dry_run_plan(
        self,
        vm_name: str,
        *,
        launcher: str,
        launch_env: dict[str, str] | None,
        checkpoint: dict[str, str] | None = None,
    ) -> dict[str, object]:
        return {
            "action": "relaunch_preempted_vm",
            "vm_name": vm_name,
            "vm_prefix": vm_prefix(vm_name),
            "launcher": launcher,
            "launch_env": launch_env or {},
            "progress_checkpoint": checkpoint or {},
            "effect": "re-run the backfill launcher with the captured launch params (streams durable logs + registers heartbeat)",
        }

    def relaunch(
        self,
        vm_name: str,
        *,
        launcher: str,
        asset_group: str = "",
        launch_env: dict[str, str] | None = None,
        checkpoint: dict[str, str] | None = None,
        dry_run: bool = False,
    ) -> dict[str, object]:
        """Re-launch the preempted backfill, budget-bounded. Never raises.

        - no ``launcher`` binding → ``status=SKIPPED`` (``reason=no_launcher_binding``)
          + a self-emitted CRITICAL ``DP_VM_PREEMPTED_NO_RELAUNCH``.
        - budget exhausted → ``status=PAGE`` (``reason=budget_exceeded``) + CRITICAL.
        - launcher subprocess raises, or returns non-zero (incl. the launcher's
          OWN ``tardis_concurrency_guard`` refusing the cap) → ``status=FAILED``
          + CRITICAL.
        - else → ``status=SUCCEEDED`` — a quiet INFO ``DP_VM_PREEMPTED_RECOVERED``
          resolved-bookend (the routine, benign case; SPOT reclaim is expected).
        """
        prefix = vm_prefix(vm_name)
        base: dict[str, str | bool | int | None] = {
            "vm_name": vm_name,
            "vm_prefix": prefix,
            "asset_group": asset_group,
        }

        if not launcher:
            log_event(
                _EVENT_VM_PREEMPTED_NO_RELAUNCH,
                severity="CRITICAL",
                details={
                    **base,
                    "recovery_action": "relaunch_preempted_vm",
                    "reason": "no_launcher_binding",
                    "detail": "DP_VM_PREEMPTED finding carried no relaunch_launcher — cannot relaunch deterministically",
                },
            )
            return {
                **base,
                "status": "SKIPPED",
                "reason": "no_launcher_binding",
                "detail": "DP_VM_PREEMPTED finding carried no relaunch_launcher binding",
            }

        if dry_run:
            return {
                **base,
                "status": "DRY_RUN",
                **self.dry_run_plan(vm_name, launcher=launcher, launch_env=launch_env, checkpoint=checkpoint),
            }

        count_today = self._relaunches_today(prefix)
        if count_today >= self._max_per_day:
            log_event(
                _EVENT_VM_PREEMPTED_NO_RELAUNCH,
                severity="CRITICAL",
                details={
                    **base,
                    "recovery_action": "relaunch_preempted_vm",
                    "relaunch_budget_exceeded": True,
                    "relaunches_today": count_today,
                    "max_per_day": self._max_per_day,
                    "detail": "preemption relaunch budget spent for today — investigate why this VM keeps "
                    "preempting (zone SPOT capacity? a concurrent live/forward VM repeatedly winning "
                    "the Tardis slot?) rather than relaunching blindly again",
                },
            )
            return {
                **base,
                "status": "PAGE",
                "reason": "budget_exceeded",
                "relaunches_today": count_today,
                "max_per_day": self._max_per_day,
            }

        # ── Progress-checkpoint resume — the durable fix for the SPOT day-one-replay
        # bug. If the VM wrote a PROGRESS.json checkpoint as it completed days (UTL
        # ``record_vm_progress`` -> ``ManifestWriter.record_captured``), RESUME from its
        # frontier instead of replaying the terminated VM's original START_DATE.
        #
        # This is what makes a ``--force`` / ``redo_all`` run recover correctly:
        # ``--force`` DISABLES presence-skip, so a plain replay of the original
        # START_DATE restarts the backfill at day one on EVERY preemption — forever
        # (measured 2026-07-18: ~54 days/hour over a 2,390-day range => ~44h runtime,
        # SPOT reclaim after ~10 min => re-doing 2019-01-01.. indefinitely). Overriding
        # START_DATE to the last recorded date makes progress monotonic.
        #
        # SAFETY: only skip START_DATE forward when the checkpoint frontier is
        # ``monotonic`` (the run recorded dates in chronological order, so every date
        # BEFORE the frontier is complete — resuming from it redoes at most the last,
        # possibly-partial day and skips nothing). A NON-monotonic run (venue-outer
        # iteration) has undone dates behind its max, so moving START_DATE up would
        # DROP them; for that case (and when there is no checkpoint at all) a
        # ``--force`` run PAGEs loudly rather than risk a silent gap.
        # SSOT: ``codex/05-infrastructure/spot-vms-for-backfill.md`` § "Preemption
        # recovery MUST resume from PROGRESS, never replay START_DATE".
        env = dict(launch_env or {})
        force = str(env.get("VM_FORCE", "")).strip().lower() == "true"
        resume_date = ""
        if checkpoint is not None:
            monotonic = str(checkpoint.get("monotonic", "false")).strip().lower() == "true"
            last = str(checkpoint.get("last_completed_date", "")).strip()
            if last and monotonic:
                resume_date = last

        if resume_date:
            # The persisted date key is ``START_DATE`` (what lc_write_launch_params
            # writes + the child launcher reads), NOT ``VM_START_DATE`` (a
            # gcloud-metadata-only alias). Resume verbatim from the frontier — the
            # writer records the max date SEEN (the possibly-partial current day), so
            # redoing that one day guarantees no skip.
            env["START_DATE"] = resume_date
            log_event(
                _EVENT_VM_PREEMPTED,
                severity="INFO",
                details={
                    **base,
                    "recovery_action": "relaunch_preempted_vm",
                    "resume_from_checkpoint": resume_date,
                    "force_run": force,
                    "detail": "resuming from the monotonic PROGRESS checkpoint frontier instead of "
                    "replaying the original START_DATE",
                },
            )
        elif force:
            log_event(
                _EVENT_VM_PREEMPTED_NO_RELAUNCH,
                severity="CRITICAL",
                details={
                    **base,
                    "recovery_action": "relaunch_preempted_vm",
                    "force_run_not_replayable": True,
                    "detail": "preempted VM ran with VM_FORCE=true (redo_all) and left no monotonic "
                    "PROGRESS checkpoint; replaying its original START_DATE would restart the backfill "
                    "from day one on EVERY preemption because --force disables the presence-skip a normal "
                    "backfill resumes by. Relaunch from last_completed_unit+1 (measure from the TARGET "
                    "artifact, entity-scoped) or ensure the run emits record_vm_progress checkpoints.",
                },
            )
            return {**base, "status": "PAGE", "reason": "force_run_not_replayable"}

        # ── Code-tarball pin re-resolution. A pinned tarball can be REAPED out from
        # under a long-running fleet by the daily retention sweep (the 2026-07-20
        # cefi-migration outage: `cleanup_old_tarballs.py --keep 5` ranks purely on
        # mtime, so a fleet outliving 5 pushes of a high-velocity repo like UAC lost
        # its pin, and every relaunch then died at setup with "refusing floating
        # fallback" + self-delete — silently). Retention is now pin-aware
        # (`deployment_service.vm.tarball_pins`), but a pin reaped BEFORE that
        # shipped, or one aged past the grace window, still has to be recoverable.
        #
        # So: re-point a dead pin at the newest COMPLETE (tarball+manifest) pair and
        # say so LOUDLY — the code identity of the relaunch changed, which is an
        # actionable operator signal, not a lifecycle event. If nothing resolves we
        # PAGE rather than degrade: `setup-data-pipeline-vm.sh`'s refusal of a
        # floating fallback is CORRECT and is deliberately preserved here.
        #
        # The pins are read from the DURABLE REGISTRY first, not from `env`:
        # `launch_env` comes from LAUNCH_PARAMS.json, which no pinning launcher
        # writes, so resolving straight from `env` re-resolved nothing at all.
        env = self._merge_recorded_pins(vm_name, env)
        env, repins, unresolved_pins = self._resolve_tarball_pins(env)
        for repin in repins:
            log_event(
                _EVENT_VM_TARBALL_REPINNED,
                severity="CRITICAL",
                details={
                    **base,
                    **repin,
                    "recovery_action": "relaunch_preempted_vm",
                    "detail": (
                        f"pinned tarball {repin['tarball_name']}@{repin['old_sha'][:12]} no longer exists "
                        f"(reaped by retention?) — RE-PINNED to {repin['new_sha'][:12]}. The relaunch runs "
                        "DIFFERENT code than the preempted VM did; verify this is acceptable for the "
                        "in-flight migration, and rebuild the original sha via create-code-tarballs.sh if not."
                    ),
                },
            )
        if unresolved_pins:
            log_event(
                _EVENT_VM_PREEMPTED_NO_RELAUNCH,
                severity="CRITICAL",
                details={
                    **base,
                    "recovery_action": "relaunch_preempted_vm",
                    "unresolved_tarball_pins": ",".join(sorted(unresolved_pins)),
                    "detail": "the VM's pinned code tarball(s) are GONE and no SHA-pinned replacement with a "
                    "complete tarball+manifest pair exists. Refusing to relaunch onto the floating tarball "
                    "(un-asserted code against a half-migrated corpus is a data-correctness hazard). Rebuild "
                    "the pin with create-code-tarballs.sh, then re-run this relaunch.",
                },
            )
            return {
                **base,
                "status": "PAGE",
                "reason": "tarball_pin_unresolvable",
                "unresolved_tarball_pins": ",".join(sorted(unresolved_pins)),
            }

        self._stamp_relaunch(prefix)
        env = _drop_stale_rate_budget(env)
        try:
            result = self._run_launcher(launcher, env=env)
        except Exception as exc:  # launcher/SDK failure → CRITICAL + file_issue fallthrough
            logger.warning("relaunch_preempted_vm: launcher %s failed: %s", launcher, exc)
            log_event(
                _EVENT_VM_PREEMPTED_NO_RELAUNCH,
                severity="CRITICAL",
                details={
                    **base,
                    "recovery_action": "relaunch_preempted_vm",
                    "launcher": launcher,
                    "detail": f"launcher raised: {exc!r}"[:500],
                },
            )
            return {**base, "status": "FAILED", "launcher": launcher, "detail": f"launcher failed: {exc!r}"[:500]}

        if result.returncode != 0:
            # Includes the launcher's OWN tardis_concurrency_guard refusing (e.g. a
            # live/forward VM currently holds the single Tardis slot) — the guard
            # binds INSIDE the launcher subprocess, so a refusal surfaces here as a
            # non-zero exit, never a silent no-op.
            log_event(
                _EVENT_VM_PREEMPTED_NO_RELAUNCH,
                severity="CRITICAL",
                details={
                    **base,
                    "recovery_action": "relaunch_preempted_vm",
                    "launcher": launcher,
                    "returncode": result.returncode,
                    "stderr": (result.stderr or "")[-500:],
                    "detail": "relaunch subprocess failed — likely the launcher's own concurrency guard "
                    "refusing (another Tardis consumer holds the slot) or a genuine launcher error",
                },
            )
            return {
                **base,
                "status": "FAILED",
                "launcher": launcher,
                "returncode": result.returncode,
                "stderr": (result.stderr or "")[-500:],
            }

        log_event(
            _EVENT_VM_PREEMPTED_RECOVERED,
            severity="INFO",
            details={
                **base,
                "recovery_action": "relaunch_preempted_vm",
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

    # ── per-(prefix, day) budget sentinel (own namespace — see the module
    # constant above) ───────────────────────────────────────────────────────

    def _budget_path(self, prefix: str) -> Path:
        return self._budget_dir / f"{prefix}.json"

    def _day_key(self) -> str:
        return self._now().strftime("%Y-%m-%d")

    def _relaunches_today(self, prefix: str) -> int:
        path = self._budget_path(prefix)
        if not path.exists():
            return 0
        try:
            state = cast("dict[str, object]", json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            return 0
        if state.get("day") != self._day_key():
            return 0  # a new day resets the budget
        count = state.get("count", 0)
        return int(cast("str | int | float", count))

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
