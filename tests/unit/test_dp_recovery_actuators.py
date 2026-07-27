"""Unit tests for the Phase-6-C self-healing DP actuators + the escalation wiring.

Credential-free: the Cloud Run JobsClient + the VM launcher subprocess are
injected (never the real SDK / real subprocess), ``log_event`` is monkeypatched,
no real VM / job is launched.

Covers:
  - an ``auto_recover`` event invokes its actuator (consolidator relaunch
    re-executes the mocked Cloud Run Job + respects the 1/window cooldown);
  - OOM (exit-137) backfill relaunch ≤2/day then pages;
  - a non-OOM exit is skipped (page tier owns it);
  - an ``auto_recover`` event with NO wired actuator falls through to file_issue;
  - a FAILED actuator falls through to file_issue.
"""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from deployment_service.data_pipeline_monitors import escalation
from deployment_service.data_pipeline_monitors.escalation import (
    EscalationTier,
    PipelineFinding,
)
from scripts.recovery.relaunch_backfill_vm import RelaunchBackfillVm, RelaunchPreemptedVm, vm_prefix
from scripts.recovery.relaunch_consolidator import RelaunchConsolidator
from scripts.recovery.relaunch_stalled_vm import RelaunchStalledVm

_FIXED_NOW = datetime(2026, 6, 22, 12, 0, 0, tzinfo=UTC)


def _patch_log_event(monkeypatch) -> list[tuple[str, str, dict]]:
    emitted: list[tuple[str, str, dict]] = []
    for module in (
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        "scripts.recovery.relaunch_consolidator.log_event",
        "scripts.recovery.relaunch_backfill_vm.log_event",
        "scripts.recovery.relaunch_stalled_vm.log_event",
    ):
        monkeypatch.setattr(
            module,
            lambda event, severity="INFO", details=None: emitted.append((event, severity, details or {})),
        )
    # Mute the best-effort fast-spawn dispatch (Fix 3) so the wiring tests are
    # hermetic + block-network safe — its real non-raising behaviour has its own test.
    monkeypatch.setattr(
        escalation, "_dispatch_to_orchestrator", lambda _f, _p: {"dispatched": False, "reason": "muted"}
    )
    return emitted


# ── relaunch_consolidator ───────────────────────────────────────────────────


def test_consolidator_relaunch_re_executes_job(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    runs: list[tuple[str, str, str]] = []

    def fake_run_job(job_name: str, *, project_id: str, region: str) -> str:
        runs.append((job_name, project_id, region))
        return f"projects/{project_id}/locations/{region}/jobs/{job_name}/executions/exec-1"

    actuator = RelaunchConsolidator(
        cooldown_dir=tmp_path,
        project_id="my-proj",
        now=lambda: _FIXED_NOW,
        run_job=fake_run_job,
    )
    result = actuator.relaunch("defi")
    assert result["status"] == "SUCCEEDED"
    assert runs == [("manifest-consolidator-defi", "my-proj", "asia-northeast1")]


def test_consolidator_relaunch_respects_one_per_window(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    runs: list[str] = []

    def fake_run_job(job_name: str, *, project_id: str, region: str) -> str:
        runs.append(job_name)
        return "exec"

    actuator = RelaunchConsolidator(cooldown_dir=tmp_path, project_id="p", now=lambda: _FIXED_NOW, run_job=fake_run_job)
    first = actuator.relaunch("cefi")
    second = actuator.relaunch("cefi")  # inside the cooldown window
    assert first["status"] == "SUCCEEDED"
    assert second["status"] == "SUCCEEDED"
    assert second["relaunch_skipped"] == "cooldown"
    assert runs == ["manifest-consolidator-cefi"]  # exactly ONE real run


def test_consolidator_relaunch_dry_run_does_not_execute(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    runs: list[str] = []
    actuator = RelaunchConsolidator(
        cooldown_dir=tmp_path,
        project_id="p",
        now=lambda: _FIXED_NOW,
        run_job=lambda j, **_: runs.append(j) or "x",
    )
    result = actuator.relaunch("tradfi", dry_run=True)
    assert result["status"] == "DRY_RUN"
    assert runs == []


def test_consolidator_relaunch_unknown_ag_fails(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    actuator = RelaunchConsolidator(cooldown_dir=tmp_path, project_id="p", now=lambda: _FIXED_NOW)
    result = actuator.relaunch("nonsense")
    assert result["status"] == "FAILED"


# ── relaunch_backfill_vm ────────────────────────────────────────────────────


def _ok_launcher(_name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["bash", _name], returncode=0, stdout="launched", stderr="")


def test_backfill_relaunch_oom_re_launches_within_budget(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    launched: list[str] = []

    def launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append(name)
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    actuator = RelaunchBackfillVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=launcher)
    r1 = actuator.relaunch("sports-bf-1", exit_code=137, launcher="launch-x.sh", asset_group="sports")
    r2 = actuator.relaunch("sports-bf-2", exit_code=137, launcher="launch-x.sh", asset_group="sports")
    assert r1["status"] == "SUCCEEDED"
    assert r2["status"] == "SUCCEEDED"
    # Same vm-prefix "sports" → 2 relaunches consumed the budget.
    assert launched == ["launch-x.sh", "launch-x.sh"]
    assert r2["relaunches_today"] == 2


def test_backfill_relaunch_third_oom_pages(tmp_path: Path, monkeypatch):
    emitted = _patch_log_event(monkeypatch)
    actuator = RelaunchBackfillVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=_ok_launcher)
    actuator.relaunch("sports-a", exit_code=137, launcher="l.sh")
    actuator.relaunch("sports-b", exit_code=137, launcher="l.sh")
    third = actuator.relaunch("sports-c", exit_code=137, launcher="l.sh")  # budget spent
    assert third["status"] == "PAGE"
    assert third["reason"] == "budget_exceeded"
    # The page emits a CRITICAL DP_VM_EXIT_NONZERO with the budget flag.
    assert any(
        e[0] == "DP_VM_EXIT_NONZERO" and e[1] == "CRITICAL" and e[2].get("relaunch_budget_exceeded") for e in emitted
    )


# ── relaunch_stalled_vm (DP_VM_STALL self-heal, Fix 1) ──────────────────────


def test_stalled_relaunch_re_launches_within_budget(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    launched: list[str] = []

    def launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append(name)
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    actuator = RelaunchStalledVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=launcher)
    # the stall relaunch is UNCONDITIONAL on exit code (the watchdog killed it)
    r1 = actuator.relaunch("tradfi-bf-1", launcher="launch-tradfi-bf-cme.sh", asset_group="tradfi")
    r2 = actuator.relaunch("tradfi-bf-2", launcher="launch-tradfi-bf-cme.sh", asset_group="tradfi")
    assert r1["status"] == "SUCCEEDED"
    assert r2["status"] == "SUCCEEDED"
    assert launched == ["launch-tradfi-bf-cme.sh", "launch-tradfi-bf-cme.sh"]  # same vm-prefix → shared budget
    assert r2["relaunches_today"] == 2


def test_stalled_relaunch_third_pages(tmp_path: Path, monkeypatch):
    emitted = _patch_log_event(monkeypatch)
    actuator = RelaunchStalledVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=_ok_launcher)
    actuator.relaunch("tradfi-a", launcher="l.sh")
    actuator.relaunch("tradfi-b", launcher="l.sh")
    third = actuator.relaunch("tradfi-c", launcher="l.sh")  # budget spent
    assert third["status"] == "PAGE"
    assert third["reason"] == "budget_exceeded"
    # The page emits a CRITICAL DP_VM_STALL with the budget flag.
    assert any(e[0] == "DP_VM_STALL" and e[1] == "CRITICAL" and e[2].get("relaunch_budget_exceeded") for e in emitted)


def test_stalled_relaunch_no_launcher_skipped(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    launched: list[str] = []
    actuator = RelaunchStalledVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=lambda n, *, env: launched.append(n) or _ok_launcher(n, env=env),
    )
    result = actuator.relaunch("mystery-vm", launcher="")  # no launcher binding
    assert result["status"] == "SKIPPED"
    assert result["reason"] == "no_launcher_binding"
    assert launched == []  # nothing relaunched — caller falls through to file_issue


def test_stalled_relaunch_dry_run_does_not_execute(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    launched: list[str] = []
    actuator = RelaunchStalledVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=lambda n, *, env: launched.append(n) or _ok_launcher(n, env=env),
    )
    result = actuator.relaunch("tradfi-bf", launcher="l.sh", dry_run=True)
    assert result["status"] == "DRY_RUN"
    assert launched == []


# ── relaunch_stalled_vm progress-checkpoint resume (operator ask 2026-07-27) ─
# "stale vms should be watchdog killed and relaunched if they weren't complete"
# — mirrors RelaunchPreemptedVm's checkpoint-resume contract (minus tarball-repin).


def test_stalled_relaunch_resumes_from_monotonic_checkpoint(tmp_path: Path, monkeypatch):
    emitted = _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append((name, dict(env)))
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    actuator = RelaunchStalledVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=launcher)
    result = actuator.relaunch(
        "tradfi-bf-cme",
        launcher="launch-tradfi-bf-cme.sh",
        launch_env={"VENUE": "CME", "START_DATE": "2019-01-01"},
        checkpoint={"last_completed_date": "2026-05-01", "monotonic": "true"},
    )
    assert result["status"] == "SUCCEEDED"
    assert launched[0][1]["START_DATE"] == "2026-05-01"
    assert launched[0][1]["VENUE"] == "CME"
    assert any(
        e[0] == "DP_VM_STALL" and e[1] == "INFO" and e[2].get("resume_from_checkpoint") == "2026-05-01" for e in emitted
    )


def test_stalled_relaunch_force_run_no_checkpoint_pages(tmp_path: Path, monkeypatch):
    emitted = _patch_log_event(monkeypatch)
    launched: list[str] = []
    actuator = RelaunchStalledVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=lambda n, *, env: launched.append(n) or _ok_launcher(n, env=env),
    )
    result = actuator.relaunch(
        "tradfi-bf-cme",
        launcher="launch-tradfi-bf-cme.sh",
        launch_env={"VM_FORCE": "true", "START_DATE": "2019-01-01"},
    )
    assert result["status"] == "PAGE"
    assert result["reason"] == "force_run_not_replayable"
    assert launched == []
    assert any(e[0] == "DP_VM_STALL" and e[1] == "CRITICAL" and e[2].get("force_run_not_replayable") for e in emitted)


def test_stalled_relaunch_non_force_no_checkpoint_replays_verbatim(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append((name, dict(env)))
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    actuator = RelaunchStalledVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=launcher)
    result = actuator.relaunch(
        "tradfi-bf-cme",
        launcher="launch-tradfi-bf-cme.sh",
        launch_env={"START_DATE": "2026-02-01", "VENUE": "CME"},
    )
    assert result["status"] == "SUCCEEDED"
    assert launched[0][1]["START_DATE"] == "2026-02-01"  # unchanged — no checkpoint, verbatim replay


def test_stalled_relaunch_no_launch_env_or_checkpoint_still_works(tmp_path: Path, monkeypatch):
    """Back-compat: a caller that never passes launch_env/checkpoint (today's
    only real caller shape until the watcher wiring rolls out) keeps working."""
    _patch_log_event(monkeypatch)
    launched: list[str] = []
    actuator = RelaunchStalledVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=lambda n, *, env: launched.append(n) or _ok_launcher(n, env=env),
    )
    result = actuator.relaunch("tradfi-bf", launcher="launch-tradfi-bf-cme.sh")
    assert result["status"] == "SUCCEEDED"
    assert launched == ["launch-tradfi-bf-cme.sh"]


# ── relaunch_preempted_vm (DP_VM_PREEMPTED self-heal, Fix 1) ────────────────


def test_preempted_relaunch_replays_captured_launch_env(tmp_path: Path, monkeypatch):
    """The relaunch subprocess receives the EXACT env captured at VM-creation
    time (venues/START_DATE/concurrency/lease) — never a blind relaunch onto
    the launcher's bare defaults."""
    emitted = _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append((name, dict(env)))
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    actuator = RelaunchPreemptedVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=launcher)
    captured_env = {"VENUES": "BINANCE-FUTURES BYBIT", "START_DATE": "2026-02-01", "SINGLE_VM_QUEUE": "1"}
    result = actuator.relaunch(
        "cefi-queue-heavy-binancefutu-x15-20260716-075338",
        launcher="launch-cefi-sharded-backfill.sh",
        asset_group="cefi",
        launch_env=captured_env,
    )
    assert result["status"] == "SUCCEEDED"
    assert launched == [("launch-cefi-sharded-backfill.sh", captured_env)]
    # Success is QUIET — INFO only, never CRITICAL (SPOT reclaim is benign/expected).
    assert any(e[0] == "DP_VM_PREEMPTED" and e[1] == "INFO" and e[2].get("relaunched") for e in emitted)
    assert not any(e[1] == "CRITICAL" for e in emitted)


def test_preempted_relaunch_is_unconditional_on_exit_code(tmp_path: Path, monkeypatch):
    """Unlike the OOM actuator, the preemption verdict itself is the trigger —
    there is no exit_code gate (a preempted VM's exit code is whatever SIGTERM
    produced, not a signal of the failure mode)."""
    _patch_log_event(monkeypatch)
    actuator = RelaunchPreemptedVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=_ok_launcher)
    result = actuator.relaunch("cefi-fwd-1", launcher="launch-cefi-forward-poll.sh")
    assert result["status"] == "SUCCEEDED"


def test_preempted_relaunch_no_launcher_emits_critical_no_relaunch(tmp_path: Path, monkeypatch):
    """A preempted VM with no resolvable launcher binding must NOT vanish
    silently — it self-emits the belt-and-braces CRITICAL alert (Fix 2)."""
    emitted = _patch_log_event(monkeypatch)
    launched: list[str] = []
    actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=lambda n, *, env: launched.append(n) or _ok_launcher(n, env=env),
    )
    result = actuator.relaunch("mystery-vm", launcher="")
    assert result["status"] == "SKIPPED"
    assert result["reason"] == "no_launcher_binding"
    assert launched == []
    assert any(e[0] == "DP_VM_PREEMPTED_NO_RELAUNCH" and e[1] == "CRITICAL" for e in emitted)


def test_preempted_relaunch_guard_refusal_emits_critical_no_relaunch(tmp_path: Path, monkeypatch):
    """A non-zero exit from the launcher subprocess — e.g. its OWN
    tardis_concurrency_guard refusing because a live/forward VM holds the
    single Tardis slot — is a FAILED relaunch, not a silent one."""
    emitted = _patch_log_event(monkeypatch)

    def refusing_launcher(_name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["bash", _name], returncode=1, stdout="", stderr="ERROR: Tardis concurrent-VM cap would be exceeded"
        )

    actuator = RelaunchPreemptedVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=refusing_launcher)
    result = actuator.relaunch("cefi-queue-heavy-x-1", launcher="launch-cefi-sharded-backfill.sh")
    assert result["status"] == "FAILED"
    assert result["returncode"] == 1
    assert any(e[0] == "DP_VM_PREEMPTED_NO_RELAUNCH" and e[1] == "CRITICAL" for e in emitted)


def test_preempted_relaunch_launcher_exception_emits_critical_no_relaunch(tmp_path: Path, monkeypatch):
    emitted = _patch_log_event(monkeypatch)

    def boom_launcher(_name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        raise RuntimeError("subprocess exec failed")

    actuator = RelaunchPreemptedVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=boom_launcher)
    result = actuator.relaunch("cefi-queue-heavy-x-1", launcher="launch-cefi-sharded-backfill.sh")
    assert result["status"] == "FAILED"
    assert any(e[0] == "DP_VM_PREEMPTED_NO_RELAUNCH" and e[1] == "CRITICAL" for e in emitted)


def test_preempted_relaunch_generous_budget_then_pages(tmp_path: Path, monkeypatch):
    """The preemption budget is deliberately MUCH higher than OOM's ≤2/day — a
    SPOT VM can legitimately preempt hourly under the operator's cap-1 regime,
    so a low budget would defeat the whole point of this actuator by mid-
    afternoon. Use a tiny max_per_day here to exercise the page path quickly."""
    emitted = _patch_log_event(monkeypatch)
    actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path, max_per_day=2, now=lambda: _FIXED_NOW, run_launcher=_ok_launcher
    )
    actuator.relaunch("cefi-a", launcher="l.sh")
    actuator.relaunch("cefi-b", launcher="l.sh")
    third = actuator.relaunch("cefi-c", launcher="l.sh")  # same vm-prefix "cefi" → budget spent
    assert third["status"] == "PAGE"
    assert third["reason"] == "budget_exceeded"
    assert any(
        e[0] == "DP_VM_PREEMPTED_NO_RELAUNCH" and e[1] == "CRITICAL" and e[2].get("relaunch_budget_exceeded")
        for e in emitted
    )


def test_preempted_relaunch_dry_run_does_not_execute(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    launched: list[str] = []
    actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=lambda n, *, env: launched.append(n) or _ok_launcher(n, env=env),
    )
    result = actuator.relaunch("cefi-queue-heavy-x-1", launcher="launch-cefi-sharded-backfill.sh", dry_run=True)
    assert result["status"] == "DRY_RUN"
    assert launched == []


# ── progress-checkpoint resume (the SPOT day-one-replay durable fix) ─────────


def test_preempted_relaunch_resumes_from_monotonic_checkpoint(tmp_path: Path, monkeypatch):
    """A monotonic PROGRESS checkpoint overrides START_DATE to the last completed
    date, so the relaunch resumes from the frontier instead of replaying the
    original START_DATE from genesis. SSOT: spot-vms-for-backfill.md."""
    emitted = _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append((name, dict(env)))
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    actuator = RelaunchPreemptedVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=launcher)
    result = actuator.relaunch(
        "cefi-bf-x",
        launcher="launch-cefi-sharded-backfill.sh",
        launch_env={"VENUES": "BINANCE-FUTURES", "START_DATE": "2019-01-01"},
        checkpoint={"last_completed_date": "2026-02-15", "monotonic": "true"},
    )
    assert result["status"] == "SUCCEEDED"
    # START_DATE overridden to the checkpoint frontier (NOT the original genesis);
    # the rest of the captured scope is preserved.
    assert launched[0][1]["START_DATE"] == "2026-02-15"
    assert launched[0][1]["VENUES"] == "BINANCE-FUTURES"
    assert any(
        e[0] == "DP_VM_PREEMPTED" and e[1] == "INFO" and e[2].get("resume_from_checkpoint") == "2026-02-15"
        for e in emitted
    )


def test_preempted_relaunch_force_run_with_checkpoint_auto_resumes(tmp_path: Path, monkeypatch):
    """A --force / redo_all run (VM_FORCE=true) that left a monotonic checkpoint
    RESUMES from the frontier — force no longer dead-ends at PAGE. This is the
    core "resume properly" fix: force disables presence-skip, so without the
    checkpoint a replay would restart at day one forever."""
    _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append((name, dict(env)))
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    actuator = RelaunchPreemptedVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=launcher)
    result = actuator.relaunch(
        "sports-bf-fixtures",
        launcher="launch-features-sports-backfill-vm.sh",
        launch_env={"VM_FORCE": "true", "START_DATE": "2019-01-01"},
        checkpoint={"last_completed_date": "2024-06-30", "monotonic": "true"},
    )
    assert result["status"] == "SUCCEEDED"
    assert launched[0][1]["START_DATE"] == "2024-06-30"  # resumed, not replayed from genesis


def test_preempted_relaunch_force_run_no_checkpoint_pages(tmp_path: Path, monkeypatch):
    """A --force run with NO checkpoint cannot be safely replayed (day-one loop)
    nor safely skipped — it PAGEs loudly. (This guard only fires once VM_FORCE is
    persisted into launch_env — which the launcher rollout does.)"""
    emitted = _patch_log_event(monkeypatch)
    launched: list[str] = []
    actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=lambda n, *, env: launched.append(n) or _ok_launcher(n, env=env),
    )
    result = actuator.relaunch(
        "sports-bf-fixtures",
        launcher="launch-features-sports-backfill-vm.sh",
        launch_env={"VM_FORCE": "true", "START_DATE": "2019-01-01"},
    )
    assert result["status"] == "PAGE"
    assert result["reason"] == "force_run_not_replayable"
    assert launched == []
    assert any(
        e[0] == "DP_VM_PREEMPTED_NO_RELAUNCH" and e[1] == "CRITICAL" and e[2].get("force_run_not_replayable")
        for e in emitted
    )


def test_preempted_relaunch_force_run_non_monotonic_checkpoint_pages(tmp_path: Path, monkeypatch):
    """A non-monotonic checkpoint (dates recorded out of order — venue-outer
    iteration) is NOT a safe skip-ahead point: undone dates sit behind the max.
    A --force run with such a checkpoint PAGEs rather than risk dropping them."""
    emitted = _patch_log_event(monkeypatch)
    launched: list[str] = []
    actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=lambda n, *, env: launched.append(n) or _ok_launcher(n, env=env),
    )
    result = actuator.relaunch(
        "mtds-bf-multivenue",
        launcher="launch-mtds-backfill-vm.sh",
        launch_env={"VM_FORCE": "true", "START_DATE": "2019-01-01"},
        checkpoint={"last_completed_date": "2024-06-30", "monotonic": "false"},
    )
    assert result["status"] == "PAGE"
    assert result["reason"] == "force_run_not_replayable"
    assert launched == []
    assert any(e[0] == "DP_VM_PREEMPTED_NO_RELAUNCH" and e[1] == "CRITICAL" for e in emitted)


def test_preempted_relaunch_non_force_no_checkpoint_replays_verbatim(tmp_path: Path, monkeypatch):
    """A normal (non-force) run with no checkpoint keeps today's behavior — a
    verbatim replay of the captured launch env (presence-skip resumes it). No
    START_DATE override, no PAGE."""
    _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append((name, dict(env)))
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    actuator = RelaunchPreemptedVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=launcher)
    result = actuator.relaunch(
        "cefi-bf-x",
        launcher="launch-cefi-sharded-backfill.sh",
        launch_env={"START_DATE": "2026-02-01", "VENUES": "BYBIT"},
    )
    assert result["status"] == "SUCCEEDED"
    assert launched[0][1]["START_DATE"] == "2026-02-01"  # unchanged — verbatim replay


def test_preempted_relaunch_budget_separate_from_oom_budget(tmp_path: Path, monkeypatch):
    """The preemption actuator's budget dir is separate from the OOM actuator's
    — exhausting one must never affect the other (different failure classes,
    different cadence)."""
    _patch_log_event(monkeypatch)
    oom_actuator = RelaunchBackfillVm(budget_dir=tmp_path / "oom", now=lambda: _FIXED_NOW, run_launcher=_ok_launcher)
    preempt_actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path / "preempt", now=lambda: _FIXED_NOW, run_launcher=_ok_launcher
    )
    oom_actuator.relaunch("cefi-a", exit_code=137, launcher="l.sh")
    oom_actuator.relaunch("cefi-b", exit_code=137, launcher="l.sh")  # OOM budget (2/day) now spent for "cefi"
    # The preemption actuator for the SAME vm-prefix "cefi" is unaffected.
    result = preempt_actuator.relaunch("cefi-c", launcher="l.sh")
    assert result["status"] == "SUCCEEDED"


def test_backfill_relaunch_non_oom_skipped(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    launched: list[str] = []
    actuator = RelaunchBackfillVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=lambda n, *, env: launched.append(n) or _ok_launcher(n, env=env),
    )
    result = actuator.relaunch("cefi-bf", exit_code=1, launcher="l.sh")
    assert result["status"] == "SKIPPED"
    assert result["reason"] == "not_oom"
    assert launched == []  # no relaunch for a non-OOM crash


def test_vm_prefix_keying():
    assert vm_prefix("sports-full-sweep-2025") == "sports"
    assert vm_prefix("nodash") == "nodash"


# ── escalation route_finding wiring ─────────────────────────────────────────


def test_route_auto_recover_invokes_consolidator_actuator(tmp_path: Path, monkeypatch):
    emitted = _patch_log_event(monkeypatch)
    runs: list[str] = []

    def fake_actuator_class(*_a, **_k):  # noqa: ANN002, ANN003
        return RelaunchConsolidator(
            cooldown_dir=tmp_path,
            project_id="p",
            now=lambda: _FIXED_NOW,
            run_job=lambda j, **_: runs.append(j) or "exec",
        )

    monkeypatch.setattr("scripts.recovery.relaunch_consolidator.RelaunchConsolidator", fake_actuator_class)
    finding = PipelineFinding(
        event="CONSOLIDATOR_DOWN",
        severity="CRITICAL",
        tier=EscalationTier.AUTO_RECOVER,
        summary="consolidator down",
        details={"asset_group": "defi"},
        registry_id="DP-MANIFEST-001",
    )
    result = escalation.route_finding(finding)
    assert result["effective_tier"] == "auto_recover"  # recovered → stays auto_recover
    assert result["recovery"]["recovered"] is True
    assert runs == ["manifest-consolidator-defi"]
    # The CONSOLIDATOR_DOWN event is still emitted.
    assert any(e[0] == "CONSOLIDATOR_DOWN" for e in emitted)


def test_route_auto_recover_no_actuator_falls_through_to_file_issue(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)
    finding = PipelineFinding(
        event="DP_NULL_EMPTY_DOUBLE_COUNT",  # auto_recover in registry, no wired actuator
        severity="WARN",
        tier=EscalationTier.AUTO_RECOVER,
        summary="dedup double count",
        details={"asset_group": "cefi"},
        registry_id="DP-ORDER-003",
    )
    result = escalation.route_finding(finding, pm_repo_path=str(pm))
    assert result["effective_tier"] == "file_issue"  # no actuator → file_issue
    assert result["issue_path"] is not None  # the issue doc was written


def test_route_auto_recover_failed_actuator_falls_through(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)

    def failing_class(*_a, **_k):  # noqa: ANN002, ANN003
        def _boom(j, **_):  # noqa: ANN001, ANN002, ANN003
            raise RuntimeError("cloud run unreachable")

        return RelaunchConsolidator(cooldown_dir=tmp_path, project_id="p", now=lambda: _FIXED_NOW, run_job=_boom)

    monkeypatch.setattr("scripts.recovery.relaunch_consolidator.RelaunchConsolidator", failing_class)
    finding = PipelineFinding(
        event="CONSOLIDATOR_DOWN",
        severity="CRITICAL",
        tier=EscalationTier.AUTO_RECOVER,
        summary="consolidator down",
        details={"asset_group": "defi"},
        registry_id="DP-MANIFEST-001",
    )
    result = escalation.route_finding(finding, pm_repo_path=str(pm))
    assert result["recovery"]["recovered"] is False
    assert result["effective_tier"] == "file_issue"
    assert result["issue_path"] is not None


def test_route_auto_recover_actuators_unavailable_falls_through(tmp_path: Path, monkeypatch):
    """Incident 2026-06-23: in a packaged runtime (the deployment-api Cloud Run
    image installs the wheel then drops ``scripts/``), the actuators are not
    importable. The module must still LOAD and the auto_recover tier must degrade
    to file_issue — NEVER crash the monitor at import or at dispatch."""
    _patch_log_event(monkeypatch)
    monkeypatch.setattr(escalation, "_ACTUATORS_AVAILABLE", False)
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)
    finding = PipelineFinding(
        event="CONSOLIDATOR_DOWN",
        severity="CRITICAL",
        tier=EscalationTier.AUTO_RECOVER,
        summary="consolidator down (actuators absent from runtime)",
        details={"asset_group": "defi"},
        registry_id="DP-MANIFEST-001",
    )
    result = escalation.route_finding(finding, pm_repo_path=str(pm))
    assert result["recovery"]["recovered"] is False
    assert result["recovery"]["result"]["status"] == "UNAVAILABLE"
    assert result["effective_tier"] == "file_issue"  # degraded, not crashed
    assert result["issue_path"] is not None


def test_actuator_unavailable_dispatches_worker_even_without_pm_clone(monkeypatch):
    """The escalate-to-orchestrator relaunch hand-off (operator decision 2026-06-23).

    When a WIRED auto_recover actuator cannot actuate in the Cloud Run monitor image
    (``_ACTUATORS_AVAILABLE`` False) AND there is NO PM clone on disk, ``route_finding``
    MUST still fire the dispatch so a planning-VM worker relaunches — the relaunch is
    never stranded on the image-bound monitor.
    """
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: None,
    )
    monkeypatch.setattr(escalation, "_ACTUATORS_AVAILABLE", False)
    dispatched: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        escalation,
        "_dispatch_to_orchestrator",
        lambda f, _p: dispatched.append((f.event, dict(f.details))) or {"dispatched": True, "reason": "http_204"},
    )
    finding = PipelineFinding(
        event="DP_VM_STALL",
        severity="WARN",
        tier=EscalationTier.AUTO_RECOVER,
        summary="vm stalled",
        details={
            "vm_name": "uts-prod-tradfi-bf-cme-x",
            "relaunch_launcher": "launch-tradfi-bf-cme.sh",
            "asset_group": "tradfi",
            "deployment_id": "dep-123",
        },
        registry_id="DP-VM-003",
    )
    # pm_repo_path points at a non-existent dir → no PM clone (the Cloud Run case).
    result = escalation.route_finding(finding, pm_repo_path="/nonexistent/pm/path")
    assert result["effective_tier"] == "file_issue"
    assert result["recovery"]["result"]["status"] == "UNAVAILABLE"
    assert len(dispatched) == 1, "degraded actuator must dispatch a worker even with no PM clone"
    assert dispatched[0][0] == "DP_VM_STALL"


def test_dispatch_payload_carries_relaunch_binding(monkeypatch):
    """The dispatch ``client_payload`` carries the STRUCTURED relaunch binding so the
    worker relaunches from the registries, not by parsing the context text."""

    class _FakeSecretClient:
        def get_secret(self, _name: str) -> str:
            return "ghp_faketoken"

    monkeypatch.setattr(escalation, "get_secret_client", lambda: _FakeSecretClient())
    captured: dict[str, object] = {}

    class _Resp:
        status = 204

        def close(self) -> None:
            return None

    def _fake_urlopen(req, timeout=15):  # noqa: ANN001, ANN202, ARG001
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return _Resp()

    monkeypatch.setattr(escalation.urllib.request, "urlopen", _fake_urlopen)
    finding = PipelineFinding(
        event="DP_VM_STALL",
        severity="WARN",
        tier=EscalationTier.AUTO_RECOVER,
        summary="vm stalled",
        details={
            "vm_name": "vm-x",
            "relaunch_launcher": "launch-x.sh",
            "deployment_id": "dep-1",
            "asset_group": "tradfi",
        },
        registry_id="DP-VM-003",
    )
    out = escalation._dispatch_to_orchestrator(finding, None)
    assert out["dispatched"] is True
    client_payload = captured["body"]["client_payload"]  # type: ignore[index]
    assert client_payload["action"] == "relaunch_vm"
    assert client_payload["vm_name"] == "vm-x"
    assert client_payload["relaunch_launcher"] == "launch-x.sh"
    assert client_payload["deployment_id"] == "dep-1"
    assert client_payload["asset_group"] == "tradfi"
    assert "rb_infra_relaunch.md" in client_payload["context"]


def test_route_auto_recover_oom_relaunch_via_finding(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    launched: list[str] = []

    def launcher_class(*_a, **_k):  # noqa: ANN002, ANN003
        return RelaunchBackfillVm(
            budget_dir=tmp_path,
            now=lambda: _FIXED_NOW,
            run_launcher=lambda n, *, env: launched.append(n) or _ok_launcher(n, env=env),
        )

    monkeypatch.setattr("scripts.recovery.relaunch_backfill_vm.RelaunchBackfillVm", launcher_class)
    finding = PipelineFinding(
        event="DP_VM_EXIT_NONZERO",
        severity="CRITICAL",
        tier=EscalationTier.AUTO_RECOVER,
        summary="vm OOM",
        details={"vm_name": "sports-bf-1", "exit_code": 137, "oom": True, "relaunch_launcher": "launch-x.sh"},
        registry_id="DP-VM-001",
    )
    result = escalation.route_finding(finding)
    assert result["effective_tier"] == "auto_recover"
    assert result["recovery"]["recovered"] is True
    assert launched == ["launch-x.sh"]


def test_route_auto_recover_preempted_relaunch_via_finding(tmp_path: Path, monkeypatch):
    """DP_VM_PREEMPTED is wired to relaunch_preempted_vm — a successful relaunch
    replays details["launch_env"] verbatim and stays auto_recover (no page)."""
    _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def preempted_class(*_a, **_k):  # noqa: ANN002, ANN003
        return RelaunchPreemptedVm(
            budget_dir=tmp_path,
            now=lambda: _FIXED_NOW,
            run_launcher=lambda n, *, env: (
                launched.append((n, dict(env)))
                or subprocess.CompletedProcess(args=["bash", n], returncode=0, stdout="ok", stderr="")
            ),
        )

    monkeypatch.setattr("scripts.recovery.relaunch_backfill_vm.RelaunchPreemptedVm", preempted_class)
    launch_env = {"VENUES": "BINANCE-FUTURES", "START_DATE": "2026-02-01"}
    finding = PipelineFinding(
        event="DP_VM_PREEMPTED",
        severity="INFO",
        tier=EscalationTier.AUTO_RECOVER,
        summary="vm preempted",
        details={
            "vm_name": "cefi-queue-heavy-x-1",
            "relaunch_launcher": "launch-cefi-sharded-backfill.sh",
            "asset_group": "cefi",
            "launch_env": launch_env,
        },
        registry_id="DP-VM-007",
    )
    result = escalation.route_finding(finding)
    assert result["effective_tier"] == "auto_recover"
    assert result["recovery"]["recovered"] is True
    assert launched == [("launch-cefi-sharded-backfill.sh", launch_env)]


def test_route_auto_recover_preempted_relaunch_failure_falls_through(tmp_path: Path, monkeypatch):
    """A FAILED preemption relaunch (e.g. the launcher's own concurrency guard
    refusing) falls through to file_issue — the belt-and-braces alert path."""
    _patch_log_event(monkeypatch)

    def preempted_class(*_a, **_k):  # noqa: ANN002, ANN003
        return RelaunchPreemptedVm(
            budget_dir=tmp_path,
            now=lambda: _FIXED_NOW,
            run_launcher=lambda n, *, env: subprocess.CompletedProcess(
                args=["bash", n], returncode=1, stdout="", stderr="guard refused"
            ),
        )

    monkeypatch.setattr("scripts.recovery.relaunch_backfill_vm.RelaunchPreemptedVm", preempted_class)
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)
    finding = PipelineFinding(
        event="DP_VM_PREEMPTED",
        severity="INFO",
        tier=EscalationTier.AUTO_RECOVER,
        summary="vm preempted",
        details={
            "vm_name": "cefi-queue-heavy-x-1",
            "relaunch_launcher": "launch-cefi-sharded-backfill.sh",
            "asset_group": "cefi",
        },
        registry_id="DP-VM-007",
    )
    result = escalation.route_finding(finding, pm_repo_path=str(pm))
    assert result["recovery"]["recovered"] is False
    assert result["effective_tier"] == "file_issue"
    assert result["issue_path"] is not None


def test_preempted_relaunch_refuses_to_replay_a_force_run(tmp_path: Path, monkeypatch):
    """A --force/redo_all run must NOT be replayed — it would restart at day one forever.

    Regression pin (codified 2026-07-18, codex/05-infrastructure/spot-vms-for-backfill.md
    "Preemption recovery MUST resume from PROGRESS, never replay START_DATE"): this
    actuator replays the ORIGINAL VM_START_DATE, which is correct only because
    presence-skip absorbs the redo. --force DISABLES that skip, so replaying restarts
    the backfill from the beginning on EVERY preemption — burning quota, never
    converging. Measured: ~54 days/hour over a 2,390-day range, preempted after ~10min.
    PAGE loudly instead of looping silently.
    """
    emitted = _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append((name, dict(env)))
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    actuator = RelaunchPreemptedVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=launcher)
    result = actuator.relaunch(
        "af-backfill-20260718-150353",
        launcher="launch-api-football-backfill-vm.sh",
        asset_group="sports",
        launch_env={"VM_FORCE": "true", "VM_START_DATE": "2019-01-01", "VM_END_DATE": "2026-07-17"},
    )

    assert result["status"] == "PAGE"
    assert result["reason"] == "force_run_not_replayable"
    # The launcher must NOT have been invoked — a silent infinite restart is the bug.
    assert launched == []
    assert any(
        e[0] == "DP_VM_PREEMPTED_NO_RELAUNCH" and e[1] == "CRITICAL" and e[2].get("force_run_not_replayable")
        for e in emitted
    )


def test_preempted_relaunch_still_replays_a_non_force_run(tmp_path: Path, monkeypatch):
    """The force guard must not regress the normal (skip-enabled) resume path."""
    _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append((name, dict(env)))
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    actuator = RelaunchPreemptedVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=launcher)
    env = {"VM_FORCE": "false", "VM_START_DATE": "2026-02-01"}
    result = actuator.relaunch(
        "cefi-queue-heavy-binancefutu-x15-20260716-075338",
        launcher="launch-cefi-sharded-backfill.sh",
        asset_group="cefi",
        launch_env=env,
    )
    assert result["status"] == "SUCCEEDED"
    assert launched == [("launch-cefi-sharded-backfill.sh", env)]


def test_preempted_relaunch_drops_stale_rate_budget(tmp_path: Path, monkeypatch):
    """Rate-budget keys must NOT be replayed — they are launch-time-derived.

    A VM preempted while ALONE carries a full-key budget; replaying that into a now
    crowded fleet oversubscribes the shared key (measured 2026-07-18: 5 concurrent
    api-football VMs each holding a full share => 61 rateLimit FALSE failures in 30min).
    Dropping them forces the launcher to RE-DERIVE from the measured running fleet.
    """
    _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        launched.append((name, dict(env)))
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    actuator = RelaunchPreemptedVm(budget_dir=tmp_path, now=lambda: _FIXED_NOW, run_launcher=launcher)
    result = actuator.relaunch(
        "af-backfill-20260718-150353",
        launcher="launch-api-football-backfill-vm.sh",
        asset_group="sports",
        launch_env={
            "VM_START_DATE": "2019-01-01",
            "SPORTS_ADAPTER_RATE_RPM": "1200",
            "SPORTS_ADAPTER_CONCURRENCY": "16",
            "FLEET_VMS": "1",
            "REMAINING_DAILY_QUOTA": "450000",
        },
    )
    assert result["status"] == "SUCCEEDED"
    replayed = launched[0][1]
    for stale in ("SPORTS_ADAPTER_RATE_RPM", "SPORTS_ADAPTER_CONCURRENCY", "FLEET_VMS", "REMAINING_DAILY_QUOTA"):
        assert stale not in replayed, f"{stale} must be re-derived on replay, not replayed"
    # Non-rate params still replay verbatim.
    assert replayed["VM_START_DATE"] == "2019-01-01"


# ── Code-tarball pin re-resolution on relaunch (2026-07-20 outage) ──────────
#
# The retention sweep reaped a pinned `@sha.tar.gz` out from under a running
# migration fleet, so every relaunch died at setup ("refusing floating fallback")
# and self-deleted. Retention is now pin-aware, but a relaunch must ALSO survive
# a pin that was already reaped — loudly, never by degrading to floating.


def _pin_launcher(sink: list[tuple[str, dict[str, str]]]):
    def launcher(name: str, *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        sink.append((name, dict(env)))
        return subprocess.CompletedProcess(args=["bash", name], returncode=0, stdout="ok", stderr="")

    return launcher


def test_preempted_relaunch_repins_missing_tarball_and_logs_loudly(tmp_path: Path, monkeypatch):
    """A reaped pin is re-pinned to the newest available SHA + a CRITICAL audit line."""
    emitted = _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=_pin_launcher(launched),
        resolve_pin=lambda tarball, requested: "newsha456" if requested == "deadsha123" else requested,
    )
    result = actuator.relaunch(
        "canonical-migration-cefi-1",
        launcher="launch-canonical-migration-vm.sh",
        launch_env={"UAC_TARBALL_SHA": "deadsha123", "START_DATE": "2019-01-01"},
    )

    assert result["status"] == "SUCCEEDED"
    # The launcher receives the RE-PINNED sha — never an unset/empty value, which
    # every `[[ -n ... ]]` guard would read as a floating pull.
    assert launched[0][1]["UAC_TARBALL_SHA"] == "newsha456"
    repin_events = [e for e in emitted if e[0] == "DP_VM_TARBALL_REPINNED"]
    assert len(repin_events) == 1, "the code-identity change must be announced exactly once"
    assert repin_events[0][1] == "CRITICAL", "an unannounced code-identity swap is the failure mode"
    assert repin_events[0][2]["old_sha"] == "deadsha123"
    assert repin_events[0][2]["new_sha"] == "newsha456"


def test_preempted_relaunch_intact_pin_is_untouched_and_silent(tmp_path: Path, monkeypatch):
    """The common case: pin resolves, nothing is re-pinned, no audit noise."""
    emitted = _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=_pin_launcher(launched),
        resolve_pin=lambda tarball, requested: requested,
    )
    result = actuator.relaunch(
        "canonical-migration-cefi-2",
        launcher="launch-canonical-migration-vm.sh",
        launch_env={"UAC_TARBALL_SHA": "livesha", "UTL_TARBALL_SHA": "utlsha"},
    )

    assert result["status"] == "SUCCEEDED"
    assert launched[0][1]["UAC_TARBALL_SHA"] == "livesha"
    assert launched[0][1]["UTL_TARBALL_SHA"] == "utlsha"
    assert not [e for e in emitted if e[0] == "DP_VM_TARBALL_REPINNED"]


def test_preempted_relaunch_pages_when_no_pin_resolves_never_floats(tmp_path: Path, monkeypatch):
    """No surviving pinned pair => PAGE. Degrading to the floating tarball is banned."""
    emitted = _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=_pin_launcher(launched),
        resolve_pin=lambda tarball, requested: None,
    )
    result = actuator.relaunch(
        "canonical-migration-cefi-3",
        launcher="launch-canonical-migration-vm.sh",
        launch_env={"UAC_TARBALL_SHA": "deadsha123"},
    )

    assert result["status"] == "PAGE"
    assert result["reason"] == "tarball_pin_unresolvable"
    assert not launched, "must not launch onto un-asserted floating code"
    assert any(
        e[0] == "DP_VM_PREEMPTED_NO_RELAUNCH" and e[1] == "CRITICAL" and "UAC_TARBALL_SHA" in str(e[2]) for e in emitted
    )


def test_preempted_relaunch_without_pins_never_calls_the_resolver(tmp_path: Path, monkeypatch):
    """Unpinned fleets keep their existing behaviour — no new GCS dependency."""
    _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def boom(tarball: str, requested: str) -> str | None:
        raise AssertionError("resolver must not run when no pin is recorded")

    actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=_pin_launcher(launched),
        resolve_pin=boom,
    )
    result = actuator.relaunch(
        "cefi-bf-unpinned",
        launcher="launch-cefi-sharded-backfill.sh",
        launch_env={"VENUES": "BINANCE-FUTURES"},
    )
    assert result["status"] == "SUCCEEDED"


# ── The re-pin actuator reads pins from the AUTHORITATIVE REGISTRY ───────────
# The v1 actuator re-resolved pins out of `launch_env` (LAUNCH_PARAMS.json). No
# pinning launcher writes that blob, so the loop body never executed once in
# production: it looked implemented and did nothing. These tests drive the
# registry path explicitly, and the first one FAILS on the v1 code.


def test_preempted_relaunch_reads_pins_from_the_durable_registry(tmp_path: Path, monkeypatch):
    """launch_env carries NO pins — exactly the production shape — yet the pin is found."""
    emitted = _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=_pin_launcher(launched),
        resolve_pin=lambda tarball, requested: "newsha456" if requested == "deadsha123" else requested,
        # what collect_pins_for_vm() returns for this VM from TARBALL_PINS.json
        load_vm_pins=lambda vm: {"UAC_TARBALL_SHA": "deadsha123"},
    )
    result = actuator.relaunch(
        "canonical-migration-cefi-9",
        launcher="launch-canonical-migration-vm.sh",
        # NOTE: no *_TARBALL_SHA here. This is what LAUNCH_PARAMS.json really looks like.
        launch_env={"START_DATE": "2019-01-01"},
    )

    assert result["status"] == "SUCCEEDED"
    assert launched[0][1]["UAC_TARBALL_SHA"] == "newsha456", (
        "v1 regression: with no pin in launch_env the actuator re-resolved nothing at all"
    )
    repins = [e for e in emitted if e[0] == "DP_VM_TARBALL_REPINNED"]
    assert len(repins) == 1 and repins[0][1] == "CRITICAL"


def test_launch_env_pin_wins_over_the_registry(tmp_path: Path, monkeypatch):
    """The captured launch env is the more specific record where it has a value."""
    _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=_pin_launcher(launched),
        resolve_pin=lambda tarball, requested: requested,
        load_vm_pins=lambda vm: {"UAC_TARBALL_SHA": "from-registry"},
    )
    result = actuator.relaunch(
        "canonical-migration-cefi-10",
        launcher="launch-canonical-migration-vm.sh",
        launch_env={"UAC_TARBALL_SHA": "from-launch-env"},
    )

    assert result["status"] == "SUCCEEDED"
    assert launched[0][1]["UAC_TARBALL_SHA"] == "from-launch-env"


def test_registry_read_failure_degrades_to_launch_env_never_raises(tmp_path: Path, monkeypatch):
    """A registry blip must not turn a recoverable preemption into a crash."""
    _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def boom(vm: str) -> dict[str, str]:
        raise RuntimeError("GCS 503")

    actuator = RelaunchPreemptedVm(
        budget_dir=tmp_path,
        now=lambda: _FIXED_NOW,
        run_launcher=_pin_launcher(launched),
        resolve_pin=lambda tarball, requested: requested,
        load_vm_pins=boom,
    )
    result = actuator.relaunch(
        "canonical-migration-cefi-11",
        launcher="launch-canonical-migration-vm.sh",
        launch_env={"UAC_TARBALL_SHA": "livesha"},
    )

    assert result["status"] == "SUCCEEDED"
    assert launched[0][1]["UAC_TARBALL_SHA"] == "livesha"


# ── route_finding wiring: DP_VM_STALL threads launch_env/checkpoint ─────────


def test_route_auto_recover_stalled_relaunch_resumes_from_checkpoint(tmp_path: Path, monkeypatch):
    """DP_VM_STALL is wired to relaunch_stalled_vm — a finding carrying
    launch_env + progress_checkpoint resumes from the checkpoint frontier
    (operator ask 2026-07-27), mirroring the PREEMPTED wiring test above."""
    _patch_log_event(monkeypatch)
    launched: list[tuple[str, dict[str, str]]] = []

    def stalled_class(*_a, **_k):  # noqa: ANN002, ANN003
        return RelaunchStalledVm(
            budget_dir=tmp_path,
            now=lambda: _FIXED_NOW,
            run_launcher=lambda n, *, env: (
                launched.append((n, dict(env)))
                or subprocess.CompletedProcess(args=["bash", n], returncode=0, stdout="ok", stderr="")
            ),
        )

    monkeypatch.setattr("scripts.recovery.relaunch_stalled_vm.RelaunchStalledVm", stalled_class)
    finding = PipelineFinding(
        event="DP_VM_STALL",
        severity="WARN",
        tier=EscalationTier.AUTO_RECOVER,
        summary="vm stalled",
        details={
            "vm_name": "tradfi-bf-cme-1",
            "relaunch_launcher": "launch-tradfi-bf-cme.sh",
            "asset_group": "tradfi",
            "launch_env": {"START_DATE": "2019-01-01"},
            "progress_checkpoint": {"last_completed_date": "2026-05-01", "monotonic": "true"},
        },
        registry_id="DP-VM-003",
    )
    result = escalation.route_finding(finding)
    assert result["effective_tier"] == "auto_recover"
    assert result["recovery"]["recovered"] is True
    assert launched == [("launch-tradfi-bf-cme.sh", {"START_DATE": "2026-05-01"})]


# ── route_finding: OOM successful relaunch also files an investigate doc ────
# (operator ask 2026-07-27: "file an issue doc to investigate the oom when a
# human wakes up" — even when the escalated-memory relaunch itself succeeds.)


def test_oom_relaunch_with_bigger_machine_files_investigate_issue_doc(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)

    def launcher_class(*_a, **_k):  # noqa: ANN002, ANN003
        return RelaunchBackfillVm(
            budget_dir=tmp_path / "budget",
            now=lambda: _FIXED_NOW,
            run_launcher=lambda n, *, env: _ok_launcher(n, env=env),
        )

    monkeypatch.setattr("scripts.recovery.relaunch_backfill_vm.RelaunchBackfillVm", launcher_class)
    finding = PipelineFinding(
        event="DP_VM_EXIT_NONZERO",
        severity="CRITICAL",
        tier=EscalationTier.AUTO_RECOVER,
        summary="VM cefi-cs7-4d terminated with exit_code=137 (OOM)",
        details={
            "vm_name": "cefi-cs7-4d",
            "exit_code": 137,
            "oom": True,
            "relaunch_launcher": "launch-canonical-migration-vm.sh",
            "asset_group": "cefi",
            "bigger_machine": True,
            "machine_type": "e2-standard-8",
        },
        registry_id="DP-VM-001",
    )
    result = escalation.route_finding(finding, pm_repo_path=str(pm))
    assert result["effective_tier"] == "auto_recover"  # the relaunch itself still succeeded quietly
    assert result["recovery"]["recovered"] is True
    oom_path = result.get("oom_investigate_issue_path")
    assert oom_path is not None, "a successful escalated-memory OOM relaunch must file a human follow-up"
    contents = Path(oom_path).read_text(encoding="utf-8")
    assert "investigate OOM root cause" in contents
    assert "cefi" in contents  # the vm-prefix appears in the title


def test_oom_relaunch_without_bigger_machine_hint_does_not_file_investigate_doc(tmp_path: Path, monkeypatch):
    """A finding with no bigger_machine hint never escalated the machine type, so
    there is nothing new to investigate beyond the ordinary path — no doc."""
    _patch_log_event(monkeypatch)
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)

    def launcher_class(*_a, **_k):  # noqa: ANN002, ANN003
        return RelaunchBackfillVm(
            budget_dir=tmp_path / "budget",
            now=lambda: _FIXED_NOW,
            run_launcher=lambda n, *, env: _ok_launcher(n, env=env),
        )

    monkeypatch.setattr("scripts.recovery.relaunch_backfill_vm.RelaunchBackfillVm", launcher_class)
    finding = PipelineFinding(
        event="DP_VM_EXIT_NONZERO",
        severity="CRITICAL",
        tier=EscalationTier.AUTO_RECOVER,
        summary="VM sports-bf-1 terminated with exit_code=137 (OOM)",
        details={"vm_name": "sports-bf-1", "exit_code": 137, "oom": True, "relaunch_launcher": "launch-x.sh"},
        registry_id="DP-VM-001",
    )
    result = escalation.route_finding(finding, pm_repo_path=str(pm))
    assert result["effective_tier"] == "auto_recover"
    assert result.get("oom_investigate_issue_path") is None
    assert not list((pm / "plans" / "active" / "issues").iterdir()), "no issue doc should have been written"


def test_oom_relaunch_investigate_doc_idempotent_same_day(tmp_path: Path, monkeypatch):
    """A second same-day OOM of the SAME vm-prefix collapses into the same doc
    (via _write_issue_doc's own slug+date dedup) rather than filing a new one
    per relaunch."""
    _patch_log_event(monkeypatch)
    pm = tmp_path / "unified-trading-pm"
    (pm / "plans" / "active" / "issues").mkdir(parents=True)

    def launcher_class(*_a, **_k):  # noqa: ANN002, ANN003
        return RelaunchBackfillVm(
            budget_dir=tmp_path / "budget",
            now=lambda: _FIXED_NOW,
            run_launcher=lambda n, *, env: _ok_launcher(n, env=env),
        )

    monkeypatch.setattr("scripts.recovery.relaunch_backfill_vm.RelaunchBackfillVm", launcher_class)

    def _finding(vm_name: str) -> PipelineFinding:
        return PipelineFinding(
            event="DP_VM_EXIT_NONZERO",
            severity="CRITICAL",
            tier=EscalationTier.AUTO_RECOVER,
            summary=f"VM {vm_name} terminated with exit_code=137 (OOM)",
            details={
                "vm_name": vm_name,
                "exit_code": 137,
                "oom": True,
                "relaunch_launcher": "launch-canonical-migration-vm.sh",
                "bigger_machine": True,
            },
            registry_id="DP-VM-001",
        )

    r1 = escalation.route_finding(_finding("cefi-cs7-4d"), pm_repo_path=str(pm))
    r2 = escalation.route_finding(_finding("cefi-cs8-2f"), pm_repo_path=str(pm))
    assert r1["oom_investigate_issue_path"] == r2["oom_investigate_issue_path"]
    assert len(list((pm / "plans" / "active" / "issues").iterdir())) == 1
