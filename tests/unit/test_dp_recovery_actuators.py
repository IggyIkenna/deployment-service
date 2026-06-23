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

import subprocess
from datetime import UTC, datetime
from pathlib import Path

from deployment_service.data_pipeline_monitors import escalation
from deployment_service.data_pipeline_monitors.escalation import (
    EscalationTier,
    PipelineFinding,
)
from scripts.recovery.relaunch_backfill_vm import RelaunchBackfillVm, vm_prefix
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

    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.RelaunchConsolidator", fake_actuator_class
    )
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

    monkeypatch.setattr("deployment_service.data_pipeline_monitors.escalation.RelaunchConsolidator", failing_class)
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


def test_route_auto_recover_oom_relaunch_via_finding(tmp_path: Path, monkeypatch):
    _patch_log_event(monkeypatch)
    launched: list[str] = []

    def launcher_class(*_a, **_k):  # noqa: ANN002, ANN003
        return RelaunchBackfillVm(
            budget_dir=tmp_path,
            now=lambda: _FIXED_NOW,
            run_launcher=lambda n, *, env: launched.append(n) or _ok_launcher(n, env=env),
        )

    monkeypatch.setattr("deployment_service.data_pipeline_monitors.escalation.RelaunchBackfillVm", launcher_class)
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
