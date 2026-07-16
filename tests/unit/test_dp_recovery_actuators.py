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
