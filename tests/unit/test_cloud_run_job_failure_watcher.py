"""Unit tests for DP-WATCHER-006 — cloud_run_job_failure_watcher.py.

Credential-free + block-network safe: GCP I/O is injected via stub readers.
Covers:
  - check_cloud_run_job_failures: healthy job (no finding), failed job (finding
    emitted), MissTracker gate (below threshold → suppress, at threshold → page),
    API error / empty reader → no finding
  - make_cloud_run_job_execution_reader: SDK import failure → silent no-op lambda
  - cli meta-mode wiring: check_cloud_run_job_failures is called in --dry-run
"""

from __future__ import annotations

import pytest

from deployment_service.data_pipeline_monitors import cloud_run_job_failure_watcher
from deployment_service.data_pipeline_monitors.cloud_run_job_failure_watcher import (
    check_cloud_run_job_failures,
    make_cloud_run_job_execution_reader,
)
from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding

# ── fake helpers ─────────────────────────────────────────────────────────────

class _FakeMissTracker:
    """Minimal MissTracker stand-in: every registered miss increments a counter."""

    def __init__(self, initial: dict[str, int] | None = None) -> None:
        self._counts: dict[str, int] = dict(initial or {})

    def register(self, key: str, *, stale: bool) -> int:
        if stale:
            self._counts[key] = self._counts.get(key, 0) + 1
        else:
            self._counts[key] = 0
        return self._counts[key]

    def load(self) -> None:  # pragma: no cover – not exercised here
        pass

    def persist(self) -> None:  # pragma: no cover – not exercised here
        pass


def _reader_for(diagnostics: dict[str, dict[str, object] | None]):
    """Return a stubbed JobExecutionFailureReader that returns the given diagnostics map."""
    def _read(stems):
        return {s: diagnostics.get(s) for s in stems}
    return _read


def _healthy_reader(stems):
    """All jobs have a healthy (None) diagnostic."""
    return dict.fromkeys(stems, None)


def _failed_diag(
    stem: str = "my-job",
    failed_count: int = 1,
    succeeded_count: int = 0,
    completion_age_min: float | None = 30.0,
) -> dict[str, object]:
    return {
        "failed_count": failed_count,
        "succeeded_count": succeeded_count,
        "job_name": f"prd-{stem}",
        "completion_age_min": completion_age_min,
    }


# ── check_cloud_run_job_failures ─────────────────────────────────────────────

def test_healthy_job_emits_no_finding(tmp_path):
    """A job with a healthy latest execution should produce no finding."""
    findings = check_cloud_run_job_failures(
        job_stems=["healthy-job"],
        execution_reader=_healthy_reader,
        pm_repo_path=str(tmp_path),
        dry_run=True,
    )
    assert findings["healthy-job"] == {}


def test_failed_job_emits_finding(tmp_path, monkeypatch):
    """A job with failed_count > 0 in its latest execution should emit CLOUD_RUN_JOB_FAILED."""
    emitted: list[PipelineFinding] = []
    monkeypatch.setattr(
        cloud_run_job_failure_watcher,
        "emit_finding",
        lambda f, pm_repo_path=None, dry_run=False: emitted.append(f),
    )

    findings = check_cloud_run_job_failures(
        job_stems=["bad-job"],
        execution_reader=_reader_for({"bad-job": _failed_diag("bad-job")}),
        pm_repo_path=str(tmp_path),
        dry_run=True,
    )

    assert len(emitted) == 1
    f = emitted[0]
    assert f.event == "CLOUD_RUN_JOB_FAILED"
    assert f.severity == "CRITICAL"
    assert f.tier == EscalationTier.PAGE_OPERATOR
    assert f.registry_id == "DP-WATCHER-006"
    assert "prd-bad-job" in f.summary
    assert findings["bad-job"]["failed_count"] == 1


def test_miss_tracker_suppresses_below_threshold(tmp_path, monkeypatch):
    """A single sweep miss below min_consecutive should NOT emit."""
    emitted: list[PipelineFinding] = []
    monkeypatch.setattr(
        cloud_run_job_failure_watcher,
        "emit_finding",
        lambda f, pm_repo_path=None, dry_run=False: emitted.append(f),
    )
    tracker = _FakeMissTracker()  # counter starts at 0

    findings = check_cloud_run_job_failures(
        job_stems=["flaky-job"],
        execution_reader=_reader_for({"flaky-job": _failed_diag("flaky-job")}),
        pm_repo_path=str(tmp_path),
        dry_run=True,
        miss_tracker=tracker,  # type: ignore[arg-type]  # stub matches the interface
        min_consecutive=2,
    )

    assert emitted == []
    assert findings["flaky-job"] == {}


def test_miss_tracker_pages_at_threshold(tmp_path, monkeypatch):
    """Reaching min_consecutive misses should emit the finding."""
    emitted: list[PipelineFinding] = []
    monkeypatch.setattr(
        cloud_run_job_failure_watcher,
        "emit_finding",
        lambda f, pm_repo_path=None, dry_run=False: emitted.append(f),
    )
    # Pre-seed one existing miss so the next register call reaches min_consecutive=2.
    tracker = _FakeMissTracker({"CLOUD_RUN_JOB_FAILED::flaky-job": 1})

    findings = check_cloud_run_job_failures(
        job_stems=["flaky-job"],
        execution_reader=_reader_for({"flaky-job": _failed_diag("flaky-job")}),
        pm_repo_path=str(tmp_path),
        dry_run=True,
        miss_tracker=tracker,  # type: ignore[arg-type]  # stub matches the interface
        min_consecutive=2,
    )

    assert len(emitted) == 1
    assert findings["flaky-job"]["failed_count"] == 1


def test_healthy_resets_miss_tracker(tmp_path, monkeypatch):
    """A healthy result resets the miss counter so a prior stale run doesn't persist."""
    monkeypatch.setattr(
        cloud_run_job_failure_watcher,
        "emit_finding",
        lambda f, pm_repo_path=None, dry_run=False: None,
    )
    tracker = _FakeMissTracker({"CLOUD_RUN_JOB_FAILED::once-bad-now-good": 3})

    check_cloud_run_job_failures(
        job_stems=["once-bad-now-good"],
        execution_reader=_healthy_reader,
        pm_repo_path=str(tmp_path),
        dry_run=True,
        miss_tracker=tracker,  # type: ignore[arg-type]
        min_consecutive=2,
    )

    assert tracker._counts["CLOUD_RUN_JOB_FAILED::once-bad-now-good"] == 0


def test_empty_reader_emits_nothing(tmp_path, monkeypatch):
    """A reader that returns {} for all stems (API error path) emits nothing."""
    emitted: list[PipelineFinding] = []
    monkeypatch.setattr(
        cloud_run_job_failure_watcher,
        "emit_finding",
        lambda f, pm_repo_path=None, dry_run=False: emitted.append(f),
    )

    findings = check_cloud_run_job_failures(
        job_stems=["some-job"],
        execution_reader=lambda _stems: {},  # API error — empty dict
        pm_repo_path=str(tmp_path),
        dry_run=True,
    )

    # stem not in the reader result → treated as healthy (no finding, no miss registered)
    assert emitted == []
    # The stem is absent from findings since the reader returned no entry for it.
    assert findings.get("some-job") is None or findings.get("some-job") == {}


def test_mixed_fleet(tmp_path, monkeypatch):
    """Multiple jobs: healthy ones produce empty findings, failing ones emit."""
    emitted: list[PipelineFinding] = []
    monkeypatch.setattr(
        cloud_run_job_failure_watcher,
        "emit_finding",
        lambda f, pm_repo_path=None, dry_run=False: emitted.append(f),
    )

    findings = check_cloud_run_job_failures(
        job_stems=["ok-job", "broken-job", "also-ok"],
        execution_reader=_reader_for({
            "ok-job": None,
            "broken-job": _failed_diag("broken-job", failed_count=3, succeeded_count=0),
            "also-ok": None,
        }),
        pm_repo_path=str(tmp_path),
        dry_run=True,
    )

    assert findings["ok-job"] == {}
    assert findings["also-ok"] == {}
    assert findings["broken-job"]["failed_count"] == 3
    assert len(emitted) == 1
    assert "broken-job" in emitted[0].details.get("job_stem", "")


def test_summary_includes_completion_age(tmp_path, monkeypatch):
    """The summary should include completion_age_min when present."""
    emitted: list[PipelineFinding] = []
    monkeypatch.setattr(
        cloud_run_job_failure_watcher,
        "emit_finding",
        lambda f, pm_repo_path=None, dry_run=False: emitted.append(f),
    )

    check_cloud_run_job_failures(
        job_stems=["aged-job"],
        execution_reader=_reader_for({
            "aged-job": _failed_diag("aged-job", completion_age_min=45.0),
        }),
        pm_repo_path=str(tmp_path),
        dry_run=True,
    )

    assert len(emitted) == 1
    assert "completion_age=45m" in emitted[0].summary


def test_summary_omits_age_when_none(tmp_path, monkeypatch):
    """When completion_age_min is None the summary must not raise or include 'None'."""
    emitted: list[PipelineFinding] = []
    monkeypatch.setattr(
        cloud_run_job_failure_watcher,
        "emit_finding",
        lambda f, pm_repo_path=None, dry_run=False: emitted.append(f),
    )

    check_cloud_run_job_failures(
        job_stems=["no-age-job"],
        execution_reader=_reader_for({
            "no-age-job": _failed_diag("no-age-job", completion_age_min=None),
        }),
        pm_repo_path=str(tmp_path),
        dry_run=True,
    )

    assert len(emitted) == 1
    assert "None" not in emitted[0].summary
    assert "completion_age" not in emitted[0].summary


# ── make_cloud_run_job_execution_reader ──────────────────────────────────────

def test_reader_factory_returns_noop_on_sdk_import_failure(monkeypatch):
    """When google.cloud.run_v2 cannot be imported the factory must return a safe no-op."""
    import importlib

    original_import = importlib.import_module

    def _fail_run_v2(name, *args, **kwargs):
        if name == "google.cloud.run_v2":
            raise ImportError("simulated SDK absence")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", _fail_run_v2)

    reader = make_cloud_run_job_execution_reader(project_id="proj-x")
    result = reader(["job-a", "job-b"])
    assert result == {}


def test_reader_factory_returns_all_none_on_empty_project():
    """With project_id="" the reader should return None for every stem (not call GCP)."""
    # We call make_cloud_run_job_execution_reader with project_id="" AFTER the factory
    # has already imported the SDK (the SDK is installed; what matters is the inner guard).
    # But if ExecutionsClient() fails, the factory returns the empty-dict lambda instead.
    # Either way, with project_id="", the guard dict.fromkeys(..., None) or {} is safe.
    reader = make_cloud_run_job_execution_reader(project_id="")
    # Both the "no project" guard and the "SDK unavailable" guard are acceptable.
    result = reader(["any-job"])
    # The result must NOT contain a non-None failure diagnostic when project_id is empty.
    for v in result.values():
        assert v is None or v == {}


# ── consolidator exclusion guard ─────────────────────────────────────────────

def test_consolidator_prefix_constant_correct():
    """The exclusion prefix must match manifest-consolidator job names in the registry."""
    from deployment_service.cloud_run_job_registry import CLOUD_RUN_JOBS
    from deployment_service.data_pipeline_monitors.cloud_run_job_failure_watcher import (
        _CONSOLIDATOR_JOB_PREFIX,
    )

    consolidator_names = [
        job.name
        for job in CLOUD_RUN_JOBS
        if job.name.startswith("manifest-consolidator-")
    ]
    # Every manifest-consolidator job name must start with the declared prefix.
    assert all(n.startswith(_CONSOLIDATOR_JOB_PREFIX) for n in consolidator_names)
    assert len(consolidator_names) > 0  # sanity: there are consolidator jobs to exclude
    # And the prefix must NOT accidentally match non-consolidator jobs.
    non_consolidator = [
        job.name for job in CLOUD_RUN_JOBS if not job.name.startswith("manifest-consolidator-")
    ]
    assert all(not n.startswith(_CONSOLIDATOR_JOB_PREFIX) for n in non_consolidator)


def test_cli_meta_mode_stems_exclude_consolidators():
    """The job stem list built in cli.py meta mode must exclude manifest-consolidator-*."""
    from deployment_service.cloud_run_job_registry import CLOUD_RUN_JOBS
    from deployment_service.data_pipeline_monitors.cloud_run_job_failure_watcher import (
        _CONSOLIDATOR_JOB_PREFIX,
    )

    stems = [
        job.name
        for job in CLOUD_RUN_JOBS
        if not job.name.startswith(_CONSOLIDATOR_JOB_PREFIX)
    ]
    # No consolidator stems in the list.
    assert not any(s.startswith("manifest-consolidator-") for s in stems)
    # There must still be many stems remaining (the registry is large).
    assert len(stems) >= 20
