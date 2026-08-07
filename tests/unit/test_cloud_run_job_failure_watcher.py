"""Unit tests for DP-WATCHER-006 — cloud_run_job_failure_watcher."""

from __future__ import annotations

import contextlib
from collections.abc import Sequence

import pytest
from unified_api_contracts import DeploymentCloud, DeploymentKind, DeploymentTarget, DeploymentUmbrella

from deployment_service.data_pipeline_monitors import cloud_run_job_failure_watcher
from deployment_service.data_pipeline_monitors.cloud_run_job_failure_watcher import (
    CloudRunJobExecutionReader,
    check_cloud_run_job_failures,
    make_cloud_run_job_execution_reader,
)
from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding
from deployment_service.data_pipeline_monitors.meta_watchers import DEFAULT_MIN_CONSECUTIVE_MISSES, MissTracker

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _target(name: str, service: str = "my-service", asset_group: str = "") -> DeploymentTarget:
    return DeploymentTarget(
        name=name,
        kind=DeploymentKind.CLOUD_RUN_JOB,
        umbrella=DeploymentUmbrella.BATCH,
        cloud=DeploymentCloud.GCP,
        service=service,
        asset_group=asset_group,
        lifecycle_class="",
    )


def _succeeded_reader(targets: Sequence[DeploymentTarget]) -> dict[str, dict[str, object] | None]:
    """All jobs succeeded (latest execution has no failures)."""
    return {t.name: None for t in targets}


def _failed_reader(
    failed_names: set[str],
    failed_count: int = 1,
    completion_age_min: float | None = 5.0,
) -> CloudRunJobExecutionReader:
    """Jobs in ``failed_names`` have failed_count > 0; others succeeded."""

    def _read(targets: Sequence[DeploymentTarget]) -> dict[str, dict[str, object] | None]:
        result: dict[str, dict[str, object] | None] = {}
        for t in targets:
            if t.name in failed_names:
                result[t.name] = {
                    "failed_count": failed_count,
                    "completion_age_min": completion_age_min,
                }
            else:
                result[t.name] = None
        return result

    return _read


def _collect_findings(monkeypatch: pytest.MonkeyPatch) -> list[PipelineFinding]:
    emitted: list[PipelineFinding] = []

    def _fake_emit(finding: PipelineFinding, *, pm_repo_path: object = None, dry_run: bool = False) -> None:
        emitted.append(finding)

    monkeypatch.setattr(cloud_run_job_failure_watcher, "emit_finding", _fake_emit)
    return emitted


# ---------------------------------------------------------------------------
# check_cloud_run_job_failures — core logic
# ---------------------------------------------------------------------------


def test_no_failures_emits_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted = _collect_findings(monkeypatch)
    targets = [_target("job-a"), _target("job-b")]
    result = check_cloud_run_job_failures(
        targets=targets,
        execution_reader=_succeeded_reader,
        dry_run=True,
    )
    assert emitted == []
    assert all(v == {} for v in result.values())


def test_failed_job_emits_critical_page_operator(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted = _collect_findings(monkeypatch)
    targets = [_target("job-ok"), _target("job-bad", service="bad-svc")]
    result = check_cloud_run_job_failures(
        targets=targets,
        execution_reader=_failed_reader({"job-bad"}, failed_count=3, completion_age_min=12.0),
        dry_run=True,
    )
    assert len(emitted) == 1
    f = emitted[0]
    assert f.event == "DP_CLOUD_RUN_JOB_EXEC_FAILED"
    assert f.severity == "CRITICAL"
    assert f.tier == EscalationTier.PAGE_OPERATOR
    assert f.registry_id == "DP-WATCHER-006"
    assert "job-bad" in f.summary
    assert "bad-svc" in f.summary
    assert result["job-bad"]["failed_count"] == 3
    assert result["job-ok"] == {}


def test_multiple_failed_jobs_each_emit(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted = _collect_findings(monkeypatch)
    targets = [_target("alpha"), _target("beta"), _target("gamma")]
    result = check_cloud_run_job_failures(
        targets=targets,
        execution_reader=_failed_reader({"alpha", "gamma"}),
        dry_run=True,
    )
    assert len(emitted) == 2
    fired_names = {f.details["job_stem"] for f in emitted}
    assert fired_names == {"alpha", "gamma"}
    assert result["beta"] == {}


def test_details_carry_service_and_umbrella(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted = _collect_findings(monkeypatch)
    target = _target("the-job", service="my-svc", asset_group="cefi")
    check_cloud_run_job_failures(
        targets=[target],
        execution_reader=_failed_reader({"the-job"}, failed_count=2),
        dry_run=True,
    )
    assert len(emitted) == 1
    d = emitted[0].details
    assert d["job_stem"] == "the-job"
    assert d["service"] == "my-svc"
    assert d["asset_group"] == "cefi"
    assert d["failed_count"] == 2


def test_completion_age_none_in_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted = _collect_findings(monkeypatch)
    check_cloud_run_job_failures(
        targets=[_target("j")],
        execution_reader=_failed_reader({"j"}, completion_age_min=None),
        dry_run=True,
    )
    assert "unknown age" in emitted[0].summary


def test_completion_age_formatted_in_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted = _collect_findings(monkeypatch)
    check_cloud_run_job_failures(
        targets=[_target("j")],
        execution_reader=_failed_reader({"j"}, completion_age_min=42.3),
        dry_run=True,
    )
    assert "42m" in emitted[0].summary


# ---------------------------------------------------------------------------
# MissTracker gating
# ---------------------------------------------------------------------------

_LOG_BUCKET = "deployment-scripts-test"


class _StorageForMissTracker:
    """Minimal in-memory storage implementing the UTL StorageClient interface."""

    def __init__(self) -> None:
        self._blobs: dict[tuple[str, str], bytes] = {}

    def blob_exists(self, bucket: str, path: str) -> bool:
        return (bucket, path) in self._blobs

    def download_bytes(self, bucket: str, path: str) -> bytes:
        return self._blobs[(bucket, path)]

    def upload_bytes(self, bucket: str, path: str, data: bytes, content_type: str | None = None) -> str:
        self._blobs[(bucket, path)] = data
        return f"gs://{bucket}/{path}"

    def get_blob_metadata(self, bucket: str, path: str) -> object | None:
        return None


def _fresh_tracker() -> MissTracker:
    """Return a MissTracker backed by an empty in-memory storage (counts start at 0)."""
    return MissTracker.load(storage_client=_StorageForMissTracker(), log_bucket=_LOG_BUCKET)  # type: ignore[arg-type]


def test_consecutive_miss_suppresses_first_sweep(monkeypatch: pytest.MonkeyPatch) -> None:
    """With DEFAULT_MIN_CONSECUTIVE_MISSES=2, a single failed sweep should NOT page."""
    emitted = _collect_findings(monkeypatch)
    tracker = _fresh_tracker()
    check_cloud_run_job_failures(
        targets=[_target("slow-job")],
        execution_reader=_failed_reader({"slow-job"}),
        miss_tracker=tracker,
        min_consecutive=DEFAULT_MIN_CONSECUTIVE_MISSES,
        dry_run=True,
    )
    # First miss: count=1 < min_consecutive=2 — should NOT emit
    assert emitted == []


def test_consecutive_miss_pages_after_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    """After reaching min_consecutive (2) misses the alert fires on sweep 2."""
    emitted = _collect_findings(monkeypatch)
    storage = _StorageForMissTracker()
    # Sweep 1 — stale, count reaches 1 (< min_consecutive=2, no page)
    t1 = MissTracker.load(storage_client=storage, log_bucket=_LOG_BUCKET)  # type: ignore[arg-type]
    check_cloud_run_job_failures(
        targets=[_target("slow-job")],
        execution_reader=_failed_reader({"slow-job"}),
        miss_tracker=t1,
        min_consecutive=DEFAULT_MIN_CONSECUTIVE_MISSES,
        dry_run=True,
    )
    assert emitted == []
    # Persist the counter so sweep 2 sees count=1
    t1.persist(dry_run=False)
    # Sweep 2 — stale again, count reaches 2 == min_consecutive → fires
    t2 = MissTracker.load(storage_client=storage, log_bucket=_LOG_BUCKET)  # type: ignore[arg-type]
    check_cloud_run_job_failures(
        targets=[_target("slow-job")],
        execution_reader=_failed_reader({"slow-job"}),
        miss_tracker=t2,
        min_consecutive=DEFAULT_MIN_CONSECUTIVE_MISSES,
        dry_run=True,
    )
    assert len(emitted) == 1


def test_success_resets_miss_counter(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful execution after two failed sweeps resets and stops paging."""
    emitted = _collect_findings(monkeypatch)
    storage = _StorageForMissTracker()
    # Sweep 1 + 2 — accumulate to threshold then fire
    for _ in range(DEFAULT_MIN_CONSECUTIVE_MISSES):
        t = MissTracker.load(storage_client=storage, log_bucket=_LOG_BUCKET)  # type: ignore[arg-type]
        check_cloud_run_job_failures(
            targets=[_target("ok-job")],
            execution_reader=_failed_reader({"ok-job"}),
            miss_tracker=t,
            min_consecutive=DEFAULT_MIN_CONSECUTIVE_MISSES,
            dry_run=True,
        )
        t.persist(dry_run=False)
    # Now the job recovers (no failure on latest execution) — persist the reset counter
    t_recover = MissTracker.load(storage_client=storage, log_bucket=_LOG_BUCKET)  # type: ignore[arg-type]
    check_cloud_run_job_failures(
        targets=[_target("ok-job")],
        execution_reader=_succeeded_reader,
        miss_tracker=t_recover,
        min_consecutive=DEFAULT_MIN_CONSECUTIVE_MISSES,
        dry_run=True,
    )
    t_recover.persist(dry_run=False)  # persist the reset so the next load sees count=0
    # Reset sweep fires nothing new; clear emitted from the two paging sweeps
    emitted.clear()
    # Next failure cycle starts fresh — sweep 1 should NOT page again
    t_fresh = MissTracker.load(storage_client=storage, log_bucket=_LOG_BUCKET)  # type: ignore[arg-type]
    check_cloud_run_job_failures(
        targets=[_target("ok-job")],
        execution_reader=_failed_reader({"ok-job"}),
        miss_tracker=t_fresh,
        min_consecutive=DEFAULT_MIN_CONSECUTIVE_MISSES,
        dry_run=True,
    )
    assert emitted == [], "counter should have been reset; first miss of new cycle must not page"


# ---------------------------------------------------------------------------
# make_cloud_run_job_execution_reader — fallback on import failure
# ---------------------------------------------------------------------------


def test_reader_returns_empty_on_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """When google.cloud.run_v2 cannot be imported, the reader silently returns {}."""
    import importlib as _importlib

    _orig = _importlib.import_module

    def _raise(name: str, *args: object, **kwargs: object) -> object:
        if name == "google.cloud.run_v2":
            raise ImportError("no google-cloud-run installed")
        return _orig(name, *args, **kwargs)

    monkeypatch.setattr(_importlib, "import_module", _raise)

    reader = make_cloud_run_job_execution_reader(project_id="my-proj")
    targets = [_target("j1"), _target("j2")]
    result = reader(targets)
    assert result == {}


def test_reader_returns_all_none_when_no_project_id() -> None:
    """Empty project_id → all-None dict without making any GCP calls."""

    class _FakeClient:
        def list_executions(self, *, parent: str) -> list[object]:
            raise AssertionError("should not call list_executions with empty project_id")

    import importlib as _importlib
    import types

    fake_mod = types.ModuleType("google.cloud.run_v2")
    fake_mod.ExecutionsClient = lambda: _FakeClient()  # type: ignore[attr-defined]

    import sys

    sys.modules["google.cloud.run_v2"] = fake_mod
    try:
        reader = make_cloud_run_job_execution_reader(project_id="")
        targets = [_target("a"), _target("b")]
        result = reader(targets)
    finally:
        del sys.modules["google.cloud.run_v2"]

    assert result == {"a": None, "b": None}


# ---------------------------------------------------------------------------
# CLI wiring smoke test (dry-run)
# ---------------------------------------------------------------------------


def test_cli_meta_mode_stubs_cloud_run_job_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    """The meta-mode CLI runs without error when the job-failure reader is stubbed."""
    from deployment_service.data_pipeline_monitors import cli

    # Stub out all credential-bound factory functions so this test is network-free.
    monkeypatch.setattr(cli, "_project_id", lambda: "test-proj")
    monkeypatch.setattr(cli, "_log_bucket", lambda: "test-log-bucket")
    monkeypatch.setattr(cli, "_scheduler_env_prefix", lambda: "prd-")
    monkeypatch.setattr(cli, "get_storage_client", lambda: _StorageForMissTracker())
    monkeypatch.setattr(cli, "UnifiedCloudConfig", lambda: _FakeConfig())
    monkeypatch.setattr(cli, "PubSubEventSink", lambda **_kw: None)
    monkeypatch.setattr(cli, "setup_events", lambda **_kw: None)
    monkeypatch.setattr(cli, "run_lifecycle", _NullLifecycle)
    monkeypatch.setattr(cli, "_make_scheduler_state_reader", lambda: lambda _job: None)
    monkeypatch.setattr(
        cli.consolidator_scheduler_watcher,
        "make_consolidator_scheduler_lister",
        lambda project_id, scheduler_rpc_timeout_secs=10.0: lambda: [],
    )
    monkeypatch.setattr(cli, "_make_execution_history_reader", lambda: lambda _job: None)
    monkeypatch.setattr(
        cli.consolidator_oom_watcher,
        "make_consolidator_execution_oom_reader",
        lambda project_id, env_prefix="", location="asia-northeast1": lambda _ags: {},
    )
    # DP-WATCHER-006: stub the new job-failure reader (returns {} = no failures).
    monkeypatch.setattr(
        cli.cloud_run_job_failure_watcher,
        "make_cloud_run_job_execution_reader",
        lambda project_id, env_prefix="", location="asia-northeast1": lambda _targets: {},
    )
    rc = cli.main(["--mode", "meta", "--dry-run"])
    assert rc == 0


# ---------------------------------------------------------------------------
# Fakes shared by multiple test sections
# ---------------------------------------------------------------------------


class _FakeConfig:
    gcp_project_id = "test-proj"


@contextlib.contextmanager
def _NullLifecycle(*, service_name: str = "") -> object:  # type: ignore[misc]
    yield
