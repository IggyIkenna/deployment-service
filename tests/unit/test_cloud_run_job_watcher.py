"""Unit tests for cloud_run_job_watcher (DP-WATCHER-006).

Credential-free: the execution reader is injected as a pure callable; MissTracker
uses an in-memory FakeStorage. No GCP SDK import required.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from unified_api_contracts import DeploymentCloud, DeploymentKind, DeploymentTarget, DeploymentUmbrella

from deployment_service.data_pipeline_monitors import cloud_run_job_watcher, meta_watchers
from deployment_service.data_pipeline_monitors.cloud_run_job_watcher import (
    _DP_CLOUD_RUN_JOB_FAILED,
    _crj_miss_key,
    check_cloud_run_jobs,
)

# ── fakes ─────────────────────────────────────────────────────────────────────


class _FakeBlobMeta:
    def __init__(self, last_modified: str | None):
        self.last_modified = last_modified


class FakeStorage:
    def __init__(self, blobs: dict[tuple[str, str], tuple[bytes, float | None]] | None = None):
        self.blobs = blobs or {}
        self.uploaded: dict[tuple[str, str], bytes] = {}

    def blob_exists(self, bucket: str, path: str) -> bool:
        return (bucket, path) in self.blobs

    def download_bytes(self, bucket: str, path: str) -> bytes:
        return self.blobs[(bucket, path)][0]

    def get_blob_metadata(self, bucket: str, path: str) -> _FakeBlobMeta | None:
        entry = self.blobs.get((bucket, path))
        if entry is None:
            return None
        _data, age_min = entry
        if age_min is None:
            return _FakeBlobMeta(None)
        ts = (datetime.now(UTC) - timedelta(minutes=age_min)).isoformat()
        return _FakeBlobMeta(ts)

    def upload_bytes(self, bucket: str, path: str, data: bytes, content_type: str | None = None) -> str:
        self.uploaded[(bucket, path)] = data
        self.blobs[(bucket, path)] = (data, 0.0)
        return f"gs://{bucket}/{path}"


LOG_BUCKET = "deployment-scripts-test-project"


def _make_tracker(storage: FakeStorage | None = None) -> meta_watchers.MissTracker:
    s = storage or FakeStorage()
    return meta_watchers.MissTracker.load(storage_client=s, log_bucket=LOG_BUCKET)


def _job(name: str, service: str = "some-service", asset_group: str = "") -> DeploymentTarget:
    return DeploymentTarget(
        name=name,
        kind=DeploymentKind.CLOUD_RUN_JOB,
        umbrella=DeploymentUmbrella.BATCH,
        cloud=DeploymentCloud.GCP,
        service=service,
        asset_group=asset_group,
        lifecycle_class="",
    )


def _capture_emits(monkeypatch: Any) -> list[tuple[str, str]]:
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    return emitted


# ── check_cloud_run_jobs: no failures ─────────────────────────────────────────


def test_no_findings_when_reader_returns_none(monkeypatch: Any) -> None:
    emitted = _capture_emits(monkeypatch)
    job = _job("batch-live-smoke-matrix-daily")
    result = check_cloud_run_jobs(
        jobs=[job],
        execution_reader=lambda _stem: None,
        dry_run=True,
    )
    assert result["batch-live-smoke-matrix-daily"] == {}
    assert not emitted


def test_no_findings_when_all_jobs_succeed(monkeypatch: Any) -> None:
    emitted = _capture_emits(monkeypatch)
    jobs = [_job("job-a"), _job("job-b")]
    result = check_cloud_run_jobs(
        jobs=jobs,
        execution_reader=lambda _stem: None,
        dry_run=True,
    )
    assert all(v == {} for v in result.values())
    assert not emitted


# ── check_cloud_run_jobs: failure detection ────────────────────────────────────


def test_finding_emitted_on_failed_execution_no_miss_tracker(monkeypatch: Any) -> None:
    emitted = _capture_emits(monkeypatch)
    job = _job("client-reporting-batch", service="client-reporting-api")
    diag: dict[str, object] = {"failed_count": 3, "completion_age_min": 45.0}
    result = check_cloud_run_jobs(
        jobs=[job],
        execution_reader=lambda _stem: diag,
    )
    assert result["client-reporting-batch"]["failed_count"] == 3
    assert any(e[0] == _DP_CLOUD_RUN_JOB_FAILED and e[1] == "CRITICAL" for e in emitted)


def test_finding_details_include_service_and_asset_group(monkeypatch: Any) -> None:
    _capture_emits(monkeypatch)
    job = _job("manifest-consolidator-cefi", service="manifest-consolidator", asset_group="cefi")
    diag: dict[str, object] = {"failed_count": 1, "completion_age_min": 10.0}
    result = check_cloud_run_jobs(
        jobs=[job],
        execution_reader=lambda _stem: diag,
    )
    details = result["manifest-consolidator-cefi"]
    assert details["service"] == "manifest-consolidator"
    assert details["asset_group"] == "cefi"
    assert details["job_name"] == "manifest-consolidator-cefi"


def test_finding_details_completion_age_none(monkeypatch: Any) -> None:
    _capture_emits(monkeypatch)
    job = _job("catalogue-regen-nightly")
    diag: dict[str, object] = {"failed_count": 1, "completion_age_min": None}
    result = check_cloud_run_jobs(
        jobs=[job],
        execution_reader=lambda _stem: diag,
    )
    assert result["catalogue-regen-nightly"]["completion_age_min"] is None


# ── MissTracker gating ────────────────────────────────────────────────────────


def test_consecutive_miss_suppresses_first_then_pages_second(monkeypatch: Any) -> None:
    emitted = _capture_emits(monkeypatch)
    job = _job("cost-snapshot", service="deployment-api")
    diag: dict[str, object] = {"failed_count": 2, "completion_age_min": 30.0}
    storage = FakeStorage()

    t1 = _make_tracker(storage)
    result1 = check_cloud_run_jobs(
        jobs=[job],
        execution_reader=lambda _stem: diag,
        miss_tracker=t1,
        min_consecutive=2,
    )
    t1.persist()
    assert result1["cost-snapshot"] == {}
    assert not any(e[0] == _DP_CLOUD_RUN_JOB_FAILED for e in emitted)

    t2 = _make_tracker(storage)
    result2 = check_cloud_run_jobs(
        jobs=[job],
        execution_reader=lambda _stem: diag,
        miss_tracker=t2,
        min_consecutive=2,
    )
    assert result2["cost-snapshot"]["failed_count"] == 2
    assert any(e[0] == _DP_CLOUD_RUN_JOB_FAILED and e[1] == "CRITICAL" for e in emitted)


def test_miss_counter_resets_on_success(monkeypatch: Any) -> None:
    _capture_emits(monkeypatch)
    job = _job("cost-snapshot")
    storage = FakeStorage()

    t1 = _make_tracker(storage)
    check_cloud_run_jobs(
        jobs=[job],
        execution_reader=lambda _stem: {"failed_count": 1, "completion_age_min": 5.0},
        dry_run=True,
        miss_tracker=t1,
        min_consecutive=2,
    )
    t1.persist()
    assert t1._counts[_crj_miss_key("cost-snapshot")] == 1

    t2 = _make_tracker(storage)
    check_cloud_run_jobs(
        jobs=[job],
        execution_reader=lambda _stem: None,
        dry_run=True,
        miss_tracker=t2,
        min_consecutive=2,
    )
    assert _crj_miss_key("cost-snapshot") not in t2._counts


def test_multiple_jobs_tracked_independently(monkeypatch: Any) -> None:
    emitted = _capture_emits(monkeypatch)
    job_a = _job("job-a")
    job_b = _job("job-b")
    storage = FakeStorage()

    def reader(stem: str) -> dict[str, object] | None:
        if stem == "job-a":
            return {"failed_count": 1, "completion_age_min": 5.0}
        return None

    t1 = _make_tracker(storage)
    r1 = check_cloud_run_jobs(
        jobs=[job_a, job_b],
        execution_reader=reader,
        miss_tracker=t1,
        min_consecutive=2,
    )
    t1.persist()
    assert r1["job-a"] == {}
    assert r1["job-b"] == {}
    assert not emitted

    t2 = _make_tracker(storage)
    r2 = check_cloud_run_jobs(
        jobs=[job_a, job_b],
        execution_reader=reader,
        miss_tracker=t2,
        min_consecutive=2,
    )
    assert r2["job-a"]["failed_count"] == 1
    assert r2["job-b"] == {}
    assert any(e[0] == _DP_CLOUD_RUN_JOB_FAILED for e in emitted)


def test_no_miss_tracker_pages_on_first_sweep(monkeypatch: Any) -> None:
    emitted = _capture_emits(monkeypatch)
    job = _job("honest-coverage-daily-launcher")
    check_cloud_run_jobs(
        jobs=[job],
        execution_reader=lambda _stem: {"failed_count": 1, "completion_age_min": 20.0},
    )
    assert any(e[0] == _DP_CLOUD_RUN_JOB_FAILED for e in emitted)


# ── make_cloud_run_job_execution_reader factory ────────────────────────────────


def test_reader_factory_returns_none_on_import_error() -> None:
    import sys
    original = sys.modules.get("google.cloud.run_v2", None)
    sys.modules["google.cloud.run_v2"] = None  # type: ignore[assignment]
    try:
        reader = cloud_run_job_watcher.make_cloud_run_job_execution_reader(
            project_id="test-project",
            env_prefix="uts-prod",
        )
        assert reader("some-job") is None
    finally:
        if original is None:
            sys.modules.pop("google.cloud.run_v2", None)
        else:
            sys.modules["google.cloud.run_v2"] = original


def test_reader_factory_returns_none_when_no_project_id() -> None:
    reader = cloud_run_job_watcher.make_cloud_run_job_execution_reader(project_id="")
    assert reader("some-job") is None


def test_reader_constructs_full_job_name_with_env_prefix() -> None:
    observed_parents: list[str] = []

    class _FakeExecutionsClient:
        def list_executions(self, *, parent: str):  # type: ignore[override]
            observed_parents.append(parent)
            return []

    import sys
    import types

    fake_run_v2 = types.ModuleType("google.cloud.run_v2")
    fake_run_v2.ExecutionsClient = _FakeExecutionsClient  # type: ignore[attr-defined]
    sys.modules["google.cloud.run_v2"] = fake_run_v2
    try:
        reader = cloud_run_job_watcher.make_cloud_run_job_execution_reader(
            project_id="my-project",
            env_prefix="uts-prod",
            location="asia-northeast1",
        )
        result = reader("cost-snapshot")
        assert result is None
        assert observed_parents == [
            "projects/my-project/locations/asia-northeast1/jobs/uts-prod-cost-snapshot"
        ]
    finally:
        sys.modules.pop("google.cloud.run_v2", None)


def test_reader_constructs_full_job_name_without_env_prefix() -> None:
    observed_parents: list[str] = []

    class _FakeExecutionsClient:
        def list_executions(self, *, parent: str):  # type: ignore[override]
            observed_parents.append(parent)
            return []

    import sys
    import types

    fake_run_v2 = types.ModuleType("google.cloud.run_v2")
    fake_run_v2.ExecutionsClient = _FakeExecutionsClient  # type: ignore[attr-defined]
    sys.modules["google.cloud.run_v2"] = fake_run_v2
    try:
        reader = cloud_run_job_watcher.make_cloud_run_job_execution_reader(
            project_id="my-project",
            env_prefix="",
            location="asia-northeast1",
        )
        reader("some-stem")
        assert observed_parents == ["projects/my-project/locations/asia-northeast1/jobs/some-stem"]
    finally:
        sys.modules.pop("google.cloud.run_v2", None)


def test_reader_suppresses_when_success_newer_than_failure() -> None:
    call_count = 0

    class _FakeExecution:
        def __init__(self, failed: int, age_sec: int) -> None:
            from datetime import UTC, datetime, timedelta

            self.failed_count = failed
            self.completion_time = datetime.now(UTC) - timedelta(seconds=age_sec)

    class _FakeExecutionsClient:
        def list_executions(self, *, parent: str):  # type: ignore[override]
            nonlocal call_count
            call_count += 1
            return [
                _FakeExecution(failed=1, age_sec=3600),
                _FakeExecution(failed=0, age_sec=1800),
            ]

    import sys
    import types

    fake_run_v2 = types.ModuleType("google.cloud.run_v2")
    fake_run_v2.ExecutionsClient = _FakeExecutionsClient  # type: ignore[attr-defined]
    sys.modules["google.cloud.run_v2"] = fake_run_v2
    try:
        reader = cloud_run_job_watcher.make_cloud_run_job_execution_reader(
            project_id="my-project",
            env_prefix="",
        )
        result = reader("some-job")
        assert result is None, "should suppress — success is newer than failure"
    finally:
        sys.modules.pop("google.cloud.run_v2", None)


def test_reader_returns_diagnostics_when_failure_newest() -> None:
    class _FakeExecution:
        def __init__(self, failed: int, age_sec: int) -> None:
            from datetime import UTC, datetime, timedelta

            self.failed_count = failed
            self.completion_time = datetime.now(UTC) - timedelta(seconds=age_sec)

    class _FakeExecutionsClient:
        def list_executions(self, *, parent: str):  # type: ignore[override]
            return [
                _FakeExecution(failed=0, age_sec=3600),
                _FakeExecution(failed=2, age_sec=300),
            ]

    import sys
    import types

    fake_run_v2 = types.ModuleType("google.cloud.run_v2")
    fake_run_v2.ExecutionsClient = _FakeExecutionsClient  # type: ignore[attr-defined]
    sys.modules["google.cloud.run_v2"] = fake_run_v2
    try:
        reader = cloud_run_job_watcher.make_cloud_run_job_execution_reader(
            project_id="my-project",
            env_prefix="",
        )
        result = reader("some-job")
        assert result is not None
        assert result["failed_count"] == 2
        age = result["completion_age_min"]
        assert age is not None and 4 < float(age) < 8
    finally:
        sys.modules.pop("google.cloud.run_v2", None)


def test_reader_api_error_returns_none() -> None:
    class _FakeExecutionsClient:
        def list_executions(self, *, parent: str):  # type: ignore[override]
            raise RuntimeError("API unavailable")

    import sys
    import types

    fake_run_v2 = types.ModuleType("google.cloud.run_v2")
    fake_run_v2.ExecutionsClient = _FakeExecutionsClient  # type: ignore[attr-defined]
    sys.modules["google.cloud.run_v2"] = fake_run_v2
    try:
        reader = cloud_run_job_watcher.make_cloud_run_job_execution_reader(
            project_id="my-project",
            env_prefix="",
        )
        result = reader("some-job")
        assert result is None
    finally:
        sys.modules.pop("google.cloud.run_v2", None)
