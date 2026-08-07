"""Unit tests for DP-WATCHER-006 — cloud_run_job_failure_watcher."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from deployment_service.data_pipeline_monitors import cloud_run_job_failure_watcher
from deployment_service.data_pipeline_monitors.cloud_run_job_failure_watcher import (
    JobExecutionReader,
    _failure_miss_key,
    _gcp_job_stems,
    check_cloud_run_job_failures,
)
from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding

# ---------------------------------------------------------------------------
# Helpers / stubs
# ---------------------------------------------------------------------------


class _FakeMissTracker:
    """Minimal MissTracker stub for testing consecutive-miss gating."""

    def __init__(self, *, always_gate: bool = False, hit_threshold: int = 0) -> None:
        self.calls: dict[str, int] = {}
        self._always_gate = always_gate
        self._hit_threshold = hit_threshold

    def register(self, key: str, *, stale: bool) -> int:
        if not stale:
            self.calls[key] = 0
            return 0
        count = self.calls.get(key, 0) + 1
        self.calls[key] = count
        if self._always_gate:
            return self._hit_threshold  # always at threshold → never suppress
        return count

    def persist(self, *, dry_run: bool = False) -> None:
        pass


def _reader_that_returns(mapping: dict[str, dict[str, object] | None]) -> JobExecutionReader:
    """Build a reader that returns the pre-set diagnostics dict per job stem."""

    def _read(stem: str) -> dict[str, object] | None:
        return mapping.get(stem)

    return _read


def _null_reader() -> JobExecutionReader:
    return lambda _stem: None


# ---------------------------------------------------------------------------
# _gcp_job_stems()
# ---------------------------------------------------------------------------


def test_gcp_job_stems_returns_non_empty() -> None:
    stems = _gcp_job_stems()
    assert len(stems) > 0, "registry must contain at least one non-consolidator GCP Cloud Run Job"


def test_gcp_job_stems_excludes_consolidators() -> None:
    stems = _gcp_job_stems()
    for stem, _ in stems:
        assert not stem.startswith("manifest-consolidator-"), (
            f"manifest-consolidator jobs must be excluded (covered by DP-WATCHER-005): {stem}"
        )


def test_gcp_job_stems_all_have_names() -> None:
    stems = _gcp_job_stems()
    for stem, service in stems:
        assert stem, "every registered job must have a non-empty stem"
        # service may be empty for some entries (cross-asset jobs)


def test_gcp_job_stems_includes_known_singleton() -> None:
    stems = _gcp_job_stems()
    stem_names = {s for s, _ in stems}
    # live-event-log-compactor is a singleton job known to be in the registry
    assert "live-event-log-compactor" in stem_names, "live-event-log-compactor must appear in the job stems"


# ---------------------------------------------------------------------------
# _failure_miss_key()
# ---------------------------------------------------------------------------


def test_failure_miss_key_format() -> None:
    key = _failure_miss_key("my-job")
    assert "DP_CLOUD_RUN_JOB_FAILED" in key
    assert "my-job" in key


# ---------------------------------------------------------------------------
# check_cloud_run_job_failures — happy paths
# ---------------------------------------------------------------------------


def test_check_no_failures_returns_empty_dicts() -> None:
    stems = [s for s, _ in _gcp_job_stems()]
    reader = _null_reader()
    result = check_cloud_run_job_failures(execution_reader=reader, dry_run=True)
    for stem in stems:
        assert stem in result
        assert result[stem] == {}


def test_check_single_failure_emits_finding(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted: list[PipelineFinding] = []

    def _fake_emit(finding: PipelineFinding, *, pm_repo_path: object, dry_run: bool) -> None:
        emitted.append(finding)

    monkeypatch.setattr(cloud_run_job_failure_watcher, "emit_finding", _fake_emit)

    # Pick one real stem from the registry
    stem, service = _gcp_job_stems()[0]
    diag: dict[str, object] = {
        "failed_count": 3,
        "succeeded_count": 0,
        "completion_age_min": 45.0,
        "job_name": f"prd-{stem}",
    }
    reader = _reader_that_returns({stem: diag})
    # Use a tracker that immediately hits the threshold (min_consecutive=1)
    tracker = _FakeMissTracker(always_gate=True, hit_threshold=1)

    result = check_cloud_run_job_failures(
        execution_reader=reader,
        dry_run=True,
        miss_tracker=tracker,
        min_consecutive=1,
    )

    assert len(emitted) == 1
    f = emitted[0]
    assert f.event == "DP_CLOUD_RUN_JOB_FAILED"
    assert f.severity == "CRITICAL"
    assert f.tier is EscalationTier.PAGE_OPERATOR
    assert f.registry_id == "DP-WATCHER-006"
    assert "failed_count" in f.details
    assert result[stem].get("failed_count") == 3


def test_check_miss_below_threshold_suppresses(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted: list[PipelineFinding] = []

    def _fake_emit(finding: PipelineFinding, *, pm_repo_path: object, dry_run: bool) -> None:
        emitted.append(finding)

    monkeypatch.setattr(cloud_run_job_failure_watcher, "emit_finding", _fake_emit)

    stem, _ = _gcp_job_stems()[0]
    diag: dict[str, object] = {
        "failed_count": 1,
        "succeeded_count": 0,
        "completion_age_min": 10.0,
        "job_name": stem,
    }
    reader = _reader_that_returns({stem: diag})
    # Tracker returns miss count = 1 but threshold is 3 → suppress
    tracker = _FakeMissTracker(always_gate=False)  # increments naturally from 0

    result = check_cloud_run_job_failures(
        execution_reader=reader,
        dry_run=True,
        miss_tracker=tracker,
        min_consecutive=3,
    )

    assert emitted == [], "should be suppressed when below consecutive-miss threshold"
    assert result[stem] == {}


def test_check_miss_tracker_reset_on_success() -> None:
    stem, _ = _gcp_job_stems()[0]
    tracker = _FakeMissTracker(always_gate=False)
    # First call: failure → register stale
    reader_fail = _reader_that_returns({stem: {"failed_count": 1, "job_name": stem}})
    check_cloud_run_job_failures(execution_reader=reader_fail, dry_run=True, miss_tracker=tracker, min_consecutive=5)
    key = _failure_miss_key(stem)
    assert tracker.calls.get(key, 0) == 1

    # Second call: success → register NOT stale → counter resets
    reader_ok = _null_reader()
    check_cloud_run_job_failures(execution_reader=reader_ok, dry_run=True, miss_tracker=tracker, min_consecutive=5)
    assert tracker.calls.get(key, 0) == 0


def test_check_summary_contains_job_name(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted: list[PipelineFinding] = []

    monkeypatch.setattr(
        cloud_run_job_failure_watcher,
        "emit_finding",
        lambda f, *, pm_repo_path, dry_run: emitted.append(f),
    )

    stem, _ = _gcp_job_stems()[0]
    job_name = f"prd-{stem}"
    diag: dict[str, object] = {
        "failed_count": 2,
        "succeeded_count": 0,
        "completion_age_min": 120.0,
        "job_name": job_name,
    }
    reader = _reader_that_returns({stem: diag})
    tracker = _FakeMissTracker(always_gate=True, hit_threshold=1)

    check_cloud_run_job_failures(execution_reader=reader, dry_run=True, miss_tracker=tracker, min_consecutive=1)

    assert emitted, "expected exactly one finding"
    assert job_name in emitted[0].summary
    assert "2 failed" in emitted[0].summary


def test_check_finding_details_has_label(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted: list[PipelineFinding] = []

    monkeypatch.setattr(
        cloud_run_job_failure_watcher,
        "emit_finding",
        lambda f, *, pm_repo_path, dry_run: emitted.append(f),
    )

    stem, _ = _gcp_job_stems()[0]
    diag: dict[str, object] = {"failed_count": 1, "job_name": stem, "completion_age_min": 5.0}
    reader = _reader_that_returns({stem: diag})
    tracker = _FakeMissTracker(always_gate=True, hit_threshold=1)

    check_cloud_run_job_failures(execution_reader=reader, dry_run=True, miss_tracker=tracker, min_consecutive=1)

    assert emitted
    # label must be present so _alert_key gives a per-job identity
    assert emitted[0].details.get("label") == stem


# ---------------------------------------------------------------------------
# make_job_execution_reader — unit tests via mock
# ---------------------------------------------------------------------------


class _FakeExecution:
    def __init__(
        self,
        *,
        failed_count: int = 0,
        succeeded_count: int = 0,
        completion_time: object | None = None,
    ) -> None:
        self.failed_count = failed_count
        self.succeeded_count = succeeded_count
        self.completion_time = completion_time


class _FakeCompletionTime:
    def __init__(self, *, dt: object) -> None:
        self._dt = dt

    def ToDatetime(self, tzinfo: object = None) -> Any:  # noqa: N803
        from datetime import UTC, datetime

        if isinstance(self._dt, datetime):
            return self._dt.replace(tzinfo=UTC) if self._dt.tzinfo is None else self._dt
        return self._dt


def test_make_reader_returns_null_reader_when_sdk_unavailable() -> None:
    with patch.object(
        cloud_run_job_failure_watcher.importlib,
        "import_module",
        side_effect=ImportError("no module"),
    ):
        reader = cloud_run_job_failure_watcher.make_job_execution_reader("proj", "prd")
    assert reader("some-job") is None


def test_make_reader_returns_none_when_all_executions_succeeded() -> None:
    from datetime import UTC, datetime

    completion_dt = datetime(2026, 8, 7, 10, 0, 0, tzinfo=UTC)
    ok_exec = _FakeExecution(succeeded_count=1, failed_count=0, completion_time=completion_dt)

    mock_client = MagicMock()
    mock_client.list_executions.return_value = [ok_exec]

    mock_run_mod = MagicMock()
    mock_run_mod.ExecutionsClient.return_value = mock_client

    with patch.object(cloud_run_job_failure_watcher.importlib, "import_module", return_value=mock_run_mod):
        reader = cloud_run_job_failure_watcher.make_job_execution_reader("proj", "prd")
    assert reader("live-event-log-compactor") is None


def test_make_reader_returns_diagnostics_when_latest_failed() -> None:
    from datetime import UTC, datetime

    older_ok = _FakeExecution(
        succeeded_count=1, failed_count=0, completion_time=datetime(2026, 8, 6, 8, 0, 0, tzinfo=UTC)
    )
    newer_fail = _FakeExecution(
        succeeded_count=0,
        failed_count=2,
        completion_time=datetime(2026, 8, 7, 12, 0, 0, tzinfo=UTC),
    )

    mock_client = MagicMock()
    mock_client.list_executions.return_value = [older_ok, newer_fail]

    mock_run_mod = MagicMock()
    mock_run_mod.ExecutionsClient.return_value = mock_client

    with patch.object(cloud_run_job_failure_watcher.importlib, "import_module", return_value=mock_run_mod):
        reader = cloud_run_job_failure_watcher.make_job_execution_reader("proj", "prd")
    result = reader("live-event-log-compactor")

    assert result is not None
    assert result["failed_count"] == 2
    assert result["job_name"] == "prd-live-event-log-compactor"
    assert "completion_age_min" in result


def test_make_reader_returns_none_when_latest_is_success_despite_older_failure() -> None:
    from datetime import UTC, datetime

    older_fail = _FakeExecution(
        succeeded_count=0, failed_count=1, completion_time=datetime(2026, 8, 6, 8, 0, 0, tzinfo=UTC)
    )
    newer_ok = _FakeExecution(
        succeeded_count=1,
        failed_count=0,
        completion_time=datetime(2026, 8, 7, 12, 0, 0, tzinfo=UTC),
    )

    mock_client = MagicMock()
    mock_client.list_executions.return_value = [older_fail, newer_ok]

    mock_run_mod = MagicMock()
    mock_run_mod.ExecutionsClient.return_value = mock_run_mod
    mock_run_mod.ExecutionsClient.return_value = mock_client

    with patch.object(cloud_run_job_failure_watcher.importlib, "import_module", return_value=mock_run_mod):
        reader = cloud_run_job_failure_watcher.make_job_execution_reader("proj", "prd")
    result = reader("live-event-log-compactor")

    assert result is None, "most recent execution succeeded — should not alert"


def test_make_reader_returns_none_when_no_completions() -> None:
    in_progress = _FakeExecution(succeeded_count=0, failed_count=0, completion_time=None)

    mock_client = MagicMock()
    mock_client.list_executions.return_value = [in_progress]

    mock_run_mod = MagicMock()
    mock_run_mod.ExecutionsClient.return_value = mock_client

    with patch.object(cloud_run_job_failure_watcher.importlib, "import_module", return_value=mock_run_mod):
        reader = cloud_run_job_failure_watcher.make_job_execution_reader("proj", "prd")
    assert reader("my-job") is None


def test_make_reader_returns_none_on_api_error() -> None:
    mock_client = MagicMock()
    mock_client.list_executions.side_effect = Exception("API error")

    mock_run_mod = MagicMock()
    mock_run_mod.ExecutionsClient.return_value = mock_client

    with patch.object(cloud_run_job_failure_watcher.importlib, "import_module", return_value=mock_run_mod):
        reader = cloud_run_job_failure_watcher.make_job_execution_reader("proj", "prd")
    result = reader("my-job")

    assert result is None, "API errors must fail toward silence (None)"


def test_make_reader_no_env_prefix_uses_stem_as_job_name() -> None:
    from datetime import UTC, datetime

    fail_exec = _FakeExecution(
        succeeded_count=0, failed_count=1, completion_time=datetime(2026, 8, 7, 10, 0, tzinfo=UTC)
    )
    mock_client = MagicMock()
    mock_client.list_executions.return_value = [fail_exec]
    mock_run_mod = MagicMock()
    mock_run_mod.ExecutionsClient.return_value = mock_client

    with patch.object(cloud_run_job_failure_watcher.importlib, "import_module", return_value=mock_run_mod):
        reader = cloud_run_job_failure_watcher.make_job_execution_reader("proj", env_prefix="")
    result = reader("my-standalone-job")

    assert result is not None
    assert result["job_name"] == "my-standalone-job"


def test_make_reader_protobuf_timestamp_coercion() -> None:
    """Ensure ToDatetime() is called when completion_time is not a datetime instance."""
    from datetime import UTC, datetime

    proto_ts = _FakeCompletionTime(dt=datetime(2026, 8, 7, 9, 0, 0, tzinfo=UTC))
    fail_exec = _FakeExecution(succeeded_count=0, failed_count=1, completion_time=proto_ts)

    mock_client = MagicMock()
    mock_client.list_executions.return_value = [fail_exec]
    mock_run_mod = MagicMock()
    mock_run_mod.ExecutionsClient.return_value = mock_client

    with patch.object(cloud_run_job_failure_watcher.importlib, "import_module", return_value=mock_run_mod):
        reader = cloud_run_job_failure_watcher.make_job_execution_reader("proj", "prd")
    result = reader("my-job")

    assert result is not None
    assert result["failed_count"] == 1
