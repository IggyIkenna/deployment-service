"""Unit tests for cloud_run_job_exec_watcher (DP-VM-013).

Credential-free: Cloud Run execution I/O is injected via a stub reader.

Covers:
  - failed execution → DP_CLOUD_RUN_JOB_EXEC_FAILED WARN emitted
  - succeeded (None) → no alert
  - consecutive-miss gating: first sweep silent, second sweep fires
  - API unavailable (empty dict) → no false alert
  - dry_run → no event emitted
  - all_cloud_run_job_stems() returns a non-empty list matching the registry
"""

from __future__ import annotations

from deployment_service.data_pipeline_monitors import cloud_run_job_exec_watcher, meta_watchers
from deployment_service.data_pipeline_monitors._miss_tracker import (
    MissTracker,
)

# ── helpers ──────────────────────────────────────────────────────────────────


def _make_reader(
    diagnostics: dict[str, dict[str, object] | None],
) -> cloud_run_job_exec_watcher.JobExecFailedReader:
    """Stub reader returning the pre-built diagnostics dict."""
    return lambda _stems: dict(diagnostics)


class _FakeStorage:
    """Minimal in-memory StorageClient for MissTracker — no GCS reads needed."""

    def blob_exists(self, bucket: str, path: str) -> bool:
        return False

    def download_bytes(self, bucket: str, path: str) -> bytes:
        return b""

    def get_blob_metadata(self, bucket: str, path: str) -> None:
        return None

    def upload_bytes(
        self, bucket: str, path: str, data: bytes, content_type: str = "application/json"
    ) -> None:
        pass


def _make_miss_tracker() -> MissTracker:
    return MissTracker.load(storage_client=_FakeStorage(), log_bucket="test-bucket")


# ── test_failed_exec_emits_warn ───────────────────────────────────────────────


def test_failed_exec_emits_warn(monkeypatch: object) -> None:
    """Reader reports failed_count > 0 → DP_CLOUD_RUN_JOB_EXEC_FAILED WARN."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    reader = _make_reader({"live-event-log-compactor": {"failed_count": 3, "completion_age_min": 45.0}})
    results = cloud_run_job_exec_watcher.check_cloud_run_job_exec_failures(
        job_stems=["live-event-log-compactor"],
        execution_failed_reader=reader,
        dry_run=False,
    )
    assert results["live-event-log-compactor"].get("failed_count") == 3
    assert any(e[0] == "DP_CLOUD_RUN_JOB_EXEC_FAILED" and e[1] == "WARN" for e in emitted), emitted


# ── test_succeeded_exec_no_alert ─────────────────────────────────────────────


def test_succeeded_exec_no_alert(monkeypatch: object) -> None:
    """Reader returns None (most-recent execution succeeded) → no alert emitted."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[str] = []
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    reader = _make_reader({"dp-daily-digest": None})
    results = cloud_run_job_exec_watcher.check_cloud_run_job_exec_failures(
        job_stems=["dp-daily-digest"],
        execution_failed_reader=reader,
        dry_run=False,
    )
    assert results["dp-daily-digest"] == {}
    assert "DP_CLOUD_RUN_JOB_EXEC_FAILED" not in emitted


# ── test_consecutive_miss_gating ─────────────────────────────────────────────


def test_consecutive_miss_gating_first_sweep_silent(monkeypatch: object) -> None:
    """First failure → below min_consecutive=2 threshold → no alert yet."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[str] = []
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    tracker = _make_miss_tracker()
    reader = _make_reader({"client-reporting-update": {"failed_count": 1, "completion_age_min": 10.0}})
    results = cloud_run_job_exec_watcher.check_cloud_run_job_exec_failures(
        job_stems=["client-reporting-update"],
        execution_failed_reader=reader,
        dry_run=False,
        miss_tracker=tracker,
        min_consecutive=2,
    )
    assert results["client-reporting-update"] == {}
    assert "DP_CLOUD_RUN_JOB_EXEC_FAILED" not in emitted


def test_consecutive_miss_gating_second_sweep_fires(monkeypatch: object) -> None:
    """Two consecutive failures → meets min_consecutive=2 → alert fires."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    tracker = _make_miss_tracker()
    reader = _make_reader({"client-reporting-update": {"failed_count": 1, "completion_age_min": 10.0}})
    # Sweep 1 — silent
    cloud_run_job_exec_watcher.check_cloud_run_job_exec_failures(
        job_stems=["client-reporting-update"],
        execution_failed_reader=reader,
        dry_run=False,
        miss_tracker=tracker,
        min_consecutive=2,
    )
    # Sweep 2 — should fire
    results = cloud_run_job_exec_watcher.check_cloud_run_job_exec_failures(
        job_stems=["client-reporting-update"],
        execution_failed_reader=reader,
        dry_run=False,
        miss_tracker=tracker,
        min_consecutive=2,
    )
    assert results["client-reporting-update"].get("failed_count") == 1
    assert any(e[0] == "DP_CLOUD_RUN_JOB_EXEC_FAILED" for e in emitted), emitted


def test_miss_counter_resets_on_success(monkeypatch: object) -> None:
    """After a failure sweep, a success resets the miss counter."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[str] = []
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    tracker = _make_miss_tracker()
    fail_reader = _make_reader({"dp-heartbeat-watcher": {"failed_count": 2, "completion_age_min": 5.0}})
    ok_reader = _make_reader({"dp-heartbeat-watcher": None})

    # Failure sweep 1
    cloud_run_job_exec_watcher.check_cloud_run_job_exec_failures(
        job_stems=["dp-heartbeat-watcher"],
        execution_failed_reader=fail_reader,
        dry_run=False,
        miss_tracker=tracker,
        min_consecutive=2,
    )
    # Success sweep — resets counter
    cloud_run_job_exec_watcher.check_cloud_run_job_exec_failures(
        job_stems=["dp-heartbeat-watcher"],
        execution_failed_reader=ok_reader,
        dry_run=False,
        miss_tracker=tracker,
        min_consecutive=2,
    )
    # Another failure sweep — counter restarts at 1, still below threshold
    cloud_run_job_exec_watcher.check_cloud_run_job_exec_failures(
        job_stems=["dp-heartbeat-watcher"],
        execution_failed_reader=fail_reader,
        dry_run=False,
        miss_tracker=tracker,
        min_consecutive=2,
    )
    assert "DP_CLOUD_RUN_JOB_EXEC_FAILED" not in emitted


# ── test_api_error_no_false_alert ─────────────────────────────────────────────


def test_api_unavailable_empty_dict_no_false_alert(monkeypatch: object) -> None:
    """Reader returns empty dict (SDK import failed) → no false alert."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[str] = []
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    def _empty_reader(_stems: object) -> dict[str, dict[str, object] | None]:
        return {}

    results = cloud_run_job_exec_watcher.check_cloud_run_job_exec_failures(
        job_stems=["catalogue-regen-nightly"],
        execution_failed_reader=_empty_reader,
        dry_run=False,
    )
    # stem has no finding (reader returned nothing for it)
    assert "DP_CLOUD_RUN_JOB_EXEC_FAILED" not in emitted
    assert results.get("catalogue-regen-nightly") == {}


# ── test_dry_run ─────────────────────────────────────────────────────────────


def test_dry_run_does_not_emit(monkeypatch: object) -> None:
    """dry_run=True → finding is NOT routed/emitted even on confirmed failure."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[str] = []
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append(event),
    )
    reader = _make_reader({"batch-live-smoke-matrix-daily": {"failed_count": 5, "completion_age_min": 120.0}})
    cloud_run_job_exec_watcher.check_cloud_run_job_exec_failures(
        job_stems=["batch-live-smoke-matrix-daily"],
        execution_failed_reader=reader,
        dry_run=True,
    )
    assert "DP_CLOUD_RUN_JOB_EXEC_FAILED" not in emitted


# ── test_all_cloud_run_job_stems ─────────────────────────────────────────────


def test_all_cloud_run_job_stems_non_empty_and_contains_singletons() -> None:
    """all_cloud_run_job_stems() returns a non-empty list of stem strings."""
    stems = cloud_run_job_exec_watcher.all_cloud_run_job_stems()
    assert len(stems) > 0
    assert all(isinstance(s, str) and s for s in stems)
    # A few singletons that must always be registered
    assert "dp-meta-watchers" in stems
    assert "live-event-log-compactor" in stems
    assert "manifest-consolidator-cefi" in stems


def test_all_cloud_run_job_stems_no_duplicates() -> None:
    stems = cloud_run_job_exec_watcher.all_cloud_run_job_stems()
    assert len(stems) == len(set(stems)), "duplicate stems in CLOUD_RUN_JOBS"


# ── test_completion_age_none_handled ─────────────────────────────────────────


def test_completion_age_none_in_summary(monkeypatch: object) -> None:
    """completion_age_min=None does not crash the summary f-string."""
    meta_watchers.reset_emitted_tracker()
    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "deployment_service.data_pipeline_monitors.escalation.log_event",
        lambda event, severity="INFO", details=None: emitted.append((event, severity)),
    )
    reader = _make_reader({"tarball-cleanup": {"failed_count": 1, "completion_age_min": None}})
    results = cloud_run_job_exec_watcher.check_cloud_run_job_exec_failures(
        job_stems=["tarball-cleanup"],
        execution_failed_reader=reader,
        dry_run=False,
    )
    assert results["tarball-cleanup"].get("failed_count") == 1
    assert any(e[0] == "DP_CLOUD_RUN_JOB_EXEC_FAILED" for e in emitted)
