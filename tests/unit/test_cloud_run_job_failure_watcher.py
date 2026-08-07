"""Unit tests for DP-VM-013 — cloud_run_job_failure_watcher.

All tests are credential-free: the ``execution_reader`` callable is injected.
"""

from __future__ import annotations

import pytest

from deployment_service.data_pipeline_monitors import cloud_run_job_failure_watcher as watcher
from deployment_service.data_pipeline_monitors import meta_watchers


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tracker_with(
    counts: dict[str, int], *, log_bucket: str = "test-log"
) -> meta_watchers.MissTracker:
    """Build a MissTracker pre-seeded with the given miss counts (no GCS)."""
    from unittest.mock import MagicMock

    storage_client = MagicMock()
    t = meta_watchers.MissTracker(storage_client=storage_client, log_bucket=log_bucket)
    for key, val in counts.items():
        t._counts[key] = val  # noqa: SLF001
    return t


class _FakeTarget:
    def __init__(self, name: str) -> None:
        self.name = name


# ---------------------------------------------------------------------------
# _classify_failure_reason
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "reason,message,expected_prefix",
    [
        ("signal: 9", "", "OOM(signal: 9)"),
        ("", "OOMKilled", "OOM(oomkill)"),
        ("exit code 137", "container failed", "OOM(exit code 137)"),
        ("memory limit exceeded", "", "OOM(memory limit exceeded)"),
        ("", "", ""),
    ],
)
def test_classify_failure_reason(reason: str, message: str, expected_prefix: str) -> None:
    class _Cond:
        pass

    c = _Cond()
    c.reason = reason  # type: ignore[attr-defined]
    c.message = message  # type: ignore[attr-defined]
    result = watcher._classify_failure_reason([c])  # noqa: SLF001
    if expected_prefix:
        assert result.startswith(expected_prefix)
    else:
        assert result == ""


# ---------------------------------------------------------------------------
# check_cloud_run_job_executions — happy path (no failures)
# ---------------------------------------------------------------------------


def test_check_no_failures_returns_empty_dicts(monkeypatch: pytest.MonkeyPatch) -> None:
    stems = ["client-reporting-batch", "live-event-log-compactor"]
    fake_jobs = [_FakeTarget(s) for s in stems]

    import deployment_service.cloud_run_job_registry as reg

    monkeypatch.setattr(reg, "CLOUD_RUN_JOBS", fake_jobs)

    def _reader(_job_name: str) -> dict[str, object] | None:
        return None

    result = watcher.check_cloud_run_job_executions(execution_reader=_reader, dry_run=True)
    assert set(result.keys()) == set(stems)
    assert all(v == {} for v in result.values())


# ---------------------------------------------------------------------------
# check_cloud_run_job_executions — failure path, no MissTracker (immediate fire)
# ---------------------------------------------------------------------------


def test_check_failure_fires_without_miss_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_jobs = [_FakeTarget("client-reporting-batch")]

    import deployment_service.cloud_run_job_registry as reg

    monkeypatch.setattr(reg, "CLOUD_RUN_JOBS", fake_jobs)

    emitted: list[object] = []
    monkeypatch.setattr(watcher, "emit_finding", lambda f, **_kw: emitted.append(f))

    def _reader(_job: str) -> dict[str, object] | None:
        return {"failed_count": 2, "failure_reason": "OOM(signal: 9)", "completion_age_min": 5.0}

    result = watcher.check_cloud_run_job_executions(
        execution_reader=_reader,
        dry_run=True,
        miss_tracker=None,
    )
    assert len(emitted) == 1
    assert emitted[0].event == "DP_CLOUD_RUN_JOB_EXECUTION_FAILED"  # type: ignore[attr-defined]
    assert emitted[0].registry_id == "DP-VM-013"  # type: ignore[attr-defined]
    assert result["client-reporting-batch"]["failed_count"] == 2


# ---------------------------------------------------------------------------
# check_cloud_run_job_executions — MissTracker gates below threshold
# ---------------------------------------------------------------------------


def test_check_failure_below_consecutive_miss_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_jobs = [_FakeTarget("live-event-log-compactor")]

    import deployment_service.cloud_run_job_registry as reg

    monkeypatch.setattr(reg, "CLOUD_RUN_JOBS", fake_jobs)

    emitted: list[object] = []
    monkeypatch.setattr(watcher, "emit_finding", lambda f, **_kw: emitted.append(f))

    tracker = _tracker_with({})

    def _reader(_job: str) -> dict[str, object] | None:
        return {"failed_count": 1, "failure_reason": "", "completion_age_min": 10.0}

    result = watcher.check_cloud_run_job_executions(
        execution_reader=_reader,
        dry_run=True,
        miss_tracker=tracker,
        min_consecutive=2,
    )
    assert emitted == []
    assert result["live-event-log-compactor"] == {}


def test_check_failure_fires_at_consecutive_miss_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_jobs = [_FakeTarget("live-event-log-compactor")]

    import deployment_service.cloud_run_job_registry as reg

    monkeypatch.setattr(reg, "CLOUD_RUN_JOBS", fake_jobs)

    emitted: list[object] = []
    monkeypatch.setattr(watcher, "emit_finding", lambda f, **_kw: emitted.append(f))

    miss_key = watcher._job_miss_key("live-event-log-compactor")  # noqa: SLF001
    tracker = _tracker_with({miss_key: 1})

    def _reader(_job: str) -> dict[str, object] | None:
        return {"failed_count": 3, "failure_reason": "OOM(oomkill)", "completion_age_min": 2.0}

    result = watcher.check_cloud_run_job_executions(
        execution_reader=_reader,
        dry_run=True,
        miss_tracker=tracker,
        min_consecutive=2,
    )
    assert len(emitted) == 1
    assert result["live-event-log-compactor"]["failed_count"] == 3


# ---------------------------------------------------------------------------
# check_cloud_run_job_executions — consolidator family is skipped
# ---------------------------------------------------------------------------


def test_consolidator_family_is_excluded(monkeypatch: pytest.MonkeyPatch) -> None:
    stems = [
        "manifest-consolidator-market-data-cefi",
        "manifest-consolidator-market-data-defi",
        "client-reporting-batch",
    ]
    fake_jobs = [_FakeTarget(s) for s in stems]

    import deployment_service.cloud_run_job_registry as reg

    monkeypatch.setattr(reg, "CLOUD_RUN_JOBS", fake_jobs)

    called_with: list[str] = []

    def _reader(job_name: str) -> dict[str, object] | None:
        called_with.append(job_name)
        return {"failed_count": 1, "failure_reason": "", "completion_age_min": None}

    emitted: list[object] = []
    monkeypatch.setattr(watcher, "emit_finding", lambda f, **_kw: emitted.append(f))

    result = watcher.check_cloud_run_job_executions(
        execution_reader=_reader,
        dry_run=True,
        miss_tracker=None,
    )
    assert all("manifest-consolidator" not in j for j in called_with)
    assert "client-reporting-batch" in called_with
    assert len(emitted) == 1
    assert result.get("client-reporting-batch", {}).get("failed_count") == 1


# ---------------------------------------------------------------------------
# check_cloud_run_job_executions — env_prefix is applied to job names
# ---------------------------------------------------------------------------


def test_env_prefix_applied_to_job_name(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_jobs = [_FakeTarget("client-reporting-batch")]

    import deployment_service.cloud_run_job_registry as reg

    monkeypatch.setattr(reg, "CLOUD_RUN_JOBS", fake_jobs)

    called_with: list[str] = []

    def _reader(job_name: str) -> dict[str, object] | None:
        called_with.append(job_name)
        return None

    watcher.check_cloud_run_job_executions(
        execution_reader=_reader,
        env_prefix="prd-",
        dry_run=True,
    )
    assert called_with == ["prd-client-reporting-batch"]


# ---------------------------------------------------------------------------
# make_cloud_run_job_execution_reader — SDK import failure returns always-None reader
# ---------------------------------------------------------------------------


def test_reader_factory_returns_none_reader_on_import_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins

    original_import = builtins.__import__

    def _bad_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "google.cloud.run_v2":
            raise ImportError("SDK not available")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _bad_import)

    reader = watcher.make_cloud_run_job_execution_reader(project_id="proj")
    result = reader("some-job")
    assert result is None


def test_reader_factory_returns_none_when_no_project_id() -> None:
    reader = watcher.make_cloud_run_job_execution_reader(project_id="")
    result = reader("some-job")
    assert result is None
