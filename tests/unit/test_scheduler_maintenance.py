"""Unit tests for the scheduler pause/resume maintenance-window wrapper.

The GCS-marker logic is exercised via an in-memory fake storage client (same convention
as ``unified-trading-library``'s ``test_maintenance_window.py``); the Cloud Scheduler
calls are exercised via injected fake pauser/resumer callables (mirrors the existing
``SchedulerStateReader`` injection pattern in ``meta_watchers.py``) — no real GCP
credentials, no network.
"""

from __future__ import annotations

import pytest
from unified_trading_library import MaintenanceWindowActiveError

from deployment_service.data_pipeline_monitors import scheduler_maintenance as sm


class _FakeStorage:
    """In-memory stand-in for the UTL StorageClient with generation semantics."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, int]] = {}
        self._gen = 0

    def _next_gen(self) -> int:
        self._gen += 1
        return self._gen

    def download_bytes_with_generation(self, bucket: str, path: str) -> tuple[bytes | None, int]:
        current = self.objects.get((bucket, path))
        if current is None:
            return None, 0
        return current

    def conditional_upload_bytes(
        self,
        bucket: str,
        path: str,
        data: bytes,
        *,
        if_generation_match: int,
        content_type: str | None = None,
    ) -> int | None:
        current = self.objects.get((bucket, path))
        current_gen = current[1] if current is not None else 0
        if if_generation_match != current_gen:
            return None
        gen = self._next_gen()
        self.objects[(bucket, path)] = (data, gen)
        return gen

    def conditional_delete_blob(self, bucket: str, path: str, *, if_generation_match: int) -> bool:
        current = self.objects.get((bucket, path))
        if current is None:
            return False
        if if_generation_match != current[1]:
            return False
        del self.objects[(bucket, path)]
        return True


@pytest.fixture
def fake_storage(monkeypatch: pytest.MonkeyPatch) -> _FakeStorage:
    store = _FakeStorage()
    monkeypatch.setattr(sm, "get_storage_client", lambda: store)
    return store


class _RecordingAction:
    """Fake ``SchedulerAction`` — records every job name it was called with."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, job_name: str) -> None:
        self.calls.append(job_name)


_BUCKET = "instruments-store-sports-prd-test-project"
_JOB = "prd-manifest-consolidator-instruments-sports-cron"


@pytest.mark.unit
class TestPauseForMaintenance:
    def test_acquires_window_and_pauses_jobs(self, fake_storage: _FakeStorage):
        pauser = _RecordingAction()
        window = sm.pause_for_maintenance(
            bucket=_BUCKET,
            surface="instruments-sports",
            scheduler_jobs=[_JOB],
            reason="CF-8 backfill",
            locked_by="slot-6",
            pauser=pauser,
        )
        assert window.locked_by == "slot-6"
        assert pauser.calls == [_JOB]
        # Window is now visibly readable.
        held = sm.maintenance_status(_BUCKET)
        assert held is not None
        assert held.reason == "CF-8 backfill"

    def test_refuses_when_another_holder_is_live(self, fake_storage: _FakeStorage):
        sm.pause_for_maintenance(
            bucket=_BUCKET,
            surface="instruments-sports",
            scheduler_jobs=[_JOB],
            reason="slot-3's backfill",
            locked_by="slot-3",
            pauser=_RecordingAction(),
        )
        pauser_b = _RecordingAction()
        with pytest.raises(MaintenanceWindowActiveError, match="slot-3"):
            sm.pause_for_maintenance(
                bucket=_BUCKET,
                surface="instruments-sports",
                scheduler_jobs=[_JOB],
                reason="slot-6's backfill",
                locked_by="slot-6",
                pauser=pauser_b,
            )
        # Refused BEFORE the pauser ever ran — never pauses a cron it can't safely account for.
        assert pauser_b.calls == []

    def test_pauses_multiple_jobs_in_order(self, fake_storage: _FakeStorage):
        pauser = _RecordingAction()
        sm.pause_for_maintenance(
            bucket=_BUCKET,
            surface="instruments-sports",
            scheduler_jobs=[_JOB, "prd-manifest-consolidator-market-data-sports-cron"],
            reason="both surfaces",
            locked_by="slot-6",
            pauser=pauser,
        )
        assert pauser.calls == [_JOB, "prd-manifest-consolidator-market-data-sports-cron"]


@pytest.mark.unit
class TestResumeAfterMaintenance:
    def test_releases_window_and_resumes_jobs(self, fake_storage: _FakeStorage):
        sm.pause_for_maintenance(
            bucket=_BUCKET,
            surface="instruments-sports",
            scheduler_jobs=[_JOB],
            reason="CF-8 backfill",
            locked_by="slot-6",
            pauser=_RecordingAction(),
        )
        resumer = _RecordingAction()
        cleared = sm.resume_after_maintenance(
            bucket=_BUCKET, scheduler_jobs=[_JOB], locked_by="slot-6", resumer=resumer
        )
        assert cleared is True
        assert resumer.calls == [_JOB]
        assert sm.maintenance_status(_BUCKET) is None

    def test_refuses_resume_by_a_different_holder_without_force(self, fake_storage: _FakeStorage):
        sm.pause_for_maintenance(
            bucket=_BUCKET,
            surface="instruments-sports",
            scheduler_jobs=[_JOB],
            reason="slot-3's backfill",
            locked_by="slot-3",
            pauser=_RecordingAction(),
        )
        resumer = _RecordingAction()
        with pytest.raises(MaintenanceWindowActiveError, match="slot-3"):
            sm.resume_after_maintenance(
                bucket=_BUCKET, scheduler_jobs=[_JOB], locked_by="operator-raw-gcloud", resumer=resumer
            )
        # Refused BEFORE the resumer ever ran (the cron stays paused).
        assert resumer.calls == []
        assert sm.maintenance_status(_BUCKET) is not None

    def test_force_overrides_a_foreign_holder(self, fake_storage: _FakeStorage):
        sm.pause_for_maintenance(
            bucket=_BUCKET,
            surface="instruments-sports",
            scheduler_jobs=[_JOB],
            reason="slot-3's backfill",
            locked_by="slot-3",
            pauser=_RecordingAction(),
        )
        resumer = _RecordingAction()
        cleared = sm.resume_after_maintenance(
            bucket=_BUCKET, scheduler_jobs=[_JOB], locked_by="operator", force=True, resumer=resumer
        )
        assert cleared is True
        assert resumer.calls == [_JOB]

    def test_resume_with_no_window_still_resumes_jobs(self, fake_storage: _FakeStorage):
        """No window held (e.g. it already expired) — resume still runs the real action;
        only the return value signals nothing was cleared."""
        resumer = _RecordingAction()
        cleared = sm.resume_after_maintenance(
            bucket=_BUCKET, scheduler_jobs=[_JOB], locked_by="slot-6", resumer=resumer
        )
        assert cleared is False
        assert resumer.calls == [_JOB]

    def test_partial_resume_failure_leaves_window_held(self, fake_storage: _FakeStorage):
        """Regression for the inverted-ordering bug: if ``act(job)`` raises partway through a
        multi-job resume, the window must stay HELD (not released) — otherwise a concurrent
        ``--status`` caller would see "clear" while jobs remain paused mid-resume."""
        job_2 = "prd-manifest-consolidator-market-data-sports-cron"
        sm.pause_for_maintenance(
            bucket=_BUCKET,
            surface="instruments-sports",
            scheduler_jobs=[_JOB, job_2],
            reason="CF-8 backfill",
            locked_by="slot-6",
            pauser=_RecordingAction(),
        )

        calls: list[str] = []

        def _flaky_resumer(job_name: str) -> None:
            calls.append(job_name)
            if job_name == job_2:
                raise RuntimeError("simulated Cloud Scheduler API error on job 2 of N")

        with pytest.raises(RuntimeError, match="simulated Cloud Scheduler API error"):
            sm.resume_after_maintenance(
                bucket=_BUCKET, scheduler_jobs=[_JOB, job_2], locked_by="slot-6", resumer=_flaky_resumer
            )

        assert calls == [_JOB, job_2]
        # The window is still live — a second caller's --status check reflects reality
        # instead of falsely reading "clear" while job_2 never actually resumed.
        held = sm.maintenance_status(_BUCKET)
        assert held is not None
        assert held.locked_by == "slot-6"


@pytest.mark.unit
class TestMaintenanceStatus:
    def test_status_none_when_no_window(self, fake_storage: _FakeStorage):
        assert sm.maintenance_status(_BUCKET) is None

    def test_status_reads_live_window(self, fake_storage: _FakeStorage):
        sm.pause_for_maintenance(
            bucket=_BUCKET,
            surface="instruments-sports",
            scheduler_jobs=[_JOB],
            reason="CF-8 backfill",
            locked_by="slot-6",
            pauser=_RecordingAction(),
        )
        window = sm.maintenance_status(_BUCKET)
        assert window is not None
        assert window.locked_by == "slot-6"
        assert list(window.scheduler_jobs) == [_JOB]


@pytest.mark.unit
class TestCli:
    def test_status_subcommand_no_window(self, fake_storage: _FakeStorage, capsys: pytest.CaptureFixture[str]):
        rc = sm._cli(["--bucket", _BUCKET, "status"])
        assert rc == 0
        assert "no live window" in capsys.readouterr().out

    def test_status_subcommand_shows_holder(self, fake_storage: _FakeStorage, capsys: pytest.CaptureFixture[str]):
        sm.pause_for_maintenance(
            bucket=_BUCKET,
            surface="instruments-sports",
            scheduler_jobs=[_JOB],
            reason="CF-8 backfill",
            locked_by="slot-6",
            pauser=_RecordingAction(),
        )
        rc = sm._cli(["--bucket", _BUCKET, "status"])
        assert rc == 1
        out = capsys.readouterr().out
        assert "slot-6" in out
        assert "CF-8 backfill" in out
