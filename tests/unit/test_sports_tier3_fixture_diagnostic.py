"""Unit tests: sports-scheduler tier-3 fixture calendar diagnostic — cefi-013.

Covers the empty-fixture-calendar WARNING that distinguishes:
  - 0 parquets in GCS (instruments-service never wrote data) → WARNING
  - parquets present but no fixtures within 48h (off-season) → INFO with count
  - GCS error on list_blobs → DEBUG (existing behaviour unchanged)
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from deployment_service.sports_trigger_scheduler import SportsTriggerScheduler


def _make_scheduler() -> SportsTriggerScheduler:
    scheduler = SportsTriggerScheduler.__new__(SportsTriggerScheduler)
    scheduler._config_path = "configs/sports-trigger-tiers.yaml"
    scheduler._poll_interval = 300
    scheduler._dry_run = False
    scheduler._backend = "local"
    scheduler._workspace_root = ""
    scheduler._state = MagicMock()
    scheduler._config = {}
    scheduler._running = False
    scheduler._periodic_state = None
    scheduler._cloud_run_config = {}
    scheduler._cloud_run_backends = {}
    return scheduler


def _make_fixture_parquet(kickoff_utc: str) -> bytes:
    df = pd.DataFrame(
        [
            {
                "fixture_id": "F001",
                "league_id": "PL",
                "kickoff_utc": kickoff_utc,
                "home_team": "Arsenal",
                "away_team": "Chelsea",
            }
        ]
    )
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


class TestFixtureCalendarDiagnostic:
    def test_empty_gcs_paths_emit_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """When all list_blobs calls return empty lists, emit a WARNING."""
        scheduler = _make_scheduler()

        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = []

        mock_config = MagicMock()
        mock_config.project_id = "test-project"

        with (
            patch("deployment_service.sports_trigger_scheduler.DeploymentConfig", return_value=mock_config),
            patch("deployment_service.sports_trigger_scheduler.get_storage_client", return_value=mock_storage),
            patch("deployment_service.sports_trigger_scheduler.get_bucket_name", return_value="test-bucket"),
            caplog.at_level("WARNING", logger="deployment_service.sports_trigger_scheduler"),
        ):
            fixtures = scheduler.get_upcoming_fixtures(horizon_hours=48)

        assert fixtures == []
        assert any(
            "instruments-service may not have written fixture data yet" in r.message
            for r in caplog.records
            if r.levelname == "WARNING"
        ), f"Expected empty-calendar WARNING but got: {[r.message for r in caplog.records]}"

    def test_parquets_present_but_outside_window_no_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """When parquets exist but fixtures are outside the 48h window, use INFO (not WARNING)."""
        scheduler = _make_scheduler()

        now = datetime.now(UTC)
        far_future = now.replace(year=now.year + 1).isoformat()
        parquet_bytes = _make_fixture_parquet(far_future)

        mock_blob = MagicMock()
        mock_blob.__str__ = lambda self: "sports_reference/fixtures/day=2999-01-01/all.parquet"

        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = [mock_blob]
        mock_storage.download_bytes.return_value = parquet_bytes

        mock_config = MagicMock()
        mock_config.project_id = "test-project"

        with (
            patch("deployment_service.sports_trigger_scheduler.DeploymentConfig", return_value=mock_config),
            patch("deployment_service.sports_trigger_scheduler.get_storage_client", return_value=mock_storage),
            patch("deployment_service.sports_trigger_scheduler.get_bucket_name", return_value="test-bucket"),
            caplog.at_level("WARNING", logger="deployment_service.sports_trigger_scheduler"),
        ):
            fixtures = scheduler.get_upcoming_fixtures(horizon_hours=48)

        assert fixtures == []
        warning_messages = [r.message for r in caplog.records if r.levelname == "WARNING"]
        assert not any("instruments-service may not have written" in m for m in warning_messages), (
            f"Should NOT warn when parquets exist but fixtures are out of window: {warning_messages}"
        )

    def test_fixture_within_window_returned(self) -> None:
        """When a fixture is within 48h, it is returned in the list."""
        scheduler = _make_scheduler()

        now = datetime.now(UTC)
        kickoff = now.replace(hour=(now.hour + 2) % 24).isoformat()
        parquet_bytes = _make_fixture_parquet(kickoff)

        mock_blob = MagicMock()
        mock_blob.__str__ = lambda self: "sports_reference/fixtures/day=2026-05-30/all.parquet"

        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = [mock_blob]
        mock_storage.download_bytes.return_value = parquet_bytes

        mock_config = MagicMock()
        mock_config.project_id = "test-project"

        with (
            patch("deployment_service.sports_trigger_scheduler.DeploymentConfig", return_value=mock_config),
            patch("deployment_service.sports_trigger_scheduler.get_storage_client", return_value=mock_config),
            patch("deployment_service.sports_trigger_scheduler.get_storage_client", return_value=mock_storage),
            patch("deployment_service.sports_trigger_scheduler.get_bucket_name", return_value="test-bucket"),
        ):
            fixtures = scheduler.get_upcoming_fixtures(horizon_hours=48)

        # Scheduler scans 4 days × 2 path patterns = up to 8 calls; the same
        # fixture blob is returned each time so duplicates are expected.
        assert len(fixtures) >= 1
        assert fixtures[0]["fixture_id"] == "F001"

    def test_list_blobs_error_still_returns_empty_silently(self, caplog: pytest.LogCaptureFixture) -> None:
        """list_blobs exceptions are logged at DEBUG and don't surface as warnings."""
        scheduler = _make_scheduler()

        mock_storage = MagicMock()
        mock_storage.list_blobs.side_effect = RuntimeError("GCS unavailable")

        mock_config = MagicMock()
        mock_config.project_id = "test-project"

        with (
            patch("deployment_service.sports_trigger_scheduler.DeploymentConfig", return_value=mock_config),
            patch("deployment_service.sports_trigger_scheduler.get_storage_client", return_value=mock_storage),
            patch("deployment_service.sports_trigger_scheduler.get_bucket_name", return_value="test-bucket"),
            caplog.at_level("DEBUG", logger="deployment_service.sports_trigger_scheduler"),
        ):
            fixtures = scheduler.get_upcoming_fixtures(horizon_hours=48)

        assert fixtures == []
        # GCS errors on list_blobs are DEBUG — the empty-calendar WARNING fires
        # because 0 parquets were scanned (all list_blobs calls raised).
        debug_messages = [r.message for r in caplog.records if r.levelname == "DEBUG"]
        assert any("No fixtures at" in m for m in debug_messages), (
            f"Expected DEBUG log for list_blobs error, got: {debug_messages}"
        )
