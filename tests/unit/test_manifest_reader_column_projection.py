"""
Regression tests pinning the ``read_availability_index(columns=...)`` projection
on every ``ManifestReader`` call site.

A bare ``read_availability_index(bucket)`` call (no ``columns=``) decodes the
WHOLE consolidated availability index into a pandas DataFrame — 1.58 GB on disk
for the defi prod index — a real OOM risk on memory-constrained Cloud Run jobs
/ VMs (same incident class as ``mtds_backfill_vm_startup_oom_rc137_2026_07_14``).
These tests pin the exact ``columns=`` list each call site passes so a future
edit can't silently drop back to a bare, unprojected call.

See: unified-trading-pm/plans/active/issues/read_availability_index_bare_defi_callers_2026_07_27.md
"""

from unittest.mock import patch

import pandas as pd
import pytest

from deployment_service.cli.utils.manifest_reader import ManifestReader


class TestManifestReaderColumnProjection:
    @pytest.mark.unit
    def test_is_available_is_column_projected(self):
        reader = ManifestReader()
        with (
            patch(
                "deployment_service.cli.utils.manifest_reader.get_bucket_name",
                return_value="instruments-store-cefi-prd-fake",
            ),
            patch(
                "deployment_service.cli.utils.manifest_reader.read_availability_index",
                return_value=pd.DataFrame(),
            ) as mock_read,
        ):
            reader.is_available()

        mock_read.assert_called_once_with("instruments-store-cefi-prd-fake", columns=["date"])

    @pytest.mark.unit
    def test_get_completion_is_column_projected(self):
        reader = ManifestReader()
        with (
            patch.object(ManifestReader, "_resolve_all_buckets", return_value=["bucket-a"]),
            patch(
                "deployment_service.cli.utils.manifest_reader.read_availability_index",
                return_value=pd.DataFrame(),
            ) as mock_read,
        ):
            reader.get_completion(
                service="market-tick-data-service",
                asset_group="DEFI",
                start_date="2026-07-01",
                end_date="2026-07-02",
            )

        mock_read.assert_called_once_with("bucket-a", columns=["date", "service_name", "venue"])

    @pytest.mark.unit
    def test_get_manifest_status_is_column_projected(self):
        reader = ManifestReader()
        with (
            patch.object(ManifestReader, "_resolve_all_buckets", return_value=["bucket-a"]),
            patch(
                "deployment_service.cli.utils.manifest_reader.read_availability_index",
                return_value=pd.DataFrame(),
            ) as mock_read,
        ):
            reader.get_manifest_status(
                service="market-tick-data-service",
                start_date="2026-07-01",
                end_date="2026-07-02",
                asset_groups=["DEFI"],
            )

        mock_read.assert_called_once_with("bucket-a", columns=["date", "venue", "service_name", "league_id"])

    @pytest.mark.unit
    def test_get_venue_detail_scan_path_is_column_projected(self):
        reader = ManifestReader()
        with (
            patch.object(ManifestReader, "_resolve_bucket", return_value="bucket-a"),
            patch("deployment_service.cli.utils.manifest_reader.get_storage_client"),
            patch(
                "deployment_service.cli.utils.manifest_reader.read_availability_index",
                return_value=pd.DataFrame(),
            ) as mock_read,
        ):
            # date=None (default) exercises the "scan for latest date" path —
            # the only path that reads the availability index.
            reader.get_venue_detail(service="market-tick-data-service", asset_group="DEFI", venue="UNISWAP")

        mock_read.assert_called_once_with("bucket-a", columns=["venue", "date"])

    @pytest.mark.unit
    def test_get_coverage_summary_is_column_projected(self):
        reader = ManifestReader()
        with (
            patch.object(ManifestReader, "_resolve_all_buckets", return_value=["bucket-a"]),
            patch(
                "deployment_service.cli.utils.manifest_reader.read_availability_index",
                return_value=pd.DataFrame(),
            ) as mock_read,
        ):
            reader.get_coverage_summary(service="market-tick-data-service", asset_groups=["DEFI"])

        mock_read.assert_called_once_with("bucket-a", columns=["date", "venue", "instrument_count"])
