"""Regression tests: data_status_sports.py fixtures split-entity reader gap.

instruments-service cut the sports FIXTURES writer over to the
fixtures_schedule/fixtures_outcomes entity-folder split with no legacy
dual-write (2026-07-14+). Both fixture probes in this module only knew
about the legacy singleton `entity=fixtures` (and the oldest
`sports_reference/fixtures/day=` shape), so post-cutover dates silently
reported 0 fixtures — read as genuine expected-absence rather than a
writer-cutover artifact. See unified-trading-pm/plans/active/issues/
features_sports_fixtures_split_reader_gap_2026_07_15.md.
"""

from __future__ import annotations

import logging

import pytest

from deployment_service.cli.utils.data_status_sports import (
    _check_league_status,
    _load_fixture_counts_for_date,
)


class _FakeBlob:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeBucket:
    def __init__(self, blob_names: list[str]) -> None:
        self._blob_names = blob_names

    def list_blobs(self, prefix: str, max_results: int = 5000):
        return [_FakeBlob(n) for n in self._blob_names if n.startswith(prefix)]


class _FakeGCSClient:
    def __init__(self, blob_names: list[str]) -> None:
        self._bucket = _FakeBucket(blob_names)

    def bucket(self, _bucket_name: str) -> _FakeBucket:
        return self._bucket


class _FakeCloudClient:
    def __init__(self, blob_names: list[str]) -> None:
        self.client = _FakeGCSClient(blob_names)


class TestLoadFixtureCountsForDateSplitEntity:
    def test_legacy_singleton_entity_fixtures_still_counted(self) -> None:
        cloud_client = _FakeCloudClient(
            [
                "sports_reference/by_date/day=2026-07-13/entity=fixtures/league=UCL/fixtures.parquet",
            ]
        )
        counts = _load_fixture_counts_for_date(cloud_client, "bkt", "2026-07-13")  # type: ignore[arg-type]
        assert counts == {"UCL": 1}

    def test_post_cutover_split_entity_fixtures_schedule_counted(self) -> None:
        # 2026-07-14+: writer only emits entity=fixtures_schedule / entity=fixtures_outcomes,
        # no entity=fixtures at all — previously this returned {} (silent expected-absence).
        cloud_client = _FakeCloudClient(
            [
                "sports_reference/by_date/day=2026-07-14/pipeline_mode=batch_api_football/"
                "entity=fixtures_schedule/league=UCL/fixtures_schedule.parquet",
                "sports_reference/by_date/day=2026-07-14/pipeline_mode=batch_api_football/"
                "entity=fixtures_outcomes/league=UCL/fixtures_outcomes.parquet",
            ]
        )
        counts = _load_fixture_counts_for_date(cloud_client, "bkt", "2026-07-14")  # type: ignore[arg-type]
        assert counts == {"UCL": 1}

    def test_oldest_legacy_path_still_used_as_final_fallback(self) -> None:
        cloud_client = _FakeCloudClient(
            [
                "sports_reference/fixtures/day=2020-01-01/league=UCL/fixtures.parquet",
            ]
        )
        counts = _load_fixture_counts_for_date(cloud_client, "bkt", "2020-01-01")  # type: ignore[arg-type]
        assert counts == {"UCL": 1}

    def test_oldest_legacy_path_hit_logs_greppable_marker(self, caplog: pytest.LogCaptureFixture) -> None:
        """Legacy-path hits must log a distinct LEGACY_FLAT_PATH_HIT marker —
        the instrumentation asked for by sports_legacy_duplicate_triage_2026_07_22.md
        § Part 4 / sports_satellite_ao_dispatch_batch2_2026_07_24.md, since this
        reader is a confirmed live dependency (sole source for ~478 of 28,100
        post-floor rows), not a dead branch safe to silently remove."""
        cloud_client = _FakeCloudClient(
            [
                "sports_reference/fixtures/day=2020-01-01/league=UCL/fixtures.parquet",
            ]
        )
        with caplog.at_level(logging.WARNING):
            _load_fixture_counts_for_date(cloud_client, "bkt", "2020-01-01")  # type: ignore[arg-type]

        assert any("LEGACY_FLAT_PATH_HIT" in r.getMessage() for r in caplog.records), (
            f"expected a LEGACY_FLAT_PATH_HIT warning, got: {[r.getMessage() for r in caplog.records]}"
        )

    def test_canonical_hit_does_not_log_legacy_marker(self, caplog: pytest.LogCaptureFixture) -> None:
        """A canonical-path hit (no fallback needed) must NOT emit the legacy marker."""
        cloud_client = _FakeCloudClient(
            [
                "sports_reference/by_date/day=2026-07-13/entity=fixtures/league=UCL/fixtures.parquet",
            ]
        )
        with caplog.at_level(logging.WARNING):
            _load_fixture_counts_for_date(cloud_client, "bkt", "2026-07-13")  # type: ignore[arg-type]

        assert not any("LEGACY_FLAT_PATH_HIT" in r.getMessage() for r in caplog.records)

    def test_no_fixtures_anywhere_returns_empty(self) -> None:
        cloud_client = _FakeCloudClient([])
        counts = _load_fixture_counts_for_date(cloud_client, "bkt", "2026-07-14")  # type: ignore[arg-type]
        assert counts == {}


class TestCheckLeagueStatusSplitEntity:
    def test_post_cutover_league_data_detected_via_fixtures_schedule(self) -> None:
        date_str = "2026-07-14"
        league_id = "UCL"
        cloud_client = _FakeCloudClient(
            [
                f"sports_reference/by_date/day={date_str}/pipeline_mode=batch_api_football/"
                f"entity=fixtures_schedule/league={league_id}/fixtures_schedule.parquet",
            ]
        )
        fixture_calendar = {date_str: {league_id: 1}}

        status = _check_league_status(
            league_id=league_id,
            all_dates=[date_str],
            fixture_calendar=fixture_calendar,
            cloud_client=cloud_client,  # type: ignore[arg-type]
            bucket_name="bkt",
        )

        assert status.total_expected == 1
        # Before the fix this was 0 (missing) — the split-entity blob went
        # undetected by the legacy-only probes and read as missing data.
        assert status.total_actual == 1
        assert status.missing_fixture_dates == []

    def test_genuine_gap_still_reported_missing(self) -> None:
        date_str = "2026-07-14"
        league_id = "UCL"
        cloud_client = _FakeCloudClient([])  # no fixture data at all
        fixture_calendar = {date_str: {league_id: 1}}

        status = _check_league_status(
            league_id=league_id,
            all_dates=[date_str],
            fixture_calendar=fixture_calendar,
            cloud_client=cloud_client,  # type: ignore[arg-type]
            bucket_name="bkt",
        )

        assert status.total_expected == 1
        assert status.total_actual == 0
        assert status.missing_fixture_dates == [date_str]

    def test_legacy_path_hit_logs_greppable_marker(self, caplog: pytest.LogCaptureFixture) -> None:
        """_check_league_status independently duplicates the legacy-path fallback
        logic (a separate code path from _load_fixture_counts_for_date's), so it
        needs its own LEGACY_FLAT_PATH_HIT instrumentation too."""
        date_str = "2020-01-01"
        league_id = "UCL"
        cloud_client = _FakeCloudClient(
            [
                f"sports_reference/fixtures/day={date_str}/league={league_id}/fixtures.parquet",
            ]
        )
        fixture_calendar = {date_str: {league_id: 1}}

        with caplog.at_level(logging.WARNING):
            status = _check_league_status(
                league_id=league_id,
                all_dates=[date_str],
                fixture_calendar=fixture_calendar,
                cloud_client=cloud_client,  # type: ignore[arg-type]
                bucket_name="bkt",
            )

        assert status.total_actual == 1
        assert any("LEGACY_FLAT_PATH_HIT" in r.getMessage() for r in caplog.records), (
            f"expected a LEGACY_FLAT_PATH_HIT warning, got: {[r.getMessage() for r in caplog.records]}"
        )
