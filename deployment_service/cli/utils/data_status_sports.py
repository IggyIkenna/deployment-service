"""Sports fixture-based data status display.

For sports data, the denominator is fixture count per league per date (not calendar days).
A date with no fixtures for a league is "expected absence", not "missing data".

Reads fixture calendar from GCS to determine expected fixture counts,
and uses UAC transfer_windows to determine if transfer data is expected.
"""

import json
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import click
from unified_api_contracts.sports import is_transfer_window_open
from unified_trading_library import resolve_bucket_name

from ...cloud_client import CloudClient

logger = logging.getLogger(__name__)

# Sports fixture path templates. The bucket is resolved via the cloud-providers.yaml
# SSOT (``resolve_bucket_name`` → env-tiered ``instruments-store-sports-prd-{pid}``);
# the legacy no-env ``instruments-store-sports-{pid}`` bucket is DELETED at cutover, so
# never hardcode it. The probe chain mirrors UAC ``candidate_parquet_paths()``:
#   1. pipeline_mode-aware canonical path (migrated data),
#   2. bare ``entity=fixtures`` (pre-migration canonical shape — still present 2018-2025),
#   3. post-2026-07-14 writer-cutover ``entity=fixtures_schedule`` split,
#   4. oldest legacy ``sports_reference/fixtures/`` path.
_FIXTURES_CANONICAL_PREFIX = "sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/entity=fixtures/"
_FIXTURES_PREFIX = "sports_reference/by_date/day={date}/entity=fixtures/"
# Post-cutover writer shape (2026-07-14+, instruments-service
# sports_fixtures.py): entity=fixtures split into fixtures_schedule/
# fixtures_outcomes with NO legacy dual-write. fixtures_schedule alone
# covers every fixture (played or not) so it's the equivalent presence
# marker to the old singleton entity=fixtures.
_FIXTURES_SCHEDULE_PREFIX = (
    "sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/entity=fixtures_schedule/"
)
_FIXTURES_LEGACY_PREFIX = "sports_reference/fixtures/day={date}/"


def _load_fixture_counts_for_date(
    cloud_client: CloudClient,
    bucket_name: str,
    date_str: str,
) -> dict[str, int]:
    """Load fixture counts per league for a single date from GCS.

    Probes the canonical pipeline_mode-aware fixtures path first, then the bare
    ``entity=fixtures`` shape, then the post-cutover ``entity=fixtures_schedule``
    split, then the oldest legacy path. Returns mapping of league_id -> fixture_count.
    """
    league_counts: dict[str, int] = defaultdict(int)

    # Canonical pipeline_mode-aware fixtures path (migrated data).
    canonical_prefix = _FIXTURES_CANONICAL_PREFIX.format(date=date_str)
    _scan_fixture_prefix(cloud_client, bucket_name, canonical_prefix, league_counts)

    # Bare entity=fixtures (pre-migration canonical shape).
    if not league_counts:
        bare_prefix = _FIXTURES_PREFIX.format(date=date_str)
        _scan_fixture_prefix(cloud_client, bucket_name, bare_prefix, league_counts)

    # Post-cutover split-entity path (entity=fixtures_schedule).
    if not league_counts:
        schedule_prefix = _FIXTURES_SCHEDULE_PREFIX.format(date=date_str)
        _scan_fixture_prefix(cloud_client, bucket_name, schedule_prefix, league_counts)

    # If still nothing found, try oldest legacy path.
    if not league_counts:
        legacy_prefix = _FIXTURES_LEGACY_PREFIX.format(date=date_str)
        _scan_fixture_prefix(cloud_client, bucket_name, legacy_prefix, league_counts)
        if league_counts:
            # Greppable/aggregatable marker (findings:
            # sports_legacy_duplicate_triage_2026_07_22.md § Part 4) — this is a
            # confirmed live reader of the legacy flat sports_reference/fixtures/
            # path, the SOLE data source for the ~478 post-floor rows with no
            # canonical twin at all. Logged at WARNING so real hit frequency is
            # measurable before any future delete reconsideration.
            logger.warning(
                "LEGACY_FLAT_PATH_HIT data_status_sports date=%s: %d league(s) resolved only via the oldest "
                "legacy sports_reference/fixtures/ prefix (no canonical/bare/schedule-split data found)",
                date_str,
                len(league_counts),
            )

    return dict(league_counts)


def _scan_fixture_prefix(
    cloud_client: CloudClient,
    bucket_name: str,
    prefix: str,
    league_counts: dict[str, int],
) -> None:
    """Scan a GCS prefix for league-partitioned fixture parquets.

    Populates league_counts with the number of fixture files per league.
    Fixture files are at: {prefix}league={league_id}/fixtures.parquet
    """
    if cloud_client.client is None:
        return
    bucket = cloud_client.client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix, max_results=5000))

    for blob in blobs:
        blob_name = blob.name
        if not blob_name.endswith(".parquet"):
            continue
        # Extract league_id from path: .../league={league_id}/...
        parts = blob_name.split("/")
        for part in parts:
            if part.startswith("league="):
                league_id = part[len("league=") :]
                league_counts[league_id] += 1
                break


def _load_fixture_calendar(
    cloud_client: CloudClient,
    bucket_name: str,
    date_strings: list[str],
    max_workers: int = 20,
) -> dict[str, dict[str, int]]:
    """Load fixture calendar for all dates in range.

    Returns mapping of date_str -> {league_id -> fixture_count}.
    """
    calendar: dict[str, dict[str, int]] = {}

    with ThreadPoolExecutor(max_workers=min(max_workers, len(date_strings))) as executor:
        futures = {
            executor.submit(_load_fixture_counts_for_date, cloud_client, bucket_name, d): d for d in date_strings
        }
        for future in as_completed(futures):
            date_str = futures[future]
            try:
                league_counts = future.result()
                calendar[date_str] = league_counts
            except (OSError, ValueError, RuntimeError) as exc:
                logger.warning("Failed to load fixture calendar for %s: %s", date_str, exc)
                calendar[date_str] = {}

    return calendar


def display_sports_league_breakdown(
    service: str,
    start_date: datetime,
    end_date: datetime,
    output: str,
    show_missing: bool,
) -> None:
    """Display sports data status with fixture-based denominators per league.

    Instead of counting calendar days, this uses fixture counts per league as
    the denominator. Dates with no fixtures for a league are expected absences.
    Transfer window status is also reported for transfer-related data types.
    """
    bucket_name = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")

    cloud_client = CloudClient()

    # Generate date list
    all_dates: list[str] = []
    current = start_date
    while current <= end_date:
        all_dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    click.echo()
    click.echo(f"SPORTS LEAGUE BREAKDOWN: {click.style(service, fg='cyan', bold=True)}")
    click.echo(
        f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({len(all_dates)} days)"
    )
    click.echo("=" * 70)
    click.echo()
    click.echo(
        click.style(
            "Loading fixture calendar from GCS (fixture-based denominator)...",
            dim=True,
        )
    )

    # Load fixture calendar: date -> {league_id -> fixture_count}
    fixture_calendar = _load_fixture_calendar(cloud_client, bucket_name, all_dates)

    # Collect all leagues seen across the date range
    all_leagues: set[str] = set()
    for league_counts in fixture_calendar.values():
        all_leagues.update(league_counts.keys())

    if not all_leagues:
        click.echo(
            click.style(
                "No fixture data found in GCS for the given date range.",
                fg="red",
            )
        )
        return

    click.echo(f"Found {len(all_leagues)} leagues across {len(all_dates)} dates.")
    click.echo()

    # Now check actual data files per league per date. For instruments-service sports,
    # canonical data lives at:
    #   sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/entity=fixtures/league={league_id}/
    #   (bare entity=fixtures/ and entity=fixtures_schedule/ are the fallback shapes).
    # We check against the fixture calendar denominator.

    click.echo(click.style("Checking data completeness per league...", dim=True))

    # Build per-league results
    league_results: dict[str, _LeagueStatus] = {}
    for league_id in sorted(all_leagues):
        league_results[league_id] = _check_league_status(
            league_id=league_id,
            all_dates=all_dates,
            fixture_calendar=fixture_calendar,
            cloud_client=cloud_client,
            bucket_name=bucket_name,
        )

    click.echo()

    # Display results
    if output == "json":
        _display_json(league_results, service, start_date, end_date)
    elif output == "summary":
        _display_summary(league_results)
    else:
        _display_tree(league_results, show_missing)

    # Show transfer window info
    _display_transfer_window_info(all_dates, sorted(all_leagues))


class _LeagueStatus:
    """Status tracking for a single league across a date range."""

    __slots__ = (
        "actual_fixture_dates",
        "expected_fixture_dates",
        "league_id",
        "missing_fixture_dates",
        "no_fixture_dates",
        "total_actual",
        "total_expected",
    )

    def __init__(self, league_id: str) -> None:
        self.league_id = league_id
        self.expected_fixture_dates: list[str] = []
        self.actual_fixture_dates: list[str] = []
        self.missing_fixture_dates: list[str] = []
        self.no_fixture_dates: list[str] = []
        self.total_expected: int = 0
        self.total_actual: int = 0

    @property
    def completion_pct(self) -> float:
        if self.total_expected == 0:
            return 100.0
        return (self.total_actual / self.total_expected) * 100


def _check_league_status(
    league_id: str,
    all_dates: list[str],
    fixture_calendar: dict[str, dict[str, int]],
    cloud_client: CloudClient,
    bucket_name: str,
) -> _LeagueStatus:
    """Check data completeness for a single league across all dates.

    Uses fixture calendar as the denominator: only dates with fixtures expected
    count towards the total. Dates with no fixtures are expected absences.
    """
    status = _LeagueStatus(league_id)

    for date_str in all_dates:
        date_fixtures = fixture_calendar.get(date_str, {})
        fixture_count = date_fixtures.get(league_id, 0)

        if fixture_count == 0:
            # No fixtures expected for this league on this date
            status.no_fixture_dates.append(date_str)
            continue

        # Fixtures expected -- check if data file exists
        status.expected_fixture_dates.append(date_str)
        status.total_expected += 1

        # Check if fixture data exists in GCS for this league on this date.
        if cloud_client.client is None:
            continue
        bucket = cloud_client.client.bucket(bucket_name)

        # Canonical pipeline_mode-aware fixtures path (migrated data).
        canonical_prefix = (
            f"sports_reference/by_date/day={date_str}/"
            f"pipeline_mode=batch_api_football/entity=fixtures/league={league_id}/"
        )
        blobs = list(bucket.list_blobs(prefix=canonical_prefix, max_results=1))
        has_data = any(b.name.endswith(".parquet") for b in blobs)

        if not has_data:
            # Bare entity=fixtures (pre-migration canonical shape).
            bare_prefix = f"sports_reference/by_date/day={date_str}/entity=fixtures/league={league_id}/"
            bare_blobs = list(bucket.list_blobs(prefix=bare_prefix, max_results=1))
            has_data = any(b.name.endswith(".parquet") for b in bare_blobs)

        if not has_data:
            # Post-cutover split-entity path (entity=fixtures_schedule).
            schedule_prefix = (
                f"sports_reference/by_date/day={date_str}/"
                f"pipeline_mode=batch_api_football/entity=fixtures_schedule/league={league_id}/"
            )
            schedule_blobs = list(bucket.list_blobs(prefix=schedule_prefix, max_results=1))
            has_data = any(b.name.endswith(".parquet") for b in schedule_blobs)

        if not has_data:
            # Try oldest legacy path.
            legacy_prefix = f"sports_reference/fixtures/day={date_str}/league={league_id}/"
            legacy_blobs = list(bucket.list_blobs(prefix=legacy_prefix, max_results=1))
            has_data = any(b.name.endswith(".parquet") for b in legacy_blobs)
            if has_data:
                # Same greppable marker as _load_fixture_counts_for_date's legacy
                # branch (findings: sports_legacy_duplicate_triage_2026_07_22.md §
                # Part 4) — this per-league check independently duplicates the
                # legacy-path fallback logic, so it needs its own instrumentation.
                logger.warning(
                    "LEGACY_FLAT_PATH_HIT data_status_sports league=%s date=%s: resolved only via the oldest "
                    "legacy sports_reference/fixtures/ path (no canonical/bare/schedule-split object found)",
                    league_id,
                    date_str,
                )

        if has_data:
            status.actual_fixture_dates.append(date_str)
            status.total_actual += 1
        else:
            status.missing_fixture_dates.append(date_str)

    return status


def _display_tree(
    league_results: dict[str, _LeagueStatus],
    show_missing: bool,
) -> None:
    """Display tree-format output for sports league breakdown."""
    # Overall stats
    total_expected = sum(s.total_expected for s in league_results.values())
    total_actual = sum(s.total_actual for s in league_results.values())
    total_no_fixture = sum(len(s.no_fixture_dates) for s in league_results.values())
    overall_pct = (total_actual / total_expected * 100) if total_expected > 0 else 100.0
    overall_icon = "PASS" if overall_pct == 100 else ("PARTIAL" if overall_pct > 0 else "FAIL")
    overall_color = "green" if overall_pct == 100 else ("yellow" if overall_pct > 0 else "red")

    click.echo(
        f"[{overall_icon}] Overall: "
        + click.style(f"{overall_pct:.1f}%", fg=overall_color)
        + f" ({total_actual}/{total_expected} fixture-dates)"
    )
    click.echo(
        click.style(
            f"  ({total_no_fixture} league-dates skipped: no fixtures scheduled)",
            dim=True,
        )
    )
    click.echo()

    # Per-league breakdown
    sorted_leagues = sorted(
        league_results.items(),
        key=lambda kv: kv[1].completion_pct,
    )

    for i, (league_id, status) in enumerate(sorted_leagues):
        pct = status.completion_pct
        color = "green" if pct == 100 else ("yellow" if pct > 0 else "red")
        is_last = i == len(sorted_leagues) - 1
        prefix = "  |-- " if not is_last else "  `-- "

        click.echo(
            f"{prefix}{click.style(league_id, fg=color, bold=True)}: "
            f"{status.total_actual}/{status.total_expected} fixtures "
            f"({pct:.0f}%)"
            + (
                click.style(
                    f" [{len(status.no_fixture_dates)} days: no fixtures]",
                    dim=True,
                )
                if status.no_fixture_dates
                else ""
            )
        )

        if show_missing and status.missing_fixture_dates:
            missing = sorted(status.missing_fixture_dates)
            if len(missing) <= 5:
                for d in missing:
                    click.echo(f"  |       missing: {d}")
            else:
                for d in missing[:3]:
                    click.echo(f"  |       missing: {d}")
                click.echo(f"  |       ... and {len(missing) - 3} more")


def _display_summary(
    league_results: dict[str, _LeagueStatus],
) -> None:
    """Display summary-format output for sports league breakdown."""
    total_expected = sum(s.total_expected for s in league_results.values())
    total_actual = sum(s.total_actual for s in league_results.values())
    overall_pct = (total_actual / total_expected * 100) if total_expected > 0 else 100.0

    click.echo()
    click.echo(f"Overall: {overall_pct:.1f}% ({total_actual}/{total_expected} fixture-dates)")
    click.echo()
    for league_id in sorted(league_results.keys()):
        status = league_results[league_id]
        pct = status.completion_pct
        click.echo(f"  {league_id}: {pct:.1f}% ({status.total_actual}/{status.total_expected})")


def _display_json(
    league_results: dict[str, _LeagueStatus],
    service: str,
    start_date: datetime,
    end_date: datetime,
) -> None:
    """Display JSON-format output for sports league breakdown."""
    total_expected = sum(s.total_expected for s in league_results.values())
    total_actual = sum(s.total_actual for s in league_results.values())

    leagues_json: dict[str, dict[str, object]] = {}
    for league_id, status in sorted(league_results.items()):
        leagues_json[league_id] = {
            "completion_percent": round(status.completion_pct, 1),
            "expected_fixture_dates": status.total_expected,
            "actual_fixture_dates": status.total_actual,
            "missing_dates": status.missing_fixture_dates,
            "no_fixture_dates_count": len(status.no_fixture_dates),
        }

    result: dict[str, object] = {
        "service": service,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "denominator": "fixture_count",
        "overall_completion": round((total_actual / total_expected * 100) if total_expected > 0 else 100.0, 1),
        "overall_expected": total_expected,
        "overall_actual": total_actual,
        "leagues": leagues_json,
    }

    click.echo(json.dumps(result, indent=2))


def _display_transfer_window_info(
    all_dates: list[str],
    leagues: list[str],
) -> None:
    """Display transfer window status for the date range.

    Reports which leagues have open transfer windows, which affects
    whether transfer-related data types are expected.
    """
    click.echo()
    click.echo(click.style("TRANSFER WINDOW STATUS:", fg="cyan", bold=True))
    click.echo("-" * 70)

    has_any_window = False
    for league_id in leagues:
        open_dates: list[str] = []
        for date_str in all_dates:
            d = date.fromisoformat(date_str)
            if is_transfer_window_open(league_id, d):
                open_dates.append(date_str)

        if open_dates:
            has_any_window = True
            click.echo(
                f"  {league_id}: transfer window OPEN on"
                f" {len(open_dates)}/{len(all_dates)} dates"
                f" ({open_dates[0]} - {open_dates[-1]})"
            )
        else:
            click.echo(
                click.style(
                    f"  {league_id}: transfer window CLOSED (no transfer data expected)",
                    dim=True,
                )
            )

    if not has_any_window:
        click.echo(
            click.style(
                "  All transfer windows closed -- transfer data not expected.",
                dim=True,
            )
        )
