"""Data status command and related functions."""

import json
import logging
import sys
import time
from datetime import datetime
from typing import cast

import click

from ..utils.data_status_checkers import (
    check_data_types_detailed,
    check_feature_groups_detailed,
    check_timeframes_detailed,
)
from ..utils.data_status_display_dynamic import display_dynamic_service_status
from ..utils.data_status_display_fixed import display_fixed_service_status
from ..utils.data_status_extended import (
    run_live_freshness_check,
    run_ml_experiments_check,
    run_t1_check,
)
from ..utils.data_status_formatters import format_benchmark_info
from ..utils.data_status_sports import display_sports_league_breakdown
from ..utils.data_status_venue_utils import check_instruments_venue_coverage
from ..utils.manifest_reader import ManifestReader

logger = logging.getLogger(__name__)

# Services with dynamic GCS configs (completion % is not applicable)
DYNAMIC_DIMENSION_SERVICES = {
    "execution-service",
    "ml-service",
    "strategy-service",
}

# Fixed dimension services (can show completion %)
FIXED_DIMENSION_SERVICES = {
    "instruments-service",
    "corporate-actions",
    "market-tick-data-service",
    "market-data-processing-service",
    "features-delta-one-service",
    "features-volatility-service",
    "features-onchain-service",
    "features-sports-service",
    "features-cross-instrument-service",
    "features-multi-timeframe-service",
    "features-commodity-service",
    "features-calendar-service",
}


@click.command("data-status")
@click.option("--service", "-s", default=None, help="Service to check data status for")
@click.option(
    "--start-date",
    default=None,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date (YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    default=None,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date (YYYY-MM-DD)",
)
@click.option("--asset-group", "-c", multiple=True, help="Filter by asset group (default: all)")
@click.option("--venue", "-v", multiple=True, help="Filter by venue")
@click.option(
    "--output",
    "-o",
    type=click.Choice(["tree", "json", "summary"]),
    default="tree",
    help="Output format",
)
@click.option("--show-timestamps", "-t", is_flag=True, help="Show file timestamps (oldest/newest)")
@click.option("--show-missing", "-m", is_flag=True, help="List specific missing dates")
@click.option("--benchmark", "-b", is_flag=True, help="Show performance benchmark info")
@click.option(
    "--check-venues",
    is_flag=True,
    help="[instruments-service] Check venue coverage within parquet files",
)
@click.option(
    "--check-data-types",
    is_flag=True,
    help="[market-tick-data-service] Check per-data_type completion (detailed)",
)
@click.option(
    "--check-feature-groups",
    is_flag=True,
    help="[features-*-service] Check per-feature_group completion",
)
@click.option(
    "--check-timeframes",
    is_flag=True,
    help="[market-data-processing-service] Check per-timeframe completion",
)
@click.option(
    "--sports-league-breakdown",
    is_flag=True,
    help="[SPORTS] Use fixture-based denominator per league instead of calendar days",
)
@click.option(
    "--detailed",
    "-d",
    is_flag=True,
    help="Show detailed breakdown by data_type and timeframe",
)
@click.option(
    "--fast",
    is_flag=True,
    help="Use targeted date queries (faster for short date ranges)",
)
@click.option(
    "--mode",
    type=click.Choice(["batch", "live"], case_sensitive=False),
    default="batch",
    help=("Data path mode: batch (historical by_date/day=) or live (persisted live sink under live/ prefix)."),
)
@click.option(
    "--source",
    type=click.Choice(["manifest", "gcs", "auto"], case_sensitive=False),
    default="auto",
    envvar="DATA_STATUS_SOURCE",
    help="Data source: manifest (fast, from catalogue), gcs (slow, blob scanning), auto (try manifest first)",
)
@click.option(
    "--t1-check",
    type=str,
    default="",
    metavar="CLUSTER",
    help=(
        "T+1 check: verify yesterday's data for all services in a cluster."
        " Pass cluster name (cefi, tradfi, defi, sports, full)."
    ),
)
@click.option(
    "--ml-experiments",
    is_flag=True,
    help="List ML training experiments from GCS and check model metadata existence.",
)
@click.option(
    "--live-freshness",
    is_flag=True,
    help=(
        "Report live-mode data freshness (staleness) per asset group."
        " Flags stale if >15min for CeFi/DeFi, >6h for sports."
    ),
)
@click.pass_context
def data_status(
    ctx,
    service: str | None,
    start_date: datetime | None,
    end_date: datetime | None,
    asset_group: tuple[str, ...],
    venue: tuple[str, ...],
    output: str,
    show_timestamps: bool,
    show_missing: bool,
    benchmark: bool,
    check_venues: bool,
    check_data_types: bool,
    check_feature_groups: bool,
    check_timeframes: bool,
    sports_league_breakdown: bool,
    detailed: bool,
    fast: bool,
    mode: str,
    source: str,
    t1_check: str,
    ml_experiments: bool,
    live_freshness: bool,
):
    """
    Check data completion status for a service across a date range.

    OPTIMIZED for speed: Uses batch blob listing (1 API call per bucket instead
    of 1 per date) and parallel scanning. Can check years of data in seconds.

    Shows hierarchical breakdown of data completion by dimensions (asset group, venue, etc.)
    with percentages and optionally file timestamps.

    For dynamic-dimension services (execution-service, ml-service, strategy-service),
    only timestamp information is shown since completion % is not applicable.

    Examples:
        # Check instruments-service completion (1 day)
        data-status -s instruments-service --start-date 2024-01-01 --end-date 2024-01-01

        # Check 1 month (fast - batch scanning)
        data-status -s instruments-service --start-date 2024-01-01 --end-date 2024-01-31

        # Check 1 year with timestamps
        data-status -s instruments-service --start-date 2024-01-01 --end-date 2024-12-31 -t

        # Show missing dates
        data-status -s instruments-service --start-date 2024-01-01 --end-date 2024-12-31 -m

        # Benchmark performance
        data-status -s instruments-service --start-date 2024-01-01 --end-date 2024-12-31 -b

        # Check venue coverage within instruments parquet files (instruments-service only)
        data-status -s instruments-service --start-date 2024-01-01 --end-date 2024-01-31
          --check-venues

        # Sports fixture-based breakdown per league (denominator = fixture count, not days)
        data-status -s instruments-service --start-date 2024-01-01 --end-date 2024-01-31
          --sports-league-breakdown

        # T+1 check: verify yesterday's data across all services in a cluster
        data-status --t1-check full

        # List ML experiments and model metadata
        data-status --ml-experiments

        # Live freshness: check staleness of live-mode data
        data-status -s market-tick-data-service --live-freshness
    """
    config_dir = ctx.obj.get("config_dir", "configs")

    # ── Extended modes (t1-check, ml-experiments, live-freshness) ──
    # These short-circuit before requiring --service/--start-date/--end-date.
    if t1_check:
        run_t1_check(cluster_name=t1_check, config_dir=config_dir, output=output)
        return

    if ml_experiments:
        run_ml_experiments_check(output=output)
        return

    if live_freshness:
        if not service:
            click.echo(
                click.style("--live-freshness requires --service", fg="red"),
                err=True,
            )
            sys.exit(1)
        run_live_freshness_check(service=service, asset_group=asset_group, config_dir=config_dir, output=output)
        return

    # ── Standard mode: --service, --start-date, --end-date are required ──
    if not service:
        click.echo(click.style("--service is required", fg="red"), err=True)
        sys.exit(1)
    if not start_date:
        click.echo(click.style("--start-date is required", fg="red"), err=True)
        sys.exit(1)
    if not end_date:
        click.echo(click.style("--end-date is required", fg="red"), err=True)
        sys.exit(1)

    total_start = time.time()

    resolved_source = source
    used_manifest = False

    is_specialized_check = (
        check_venues or check_data_types or check_feature_groups or check_timeframes or sports_league_breakdown
    )
    if not is_specialized_check and resolved_source in ("manifest", "auto"):
        used_manifest = _try_manifest_source(
            service=service,
            start_date=start_date,
            end_date=end_date,
            asset_group=asset_group,
            output=output,
            show_missing=show_missing,
            resolved_source=resolved_source,
            benchmark=benchmark,
            total_start=total_start,
        )
        if used_manifest:
            logger.info("Data status source: %s (resolved: manifest)", resolved_source)
            return

    if resolved_source == "manifest" and not used_manifest and not is_specialized_check:
        click.echo(
            click.style("Manifest source unavailable or returned no data", fg="red"),
            err=True,
        )
        sys.exit(1)

    logger.info("Data status source: %s (resolved: gcs)", resolved_source)

    try:
        if check_venues:
            if service != "instruments-service":
                click.echo(
                    click.style(
                        "--check-venues is only supported for instruments-service",
                        fg="red",
                    ),
                    err=True,
                )
                sys.exit(1)
            check_instruments_venue_coverage(start_date, end_date, asset_group, output, config_dir)

        elif check_data_types:
            if service != "market-tick-data-service":
                click.echo(
                    click.style(
                        "--check-data-types is only supported for market-tick-data-service",
                        fg="red",
                    ),
                    err=True,
                )
                sys.exit(1)
            check_data_types_detailed(start_date, end_date, asset_group, venue, config_dir, output)

        elif check_feature_groups:
            # Feature groups check for features-*-service
            feature_services = [
                "features-delta-one-service",
                "features-volatility-service",
                "features-onchain-service",
                "features-calendar-service",
            ]
            if service not in feature_services:
                click.echo(
                    click.style(
                        f"--check-feature-groups is only supported for: {', '.join(feature_services)}",
                        fg="red",
                    ),
                    err=True,
                )
                sys.exit(1)
            check_feature_groups_detailed(service, start_date, end_date, asset_group, config_dir, output)

        elif check_timeframes:
            if service != "market-data-processing-service":
                click.echo(
                    click.style(
                        "--check-timeframes is only supported for market-data-processing-service",
                        fg="red",
                    ),
                    err=True,
                )
                sys.exit(1)
            check_timeframes_detailed(start_date, end_date, asset_group, venue, config_dir, output)

        elif sports_league_breakdown:
            # Sports-specific: fixture-based denominator per league
            sports_services = {
                "instruments-service",
                "market-tick-data-service",
                "features-sports-service",
            }
            if service not in sports_services:
                click.echo(
                    click.style(
                        "--sports-league-breakdown is only supported for sports services:"
                        f" {', '.join(sorted(sports_services))}",
                        fg="red",
                    ),
                    err=True,
                )
                sys.exit(1)
            display_sports_league_breakdown(service, start_date, end_date, output, show_missing)

        elif service in DYNAMIC_DIMENSION_SERVICES:
            display_dynamic_service_status(service, start_date, end_date, asset_group, output, config_dir, mode)

        else:
            display_fixed_service_status(
                service,
                start_date,
                end_date,
                asset_group,
                venue,
                output,
                show_timestamps,
                show_missing,
                config_dir,
                fast,
                detailed,
                mode,
            )

        if benchmark:
            format_benchmark_info(total_start, start_date, end_date)

    except (OSError, ValueError, RuntimeError) as e:
        click.echo(click.style(f"Error: {e}", fg="red"), err=True)
        logger.exception("Data status check failed")
        sys.exit(1)


def _try_manifest_source(
    *,
    service: str,
    start_date: datetime,
    end_date: datetime,
    asset_group: tuple[str, ...],
    output: str,
    show_missing: bool,
    resolved_source: str,
    benchmark: bool,
    total_start: float,
) -> bool:
    """Attempt to get data status from the manifest catalogue.

    Returns True if manifest data was successfully displayed, False to fall through to GCS.
    """
    reader = ManifestReader()
    if not reader.is_available():
        if resolved_source == "manifest":
            click.echo(
                click.style("ManifestReader not available (duckdb or cloud access missing)", fg="red"),
                err=True,
            )
        return False

    sd = start_date.strftime("%Y-%m-%d")
    ed = end_date.strftime("%Y-%m-%d")

    asset_groups_to_check = list(asset_group) if asset_group else [None]
    all_results: list[dict[str, object]] = []

    for ag in asset_groups_to_check:
        result = reader.get_completion(
            service=service,
            asset_group=ag or "",
            start_date=sd,
            end_date=ed,
        )
        if "error" in result:
            logger.debug("Manifest query error for asset_group=%s: %s", ag, result.get("error"))
            continue
        all_results.append(result)

    if not all_results:
        return False

    if resolved_source == "auto" and all(r.get("overall_completion", 0) == 0 for r in all_results):
        return False

    _display_manifest_results(all_results, output, show_missing, service, sd, ed)

    if benchmark:
        format_benchmark_info(total_start, start_date, end_date)

    return True


def _display_manifest_results(
    results: list[dict[str, object]],
    output: str,
    show_missing: bool,
    service: str,
    start_date: str,
    end_date: str,
) -> None:
    """Format and display manifest-based results to match GCS output style."""
    if output == "json":
        click.echo(json.dumps(results, indent=2, default=str))
        return

    click.echo(click.style(f"[source: manifest] {service} ({start_date} → {end_date})", fg="cyan", bold=True))
    click.echo()

    for result in results:
        cat = result.get("asset_group", "unknown")
        completion = result.get("overall_completion", 0)
        days_complete = result.get("days_complete", 0)
        days_total = result.get("days_total", 0)

        completion_f = cast(float, completion)
        color = "green" if completion_f == 100 else "yellow" if completion_f >= 50 else "red"
        click.echo(f"  {cat}: " + click.style(f"{completion}%", fg=color) + f" ({days_complete}/{days_total} days)")

        venues_raw: object = result.get("venues", {})
        if isinstance(venues_raw, dict) and venues_raw and output == "tree":
            venues = cast("dict[str, object]", venues_raw)
            for venue_name, venue_info in venues.items():
                v_info = cast("dict[str, object]", venue_info)
                v_pct = v_info.get("completion_percent", 0)
                v_days = v_info.get("days_with_data", 0)
                v_rows = v_info.get("total_rows", 0)
                v_pct_f = cast(float, v_pct)
                v_color = "green" if v_pct_f == 100 else "yellow" if v_pct_f >= 50 else "red"
                click.echo(
                    f"    └─ {venue_name}: " + click.style(f"{v_pct}%", fg=v_color) + f" ({v_days} days, {v_rows} rows)"
                )

        if show_missing:
            missing = cast("list[str]", result.get("missing_dates", []))
            if missing:
                click.echo(f"    Missing: {', '.join(missing[:10])}")
                if len(missing) > 10:
                    click.echo(f"    ... and {len(missing) - 10} more")

    if output == "summary":
        total_complete = sum(cast(int, r.get("days_complete", 0)) for r in results)
        total_days = sum(cast(int, r.get("days_total", 0)) for r in results)
        overall = round(total_complete / total_days * 100, 1) if total_days > 0 else 0
        click.echo()
        click.echo(f"  Overall: {overall}% ({total_complete}/{total_days} asset-group-days)")
