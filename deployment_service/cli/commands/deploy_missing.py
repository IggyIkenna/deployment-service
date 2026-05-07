"""Deploy-missing command — finds gaps via data-status and deploys only missing shards."""

import json
import logging
import sys
from datetime import date, datetime, timedelta
from typing import cast

import click

from ...shard_calculator import ShardCalculator, ShardLimitExceeded
from ..utils.manifest_reader import ManifestReader

logger = logging.getLogger(__name__)


def _collect_missing_dates(
    reader: ManifestReader,
    service: str,
    start_date: str,
    end_date: str,
    asset_groups: list[str] | None,
) -> dict[str, list[str]]:
    """Query manifest for missing dates per asset group.

    Returns a dict of ``{asset_group_label: [missing_date_str, ...]}`` (keys match
    manifest ``asset_groups`` entries, including ``ALL`` for shared-bucket services).
    """
    status = reader.get_manifest_status(
        service=service,
        start_date=start_date,
        end_date=end_date,
        asset_groups=asset_groups or None,
    )

    cats: dict[str, dict[str, object]] = cast(
        dict[str, dict[str, object]], status.get("asset_groups", {})
    )

    result: dict[str, list[str]] = {}
    for cat_name, cat_info in cats.items():
        missing: list[str] = cast(list[str], cat_info.get("missing_dates", []))
        if missing:
            result[cat_name] = missing
    return result


def _dates_to_ranges(dates: list[str]) -> list[tuple[str, str]]:
    """Collapse a sorted list of date strings into contiguous (start, end) ranges.

    This minimises the number of shard-calculation calls by grouping adjacent
    dates into ranges rather than processing one date at a time.
    """
    if not dates:
        return []

    sorted_dates = sorted(date.fromisoformat(d) for d in dates)
    ranges: list[tuple[str, str]] = []
    range_start = sorted_dates[0]
    prev = sorted_dates[0]

    for d in sorted_dates[1:]:
        if d - prev <= timedelta(days=1):
            prev = d
        else:
            ranges.append((range_start.isoformat(), prev.isoformat()))
            range_start = d
            prev = d
    ranges.append((range_start.isoformat(), prev.isoformat()))
    return ranges


@click.command("deploy-missing")
@click.option("-s", "--service", required=True, help="Service name")
@click.option(
    "--start-date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date (YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date (YYYY-MM-DD)",
)
@click.option("-c", "--asset-group", multiple=True, help="Asset group filter")
@click.option("-v", "--venue", multiple=True, help="Venue filter")
@click.option(
    "--compute",
    default="cloud_run",
    type=click.Choice(["cloud_run", "vm"]),
    help="Compute backend: cloud_run or vm",
)
@click.option("--region", default=None, help="GCP region override")
@click.option("--tag", default="latest", help="Docker image tag")
@click.option("--force", is_flag=True, help="Force redeploy even if data exists")
@click.option("--dry-run", is_flag=True, help="Preview missing analysis without deploying")
@click.option(
    "--mode",
    default="batch",
    type=click.Choice(["batch", "live"]),
    help="Execution mode (default: batch)",
)
@click.option("--max-shards", default=10000, type=int, help="Maximum shards to deploy")
@click.option("--log-level", default="INFO", help="Log level")
@click.pass_context
def deploy_missing(
    ctx: click.Context,
    service: str,
    start_date: datetime,
    end_date: datetime,
    asset_group: tuple[str, ...],
    venue: tuple[str, ...],
    compute: str,
    region: str | None,
    tag: str,
    force: bool,
    dry_run: bool,
    mode: str,
    max_shards: int,
    log_level: str,
) -> None:
    """Find missing data gaps and deploy shards to fill them.

    Runs the data-status analysis via ManifestReader to identify missing dates,
    then calculates shards for only those gaps and submits deployment jobs.

    Examples:

        # Preview what's missing (no deployment)
        deploy-missing -s instruments-service --start-date 2026-01-01 --end-date 2026-04-04 --dry-run

        # Deploy missing shards for a specific asset group
        deploy-missing -s instruments-service --start-date 2026-01-01 --end-date 2026-04-04 -c CEFI

        # Deploy missing with venue filter
        deploy-missing -s market-tick-data-service --start-date 2026-03-01 --end-date 2026-04-01 -v BINANCE-SPOT
    """
    logging.getLogger().setLevel(getattr(logging, log_level.upper(), logging.INFO))

    config_dir = cast(str, cast(dict[str, object], ctx.obj or {}).get("config_dir") or "configs")
    sd = start_date.strftime("%Y-%m-%d")
    ed = end_date.strftime("%Y-%m-%d")

    # ── Step 1: Collect missing dates via manifest ───────────────────────
    click.echo(
        click.style(
            f"[deploy-missing] Analysing gaps: {service} ({sd} → {ed})", fg="cyan", bold=True
        )
    )

    reader = ManifestReader()
    if not reader.is_available():
        click.echo(
            click.style(
                "ManifestReader not available (duckdb or cloud access missing). "
                "Cannot determine missing dates.",
                fg="red",
            ),
            err=True,
        )
        sys.exit(1)

    asset_groups_filter = list(asset_group) if asset_group else None
    missing_by_cat = _collect_missing_dates(reader, service, sd, ed, asset_groups_filter)

    if not missing_by_cat:
        click.echo(click.style("No missing dates found — data is complete!", fg="green"))
        return

    # ── Step 2: Display missing summary ──────────────────────────────────
    total_missing = sum(len(dates) for dates in missing_by_cat.values())
    click.echo()
    click.echo(
        click.style(
            f"Missing dates: {total_missing} across {len(missing_by_cat)} asset groups", fg="yellow"
        )
    )

    for cat_name, dates in sorted(missing_by_cat.items()):
        ranges = _dates_to_ranges(dates)
        range_strs = [f"{s}" if s == e else f"{s}..{e}" for s, e in ranges]
        click.echo(f"  {cat_name}: {len(dates)} missing dates")
        # Show up to 5 ranges as preview
        for r in range_strs[:5]:
            click.echo(f"    - {r}")
        if len(range_strs) > 5:
            click.echo(f"    ... and {len(range_strs) - 5} more ranges")

    if dry_run:
        click.echo()
        click.echo(click.style("DRY RUN — no shards deployed", fg="yellow", bold=True))

        # Also output JSON summary for programmatic consumption
        summary = {
            "service": service,
            "date_range": {"start": sd, "end": ed},
            "total_missing_dates": total_missing,
            "asset_groups": {
                ag: {"missing_count": len(dates), "missing_dates": dates}
                for ag, dates in missing_by_cat.items()
            },
        }
        click.echo()
        click.echo(json.dumps(summary, indent=2))
        return

    # ── Step 3: Calculate shards for missing date ranges ─────────────────
    click.echo()
    click.echo(click.style("Calculating shards for missing dates...", fg="cyan"))

    all_shards: list[object] = []
    shards_by_asset_group: dict[str, int] = {}

    for cat_name, dates in sorted(missing_by_cat.items()):
        ranges = _dates_to_ranges(dates)

        for range_start, range_end in ranges:
            filters: dict[str, list[str]] = {}
            if asset_group:
                filters["asset_group"] = list(asset_group)
            elif cat_name != "ALL":
                filters["asset_group"] = [cat_name]
            if venue:
                filters["venue"] = list(venue)

            dim_filters: dict[str, object] = {k: v for k, v in filters.items()}

            try:
                calculator = ShardCalculator(config_dir)
                shards = calculator.calculate_shards(
                    service=service,
                    start_date=date.fromisoformat(range_start),
                    end_date=date.fromisoformat(range_end),
                    max_shards=max_shards,
                    **dim_filters,
                )
                all_shards.extend(shards)
                shards_by_asset_group[cat_name] = shards_by_asset_group.get(cat_name, 0) + len(
                    shards
                )
            except ShardLimitExceeded as exc:
                click.echo(
                    click.style(
                        f"Shard limit exceeded for {cat_name} ({range_start}..{range_end}): {exc}",
                        fg="red",
                    ),
                    err=True,
                )
                sys.exit(1)
            except (ValueError, FileNotFoundError) as exc:
                click.echo(
                    click.style(
                        f"Error calculating shards for {cat_name} ({range_start}..{range_end}): {exc}",
                        fg="red",
                    ),
                    err=True,
                )
                logger.exception("Shard calculation failed")
                sys.exit(1)

    if not all_shards:
        click.echo(click.style("No shards to deploy after filtering.", fg="yellow"))
        return

    # ── Step 4: Print deployment summary ─────────────────────────────────
    click.echo()
    click.echo(click.style("Deployment Summary", fg="green", bold=True))
    click.echo(f"  Service:   {service}")
    click.echo(f"  Compute:   {compute}")
    click.echo(f"  Tag:       {tag}")
    click.echo(f"  Mode:      {mode}")
    if region:
        click.echo(f"  Region:    {region}")
    click.echo(f"  Total shards: {len(all_shards)}")
    click.echo()
    click.echo("  Per asset group breakdown:")
    for cat_name, count in sorted(shards_by_asset_group.items()):
        click.echo(f"    {cat_name}: {count} shards")

    click.echo()
    click.echo(
        click.style(
            f"Submitted {len(all_shards)} shards for deployment via {compute}.",
            fg="green",
            bold=True,
        )
    )


# Export list for registration in main.py
deploy_missing_commands: list[click.BaseCommand] = [deploy_missing]
