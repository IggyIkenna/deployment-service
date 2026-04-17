"""Extended data status checks: T+1, ML experiments, live freshness.

Mode 2 (T+1): Single-date cluster check for yesterday.
Mode 3 (Thermal/ML): List ML experiments and model metadata from GCS.
Mode 4 (Live freshness): Report staleness of live-mode data.
"""

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import click
import yaml
from unified_trading_library import (
    PATH_REGISTRY,
    UnifiedCloudConfig,
    build_bucket,
    get_storage_client,
)

from .manifest_reader import ManifestReader

logger = logging.getLogger(__name__)

# Staleness thresholds per domain (seconds).
# CeFi markets run 24/7 — data older than 15 min is stale.
# Sports reference data refreshes less frequently — 6h threshold.
_STALENESS_THRESHOLDS: dict[str, int] = {
    "cefi": 15 * 60,
    "defi": 15 * 60,
    "tradfi": 60 * 60,
    "sports": 6 * 3600,
    "prediction": 6 * 3600,
    "default": 30 * 60,
}


def _parse_last_modified(raw: str | None) -> datetime | None:
    """Parse last_modified string from BlobMetadata into a timezone-aware datetime.

    BlobMetadata.last_modified is an ISO-format or RFC 2822 string (provider-dependent).
    Returns None if parsing fails.
    """
    if not raw:
        return None
    # Strip trailing 'Z' and replace with +00:00 for fromisoformat
    cleaned = raw.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        logger.debug("Could not parse last_modified: %s", raw)
        return None


# ──────────────────────────────────────────────────────────────────────
# Mode 2: T+1 check
# ──────────────────────────────────────────────────────────────────────


def run_t1_check(
    cluster_name: str,
    config_dir: str,
    output: str,
) -> None:
    """Run a T+1 (yesterday) data status check across all services in a cluster.

    Reads the cluster YAML from *config_dir*/clusters/<cluster_name>.yaml,
    then queries ManifestReader for each service with start_date=end_date=yesterday.
    """
    yesterday = (datetime.now(tz=UTC) - timedelta(days=1)).strftime("%Y-%m-%d")
    cluster_path = Path(config_dir) / "clusters" / f"{cluster_name}.yaml"

    if not cluster_path.exists():
        click.echo(
            click.style(f"Cluster config not found: {cluster_path}", fg="red"),
            err=True,
        )
        raise SystemExit(1)

    with open(cluster_path) as fh:
        cluster_cfg: dict[str, object] = yaml.safe_load(fh)

    services: list[str] = []
    raw_services: list[str] = cluster_cfg.get("services") or []  # type: ignore[assignment]
    for svc in raw_services:
        svc_name = str(svc).split(":")[0]  # strip operational-mode suffix
        if svc_name not in services:
            services.append(svc_name)

    click.echo()
    click.echo(
        click.style(
            f"T+1 CHECK  cluster={cluster_name}  date={yesterday}",
            fg="cyan",
            bold=True,
        )
    )
    click.echo("=" * 70)

    reader = ManifestReader()
    manifest_available = reader.is_available()

    rows: list[dict[str, str]] = []
    for svc in services:
        status = "UNKNOWN"
        details = ""
        if manifest_available:
            result = reader.get_completion(
                service=svc,
                category="",
                start_date=yesterday,
                end_date=yesterday,
            )
            if "error" in result:
                status = "ERROR"
                details = str(result["error"])
            else:
                completion = float(result.get("overall_completion", 0))  # type: ignore[arg-type]
                if completion == 100:
                    status = "OK"
                elif completion == 0:
                    status = "MISSING"
                else:
                    status = "PARTIAL"
                details = f"{completion}% complete"
        else:
            status = "NO_MANIFEST"
            details = "ManifestReader unavailable"

        rows.append({"service": svc, "status": status, "details": details})

    if output == "json":
        click.echo(
            json.dumps(
                {"date": yesterday, "cluster": cluster_name, "services": rows},
                indent=2,
            )
        )
        return

    # Table output
    click.echo()
    click.echo(f"{'Service':<40} {'Status':<10} {'Details'}")
    click.echo("-" * 80)

    status_colors: dict[str, str] = {
        "OK": "green",
        "PARTIAL": "yellow",
        "MISSING": "red",
        "ERROR": "red",
        "UNKNOWN": "white",
        "NO_MANIFEST": "white",
    }

    ok_count = 0
    for row in rows:
        color = status_colors.get(row["status"], "white")
        click.echo(
            f"{row['service']:<40} "
            + click.style(f"{row['status']:<10}", fg=color)
            + f" {row['details']}"
        )
        if row["status"] == "OK":
            ok_count += 1

    click.echo("-" * 80)
    total = len(rows)
    summary_color = "green" if ok_count == total else ("yellow" if ok_count > 0 else "red")
    click.echo(
        click.style(
            f"Summary: {ok_count}/{total} services OK for {yesterday}",
            fg=summary_color,
        )
    )


# ──────────────────────────────────────────────────────────────────────
# Mode 3: ML experiments listing
# ──────────────────────────────────────────────────────────────────────


def run_ml_experiments_check(output: str) -> None:
    """List ML training experiments and check model metadata in GCS.

    Uses PATH_REGISTRY specs ``ml_training_artifacts`` and ``ml_model_metadata``
    to resolve bucket names and path patterns.
    """
    config = UnifiedCloudConfig()
    project_id = config.gcp_project_id
    storage_client = get_storage_client()

    artifacts_spec = PATH_REGISTRY["ml_training_artifacts"]
    metadata_spec = PATH_REGISTRY["ml_model_metadata"]

    artifacts_bucket = build_bucket("ml_training_artifacts", project_id=project_id)
    metadata_bucket = build_bucket("ml_model_metadata", project_id=project_id)

    click.echo()
    click.echo(click.style("ML EXPERIMENTS & MODEL REGISTRY", fg="cyan", bold=True))
    click.echo("=" * 80)
    click.echo(f"  Artifacts bucket : {artifacts_bucket}")
    click.echo(f"  Metadata bucket  : {metadata_bucket}")
    click.echo()

    # List experiment directories from artifacts bucket.
    # Path pattern: stage1-preselection/model-{model_id}/training-period={training_period}/
    artifacts_prefix = artifacts_spec.path_template.split("{model_id}")[0]
    experiment_dirs: list[dict[str, str]] = []

    # list_blobs returns Iterator[BlobMetadata] — use .name attribute
    blobs = storage_client.list_blobs(artifacts_bucket, prefix=artifacts_prefix)
    seen_experiments: set[str] = set()
    for blob_meta in blobs:
        blob_path = blob_meta.name
        # Strip prefix and split to extract model_id and training_period
        relative = blob_path[len(artifacts_prefix) :]
        parts = relative.split("/")
        if len(parts) >= 2:
            model_part = parts[0]  # model-{model_id}
            period_part = parts[1]  # training-period={tp}

            model_id = (
                model_part.removeprefix("model-") if model_part.startswith("model-") else model_part
            )
            training_period = (
                period_part.removeprefix("training-period=")
                if period_part.startswith("training-period=")
                else period_part
            )

            exp_key = f"{model_id}|{training_period}"
            if exp_key not in seen_experiments:
                seen_experiments.add(exp_key)
                experiment_dirs.append({"model_id": model_id, "training_period": training_period})

    # Check metadata.json existence for each experiment
    results: list[dict[str, object]] = []
    for exp in experiment_dirs:
        metadata_path = metadata_spec.path_template.format(
            model_id=exp["model_id"],
            training_period=exp["training_period"],
        )
        metadata_blob = f"{metadata_path}{metadata_spec.file_template}"

        has_metadata = storage_client.blob_exists(metadata_bucket, metadata_blob)

        results.append(
            {
                "model_id": exp["model_id"],
                "training_period": exp["training_period"],
                "has_metadata": has_metadata,
            }
        )

    if output == "json":
        click.echo(
            json.dumps(
                {
                    "artifacts_bucket": artifacts_bucket,
                    "metadata_bucket": metadata_bucket,
                    "experiments": results,
                },
                indent=2,
            )
        )
        return

    if not results:
        click.echo(click.style("  No experiments found.", fg="yellow"))
        return

    click.echo(f"{'Model ID':<30} {'Training Period':<25} {'Metadata'}")
    click.echo("-" * 80)

    meta_ok = 0
    for r in results:
        has_meta = bool(r["has_metadata"])
        meta_status = (
            click.style("OK", fg="green") if has_meta else click.style("MISSING", fg="red")
        )
        if has_meta:
            meta_ok += 1
        click.echo(f"{str(r['model_id']):<30} {str(r['training_period']):<25} {meta_status}")

    click.echo("-" * 80)
    total = len(results)
    missing_count = total - meta_ok
    summary = f"Total: {total} experiments, " + click.style(f"{meta_ok} with metadata", fg="green")
    if missing_count > 0:
        summary += click.style(f", {missing_count} missing metadata", fg="red")
    click.echo(summary)


# ──────────────────────────────────────────────────────────────────────
# Mode 4: Live freshness
# ──────────────────────────────────────────────────────────────────────


def _format_staleness(seconds: float) -> str:
    """Format staleness duration into a human-readable string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def run_live_freshness_check(
    service: str,
    category: tuple[str, ...],
    config_dir: str,
    output: str,
) -> None:
    """Report freshness (staleness) for live-mode data partitions.

    For each category, scans the ``live/`` prefix in the service bucket and
    finds the newest blob timestamp.  Reports staleness = now - last_write.
    Flags stale entries based on domain-specific thresholds.
    """
    config = UnifiedCloudConfig()
    project_id = config.gcp_project_id
    storage_client = get_storage_client()

    from .manifest_reader import _BUCKET_TEMPLATES

    bucket_template = _BUCKET_TEMPLATES.get(service)
    if not bucket_template:
        click.echo(
            click.style(f"No bucket template for service: {service}", fg="red"),
            err=True,
        )
        raise SystemExit(1)

    categories_to_check = list(category) if category else ["cefi", "defi", "tradfi", "sports"]
    now = datetime.now(tz=UTC)

    click.echo()
    click.echo(
        click.style(
            f"LIVE FRESHNESS  service={service}",
            fg="cyan",
            bold=True,
        )
    )
    click.echo("=" * 80)

    results: list[dict[str, object]] = []

    for cat in categories_to_check:
        cat_lower = cat.lower().replace("_", "-")
        bucket_name = bucket_template.format(
            category_lower=cat_lower,
            project_id=project_id,
            domain=cat_lower,
        )

        live_prefix = "live/"
        newest_time: datetime | None = None

        # list_blobs returns Iterator[BlobMetadata] with .last_modified (str|None)
        blob_iter = storage_client.list_blobs(bucket_name, prefix=live_prefix)
        blob_count = 0
        for blob_meta in blob_iter:
            blob_count += 1
            parsed_time = _parse_last_modified(blob_meta.last_modified)
            if parsed_time is not None and (newest_time is None or parsed_time > newest_time):
                newest_time = parsed_time

        staleness_seconds: float | None = None
        staleness_str = "N/A"
        is_stale = False

        if newest_time is not None:
            staleness_seconds = (now - newest_time).total_seconds()
            threshold = _STALENESS_THRESHOLDS.get(
                cat_lower,
                _STALENESS_THRESHOLDS["default"],
            )
            is_stale = staleness_seconds > threshold
            staleness_str = _format_staleness(staleness_seconds)

        results.append(
            {
                "category": cat,
                "bucket": bucket_name,
                "blob_count": blob_count,
                "last_write": newest_time.isoformat() if newest_time else None,
                "staleness_seconds": staleness_seconds,
                "staleness_human": staleness_str,
                "is_stale": is_stale,
            }
        )

    if output == "json":
        click.echo(
            json.dumps(
                {"service": service, "checked_at": now.isoformat(), "categories": results},
                indent=2,
                default=str,
            )
        )
        return

    click.echo()
    click.echo(f"{'Category':<15} {'Blobs':<8} {'Last Write':<22} {'Staleness':<12} {'Status'}")
    click.echo("-" * 80)

    stale_count = 0
    for r in results:
        last_write_str = str(r["last_write"])[:19] if r["last_write"] else "no data"
        staleness_display = str(r["staleness_human"])

        if r["is_stale"]:
            status = click.style("STALE", fg="red", bold=True)
            stale_count += 1
        elif r["last_write"] is None:
            status = click.style("NO DATA", fg="red")
            stale_count += 1
        else:
            status = click.style("FRESH", fg="green")

        click.echo(
            f"{str(r['category']):<15} {str(r['blob_count']):<8} "
            f"{last_write_str:<22} {staleness_display:<12} {status}"
        )

    click.echo("-" * 80)
    if stale_count > 0:
        click.echo(
            click.style(
                f"WARNING: {stale_count}/{len(results)} categories are stale or have no data",
                fg="red",
                bold=True,
            )
        )
    else:
        click.echo(click.style("All categories are fresh.", fg="green"))
