"""Meta-sweep target / scheduler-job-name resolvers for the fleet monitors.

Split out of ``cli.py`` (2026-07-13, codex-compliance file-size ceiling) — these
are the pure ``FreshnessTarget`` builders + scheduler/Cloud-Run job-name
formatters that the ``meta`` mode wires into ``meta_watchers.check_catalogue_freshness``
/ ``check_high_attempted_failed`` / ``check_cron_fired``. None touch a cloud SDK
directly (only ``resolve_bucket_name`` + ``get_environment``), so they stay
import-safe + credential-free like the rest of the sweep-input builders in this
package. ``cli.py`` imports the public names below aliased to their original
underscore-prefixed names so existing call sites (incl. tests) keep working.
"""

from __future__ import annotations

from unified_trading_library import resolve_bucket_name
from unified_trading_library.cloud_interface.constants import get_environment  # noqa: qg-deep-import

from deployment_service.data_pipeline_monitors import meta_watchers

ASSET_GROUPS = ("cefi", "defi", "tradfi", "sports", "prediction")

# The catalogue regen (build_instrument_catalogue.py) writes the canonical
# artifact to ``gs://{instruments-store-{ag}-{env-short}-{pid}}/{DEPLOYMENT_ENV}/catalog.parquet``
# — bucket is env-SHORT (``-prd-``), the blob PREFIX is the LONG env name (default
# ``prod``). The monitor MUST mirror BOTH or it probes a non-existent object →
# age=None → false "missing" (KEY #3, the documented env-less-vs-env-short reader
# bug class). SSOT: instruments-service/scripts/build_instrument_catalogue.py
# (``_catalogue_object_paths`` + ``_instruments_store_bucket_for``).
_CATALOGUE_FILENAME = "catalog.parquet"
# prediction uses a dedicated FLAT bucket key (no PREDICTION entry in the per-AG
# ``instruments-store`` dict), mirroring build_instrument_catalogue's resolver.
_INSTRUMENTS_STORE_KIND_OVERRIDE: dict[str, str] = {"prediction": "instruments-store-prediction"}

# Same shape for the market-data buckets: ``prediction`` has NO per-AG
# ``market-data`` entry — it is a dedicated FLAT kind (``market-data-tick-prediction``,
# live-writing). ``resolve_bucket_name(kind="market-data", asset_group="prediction")``
# RAISES, and the ``except Exception: continue`` in the target builders below would
# then silently DROP prediction — leaving it unmonitored for the high-attempted_failed
# check (DP-FETCH-009) and the consolidator-cron freshness check. Resolve it via its
# flat key (no asset_group arg), mirroring the ``_INSTRUMENTS_STORE_KIND_OVERRIDE``
# catalogue path above.
_MARKET_DATA_KIND_OVERRIDE: dict[str, str] = {"prediction": "market-data-tick-prediction"}


def market_data_bucket(ag: str) -> str:
    """Resolve the market-data bucket for ``ag``, honouring the flat prediction key.

    ``prediction`` has no per-asset_group ``market-data`` entry (it is a dedicated
    flat ``market-data-tick-prediction`` bucket), so it is resolved with NO
    asset_group arg; the other four asset_groups use the per-AG call. Raises
    (propagated to the caller's per-target ``try``) only for a genuinely
    unresolvable bucket, never for the prediction shape.
    """
    override = _MARKET_DATA_KIND_OVERRIDE.get(ag)
    if override is not None:
        return resolve_bucket_name(cloud="gcp", kind=override)
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group=ag)


def _deployment_env_long() -> str:
    """The LONG env name used as the catalogue blob prefix (default ``prod``).

    Mirrors build_instrument_catalogue.py ``get_config("DEPLOYMENT_ENV", "prod")``
    — this is the path PREFIX (``prod/`` / ``staging/`` / ``dev/``), NOT the
    env-SHORT (``-prd-``) the bucket NAME carries. Resolved via the UTL
    ``get_environment()`` config-bootstrap function (same DEPLOYMENT_ENV →
    ENVIRONMENT → "prod" probe order the writer uses).
    """
    return get_environment().strip().lower() or "prod"


def catalogue_targets() -> list[meta_watchers.FreshnessTarget]:
    """Per-AG instrument-catalogue freshness targets (24h budget).

    The catalogue regen (build_instrument_catalogue.py) writes the per-AG
    artifact to ``{env}/catalog.parquet`` in the env-SHORT instruments-store
    bucket. Both the bucket (env-SHORT ``-prd-`` via ``resolve_bucket_name``) AND
    the blob prefix (LONG ``DEPLOYMENT_ENV``, default ``prod``) must match the
    writer or the probe reads age=None → a false DP-CATALOG-001 (KEY #3). A
    genuinely missing/stale blob still fires (with the probed path in the alert).
    """
    env_long = _deployment_env_long()
    blob_path = f"{env_long}/{_CATALOGUE_FILENAME}"
    targets: list[meta_watchers.FreshnessTarget] = []
    for ag in ASSET_GROUPS:
        kind = _INSTRUMENTS_STORE_KIND_OVERRIDE.get(ag, "instruments-store")
        try:
            # prediction's flat key takes no asset_group arg (matches the writer).
            if ag in _INSTRUMENTS_STORE_KIND_OVERRIDE:
                bucket = resolve_bucket_name(cloud="gcp", kind=kind)
            else:
                bucket = resolve_bucket_name(cloud="gcp", kind=kind, asset_group=ag)
        except Exception:
            continue
        targets.append(
            meta_watchers.FreshnessTarget(
                bucket=bucket,
                blob_path=blob_path,
                max_age_min=meta_watchers.DEFAULT_CATALOGUE_MAX_AGE_MIN,
                label=ag,
            )
        )
    return targets


def high_attempted_failed_targets() -> list[meta_watchers.FreshnessTarget]:
    """Per-AG market-data ``_index`` targets for the high-attempted_failed check
    (DP-FETCH-009). The ``label`` carries the asset_group (the alert + miss-counter
    key), ``blob_path`` is the consolidated availability index the consolidator
    writes. ``max_age_min`` is unused by ``check_high_attempted_failed`` (it reads
    counts, not freshness) — set to the index blob's path for diagnosability.
    """
    targets: list[meta_watchers.FreshnessTarget] = []
    for ag in ASSET_GROUPS:
        try:
            bucket = market_data_bucket(ag)
        except Exception:
            continue
        targets.append(
            meta_watchers.FreshnessTarget(
                bucket=bucket,
                blob_path=meta_watchers.AVAILABILITY_INDEX_BLOB,
                max_age_min=0.0,  # unused: this check reads counts, not freshness
                label=ag,
            )
        )
    return targets


# Cloud Scheduler state strings (mirror the google.cloud.scheduler_v1 Job.State enum).
_SCHEDULER_PAUSED = "PAUSED"
_SCHEDULER_ENABLED = "ENABLED"
# Long → 3-char env-short (mirrors UTL bucket_naming._DEPLOYMENT_ENV_SHORT_FORM, the
# same map ``resolve_bucket_name`` uses for the ``-prd-`` segment). Kept inline (a
# tiny constant) rather than importing the UTL private to avoid an in-function /
# deep private import; the public ``get_environment()`` (imported at top) gives the
# long name. Default → prd (the fleet default).
_ENV_SHORT_FORM: dict[str, str] = {
    "dev": "dev",
    "development": "dev",
    "staging": "stg",
    "stg": "stg",
    "prod": "prd",
    "prd": "prd",
    "production": "prd",
}


def scheduler_env_prefix() -> str:
    """The TF ``env_prefix`` segment in scheduler/job names: ``uts-{env-short}``.

    The consolidator scheduler jobs are ``{env_prefix}-manifest-consolidator-{key}-cron``
    (manifest_consolidator_scheduler.tf). ``env_prefix`` = ``uts-{deployment_env_short}``
    in the fleet TF — the env-short derived from ``get_environment()`` the same way
    ``resolve_bucket_name`` derives the bucket's ``-prd-`` segment.
    """
    short = _ENV_SHORT_FORM.get(_deployment_env_long(), "prd")
    return f"uts-{short}"


def consolidator_scheduler_job(ag: str) -> str:
    """Scheduler job name: ``{env_prefix}-manifest-consolidator-market-data-{ag}-cron``.

    Matches manifest_consolidator_scheduler.tf — PAUSED suppresses DP_CRON_DID_NOT_FIRE (KEY #2).
    """
    return f"{scheduler_env_prefix()}-manifest-consolidator-market-data-{ag}-cron"


def consolidator_cloud_run_job(ag: str) -> str:
    """Cloud Run job backing the per-AG consolidator (scheduler name minus ``-cron``).

    KEY #4: a stale ``_index`` is suppressed when a recent SUCCEEDED execution is found.
    """
    return f"{scheduler_env_prefix()}-manifest-consolidator-market-data-{ag}"
