"""DP-LIVE-001 / DP-LIVE-002 — live VM data stream blindspots.

**DP-LIVE-001** — "VM alive, data dead" staleness.
A live prediction VM that silently drops its WebSocket subscriptions keeps heartbeating
but writes no parquet.  The existing checks miss this because:

  - The availability_index freshness check passes (the consolidator IS updating it).
  - The heartbeat check passes (the VM IS alive at the OS level).

Only reading the per-VM manifest shard's ``MAX(attempted_at)`` reveals the gap.

``check_live_stream_stale`` reads each per-VM shard and alerts via
``DP_CRON_DID_NOT_FIRE`` (reused — no cross-repo UTL/UAC changes needed) with label
``live-data-stale::<vm_name>`` when the shard has been silent for longer than
``max_stale_hours`` (default 4 h).

**DP-LIVE-002** — "Manifest captured, GCS empty" sink mismatch.
A VM can call ``record_captured`` (manifest shows ``capture_status=captured``) but
route the actual parquet bytes to an in-process sink (e.g. ``InMemoryTransport``) that
discards them.  DP-LIVE-001 MISSES this because ``attempted_at`` stays fresh.

``check_live_stream_gcs_write_mismatch`` counts ``captured`` rows in the manifest and
compares to the number of GCS parquet files at the venue/data_type prefix.  If the
manifest shows ≥1 captured rows but GCS has 0 files AND the VM has been running for
longer than ``min_age_hours`` (default 1 h), it fires a CRITICAL alert so the data-loss
is caught before the next arb-detector tick.

Root cause that triggered DP-LIVE-002: Plan 04 ``LiveEventFacadeSink`` used
``transport=None`` → ``InMemoryTransport``.  26 instruments showed ``captured`` in the
manifest all day; GCS had 0 ``book_snapshot_5`` files.  Fixed in
``market-tick-data-service@3043f2dc`` (restored ``LiveWebsocketTickSink``).

Registry IDs: DP-LIVE-001, DP-LIVE-002.
"""

from __future__ import annotations

import io
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

import pandas as pd
from unified_trading_library import StorageClient, resolve_bucket_name
from unified_trading_library.events import DP_CRON_DID_NOT_FIRE  # noqa: qg-deep-import

from deployment_service.data_pipeline_monitors.escalation import EscalationTier, PipelineFinding
from deployment_service.data_pipeline_monitors.meta_watchers import (
    DEFAULT_MIN_CONSECUTIVE_MISSES,
    MissTracker,
    emit_finding,
)

logger = logging.getLogger(__name__)

DEFAULT_LIVE_STREAM_MAX_STALE_HOURS = 4.0
DEFAULT_LIVE_STREAM_MIN_AGE_HOURS = 1.0


@dataclass(frozen=True)
class LiveVmShard:
    """A single live-VM per-VM-shard probe target for DP-LIVE-001."""

    vm_name: str
    bucket: str
    blob_path: str  # _index/per_vm/<vm_name>.parquet
    max_stale_hours: float = DEFAULT_LIVE_STREAM_MAX_STALE_HOURS


def _read_vm_shard_latest_attempted_at(
    storage_client: StorageClient,
    *,
    bucket: str,
    blob: str,
) -> datetime | None:
    """Return MAX(attempted_at) across all rows in a per-VM shard parquet.

    Returns None on missing/unreadable shard — caller treats that as stale."""
    try:
        if not storage_client.blob_exists(bucket, blob):
            return None
        raw = storage_client.download_bytes(bucket, blob)
    except Exception:
        return None
    try:
        df = pd.read_parquet(io.BytesIO(raw))
    except Exception:
        return None
    if df.empty or "attempted_at" not in df.columns:
        return None
    try:
        max_ts = cast("pd.Timestamp", df["attempted_at"].max())
        if pd.isna(max_ts):
            return None
        if max_ts.tzinfo is None:
            max_ts = max_ts.tz_localize("UTC")
        return max_ts.to_pydatetime().astimezone(UTC)
    except Exception:
        return None


def build_prediction_live_shards(storage_client: StorageClient) -> list[LiveVmShard]:
    """Enumerate ``_index/per_vm/`` in the prediction bucket; one ``LiveVmShard`` per
    ``prediction-live-*`` VM.  Backfill/batch VMs are excluded.  Returns [] on error."""
    try:
        bucket = resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="prediction")
    except Exception:
        return []
    prefix = "_index/per_vm/"
    shards: list[LiveVmShard] = []
    try:
        for blob_meta in storage_client.list_blobs(bucket, prefix=prefix):
            blob_name = blob_meta.name
            if not blob_name.endswith(".parquet"):
                continue
            vm_name = blob_name[len(prefix) : -len(".parquet")]
            if not vm_name.startswith("prediction-live-"):
                continue
            shards.append(LiveVmShard(vm_name=vm_name, bucket=bucket, blob_path=blob_name))
    except Exception:
        return []
    return shards


def _live_stream_miss_key(shard: LiveVmShard) -> str:
    return f"{DP_CRON_DID_NOT_FIRE}::live-data-stale::{shard.vm_name}"


def check_live_stream_stale(
    *,
    storage_client: StorageClient,
    shards: Iterable[LiveVmShard],
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
) -> list[LiveVmShard]:
    """DP-LIVE-001 — live VM data stream staleness check.

    Reads each per-VM shard's MAX(attempted_at) and alerts via DP_CRON_DID_NOT_FIRE
    when the stream has been silent for longer than ``max_stale_hours``. Closes the
    "VM alive, data dead" blind spot: a live VM that drops its WebSocket subscriptions
    continues heartbeating (so the heartbeat check passes) and the availability_index
    still refreshes (so the cron-fired check passes), but no new parquet rows are written.
    Only a stale MAX(attempted_at) in the per-VM manifest shard reveals the gap.
    """
    stale_shards: list[LiveVmShard] = []
    now = datetime.now(tz=UTC)
    for shard in shards:
        latest = _read_vm_shard_latest_attempted_at(storage_client, bucket=shard.bucket, blob=shard.blob_path)
        if latest is None:
            age_min: float | None = None
            is_stale = True
        else:
            age_min = (now - latest).total_seconds() / 60.0
            is_stale = age_min > shard.max_stale_hours * 60.0
        miss_key = _live_stream_miss_key(shard)
        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=is_stale)
            if is_stale and misses < min_consecutive:
                logger.info(
                    "live_stream_watcher: DP_CRON_DID_NOT_FIRE for '%s' "
                    "below consecutive-miss threshold (%d/%d) — not paging yet",
                    shard.vm_name,
                    misses,
                    min_consecutive,
                )
                continue
        if not is_stale:
            continue
        stale_shards.append(shard)
        probed = f"gs://{shard.bucket}/{shard.blob_path}"  # noqa: gs-uri — human alert label, not a storage lookup
        if age_min is None:
            state = "shard ABSENT"
        else:
            state = f"last data {age_min:.0f}m ago (={age_min / 60.0:.1f}h) > budget {shard.max_stale_hours:.0f}h"
        summary = f"live VM data stream stale — vm={shard.vm_name}, {state}, probed {probed}"
        emit_finding(
            PipelineFinding(
                event=DP_CRON_DID_NOT_FIRE,
                severity="CRITICAL",
                tier=EscalationTier.PAGE_OPERATOR,
                summary=summary,
                details={
                    "label": f"live-data-stale::{shard.vm_name}",
                    "vm_name": shard.vm_name,
                    "bucket": shard.bucket,
                    "blob_path": shard.blob_path,
                    "probed_path": probed,
                    "age_min": age_min,
                    "max_stale_hours": shard.max_stale_hours,
                },
                registry_id="DP-LIVE-001",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
    return stale_shards


def _read_vm_shard_gcs_write_state(
    storage_client: StorageClient,
    *,
    bucket: str,
    blob: str,
) -> tuple[int, str | None, str | None, str | None, datetime | None]:
    """Return (captured_count, venue, data_type, date_iso, shard_written_at) from a per-VM shard.

    ``captured_count`` is the number of rows with ``capture_status == "captured"``.
    Returns (0, None, None, None, None) on missing/unreadable shard.
    """
    try:
        if not storage_client.blob_exists(bucket, blob):
            return 0, None, None, None, None
        raw = storage_client.download_bytes(bucket, blob)
    except Exception:
        return 0, None, None, None, None
    try:
        df = pd.read_parquet(io.BytesIO(raw))
    except Exception:
        return 0, None, None, None, None
    if df.empty:
        return 0, None, None, None, None
    captured_count = int((df.get("capture_status", pd.Series(dtype=str)) == "captured").sum())
    venue: str | None = cast("str", df["venue"].iloc[0]) if "venue" in df.columns else None
    data_type: str | None = cast("str", df["data_type"].iloc[0]) if "data_type" in df.columns else None
    date_iso: str | None = cast("str", df["date"].iloc[0]) if "date" in df.columns else None
    shard_written_at: datetime | None = None
    if "written_at" in df.columns:
        try:
            max_ts = cast("pd.Timestamp", df["written_at"].max())
            if not pd.isna(max_ts):
                if max_ts.tzinfo is None:
                    max_ts = max_ts.tz_localize("UTC")
                shard_written_at = max_ts.to_pydatetime().astimezone(UTC)
        except Exception:
            pass
    return captured_count, venue, data_type, date_iso, shard_written_at


def check_live_stream_gcs_write_mismatch(
    *,
    storage_client: StorageClient,
    shards: Iterable[LiveVmShard],
    pm_repo_path: str | None = None,
    dry_run: bool = False,
    miss_tracker: MissTracker | None = None,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE_MISSES,
    min_age_hours: float = DEFAULT_LIVE_STREAM_MIN_AGE_HOURS,
) -> list[LiveVmShard]:
    """DP-LIVE-002 — manifest-captured vs GCS sink mismatch.

    Catches the class of bug where ``record_captured`` is called (manifest shows
    ``capture_status=captured``) but the data sink routes parquets to an in-process
    store (e.g. ``InMemoryTransport``) that discards them.  DP-LIVE-001 misses this
    because ``attempted_at`` stays fresh even when 0 GCS files are written.

    For each shard: if the manifest shows ≥1 captured rows AND the shard has been
    running for ≥``min_age_hours`` (default 1 h) AND GCS has 0 parquet files at the
    ``raw_tick_data/by_date/day=<date>/`` prefix for that venue/data_type, fires a
    CRITICAL alert (registry_id="DP-LIVE-002").
    """
    mismatch_shards: list[LiveVmShard] = []
    now = datetime.now(tz=UTC)
    for shard in shards:
        captured_count, venue, data_type, date_iso, shard_written_at = _read_vm_shard_gcs_write_state(
            storage_client, bucket=shard.bucket, blob=shard.blob_path
        )
        if captured_count == 0 or not venue or not data_type or not date_iso:
            continue
        if shard_written_at is not None:
            age_hours = (now - shard_written_at).total_seconds() / 3600.0
            if age_hours < min_age_hours:
                continue
        prefix = f"raw_tick_data/by_date/day={date_iso}/"
        venue_token = f"venue={venue}"
        data_type_token = f"data_type={data_type}"
        try:
            blobs = list(storage_client.list_blobs(shard.bucket, prefix=prefix))
        except Exception as exc:
            logger.warning("DP-LIVE-002: tick prefix %s unreachable: %s", prefix, exc)
            continue
        gcs_file_count = sum(
            1
            for b in blobs
            if (b if isinstance(b, str) else getattr(b, "name", "")).endswith(".parquet")
            and venue_token in (b if isinstance(b, str) else getattr(b, "name", ""))
            and data_type_token in (b if isinstance(b, str) else getattr(b, "name", ""))
        )
        if gcs_file_count > 0:
            continue
        miss_key = f"{DP_CRON_DID_NOT_FIRE}::live-data-gcs-mismatch::{shard.vm_name}"
        if miss_tracker is not None:
            misses = miss_tracker.register(miss_key, stale=True)
            if misses < min_consecutive:
                logger.info(
                    "live_stream_watcher: DP-LIVE-002 for '%s' below consecutive-miss threshold (%d/%d)",
                    shard.vm_name,
                    misses,
                    min_consecutive,
                )
                continue
        mismatch_shards.append(shard)
        summary = (
            f"DP-LIVE-002: manifest shows {captured_count} captured rows but GCS has 0 "
            f"{data_type} parquets for venue={venue} day={date_iso} — "
            f"tick data is routed to a non-GCS sink (check LiveWebsocketTickSink vs LiveEventFacadeSink). "
            f"vm={shard.vm_name}"
        )
        emit_finding(
            PipelineFinding(
                event=DP_CRON_DID_NOT_FIRE,
                severity="CRITICAL",
                tier=EscalationTier.PAGE_OPERATOR,
                summary=summary,
                details={
                    "label": f"live-data-gcs-mismatch::{shard.vm_name}",
                    "vm_name": shard.vm_name,
                    "venue": venue,
                    "data_type": data_type,
                    "date_iso": date_iso,
                    "manifest_captured_count": captured_count,
                    "gcs_file_count": gcs_file_count,
                    "bucket": shard.bucket,
                },
                registry_id="DP-LIVE-002",
            ),
            pm_repo_path=pm_repo_path,
            dry_run=dry_run,
        )
    return mismatch_shards
