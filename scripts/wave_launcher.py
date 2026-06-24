#!/usr/bin/env python3
# Epic: tradfi_master
# Lifecycle: permanent
# Delete-when: NA
"""Autonomous TradFi OHLCV backfill wave-launcher.

Drives the TradFi OHLCV backfill to 100% capture by reading the availability
manifest, computing the cells that still need work (``expected_unattempted`` +
``attempted_failed`` — the latter is the P1 failed-cell retry), and launching a
HARD-CAPPED wave of per-venue OHLCV backfill VMs every scheduler tick.

Why this exists
---------------
The current 8-VM wave was launched MANUALLY: there is no scheduler driving it,
so it stalls the moment a VM finishes. This module is the missing autonomous
driver — a Cloud Run Job + Scheduler (every 2-3h) re-evaluates the gap and tops
the running fleet back up to ``MAX_CONCURRENT``.

Multi-source (2026-06-22)
-------------------------
The backfill is MULTI-SOURCE, not databento-only:

* CME / CBOE / NASDAQ / NYSE → Databento (GLBX.MDP3 / DBEQ.BASIC / XCBF.PITCH),
  fetching ``ohlcv_1m`` + ``ohlcv_1s`` (L0/free; 15m/24h aggregate downstream).
* FX (spot pairs, e.g. USD/KRW) → Yahoo Finance DAILY (``ohlcv_24h``), via
  ``launch-tradfi-bf-fx-ohlcv-24h.sh`` (venue-routed to the Yahoo adapter; no
  ``--source`` — the MTDS CLI only accepts databento|massive there).
* ICE (IFEU/IFUS — Brent/Gasoil/softs/DX) → **NO source available**, so ICE is
  INTENTIONALLY NOT dispatched. Databento dropped ICE in the 3-dataset
  subscription lockdown, and Massive's flat-files carry NO ICE prefix (only
  ``us_futures_{cme,cbot,comex,nymex}`` + equities + ``global_forex``). ICE
  genuinely needs an ICE-data subscription ask (operator decision).

Dispatch model (1 dispatch == exactly 1 VM): CME shards one VM per (root, year)
via ``--only-root``; CBOE / NASDAQ / NYSE / FX ride ONE VM per year. A venue's
gap cell only counts toward the wave if its data_type is in that venue's
addressable set (``VENUE_DATA_TYPES``: FX=ohlcv_24h, Databento venues=1m/1s).
Out-of-scope data_types (trades / tbbo / mbp_10 / earnings) + un-sourced venues
(ICE / YAHOO_FINANCE / UNKNOWN) are reported but never launched.

Idempotency
-----------
* Already-captured shards: we pass ``--no-force-window`` so each VM's MTDS
  pre-flight reads the manifest and SKIPS captured shards (only the gap shards
  in the window are fetched). The manifest gap-recompute each tick also keeps a
  fully-captured (venue, year) out of the candidate set.
* Already-running shards: every running ``tradfi-bf-*`` VM's name encodes
  ``venue`` / ``root`` / ``year`` — we parse it and never re-dispatch a cell a
  running VM already owns.

Singleton-lock note
-------------------
The per-venue launchers carry a singleton lock that refuses to launch while any
``tradfi-bf-*`` VM is RUNNING (a 1-VM-at-a-time Databento-account guard). The
WAVE-LAUNCHER itself is the concurrency governor (it caps at MAX_CONCURRENT and
never exceeds it), so it passes ``--force`` to bypass the per-launcher singleton
— the cap, not the lock, is what bounds concurrent Databento load here.

Usage
-----
    # compute + print the wave it WOULD launch; launch nothing
    python3 wave_launcher.py --dry-run

    # one live tick: top the fleet up to MAX_CONCURRENT
    python3 wave_launcher.py

Env (Cloud Run Job sets these):
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prod
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp
    WAVE_MAX_CONCURRENT=12   (default 12; HARD CEILING 20)
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from unified_trading_library import get_storage_client, log_event, setup_events

logger = logging.getLogger("tradfi_wave_launcher")

# ── Constants ────────────────────────────────────────────────────────────────
ASSET_GROUP = "tradfi"
MANIFEST_BUCKET_TPL = "market-data-tick-tradfi-{env_short}-{project}"
MANIFEST_KEY = "_index/availability_index.parquet"
# Host-cron last-run sentinel (2026-06-24). The wave-launcher runs as a HOST cron
# (0 */3 on the monitor host), invisible to Cloud Run execution history — so the
# dp-meta-watchers fleet monitor cannot cross-check it via the Cloud-Run-history
# reader and would false-fire DP_CRON_DID_NOT_FIRE. Instead the monitor probes THIS
# sentinel's freshness (written to the SAME log bucket the monitor reads,
# ``deployment-scripts-{project}``) as the authoritative host-cron-fired signal.
# Same ``{"ts": <ISO>}`` shape the monitor sentinels use (blob_age_minutes reads it).
_LOG_BUCKET_TPL = "deployment-scripts-{project}"
WAVE_LAUNCHER_LAST_RUN_BLOB = "vm-census/wave-launcher-last-run.json"

# Statuses that count as "needs work". attempted_failed is the P1 retry.
NEEDS_WORK = frozenset({"expected_unattempted", "attempted_failed"})

# The default OHLCV data_types a per-venue launcher fetches (Databento L0 path).
OHLCV_DATA_TYPES = frozenset({"ohlcv_1m", "ohlcv_1s"})

# Per-venue addressable data_types (multi-source, 2026-06-22). A venue's gap
# cells only count toward the wave if their data_type is addressable by that
# venue's launcher. Databento OHLCV venues fetch ohlcv_1m/1s; FX is Yahoo
# DAILY (ohlcv_24h) — a different surface entirely, so it has its own set.
# Venues absent from this map fall back to OHLCV_DATA_TYPES.
VENUE_DATA_TYPES: dict[str, frozenset[str]] = {
    "FX": frozenset({"ohlcv_24h"}),
}


def _addressable_data_types(venue: str) -> frozenset[str]:
    """data_types the per-venue launcher for *venue* actually backfills."""
    return VENUE_DATA_TYPES.get(venue, OHLCV_DATA_TYPES)


# Venues with an OHLCV backfill launcher → multi-source (2026-06-22):
#   * CME / CBOE / NASDAQ / NYSE → Databento (GLBX.MDP3 / DBEQ.BASIC / XCBF.PITCH)
#   * FX                          → Yahoo Finance daily (ohlcv_24h, venue-routed)
# ICE is INTENTIONALLY ABSENT: it is NOT backfillable today. Databento dropped
# ICE (IFEU/IFUS) in the 3-dataset subscription lockdown, AND Massive's
# flat-files carry NO ICE prefix (only us_futures_{cme,cbot,comex,nymex} +
# equities + global_forex) — verified by S3 bucket probe 2026-06-22. ICE
# therefore genuinely needs an ICE-data subscription ask (operator decision),
# NOT a wave-launcher dispatch. SSOT: tradfi_multisource_backfill_2026_06_22.md.
LAUNCHER_FOR_VENUE: dict[str, str] = {
    "CME": "launch-tradfi-bf-cme-ohlcv-1m.sh",
    "CBOE": "launch-tradfi-bf-cboe-ohlcv-1m.sh",
    "NASDAQ": "launch-tradfi-bf-nasdaq-ohlcv-1m.sh",
    "NYSE": "launch-tradfi-bf-nyse-ohlcv-1m.sh",
    "FX": "launch-tradfi-bf-fx-ohlcv-24h.sh",
}

# Venues that shard one-VM-per-(root, year) and expose --only-root, so the
# wave-launcher narrows to a single root per dispatch.
PER_ROOT_VENUES = frozenset({"CME"})

# Venues that shard one-VM-per-year (whole universe in a single VM).
PER_YEAR_VENUES = frozenset({"CBOE", "NASDAQ", "NYSE", "FX"})

DEFAULT_MAX_CONCURRENT = 12
HARD_CEILING_MAX_CONCURRENT = 20  # operator HARD cap — NEVER exceed.

VM_PREFIX = "tradfi-bf-"
SCRIPT_DIR = Path(__file__).resolve().parent
VENUE_LAUNCHER_DIR = SCRIPT_DIR / "vm"


# ── Config helpers (no os.getenv for cloud config; env-injected by Cloud Run) ─
def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required (env-injected by the Cloud Run Job).")
    return value


def _max_concurrent() -> int:
    raw = os.environ.get("WAVE_MAX_CONCURRENT", str(DEFAULT_MAX_CONCURRENT))
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"WAVE_MAX_CONCURRENT must be an int (got {raw!r}).") from exc
    if value < 1:
        raise RuntimeError(f"WAVE_MAX_CONCURRENT must be >= 1 (got {value}).")
    # HARD ceiling — clamp, never exceed.
    return min(value, HARD_CEILING_MAX_CONCURRENT)


def _manifest_bucket() -> str:
    project = _required_env("GCP_PROJECT_ID")
    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT", "prd")
    return MANIFEST_BUCKET_TPL.format(env_short=env_short, project=project)


def _write_last_run_sentinel() -> None:
    """Record that this host-cron tick FIRED — the dp-meta-watchers host-cron signal.

    Writes ``vm-census/wave-launcher-last-run.json`` (``{"ts": <ISO>}``) to the log
    bucket the fleet monitor reads. The monitor probes its freshness (budget 2x the
    3h cadence) instead of a Cloud Run execution-history cross-check, which is blind
    to a HOST cron → no false ``DP_CRON_DID_NOT_FIRE``. Written at tick START so even
    a 0-gap "backfill complete" tick records the cron fired. Best-effort: a write
    failure must never crash the tick (the launch is the real work).
    """
    try:
        project = _required_env("GCP_PROJECT_ID")
        bucket = _LOG_BUCKET_TPL.format(project=project)
        payload = json.dumps({"ts": datetime.now(UTC).isoformat(), "source": "wave_launcher"}, sort_keys=True)
        storage = get_storage_client()
        # Use the UTL StorageClient.upload_bytes API (the SAME call the dp-* monitors'
        # write_monitor_last_run uses successfully) — the raw `.bucket().blob().upload_from_string()`
        # is not reliably exposed on the cloud-agnostic client, so it failed silently here.
        storage.upload_bytes(
            bucket,
            WAVE_LAUNCHER_LAST_RUN_BLOB,
            payload.encode("utf-8"),
            content_type="application/json",
        )
    except Exception as exc:
        logger.warning("wave_launcher last-run sentinel write failed (best-effort): %s", exc)


# ── Dispatch atom ────────────────────────────────────────────────────────────
@dataclass(frozen=True, order=True)
class Dispatch:
    """One launcher invocation that produces exactly one VM."""

    venue: str
    year: int
    root: str | None  # set only for PER_ROOT_VENUES

    @property
    def cell_key(self) -> tuple[str, str, int]:
        return (self.venue, self.root or "*", self.year)

    def launcher_args(self) -> list[str]:
        args = ["--year", str(self.year), "--no-force-window", "--force"]
        if self.root is not None:
            args = ["--only-root", self.root, *args]
        return args

    def describe(self) -> str:
        root = f" root={self.root}" if self.root else ""
        return f"{self.venue} year={self.year}{root}"


# ── Manifest read + gap computation ──────────────────────────────────────────
def load_manifest() -> pd.DataFrame:
    bucket = _manifest_bucket()
    storage = get_storage_client()
    blob = storage.bucket(bucket).blob(MANIFEST_KEY)
    raw = blob.download_as_bytes()
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded manifest %s/%s rows=%d", bucket, MANIFEST_KEY, len(df))
    return df


def _cme_root_universe() -> set[str]:
    """Parse the CME root list from the launcher (SSOT for valid --only-root)."""
    launcher = VENUE_LAUNCHER_DIR / LAUNCHER_FOR_VENUE["CME"]
    roots: set[str] = set()
    if not launcher.exists():
        return roots
    in_block = False
    for line in launcher.read_text().splitlines():
        stripped = line.strip()
        if "CME_ROOTS=(" in stripped:  # tolerate `declare -a CME_ROOTS=(`
            in_block = True
            continue
        if in_block:
            if stripped.startswith(")"):
                break
            match = re.match(r'"([^|"]+)\|', stripped)
            if match:
                roots.add(match.group(1))
    return roots


def _derive_cme_root(underlying: str, instrument_id: str) -> str | None:
    """Map a manifest row to a CME root symbol, skipping COMBO/derived rows."""
    candidate = (underlying or "").strip()
    if not candidate or ":" in candidate:  # '' or 'CME:COMBO:...' → not a root
        # fall back to instrument_id parse: CME:FUTURE:ESM5 → ES-ish; too fuzzy.
        return None
    return candidate.upper()


def compute_dispatch_candidates(df: pd.DataFrame) -> tuple[list[Dispatch], dict[str, int]]:
    """Return (ordered candidate dispatches, out-of-scope gap summary)."""
    # SOURCE-RESOLVE (2026-06-24): a logical cell is a gap only if NO source captured it.
    # Multi-source union rule: ≥1 captured row for a cell → the whole cell is covered.
    # This prevents orphaned EU rows from a now-secondary source (e.g. massive) from
    # triggering re-dispatch when another source (e.g. databento) already captured the cell.
    # Logical-cell key: (venue, date, data_type, instrument_id) — excludes source/pipeline_mode.
    _CELL_KEY = ["venue", "date", "data_type", "instrument_id"]
    _cell_cols = [c for c in _CELL_KEY if c in df.columns]
    if _cell_cols and "capture_status" in df.columns:
        _captured = df[df["capture_status"] == "captured"][_cell_cols].drop_duplicates()
        if len(_captured):
            # Use a merge to efficiently identify rows whose logical cell is already covered.
            _gap_candidates = df[df["capture_status"].isin(NEEDS_WORK)]
            _merged = _gap_candidates[_cell_cols].merge(_captured.assign(_covered=True), on=_cell_cols, how="left")
            _not_covered = _merged["_covered"].isna()
            df = pd.concat(
                [
                    df[~df["capture_status"].isin(NEEDS_WORK)],
                    _gap_candidates[_not_covered.values],
                ],
                ignore_index=True,
            )
    need = df[df["capture_status"].isin(NEEDS_WORK)].copy()
    need["year"] = pd.to_datetime(need["date"], errors="coerce").dt.year
    need = need[need["year"].notna()]
    need["year"] = need["year"].astype(int)

    # Out-of-scope accounting (reported, never launched).
    out_of_scope: dict[str, int] = {}

    # A cell is addressable iff its venue has a launcher AND its data_type is in
    # THAT venue's addressable set (FX=ohlcv_24h, Databento venues=ohlcv_1m/1s).
    # Vectorised per-venue (mirrors the original .isin() style): OR together each
    # launcher venue's (venue==V) & (data_type in _addressable_data_types(V)).
    addressable_mask = need["venue"].isin([])  # all-False seed of the right index
    for _venue, _dts in ((v, _addressable_data_types(v)) for v in LAUNCHER_FOR_VENUE):
        addressable_mask = addressable_mask | ((need["venue"] == _venue) & need["data_type"].isin(_dts))
    oos = need[~addressable_mask]
    if len(oos):
        for (venue, dtype), grp in oos.groupby(["venue", "data_type"]):
            out_of_scope[f"{venue}:{dtype}"] = len(grp)

    addr = need[addressable_mask]
    cme_roots = _cme_root_universe()

    candidates: dict[tuple[str, str, int], Dispatch] = {}
    for row in addr.itertuples(index=False):
        venue = row.venue
        year = int(row.year)
        if venue in PER_ROOT_VENUES:
            root = _derive_cme_root(getattr(row, "underlying", ""), getattr(row, "instrument_id", ""))
            if root is None or (cme_roots and root not in cme_roots):
                out_of_scope[f"{venue}:unmapped_root"] = out_of_scope.get(f"{venue}:unmapped_root", 0) + 1
                continue
            dispatch = Dispatch(venue=venue, year=year, root=root)
        else:  # PER_YEAR_VENUES
            dispatch = Dispatch(venue=venue, year=year, root=None)
        candidates[dispatch.cell_key] = dispatch

    # Deterministic order: oldest year first (fills history front-to-back),
    # then venue, then root.
    ordered = sorted(candidates.values(), key=lambda d: (d.year, d.venue, d.root or ""))
    return ordered, out_of_scope


# ── Running-VM inspection (idempotency) ──────────────────────────────────────
# Venue-agnostic + timeframe-agnostic: ``ohlcv-1m`` (Databento venues) AND
# ``ohlcv-24h`` (FX/Yahoo). The optional ``root`` segment is the per-root CME
# shard (e.g. ``es``); PER_YEAR venues (incl. FX) carry none.
_VM_NAME_RE = re.compile(
    r"^tradfi-bf-(?P<venue>cme|cboe|nasdaq|nyse|ice|fx)-ohlcv-(?:1m|1s|24h|15m)-"
    r"(?:(?P<root>[a-z0-9-]+?)-)?(?P<year>\d{4})-\d{8}-\d{6}$"
)


def list_running_vms() -> list[str]:
    result = subprocess.run(
        [
            "gcloud",
            "compute",
            "instances",
            "list",
            "--filter",
            f'name~"^{VM_PREFIX}" AND status=RUNNING',
            "--format",
            "value(name)",
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        logger.warning("gcloud list failed rc=%d stderr=%s", result.returncode, result.stderr.strip())
        return []
    return [n for n in result.stdout.splitlines() if n.strip()]


def running_cell_keys(vm_names: Sequence[str]) -> set[tuple[str, str, int]]:
    keys: set[tuple[str, str, int]] = set()
    for name in vm_names:
        match = _VM_NAME_RE.match(name)
        if not match:
            continue
        venue = match.group("venue").upper()
        year = int(match.group("year"))
        root = match.group("root")
        keys.add((venue, (root.upper() if root else "*"), year))
        # PER_YEAR venues are also covered by a wildcard (venue, '*', year).
        keys.add((venue, "*", year))
    return keys


# ── Launch ───────────────────────────────────────────────────────────────────
def launch(dispatch: Dispatch, env: str) -> bool:
    launcher = VENUE_LAUNCHER_DIR / LAUNCHER_FOR_VENUE[dispatch.venue]
    cmd = ["bash", str(launcher), "--env", env, *dispatch.launcher_args()]
    logger.info("LAUNCH %s :: %s", dispatch.describe(), " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600, check=False)
    if result.returncode != 0:
        logger.error(
            "LAUNCH FAILED %s rc=%d stderr=%s",
            dispatch.describe(),
            result.returncode,
            result.stderr.strip()[-500:],
        )
        return False
    logger.info("LAUNCH OK %s :: %s", dispatch.describe(), result.stdout.strip().splitlines()[-1:])
    return True


def _emit(event: str, message: str, **fields: object) -> None:
    try:
        log_event(event, severity="INFO", details={"message": message, **fields})
    except Exception as exc:  # pragma: no cover - alerting best-effort
        logger.warning("event emit failed event=%s err=%s", event, exc)


# ── Main tick ────────────────────────────────────────────────────────────────
def run_tick(*, dry_run: bool) -> int:
    env = os.environ.get("DEPLOYMENT_ENV", "prod")
    max_concurrent = _max_concurrent()

    # Host-cron-fired sentinel FIRST (non-dry-run) — proves this tick ran even if it
    # finds 0 gaps, so the fleet monitor's host-cron freshness probe stays green.
    if not dry_run:
        _write_last_run_sentinel()

    df = load_manifest()
    candidates, out_of_scope = compute_dispatch_candidates(df)

    running = [] if dry_run else list_running_vms()
    running_count = len(running)
    running_keys = running_cell_keys(running)

    # Drop candidates a running VM already owns.
    pending = [d for d in candidates if d.cell_key not in running_keys]

    in_scope_remaining = len(candidates)
    budget = max(0, max_concurrent - running_count)
    wave = pending[:budget]

    print("=" * 72)
    print("TRADFI OHLCV WAVE-LAUNCHER  (dry-run)" if dry_run else "TRADFI OHLCV WAVE-LAUNCHER  (LIVE)")
    print(f"  MAX_CONCURRENT       : {max_concurrent} (ceiling {HARD_CEILING_MAX_CONCURRENT})")
    print(f"  running tradfi-bf VMs: {running_count}")
    print(f"  in-scope OHLCV gaps  : {in_scope_remaining} (venue,root,year) dispatch atoms")
    print(f"  budget this tick     : {budget}")
    print(f"  wave size            : {len(wave)}")
    if out_of_scope:
        print("  out-of-scope gaps (NOT launched — non-OHLCV dt / venue / unmapped root):")
        for key in sorted(out_of_scope):
            print(f"      {key}: {out_of_scope[key]}")
    print("  WAVE:")
    for d in wave:
        print(f"      {d.describe()}   args={' '.join(d.launcher_args())}")
    print("=" * 72)

    # Completion: nothing in-scope left to do.
    if in_scope_remaining == 0:
        msg = "TRADFI BACKFILL COMPLETE — 0 in-scope OHLCV (expected_unattempted+attempted_failed) cells remain"
        print(msg)
        if not dry_run:
            _emit(
                "DP_TRADFI_BACKFILL_COMPLETE",
                msg,
                asset_group=ASSET_GROUP,
                running=running_count,
                out_of_scope=sum(out_of_scope.values()),
            )
        return 0

    if dry_run:
        return 0

    launched = 0
    failed = 0
    for dispatch in wave:
        try:
            if launch(dispatch, env):
                launched += 1
            else:
                failed += 1
        except Exception as exc:  # per-wave error isolation (shard isolation)
            failed += 1
            logger.error("dispatch error %s err=%s", dispatch.describe(), exc)
            continue

    _emit(
        "DP_TRADFI_WAVE_LAUNCHED",
        f"wave launched={launched} failed={failed} running_before={running_count} "
        f"in_scope_remaining={in_scope_remaining}",
        asset_group=ASSET_GROUP,
        launched=launched,
        failed=failed,
        running_before=running_count,
        max_concurrent=max_concurrent,
        in_scope_remaining=in_scope_remaining,
    )
    print(f"LAUNCHED={launched} FAILED={failed} (running_before={running_count})")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Compute + print the wave; launch nothing.")
    args = parser.parse_args(argv)

    try:
        setup_events(service_name="tradfi_wave_launcher", mode="local")
    except Exception as exc:  # pragma: no cover - events best-effort
        logger.warning("setup_events failed (continuing): %s", exc)

    started = datetime.now(UTC)
    rc = run_tick(dry_run=args.dry_run)
    logger.info("wave-launcher tick done rc=%d elapsed=%ss", rc, (datetime.now(UTC) - started).total_seconds())
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
