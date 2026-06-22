#!/usr/bin/env python3
# Epic: tradfi_master
# Lifecycle: permanent
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

Dispatch model (1 dispatch == exactly 1 VM)
-------------------------------------------
The per-venue OHLCV launchers
(``launch-tradfi-bf-{cme,cboe,nasdaq,nyse,ice}-ohlcv-1m.sh``) shard differently:

* CME / CBOE: one VM per (root, year). CME exposes ``--only-root`` so we narrow
  to a single root → 1 VM. CBOE has a single root (VX) so ``--year`` alone is
  already 1 VM.
* NASDAQ / NYSE: the whole equity ticker universe rides ONE VM per year, so
  ``--year`` alone is 1 VM.

So the wave-launcher's atom is "one launcher invocation that produces exactly
one VM", which makes the MAX_CONCURRENT cap trivially exact (budget == VMs).

These launchers fetch ``ohlcv_1m`` + ``ohlcv_1s`` only (the registry's L0/free
Databento OHLCV path; 15m/24h aggregate downstream via MDPS). So the
wave-launcher's addressable surface is exactly the ``ohlcv_1m`` / ``ohlcv_1s``
gap cells at the OHLCV venues. Non-OHLCV data_types (trades / tbbo / mbp_10 /
earnings / corporate_action) and non-OHLCV venues (FX / YAHOO_FINANCE /
UNKNOWN) are NOT in scope for this launcher and are reported but never launched.

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
import logging
import os
import re
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from unified_trading_library.cloud_interface import get_storage_client
from unified_trading_library.events import log_event, setup_events

logger = logging.getLogger("tradfi_wave_launcher")

# ── Constants ────────────────────────────────────────────────────────────────
ASSET_GROUP = "tradfi"
MANIFEST_BUCKET_TPL = "market-data-tick-tradfi-{env_short}-{project}"
MANIFEST_KEY = "_index/availability_index.parquet"

# Statuses that count as "needs work". attempted_failed is the P1 retry.
NEEDS_WORK = frozenset({"expected_unattempted", "attempted_failed"})

# The per-venue OHLCV launchers only fetch these data_types.
OHLCV_DATA_TYPES = frozenset({"ohlcv_1m", "ohlcv_1s"})

# Venues with an OHLCV backfill launcher. ICE has no declared roots yet
# (scaffolding launcher) so it is intentionally NOT dispatched.
LAUNCHER_FOR_VENUE: dict[str, str] = {
    "CME": "launch-tradfi-bf-cme-ohlcv-1m.sh",
    "CBOE": "launch-tradfi-bf-cboe-ohlcv-1m.sh",
    "NASDAQ": "launch-tradfi-bf-nasdaq-ohlcv-1m.sh",
    "NYSE": "launch-tradfi-bf-nyse-ohlcv-1m.sh",
}

# Venues that shard one-VM-per-(root, year) and expose --only-root, so the
# wave-launcher narrows to a single root per dispatch.
PER_ROOT_VENUES = frozenset({"CME"})

# Venues that shard one-VM-per-year (whole universe in a single VM).
PER_YEAR_VENUES = frozenset({"CBOE", "NASDAQ", "NYSE"})

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
    need = df[df["capture_status"].isin(NEEDS_WORK)].copy()
    need["year"] = pd.to_datetime(need["date"], errors="coerce").dt.year
    need = need[need["year"].notna()]
    need["year"] = need["year"].astype(int)

    # Out-of-scope accounting (reported, never launched).
    out_of_scope: dict[str, int] = {}
    addressable_mask = need["venue"].isin(LAUNCHER_FOR_VENUE) & need["data_type"].isin(OHLCV_DATA_TYPES)
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
_VM_NAME_RE = re.compile(
    r"^tradfi-bf-(?P<venue>cme|cboe|nasdaq|nyse|ice)-ohlcv-1m-"
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
