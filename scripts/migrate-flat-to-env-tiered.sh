#!/usr/bin/env bash
# ============================================================================
# migrate-flat-to-env-tiered.sh — Phase 2.6 Step 2.6.2
#
# Copy all objects from flat (legacy, no-env-tier) buckets into env-tiered
# (prod) buckets. This is the bundled runner that replaces per-bucket calls
# to launch-bucket-rsync-vm.sh for direct (no-VM) migration.
#
# GCP copy:  unified_trading_library.cloud_interface.gcs_copy_object
#            (REST API — 250× faster than gcloud/gsutil, GIL-free parallelism)
# AWS copy:  aws s3 sync (with --no-progress for CI-clean output)
#
# Flat naming: {prefix}-{ag}-{project_id}           (no DEPLOYMENT_ENV_SHORT)
# Tiered naming: {prefix}-{ag}-{env_short}-{project_id}  (e.g. -prd-)
#
# Usage
# -----
#   # Dry-run: print plan (object counts + bytes), no copy
#   bash deployment-service/scripts/migrate-flat-to-env-tiered.sh \
#     --env prod --cloud gcp --dry-run
#
#   # Apply GCP only
#   bash deployment-service/scripts/migrate-flat-to-env-tiered.sh \
#     --env prod --cloud gcp --apply
#
#   # Apply AWS only
#   bash deployment-service/scripts/migrate-flat-to-env-tiered.sh \
#     --env prod --cloud aws --apply
#
#   # Apply both clouds
#   bash deployment-service/scripts/migrate-flat-to-env-tiered.sh \
#     --env prod --cloud both --apply
#
# Requirements
# ------------
#   GCP: deployment-service .venv (UTL + google-cloud-storage installed)
#        ADC credentials: gcloud auth application-default login
#   AWS: aws CLI with credentials for AWS_ACCOUNT_ID
#
# Single-walk discipline: this script is THE one migration run per bucket.
#   No partial re-runs on same objects; each run is idempotent (overwrites).
#
# References
# ----------
#   plans/active/code_freeze_migrate_backfill_sequencing_2026_05_10.md
#     § "Step 2.6.2 — Rsync flat→env-tiered"
#   codex/05-infrastructure/gcs-object-operations.md
#   CLAUDE.md § "GCS object ops in migration scripts"
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOYMENT_SERVICE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "${DEPLOYMENT_SERVICE_DIR}/.." && pwd)}"
VENV_PYTHON="${DEPLOYMENT_SERVICE_DIR}/.venv/bin/python3"

# ── Defaults ────────────────────────────────────────────────────────────────
ENV=""
CLOUD="both"
DRY_RUN=1
WORKERS=16

GCP_PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-427895769566}"
AWS_REGION="${AWS_REGION:-ap-northeast-1}"

# ── Usage ───────────────────────────────────────────────────────────────────
usage() {
  grep -E "^#( |$)" "${BASH_SOURCE[0]}" | head -55 | sed 's/^# \?//'
  exit "${1:-0}"
}

# ── Logging ─────────────────────────────────────────────────────────────────
log()  { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*"; }
err()  { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] ERROR: $*" >&2; }

# ── Arg parsing ─────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --env)     ENV="$2"; shift 2 ;;
    --cloud)   CLOUD="$2"; shift 2 ;;  # gcp | aws | both
    --dry-run) DRY_RUN=1; shift ;;
    --apply)   DRY_RUN=0; shift ;;
    --workers) WORKERS="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) err "Unknown argument: $1"; usage 1 ;;
  esac
done

if [[ -z "$ENV" ]]; then
  err "--env is required (prod | staging | dev)"
  usage 1
fi

# env-short (3-char form for bucket names)
case "$ENV" in
  prod|production) ENV_SHORT="prd" ;;
  staging)         ENV_SHORT="stg" ;;
  dev|development) ENV_SHORT="dev" ;;
  test)            ENV_SHORT="test" ;;
  *)               ENV_SHORT="$ENV" ;;
esac

if [[ "$DRY_RUN" -eq 1 ]]; then
  log "MODE: DRY-RUN (pass --apply to execute copies)"
else
  log "MODE: APPLY — writing to ${ENV} (env_short=${ENV_SHORT})"
fi

# ── GCP flat bucket list ─────────────────────────────────────────────────────
# Flat = on-disk legacy name (no DEPLOYMENT_ENV_SHORT segment).
# Tiered = same name with -${ENV_SHORT}- inserted before {project_id}.
# Must stay in sync with archive-flat-buckets.sh § build_gcp_flat_bucket_list.
build_gcp_pairs() {
  local pid="$1"
  local env_short="$2"
  local pairs=()

  _pair() { pairs+=("gs://${1}-${pid}" "gs://${1}-${env_short}-${pid}"); }
  _pair_ag() {
    local prefix="$1"
    for ag in cefi defi tradfi sports; do
      pairs+=("gs://${prefix}-${ag}-${pid}" "gs://${prefix}-${ag}-${env_short}-${pid}")
    done
    pairs+=("gs://${prefix}-pred-${pid}" "gs://${prefix}-pred-${env_short}-${pid}")
  }

  # Tier 1 — DeFi raw on-chain
  for kind in dex-pools dex-swaps evm-defi eigenlayer-rewards solana-defi; do
    _pair "$kind"
  done

  # Tier 2 — instruments-store × asset_groups
  for ag in cefi defi tradfi sports; do
    pairs+=("gs://instruments-store-${ag}-${pid}" "gs://instruments-store-${ag}-${env_short}-${pid}")
  done
  pairs+=("gs://instruments-store-pred-${pid}" "gs://instruments-store-pred-${env_short}-${pid}")

  # Tier 3 — features cross-asset
  _pair "features-calendar"
  _pair "features-pred"
  _pair "features-sports"

  # Tier 4 — features × kind × asset_group
  for kind in features-delta-one features-volatility features-xinstrument features-mtf; do
    for ag in cefi tradfi defi pred sports; do
      pairs+=("gs://${kind}-${ag}-${pid}" "gs://${kind}-${ag}-${env_short}-${pid}")
    done
  done
  for ag in cefi defi; do
    pairs+=("gs://features-onchain-${ag}-${pid}" "gs://features-onchain-${ag}-${env_short}-${pid}")
  done

  # Tier 5 — ML stores
  for kind in ml-models-store ml-predictions-store ml-configs-store ml-training-artifacts ml-artifacts; do
    _pair "$kind"
  done

  # Tier 6 — strategy + execution
  for ag in cefi tradfi defi pred; do
    pairs+=("gs://strategy-store-${ag}-${pid}" "gs://strategy-store-${ag}-${env_short}-${pid}")
    pairs+=("gs://execution-store-${ag}-${pid}" "gs://execution-store-${ag}-${env_short}-${pid}")
  done

  # Tier 7 — market-data
  for ag in cefi defi tradfi sports pred; do
    pairs+=("gs://market-data-tick-${ag}-${pid}" "gs://market-data-tick-${ag}-${env_short}-${pid}")
  done

  printf '%s\n' "${pairs[@]}"
}

# ── AWS flat bucket list ─────────────────────────────────────────────────────
build_aws_pairs() {
  local acct="$1"
  local env_short="$2"
  local pairs=()

  # Tier 1 — DeFi raw on-chain
  for kind in dex-pools dex-swaps evm-defi eigenlayer-rewards solana-defi; do
    pairs+=("unified-trading-${kind}-${acct}" "unified-trading-${kind}-${env_short}-${acct}")
  done

  # Tier 2 — instruments × asset_groups
  for ag in cefi defi tradfi sports; do
    pairs+=("unified-trading-instruments-${ag}-${acct}" "unified-trading-instruments-${ag}-${env_short}-${acct}")
  done
  pairs+=("unified-trading-instruments-pred-${acct}" "unified-trading-instruments-pred-${env_short}-${acct}")

  # Tier 3 — features cross-asset
  for kind in features-calendar features-pred features-sports; do
    pairs+=("unified-trading-${kind}-${acct}" "unified-trading-${kind}-${env_short}-${acct}")
  done

  # Tier 4 — features × kind × asset_group
  for kind in features-delta-one features-volatility features-xinstrument features-mtf; do
    for ag in cefi tradfi defi pred sports; do
      pairs+=("unified-trading-${kind}-${ag}-${acct}" "unified-trading-${kind}-${ag}-${env_short}-${acct}")
    done
  done
  for ag in cefi defi; do
    pairs+=("unified-trading-features-onchain-${ag}-${acct}" "unified-trading-features-onchain-${ag}-${env_short}-${acct}")
  done

  # Tier 5 — ML stores
  for kind in ml-models ml-predictions ml-configs ml-training-artifacts ml-artifacts; do
    pairs+=("unified-trading-${kind}-${acct}" "unified-trading-${kind}-${env_short}-${acct}")
  done

  # Tier 6 — strategy + execution
  for ag in cefi tradfi defi pred; do
    pairs+=("unified-trading-strategy-${ag}-${acct}" "unified-trading-strategy-${ag}-${env_short}-${acct}")
    pairs+=("unified-trading-execution-${ag}-${acct}" "unified-trading-execution-${ag}-${env_short}-${acct}")
  done

  # Tier 7 — market-data
  for ag in cefi defi tradfi sports pred; do
    pairs+=("unified-trading-market-data-${ag}-${acct}" "unified-trading-market-data-${ag}-${env_short}-${acct}")
  done

  printf '%s\n' "${pairs[@]}"
}

# ── GCP migration — Python via UTL gcs_copy_object ──────────────────────────
do_gcp_migration() {
  local dry_run="$1"
  local workers="$2"
  local pid="$3"
  local env_short="$4"

  if [[ ! -x "$VENV_PYTHON" ]]; then
    err "deployment-service .venv not found at $VENV_PYTHON"
    err "Run: cd ${DEPLOYMENT_SERVICE_DIR} && bash scripts/setup.sh"
    return 1
  fi

  log "GCP: running migration via UTL gcs_copy_object (workers=${workers})"

  # Write pairs to temp file so Python can read without stdin conflict
  local tmp_pairs
  tmp_pairs="$(mktemp /tmp/migrate-flat-gcp-pairs-XXXXXX)"
  trap "rm -f '${tmp_pairs}'" RETURN
  build_gcp_pairs "$pid" "$env_short" | paste - - > "$tmp_pairs"

  "$VENV_PYTHON" - "$dry_run" "$workers" "$tmp_pairs" <<'PYEOF'
from __future__ import annotations
import sys
import concurrent.futures
import threading
from google.cloud import storage  # google-cloud-storage (available in .venv)
from unified_trading_library.cloud_interface import gcs_copy_object  # type: ignore[import]

dry_run = sys.argv[1] == "1"
workers = int(sys.argv[2])
pairs_file = sys.argv[3]

# Read src\tdest pairs from temp file
with open(pairs_file) as f:
    pair_lines = f.read().strip().split('\n')
pairs: list[tuple[str, str]] = []
for line in pair_lines:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t', 1)
    if len(parts) == 2:
        pairs.append((parts[0].strip(), parts[1].strip()))

client = storage.Client()

def bucket_exists(uri: str) -> bool:
    bname = uri.removeprefix("gs://").split("/")[0]
    try:
        client.get_bucket(bname)
        return True
    except Exception:
        return False

def copy_bucket(src_uri: str, dest_uri: str) -> dict[str, object]:
    src_bucket = src_uri.removeprefix("gs://")
    dest_bucket = dest_uri.removeprefix("gs://")
    if not bucket_exists(src_uri):
        return {"src": src_uri, "dest": dest_uri, "skipped": True, "reason": "src_missing", "count": 0}
    if not bucket_exists(dest_uri):
        return {"src": src_uri, "dest": dest_uri, "skipped": True, "reason": "dest_missing", "count": 0}

    blobs = list(client.list_blobs(src_bucket))
    total = len(blobs)
    if total == 0:
        return {"src": src_uri, "dest": dest_uri, "skipped": False, "count": 0, "bytes": 0}

    total_bytes = sum(b.size or 0 for b in blobs)

    if dry_run:
        print(f"  DRY-RUN  {src_uri} → {dest_uri}  objects={total}  bytes={total_bytes:,}")
        return {"src": src_uri, "dest": dest_uri, "skipped": False, "count": total, "bytes": total_bytes, "dry_run": True}

    copied = 0
    lock = threading.Lock()

    def copy_one(blob: storage.Blob) -> None:
        nonlocal copied
        gcs_copy_object(
            f"gs://{src_bucket}/{blob.name}",
            f"gs://{dest_bucket}/{blob.name}",
        )
        with lock:
            copied += 1
            if copied % 1000 == 0:
                print(f"  {src_uri}: {copied}/{total}", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(copy_one, blobs))

    print(f"  DONE  {src_uri} → {dest_uri}  objects={total}  bytes={total_bytes:,}")
    return {"src": src_uri, "dest": dest_uri, "skipped": False, "count": total, "bytes": total_bytes}

total_objects = 0
total_bytes = 0
skipped = 0
errors: list[str] = []

for src, dest in pairs:
    try:
        r = copy_bucket(src, dest)
        if r.get("skipped"):
            skipped += 1
        else:
            total_objects += r.get("count", 0)
            total_bytes += r.get("bytes", 0)
    except Exception as exc:
        errors.append(f"{src}: {exc}")
        print(f"  ERROR  {src}: {exc}", file=sys.stderr)

print(f"\nGCP SUMMARY: pairs={len(pairs)}  skipped={skipped}  objects={total_objects}  bytes={total_bytes:,}  errors={len(errors)}")
if errors:
    print("ERRORS:")
    for e in errors:
        print(f"  {e}", file=sys.stderr)
    sys.exit(1)
PYEOF
}

# ── AWS migration — aws s3 sync ──────────────────────────────────────────────
do_aws_migration() {
  local dry_run="$1"
  local acct="$2"
  local env_short="$3"
  local region="$4"

  local pairs_arr=()
  while IFS=$'\t' read -r src dest; do
    [[ -z "$src" || -z "$dest" ]] && continue
    pairs_arr+=("$src" "$dest")
  done < <(build_aws_pairs "$acct" "$env_short" | paste - -)

  local total_pairs=$(( ${#pairs_arr[@]} / 2 ))
  log "AWS: ${total_pairs} bucket pairs to process (workers serial — aws s3 sync internal parallelism)"

  local i=0
  while [[ $i -lt ${#pairs_arr[@]} ]]; do
    local src="${pairs_arr[$i]}"
    local dest="${pairs_arr[$((i+1))]}"
    i=$((i+2))

    # Check source exists
    if ! aws s3api head-bucket --bucket "$src" --region "$region" 2>/dev/null; then
      log "  SKIP  s3://${src} — does not exist"
      continue
    fi
    # Check dest exists
    if ! aws s3api head-bucket --bucket "$dest" --region "$region" 2>/dev/null; then
      log "  SKIP  s3://${dest} — dest does not exist (provision first)"
      continue
    fi

    if [[ "$dry_run" -eq 1 ]]; then
      local obj_count
      obj_count=$(aws s3 ls "s3://${src}" --recursive --region "$region" 2>/dev/null | wc -l | tr -d ' ')
      log "  DRY-RUN  s3://${src} → s3://${dest}  objects~${obj_count}"
    else
      log "  SYNC  s3://${src} → s3://${dest}"
      aws s3 sync "s3://${src}/" "s3://${dest}/" \
        --region "$region" \
        --no-progress \
        --sse aws:kms \
        --exact-timestamps \
        2>&1 | tail -5
      log "  DONE  s3://${src} → s3://${dest}"
    fi
  done

  log "AWS migration complete."
}

# ── Main ────────────────────────────────────────────────────────────────────
log "migrate-flat-to-env-tiered.sh  env=${ENV}  env_short=${ENV_SHORT}  cloud=${CLOUD}  dry_run=${DRY_RUN}  workers=${WORKERS}"
log "GCP_PROJECT_ID=${GCP_PROJECT_ID}  AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID}"

case "$CLOUD" in
  gcp|both)
    log "=== GCP migration ==="
    do_gcp_migration "$DRY_RUN" "$WORKERS" "$GCP_PROJECT_ID" "$ENV_SHORT"
    ;;
esac

case "$CLOUD" in
  aws|both)
    log "=== AWS migration ==="
    do_aws_migration "$DRY_RUN" "$AWS_ACCOUNT_ID" "$ENV_SHORT" "$AWS_REGION"
    ;;
esac

log "Done."
