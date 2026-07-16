#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch GCE VM for Drift historical data backfill (Solana DeFi)
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Converted from inline STARTUP_FILE heredoc (O-1 launcher consolidation, 2026-05-21).
# Pre-condition: run `bash create-code-tarballs.sh` first (CORE tarballs include MTDS).
# Handler: VM_TASK=solana-drift-backfill in setup-data-pipeline-vm.sh.
#
# Migrated 2026-07-16 (issues/drift_helius_path_obsolete_2026_07_15.md): routes to
# the Velocity Data API ingester (backfill_drift_v2_historical.py) — the legacy
# Helius sig-index walker path is obsolete (intractable sig/day wall). Downloads
# Drift funding + trades and writes to GCS as parquet. Machine defaults to
# e2-highmem-8 (not e2-standard-4) per the manifest-index-read OOM caveat tracked
# in manifest_index_read_oom_canonical_cache_2026_06_24.md — this handler's RSS
# climbs sharply during record_captured()/manifest close on smaller machines.
#
# 2026-07-16 (plan mvp_backfill_defi_onchain_v10_2026_06_27.md G1.5): the
# 2026-07-16 initial migration run only covered ONE market (SOL-PERP, the prior
# hardcoded default) — 51,301 expected_unattempted cells remained across the
# other DRIFT markets/full history. DRIFT_MARKET is now DRIFT_MARKETS (plural,
# comma-separated; --market/DRIFT_MARKET kept as single-value back-compat
# aliases). With no --market(s) override, the launcher derives the FULL DRIFT
# PERPETUAL market list live from the instruments-service defi catalogue (the
# same catalog.parquet enumerate_expected_universe.py reads) instead of a
# hand-typed list — see _resolve_drift_markets() below.
#
# Usage:
#   bash launch-mtds-solana-drift-backfill-vm.sh                       # full catalogue market list, full history
#   bash launch-mtds-solana-drift-backfill-vm.sh --market BTC-PERP
#   bash launch-mtds-solana-drift-backfill-vm.sh --markets SOL-PERP,BTC-PERP,ETH-PERP
#   bash launch-mtds-solana-drift-backfill-vm.sh --start 2024-01-01 --end 2026-01-01
#   bash launch-mtds-solana-drift-backfill-vm.sh --dry-run
#   bash launch-mtds-solana-drift-backfill-vm.sh --env staging
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-highmem-8}"
DRY_RUN=false
# Drift v2 mainnet launch (instruments-service _solana_utils.py
# SOLANA_PROTOCOL_DEPLOY_DATES["drift"]) — full per-market history floor, not
# the prior 180-day rolling default (this launcher now targets a full-history
# gap-fill, not a rolling top-up).
START_DATE="${START_DATE:-2022-11-04}"
END_DATE="${END_DATE:-$(date +%Y-%m-%d)}"
DRIFT_MARKETS="${DRIFT_MARKETS:-${DRIFT_MARKET:-}}"
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)  DRY_RUN=true; shift ;;
    --project)  PROJECT_ID="$2"; shift 2 ;;
    --zone)     ZONE="$2"; shift 2 ;;
    --start)    START_DATE="$2"; shift 2 ;;
    --end)      END_DATE="$2"; shift 2 ;;
    --market)   DRIFT_MARKETS="$2"; shift 2 ;;
    --markets)  DRIFT_MARKETS="$2"; shift 2 ;;
    --force)    FORCE=true; shift ;;
    --env)      DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Derive the full DRIFT PERPETUAL market list from the live instruments-service
# defi catalogue when the caller didn't pin a specific market/list — never
# hand-typed (plan G1.5 requirement). Reads the same
# gs://instruments-store-defi-{env}-{project}/prod/catalog.parquet
# enumerate_expected_universe.py uses to derive the expected_unattempted
# denominator, filtered to venue=DRIFT, instrument_type in (PERP, PERPETUAL).
_resolve_drift_markets() {
  local py="${REPO_ROOT}/.venv/bin/python"
  [[ -x "$py" ]] || py="python3"
  GCP_PROJECT_ID="${PROJECT_ID}" PYTHONPATH="${REPO_ROOT}" "$py" - <<'PYEOF'
from unified_trading_library import resolve_bucket_name
from google.cloud import storage
import pyarrow.parquet as pq
import io

bucket_name = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")
client = storage.Client()
blob = client.bucket(bucket_name).blob("prod/catalog.parquet")
data = blob.download_as_bytes()
table = pq.read_table(io.BytesIO(data), columns=["venue", "instrument_type", "raw_symbol"])
df = table.to_pandas()
d = df[df["venue"].astype(str).str.upper() == "DRIFT"]
perp = d[d["instrument_type"].astype(str).str.upper().isin(["PERP", "PERPETUAL"])]
markets = sorted(perp["raw_symbol"].astype(str).unique().tolist())
print(",".join(markets))
PYEOF
}

if [[ -z "$DRIFT_MARKETS" ]]; then
  echo "No --market(s) given — deriving full DRIFT PERPETUAL market list from the instruments-service catalogue..." >&2
  DRIFT_MARKETS="$(_resolve_drift_markets)"
  if [[ -z "$DRIFT_MARKETS" ]]; then
    echo "ERROR: catalogue-derived DRIFT market list is empty — pass --markets explicitly or check catalogue availability." >&2
    exit 1
  fi
  echo "Derived $(echo "$DRIFT_MARKETS" | tr ',' '\n' | wc -l | tr -d ' ') DRIFT markets from catalogue: ${DRIFT_MARKETS}" >&2
fi

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="mtds-solana-drift-backfill"

source "${SCRIPT_DIR}/lib/launcher_common.sh"

# gcloud instance metadata reserves ',' as the k1=v1,k2=v2 separator (same
# convention VM_DATA_TYPES/VM_SOLANA_PROTOCOLS already use) — join the market
# list with ';' for the metadata value; setup-data-pipeline-vm.sh converts it
# back to ',' before handing it to --markets.
DRIFT_MARKETS_METADATA="${DRIFT_MARKETS//,/;}"
DRIFT_MARKET_COUNT="$(echo "$DRIFT_MARKETS" | tr ',' '\n' | wc -l | tr -d ' ')"

echo "============================================================"
echo "Solana Drift Backfill VM Launcher (Pattern A)"
echo "  VM:        ${VM_NAME}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Markets:   ${DRIFT_MARKET_COUNT} — ${DRIFT_MARKETS}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^mtds-solana-drift-\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    echo "WARN: Solana Drift VM already running: ${EXISTING}" >&2
    echo "      Use --force to bypass. Aborting." >&2
    exit 1
  fi
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=solana-drift-backfill  VM_DRIFT_MARKET=${DRIFT_MARKETS_METADATA}"
  exit 0
fi

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=solana-drift-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_DRIFT_MARKET=${DRIFT_MARKETS_METADATA}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Creating VM ${VM_NAME} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]..."
# shellcheck disable=SC2086
if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  ${PROVISIONING_FLAGS} \
  --scopes=cloud-platform \
  --no-restart-on-failure \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --labels="purpose=mtds-solana-drift-backfill,env=${DEPLOYMENT_ENV}" \
  --metadata="${METADATA}"

echo ""
echo "  VM created: ${VM_NAME}"
echo "  T+10 check: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:       gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
