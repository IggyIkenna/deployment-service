#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a dedicated GCE VM to backfill dex_pool_state for the Solana DEX
# venues ORCA / RAYDIUM / KAMINO — analogous to other dedicated per-protocol
# Solana backfill VMs.
# (mvp_backfill_defi_onchain_v10_2026_06_27.md G1.6.)
#
# Registered prefix: "mtds-solana-defi-backfill" was already listed in
# vm_zombie_watchdog.VM_PREFIX_TO_BUCKET + launcher_registry (mapped to None,
# "multi-protocol fan-out — no single-VM launcher") — this script fills that
# gap. Update launcher_registry.py's entry to point here when this ships.
#
# Root cause of the ORCA/RAYDIUM/KAMINO gap: the original mtds-dex-pools-backfill
# / mtds-dex-swaps-backfill VMs iterate protocols via DexPoolsHandler /
# DexSwapsHandler, which resolve chains through UAC
# get_supported_chains_for_protocol() — a SUBGRAPH_IDS-only lookup. ORCA /
# RAYDIUM / KAMINO have no subgraph (they're REST-API venues), so that lookup
# returns [] and the handlers skip them entirely — dead code, not a real
# attempt. The working, already-shipped path is SolanaDefiHandler
# (--operation collect-solana-defi, VM_TASK=solana-defi-backfill), which
# hardcodes its own Solana protocol list and includes the forward-only-honest
# write gate (_filter_rows_to_target_day, incident
# solana_defi_fake_history_snapshot_2026_06_17.md): the ORCA/RAYDIUM/KAMINO
# REST endpoints expose only the CURRENT pool/vault set (no historical
# endpoint), so a historical backfill date's now-snapshot rows are dropped and
# recorded as honest absence (record_zero_rows -> EXPECTED_PRE_VENUE_LAUNCH or
# SOURCE_RETURNED_ZERO) instead of being falsely back-dated as "captured".
# This still resolves every IS-seeded expected_unattempted cell in the
# manifest (attempted_failed=0 / expected_unattempted=0 target) even though
# genuine "captured" rows only land for the day the VM is actually running
# (today) — the same accepted shape as the marginfi/solend Solana legs
# already running under this plan.
#
# NOTE — dex_pool_swaps is OUT OF SCOPE for this launcher: no code path in
# market-tick-data-service produces individual swap events for
# ORCA/RAYDIUM/KAMINO (SolanaDefiHandler/_solana_defi_fetch.py only implement
# dex_pool_state via REST pool-list snapshots; DexSwapsHandler has no Solana
# routing at all — get_subgraph_id(protocol, "SOLANA") is always None for
# these venues). Building a swap-level Solana indexer is new capability, not
# a VM launch — tracked as a follow-up finding, not absorbed here.
#
# Pattern A (canonical tarball) — startup-script-url=gs://.../vm/setup-data-pipeline-vm.sh
# Pre-condition: run `bash create-code-tarballs.sh` first (CORE tarballs include MTDS).
#
# Usage:
#   bash launch-mtds-solana-defi-backfill-vm.sh
#   bash launch-mtds-solana-defi-backfill-vm.sh --start 2024-01-01 --end 2026-01-01
#   bash launch-mtds-solana-defi-backfill-vm.sh --protocols "kamino;orca;raydium"
#   bash launch-mtds-solana-defi-backfill-vm.sh --dry-run
#   bash launch-mtds-solana-defi-backfill-vm.sh --env staging
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-central-element-323112}"
ZONE="${ZONE:-asia-northeast1-c}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
DRY_RUN=false
START_DATE="${START_DATE:-2023-01-01}"
END_DATE="${END_DATE:-$(date +%Y-%m-%d)}"
# ORCA/RAYDIUM/KAMINO only per G1.6 scope — phoenix has no working public API
# (api.phoenix.trade DNS dead) and lending/lst protocols have their own
# dedicated launchers already (jito/marinade).
# ';' separator (not ',') — gcloud --metadata uses ',' as the KEY separator.
SOLANA_PROTOCOLS="${SOLANA_PROTOCOLS:-kamino;orca;raydium}"
FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)     DRY_RUN=true; shift ;;
    --project)     PROJECT_ID="$2"; shift 2 ;;
    --zone)        ZONE="$2"; shift 2 ;;
    --start)       START_DATE="$2"; shift 2 ;;
    --end)         END_DATE="$2"; shift 2 ;;
    --protocols)   SOLANA_PROTOCOLS="$2"; shift 2 ;;
    --force)       FORCE=true; shift ;;
    --env)         DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

CODE_BUCKET="deployment-scripts-${PROJECT_ID}"
VM_NAME="${VM_NAME:-mtds-solana-defi-backfill}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/launcher_common.sh"

echo "============================================================"
echo "Solana DeFi (dex_pool_state: ORCA/RAYDIUM/KAMINO) Backfill VM Launcher (Pattern A)"
echo "  VM:        ${VM_NAME}"
echo "  Project:   ${PROJECT_ID}"
echo "  Zone:      ${ZONE}"
echo "  Machine:   ${MACHINE_TYPE}"
echo "  Range:     ${START_DATE} → ${END_DATE}"
echo "  Protocols: ${SOLANA_PROTOCOLS}"
echo "  Env:       ${DEPLOYMENT_ENV}"
echo "  Tarball:   gs://${CODE_BUCKET}/code/mtds-code.tar.gz"
echo "============================================================"

if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter="name~\"^mtds-solana-defi-\" AND status=RUNNING" \
    --zones="${ZONE}" \
    --project="${PROJECT_ID}" \
    --format='value(name)' 2>/dev/null | head -1 || true)"
  if [[ -n "$EXISTING" ]]; then
    echo "WARN: Solana DeFi backfill VM already running: ${EXISTING}" >&2
    echo "      Use --force to bypass. Aborting." >&2
    exit 1
  fi
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would launch VM ${VM_NAME} — skipping gcloud create."
  echo "  startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
  echo "  VM_TASK=solana-defi-backfill  VM_SOLANA_PROTOCOLS=${SOLANA_PROTOCOLS}"
  exit 0
fi

METADATA="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
METADATA="${METADATA},VM_TASK=solana-defi-backfill"
# Declare the REAL CLI operation this VM_TASK branch runs (collect-solana-defi,
# hardcoded in setup-data-pipeline-vm.sh's solana-defi-backfill branch regardless
# of this metadata value). Without this, VM_OPERATION defaults to "download" and
# the generic OOM preflight (setup-data-pipeline-vm.sh ~L867) treats this VM as a
# bulk manifest-merge job and self-deletes (exit 78) whenever the defi
# manifest-consolidator index is stale — a false positive for this small
# per-date REST-fetch operation, which never reads the consolidated index.
METADATA="${METADATA},VM_OPERATION=collect-solana-defi"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_SOLANA_PROTOCOLS=${SOLANA_PROTOCOLS}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},MANIFEST_CONSOLIDATED_STALENESS_SEC=86400"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"

# SPOT by default (--no-restart-on-failure already passed below); --on-demand clears it.
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
  --scopes=cloud-platform \
  --no-restart-on-failure \
  ${PROVISIONING_FLAGS} \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  # Download-heavy backfill VM: pd-balanced >=250GB is MANDATORY. A pd-standard 50GB
  # boot disk sustains only ~6 MB/s of writes and its burst credits deplete by CUMULATIVE
  # BYTES WRITTEN — measured 2026-07-18, it throttled the CeFi backfill to 2.36 MB/s after
  # ~7.5GB (iostat %util 99.94, w_await 1015ms, CPU idle, RAM free). On pd-balanced 250GB the
  # same workload sustained 11.1 MB/s to 18.7GB+ with peaks of 18.15 MB/s — a 4.7x gain.
  # Enforced by scripts/quality_gates/check_backfill_vm_disk_provisioning.py.
  --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
  --labels="purpose=mtds-solana-defi-backfill,env=${DEPLOYMENT_ENV}" \
  --metadata="${METADATA}"

echo ""
echo "  VM created: ${VM_NAME}"
echo "  T+10 check: gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'"
echo "  Logs:       gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo ""
echo "============================================================"
echo "Done."
echo "============================================================"
