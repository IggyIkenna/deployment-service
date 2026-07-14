#!/usr/bin/env bash
# Epic: defi_master
# Lifecycle: oneoff
# Delete-when: drift_v2 sig-index gap (2025-01-15 → 2025-12-23) fully walked + parts verified readable
#              by _load_drift_v2_sig_index (mvp_backfill_defi_onchain_v10_2026_06_27.md G1.5 closes)
#
# Launch a Drift V2 sig-index PARALLEL-WALKER segment VM.
#
# Operator ruling (b), 2026-07-14 (defi_perp_funding_mvp_scope_contradiction_2026_06_29.md):
# close the ~11-month unindexed sig-index gap (2025-01-15 → 2025-12-23) with a MODEST number
# (2-3) of parallel SPOT walker segments — no Helius plan upgrade. Each segment runs
# market_tick_data_service.scripts.build_drift_v2_sig_index walking Helius RPC
# getSignaturesForAddress backwards from its anchor, flushing (signature, slot, blockTime)
# parts to GCS. The MTDS reader (_DRIFT_V2_SIG_INDEX_PARTS_PREFIXES) already includes
# _index/drift_v2_sig_index_parts{,_b,_gap}/ so segment output is consumed with no code change.
#
# Segments (each VM = one segment; keep the fleet at 2-3 — every walker shares the ONE
# Helius API key, so aggressive parallelism just converts into 429/backoff waste):
#   * resume-parts:  --resume on the default _parts/ prefix — continues Builder #1
#     backwards from its oldest indexed sig (2025-12-23) toward --back-to.
#   * anchored:      --before-sig <sig> --parts-prefix _index/drift_v2_sig_index_parts_gap/
#     — walks backwards from a mid-gap anchor signature toward --back-to.
#
# Usage:
#   # Segment 1 — resume Builder #1 backwards to the gap midpoint:
#   bash launch-mtds-drift-sig-walker-vm.sh --segment resume --back-to 2025-07-01
#
#   # Segment 2 — anchored mid-gap walker into _parts_gap/ down to the gap floor:
#   bash launch-mtds-drift-sig-walker-vm.sh --segment gap --back-to 2025-01-15 \
#       --before-sig <anchor-sig> --parts-prefix _index/drift_v2_sig_index_parts_gap/
#
# Env overrides:
#   ON_DEMAND=true      opt out of the SPOT default (HARD RULE: backfill VMs default SPOT)
#   MACHINE_TYPE=...    default e2-standard-4 (walker is network-bound; RSS bounded by chunked flush)
#   BOOT_DISK_GB=50     boot disk size
#
# The walker is idempotent under preemption: --resume re-seeds before=<oldest persisted sig>
# from the segment's own parts prefix, so a SPOT preemption costs at most one chunk re-walk.
#
# Output: gs://market-data-tick-defi-prd-{pid}/_index/drift_v2_sig_index_parts*/part-NNNNNN.parquet
# Logs:   gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
SEGMENT=""
BACK_TO=""
BEFORE_SIG=""
PARTS_PREFIX=""
CHUNK_SIZE="100000"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --segment)      SEGMENT="$2"; shift 2 ;;
        --back-to)      BACK_TO="$2"; shift 2 ;;
        --before-sig)   BEFORE_SIG="$2"; shift 2 ;;
        --parts-prefix) PARTS_PREFIX="$2"; shift 2 ;;
        --chunk-size)   CHUNK_SIZE="$2"; shift 2 ;;
        --env)          DEPLOYMENT_ENV="$2"; shift 2 ;;
        --dry-run)      DRY_RUN=true; shift ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ -z "$SEGMENT" || -z "$BACK_TO" ]]; then
    echo "Usage: $0 --segment <label> --back-to <YYYY-MM-DD> [--before-sig <sig> --parts-prefix <gcs-prefix>] [--chunk-size N] [--env prod] [--dry-run]" >&2
    exit 2
fi
if [[ ! "$BACK_TO" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "ERROR: --back-to must be YYYY-MM-DD (got: $BACK_TO)" >&2
    exit 2
fi
# Anchored mode needs BOTH --before-sig and a disjoint --parts-prefix (writing an anchored
# walk into the default _parts/ prefix would interleave chunk_seq with Builder #1's resume).
if [[ -n "$BEFORE_SIG" && -z "$PARTS_PREFIX" ]]; then
    echo "ERROR: --before-sig requires --parts-prefix (disjoint output prefix for the segment)" >&2
    exit 2
fi
if [[ -n "$PARTS_PREFIX" && "$PARTS_PREFIX" != _index/drift_v2_sig_index_parts* ]]; then
    echo "ERROR: --parts-prefix must start with _index/drift_v2_sig_index_parts (got: $PARTS_PREFIX)" >&2
    echo "       (the MTDS reader only loads _DRIFT_V2_SIG_INDEX_PARTS_PREFIXES — an off-list prefix is invisible)" >&2
    exit 2
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
BOOT_DISK_GB="${BOOT_DISK_GB:-50}"

# Backfill/idempotent VMs default to SPOT (HARD RULE: codex/05-infrastructure/spot-vms-for-backfill.md).
# The walker resumes from its own persisted parts, so preemption costs ≤1 chunk re-walk.
if [[ "${ON_DEMAND:-false}" == "true" ]]; then
    PROVISIONING_ARGS=(--provisioning-model=STANDARD)
else
    PROVISIONING_ARGS=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="mtds-drift-sig-walker-${SEGMENT}-${RUN_TS}"

# Build the walker command. --resume is ALWAYS passed: fresh prefixes start from the anchor
# (or HEAD), non-empty prefixes continue from their own oldest persisted sig — safe in both
# modes and preemption-tolerant.
BACKFILL_CMD="python -m market_tick_data_service.scripts.build_drift_v2_sig_index --resume --back-to ${BACK_TO} --chunk-size ${CHUNK_SIZE}"
[[ -n "$BEFORE_SIG" ]] && BACKFILL_CMD="${BACKFILL_CMD} --before-sig ${BEFORE_SIG}"
[[ -n "$PARTS_PREFIX" ]] && BACKFILL_CMD="${BACKFILL_CMD} --parts-prefix ${PARTS_PREFIX}"

# Route via the generic VM_TASK=mdps-backfill BACKFILL_CMD runner (tarball pull + venv,
# then VM_BACKFILL_CMD verbatim with `python ` → `$VENV/bin/python `).
# VM_OPERATION MUST NOT be "download": setup-data-pipeline-vm.sh's generic OOM preflight
# self-deletes any market_tick_data_service VM_OPERATION==download VM when the consolidated
# availability index is stale (the G1.6 mtds-solana-defi-backfill SETUP_EXIT_STATUS=78
# incident, mvp_backfill_defi_onchain_v10_2026_06_27.md) — the walker never reads that index.
METADATA="VM_TASK=mdps-backfill"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=drift-sig-walk"
METADATA="${METADATA},VM_ASSET_GROUP=DEFI"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

echo "Launching ${VM_NAME}: Drift V2 sig-index walker segment=${SEGMENT}"
echo "  back-to=${BACK_TO} before-sig=${BEFORE_SIG:-<resume-seeded>} parts-prefix=${PARTS_PREFIX:-<default _parts/>}"
echo "  cmd: ${BACKFILL_CMD}"

if $DRY_RUN; then
    echo "[DRY RUN] Would create ${VM_NAME} (${MACHINE_TYPE}, SPOT=$([[ "${ON_DEMAND:-false}" == "true" ]] && echo no || echo yes)) with metadata:"
    echo "  ${METADATA}"
    exit 0
fi

lc_verify_tarball_freshness "$CODE_BUCKET" \
    market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
    || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    "${PROVISIONING_ARGS[@]}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=drift-sig-walker,segment="${SEGMENT}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "GCS log:     gcloud storage cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Parts:       gcloud storage ls gs://market-data-tick-defi-prd-${PROJECT}/${PARTS_PREFIX:-_index/drift_v2_sig_index_parts/}"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
