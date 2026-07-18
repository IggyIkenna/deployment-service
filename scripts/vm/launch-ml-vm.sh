#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch an ML VM for ml-service — runs training, inference, or evaluation
# for a single model-family / instrument / target-type combination on GCE,
# writing artefacts to the ml model_registry in GCS. Consolidates the former
# launch-ml-training-vm.sh (archived 2026-05-20, ml_repo_consolidation plan).
#
# NOT singleton-locked: parallel runs are expected (different instruments,
# different target types, different hyper-param grids). The ml-service does not
# share a rate-limited API key — it reads local parquet features + fits/infers
# models locally.
#
# Runs the unified ml-service CLI via setup-data-pipeline-vm.sh metadata
# routing (the VM_TASK=ml-training branch routes through VM_BACKFILL_CMD,
# which carries the full command string verbatim).
#
# Example assembled command (training):
#   python -m ml_service \
#     --operation train --mode batch --asset-group TRADFI \
#     --instruments ES_FRONT --timeframes 1m \
#     --target-types swing_high swing_low \
#     --start-date 2022-01-01 --end-date 2025-12-31
#
# Example assembled command (inference):
#   python -m ml_service \
#     --operation infer --mode batch --asset-group TRADFI \
#     --instruments ES_FRONT --start-date 2026-01-01 --end-date 2026-05-20
#
# Prerequisites:
#   - Tarballs uploaded via `bash create-code-tarballs.sh --ml-training`
#   - Historical feature parquet present in the feature-group GCS paths
#   - model_registry GCS bucket provisioned
#
# Usage:
#   bash launch-ml-vm.sh --dry-run                    # preview
#   bash launch-ml-vm.sh \
#       --asset-group TRADFI --instruments ES_FRONT \
#       --target-types swing_high swing_low \
#       --timeframes 1m \
#       --start-date 2022-01-01 --end-date 2025-12-31
#   bash launch-ml-vm.sh --operation infer ...        # inference run
#   bash launch-ml-vm.sh --gpu ...                    # use GPU machine
#
# Machine type:
#   --machine cpu   → n2-highmem-8 (64 GB RAM, no GPU — default)
#                     enough for LightGBM / XGBoost / CatBoost on 5-year
#                     1-minute data.
#   --machine gpu   → n1-standard-8 + 1×T4 (~$0.35/h) — only if the harness
#                     config uses a GPU-enabled booster. Most swing_high /
#                     swing_low models do fine on CPU.
#   --machine high  → n2-highmem-16 (128 GB RAM, no GPU) — for larger
#                     hyper-param grids or Optuna multi-trial.
#
# Observability: inherits the STALL_TIMEOUT_SEC=600 log-mtime watchdog +
# `timeout 30s` run_heartbeat + py-spy stack dump + pkill-by-name fallback
# from vm-exec-with-gcs-tee.sh (deployed 2026-04-19 after the VM silent-
# hang class bug — memory project_vm_hang_class_bug_and_mdps_setup_fix).
set -euo pipefail

# ── Defaults + arg parsing ──
DRY_RUN=false
ASSET_GROUP="TRADFI"
INSTRUMENTS=""
TARGET_TYPES=""
TIMEFRAMES=""
START_DATE=""
END_DATE=""
MACHINE_CHOICE="cpu"
OPERATION="train"
EXTRA_METADATA=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)         DRY_RUN=true; shift ;;
        --asset-group)        ASSET_GROUP="${2^^}"; shift 2 ;;
        --instruments)     INSTRUMENTS="$2"; shift 2 ;;
        --target-types)    TARGET_TYPES="$2"; shift 2 ;;
        --timeframes)      TIMEFRAMES="$2"; shift 2 ;;
        --start-date)      START_DATE="$2"; shift 2 ;;
        --end-date)        END_DATE="$2"; shift 2 ;;
        --machine)         MACHINE_CHOICE="${2,,}"; shift 2 ;;
        --operation)       OPERATION="$2"; shift 2 ;;
        --extra-metadata)  EXTRA_METADATA="$2"; shift 2 ;;
        --env)             DEPLOYMENT_ENV="$2"; shift 2 ;;
        --help|-h)
            grep '^#' "$0" | head -60
            exit 0
            ;;
        *)
            echo "Unknown arg: $1" >&2
            echo "Usage: $0 [--dry-run] [--asset-group TRADFI|CEFI|SPORTS] [--instruments 'ES_FRONT;BTC'] [--target-types 'swing_high;swing_low'] [--timeframes '1m;1h'] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--machine cpu|gpu|high] [--operation train|infer|evaluate|grid-search|pipeline] [--env <prod|staging|dev>]" >&2
            exit 1
            ;;
    esac
done

if [[ -z "$INSTRUMENTS" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "ERROR: --instruments, --start-date, --end-date are required" >&2
    exit 1
fi

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"  # feature parquet + model artefacts can be 10-30 GB per run

# Machine-type resolution — CPU default, GPU opt-in for larger grids.
case "$MACHINE_CHOICE" in
    cpu)  MACHINE_TYPE="n2-highmem-8"; ACCELERATOR="" ;;
    high) MACHINE_TYPE="n2-highmem-16"; ACCELERATOR="" ;;
    gpu)  MACHINE_TYPE="n1-standard-8"; ACCELERATOR="--accelerator=type=nvidia-tesla-t4,count=1" ;;
    *)
        echo "ERROR: Unknown --machine value: $MACHINE_CHOICE (want: cpu|high|gpu)" >&2
        exit 1
        ;;
esac

# Instrument-specific VM name prefix — first instrument label, sanitised.
FIRST_INST="${INSTRUMENTS%%;*}"
FIRST_INST="${FIRST_INST%%,*}"
FIRST_INST_LOWER="$(echo "$FIRST_INST" | tr '[:upper:]_' '[:lower:]-')"

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="ml-${FIRST_INST_LOWER}-${RUN_TS}"

# Assemble the ml-service CLI command.
ML_CMD="python -m ml_service"
ML_CMD="${ML_CMD} --operation ${OPERATION}"
ML_CMD="${ML_CMD} --mode batch"
ML_CMD="${ML_CMD} --asset-group ${ASSET_GROUP}"
# ml-service argparse uses space-separated nargs='+'; semicolons + commas get
# converted to spaces here so the assembled command is directly executable.
ML_CMD="${ML_CMD} --instruments ${INSTRUMENTS//[,;]/ }"
[[ -n "$TARGET_TYPES" ]] && ML_CMD="${ML_CMD} --target-types ${TARGET_TYPES//[,;]/ }"
[[ -n "$TIMEFRAMES" ]] && ML_CMD="${ML_CMD} --timeframes ${TIMEFRAMES//[,;]/ }"
ML_CMD="${ML_CMD} --start-date ${START_DATE}"
ML_CMD="${ML_CMD} --end-date ${END_DATE}"

# Route through the features-backfill VM_TASK branch which already reads
# VM_BACKFILL_CMD verbatim — cleanest seam without setup-script modification.
METADATA="VM_TASK=features-backfill"
METADATA="${METADATA},VM_SERVICE=ml_service"
METADATA="${METADATA},VM_OPERATION=${OPERATION}"
METADATA="${METADATA},VM_ASSET_GROUP=${ASSET_GROUP}"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_BACKFILL_CMD=${ML_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
[[ -n "$EXTRA_METADATA" ]] && METADATA="${METADATA},${EXTRA_METADATA}"

if $DRY_RUN; then
    cat <<EOF
[DRY-RUN] $VM_NAME
  category=$ASSET_GROUP instruments=$INSTRUMENTS target_types=$TARGET_TYPES timeframes=$TIMEFRAMES
  operation=$OPERATION window=${START_DATE}..${END_DATE}
  machine=$MACHINE_TYPE ${ACCELERATOR:+accelerator=$ACCELERATOR }zone=$ZONE
  assembled_cmd=$ML_CMD
  metadata=$METADATA
EOF
    echo ""
    echo "=========================================="
    echo "DRY-RUN: 1 VM would be launched"
    echo "=========================================="
else
    echo "Launching $VM_NAME ($ASSET_GROUP $INSTRUMENTS ${START_DATE}..${END_DATE})"
    # shellcheck disable=SC2086
    gcloud compute instances create "$VM_NAME" \
        --project="$PROJECT" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        --image-family=ubuntu-2404-lts-amd64 \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
        --scopes=cloud-platform \
        $ACCELERATOR \
        --metadata="startup-script-url=${STARTUP},${METADATA}" \
        --labels=purpose=ml,run-ts="${RUN_TS}",category="${ASSET_GROUP,,}",env="${DEPLOYMENT_ENV}"
    echo ""
    echo "VM launched: $VM_NAME"
    echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/features-backfill.log'"
    echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
    echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
fi
