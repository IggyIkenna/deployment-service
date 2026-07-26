#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch an ML training VM for ml-training-service — trains a single
# model-family / instrument / target-type combination on GCE, writing the
# artefact to the ml model_registry in GCS. Used for the CME S&P 500 ML
# directional signal (Tier 1 MVP) and any subsequent tradfi/cefi/sports
# training runs that need more RAM / CPU / GPU than a laptop has.
#
# NOT singleton-locked: parallel training is expected (different instruments,
# different target types, different hyper-param grids). The ml-training-service
# does not share a rate-limited API key — it reads local parquet features +
# fits models locally.
#
# Runs the unified ml-training-service CLI via setup-data-pipeline-vm.sh
# metadata routing (the generic `VM_TASK` branch at line 435 of setup-data-
# pipeline-vm.sh assembles `python -m $VM_SERVICE --operation $VM_OPERATION
# --mode batch --asset-group $VM_ASSET_GROUP ...`).
#
# Example assembled command:
#   python -m ml_service.training \
#     --operation train --mode batch --asset-group TRADFI \
#     --instruments ES_FRONT --timeframes 1m \
#     --target-types swing_high swing_low \
#     --start-date 2022-01-01 --end-date 2025-12-31
#
# Prerequisites:
#   - Tarballs uploaded via `bash create-code-tarballs.sh --ml-training`
#   - Historical feature parquet present in the feature-group GCS paths
#   - model_registry GCS bucket provisioned
#
# Usage:
#   bash launch-ml-training-vm.sh --dry-run                    # preview
#   bash launch-ml-training-vm.sh \
#       --asset-group TRADFI --instruments ES_FRONT \
#       --target-types swing_high swing_low \
#       --timeframes 1m \
#       --start-date 2022-01-01 --end-date 2025-12-31
#   bash launch-ml-training-vm.sh --gpu ...                    # use GPU machine
#   # SPORTS requires --family (pregame_xg_family|pregame_clv_family|ht_xg_family|
#   # ht_clv_family|meta_family) -- ml_training_service's CLI itself requires it
#   # for --asset-group SPORTS. --extra-train-args passes through any flag this
#   # launcher has no dedicated arg for (e.g. '--skip-walk-forward --optuna-trials 10').
#   bash launch-ml-training-vm.sh \
#       --asset-group SPORTS --instruments SPORTS:ALL \
#       --family pregame_clv_family --timeframes fixture \
#       --start-date 2020-06-06 --end-date 2026-07-20
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
FAMILY=""
EXTRA_TRAIN_ARGS=""

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
        # --family: SPORTS-only, required by ml_training_service's CLI when
        # --asset-group SPORTS (pregame_xg_family / pregame_clv_family /
        # ht_xg_family / ht_clv_family / meta_family). Semicolon/comma-
        # separated like --target-types/--timeframes above.
        --family)          FAMILY="$2"; shift 2 ;;
        # --extra-train-args: verbatim passthrough appended to the assembled
        # ml_training_service command (e.g. '--skip-walk-forward --optuna-trials 10')
        # for flags this launcher doesn't have a dedicated arg for.
        --extra-train-args) EXTRA_TRAIN_ARGS="$2"; shift 2 ;;
        --help|-h)
            grep '^#' "$0" | head -60
            exit 0
            ;;
        *)
            echo "Unknown arg: $1" >&2
            echo "Usage: $0 [--dry-run] [--asset-group TRADFI|CEFI|SPORTS] [--instruments 'ES_FRONT;BTC'] [--target-types 'swing_high;swing_low'] [--timeframes '1m;1h'] [--family 'pregame_clv_family'] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--machine cpu|gpu|high] [--operation train|evaluate|grid-search|pipeline] [--extra-train-args '--skip-walk-forward'] [--env <prod|staging|dev>]" >&2
            exit 1
            ;;
    esac
done

if [[ -z "$INSTRUMENTS" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "ERROR: --instruments, --start-date, --end-date are required" >&2
    exit 1
fi

if [[ "$ASSET_GROUP" == "SPORTS" && -z "$FAMILY" ]]; then
    echo "ERROR: --family is required when --asset-group SPORTS (pregame_xg_family|pregame_clv_family|ht_xg_family|ht_clv_family|meta_family)" >&2
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
# GCE instance names must match [a-z]([-a-z0-9]*[a-z0-9])? -- colons appear in
# sports instrument ids (e.g. SPORTS:ALL, SPORTS:LEAGUE:PREMIER_LEAGUE) and
# were previously left untranslated, producing an invalid create request.
FIRST_INST_LOWER="$(echo "$FIRST_INST" | tr '[:upper:]_:' '[:lower:]--' | tr -s '-')"

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="ml-train-${FIRST_INST_LOWER}-${RUN_TS}"

# Metadata: setup-data-pipeline-vm.sh generic branch will assemble
#   python -m ml_service.training --operation $VM_OPERATION --mode batch
#     --asset-group $VM_ASSET_GROUP --instrument-ids $VM_INSTRUMENT_IDS
#     --data-types $VM_DATA_TYPES --start-date $VM_START_DATE --end-date ...
#
# ml-training-service's CLI uses --instruments + --target-types + --timeframes
# (not --instrument-ids / --data-types); the existing setup script hard-codes
# --instrument-ids / --data-types via the generic branch. We therefore pass
# ml-training-specific args through VM_ML_ARGS which the operator-extended
# setup script can interpret — OR, simpler: pass them via VM_BACKFILL_CMD-
# style pattern (already handled by setup-data-pipeline-vm.sh's mdps-backfill
# / features-backfill branch). Tier 1 wires it through VM_BACKFILL_CMD with
# VM_TASK=ml-training so the routing is explicit + auditable.
#
# Phase B will add a dedicated `elif [[ "$VM_TASK" == "ml-training" ]]`
# branch to setup-data-pipeline-vm.sh that maps the clean ml-training CLI
# directly. For now, VM_BACKFILL_CMD is the cleanest seam that already
# exists in setup-data-pipeline-vm.sh (see line 424-434) — it carries the
# full command string verbatim.
# The repo's real importable package is ml_service (pyproject name
# "ml-service"), with the training CLI under the ml_service.training
# submodule (see ml_service/training/__main__.py) -- "ml_training_service"
# is a stale pre-rename name that was never actually importable; running it
# verbatim would fail every invocation with ModuleNotFoundError. VM_SERVICE
# below stays "ml_training_service" (it only keys the SERVICE_TARBALLS dict
# in setup-data-pipeline-vm.sh, unrelated to this literal invocation).
ML_CMD="python -m ml_service.training"
ML_CMD="${ML_CMD} --operation ${OPERATION}"
ML_CMD="${ML_CMD} --mode batch"
ML_CMD="${ML_CMD} --asset-group ${ASSET_GROUP}"
# ml-training-service argparse uses space-separated nargs='+'; semicolons in
# metadata get converted to spaces by setup-data-pipeline-vm.sh before the
# VM_BACKFILL_CMD is executed, but VM_BACKFILL_CMD does not get that
# transformation — build the space-separated form directly.
ML_CMD="${ML_CMD} --instruments ${INSTRUMENTS//[,;]/ }"
[[ -n "$TARGET_TYPES" ]] && ML_CMD="${ML_CMD} --target-types ${TARGET_TYPES//[,;]/ }"
[[ -n "$TIMEFRAMES" ]] && ML_CMD="${ML_CMD} --timeframes ${TIMEFRAMES//[,;]/ }"
[[ -n "$FAMILY" ]] && ML_CMD="${ML_CMD} --family ${FAMILY//[,;]/ }"
ML_CMD="${ML_CMD} --start-date ${START_DATE}"
ML_CMD="${ML_CMD} --end-date ${END_DATE}"
[[ -n "$EXTRA_TRAIN_ARGS" ]] && ML_CMD="${ML_CMD} ${EXTRA_TRAIN_ARGS}"

METADATA="VM_TASK=features-backfill"
# ^ reuse the features-backfill routing branch which already reads
#   VM_BACKFILL_CMD verbatim. Phase B will switch this to a dedicated
#   VM_TASK=ml-training branch once the setup script is extended.
METADATA="${METADATA},VM_SERVICE=ml_training_service"
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
        --labels=purpose=ml-training,run-ts="${RUN_TS}",category="${ASSET_GROUP,,}",env="${DEPLOYMENT_ENV}"
    echo ""
    echo "VM launched: $VM_NAME"
    echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/features-backfill.log'"
    echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
    echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
fi
