#!/usr/bin/env bash
# Launch GCE VM(s) that run the synthetic-data pipeline benchmark for a cutover archetype.
#
# Phase 5.A of `unified-trading-pm/plans/active/mock_data_pipeline_benchmarking_2026_05_10.md`.
# Each VM runs `python -m unified_trading_library.synthetic` (the Phase-4.A benchmark CLI):
# generates realism-axis-1..3 synthetic parquet input, drives the prod pipeline DAG
# (mtds_read → mdps_compute → features → ml_inference → strategy → matching_engine), and
# writes `stage_profile.parquet` + `synthetic_run_manifest.json` to the benchmark-reports
# prefix. One VM per (archetype, machine-type) so the Phase-6 aggregation gets a per-shape
# matrix; this launcher fans out over a list of machine types in one call.
#
# ┌──────────────────────────────────────────────────────────────────────────────────────┐
# │ PREREQUISITE FOR `--mode subprocess` — MET 2026-05-12 (slot 7 Day-2):                  │
# │   - `--synthetic-input-uri` declared at the ServiceCLI framework level (UTL@5aa356b);  │
# │   - `set_synthetic_input_override` installed after parse_args (UTL@c80bfbf);           │
# │   - `default_subprocess_pipeline()` ships real command templates (UTL@04044bf).        │
# │   Services using `resolve_bucket_uri` (MDPS / features-service / execution-service)    │
# │   get full reader redirection automatically. Services whose readers bypass that helper │
# │   (MTDS / ml-inference / strategy direct config reads) accept the flag but the deep    │
# │   reader wire-in is Phase 3.D — those stages may exit nonzero in subprocess mode but   │
# │   still emit per-stage profile data (start time / wall-clock / CPU / RSS) for Phase 6. │
# └──────────────────────────────────────────────────────────────────────────────────────┘
#
# No fire-and-forget (CLAUDE.md): each VM emits STARTED + per-stage progress + STOPPED to
# `gs://${PROJECT}-events/events/unified-trading-library/{YYYY-MM-DD}/{VM_NAME}/`. After
# launch, verify the event stream within 90s and re-check every ~30min until STOPPED.
#
# Zombie watchdog: VM names are `synbench-{archetype-short}-{shape-short}-{ts}`; the `synbench-`
# prefix is registered in `vm_zombie_watchdog.py:VM_PREFIX_TO_BUCKET` (heartbeat-only — the
# report bucket is not a per-vm shard writer). If you add this launcher and the prefix is
# missing from the dict, RELAUNCH THE WATCHDOG VM after adding it (silent money-burn otherwise).
#
# Usage:
#   bash launch-synthetic-benchmark-vm.sh --archetype carry_staked_basis \
#     --shapes "c2-standard-8 c2-standard-16 c2-standard-30 c3-highcpu-44" \
#     --date-start 2024-01-01 --date-end 2024-01-07 --mode subprocess --env staging
#   bash launch-synthetic-benchmark-vm.sh --archetype leveraged_funding_arb --shapes "c2-standard-16"
#
# Note: asia-northeast1-c offers `c2-standard-{4,8,16,30,60}` + `c3-highcpu-{4,8,22,44,88,176}`.
# `c2-standard-32` is NOT in this zone (use 30) — verified 2026-05-12 via
# `gcloud compute machine-types list --zones=asia-northeast1-c --filter='name~^c2-standard'`.
set -euo pipefail

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
PROJECT_NUMBER="${PROJECT_NUMBER:-1060025368044}"
CODE_BUCKET="deployment-scripts-${PROJECT}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
ARCHETYPE=""
# Phase 5 SKU matrix v2 (2026-05-19): extended beyond c3-highcpu-44 to cover high-vCPU + high-mem shapes.
# c3-highcpu-88 / c3-highcpu-176: compute-bound stages (features, strategy, ML training).
# m3-megamem-128 / m3-ultramem-160: memory-heavy stages (onchain DAG, multi-timeframe cross-family chains).
# Note: m3-* series is in asia-northeast1-c; verify quota via
#   gcloud compute machine-types list --zones=asia-northeast1-c --filter='name~^m3-'
# before launching.  STOCKOUT fallback = asia-northeast1-b (same region, same GCS locality).
SHAPES="c2-standard-8 c2-standard-16 c2-standard-30 c3-highcpu-44 c3-highcpu-88 c3-highcpu-176 m3-megamem-128 m3-ultramem-160"
DATE_START="2024-01-01"
DATE_END="2024-01-07"
MODE="stub"
ROW_COUNT_SCALE="1.0"
# SA: default to the Compute Engine default SA (matches running watchdog +
# every other actively-deployed VM in this project — see `gcloud compute
# instances list --format='value(serviceAccounts.email)'`). The historical
# `data-pipeline-vm@${PROJECT}.iam.gserviceaccount.com` SA referenced in some
# launchers does not exist in this project (verified 2026-05-12 — every
# `gcloud iam service-accounts list` enumeration; absence is the cause of
# the `serviceAccount of type was not found` failure mode). Override via
# `SERVICE_ACCOUNT=foo@... bash launch-synthetic-benchmark-vm.sh` if needed.
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-${PROJECT_NUMBER}-compute@developer.gserviceaccount.com}"

DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --archetype) ARCHETYPE="$2"; shift 2 ;;
    --shapes) SHAPES="$2"; shift 2 ;;
    --date-start) DATE_START="$2"; shift 2 ;;
    --date-end) DATE_END="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --row-count-scale) ROW_COUNT_SCALE="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$ARCHETYPE" ]]; then
  echo "ERROR: --archetype is required (e.g. carry_staked_basis, leveraged_funding_arb)" >&2
  exit 1
fi
if [[ "$MODE" == "subprocess" ]]; then
  echo "INFO: --mode subprocess (Phase 4.A-tail landed 2026-05-12 — framework SSOT wired)" >&2
  echo "      Stages where the reader uses resolve_bucket_uri get full data redirection;" >&2
  echo "      MTDS / ml-inference / strategy may exit nonzero pending Phase 3.D (still emits profile data)." >&2
fi

ARCH_SHORT="$(echo "$ARCHETYPE" | tr '[:upper:]_' '[:lower:]-' | cut -c1-16)"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
# benchmark-reports prefix — the report bucket kind is pending in cloud-providers.yaml; until
# it lands, the launcher uses the conventional `${PROJECT}-benchmark-reports` name. When the
# kind is added, switch the VM CLI to derive it via resolve_bucket_name(kind="benchmark-reports").
REPORT_URI="gs://${PROJECT}-benchmark-reports"
INPUT_URI="gs://${PROJECT}-benchmark-synthetic-input"

for SHAPE in $SHAPES; do
  SHAPE_SHORT="$(echo "$SHAPE" | tr '.' '-' | cut -c1-20)"
  VM_NAME="synbench-${ARCH_SHORT}-${SHAPE_SHORT}-${RUN_TS}"
  # Compose the synthetic-benchmark CLI invocation that `VM_TASK=synthetic-benchmark`
  # dispatch in `setup-data-pipeline-vm.sh` will run (deployment-service@3fde508
  # added the branch; reads VM_BACKFILL_CMD verbatim). Per-VM input prefix scopes
  # the bucket-resolver override to this run (Phase 4.A-tail of mock_data_pipeline_benchmarking).
  VM_INPUT_URI="${INPUT_URI}/${VM_NAME}"
  VM_REPORT_URI="${REPORT_URI}"
  VM_BACKFILL_CMD="python -m unified_trading_library.synthetic --archetype ${ARCHETYPE} --date-start ${DATE_START} --date-end ${DATE_END} --input-uri ${VM_INPUT_URI} --report-uri ${VM_REPORT_URI} --mode ${MODE} --row-count-scale ${ROW_COUNT_SCALE} --vm-shape ${SHAPE} --run-id ${VM_NAME}"
  echo "Launching synthetic-benchmark VM:"
  echo "  name:       $VM_NAME"
  echo "  archetype:  $ARCHETYPE   shape: $SHAPE   mode: $MODE   window: ${DATE_START}..${DATE_END}"
  echo "  report:     ${VM_REPORT_URI}/${ARCHETYPE}/${VM_NAME}/stage_profile.parquet"
  echo "  events:     gs://${PROJECT}-events/events/unified-trading-library/$(date -u +%Y-%m-%d)/${VM_NAME}/"
  echo "  cmd:        ${VM_BACKFILL_CMD}"
  if [[ "${DRY_RUN:-false}" == "true" ]]; then
    echo "[DRY-RUN] Would create VM: "$VM_NAME""
    echo "[DRY-RUN] (gcloud compute instances create skipped)"
  else
    gcloud compute instances create "$VM_NAME" \
      --zone="$ZONE" \
      --project="$PROJECT" \
      --machine-type="$SHAPE" \
      --image-family=debian-12 \
      --image-project=debian-cloud \
      --boot-disk-size=50GB \
      --service-account="${SERVICE_ACCOUNT}" \
      --scopes=cloud-platform \
      --metadata="\
  SERVICE_REPO=unified-trading-library,\
  SERVICE_MODULE=unified_trading_library.synthetic,\
  VM_NAME=${VM_NAME},\
  VM_SHUTDOWN_ON_COMPLETION=true,\
  VM_TASK=synthetic-benchmark,\
  VM_BACKFILL_CMD=${VM_BACKFILL_CMD},\
  SYNTHETIC_ARCHETYPE=${ARCHETYPE},\
  SYNTHETIC_MODE=${MODE},\
  SYNTHETIC_DATE_START=${DATE_START},\
  SYNTHETIC_DATE_END=${DATE_END},\
  SYNTHETIC_ROW_COUNT_SCALE=${ROW_COUNT_SCALE},\
  SYNTHETIC_INPUT_URI=${VM_INPUT_URI},\
  SYNTHETIC_REPORT_URI=${VM_REPORT_URI},\
  BENCHMARK_VM_SHAPE=${SHAPE},\
  DEPLOYMENT_ENV=${DEPLOYMENT_ENV},\
  CODE_BUCKET=${CODE_BUCKET},\
  PROJECT_ID=${PROJECT}" \
      --labels=purpose=synthetic-benchmark,archetype="${ARCH_SHORT}",shape="${SHAPE_SHORT}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}" \
      --metadata-from-file="startup-script=$(dirname "$0")/setup-data-pipeline-vm.sh"
  fi
done

echo ""
echo "VM(s) launched. Verify event streams within 90s, e.g.:"
echo "  gcloud storage ls gs://${PROJECT}-events/events/unified-trading-library/$(date -u +%Y-%m-%d)/synbench-${ARCH_SHORT}-*/"
echo "Per CLAUDE.md 'No fire-and-forget VM launches', re-check every ~30min until each VM emits STOPPED;"
echo "auto-shutdown fires at run completion via VM_SHUTDOWN_ON_COMPLETION=true. Then run the Phase-6"
echo "aggregation over ${REPORT_URI}/${ARCHETYPE}/*/stage_profile.parquet."
