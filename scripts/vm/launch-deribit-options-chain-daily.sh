#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Launch a short-lived GCE VM that captures today's Deribit BTC/ETH options_chain
# snapshot via the `deribit-options-chain` MTDS operation (handler
# `deribit_options_chain_handler.py`, registered `mtds@9ecd1e29e` under
# `infra_capture_and_devops_leftovers_2026_07_06` Plan 6 task 002).
#
# Why a separate launcher (not launch-cefi-forward-poll.sh)
# ---------------------------------------------------------
# `launch-cefi-forward-poll.sh` runs `--operation download` (Tardis-fleet
# historical/forward tick capture). The Deribit options_chain live path is a
# DIFFERENT MTDS operation registered as `deribit-options-chain` — the handler
# hits the Deribit public REST directly (`GET /public/get_instruments`,
# `GET /public/ticker`) and writes one parquet shard per (currency, expiry).
# Historical options_chain is already covered by `launch-targeted-options-
# chain-backfill.sh` (Tardis batch). This launcher is the ONGOING DAILY
# rolling snapshot — the handler's `process()` collects `date.today()` and
# is live/replay only (NO backfill).
#
# What it does
# ------------
# * Fires once per invocation; VM shuts down on completion.
# * `--operation deribit-options-chain --mode batch --asset-group CEFI` —
#   `--mode batch` because the CLI framework requires a mode and the handler
#   ignores payload dates (it always uses `date.today()`); the pipeline_mode
#   in the emitted rows is `LIVE_DERIBIT` (per-shard, per the source-aware
#   `{mode}_{source}` convention — see codex/02-data/pipeline-mode-partition.md).
# * Writes to: `gs://market-data-tick-cefi-{env}-{project}/pipeline_mode=
#   live_deribit/asset_group=cefi/venue=deribit/instrument_type=option/
#   data_type=options_chain/day={D}/underlying={BTC|ETH}/expiry={E}/*.parquet`
#
# Cost shape (per fire)
# ---------------------
# * ~2 REST calls for the instrument index (`get_instruments` for BTC + ETH)
# * ~500-2000 REST calls for tickers (bounded by `_MAX_TICKERS_PER_SHARD=2000`)
# * ~1-3 minutes wall-clock end-to-end
# * e2-standard-2 (small) — this is a REST-only workload, not tick-throughput
#
# Cadence
# -------
# Fired daily by `launch-cefi-fwd-daily-cron-vm.sh` at 09:00 UTC (same cron
# host — the additional cron line was added under the same task) OR one-off
# by an operator/main-agent invocation.
#
# Prerequisites
# -------------
# * Tarballs uploaded: bash deployment-service/scripts/vm/create-code-tarballs.sh
# * NO API key needed — the Deribit endpoints used are public.
#
# Usage
# -----
#   bash launch-deribit-options-chain-daily.sh                # today's snapshot
#   bash launch-deribit-options-chain-daily.sh --force        # bypass singleton
#   bash launch-deribit-options-chain-daily.sh --dry-run      # show plan only
#   bash launch-deribit-options-chain-daily.sh --env staging  # staging tier
#
# Singleton lock
# --------------
# Refuses to launch if any `deribit-opts-fwd-*` VM is already running in the
# zone — Deribit endpoints are public but a duplicate fire would double-write
# the same day's shards. `--force` bypasses.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/launcher_common.sh
source "${SCRIPT_DIR}/lib/launcher_common.sh"

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
MACHINE_TYPE="e2-standard-2"
BOOT_DISK_GB="30"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
DRY_RUN=false

VM_PREFIX="deribit-opts-fwd-"

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --force)     FORCE=true; shift ;;
        --dry-run)   DRY_RUN=true; shift ;;
        --env)       DEPLOYMENT_ENV="$2"; shift 2 ;;
        --project)   PROJECT="$2"; shift 2 ;;
        --zone)      ZONE="$2"; shift 2 ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            echo "Usage: $0 [--force] [--dry-run] [--env prod|staging|dev]" >&2
            exit 1
            ;;
    esac
done

CODE_BUCKET="$(lc_code_bucket "$PROJECT")"
STARTUP="gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh"

lc_validate_env "$DEPLOYMENT_ENV"
lc_singleton_check "$VM_PREFIX" "$ZONE" "$PROJECT" "$FORCE"

RUN_TS="$(lc_run_ts)"
VM_NAME="${VM_PREFIX}${RUN_TS}"

# Handler ignores payload dates (uses date.today()); pass today explicitly so
# the CLI framework's batch-mode iteration fires the handler exactly once for
# the current UTC day. Idempotent — a duplicate fire on the same day rewrites
# the same shards (recorder.record_captured overwrite semantics).
TODAY_UTC="$(date -u +%Y-%m-%d)"

METADATA="startup-script-url=${STARTUP}"
METADATA="${METADATA},VM_TASK=deribit-options-chain-daily"
METADATA="${METADATA},VM_SERVICE=market_tick_data_service"
METADATA="${METADATA},VM_OPERATION=deribit-options-chain"
METADATA="${METADATA},VM_ASSET_GROUP=CEFI"
METADATA="${METADATA},VM_START_DATE=${TODAY_UTC}"
METADATA="${METADATA},VM_END_DATE=${TODAY_UTC}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_NAME=${VM_NAME}"
METADATA="${METADATA},MANIFEST_PER_VM_SHARDS=true"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
METADATA="${METADATA},VM_LIFECYCLE_CLASS=EPHEMERAL_BATCH"

LABELS="purpose=deribit-options-chain-daily,env=${DEPLOYMENT_ENV},run-ts=${RUN_TS},lifecycle=ephemeral-batch"

echo "Launching ${VM_NAME}: Deribit BTC/ETH options_chain daily snapshot (day=${TODAY_UTC})"

if $DRY_RUN; then
    export LC_DRY_RUN=true
fi

lc_gcloud_create "$VM_NAME" "$PROJECT" "$ZONE" "$MACHINE_TYPE" "$BOOT_DISK_GB" \
    "$METADATA" "$LABELS" \
    "$(lc_tier_service_account "$DEPLOYMENT_ENV" "$PROJECT")"

cat <<EOF

VM launched:  ${VM_NAME}
Operation:    deribit-options-chain (MTDS live/replay only — handler uses date.today())
Data written: gs://market-data-tick-cefi-${DEPLOYMENT_ENV}-${PROJECT}/pipeline_mode=live_deribit/asset_group=cefi/venue=deribit/instrument_type=option/data_type=options_chain/day=${TODAY_UTC}/
Logs:         gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log
Stop:         gcloud compute instances delete ${VM_NAME} --zone=${ZONE} --quiet

Verify (T+5-10min): shards land in the bucket + VM STOPPED (VM_SHUTDOWN_ON_COMPLETION=true)
  gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='value(status)'
  gsutil ls gs://market-data-tick-cefi-${DEPLOYMENT_ENV}-${PROJECT}/pipeline_mode=live_deribit/asset_group=cefi/venue=deribit/instrument_type=option/data_type=options_chain/day=${TODAY_UTC}/
EOF
