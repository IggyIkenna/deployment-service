#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# DEPRECATION NOTE (2026-05-08, Phase 8A of features_repo_consolidation_2026_05_08):
# For NEW single-cell features-sports backfills use the consolidated launcher:
#   bash launch-features-vm.sh --feature-family sports --asset-group SPORTS \
#       --start-date YYYY-MM-DD --end-date YYYY-MM-DD --launch-mode full
# This launcher is preserved for the singleton-lock + --skip-existing semantics
# the consolidated launcher does not model. Will be archived alongside the
# legacy features-sports-service repo when Phase 7 lands.
#
# Launch a GCE VM that backfills FIXTURE_FEATURES (per-fixture denormalised
# join of Transfermarkt team value, pre-match standings, kickoff-hour
# weather) via features-sports-service batch compute. Same code path as the
# daily Cloud Run job + per-fixture Tier-3 trigger — dispatched to a VM so
# multi-year runs don't block a laptop.
#
# SSOT:
#   plans/active/features_sports_pipeline_deployment_2026_04_21.plan
#   plans/active/features_sports_denormalisation_pipeline_2026_04_21.plan
#
# Invocation inside the VM (VM_BACKFILL_CMD metadata; see
# setup-data-pipeline-vm.sh line 455 branch for features-backfill):
#
#   python -m features_sports_service \
#     --operation compute --mode batch --asset-group SPORTS \
#     --tables fixture_features \
#     --start-date $START_DATE --end-date $END_DATE
#
# Writes to
#   gs://features-sports-central-element-323112/features/by_date/day={D}/feature_group=fixture_features/...
# and records manifest rows (capture_status ∈ {captured, empty_confirmed,
# attempted_failed}) via ManifestWriter from batch_handler.
#
# Prerequisites:
#   - Tarballs refreshed:
#       bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#   - All upstream providers (API_FOOTBALL / TRANSFERMARKT / SFI / OPEN_METEO)
#     must already have data for the target window, otherwise the rows will
#     materialise as empty_confirmed (honest-coverage — not attempted_failed).
#
# Usage:
#   bash launch-features-sports-backfill-vm.sh 2018-01-01 2026-04-20
#   bash launch-features-sports-backfill-vm.sh --skip-existing 2018-01-01 2026-04-20
#   bash launch-features-sports-backfill-vm.sh --force 2018-01-01 2026-04-20
#
# --skip-existing: skip dates where fixture_features parquet already exists
#                  in GCS (safe resume after VM restart).
# --force:         bypass singleton lock (same-prefix fts-backfill-* VM lock).
#
# Singleton lock: refuses to launch if any fts-backfill-* VM is already running
# in the zone. features-sports-service batch_handler reads from several GCS
# buckets concurrently per-date and can pressure the network when many VMs
# race. One VM per window is sufficient — the bottleneck is pandas joins per
# day, not network fanout. Pass --force only when you have a genuinely
# disjoint reason (e.g. two different feature groups / two different
# non-overlapping date windows).
#
# VM-name prefix: fts-backfill- (Features-sports Singleton backfill) — distinct
# from launch-footystats-backfill-vm.sh's fs-backfill- (a different repo/service
# that collided on this exact prefix; see
# api_football_backfill_chronological_scan_never_reaches_pending_tail_2026_07_18.md)
# and from launch-features-sports-parallel-backfill-vm.sh's fss-backfill-vm-N.
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

FORCE=false
# REDO_ALL is deliberately SEPARATE from --force. --force bypasses the same-prefix
# VM singleton LOCK; --redo-all passes `--force` through to the features CLI so it
# re-derives dates the manifest already marks captured/empty. Conflating the two is
# the documented api-football mistake (lock bypass + redo_all in one flag). Without
# this the launcher CANNOT replay a writer fix over history: measured 2026-07-18, a
# 2.5h lineups re-derive logged 'SKIP fixture_lineups for <date> - manifest shows
# prior captured/empty (use --force)' on every date and wrote NOTHING.
# RESUME_REDO_ALL / RESUME_SKIP_EXISTING env fallbacks (SPOT-preemption relaunch
# support): RelaunchPreemptedVm re-invokes this launcher with ZERO CLI args, only
# the env lc_write_launch_params captured — losing REDO_ALL on relaunch is the
# exact "force disables the skip the resume relies on" bug documented for
# launch-api-football-backfill-vm.sh, so both booleans must be env-readable too.
REDO_ALL="${RESUME_REDO_ALL:-false}"
SKIP_EXISTING="${RESUME_SKIP_EXISTING:-false}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false

# Which feature tables to (re-)derive. Defaults to fixture_features for backward
# compatibility; overridable so a single table can be re-derived without touching
# the others — e.g. --tables fixture_lineups to replay the lineups normalizer fix
# (features-service@cf10b931) across history from raw, with ZERO api-football calls.
TABLES="${TABLES:-fixture_features}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --redo-all) REDO_ALL=true; shift ;;
    --skip-existing) SKIP_EXISTING=true; shift ;;
    --tables) TABLES="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --on-demand)   ON_DEMAND=true; shift ;;
    *) break ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# SPOT-preemption relaunch support: a bare re-invocation (RelaunchPreemptedVm
# passes ZERO CLI args, only the env lc_write_launch_params captured) falls back
# to RESUME_START_DATE/RESUME_END_DATE so the same window is reproduced instead
# of hitting the usage error below. Explicit positional args always win.
if [[ $# -eq 0 && -n "${RESUME_START_DATE:-}" && -n "${RESUME_END_DATE:-}" ]]; then
  set -- "$RESUME_START_DATE" "$RESUME_END_DATE"
fi

if [[ $# -ne 2 ]]; then
  cat >&2 <<EOF
Usage: bash launch-features-sports-backfill-vm.sh [--force] [--redo-all] [--skip-existing] [--tables CSV] <START_DATE> <END_DATE>

  START_DATE, END_DATE must be YYYY-MM-DD (inclusive).

Examples:
  bash launch-features-sports-backfill-vm.sh 2018-01-01 2026-04-20
  bash launch-features-sports-backfill-vm.sh --skip-existing 2018-01-01 2026-04-20
EOF
  exit 1
fi

START_DATE="$1"
END_DATE="$2"

if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR: dates must be YYYY-MM-DD (got START=$START_DATE END=$END_DATE)" >&2
  exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

# Singleton lock — prevent duplicate fts-backfill-* VMs in the zone.
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='name~"^fts-backfill-" AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: features-sports backfill VM already running in $ZONE: $EXISTING

Options:
  Inspect:   gcloud compute ssh $EXISTING --zone=$ZONE
  Tail log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log
  Force:     bash $0 --force ...

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect/Tail
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If confirmed stale:
  gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="fts-backfill-${RUN_TS}"

# Compose the in-VM command. features-backfill branch of setup-data-pipeline-vm.sh
# substitutes `python ` → the per-tarball venv python.
BACKFILL_CMD="python -m features_service.sports"
BACKFILL_CMD="${BACKFILL_CMD} --operation compute --mode batch --asset-group SPORTS"
BACKFILL_CMD="${BACKFILL_CMD} --tables ${TABLES}"
BACKFILL_CMD="${BACKFILL_CMD} --start-date ${START_DATE} --end-date ${END_DATE}"
if $SKIP_EXISTING; then
  BACKFILL_CMD="${BACKFILL_CMD} --skip-existing"
fi
if $REDO_ALL; then
  BACKFILL_CMD="${BACKFILL_CMD} --force"
fi

# SPOT by default; --on-demand / ON_DEMAND=true forces standard provisioning.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
if $ON_DEMAND; then PROVISIONING_FLAGS=""; fi

echo "Launching $VM_NAME: features-sports FIXTURE_FEATURES backfill ${START_DATE}..${END_DATE} [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
echo "  cmd: ${BACKFILL_CMD}"

METADATA="VM_TASK=features-backfill"
METADATA="${METADATA},VM_SERVICE=features_service"
METADATA="${METADATA},VM_OPERATION=compute"
METADATA="${METADATA},VM_ASSET_GROUP=SPORTS"
METADATA="${METADATA},VM_START_DATE=${START_DATE}"
METADATA="${METADATA},VM_END_DATE=${END_DATE}"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  # shellcheck disable=SC2086
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          features-service market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  # SPOT-preemption relaunch support (cefi_completion_program_2026_07_15.md P0
  # "Close the SPOT-preemption relaunch gap"): persist the EXACT env this VM was
  # launched with so exit_code_fleet_monitor's PREEMPTED auto_recover actuator
  # (relaunch_backfill_vm.RelaunchPreemptedVm) can re-invoke THIS launcher with
  # the SAME window/tables/redo-all scope instead of a blind default. Best-effort.
  lc_write_launch_params "$VM_NAME" "$PROJECT" "launch-features-sports-backfill-vm.sh" \
      "RESUME_START_DATE=${START_DATE}" \
      "RESUME_END_DATE=${END_DATE}" \
      "TABLES=${TABLES}" \
      "RESUME_REDO_ALL=${REDO_ALL}" \
      "RESUME_SKIP_EXISTING=${SKIP_EXISTING}" \
      "DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"

  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
    --zone="$ZONE" \
    --machine-type=e2-standard-4 \
    ${PROVISIONING_FLAGS} \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=features-sports-backfill,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/features-backfill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
