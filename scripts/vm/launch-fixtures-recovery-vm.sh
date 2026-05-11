#!/usr/bin/env bash
# Launch a GCE VM that runs the Phase 2 fixtures recovery from the Phase 1 truth-set.
#
# Plan: unified-trading-pm/plans/active/sports_fixtures_truthset_recovery_2026_05_06.md
# Script: instruments-service/scripts/recover_fixtures_from_truthset.py
#
# What it does
# ------------
# Reads gs://instruments-store-sports-{pid}/_audits/fixtures_recovery_set_{truthset_run_ts}.parquet
# (produced by the Phase 1 audit launcher) and:
#
#   1. Re-fetches api_football /fixtures?league&season ONCE per
#      (canonical_league_id, season) pair represented in the recovery set
#      (~423 calls per the 20260506-153914 audit).
#   2. Writes a per-league sub-partition fixtures parquet under
#      sports_reference/by_date/day={D}/entity=fixtures/league={L}/fixtures.parquet
#      using the canonical 32-column SPORTS_FIXTURES schema.
#   3. Records manifest captured rows via per-VM shard isolation
#      (VM_NAME=fixtures-recovery-{run_ts}).
#
# With --flip-empty-attempts the script ALSO flips the
# ATTEMPTED_FAILED_NO_TRUTH classification (~69k rows in the 20260506-153914
# audit) to empty_confirmed in the canonical manifest, with a backup blob
# and a per-VM mirror so the 120s reader staleness window is covered.
#
# Singleton lock: refuses to launch if any af-backfill-* OR af-audit-* VM is
# running in the zone (api_football per-key rate limit).
#
# Usage
# -----
#   bash launch-fixtures-recovery-vm.sh <truthset-run-ts>                # apply
#   bash launch-fixtures-recovery-vm.sh <truthset-run-ts> --flip-empty   # apply + flip
#   bash launch-fixtures-recovery-vm.sh <truthset-run-ts> --force        # bypass singleton lock
#
# Cost: e2-standard-2 for ~10-20 min (api_football fetches dominated by
# rate-limit pacing + ~34k small parquet uploads). Override with
# MACHINE_TYPE=e2-standard-4 if needed.
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

FORCE=false
FLIP_EMPTY=false
TRUTHSET_RUN_TS=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true; shift ;;
    --flip-empty|--flip-empty-attempts) FLIP_EMPTY=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    -h|--help)
      cat <<EOF
Usage: bash launch-fixtures-recovery-vm.sh <TRUTHSET_RUN_TS> [--force] [--flip-empty] [--env prod|staging|dev]

  TRUTHSET_RUN_TS  Phase 1 audit run identifier (e.g. 20260506-153914).
  --flip-empty     Also flip ATTEMPTED_FAILED_NO_TRUTH rows to empty_confirmed
                   (no api calls, manifest mutation only).
  --force          Bypass the af-* singleton lock.
  --env            Deployment env tier (prod/staging/dev; default prod).
EOF
      exit 0 ;;
    *)
      if [[ -z "$TRUTHSET_RUN_TS" ]]; then
        TRUTHSET_RUN_TS="$1"
        shift
      else
        echo "Unexpected arg: $1" >&2
        exit 1
      fi
      ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if [[ -z "$TRUTHSET_RUN_TS" ]]; then
  echo "ERROR: missing TRUTHSET_RUN_TS positional arg (Phase 1 run identifier, e.g. 20260506-153914)" >&2
  exit 1
fi
if ! [[ "$TRUTHSET_RUN_TS" =~ ^[0-9]{8}-[0-9]{6}$ ]]; then
  echo "ERROR: TRUTHSET_RUN_TS must be YYYYMMDD-HHMMSS (got $TRUTHSET_RUN_TS)" >&2
  exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-central-element-323112"

# Singleton lock — same scope as launch-fixtures-truthset-audit-vm.sh
# (api_football key shared between recovery + audit + backfill VMs).
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='(name~"^af-backfill-" OR name~"^af-audit-" OR name~"^af-recover-") AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: api_football VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — api_football rate-limits per-key.

Options:
  Inspect: gcloud compute ssh $EXISTING --zone=$ZONE
  Stop:    gcloud compute instances delete $EXISTING --zone=$ZONE --quiet
  Force:   bash $0 $TRUTHSET_RUN_TS --force
EOF
    exit 1
  fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="af-recover-${RUN_TS}"

# Build python invocation for setup-data-pipeline-vm.sh sports-gap-fill branch.
RECOVERY_CMD="python scripts/recover_fixtures_from_truthset.py --apply --truthset-run-ts ${TRUTHSET_RUN_TS}"
$FLIP_EMPTY && RECOVERY_CMD="${RECOVERY_CMD} --flip-empty-attempts"

echo "Launching $VM_NAME: fixtures recovery from truth-set ${TRUTHSET_RUN_TS}"
$FLIP_EMPTY && echo "  + flip ATTEMPTED_FAILED_NO_TRUTH to empty_confirmed"
echo "  cmd: ${RECOVERY_CMD}"

METADATA="VM_TASK=sports-gap-fill"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_MIGRATION_CMD=${RECOVERY_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"

gcloud compute instances create "$VM_NAME" \
  --project="$PROJECT" \
  --zone="$ZONE" \
  --machine-type="$MACHINE_TYPE" \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --scopes=cloud-platform \
  --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
  --labels=purpose=fixtures-recovery,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/sports-gap-fill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Verify STARTED event after ~90s:"
echo "  gcloud storage ls gs://${PROJECT}-events/events/instruments_service/\$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo ""
echo "On completion, written artefacts:"
echo "  per-league fixtures: sports_reference/by_date/day={D}/entity=fixtures/league={L}/fixtures.parquet"
echo "  manifest mirror:     _index/per_vm/fixtures-recovery-{run_ts}.parquet"
$FLIP_EMPTY && echo "  flip mirror:         _index/per_vm/recover-fixtures-flip-{run_ts}.parquet"
$FLIP_EMPTY && echo "  flip backup:         _index/availability_index.{run_ts}.flip_empty.bak.parquet"
