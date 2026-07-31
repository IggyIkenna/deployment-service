#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch a GCE VM that runs the api_football fixtures truth-set audit.
#
# Plan: unified-trading-pm/plans/active/sports_fixtures_truthset_recovery_2026_05_06.md
# Script: instruments-service/scripts/audit_fixtures_via_api_football.py
#
# What it does
# ------------
# Iterates every league with ``api_football`` in ``data_sources`` × every season
# in [season-start, season-end] (default 2018-2026), calls
# ``GET /fixtures?league={af_id}&season={year}`` once per pair, and produces:
#   - Truth-set parquet (every af fixture by league × season)
#   - Diff CSV (per (date, league) classification vs current canonical manifest)
#   - Recovery-set parquet (the SILENT_DROP / RETRY / MISSING subset Phase 2 consumes)
#   - Mapping-breakage CSV (leagues with api_football data_source but no api_football_id)
#
# All four artefacts land at gs://instruments-store-sports-{pid}/_audits/{name}_{run_ts}.{ext}
# plus local copies under ${TMPDIR}/fixtures_truthset_audit/.
#
# API budget: ~94 leagues × 9 seasons ≈ 846 calls. Pro-tier @ 7,500/day → ~3-4h.
#
# Usage
# -----
#   bash launch-fixtures-truthset-audit-vm.sh                                    # apply (default)
#   bash launch-fixtures-truthset-audit-vm.sh --season-start 2018 --season-end 2026
#   bash launch-fixtures-truthset-audit-vm.sh --resume 20260506-150000           # resume an interrupted run
#   bash launch-fixtures-truthset-audit-vm.sh --force                            # bypass singleton lock
#
# Singleton lock: refuses to launch if any af-backfill-* OR af-audit-* VM is
# already running in the zone. api_football shares one API key across all VMs
# and rate-limits per-key — concurrent VMs produce 429s without useful
# throughput (see 2026-04-19 SFI thundering herd). Pass --force only when you
# have a genuinely disjoint reason.
#
# Prerequisites:
#   - Tarballs refreshed:
#       bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS
#   - api-football-api-key in Secret Manager
#
# Cost: e2-standard-2 for ~3-4h. Audit is tiny pandas + ~thousand HTTP gets, so
# memory pressure is low; the existing -2 default is fine. Override with
# MACHINE_TYPE=e2-standard-4 if you want headroom.
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

FORCE=false
SEASON_START=2018
SEASON_END=2026
RESUME=""
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --season-start) SEASON_START="$2"; shift 2 ;;
    --season-end) SEASON_END="$2"; shift 2 ;;
    --resume) RESUME="$2"; shift 2 ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    -h|--help)
      cat <<EOF
Usage: bash launch-fixtures-truthset-audit-vm.sh [--force] \\
         [--season-start YEAR] [--season-end YEAR] [--resume RUN_TS] [--env prod|staging|dev]
EOF
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

if ! [[ "$SEASON_START" =~ ^[0-9]{4}$ ]] || ! [[ "$SEASON_END" =~ ^[0-9]{4}$ ]]; then
  echo "ERROR: --season-start and --season-end must be 4-digit years" >&2
  exit 1
fi
if [[ "$SEASON_START" -gt "$SEASON_END" ]]; then
  echo "ERROR: --season-start ($SEASON_START) > --season-end ($SEASON_END)" >&2
  exit 1
fi
if [[ -n "$RESUME" ]] && ! [[ "$RESUME" =~ ^[0-9]{8}-[0-9]{6}$ ]]; then
  echo "ERROR: --resume RUN_TS must be YYYYMMDD-HHMMSS (got $RESUME)" >&2
  exit 1
fi

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"

# Singleton lock: BOTH af-backfill-* AND af-audit-* share the api_football
# rate limit. Refuse if either is running.
if ! $FORCE; then
  EXISTING="$(gcloud compute instances list \
    --filter='(name~"^af-backfill-" OR name~"^af-audit-") AND status=RUNNING' \
    --zones="$ZONE" \
    --format='value(name)' 2>/dev/null | head -1)"
  if [[ -n "$EXISTING" ]]; then
    cat >&2 <<EOF
ERROR: api_football VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — api_football rate-limits per-key; concurrent
VMs thrash on 429s without producing useful data.

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
VM_NAME="af-audit-${RUN_TS}"

# Build the python invocation. setup-data-pipeline-vm.sh's "sports-gap-fill"
# branch substitutes "python " → "$VENV/bin/python " and cd's into the
# instruments workspace before running. We reuse that branch as the generic
# instruments-service-script runner — the audit script lives at
# scripts/audit_fixtures_via_api_football.py inside that workspace.
AUDIT_CMD="python scripts/audit_fixtures_via_api_football.py --apply"
AUDIT_CMD="${AUDIT_CMD} --season-start ${SEASON_START} --season-end ${SEASON_END}"
[[ -n "$RESUME" ]] && AUDIT_CMD="${AUDIT_CMD} --resume ${RESUME}"

echo "Launching $VM_NAME: api_football fixtures truth-set audit"
echo "  seasons: ${SEASON_START}..${SEASON_END}"
[[ -n "$RESUME" ]] && echo "  resume:  ${RESUME}"
echo "  cmd:     ${AUDIT_CMD}"

METADATA="VM_TASK=sports-gap-fill"
# VM_SERVICE selects which service tarball setup-data-pipeline-vm.sh installs
# AND which workspace dir to cd into. Without this, only core (UAC+UTL+deployment)
# is unpacked and the sports-gap-fill branch's `cd $WORKSPACE/instruments` fails.
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_MIGRATION_CMD=${AUDIT_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"
# instruments-store-sports-prd's consolidator merge cycle regularly takes
# 400-460s (>3x the reader's 120s default) — see
# plans/active/issues/manifest_consolidator_stale_sports_bucket_2026_07_21.md
METADATA="${METADATA},MANIFEST_CONSOLIDATED_STALENESS_SEC=1800"

MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY-RUN] Would create VM: "$VM_NAME""
  echo "[DRY-RUN] (gcloud compute instances create skipped)"
else
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          instruments-service unified-api-contracts unified-trading-library deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE:-250GB}" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=fixtures-truthset-audit,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service
fi

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:     gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/sports-gap-fill.log'"
echo "GCS log:  gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Stop:     gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Verify STARTED event after ~90s:"
echo "  gcloud storage ls gs://${PROJECT}-events/events/instruments_service/\$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo ""
echo "On completion, the audit artefacts are at:"
echo "  gs://instruments-store-sports-${PROJECT}/_audits/fixtures_{truthset,diff,recovery_set,mapping_breakage}_*"
