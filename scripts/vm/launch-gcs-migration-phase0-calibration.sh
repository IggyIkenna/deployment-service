#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after prod-run verified + GCS orphan-sweep=0
# Phase 0 calibration VM for the GCS migration bundle (per
# `unified-trading-pm/plans/active/gcs_migration_bundle_pipeline_mode_2026_05_08.md`
# Phase 0 + the runnable protocol in
# `unified-trading-pm/plans/active/issues/gcs_migration_bundle_preaudit_2026_05_08.md`).
#
# Read-only audit. Runs `instruments-service/scripts/reconcile_phantom_manifest_rows_all.py
# --asset-group {ag} --dry-run` for ALL 5 asset_groups (cefi / defi / tradfi /
# sports / prediction) sequentially. The reconciler audit covers the 5 drift
# axes from CLAUDE.md "Manifest phantom audit" + reads the canonical manifest
# per asset_group, which directly populates §§(c) hive-key audit + (d) manifest
# current shape + (e) phantom-row baseline of the Phase 0 pre-audit doc. Wall-
# clock + per-asset-group bytes egress feed §(b) parquet-count estimates +
# §(g) Phase 3 cost projection.
#
# Why a calibration VM (not a full cost+bytes scrape):
#   - The plan's Phase 1 prerequisites need (c) drift histogram + (e) phantom
#     baseline to scope Phase 2 migration drift sweeps + Phase 3 fleet sizing.
#   - Cost estimates in §(g) assume per-bucket parquet counts via gcloud du
#     which are sub-second per bucket (roll-up). A separate `gcloud storage
#     du --summarize` walk is a 10-minute follow-up the operator runs on the
#     same VM via SSH after auto-shutdown, OR a Phase 3 fleet calibration run.
#   - The Phase 0 doc itself notes (line 119): "The pre-audit IS the work for
#     Phase 0" — protocol-shipped + per-AG reconciler dry-run is the minimum
#     viable to feed Phase 1+ with concrete numbers (no more blind sizing).
#
# Default: SOLO sequential (5 asset_groups, ~2-3h aggregate per CLAUDE.md
# phantom-audit recipe — defi 13min, cefi 30-60min, tradfi 20min, sports
# 30min, prediction 5min). Same-region asia-northeast1-c per CLAUDE.md
# "phantom audit re-runnable recipe" (cross-region listing 18× slower).
#
# Singleton lock: refuses to launch if another gcs-migration-phase0-* VM is
# RUNNING in the zone. Each run touches the same canonical manifests; parallel
# instances duplicate GCS list-blob budget without speedup. --force bypasses.
#
# Usage:
#   bash launch-gcs-migration-phase0-calibration.sh                       # default; all 5 ASGs
#   bash launch-gcs-migration-phase0-calibration.sh --force               # bypass singleton
#
# Cost: e2-standard-4 + 50GB. ~2-3h aggregate wall-clock. Listing is
# read-only; no GCS Class A writes; bytes-egress is within-region (free).
#
# Output:
#   - VM stdout/stderr → gs://deployment-scripts-{pid}/vm-logs/{vm_name}/run.log
#     (per-AG `phantom-recon-{ag}.log` for each asset_group)
#   - Lifecycle events → gs://{pid}-events/events/instruments-service/{date}/{vm_name}/
#   - Auto-shutdown when the script exits (VM_SHUTDOWN_ON_COMPLETION=true)
#
# Operator post-run protocol (per Phase 0 doc § "Run protocol"):
#   1. SSH the VM (before auto-shutdown if monitoring early termination):
#      gcloud compute ssh $vm_name --zone=asia-northeast1-c
#   2. Per-AG run.log captures the reconciler's drift-axis breakdown +
#      phantom counts. tail-grep:
#      grep -nE "Axis [1-5]|TOTAL phantoms|asset_group" \
#        gs://deployment-scripts-{pid}/vm-logs/{vm_name}/phantom-recon-*.log
#   3. Append a `## Run results — YYYY-MM-DD` section to the issue doc
#      `unified-trading-pm/plans/active/issues/gcs_migration_bundle_preaudit_2026_05_08.md`
#      with the populated tables for §§(c)(d)(e) per Phase 0 doc.
#   4. Flip Phase 0 plan checkbox once the run results land in the issue doc.
#
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

FORCE=false
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

# Pre-parse named flags before any positional args.
while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --force) FORCE=true; shift ;;
        --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
        *) break ;;
    esac
done

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-standard-4"
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
        --filter='name~"^gcs-migration-phase0-" AND status=RUNNING' \
        --zones="$ZONE" \
        --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
        cat >&2 <<EOF
ERROR: gcs-migration-phase0-* VM already running in $ZONE: $EXISTING
Refusing to launch a duplicate — concurrent GCS list-blob budget shared,
no speedup. Wait for it to drain or pass --force to bypass.

Options:
  Inspect:   gsutil cat gs://${CODE_BUCKET}/vm-logs/${EXISTING}/run.log | tail -50
  Force:     bash $0 --force

CAUTION — do NOT delete $EXISTING unless you have confirmed via Inspect/Tail
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If, after that check, it is genuinely stale, construct the delete
command yourself — do not copy-paste one from this message — per infra.md
STEP 0.65's 3-signal staleness check (heartbeat age, run.log tail, manifest
mtime); when unsure, escalate instead of deleting.
EOF
        exit 1
    fi
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
VM_NAME="gcs-migration-phase0-${RUN_TS}"

echo "Launching $VM_NAME: GCS migration bundle Phase 0 calibration (all 5 asset_groups, dry-run)"

# Compose the multi-asset-group reconciler invocation. Sequential per-AG so
# we get distinct per-AG event signatures + log lines (vs racing parallelism
# that would merge log streams + duplicate GCS list-blob budget).
#
# WORKSPACE = /home/ikennaigboaka/workspace per setup-data-pipeline-vm.sh
# convention; instruments-service tarball lands at $WORKSPACE/instruments
# (TARBALL_DIRS alias). $VENV substituted by setup script's `python` rewrite.
RECON_SCRIPT="/home/ikennaigboaka/workspace/instruments/scripts/reconcile_phantom_manifest_rows_all.py"
LOG_DIR="/home/ikennaigboaka/logs"

# Bash heredoc → single command for VM_BACKFILL_CMD. setup-data-pipeline-vm.sh
# rewrites `python ` → `$VENV/bin/python ` and runs verbatim.
BACKFILL_CMD="bash -c '\
set -euo pipefail; \
mkdir -p ${LOG_DIR}; \
echo \"=== Phase 0 calibration start: \$(date -Iseconds) ===\"; \
for AG in cefi defi tradfi sports prediction; do \
  echo \"\"; \
  echo \"=== ASSET GROUP: \$AG (start \$(date -Iseconds)) ===\"; \
  python ${RECON_SCRIPT} --asset-group \"\$AG\" --dry-run 2>&1 | tee -a ${LOG_DIR}/phantom-recon-\$AG.log; \
  echo \"=== ASSET GROUP: \$AG (end \$(date -Iseconds)) ===\"; \
done; \
echo \"\"; \
echo \"=== Phase 0 calibration complete: \$(date -Iseconds) ===\"'"

METADATA="VM_TASK=phantom-recon"
METADATA="${METADATA},VM_SERVICE=instruments_service"
METADATA="${METADATA},VM_OPERATION=gcs-migration-phase0-calibration"
METADATA="${METADATA},VM_ASSET_GROUP=ALL"
METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        instruments-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
    --labels=purpose=gcs-migration-phase0,env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo ""
echo "VM launched: $VM_NAME"
echo "Logs:        gcloud compute ssh $VM_NAME --zone=$ZONE --command 'tail -f /home/ikennaigboaka/logs/phantom-recon-*.log'"
echo "GCS log:     gsutil cat gs://${CODE_BUCKET}/vm-logs/${VM_NAME}/run.log"
echo "Events:      gcloud storage ls gs://${PROJECT}-events/events/instruments-service/$(date -u +%Y-%m-%d)/${VM_NAME}/"
echo "Delete:      gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
