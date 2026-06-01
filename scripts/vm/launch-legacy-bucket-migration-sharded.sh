#!/usr/bin/env bash
# Sharded legacy→canonical tick-bucket migration launcher.
#
# Plan: unified-trading-pm/plans/active/bucket_name_ssot_legacy_dual_write_remediation_2026_06_01.md (Phase 5).
#
# Fans out one VM per (legacy-bucket × date-shard), each running the committed
# migrate_legacy_tick_buckets_to_canonical.py via the existing VM_TASK=canonical-migration
# route (setup-data-pipeline-vm.sh pulls the mtds tarball, venv, runs the command, heartbeats,
# self-deletes). Server-side gcs_copy_object → metadata-speed; parallel shards → ~30 min.
#
# Each shard copies DATA only (--no-manifest); manifests are seeded once per bucket afterward.
# Idempotent: re-runs skip objects already in canonical.
#
# Usage:
#   bash launch-legacy-bucket-migration-sharded.sh --preview        # print VM plan, no VMs
#   bash launch-legacy-bucket-migration-sharded.sh                  # launch all shards
#   bash launch-legacy-bucket-migration-sharded.sh cefi             # one bucket only
set -euo pipefail

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE="e2-standard-4"
BOOT_DISK_GB=50
RUN_TS="$(date +%Y%m%d-%H%M%S)"
PREVIEW=false
ONLY_GROUP=""

# SHA-pin the core tarballs so the VMs pull immutable copies (NOT the floating
# *-code.tar.gz, which parallel-agent rebuilds overwrite — that race caused the
# 2026-06-01 first-attempt failure where mtds-code.tar.gz lacked the migration
# script). Override via env if rebuilding from a newer commit.
UAC_TARBALL_SHA="${UAC_TARBALL_SHA:-6b98c9d91af3}"
UTL_TARBALL_SHA="${UTL_TARBALL_SHA:-13d20a251d0b}"
MTDS_TARBALL_SHA="${MTDS_TARBALL_SHA:-844124f7e0a3}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --preview) PREVIEW=true; shift ;;
    cefi|defi|tradfi|sports|prediction) ONLY_GROUP="$1"; shift ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Shard prefix-sets (kept simple: one variable per shard label; no arrays-of-strings).
PFX_Y2026="--prefix raw_tick_data/by_date/day-2026"
PFX_Y2025="--prefix raw_tick_data/by_date/day-2025"
PFX_Y2024="--prefix raw_tick_data/by_date/day-2024"
PFX_MISC="--prefix processed_candles/ --prefix configs/ --prefix databento-batch-registry/ --prefix raw_tick_data/by_date/day-2020 --prefix raw_tick_data/by_date/day-2021 --prefix raw_tick_data/by_date/day-2022 --prefix raw_tick_data/by_date/day-2023"

COUNT=0
echo "Sharded legacy→canonical migration — RUN_TS=${RUN_TS}  mode=$([[ "$PREVIEW" == true ]] && echo PREVIEW || echo LAUNCH)"

for GROUP in cefi defi tradfi sports prediction; do
  if [[ -n "$ONLY_GROUP" && "$GROUP" != "$ONLY_GROUP" ]]; then
    continue
  fi
  for LABEL in y2026 y2025 y2024 misc; do
    case "$LABEL" in
      y2026) PFX="$PFX_Y2026" ;;
      y2025) PFX="$PFX_Y2025" ;;
      y2024) PFX="$PFX_Y2024" ;;
      misc)  PFX="$PFX_MISC" ;;
    esac
    VM_NAME="canonical-migration-legacy-${GROUP}-${LABEL}-${RUN_TS}"
    CMD="python scripts/migrate_legacy_tick_buckets_to_canonical.py --apply --only ${GROUP} ${PFX} --no-manifest"
    COUNT=$((COUNT + 1))
    echo "  ${VM_NAME}"
    echo "      ${CMD}"
    if [[ "$PREVIEW" == "true" ]]; then
      continue
    fi
    GROUP_UPPER=$(echo "$GROUP" | tr '[:lower:]' '[:upper:]')
    MD="VM_TASK=canonical-migration"
    MD="${MD},VM_SERVICE=market_tick_data_service"
    MD="${MD},VM_OPERATION=migrate-legacy-${GROUP}-${LABEL}"
    MD="${MD},VM_ASSET_GROUP=${GROUP_UPPER}"
    MD="${MD},VM_MIGRATION_CMD=${CMD}"
    MD="${MD},VM_MIGRATION_MODE=full"
    MD="${MD},DEPLOYMENT_ENV=prod"
    MD="${MD},VM_SHUTDOWN_ON_COMPLETION=true"
    MD="${MD},UAC_TARBALL_SHA=${UAC_TARBALL_SHA}"
    MD="${MD},UTL_TARBALL_SHA=${UTL_TARBALL_SHA}"
    MD="${MD},MTDS_TARBALL_SHA=${MTDS_TARBALL_SHA}"
    gcloud compute instances create "$VM_NAME" \
      --project="$PROJECT" \
      --zone="$ZONE" \
      --machine-type="$MACHINE" \
      --image-family=ubuntu-2404-lts-amd64 \
      --image-project=ubuntu-os-cloud \
      --boot-disk-size="${BOOT_DISK_GB}GB" \
      --scopes=cloud-platform \
      --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${MD}" \
      --labels=purpose=legacy-bucket-migration,category="${GROUP}",shard="${LABEL}",run-ts="${RUN_TS}" \
      1>/dev/null
  done
done

echo "Total shards: ${COUNT}"
if [[ "$PREVIEW" != "true" ]]; then
  echo
  echo "Verify (T+10min): gcloud compute instances list --project=${PROJECT} --filter=\"labels.run-ts=${RUN_TS}\""
  echo "Logs: gs://${CODE_BUCKET}/vm-logs/<vm-name>/run.log"
  echo "After all complete, seed manifests:"
  echo "  python scripts/migrate_legacy_tick_buckets_to_canonical.py --apply --manifest-only"
fi
