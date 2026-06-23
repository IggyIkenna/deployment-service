#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Bucket-naming SSOT: env-aware shape codified 2026-05-11 per
# `bucket_name_ssot_canonicalisation_2026_05_10.md` Phase 0f. `--env $DEPLOYMENT_ENV`
# is propagated to VM metadata so bucket-resolution targets the right env tier.
#
# Consolidated features-service VM launcher (2026-05-08, Phase 8A of
# `unified-trading-pm/plans/active/features_repo_consolidation_2026_05_08.md`).
#
# Replaces the 8 per-family `launch-features-<family>-*.sh` launchers
# (calendar / commodity / cross_instrument / delta_one / multi_timeframe /
# onchain / sports / volatility) with one parameterised entry-point that
# dispatches to `features_service` with `--feature-family <name>`. Per the
# workspace VM launcher SSOT rule
# (`codex/05-infrastructure/launcher-script-ssot.md` + CLAUDE.md), every
# launcher MUST live under `deployment-service/scripts/vm/`.
#
# Standardised CLI axes per `codex/06-coding-standards/cli-convention.md`:
#     python -m features_service \
#         --feature-family <name> \
#         --operation compute \
#         --mode {batch,live} \
#         --asset-group {CEFI|DEFI|TRADFI|SPORTS|PREDICTION}
# (calendar is the one family that does NOT take --asset-group — its output
# is global / non-asset_group-scoped.)
#
# (feature_family × asset_group) viability matrix (lifted from the legacy
# `launch-features-backfill-vm.sh` cell map):
#     calendar         : CEFI, TRADFI                (no --asset-group passed; output is global)
#     commodity        : TRADFI only
#     cross_instrument : CEFI, TRADFI, PREDICTION
#     delta_one        : CEFI, DEFI, TRADFI, PREDICTION
#     multi_timeframe  : CEFI, DEFI, TRADFI
#     onchain          : DEFI only
#     sports           : SPORTS only
#     volatility       : CEFI, TRADFI
#
# Naming (per workspace VM Naming Convention):
#     VM_NAME = features-<family-dashed>-<asset_group_lower>-<RUN_TS>
#   e.g. features-delta-one-cefi-20260508-141500
#        features-calendar-global-20260508-141500   (calendar, no asset_group)
#
# Watchdog: the `features-` prefix is already registered as heartbeat-only in
#     `deployment-service/scripts/vm/vm_zombie_watchdog.py:329`
# (covers all 8 families' VMs with one entry).
#
# Operational safety (per workspace rules):
#   * MANIFEST_PER_VM_SHARDS=true + VM_NAME=<unique-tag> for per-VM shard
#     isolation (concurrent-backfill rule, codified 2026-05-06).
#   * RUN_TS=$(date +%Y%m%d-%H%M%S) — sortable + greppable.
#   * 50 GB boot disk; e2-standard-8 default.
#   * VM_SHUTDOWN_ON_COMPLETION=true for opt-in auto-delete after task
#     completion (read by `vm-exec-with-gcs-tee.sh:253`).
#   * DEPLOYMENT_STARTED/PROGRESS/COMPLETED events via deployment_heartbeat.py.
#   * stdout/stderr tee'd via vm-exec-with-gcs-tee.sh.
#   * asia-northeast1-c (workspace default zone).
#
# Usage:
#   bash launch-features-vm.sh \
#       --feature-family <name> \
#       --asset-group {CEFI|DEFI|TRADFI|SPORTS|PREDICTION|GLOBAL} \
#       --start-date YYYY-MM-DD \
#       --end-date YYYY-MM-DD \
#       [--mode {batch|live}] \
#       [--operation <op>] \
#       [--launch-mode {dry|full}]
#
# `--asset-group GLOBAL` is the calendar-family special case (no --asset-group
# is passed to the underlying CLI; the VM-name slug becomes "global").
#
# Examples:
#   bash launch-features-vm.sh --feature-family delta_one --asset-group CEFI \
#       --start-date 2020-01-01 --end-date 2026-04-18 --launch-mode full
#   bash launch-features-vm.sh --feature-family onchain --asset-group DEFI \
#       --start-date 2020-01-01 --end-date 2026-04-18 --launch-mode dry
#   bash launch-features-vm.sh --feature-family calendar --asset-group GLOBAL \
#       --start-date 2020-01-01 --end-date 2026-04-18 --launch-mode full
#
# Optional environment overrides:
#   FEATURE_GROUP=<group>     narrower-than-family selector (delta_one /
#                             volatility / onchain accept this; others ignore)
#   SKIP_DEPENDENCY_CHECK=1   bypass global preflight (narrow-scope runs only)
#   FORCE=1                   rewrite parquets even if manifest shows captured

set -euo pipefail

# ---------- argument parsing ----------
FEATURE_FAMILY=""
ASSET_GROUP=""
START_DATE=""
END_DATE=""
MODE="batch"
OPERATION="compute"
LAUNCH_MODE="dry"  # dry | full
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"

print_usage() {
    cat <<'EOF'
Usage: launch-features-vm.sh \
    --feature-family <name> \
    --asset-group {CEFI|DEFI|TRADFI|SPORTS|PREDICTION|GLOBAL} \
    --start-date YYYY-MM-DD \
    --end-date YYYY-MM-DD \
    [--mode {batch|live}] \
    [--operation <op>] \
    [--launch-mode {dry|full}] \
    [--env <prod|staging|dev>]

feature-family ∈ {
  calendar, commodity, cross_instrument, delta_one,
  multi_timeframe, onchain, sports, volatility
}
asset-group   ∈ { CEFI, DEFI, TRADFI, SPORTS, PREDICTION, GLOBAL }
              (GLOBAL is the calendar-family special case — no --asset-group
              is passed to the underlying CLI.)

Optional env overrides:
  FEATURE_GROUP=<group>     narrower-than-family selector
  SKIP_DEPENDENCY_CHECK=1   bypass global preflight (narrow-scope runs only)
  FORCE=1                   rewrite parquets even if manifest shows captured
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --feature-family) FEATURE_FAMILY="${2:-}"; shift 2 ;;
        --asset-group)    ASSET_GROUP="${2:-}";    shift 2 ;;
        --start-date)     START_DATE="${2:-}";     shift 2 ;;
        --end-date)       END_DATE="${2:-}";       shift 2 ;;
        --mode)           MODE="${2:-}";           shift 2 ;;
        --operation)      OPERATION="${2:-}";      shift 2 ;;
        --launch-mode)    LAUNCH_MODE="${2:-}";    shift 2 ;;
        --env)            DEPLOYMENT_ENV="${2:-}"; shift 2 ;;
        -h|--help)        print_usage; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; print_usage; exit 2 ;;
    esac
done

if [[ -z "$FEATURE_FAMILY" || -z "$ASSET_GROUP" || -z "$START_DATE" || -z "$END_DATE" ]]; then
    print_usage
    exit 2
fi

case "$DEPLOYMENT_ENV" in
    prod|staging|dev) ;;
    *) echo "ERROR: --env must be one of prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

# ---------- viability matrix ----------
_is_viable_cell() {
    local family="$1" ag="$2"
    case "$family/$ag" in
        calendar/CEFI|calendar/TRADFI|calendar/GLOBAL) return 0 ;;
        commodity/TRADFI) return 0 ;;
        cross_instrument/CEFI|cross_instrument/TRADFI|cross_instrument/PREDICTION) return 0 ;;
        delta_one/CEFI|delta_one/DEFI|delta_one/TRADFI|delta_one/PREDICTION) return 0 ;;
        multi_timeframe/CEFI|multi_timeframe/DEFI|multi_timeframe/TRADFI) return 0 ;;
        onchain/DEFI) return 0 ;;
        sports/SPORTS) return 0 ;;
        volatility/CEFI|volatility/TRADFI) return 0 ;;
        *) return 1 ;;
    esac
}

if ! _is_viable_cell "$FEATURE_FAMILY" "$ASSET_GROUP"; then
    echo "Not a viable (feature_family × asset_group) cell: $FEATURE_FAMILY × $ASSET_GROUP" >&2
    echo "See header for the viable matrix." >&2
    exit 2
fi

case "$LAUNCH_MODE" in
    dry|full) ;;
    *) echo "--launch-mode must be 'dry' or 'full' (got: $LAUNCH_MODE)" >&2; exit 2 ;;
esac

# ---------- env / naming ----------
ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
MACHINE_TYPE="e2-standard-8"
BOOT_DISK_GB="50"

RUN_TS="$(date +%Y%m%d-%H%M%S)"
ASSET_GROUP_LOWER="$(echo "$ASSET_GROUP" | tr '[:upper:]' '[:lower:]')"
FAMILY_DASHED="$(echo "$FEATURE_FAMILY" | tr '_' '-')"
VM_NAME="features-${FAMILY_DASHED}-${ASSET_GROUP_LOWER}-${RUN_TS}"

# ---------- assemble service CLI command ----------
CMD="python -m features_service --feature-family ${FEATURE_FAMILY}"
CMD="$CMD --operation ${OPERATION} --mode ${MODE}"
CMD="$CMD --start-date ${START_DATE} --end-date ${END_DATE}"

# calendar's CLI does NOT accept --asset-group (output is global, not
# per-asset_group; calendar_orchestrator.py:373 passes asset_group=""
# through to the manifest writer). Every other family requires it.
if [[ "$FEATURE_FAMILY" != "calendar" ]]; then
    CMD="$CMD --asset-group ${ASSET_GROUP}"
fi

# delta_one / volatility / onchain accept --feature-group; the other
# families don't accept the flag at all. Backfill defaults to ALL —
# strategy-/scenario-driven targeted runs override via FEATURE_GROUP env var.
case "$FEATURE_FAMILY" in
    delta_one|volatility|onchain) CMD="$CMD --feature-group ${FEATURE_GROUP:-ALL}" ;;
esac

if [[ -n "${SKIP_DEPENDENCY_CHECK:-}" ]]; then
    CMD="$CMD --skip-dependency-check"
fi

if [[ -n "${FORCE:-}" ]]; then
    CMD="$CMD --force"
fi

if [[ "$LAUNCH_MODE" == "dry" ]]; then
    CMD="$CMD --dry-run"
fi

echo "Launching $VM_NAME"
echo "  cmd: $CMD"
echo "  zone: $ZONE, machine: $MACHINE_TYPE, boot: ${BOOT_DISK_GB}G"

# ---------- VM metadata ----------
MD="VM_TASK=features-backfill"
MD="${MD},VM_SERVICE=features_service"
MD="${MD},VM_FEATURE_FAMILY=${FEATURE_FAMILY}"
MD="${MD},VM_OPERATION=${OPERATION}-${FAMILY_DASHED}-${ASSET_GROUP_LOWER}"
MD="${MD},VM_ASSET_GROUP=${ASSET_GROUP}"
MD="${MD},VM_START_DATE=${START_DATE}"
MD="${MD},VM_END_DATE=${END_DATE}"
MD="${MD},VM_BACKFILL_CMD=${CMD}"
MD="${MD},VM_BACKFILL_MODE=${LAUNCH_MODE}"
MD="${MD},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
# Per-VM shard isolation (workspace rule, codified 2026-05-06):
MD="${MD},VM_NAME=${VM_NAME}"
MD="${MD},MANIFEST_PER_VM_SHARDS=true"
# Auto-delete on completion (read by vm-exec-with-gcs-tee.sh:253):
MD="${MD},VM_SHUTDOWN_ON_COMPLETION=true"

# ---------- launch ----------
gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --boot-disk-size="${BOOT_DISK_GB}GB" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${MD}" \
    --labels=purpose=features-backfill,family="${FAMILY_DASHED}",category="${ASSET_GROUP_LOWER}",mode="${LAUNCH_MODE}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}"

echo "  SSH: gcloud compute ssh $VM_NAME --zone=$ZONE"
echo "  Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Post-backfill manifest rebuild (one per features bucket):"
echo "  python -c \"from unified_trading_library.manifest_writer import rebuild_manifest_from_canonical_paths; \\"
echo "    rebuild_manifest_from_canonical_paths('features-${FAMILY_DASHED}-${ASSET_GROUP_LOWER}-central-element-323112', \\"
echo "      service_name='features-service', prefix='features/by_date')\""
