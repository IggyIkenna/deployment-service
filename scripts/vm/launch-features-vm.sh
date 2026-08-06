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
#     cefi             : CEFI only
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
#   INSTRUMENTS=<ids>         space-separated canonical ids passed as
#                             --instruments to the service CLI (e.g. "CME:FUTURES:ES")
#   TIMEFRAME=<tf>            delta_one --timeframe override. The service CLI parser
#                             defaults --timeframe to "15s" unconditionally
#                             (delta_one/cli/parser.py) — a CEFI-only concept with no
#                             TradFi equivalent (MDPS never writes tick-level 15s
#                             candles for futures), so every TRADFI delta_one launch
#                             MUST pass this (e.g. "1m") or every instrument silently
#                             reads zero candles (found live 2026-07-26: a real ES
#                             futures_basis production launch failed end-to-end this
#                             way — every date "skipped" for lack of 15s data that was
#                             never expected to exist).
#   SKIP_DEPENDENCY_CHECK=1   bypass global preflight (narrow-scope runs only)
#   FORCE=1                   rewrite parquets even if manifest shows captured
#   MAX_WORKERS=<N>           within-VM multiproc (delta_one/volatility/onchain only —
#                             the CLI default is 4; passthrough added learning-from-cefi)
#   OOM_MONITOR=1             opt-in on-VM ps/free/dmesg poller (3s interval, uploaded to
#                             gs://.../vm-logs/<vm>/oom-hang-monitor.log every 15s) — for
#                             settling OOM-vs-hang repros, not a default-on production knob.
#                             See deployment-service/scripts/vm/oom-hang-monitor.sh.

set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

# ---------- argument parsing ----------
FEATURE_FAMILY=""
ASSET_GROUP=""
START_DATE=""
END_DATE=""
MODE="batch"
OPERATION="compute"
LAUNCH_MODE="dry"  # dry | full
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
# Idempotent backfill defaults to SPOT (~60-91% cheaper); GCP promo credits
# exhausted 2026-06-20 so on-demand burns real cash. --on-demand forces standard.
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md.
ON_DEMAND=false
# pipeline-e2e-check hooks (added 2026-07-20, mirror launch-mtds-backfill-vm.sh):
#   --vm-name <name>       deterministic VM name so the shared UTL launch_vm_and_wait
#                          engine can poll gs://.../vm-logs/<vm>/ (must prefix-match the
#                          registered "features-" VM_PREFIX_TO_BUCKET entry).
#   --sink-bucket <b>      route feature OUTPUT to a -test- bucket without mutating prod:
#                          bakes IS_TEST_RUN=true + PROTOCOL_DATA_SINK_BUCKET_{AG}=<b> into
#                          VM_BACKFILL_CMD (the caller resolves <b> via resolve_bucket_name
#                          kind=features deployment_env=test — keeps bucket logic in Python).
#   --source-bucket <b>    route feature INPUT reads to <b> (PROTOCOL_DATA_SOURCE_BUCKET).
VM_NAME_OVERRIDE="${VM_NAME_OVERRIDE:-}"
TEST_SINK_BUCKET=""
TEST_SOURCE_BUCKET=""
# Derived below once --sink-bucket/--source-bucket are parsed: forces the launch
# VM's service-account to uts-test-sa (DP-VM-002 fix — see launcher_common.sh's
# lc_tier_service_account docstring; a -test--bucket-routed run under the
# default DEPLOYMENT_ENV=prod otherwise 403s on every manifest/event write).
IS_TEST_RUN_FLAG="false"

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
  calendar, cefi, commodity, cross_instrument, delta_one,
  multi_timeframe, onchain, sports, volatility
}
asset-group   ∈ { CEFI, DEFI, TRADFI, SPORTS, PREDICTION, GLOBAL }
              (GLOBAL is the calendar-family special case — no --asset-group
              is passed to the underlying CLI.)

Optional env overrides:
  FEATURE_GROUP=<group>     narrower-than-family selector
  INSTRUMENTS=<ids>         space-separated canonical ids (e.g. "CME:FUTURES:ES")
  TIMEFRAME=<tf>            delta_one --timeframe override (defaults to "15s" in the
                            service CLI, which does not exist for TradFi — pass this
                            for every TRADFI delta_one launch)
  SKIP_DEPENDENCY_CHECK=1   bypass global preflight (narrow-scope runs only)
  FORCE=1                   rewrite parquets even if manifest shows captured
  MAX_WORKERS=<N>           within-VM multiproc (delta_one/volatility/onchain only)
  OOM_MONITOR=1             opt-in on-VM ps/free/dmesg poller for OOM-vs-hang repros
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
        --vm-name)        VM_NAME_OVERRIDE="${2:-}"; shift 2 ;;
        --sink-bucket)    TEST_SINK_BUCKET="${2:-}"; shift 2 ;;
        --source-bucket)  TEST_SOURCE_BUCKET="${2:-}"; shift 2 ;;
        --on-demand)      ON_DEMAND=true; shift ;;
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
        cefi/CEFI) return 0 ;;
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
BOOT_DISK_GB="${BOOT_DISK_GB:-250}"

# ── Auto-pin UAC to the current LDR-tip tarball (P0 contract-propagation fix) ──
# Features VMs install UAC as a sibling but historically pinned NO code tarball, so
# a UAC schema/contract change was never version-pinned at launch and the VM fell to
# the unpinned floating pull — a stale contract could reach a features VM even with a
# correct current tarball (plans/active/issues/mdps_vm_stale_uac_contract_propagation_2026_07_20.md).
# Resolve the sha of the UAC tarball that WILL actually be installed (the floating
# manifest's commit_sha, emitted only when its @<sha> pair provably exists) and pin
# it. An operator-exported UAC_TARBALL_SHA always wins (explicit pin).
if [[ -z "${UAC_TARBALL_SHA:-}" ]]; then
    UAC_TARBALL_SHA="$(lc_resolve_tarball_sha "$CODE_BUCKET" unified-api-contracts)"
fi

RUN_TS="$(date +%Y%m%d-%H%M%S)"
ASSET_GROUP_LOWER="$(echo "$ASSET_GROUP" | tr '[:upper:]' '[:lower:]')"
FAMILY_DASHED="$(echo "$FEATURE_FAMILY" | tr '_' '-')"
VM_NAME="features-${FAMILY_DASHED}-${ASSET_GROUP_LOWER}-${RUN_TS}"
# --vm-name override wins (pipeline-e2e-check driver supplies a deterministic name
# that still prefix-matches the registered "features-" VM_PREFIX_TO_BUCKET entry).
if [[ -n "$VM_NAME_OVERRIDE" ]]; then
    VM_NAME="$VM_NAME_OVERRIDE"
fi

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

# delta_one / volatility accept --timeframe (onchain doesn't); the CLI parser
# defaults it to "15s" unconditionally, which doesn't exist for TradFi — pass
# TIMEFRAME explicitly for any TradFi run (see header for the failure mode).
case "$FEATURE_FAMILY" in
    delta_one|volatility) [[ -n "${TIMEFRAME:-}" ]] && CMD="$CMD --timeframe ${TIMEFRAME}" ;;
esac

# Within-VM multiproc (learning-from-cefi/MDPS R1, todo 12 of
# data_pipeline_check_mdps_features_2026_07_20.md): delta_one / volatility / onchain
# already expose --max-workers on their service CLIs (default 4) but no launcher ever
# threaded it through. The other families' CLIs don't accept the flag at all — scoped
# to only these 3 so an unsupported flag never reaches their argparse.
case "$FEATURE_FAMILY" in
    delta_one|volatility|onchain) [[ -n "${MAX_WORKERS:-}" ]] && CMD="$CMD --max-workers ${MAX_WORKERS}" ;;
esac

if [[ -n "${SKIP_DEPENDENCY_CHECK:-}" ]]; then
    CMD="$CMD --skip-dependency-check"
fi

if [[ -n "${FORCE:-}" ]]; then
    CMD="$CMD --force"
fi

if [[ -n "${INSTRUMENTS:-}" ]]; then
    CMD="$CMD --instruments ${INSTRUMENTS}"
fi

if [[ "$LAUNCH_MODE" == "dry" ]]; then
    CMD="$CMD --dry-run"
fi

# pipeline-e2e-check -test- routing: prepend env vars to VM_BACKFILL_CMD so the
# feature writer/reader isolate to the -test- bucket without mutating prod. The VM
# runs VM_BACKFILL_CMD verbatim via `bash -c` (setup-data-pipeline-vm.sh substitutes
# only the FIRST `python ` → `$VENV/bin/python `), so a leading `KEY=val ...` env
# prefix is honoured. Feature writer resolves the sink via get_data_sink(routing_key=
# ag.lower()) which reads PROTOCOL_DATA_SINK_BUCKET_{AG_UPPER}; input via
# PROTOCOL_DATA_SOURCE_BUCKET. AG-keyed + base fallback both set (calendar=GLOBAL).
#
# MANIFEST_ALLOW_STALE_FALLBACK=true rides alongside IS_TEST_RUN=true: -test- tier
# buckets have NO standing manifest-consolidator Cloud Run Job/Scheduler cron
# (terraform's manifest_consolidator_buckets[_extended] maps only cover the
# dev/staging/prod tiers via deployment_env_short — confirmed live 2026-07-27,
# `gcloud run jobs list` has no `*-features-sports-test*` job, only the prod-tier
# `uts-prod-manifest-consolidator-features-sports` targeting
# features-sports-prd-<pid>), by design — provisioning a per-minute job against an
# ephemeral smoke-test bucket would be pure billing waste. So the VM-side read
# against a -test- bucket will ALWAYS see a stale/missing consolidated index; without
# this override `pipeline_e2e_check.py`'s own local driver process tolerates that
# (it sets this same var for itself, see features-service scripts/pipeline_e2e_check.py
# main()) but the remote VM it launches did not inherit the tolerance and hard-refused
# (ManifestConsolidatorStaleError). Test buckets are always small, so the per-VM-shard
# merge this unblocks can't hit the OOM the guard exists to prevent on prod buckets.
if [[ -n "$TEST_SINK_BUCKET" || -n "$TEST_SOURCE_BUCKET" ]]; then
    IS_TEST_RUN_FLAG="true"
    ENV_PREFIX="IS_TEST_RUN=true MANIFEST_ALLOW_STALE_FALLBACK=true"
    if [[ -n "$TEST_SINK_BUCKET" ]]; then
        ENV_PREFIX="$ENV_PREFIX PROTOCOL_DATA_SINK_BUCKET_${ASSET_GROUP}=${TEST_SINK_BUCKET} PROTOCOL_DATA_SINK_BUCKET=${TEST_SINK_BUCKET}"
    fi
    if [[ -n "$TEST_SOURCE_BUCKET" ]]; then
        ENV_PREFIX="$ENV_PREFIX PROTOCOL_DATA_SOURCE_BUCKET=${TEST_SOURCE_BUCKET}"
    fi
    CMD="${ENV_PREFIX} ${CMD}"
fi

# SPOT by default for batch backfill (idempotent → ~60-91% cheaper); --on-demand /
# ON_DEMAND=true forces standard. SAFETY: --mode live is a continuous VM that
# preemption would disrupt → always on-demand regardless of ON_DEMAND.
PROVISIONING_FLAGS="--provisioning-model=SPOT --instance-termination-action=DELETE --no-restart-on-failure"
if $ON_DEMAND || [[ "$MODE" == "live" ]]; then PROVISIONING_FLAGS=""; fi

echo "Launching $VM_NAME [$([[ -n "$PROVISIONING_FLAGS" ]] && echo SPOT || echo on-demand)]"
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
# Version-pin UAC into VM metadata (P0 contract-propagation fix) — setup reads
# UAC_TARBALL_SHA and pulls the exact SHA-pinned unified-api-contracts tarball.
[[ -n "${UAC_TARBALL_SHA:-}" ]] && MD="${MD},UAC_TARBALL_SHA=${UAC_TARBALL_SHA}"
# Opt-in on-VM ps/free/dmesg monitor for OOM-vs-hang repros (see OOM_MONITOR
# env override above; setup-data-pipeline-vm.sh reads this as VM_OOM_MONITOR).
[[ -n "${OOM_MONITOR:-}" ]] && MD="${MD},VM_OOM_MONITOR=true"

# Durable pin registry — instance metadata dies with the instance; this record is
# what survives into the preemption-relaunch window and exempts the pinned UAC
# tarball from the nightly retention sweep (lc_write_tarball_pin_record /
# deployment_service.vm.tarball_pins). UTL is recorded as a deliberate float.
lc_write_tarball_pin_record "$VM_NAME" "$PROJECT" "launch-features-vm.sh" \
    "UTL_TARBALL_SHA=${UTL_TARBALL_SHA:-}" \
    "UAC_TARBALL_SHA=${UAC_TARBALL_SHA:-}"

# ---------- launch ----------
# shellcheck disable=SC2086
if [[ "${DRY_RUN:-false}" != "true" ]]; then
    lc_verify_tarball_freshness "$CODE_BUCKET" \
        features-service market-tick-data-service unified-api-contracts unified-trading-library deployment-service \
        || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
fi

# Data-writing VM: disk sized for sustained writes. A pd-standard 50GB boot disk sustains
# only ~6 MB/s and collapsed the CeFi backfill to 2.36 MB/s after ~7.5GB (measured
# 2026-07-18; on pd-balanced 250GB the same workload held 11.09 MB/s steady-state with the
# disk only 14.7% utilised). Enforced by
# scripts/quality_gates/check_backfill_vm_disk_provisioning.py.
gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --service-account="$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT" "${IS_TEST_RUN_FLAG}")" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    ${PROVISIONING_FLAGS} \
    --boot-disk-size="${BOOT_DISK_GB}GB" --boot-disk-type="${BOOT_DISK_TYPE:-pd-balanced}" \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud \
    --scopes=cloud-platform \
    --metadata="startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${MD}" \
    --labels=purpose=features-backfill,family="${FAMILY_DASHED}",category="${ASSET_GROUP_LOWER}",mode="${LAUNCH_MODE}",env="${DEPLOYMENT_ENV}",run-ts="${RUN_TS}",managed-by=deployment-service

echo "  SSH: gcloud compute ssh $VM_NAME --zone=$ZONE"
echo "  Delete: gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet"
echo ""
echo "Post-backfill manifest rebuild (one per features bucket):"
# Bucket + prefix are RESOLVED via resolve_bucket_name + the real sports prefix.
# The previous hint built 'features-{family}-{ag}-central-element-323112' and
# hardcoded prefix 'features/by_date': for sports that is
# 'features-sports-sports-...' (404 — the real bucket is
# 'features-sports-prd-...') under a prefix that does not exist (real:
# 'sports_features/by_date'). Bucket-name interpolate fixed 2026-07-20.
# Function swapped 2026-08-04: rebuild_manifest_from_canonical_paths()
# wholesale-replaces the manifest index even on a prefix-scoped call (live
# wipe risk once the bucket name resolves); merge_manifest_from_canonical_paths()
# is the additive sibling (prefix required, not optional).
# Bucket-naming SSOT: codex/05-infrastructure/bucket-isolation-model.md.
echo "  python -c \"from unified_trading_library import resolve_bucket_name; \\"
echo "    from unified_trading_library.manifest_writer import merge_manifest_from_canonical_paths; \\"
echo "    merge_manifest_from_canonical_paths( \\"
echo "      resolve_bucket_name(cloud='gcp', kind='features', asset_group='${ASSET_GROUP_LOWER}'), \\"
echo "      prefix='sports_features/by_date')\""
