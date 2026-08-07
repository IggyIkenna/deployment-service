#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Launch per-archetype scenario regression matrix VMs (Phase 9).
#
# Each VM runs `python -m unified_trading_library.scenario.run_matrix`
# which iterates ScenarioMatrixRunner over every scenario registered for the
# given archetype. One VM per archetype; singleton-locked by archetype prefix.
#
# Pre-cutover scope (SCENARIO_REGISTRY ≥10, full criterion ≥34):
#   - Observer is the no-op probe (proves SCENARIO_REGISTRY wiring)
#   - GCS matrix.parquet emission deferred until execution-service Phase 3.E/F
#   - Full Phase 9 criterion requires SCENARIO_REGISTRY ≥34 (currently 10)
#     and execution-service adversarial-mode observer (Harsh slot 5 target)
#
# Coverage: carry_staked_basis + ARBITRAGE_PRICE_DISPERSION (per Phase 9.A)
#
# VM prefix → watchdog registration:
#   carry_staked_basis         → scenario-matrix-carry-
#   ARBITRAGE_PRICE_DISPERSION → scenario-matrix-arb-
#   (both in vm_zombie_watchdog.py VM_PREFIX_TO_BUCKET under "scenario-matrix-")
#
# Usage:
#   bash launch-scenario-runner-vm.sh                          # both archetypes, today
#   bash launch-scenario-runner-vm.sh --archetype carry        # carry only
#   bash launch-scenario-runner-vm.sh --archetype arb          # arb only
#   bash launch-scenario-runner-vm.sh --force                  # bypass singleton lock
#   bash launch-scenario-runner-vm.sh --date 2026-05-19        # explicit run date
#   bash launch-scenario-runner-vm.sh --env staging            # staging env tier
#
# Singleton lock: per-archetype, refuses to launch if a same-prefix VM is RUNNING.
# Pass --force to bypass.
#
# Post-launch verification (per "No fire-and-forget VM launches" HARD RULE):
#   gcloud compute instances list --filter="name~scenario-matrix-"
#   gcloud compute instances describe <vm-name> --zone=asia-northeast1-c
# Expect RUNNING within 60s; STOPPED on completion.
#
# Cost: e2-standard-2, ~5-10 min per archetype (10 scenarios pre-cutover).
set -euo pipefail

# shellcheck source=lib/launcher_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/launcher_common.sh"

DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-prod}"
FORCE=false
SINGLE_ARCHETYPE=""
RUN_DATE="$(date +%Y-%m-%d)"

_positional=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) export LC_DRY_RUN=true; shift ;;
    --force) FORCE=true; shift ;;
    --env) DEPLOYMENT_ENV="$2"; shift 2 ;;
    --archetype) SINGLE_ARCHETYPE="$2"; shift 2 ;;
    --date) RUN_DATE="$2"; shift 2 ;;
    *) _positional+=("$1"); shift ;;
  esac
done
set -- "${_positional[@]+"${_positional[@]}"}"

case "$DEPLOYMENT_ENV" in
  prod|staging|dev) ;;
  *) echo "ERROR: --env must be prod/staging/dev (got: $DEPLOYMENT_ENV)" >&2; exit 1 ;;
esac

ZONE="asia-northeast1-c"
PROJECT="central-element-323112"
CODE_BUCKET="deployment-scripts-${PROJECT}"
SCENARIO_REPORTS_BUCKET="scenario-reports-${PROJECT}"
RUN_TS="$(date +%Y%m%d-%H%M%S)"

# archetype_key → (vm_prefix, archetype_id passed to run_matrix)
declare -A ARCHETYPE_PREFIX
declare -A ARCHETYPE_ID
ARCHETYPE_PREFIX["carry"]="scenario-matrix-carry-"
ARCHETYPE_ID["carry"]="carry_staked_basis"

ARCHETYPE_PREFIX["arb"]="scenario-matrix-arb-"
ARCHETYPE_ID["arb"]="ARBITRAGE_PRICE_DISPERSION"

ALL_ARCHETYPES=("carry" "arb")

if [[ -n "$SINGLE_ARCHETYPE" ]]; then
  if [[ -z "${ARCHETYPE_PREFIX[$SINGLE_ARCHETYPE]:-}" ]]; then
    echo "ERROR: Unknown --archetype '$SINGLE_ARCHETYPE'. Valid: ${ALL_ARCHETYPES[*]}" >&2
    exit 1
  fi
  ARCHETYPES=("$SINGLE_ARCHETYPE")
else
  ARCHETYPES=("${ALL_ARCHETYPES[@]}")
fi

launched=()
skipped=()

for KEY in "${ARCHETYPES[@]}"; do
  PREFIX="${ARCHETYPE_PREFIX[$KEY]}"
  ARCHETYPE="${ARCHETYPE_ID[$KEY]}"

  # ── Per-archetype singleton lock ─────────────────────────────────────────────
  if ! $FORCE; then
    EXISTING="$(gcloud compute instances list \
      --filter="name~\"^${PREFIX}\" AND status=RUNNING" \
      --zones="$ZONE" \
      --format='value(name)' 2>/dev/null | head -1)"
    if [[ -n "$EXISTING" ]]; then
      echo "SKIP $ARCHETYPE — VM already running: $EXISTING (pass --force to override)" >&2
      skipped+=("$ARCHETYPE ($EXISTING)")
      continue
    fi
  fi

  VM_NAME="${PREFIX}${RUN_TS}"

  # VM_BACKFILL_CMD runs the scenario matrix CLI:
  #   python -m unified_trading_library.scenario.run_matrix \
  #       --archetype <id> --date <YYYY-MM-DD>
  # GCS --output-bucket arg omitted pre-cutover (parquet emit deferred Phase 3.E/F).
  BACKFILL_CMD="python -m unified_trading_library.scenario.run_matrix --archetype ${ARCHETYPE} --date ${RUN_DATE}"

  echo "Launching $VM_NAME: archetype=$ARCHETYPE date=$RUN_DATE"

  METADATA="VM_TASK=scenario-matrix"
  METADATA="${METADATA},VM_SERVICE=unified_trading_library"
  METADATA="${METADATA},VM_BACKFILL_CMD=${BACKFILL_CMD}"
  METADATA="${METADATA},DEPLOYMENT_ENV=${DEPLOYMENT_ENV}"
  METADATA="${METADATA},VM_SHUTDOWN_ON_COMPLETION=true"

  if [[ "${LC_DRY_RUN:-false}" != "true" ]]; then
      lc_verify_tarball_freshness "$CODE_BUCKET" \
          unified-trading-library unified-api-contracts deployment-service \
          || { echo "ERROR: aborting launch on stale tarball(s) — see above" >&2; exit 1; }
  fi

  lc_gcloud_create "$VM_NAME" "$PROJECT" "$ZONE" "e2-standard-2" "50" \
      "startup-script-url=gs://${CODE_BUCKET}/vm/setup-data-pipeline-vm.sh,${METADATA}" \
      "purpose=scenario-matrix,archetype=${KEY},env=${DEPLOYMENT_ENV},run-ts=${RUN_TS}" \
      "$(lc_tier_service_account "${DEPLOYMENT_ENV}" "$PROJECT")"

  launched+=("$VM_NAME")
done

echo ""
if [[ ${#launched[@]} -gt 0 ]]; then
  echo "Launched ${#launched[@]} VM(s):"
  for vm in "${launched[@]}"; do
    echo "  $vm"
    echo "    Log:    gcloud compute ssh $vm --zone=$ZONE --command 'sudo tail -f /home/ikennaigboaka/logs/scenario-matrix.log'"
    echo "    GCS:    gsutil ls gs://${SCENARIO_REPORTS_BUCKET}/matrix/"
    echo "    Delete: gcloud compute instances delete $vm --zone=$ZONE --quiet"
  done
  echo ""
  echo "NOTE (pre-cutover): GCS matrix.parquet emission is deferred pending"
  echo "  execution-service Phase 3.E/F. VMs validate SCENARIO_REGISTRY wiring"
  echo "  (currently $(
    python3 -c "from unified_api_contracts.canonical.crosscutting.scenario_overlay import SCENARIO_REGISTRY; print(len(SCENARIO_REGISTRY))" 2>/dev/null || echo "?"
  ) scenarios registered; full criterion requires ≥34)."
fi
if [[ ${#skipped[@]} -gt 0 ]]; then
  echo ""
  echo "Skipped ${#skipped[@]} archetype(s) (already running): ${skipped[*]}"
fi
