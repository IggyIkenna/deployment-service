#!/usr/bin/env bash
# Setup a GCE VM for data pipeline work (MTDS backfill, migrations, etc.)
#
# This script is the SSOT for VM setup. It handles:
#   1. Python 3.13 installation (required by UAC)
#   2. Build tools for C extensions (ckzg, lru-dict needed by web3/UTL)
#   3. Venv creation with correct Python version
#   4. Code deployment from GCS tarballs
#   5. Dependency installation
#
# Usage as startup script (no SSH needed — runs on boot):
#   gcloud compute instances create my-vm \
#     --zone=asia-northeast1-b \
#     --machine-type=e2-standard-4 \
#     --image-family=ubuntu-2404-lts-amd64 \
#     --image-project=ubuntu-os-cloud \
#     --scopes=cloud-platform \
#     --metadata=startup-script-url=gs://deployment-scripts-central-element-323112/vm/setup-data-pipeline-vm.sh \
#     --metadata=VM_TASK=cefi-backfill,VM_VENUE=BINANCE-FUTURES,VM_START_DATE=2020-01-01,VM_END_DATE=2026-04-15
#
# Usage via SSH (manual):
#   bash setup-data-pipeline-vm.sh
#
# Requirements:
#   - Ubuntu 24.04 LTS (has deadsnakes PPA for Python 3.13)
#   - cloud-platform scope (for GCS + Secret Manager access)
#   - Same region as GCS buckets (asia-northeast1) for fast I/O
#
# Lessons learned (2026-04-15):
#   - MUST use python3.13 (not python3/python3.12) — UAC requires >=3.13
#   - MUST install build-essential + python3.13-dev — C extensions fail without them
#   - MUST use python3.13 -m venv (not python3 -m venv) — wrong Python in venv
#   - Tarballs extract to -C target dir, must mkdir -p first
#   - nohup must use full venv path (~/venv/bin/python) not bare 'python'
#   - GCE metadata server uses HTTP internally — no SSL cert issues on Ubuntu
#   - Tardis free data on 1st of month — skip auth to save API calls
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="/var/log/vm-setup.log"
WORKSPACE="/home/ikennaigboaka/workspace"
VENV="/home/ikennaigboaka/venv"
LOGS="/home/ikennaigboaka/logs"
CODE_BUCKET="${CODE_BUCKET:-deployment-scripts-central-element-323112}"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG"; }

# ── 0. File descriptor limit (Gate G8.1) ──
# MTDS backfill opens one StreamingParquetWriter per shard (per instrument, per
# data_type). Busy venues can exceed the default Linux soft limit of 1024 FDs in
# a single day, producing OSError(24, 'Too many open files'). Raise to 65536
# for this shell AND for any systemd-managed service that inherits from the
# unit defaults, then record the effective limit in the setup log.
log "Raising file descriptor limit for backfill workload..."
ulimit -n 65536 || log "WARNING: ulimit -n 65536 failed (non-fatal, current: $(ulimit -n))"

mkdir -p /etc/systemd/system.conf.d/
cat > /etc/systemd/system.conf.d/file-descriptors.conf <<'EOF'
[Manager]
DefaultLimitNOFILE=65536
EOF
systemctl daemon-reexec 2>/dev/null || log "WARNING: systemctl daemon-reexec failed (non-fatal)"
log "File descriptor limit: $(ulimit -n) (systemd default raised to 65536)"

# ── 1. System packages ──
log "Installing system packages..."
export DEBIAN_FRONTEND=noninteractive
add-apt-repository -y ppa:deadsnakes/ppa 2>/dev/null || true
apt-get update -qq
apt-get install -y -qq \
  python3.13 python3.13-venv python3.13-dev \
  build-essential \
  git curl pipx

log "Python 3.13: $(python3.13 --version)"

# ── 2. Create venv with correct Python ──
log "Creating venv with Python 3.13..."
rm -rf "$VENV"
python3.13 -m venv "$VENV"
source "$VENV/bin/activate"
log "Venv Python: $(python --version) at $(which python)"

# Install uv inside venv (10x faster than pip for dependency resolution)
pip install uv -q 2>&1 | tail -1
log "uv: $(uv --version)"

# ── 2b. Read VM metadata early (needed for selective tarball install) ──
_meta() { curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1" 2>/dev/null || echo "${2:-}"; }
VM_TASK=$(_meta VM_TASK)
VM_VENUE=$(_meta VM_VENUE)
VM_START_DATE=$(_meta VM_START_DATE)
VM_END_DATE=$(_meta VM_END_DATE)
VM_CATEGORY=$(_meta VM_CATEGORY CEFI)
VM_OPERATION=$(_meta VM_OPERATION download)
VM_SERVICE=$(_meta VM_SERVICE market_tick_data_service)
VM_SPORTS_PROVIDER=$(_meta VM_SPORTS_PROVIDER)
VM_SPORTS_ENTITY=$(_meta VM_SPORTS_ENTITY)
# Rolling forward-poll flags (instruments-service CLI). When present, the VM
# resolves [today-N, today+M] at boot time (UTC) rather than freezing dates at
# launcher-invocation time. SSOT: codex/02-data/sports-scheduling-and-sharding.md §4.
VM_LOOKBACK_DAYS=$(_meta VM_LOOKBACK_DAYS)
VM_LOOKAHEAD_DAYS=$(_meta VM_LOOKAHEAD_DAYS)
VM_FORCE_WINDOW=$(_meta VM_FORCE_WINDOW)
VM_STRATEGY=$(_meta VM_STRATEGY)
VM_PIPELINE_MODE=$(_meta VM_PIPELINE_MODE)
VM_DATA_TYPES=$(_meta VM_DATA_TYPES)
# VM_INSTRUMENT_IDS: semicolon-separated raw venue-native symbols. Added
# 2026-04-19 so cefi-backfill launchers can restrict Tardis downloads to a
# curated symbol list (BTC/ETH majors + operator-selected x-coins, DERIBIT-only
# options). Without this, --instrument-ids is omitted and MTDS downloads the
# full symbol universe per venue — a Tardis cost explosion.
#
# Semicolons (not commas) because gcloud --metadata=K=V,K=V uses comma as the
# key separator: any comma inside a value would be misparsed as a new key.
# Semicolons are converted to spaces when appended to CLI_ARGS because argparse
# nargs='+' takes space-separated values.
VM_INSTRUMENT_IDS=$(_meta VM_INSTRUMENT_IDS)
# IS_TEST_RUN controls whether MTDS writes to market-data-tick-test-{cat} or prod.
# Read from metadata and EXPORT so Python inherits it.
# CRITICAL: only export if non-empty — Pydantic Settings treats an empty-string
# env var as a validation error for bool fields ("Input should be a valid boolean,
# unable to interpret input ''"), which breaks instruments-service startup on
# any VM that doesn't pass IS_TEST_RUN=true explicitly.
IS_TEST_RUN=$(_meta IS_TEST_RUN)
if [[ -n "$IS_TEST_RUN" ]]; then
  export IS_TEST_RUN
fi
log "VM metadata: SERVICE=$VM_SERVICE TASK=$VM_TASK CATEGORY=$VM_CATEGORY PROVIDER=$VM_SPORTS_PROVIDER"
log "VM metadata: STRATEGY=$VM_STRATEGY PIPELINE_MODE=$VM_PIPELINE_MODE"

# ── 3. Deploy code ──
# Core repos (always required) + optional service repos.
# create-code-tarballs.sh uploads these to gs://{CODE_BUCKET}/code/:
#   unified-api-contracts-code.tar.gz  (core)
#   unified-trading-library-code.tar.gz (core)
#   mtds-code.tar.gz                   (core)
#   instruments-service-code.tar.gz    (optional)
#   features-sports-service-code.tar.gz (optional)
#   ... any --include repo
log "Deploying code from GCS..."
mkdir -p "$WORKSPACE/uac" "$WORKSPACE/utl" "$WORKSPACE/mtds" "$LOGS"

# Service → tarball mapping. Only install what VM_SERVICE needs.
# UAC + UTL are always required (shared contracts + library).
declare -A SERVICE_TARBALLS=(
  ["market_tick_data_service"]="mtds-code"
  ["instruments_service"]="instruments-service-code"
  ["features_sports_service"]="features-sports-service-code"
  ["features_onchain_service"]="features-onchain-service-code"
  ["market_data_processing_service"]="market-data-processing-service-code"
  ["features_delta_one_service"]="features-delta-one-service-code"
  ["strategy_service"]="strategy-service-code"
  ["execution_service"]="execution-service-code"
  ["pnl_attribution_service"]="pnl-attribution-service-code"
  ["risk_and_exposure_service"]="risk-and-exposure-service-code"
  ["ml_training_service"]="ml-training-service-code"
  ["ml_inference_service"]="ml-inference-service-code"
  ["position_balance_monitor_service"]="position-balance-monitor-service-code"
  ["features_volatility_service"]="features-volatility-service-code"
  ["features_cross_instrument_service"]="features-cross-instrument-service-code"
  ["features_calendar_service"]="features-calendar-service-code"
  ["features_multi_timeframe_service"]="features-multi-timeframe-service-code"
  ["features_commodity_service"]="features-commodity-service-code"
  ["deployment_service"]="deployment-service-code"
)
# NOTE: unified-events-interface entry intentionally removed 2026-04-17 —
# UEI was folded into unified-trading-library.events. No repo/pyproject depends
# on it anymore; the stale 0-byte tarball in GCS broke VM setup. Keep it out.
declare -A TARBALL_DIRS=(
  ["unified-api-contracts-code"]="uac"
  ["unified-trading-library-code"]="utl"
  ["mtds-code"]="mtds"
  ["instruments-service-code"]="instruments"
  ["features-sports-service-code"]="fss"
  ["features-onchain-service-code"]="fos"
  ["market-data-processing-service-code"]="mdps"
  ["features-delta-one-service-code"]="fd1"
  ["strategy-service-code"]="strategy"
  ["execution-service-code"]="execution"
  ["pnl-attribution-service-code"]="pnl"
  ["risk-and-exposure-service-code"]="risk"
  ["ml-training-service-code"]="ml-train"
  ["ml-inference-service-code"]="ml-infer"
  ["position-balance-monitor-service-code"]="pbm"
  ["features-volatility-service-code"]="fvol"
  ["features-cross-instrument-service-code"]="fci"
  ["features-calendar-service-code"]="fcal"
  ["features-multi-timeframe-service-code"]="fmt"
  ["features-commodity-service-code"]="fcom"
  ["deployment-service-code"]="deployment"
)

# Always install core (UAC + UTL + deployment-service) + the service
# tarball for VM_SERVICE.
# UEI was folded into unified-trading-library.events 2026-04-17 — removed from
# here so the stale 0-byte UEI tarball in GCS doesn't hang VM setup.
# deployment-service-code added 2026-04-18 so deployment_heartbeat.py
# can import deployment_service.deployments_registry — without it every
# VM silently drops DEPLOYMENT_STARTED/PROGRESS/COMPLETED events.
NEEDED_TARBALLS=("unified-api-contracts-code" "unified-trading-library-code" "deployment-service-code")
SERVICE_TARBALL="${SERVICE_TARBALLS[$VM_SERVICE]:-}"
if [ -n "$SERVICE_TARBALL" ]; then
  NEEDED_TARBALLS+=("$SERVICE_TARBALL")
else
  log "WARNING: Unknown VM_SERVICE=$VM_SERVICE — installing all available tarballs"
  for k in "${!TARBALL_DIRS[@]}"; do NEEDED_TARBALLS+=("$k"); done
fi

# Transitive sibling dependency: MDPS + features-* services declare
# `market-tick-data-service>=0.1.0,<1.0.0` in their pyproject.toml but MTDS
# is not on PyPI, so without MTDS installed as a sibling editable install
# the whole resolve fails with "requirements are unsatisfiable". Two MDPS
# backfill VMs died this way 2026-04-19. Keep this list in lockstep with
# pyproject deps of each downstream service.
MTDS_DEPENDENT_SERVICES=(
  "market_data_processing_service"
  "features_delta_one_service"
  "features_onchain_service"
  "features_volatility_service"
  "features_calendar_service"
  "features_multi_timeframe_service"
  "features_cross_instrument_service"
  "features_commodity_service"
  "features_sports_service"
)
for dep_svc in "${MTDS_DEPENDENT_SERVICES[@]}"; do
  if [[ "$VM_SERVICE" == "$dep_svc" ]]; then
    case " ${NEEDED_TARBALLS[*]} " in
      *" mtds-code "*) ;;
      *) NEEDED_TARBALLS+=("mtds-code"); log "  (added mtds-code — $VM_SERVICE depends on MTDS)" ;;
    esac
    break
  fi
done

log "Tarballs to install: ${NEEDED_TARBALLS[*]}"

INSTALLED_DIRS=()

if gsutil ls "gs://${CODE_BUCKET}/code/" >/dev/null 2>&1; then
  for tarball_name in "${NEEDED_TARBALLS[@]}"; do
    dir="${TARBALL_DIRS[$tarball_name]}"
    tarball_path="/tmp/${tarball_name}.tar.gz"
    if gsutil -q cp "gs://${CODE_BUCKET}/code/${tarball_name}.tar.gz" "$tarball_path" 2>/dev/null; then
      mkdir -p "$WORKSPACE/$dir"
      tar xzf "$tarball_path" -C "$WORKSPACE/$dir"
      INSTALLED_DIRS+=("$WORKSPACE/$dir")
      log "Deployed $tarball_name → $WORKSPACE/$dir"
    else
      log "WARNING: tarball $tarball_name not found in GCS — skipping"
    fi
  done
  log "Code deployed from GCS (${#INSTALLED_DIRS[@]} repos)"
elif [ -f /home/ikennaigboaka/unified-api-contracts-code.tar.gz ]; then
  tar xzf /home/ikennaigboaka/unified-api-contracts-code.tar.gz -C "$WORKSPACE/uac"
  tar xzf /home/ikennaigboaka/unified-trading-library-code.tar.gz -C "$WORKSPACE/utl"
  tar xzf /home/ikennaigboaka/mtds-code.tar.gz -C "$WORKSPACE/mtds"
  INSTALLED_DIRS=("$WORKSPACE/uac" "$WORKSPACE/utl" "$WORKSPACE/mtds")
  log "Code deployed from local tarballs"
else
  log "ERROR: No code tarballs found. SCP them to /home/ikennaigboaka/ or upload to gs://${CODE_BUCKET}/code/"
  exit 1
fi

# ── 4. Install dependencies (with GCS wheel cache) ──
# External deps (web3, pandas, etc.) are slow to compile from source.
# Cache compiled wheels in GCS so subsequent VMs skip compilation.
# Code repos (UAC/UTL/service) are installed as editable (-e) — always fresh.
WHEEL_CACHE="/tmp/wheel-cache"
WHEEL_GCS="gs://${CODE_BUCKET}/wheels/py313-linux-x86_64"
mkdir -p "$WHEEL_CACHE"

# Try to download cached wheels
if gsutil -q ls "$WHEEL_GCS/" >/dev/null 2>&1; then
  log "Downloading cached wheels from GCS..."
  gsutil -m -q cp "$WHEEL_GCS/*.whl" "$WHEEL_CACHE/" 2>/dev/null || true
  WHEEL_COUNT=$(ls "$WHEEL_CACHE"/*.whl 2>/dev/null | wc -l)
  log "Downloaded $WHEEL_COUNT cached wheels"
fi

log "Installing Python dependencies..."
# --no-sources: ignore [tool.uv.sources] in pyproject.toml which points to
# sibling paths like ../unified-api-contracts that don't exist in our
# tarball layout (we use short names: uac, utl, instruments).
# Instead, editable installs resolve deps from each other since all are
# installed in the same call.
# Two-pass install. deployment-service declares deployment-api + fastapi
# + functions-framework as hard deps — none of which are needed by the VM
# heartbeat helper (which only touches deployments_registry.py, stdlib +
# UTL StorageClient). Install it with --no-deps to avoid a resolve
# failure that stops the whole VM. Everything else installs normally.
INSTALL_ARGS_STD=("--no-sources")
INSTALL_ARGS_NODEPS=("--no-sources" "--no-deps")
for dir in "${INSTALLED_DIRS[@]}"; do
  if [[ "$dir" == */deployment ]]; then
    INSTALL_ARGS_NODEPS+=("-e" "$dir")
  else
    INSTALL_ARGS_STD+=("-e" "$dir")
  fi
done
log "  uv pip install ${INSTALL_ARGS_STD[*]}"
uv pip install --find-links "$WHEEL_CACHE" "${INSTALL_ARGS_STD[@]}" 2>&1 | tail -5
log "  uv pip install ${INSTALL_ARGS_NODEPS[*]}"
uv pip install --find-links "$WHEEL_CACHE" "${INSTALL_ARGS_NODEPS[@]}" 2>&1 | tail -5

# deployment_service/__init__.py eagerly imports the whole package
# (monitor/orchestrator/backends), which transitively needs jinja2 +
# pyyaml for template rendering in backends/services/vm_config.py and
# yaml parsing in config_loader.py. Importing the heartbeat helper
# (`from deployment_service.deployments_registry import ...`) therefore
# evaluates the parent __init__ and fails without these. Install just
# the two minimal runtime extras needed by the init chain.
log "  uv pip install jinja2 pyyaml  (deployment_service __init__ chain extras)"
uv pip install --find-links "$WHEEL_CACHE" jinja2 pyyaml 2>&1 | tail -3
# Use STD args for the wheel-cache step below (deployment-service's
# heavyweight deps shouldn't be cached either).
INSTALL_ARGS=("${INSTALL_ARGS_STD[@]}")

# Upload any newly compiled wheels to GCS for next VM
NEW_WHEELS=$(find "$VENV/lib" -name "*.whl" -newer "$WHEEL_CACHE" 2>/dev/null | wc -l)
if [[ "$NEW_WHEELS" -gt 0 ]] || [[ ! -f "$WHEEL_CACHE/.uploaded" ]]; then
  log "Caching compiled wheels to GCS..."
  # Build wheels for all installed packages (captures compiled C extensions)
  uv pip wheel --wheel-dir "$WHEEL_CACHE" "${INSTALL_ARGS[@]}" -q 2>/dev/null || true
  gsutil -m -q cp "$WHEEL_CACHE"/*.whl "$WHEEL_GCS/" 2>/dev/null || true
  touch "$WHEEL_CACHE/.uploaded"
  log "Wheels cached to $WHEEL_GCS"
fi

python -c 'from unified_api_contracts.sports import LEAGUE_REGISTRY; print(f"UAC OK: {len(LEAGUE_REGISTRY)} leagues")'
# Verify whichever service is installed
python -c 'import market_tick_data_service; print("MTDS OK")' 2>/dev/null || true
python -c 'import instruments_service; print("instruments-service OK")' 2>/dev/null || true
log "Dependencies installed successfully (${#INSTALLED_DIRS[@]} packages)"

# ── 5. Auto-launch task (metadata already read in step 2b) ──
export GCP_PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
export CLOUD_PROVIDER="${CLOUD_PROVIDER:-gcp}"
export CLOUD_MOCK_MODE="${CLOUD_MOCK_MODE:-false}"

if [[ "$VM_PIPELINE_MODE" == "backtest" ]]; then
  # Full L1-L7 pipeline for the category — uses backfill-cluster.sh from
  # deployment-service (uploaded alongside this script).
  BACKFILL_SCRIPT="$WORKSPACE/deployment/scripts/vm/backfill-cluster.sh"
  BACKFILL_ARGS="--cluster ${VM_CATEGORY,,} --start-date $VM_START_DATE --end-date $VM_END_DATE"
  [[ -n "$VM_STRATEGY" ]] && BACKFILL_ARGS="$BACKFILL_ARGS --strategy $VM_STRATEGY"

  if [[ -f "$BACKFILL_SCRIPT" ]]; then
    log "Backtest mode: running full pipeline via backfill-cluster.sh"
    log "  Args: $BACKFILL_ARGS"
    nohup bash "$BACKFILL_SCRIPT" $BACKFILL_ARGS \
      > "$LOGS/backtest-pipeline.log" 2>&1 &
    log "Backtest pipeline launched PID: $!"
  else
    log "WARNING: backfill-cluster.sh not found at $BACKFILL_SCRIPT — falling back to e2e-testing"
    # Try e2e-testing run-full-pipeline.sh as fallback
    E2E_SCRIPT="$WORKSPACE/e2e/scripts/${VM_CATEGORY,,}/run-full-pipeline.sh"
    if [[ -f "$E2E_SCRIPT" ]]; then
      nohup bash "$E2E_SCRIPT" --start-date "$VM_START_DATE" --end-date "$VM_END_DATE" \
        > "$LOGS/backtest-pipeline.log" 2>&1 &
      log "E2E pipeline launched PID: $!"
    else
      log "ERROR: No pipeline script found for category $VM_CATEGORY"
    fi
  fi
  exit 0
fi

# Download the debug-log wrapper (tees stdout+stderr to GCS every 30s so we can
# monitor any VM task from outside even when SSH is broken).
VM_NAME_SELF=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/name" || echo "unknown-vm")
GCS_LOG_DIR="gs://deployment-scripts-central-element-323112/vm-logs/${VM_NAME_SELF}"
GCS_LOG_URI="${GCS_LOG_DIR}/run.log"
TEE_WRAPPER="/tmp/vm-exec-with-gcs-tee.sh"
HEARTBEAT_HELPER="/tmp/deployment_heartbeat.py"
HEARTBEAT_DAEMON="/tmp/heartbeat_daemon.py"
if gsutil -q cp "gs://${CODE_BUCKET}/vm/vm-exec-with-gcs-tee.sh" "$TEE_WRAPPER" 2>/dev/null; then
  chmod +x "$TEE_WRAPPER"
  log "Debug log wrapper downloaded → $TEE_WRAPPER (uploads to $GCS_LOG_URI)"
  # The wrapper looks for deployment_heartbeat.py AND heartbeat_daemon.py in
  # its own directory. The daemon owns the 60s Pub/Sub heartbeat loop + 30s
  # GCS log upload — without it we get no streaming events and no GCS log
  # tail (would have to SSH to the VM to see progress).
  if gsutil -q cp "gs://${CODE_BUCKET}/vm/deployment_heartbeat.py" "$HEARTBEAT_HELPER" 2>/dev/null; then
    log "Deployment heartbeat helper downloaded → $HEARTBEAT_HELPER"
  else
    log "WARNING: deployment_heartbeat.py not found in GCS — heartbeats will be skipped"
  fi
  if gsutil -q cp "gs://${CODE_BUCKET}/vm/heartbeat_daemon.py" "$HEARTBEAT_DAEMON" 2>/dev/null; then
    log "Heartbeat daemon downloaded → $HEARTBEAT_DAEMON"
  else
    log "WARNING: heartbeat_daemon.py not found in GCS — observability disabled (no GCS log, no Pub/Sub events)"
  fi
else
  log "WARNING: vm-exec-with-gcs-tee.sh not found in GCS — falling back to local log only"
  TEE_WRAPPER=""
fi

_launch_with_tee() {
  # $1 = command string to execute (shell-quoted or simple)
  # Redirects via the wrapper if it downloaded; otherwise plain nohup.
  local cmd="$1"
  local fallback_log="${2:-$LOGS/backfill.log}"
  # Export VM_* env vars so the tee wrapper + heartbeat subprocess can read
  # them. Without this the heartbeat registers every VM as category=UNKNOWN
  # task=vm-exec mode=full (the defaults in vm-exec-with-gcs-tee.sh), which
  # is what the 2026-04-19 Playwright audit caught across all 14 VMs.
  export VM_NAME="$VM_NAME_SELF"
  export VM_TASK="${VM_TASK:-}"
  export VM_CATEGORY="${VM_CATEGORY:-UNKNOWN}"
  export VM_MODE="${VM_MODE:-${VM_BACKFILL_MODE:-full}}"
  export VM_START_DATE="${VM_START_DATE:-}"
  export VM_END_DATE="${VM_END_DATE:-}"
  export PYTHON_BIN="$VENV/bin/python"
  if [[ -n "$TEE_WRAPPER" ]]; then
    log "Launching with GCS tee: $cmd"
    log "  VM_NAME=$VM_NAME VM_CATEGORY=$VM_CATEGORY VM_TASK=$VM_TASK VM_MODE=$VM_MODE"
    nohup bash "$TEE_WRAPPER" "$GCS_LOG_URI" bash -c "$cmd" > "$fallback_log" 2>&1 &
  else
    log "Launching plain: $cmd"
    nohup bash -c "$cmd" > "$fallback_log" 2>&1 &
  fi
  log "Task launched PID: $!"
}

if [[ "$VM_TASK" == "canonical-migration" ]]; then
  # Phase 3.4 migration scripts: MIGRATION_CMD metadata carries the full
  # command (e.g. "python -m market_tick_data_service.scripts.migrate_defi_canonical ...").
  VM_MIGRATION_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_MIGRATION_CMD" || echo "")
  if [[ -n "$VM_MIGRATION_CMD" ]]; then
    FULL_CMD="${VM_MIGRATION_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/mtds" || { log "ERROR: $WORKSPACE/mtds missing"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/canonical-migration.log"
  else
    log "ERROR: canonical-migration task without VM_MIGRATION_CMD metadata"
  fi
elif [[ "$VM_TASK" == "sports-manifest-rescan" ]]; then
  # Phase 5 (2026-04-21) — SPORTS FIXTURES per-(date, canonical_league_id)
  # manifest rescan. Runs instruments-service/scripts/rescan_sports_fixtures_canonical.py
  # to close the EPL=5 / BRASILEIRAO=2 undercount by joining af_league_id to
  # canonical league_id via UAC get_league_by_api_football_id. VM_MIGRATION_CMD
  # metadata carries the full invocation so the launcher controls flags
  # (--date / --workers / --dry-run) from the host side.
  VM_MIGRATION_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_MIGRATION_CMD" || echo "")
  if [[ -n "$VM_MIGRATION_CMD" ]]; then
    FULL_CMD="${VM_MIGRATION_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/instruments" || { log "ERROR: $WORKSPACE/instruments missing"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/sports-manifest-rescan.log"
  else
    log "ERROR: sports-manifest-rescan task without VM_MIGRATION_CMD metadata"
  fi
elif [[ "$VM_TASK" == "sports-scheduler-poll" ]]; then
  # Plan 3 (sports_scheduler_cron_activation_2026_04_21) — VM-based activation.
  # Runs the sports fixture-aware trigger scheduler as a long-lived daemon.
  #
  # Why VM not Cloud Run Job: Plans 12 + 13 blockers on deployment-service
  # image build + UTL base image. VM path uses the tarball infra (UAC + UTL +
  # deployment-service already on GCS) and runs SportsTriggerScheduler.run()
  # directly — its built-in 300-s poll loop IS the daemon.
  #
  # Config path: deployment-service tarball extracts to $WORKSPACE/deployment,
  # and configs/sports-trigger-tiers.yaml is inside that tree. Run from
  # $WORKSPACE/deployment so the default `configs/sports-trigger-tiers.yaml`
  # resolves (the CLI also tries __file__.parent.parent fallback).
  SCHEDULER_DIR="$WORKSPACE/deployment"
  if [[ ! -d "$SCHEDULER_DIR" ]]; then
    log "ERROR: $SCHEDULER_DIR missing — deployment-service tarball not extracted"
    exit 1
  fi
  cd "$SCHEDULER_DIR" || { log "ERROR: cd $SCHEDULER_DIR failed"; exit 1; }

  # The two-pass install above runs deployment-service with --no-deps (to avoid
  # pulling in FastAPI / functions-framework on heartbeat-only VMs). The
  # scheduler CLI is click-based and needs `click` explicitly + the Cloud
  # Run / Compute SDKs used by SchedulerDispatchAdapter to dispatch child
  # VMs. Install them here so sports-scheduler-poll task has a working CLI.
  log "Installing deployment-service scheduler CLI extras (click + gcloud SDKs)..."
  uv pip install --find-links "$WHEEL_CACHE" click google-cloud-run google-cloud-compute 2>&1 | tail -3

  VM_SCHEDULER_DRY_RUN=$(_meta VM_SCHEDULER_DRY_RUN)
  SCHED_ARGS="--config configs/sports-trigger-tiers.yaml --poll-interval 300"
  if [[ "$VM_SCHEDULER_DRY_RUN" == "true" ]]; then
    SCHED_ARGS="$SCHED_ARGS --dry-run"
    log "Sports scheduler DRY-RUN mode — dispatches will be logged, not fired"
  fi
  # Launch the scheduler via the tee wrapper so run.log streams to GCS every
  # 30s AND the scheduler's stdout/stderr lands in $LOGS/sports-scheduler.log
  # for SSH-tail debugging. No VM_SHUTDOWN_ON_COMPLETION — this is a daemon.
  _launch_with_tee \
    "$VENV/bin/python -m deployment_service sports-trigger run $SCHED_ARGS" \
    "$LOGS/sports-scheduler.log"
elif [[ "$VM_TASK" == "mdps-backfill" || "$VM_TASK" == "features-backfill" ]]; then
  # Phase 5b/5c backfill: BACKFILL_CMD metadata carries the full command
  # (e.g. "python -m market_data_processing_service process --start-date X --end-date Y --cefi").
  VM_BACKFILL_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$VM_BACKFILL_CMD" ]]; then
    FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
    _launch_with_tee "$FULL_CMD" "$LOGS/${VM_TASK}.log"
  else
    log "ERROR: ${VM_TASK} task without VM_BACKFILL_CMD metadata"
  fi
elif [ -n "$VM_TASK" ]; then
  _OP="$VM_OPERATION"
  if [[ "$VM_SERVICE" == "instruments_service" && "$_OP" == "download" ]]; then
    _OP="instruments"
  fi
  CLI_ARGS="--operation $_OP --mode batch --category $VM_CATEGORY"
  [[ -n "$VM_VENUE" ]] && CLI_ARGS="$CLI_ARGS --venues $VM_VENUE"
  [[ -n "$VM_START_DATE" ]] && CLI_ARGS="$CLI_ARGS --start-date $VM_START_DATE"
  [[ -n "$VM_END_DATE" ]] && CLI_ARGS="$CLI_ARGS --end-date $VM_END_DATE"
  [[ -n "$VM_LOOKBACK_DAYS" ]] && CLI_ARGS="$CLI_ARGS --lookback-days $VM_LOOKBACK_DAYS"
  [[ -n "$VM_LOOKAHEAD_DAYS" ]] && CLI_ARGS="$CLI_ARGS --lookahead-days $VM_LOOKAHEAD_DAYS"
  [[ "$VM_FORCE_WINDOW" == "true" ]] && CLI_ARGS="$CLI_ARGS --force-window"
  [[ -n "$VM_SPORTS_PROVIDER" ]] && CLI_ARGS="$CLI_ARGS --sports-provider $VM_SPORTS_PROVIDER"
  [[ -n "$VM_SPORTS_ENTITY" ]] && CLI_ARGS="$CLI_ARGS --sports-entity $VM_SPORTS_ENTITY"
  [[ -n "$VM_STRATEGY" ]] && CLI_ARGS="$CLI_ARGS --strategy $VM_STRATEGY"
  # CLI expects nargs='+' → space-separated. Metadata values arrive with
  # semicolons (see VM_INSTRUMENT_IDS comment above) to avoid collision with
  # gcloud's comma key-separator. Transform semicolons → spaces. VM_DATA_TYPES
  # historically used commas but we harmonise both on ; going forward; the
  # //,/ fallback keeps older launchers working.
  [[ -n "$VM_DATA_TYPES" ]] && CLI_ARGS="$CLI_ARGS --data-types ${VM_DATA_TYPES//[,;]/ }"
  [[ -n "$VM_INSTRUMENT_IDS" ]] && CLI_ARGS="$CLI_ARGS --instrument-ids ${VM_INSTRUMENT_IDS//[,;]/ }"
  _launch_with_tee "$VENV/bin/python -m $VM_SERVICE $CLI_ARGS" "$LOGS/backfill.log"
else
  log "No VM_TASK metadata — setup complete, ready for manual launch"
fi

log "=== VM setup complete ==="
echo "READY" > /tmp/vm_ready
