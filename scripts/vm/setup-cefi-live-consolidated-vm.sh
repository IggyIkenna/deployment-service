#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
#
# Startup script for the consolidated CeFi live-capture VM.
# Launches N MVP CeFi websocket-streaming shards as separate background processes
# on a single host.
#
# Designed for: e2-highmem-16 (16 vCPU, 128 GB RAM) or smaller
# Triggered by: launch-mtds-live-cefi-consolidated.sh
#
# MVP CeFi shards (per is_in_mvp_capture_universe SSOT, operator 2026-06-27):
#   BINANCE-FUTURES: trades, book_snapshot_5
#   BYBIT-FUTURES:   trades, book_snapshot_5, derivative_ticker
#   HYPERLIQUID:     trades, book_snapshot_5, derivative_ticker
#   KRAKEN-FUTURES:  trades, book_snapshot_5, derivative_ticker
#   OKX-FUTURES:     trades, book_snapshot_5, derivative_ticker
#   DERIBIT:         derivative_ticker (options_chain live WS is a future Phase 3.5 item)
#   ASTER:           book_snapshot_5, liquidations (live-only feeds, operator ruling 2026-07-28 —
#                    folded into this consolidated launch; see /codex/04-architecture/
#                    instruments-service-as-ssot-for-mtds.md-adjacent connector aster_book_liq_ws.py)
#
# Each shard launches its own "python -m market_tick_data_service
#   --operation websocket-streaming --mode live --asset-group CEFI
#   --shard-spec cefi:VENUE:data_type --mvp-mode" in the background, writing to
# logs/live-{venue}-{data_type}.log. A supervisor loop checks PIDs every 60s
# and restarts any dead shard.
#
# RSS monitoring: logged every 60s alongside the supervisor loop so the operator
# can assess OOM risk from the GCS-teed run.log.
set -euo pipefail

LOG="/var/log/vm-setup.log"
WORKSPACE="/home/ikennaigboaka/workspace"
VENV="/home/ikennaigboaka/venv"
LOGS="/home/ikennaigboaka/logs"
CODE_BUCKET="${CODE_BUCKET:-deployment-scripts-central-element-323112}"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG"; }

# ── EXIT trap: upload setup log on early failure ──
_on_exit() {
  local rc=$?
  [[ $rc -eq 0 ]] && return 0
  local vm_name vm_zone
  vm_name=$(curl -sf -H 'Metadata-Flavor: Google' \
    'http://metadata.google.internal/computeMetadata/v1/instance/name' 2>/dev/null || echo '')
  vm_zone=$(curl -sf -H 'Metadata-Flavor: Google' \
    'http://metadata.google.internal/computeMetadata/v1/instance/zone' 2>/dev/null | awk -F/ '{print $NF}' || echo '')
  [[ -n "$vm_name" ]] && gsutil -q cp "$LOG" "gs://${CODE_BUCKET}/vm-logs/${vm_name}/vm-setup.log" 2>/dev/null || true
  log "SETUP FAILED rc=$rc (see vm-setup.log)"
}
trap _on_exit EXIT

# ── 0. File descriptor limit ──
ulimit -n 65536 || log "WARNING: ulimit -n 65536 failed (current: $(ulimit -n))"
mkdir -p /etc/systemd/system.conf.d/
cat > /etc/systemd/system.conf.d/file-descriptors.conf <<'EOF'
[Manager]
DefaultLimitNOFILE=65536
EOF
systemctl daemon-reexec 2>/dev/null || true
log "File descriptor limit: $(ulimit -n)"

# ── 1. System packages + Python 3.13 via uv ──
log "Installing system packages..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq build-essential git curl redis-server

log "Installing uv..."
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:$PATH"
log "uv: $(uv --version)"

log "Installing Python 3.13 via uv..."
uv python install 3.13
PY313=$(uv python find 3.13)
log "Python 3.13: $($PY313 --version) at $PY313"

# ── 2. Create venv ──
log "Creating venv with Python 3.13..."
rm -rf "$VENV"
"$PY313" -m venv "$VENV"
source "$VENV/bin/activate"
log "Venv Python: $(python --version)"

# ── 2b. Read VM metadata ──
_meta() { curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1" 2>/dev/null || echo "${2:-}"; }
VM_NAME_SELF=$(_meta VM_NAME "$(curl -sf -H 'Metadata-Flavor: Google' 'http://metadata.google.internal/computeMetadata/v1/instance/name' 2>/dev/null || echo unknown)")
DEPLOYMENT_ENV=$(_meta DEPLOYMENT_ENV prod)
log "VM name: $VM_NAME_SELF  env: $DEPLOYMENT_ENV"

# ── 3. Deploy code from GCS tarballs ──
log "Deploying code from GCS..."
mkdir -p "$WORKSPACE/uac" "$WORKSPACE/utl" "$WORKSPACE/mtds" "$LOGS"

_download_tarball() {
  local tarball="$1"
  local dest_dir="$2"
  log "Downloading ${tarball}.tar.gz → $WORKSPACE/$dest_dir ..."
  gsutil -q cp "gs://${CODE_BUCKET}/code/${tarball}.tar.gz" "/tmp/${tarball}.tar.gz"
  tar -xzf "/tmp/${tarball}.tar.gz" -C "$WORKSPACE/$dest_dir"
  rm -f "/tmp/${tarball}.tar.gz"
  log "  Extracted ${tarball}"
}

_download_tarball "unified-api-contracts-code" "uac"
_download_tarball "unified-trading-library-code" "utl"
_download_tarball "mtds-code" "mtds"

# ── 4. Install deps ──
# Use --no-sources (like setup-data-pipeline-vm.sh) — tarballs have no .git;
# SETUPTOOLS_SCM_PRETEND_VERSION prevents hatch-vcs from failing on missing .git history.
log "Installing Python dependencies..."
export SETUPTOOLS_SCM_PRETEND_VERSION="0.99.0"
uv pip install --no-sources -e "$WORKSPACE/uac" -e "$WORKSPACE/utl" -e "$WORKSPACE/mtds" 2>&1 | tail -5
python -c 'import market_tick_data_service; print("MTDS OK")'
log "Dependencies installed."

# ── 5. Start Redis ──
log "Starting Redis..."
systemctl start redis-server 2>/dev/null || service redis-server start 2>/dev/null || true
export MTDS_STREAMING_REDIS_URL="redis://127.0.0.1:6379"
log "Redis started: $MTDS_STREAMING_REDIS_URL"

# ── 5a. GCS heartbeat sidecar ──
HEARTBEAT_SIDECAR="/tmp/vm_heartbeat_sidecar.sh"
if gsutil -q cp "gs://${CODE_BUCKET}/vm/vm_heartbeat_sidecar.sh" "$HEARTBEAT_SIDECAR" 2>/dev/null; then
  chmod +x "$HEARTBEAT_SIDECAR"
  nohup bash "$HEARTBEAT_SIDECAR" "$VM_NAME_SELF" >/dev/null 2>&1 &
  log "GCS heartbeat sidecar started"
fi

# ── 6. MVP CeFi shard definitions ──
# Format: "VENUE:DATA_TYPE"  — one entry per process to launch.
# MVP scope (is_in_mvp_capture_universe SSOT, operator 2026-06-27):
#   BINANCE-FUTURES: trades + book_snapshot_5 (no derivative_ticker — not in MVP for this venue per operator)
#   BYBIT-FUTURES:   trades + book_snapshot_5 + derivative_ticker
#   HYPERLIQUID:     trades + book_snapshot_5 + derivative_ticker
#   KRAKEN-FUTURES:  trades + book_snapshot_5 + derivative_ticker
#   OKX-FUTURES:     trades + book_snapshot_5 + derivative_ticker
#   DERIBIT:         derivative_ticker ONLY (options_chain WS not yet wired; trades/book5 NOT in MVP)
#   ASTER:           book_snapshot_5 + liquidations ONLY (operator ruling 2026-07-28, folded into this
#     consolidated launch per infra_capture_and_devops_leftovers_2026_07_06.md's BLK-26ed6571 mandate —
#     both are LIVE-ONLY data_types per aster_book_liq_ws.py; NOT trades, which stays batch-captured;
#     book_snapshot_5 already has a UAC BATCH capability entry (start_date 2026-06-23), liquidations is
#     deliberately live-only per the 2026-07-15 "live-only feeds must NOT seed the batch denominator" ruling)
MVP_SHARDS=(
  "BINANCE-FUTURES:trades"
  "BINANCE-FUTURES:book_snapshot_5"
  "BYBIT-FUTURES:trades"
  "BYBIT-FUTURES:book_snapshot_5"
  "BYBIT-FUTURES:derivative_ticker"
  "HYPERLIQUID:trades"
  "HYPERLIQUID:book_snapshot_5"
  "HYPERLIQUID:derivative_ticker"
  "KRAKEN-FUTURES:trades"
  "KRAKEN-FUTURES:book_snapshot_5"
  "KRAKEN-FUTURES:derivative_ticker"
  "OKX-FUTURES:trades"
  "OKX-FUTURES:book_snapshot_5"
  "OKX-FUTURES:derivative_ticker"
  "DERIBIT:derivative_ticker"
  "ASTER:book_snapshot_5"
  "ASTER:liquidations"
)

# ── 7. Export shared env for all shard processes ──
export GCP_PROJECT_ID="central-element-323112"
export CLOUD_PROVIDER="gcp"
export CLOUD_MOCK_MODE="false"
export MANIFEST_PER_VM_SHARDS="true"
export DEPLOYMENT_ENV="$DEPLOYMENT_ENV"
export MTDS_STREAMING_REDIS_URL="redis://127.0.0.1:6379"
export VM_NAME="$VM_NAME_SELF"
export PYTHONPATH="${WORKSPACE}/uac:${WORKSPACE}/utl:${WORKSPACE}/mtds:${PYTHONPATH:-}"

mkdir -p "$LOGS"

# ── 8. Launch shard supervisor ──
# The supervisor script is written to disk so it can run as a long-lived daemon
# (nohup) completely independent of this startup script's lifetime. It:
#   (a) Launches each shard as a background process with its own log
#   (b) Every 60s: logs RSS + checks PIDs, restarts dead shards
#   (c) Writes RSS snapshot to $LOGS/rss-monitor.log every 60s for OOM auditing
SUPERVISOR_SCRIPT="$LOGS/cefi-shard-supervisor.sh"
cat > "$SUPERVISOR_SCRIPT" <<'SUPERVISOR_EOF'
#!/usr/bin/env bash
# Supervisor: launch + monitor MVP CeFi websocket-streaming shards.
# Launched by setup-cefi-live-consolidated-vm.sh at boot.
set -euo pipefail

LOGS="/home/ikennaigboaka/logs"
VENV="/home/ikennaigboaka/venv"
WORKSPACE="/home/ikennaigboaka/workspace"
LOG_FILE="$LOGS/supervisor.log"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') SUPERVISOR $*" | tee -a "$LOG_FILE"; }

# MVP shards — MUST MATCH setup-cefi-live-consolidated-vm.sh §6
# ASTER book_snapshot_5 + liquidations added per operator ruling 2026-07-28 (see §6 comment for the full rationale).
MVP_SHARDS=(
  "BINANCE-FUTURES:trades"
  "BINANCE-FUTURES:book_snapshot_5"
  "BYBIT-FUTURES:trades"
  "BYBIT-FUTURES:book_snapshot_5"
  "BYBIT-FUTURES:derivative_ticker"
  "HYPERLIQUID:trades"
  "HYPERLIQUID:book_snapshot_5"
  "HYPERLIQUID:derivative_ticker"
  "KRAKEN-FUTURES:trades"
  "KRAKEN-FUTURES:book_snapshot_5"
  "KRAKEN-FUTURES:derivative_ticker"
  "OKX-FUTURES:trades"
  "OKX-FUTURES:book_snapshot_5"
  "OKX-FUTURES:derivative_ticker"
  "DERIBIT:derivative_ticker"
  "ASTER:book_snapshot_5"
  "ASTER:liquidations"
)

declare -A SHARD_PIDS=()

_shard_log() {
  local venue="$1" dt="$2"
  echo "$LOGS/live-${venue,,}-${dt//_/-}.log"
}

_launch_shard() {
  local spec="$1"
  local venue="${spec%%:*}"
  local dt="${spec##*:}"
  local shard_spec="cefi:${venue}:${dt}"
  local logfile
  logfile=$(_shard_log "$venue" "$dt")

  log "Launching shard: $shard_spec → $logfile"
  nohup "$VENV/bin/python" -m market_tick_data_service \
    --operation websocket-streaming \
    --mode live \
    --asset-group CEFI \
    --shard-spec "$shard_spec" \
    --mvp-mode \
    >> "$logfile" 2>&1 &
  local pid=$!
  SHARD_PIDS["$spec"]=$pid
  log "  shard $shard_spec started PID=$pid"
}

# Initial launch
log "=== CeFi consolidated supervisor starting: ${#MVP_SHARDS[@]} shards ==="
for shard in "${MVP_SHARDS[@]}"; do
  _launch_shard "$shard"
  sleep 2  # stagger startup to avoid simultaneous IS catalogue fetches
done
log "All shards launched. Entering monitor loop..."

# Monitor + restart loop
while true; do
  sleep 60

  # RSS report
  {
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') RSS_SNAPSHOT"
    ps aux --sort=-%mem | head -20
    echo "--- free -h ---"
    free -h
  } >> "$LOGS/rss-monitor.log" 2>&1

  # Check + restart dead shards
  for shard in "${MVP_SHARDS[@]}"; do
    local_pid="${SHARD_PIDS[$shard]:-0}"
    if [[ "$local_pid" -gt 0 ]] && kill -0 "$local_pid" 2>/dev/null; then
      : # alive
    else
      log "WARN: shard $shard (PID ${local_pid:-?}) dead — restarting"
      _launch_shard "$shard"
    fi
  done

  # Heartbeat to supervisor.log (picked up by fleet monitors)
  log "PIPELINE_HEARTBEAT shards=${#MVP_SHARDS[@]} live=true"
done
SUPERVISOR_EOF
chmod +x "$SUPERVISOR_SCRIPT"

log "Starting shard supervisor..."
nohup bash "$SUPERVISOR_SCRIPT" >> "$LOGS/supervisor.log" 2>&1 &
SUPERVISOR_PID=$!
log "Supervisor started PID=$SUPERVISOR_PID"

# ── 9. Wait for shards to initialise then report status ──
log "Waiting 30s for initial shard connections..."
sleep 30

log "=== SHARD PROCESS STATUS (T+30s) ==="
ps aux | grep "market_tick_data_service" | grep -v grep | tee -a "$LOG" || log "(no MTDS processes found yet)"

log "=== MEMORY STATUS (T+30s) ==="
free -h | tee -a "$LOG"

log "=== LOG FILES ==="
ls -la "$LOGS"/live-*.log 2>/dev/null | tee -a "$LOG" || log "(log files not yet created)"

log "=== VM SETUP COMPLETE ==="
log "Consolidated CeFi live VM running ${#MVP_SHARDS[@]} MVP shards."
log "Supervisor PID=$SUPERVISOR_PID — restarts dead shards every 60s."
log "RSS monitor: $LOGS/rss-monitor.log"
log "Shard logs: $LOGS/live-{venue}-{data_type}.log"
