#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
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

# ── EXIT trap: loud-failure forensics + self-delete on early-bootstrap failure ──
# 2026-05-28 follow-up to the cefi-heavy zombie incident: this script uses
# `set -euo pipefail` so any apt/tarball/pip failure exits non-zero before
# _launch_with_tee fires. Without this trap the VM_SHUTDOWN_ON_COMPLETION
# wiring in vm-exec-with-gcs-tee.sh never runs (the wrapper is never launched),
# so the VM sits RUNNING until the zombie-watchdog reaps it. When the watchdog
# is also broken (2026-05-24 → 28 window), VMs accumulate as zombies.
#
# 2026-07-27 fix (mdps_features_live_launcher_shared_venv_dependency_conflict_2026_07_26.md):
# both the forensics upload AND the self-delete used to be gated behind
# VM_SHUTDOWN_ON_COMPLETION=true, so every long-running "live" launcher
# (VM_SHUTDOWN_ON_COMPLETION=false by design, e.g. launch-mdps-features-live.sh) got
# ZERO signal on a bootstrap failure — no log, no marker, no self-delete — the VM just
# sat RUNNING indefinitely billing with no process (confirmed: a `uv pip install`
# conflict left a VM silently stalled for 2.5h). Worse, the zombie VM also blocked any
# retry: every launcher's singleton-lock check refuses to launch a same-prefix VM while
# one is RUNNING. This trap only ever fires BEFORE the real task launches (disarmed via
# `trap - EXIT` at the bottom of _launch_with_tee once the wrapped process is actually
# running — see below), so VM_SHUTDOWN_ON_COMPLETION=false's real intent ("don't delete
# a successfully-launched live consumer when it later exits/restarts") never applies
# here — a bootstrap that never reached launch has nothing worth preserving. Fixed:
# forensics upload + self-delete both now run UNCONDITIONALLY on any bootstrap failure,
# regardless of VM_SHUTDOWN_ON_COMPLETION. Also writes the SAME rc to the canonical
# `EXIT_STATUS` blob (_gcs.EXIT_STATUS_BLOB) that exit_code_fleet_monitor.py's
# read_terminal_exit_code() already polls for terminated VMs — once this VM
# self-deletes, the existing DP_VM_EXIT_NONZERO alerting path picks it up for free,
# same as any other launcher's task-crash, instead of needing a new monitor.
# Runs detached so SIGHUP/SIGTERM on VM teardown can't interrupt the upload or delete.
_self_delete_on_setup_failure() {
    local rc=$?
    [[ $rc -eq 0 ]] && return 0
    local vm_name vm_zone
    vm_name=$(curl -sf -H 'Metadata-Flavor: Google' \
        'http://metadata.google.internal/computeMetadata/v1/instance/name' 2>/dev/null || echo '')
    vm_zone=$(curl -sf -H 'Metadata-Flavor: Google' \
        'http://metadata.google.internal/computeMetadata/v1/instance/zone' 2>/dev/null | awk -F/ '{print $NF}')
    [[ -n "$vm_name" && -n "$vm_zone" ]] || return 0
    log "SETUP FAILED rc=$rc — uploading log + EXIT_STATUS, scheduling self-delete" || true
    gsutil -q cp "$LOG" "gs://${CODE_BUCKET}/vm-logs/${vm_name}/vm-setup.log" 2>/dev/null || true
    echo "$rc" | gsutil -q cp - "gs://${CODE_BUCKET}/vm-logs/${vm_name}/SETUP_EXIT_STATUS" 2>/dev/null || true
    echo "$rc" | gsutil -q cp - "gs://${CODE_BUCKET}/vm-logs/${vm_name}/EXIT_STATUS" 2>/dev/null || true
    nohup setsid bash -c "
        sleep 10
        gcloud compute instances delete '$vm_name' --zone='$vm_zone' --quiet --delete-disks=all \
            || sudo shutdown -h now
    " </dev/null >/dev/null 2>&1 &
}
trap _self_delete_on_setup_failure EXIT

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

# ── 0b. SPOT-preemption signal (FLEET-WIDE seam) ──
# Writes gs://<CODE_BUCKET>/vm-logs/<vm>/PREEMPTED when — and ONLY when — the
# metadata server reports instance/preempted=true. That blob is the AUTHORITATIVE
# trigger for the whole preemption auto-recovery chain, which is otherwise inert:
#   _gcs.is_vm_preempted -> classify_terminated_vm(preempted=True) -> PREEMPTED
#   -> DP_VM_PREEMPTED (AUTO_RECOVER) -> escalation._recover_preempted_vm
#   -> RelaunchPreemptedVm (replays LAUNCH_PARAMS.json, resuming START_DATE from
#      the monotonic PROGRESS.json checkpoint the tee-wrapper writes).
# SSOT: codex/05-infrastructure/spot-vms-for-backfill.md § "Preemption recovery
# MUST resume from PROGRESS, never replay START_DATE".
#
# WHY HERE and not per-launcher (2026-07-20): that codex section claims the signal
# is "wired fleet-wide via scripts/vm/lib/launcher_common.sh", but
# lc_write_preemption_signal_file was only ever CALLED by launch-cefi-sharded-backfill.sh
# (+8 launchers with an inline copy). MEASURED: 57 launchers pass
# --provisioning-model=SPOT and 47 of them wrote no PREEMPTED blob at all, so a
# preempted VM was classified EXIT_NONZERO/GONE_NO_CAPTURE and PAGED a human
# instead of auto-relaunching. All 47 share THIS script as their startup-script-url,
# so installing the signal here fixes every one of them — and every future launcher —
# with no per-launcher edit. Same shared-seam reasoning that made the PROGRESS.json
# writer fleet-wide via vm-exec-with-gcs-tee.sh.
#
# This is the SAME contract, not a second mechanism: identical blob path, identical
# "preempted" payload, identical preempted=true gate as
# launcher_common.sh::lc_write_preemption_signal_file. A launcher that ALSO attaches
# its own metadata shutdown-script keeps working — both write the same object with
# the same content, so the two are idempotent, not competing. (gcloud accepts only
# ONE metadata shutdown-script per instance, which is exactly why this seam is a
# systemd unit instead: it composes instead of colliding.)
#
# Unit shape mirrors Google's own google-shutdown-scripts.service (ExecStart=/bin/true
# + RemainAfterExit + ExecStop), so it runs during the shutdown transaction while the
# network is still up — inside GCE's ~30s preemption notice.
log "Installing SPOT-preemption signal shutdown unit..."
cat > /usr/local/sbin/uts-preemption-signal.sh <<'PREEMPT_SIGNAL_EOF'
#!/usr/bin/env bash
# Emit the durable PREEMPTED marker iff this shutdown IS a SPOT reclaim.
# Best-effort by construction: never block or fail a shutdown.
#
# Bounded retry (2026-07-21, exit_code_fleet_monitor_clean_misclassifies_premature_kill
# item 2): --instance-termination-action=DELETE does not shorten the guest's preemption
# window vs STOP (GCE runs the same ACPI-soft-off shutdown sequence before either
# outcome; DELETE only changes what happens to the instance AFTER the guest has already
# shut down) — confirmed against GCE preemption-handling docs, so TimeoutStopSec=25 is
# not structurally raced by the termination action. The real remaining risk is a single
# flaky `gcloud storage cp` (DNS hiccup, transient 5xx) silently dropping the marker with
# no retry. Every network call below is now `--max-time`-capped and the write itself
# retries once, so the worst case (3 metadata curls @2s + 2 write attempts @7s) is ~20s
# — comfortably inside the 25s budget with slack for script/systemd overhead.
PREEMPTED=$(curl -sf --max-time 2 -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/preempted' 2>/dev/null || echo 'false')
[[ "$PREEMPTED" == "true" ]] || exit 0
VM_NAME=$(curl -sf --max-time 2 -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/name' 2>/dev/null || echo "")
PROJECT=$(curl -sf --max-time 2 -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/project/project-id' 2>/dev/null || echo "")
[[ -n "$VM_NAME" && -n "$PROJECT" ]] || exit 0
MARKER_URI="gs://deployment-scripts-${PROJECT}/vm-logs/${VM_NAME}/PREEMPTED"
if echo "preempted" | timeout 7 gcloud storage cp - "$MARKER_URI" --quiet 2>/dev/null; then
  echo "[preemption-shutdown] wrote PREEMPTED signal for ${VM_NAME}" >&2
elif echo "preempted" | timeout 7 gcloud storage cp - "$MARKER_URI" --quiet 2>/dev/null; then
  echo "[preemption-shutdown] wrote PREEMPTED signal for ${VM_NAME} (retry succeeded)" >&2
else
  echo "[preemption-shutdown] FAILED to write PREEMPTED signal for ${VM_NAME} after 2 attempts — will fall through to PARTIAL_UNCONFIRMED, not silent CLEAN" >&2
fi
PREEMPT_SIGNAL_EOF
chmod +x /usr/local/sbin/uts-preemption-signal.sh
cat > /etc/systemd/system/uts-preemption-signal.service <<'PREEMPT_UNIT_EOF'
[Unit]
Description=UTS SPOT-preemption signal (writes vm-logs/<vm>/PREEMPTED on GCE reclaim)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=true
ExecStart=/bin/true
ExecStop=/usr/local/sbin/uts-preemption-signal.sh
# Bounded: the GCE preemption notice is ~30s. Never hang a shutdown past it.
TimeoutStopSec=25

[Install]
WantedBy=multi-user.target
PREEMPT_UNIT_EOF
# Non-fatal: a VM without this signal degrades to today's behaviour (a preemption
# PAGEs instead of auto-relaunching) — it must never brick setup under `set -e`.
systemctl daemon-reload 2>/dev/null || log "WARNING: systemctl daemon-reload failed (non-fatal)"
if systemctl enable --now uts-preemption-signal.service 2>/dev/null; then
  log "SPOT-preemption signal unit active (preempted VMs will auto-relaunch via RelaunchPreemptedVm)"
else
  log "WARNING: could not enable uts-preemption-signal.service — a preemption will PAGE, not auto-relaunch"
fi

# ── 1. System packages + Python 3.13 via uv (deadsnakes PPA was unreliable) ──
log "Installing system packages..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq \
  build-essential \
  git curl

# Install uv (Astral) — bundles its own Python build infra, no apt-PPA race.
# The deadsnakes PPA was hit-or-miss on launchpad CDN ('Failed to fetch' /
# 'Cannot initiate the connection'), repeatedly bricking VM startup. uv
# downloads Python from python-build-standalone instead — single TCP host
# (github.com), much more reliable on cold-boot networks.
log "Installing uv..."
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:$PATH"
log "uv: $(uv --version)"

log "Installing Python 3.13 via uv..."
uv python install 3.13
PY313=$(uv python find 3.13)
log "Python 3.13: $($PY313 --version) at $PY313"

# ── 2. Create venv with correct Python ──
log "Creating venv with Python 3.13..."
rm -rf "$VENV"
"$PY313" -m venv "$VENV"
source "$VENV/bin/activate"
log "Venv Python: $(python --version) at $(which python)"

# uv is already on PATH from /root/.local/bin (system install above) — no
# need to also pip-install it inside the venv.
log "uv (system, used for venv installs): $(uv --version)"

# ── 2b. Read VM metadata early (needed for selective tarball install) ──
_meta() { curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1" 2>/dev/null || echo "${2:-}"; }
VM_TASK=$(_meta VM_TASK)
VM_VENUE=$(_meta VM_VENUE)
VM_START_DATE=$(_meta VM_START_DATE)
VM_END_DATE=$(_meta VM_END_DATE)
VM_ASSET_GROUP=$(_meta VM_ASSET_GROUP "$(_meta VM_CATEGORY CEFI)")
VM_OPERATION=$(_meta VM_OPERATION download)
VM_SERVICE=$(_meta VM_SERVICE market_tick_data_service)
VM_SPORTS_PROVIDER=$(_meta VM_SPORTS_PROVIDER)
VM_SPORTS_ENTITY=$(_meta VM_SPORTS_ENTITY)
# Registry-driven rate-budget (operator design 2026-06-23): the launcher splits
# the source's fleet req/min ceiling across N concurrent VMs
# (deployment-service launch_budget_registry.allocate_rate_budget →
# per_vm_rpm = source_rpm // N) and stamps the allocated share + matched
# concurrency here. Exported as SPORTS_ADAPTER_RATE_RPM / SPORTS_ADAPTER_CONCURRENCY
# so the typed InstrumentsServiceConfig (pydantic-settings, never os.getenv) reads
# them → the sports adapter runs its self-enforced token-bucket throttle at exactly
# this rate as the PRIMARY control (the 429 backoff stays only as the safety net).
VM_SPORTS_ADAPTER_RATE_RPM=$(_meta SPORTS_ADAPTER_RATE_RPM)
[[ -n "$VM_SPORTS_ADAPTER_RATE_RPM" ]] && export SPORTS_ADAPTER_RATE_RPM="$VM_SPORTS_ADAPTER_RATE_RPM"
VM_SPORTS_ADAPTER_CONCURRENCY=$(_meta SPORTS_ADAPTER_CONCURRENCY)
[[ -n "$VM_SPORTS_ADAPTER_CONCURRENCY" ]] && export SPORTS_ADAPTER_CONCURRENCY="$VM_SPORTS_ADAPTER_CONCURRENCY"
# Rolling forward-poll flags (instruments-service CLI). When present, the VM
# resolves [today-N, today+M] at boot time (UTC) rather than freezing dates at
# launcher-invocation time. SSOT: codex/02-data/sports-scheduling-and-sharding.md §4.
VM_LOOKBACK_DAYS=$(_meta VM_LOOKBACK_DAYS)
VM_LOOKAHEAD_DAYS=$(_meta VM_LOOKAHEAD_DAYS)
VM_FORCE_WINDOW=$(_meta VM_FORCE_WINDOW)
VM_FORCE=$(_meta VM_FORCE)
# Tardis adapter feature flag — when "true", per-symbol downloads use the
# streaming finalize path (chunked HTTP → temp parquet → per-row-group
# canonical write) instead of the legacy full-materialise path. Set in VM
# metadata for memory-bound shards (Coinbase BTC-USD book_snapshot_5).
# Read into env so MTDS Tardis adapter (tardis_adapter.py download_batch)
# picks it up via os.environ.get.
TARDIS_STREAMING_FINALIZE=$(_meta TARDIS_STREAMING_FINALIZE)
[[ -n "$TARDIS_STREAMING_FINALIZE" ]] && export TARDIS_STREAMING_FINALIZE
# Tarball SHA gate — when set, VM boot asserts that every installed tarball's
# manifest.json commit_sha matches this value. Set by launchers that pin a
# specific deployment commit. Raises ManifestShaDriftError on mismatch + exits 1.
TARBALL_EXPECTED_SHA=$(_meta TARBALL_EXPECTED_SHA)
# Per-tarball SHA pins — when set, VM downloads the SHA-pinned tarball from GCS
# instead of the fixed-name one. Prevents race conditions where another agent
# rebuilds the fixed-name tarball from a different commit between tarball build
# and VM launch. Format: full 40-char commit SHA. Key per tarball:
#   UTL_TARBALL_SHA  → unified-trading-library-code@{sha}.tar.gz
#   UAC_TARBALL_SHA  → unified-api-contracts-code@{sha}.tar.gz
#   MDPS_TARBALL_SHA → market-data-processing-service-code@{sha}.tar.gz
UTL_TARBALL_SHA=$(_meta UTL_TARBALL_SHA)
UAC_TARBALL_SHA=$(_meta UAC_TARBALL_SHA)
MDPS_TARBALL_SHA=$(_meta MDPS_TARBALL_SHA)
# MTDS_TARBALL_SHA → mtds-code@{sha}.tar.gz. (The pin *case* was added in 58ee0a9 but the
# metadata read was missing, so the mtds pin never engaged — completed here.)
MTDS_TARBALL_SHA=$(_meta MTDS_TARBALL_SHA)
# Tardis pyarrow-CSV block size in MiB. Default 8 MiB (lives in MTDS
# tardis_stream_processor._resolve_block_size_bytes); set 1-2 for 16 GB VMs
# running heavy Coinbase BTC-USD book_snapshot_5 days, higher for fatter VMs
# that prefer fewer parquet row-groups (slightly better compression).
TARDIS_STREAM_BLOCK_SIZE_MB=$(_meta TARDIS_STREAM_BLOCK_SIZE_MB)
[[ -n "$TARDIS_STREAM_BLOCK_SIZE_MB" ]] && export TARDIS_STREAM_BLOCK_SIZE_MB
# Tardis single-concurrent-IP lease (option (a) stopgap, DEFAULT-OFF —
# tardis_concurrent_ip_lockout_2026_07_12). When TARDIS_CONCURRENCY_LEASE=1 AND a
# control bucket is set, the MTDS Tardis process acquires a workspace-wide GCS TTL
# lease before any keyed datasets.tardis.dev call so only ONE Tardis-calling VM
# runs at a time (serialises waves — see tardis_concurrency_lease.py). Only export
# when non-empty (empty-string breaks Pydantic bool/str parsing, same as IS_TEST_RUN).
TARDIS_CONCURRENCY_LEASE=$(_meta TARDIS_CONCURRENCY_LEASE)
[[ -n "$TARDIS_CONCURRENCY_LEASE" ]] && export TARDIS_CONCURRENCY_LEASE
TARDIS_CONCURRENCY_LEASE_BUCKET=$(_meta TARDIS_CONCURRENCY_LEASE_BUCKET)
[[ -n "$TARDIS_CONCURRENCY_LEASE_BUCKET" ]] && export TARDIS_CONCURRENCY_LEASE_BUCKET
# Databento concurrency knobs (opt-in, DEFAULT-OFF) — read by MTDS
# market_interface/config.py (DatabentoClientConfig validation_alias). Only
# export when the metadata value is non-empty so an unset value falls through
# to the config's own default rather than exporting an empty string.
DATABENTO_MAX_CONCURRENT_REQUESTS=$(_meta DATABENTO_MAX_CONCURRENT_REQUESTS)
[[ -n "$DATABENTO_MAX_CONCURRENT_REQUESTS" ]] && export DATABENTO_MAX_CONCURRENT_REQUESTS
DATABENTO_RATE_LIMIT_TARGET_UTILIZATION=$(_meta DATABENTO_RATE_LIMIT_TARGET_UTILIZATION)
[[ -n "$DATABENTO_RATE_LIMIT_TARGET_UTILIZATION" ]] && export DATABENTO_RATE_LIMIT_TARGET_UTILIZATION
VM_STRATEGY=$(_meta VM_STRATEGY)
VM_PIPELINE_MODE=$(_meta VM_PIPELINE_MODE)
VM_DATA_TYPES=$(_meta VM_DATA_TYPES)
# VM_NUM_WORKERS: multi-process-per-VM Tardis downloader (2026-07-19). Default 1 =
# today's single process, byte-identical. >1 fans the CeFi batch download out into N
# processes on THIS one VM, each with a DISJOINT --venues slice and a DISTINCT
# VM_NAME=<vm>-pK manifest shard. cap-1 governs VMs/IPs not processes, and a 2nd
# process was measured opening 24 more Tardis sockets (38 total) — so N processes
# multiply the ~15-socket-per-process fetch ceiling that pins single-process at ~13
# MB/s. SSOT: plans/active/issues/backfill_vm_disk_starvation_misdiagnosed_as_tardis_quota_2026_07_18.md
VM_NUM_WORKERS=$(_meta VM_NUM_WORKERS 1)
# VM_SOURCE: operator free-switch --source (databento|massive) for TradFi OHLCV
# downloads (2026-06-19). The MTDS TickDataHandler REQUIRES --source for a TradFi
# OHLCV run (provenance-ambiguous massive-vs-databento legs) — it selects the
# fetching adapter AND stamps row-level provenance. Without it the run fails every
# payload ("--source databento|massive is REQUIRED") and writes 0 rows. Ignored by
# the CLI for non-tradfi venue-fixed runs. SSOT:
# codex/02-data/tradfi-databento-sourcing-ssot.md.
VM_SOURCE=$(_meta VM_SOURCE)
# VM_LENDING_PROTOCOLS: semicolon-separated protocol allowlist for the CLI's
# --lending-protocols (nargs='+'). Scopes a lending-indices backfill VM to a
# subset of lending_indices_handler.py's protocol dispatch (e.g. a
# single-protocol backfill after wiring a new one in) instead of running the
# full _DEFAULT_PROTOCOLS list. Analogous to VM_SOLANA_PROTOCOLS for
# solana-defi-backfill. SSOT: launch-mtds-lending-indices-backfill-vm.sh.
VM_LENDING_PROTOCOLS=$(_meta VM_LENDING_PROTOCOLS)
# VM_DEX_POOLS_PROTOCOLS / VM_DEX_SWAPS_PROTOCOLS: same allowlist pattern as
# VM_LENDING_PROTOCOLS, for dex_pools_handler.py --dex-pools-protocols /
# dex_swaps_handler.py --dex-swaps-protocols. Scopes a backfill to the
# newly-wired protocols (velodrome_v2/trader_joe_v2/uniswap_v4/uniswap_v2,
# mtds_defi_dex_zero_capture_protocols_2026_07_14) instead of re-running the
# full _DEFAULT_PROTOCOLS list (which would needlessly re-check the 9 already
# fully-captured protocols across the whole backfill window).
VM_DEX_POOLS_PROTOCOLS=$(_meta VM_DEX_POOLS_PROTOCOLS)
VM_DEX_SWAPS_PROTOCOLS=$(_meta VM_DEX_SWAPS_PROTOCOLS)
# VM_DURATION_HOURS: optional run-time cap for services that accept --duration-hours.
# Used by alerting-quietness-baseline (48h quietness run per Phase 7 of alerting plan).
VM_DURATION_HOURS=$(_meta VM_DURATION_HOURS)
# Recovery-mode fixture-id allowlist (instruments-service --recovery-fixture-ids).
# Path to a parquet on GCS or local disk that scopes per-fixture entity fetches
# to a specific af_fixture_id allowlist. Used for targeted recovery work — see
# unified-trading-pm/plans/active/sports_fixtures_truthset_recovery_2026_05_06.md.
VM_RECOVERY_FIXTURE_IDS=$(_meta VM_RECOVERY_FIXTURE_IDS)
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
# VM_GAS_FEE_CHAINS / VM_GAS_FEE_SAMPLE_INTERVAL: optional gas-fee CLI args used
# in the generic task handler (lines below). Must be pre-initialised here so
# set -u does not fire when the metadata key is absent on non-gas-fee VMs.
VM_GAS_FEE_CHAINS=$(_meta VM_GAS_FEE_CHAINS)
VM_GAS_FEE_SAMPLE_INTERVAL=$(_meta VM_GAS_FEE_SAMPLE_INTERVAL)
# VM_MAX_DURATION_SECONDS: bounds a live_websocket smoke-check run (launch-mtds-live.sh
# --test-run --max-duration-seconds N); pre-initialised for the same set -u reason above.
VM_MAX_DURATION_SECONDS=$(_meta VM_MAX_DURATION_SECONDS)
# VM_SHARD_SPEC: live_websocket shard ("asset_group:venue:data_type", e.g. "cefi:HYPERLIQUID:trades").
# VM_MODE_LIVE: explicit mode from live launchers (launch-mtds-live.sh sets VM_MODE=live in metadata).
# Named VM_MODE_LIVE to avoid collision with the VM_MODE export inside _launch_with_tee() at line ~714
# which sources VM_BACKFILL_MODE, not the metadata VM_MODE key.
VM_SHARD_SPEC=$(_meta VM_SHARD_SPEC)
VM_MODE_LIVE=$(_meta VM_MODE)
# VM_LIVE_SOURCE: live websocket source selector ("native" default | "tardis-machine").
# Set by launch-mtds-live.sh --live-source. When "tardis-machine", a local
# tardis-machine Node sidecar (stream-normalized, FREE) is installed + started
# below before the CLI runs, and --live-source tardis-machine is passed to MTDS.
VM_LIVE_SOURCE=$(_meta VM_LIVE_SOURCE)
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
# MANIFEST_ALLOW_STALE_FALLBACK — opt-in escape hatch for the manifest consolidator's
# stale-index guard (unified_trading_library.manifest_writer._read_index), which
# refuses to fall back to a per-VM-shard merge on a stale/missing consolidated index
# (OOM risk on large prod buckets). Test buckets are always small (smoke-test data
# only), so the OOM concern doesn't apply there; pipeline_e2e_check.py's launcher
# invocations set this alongside IS_TEST_RUN so a skip-leg run against a `-test-`
# bucket whose consolidator hasn't run yet still gets a real freshness read instead
# of erroring out. Same empty-string-breaks-Pydantic-bool guard as IS_TEST_RUN above.
MANIFEST_ALLOW_STALE_FALLBACK=$(_meta MANIFEST_ALLOW_STALE_FALLBACK)
if [[ -n "$MANIFEST_ALLOW_STALE_FALLBACK" ]]; then
  export MANIFEST_ALLOW_STALE_FALLBACK
fi
# SHARD_INDEX (Part 4 — TheGraph key-pool sharding). The DeFi subgraph launchers
# stamp SHARD_INDEX so each VM starts on a distinct key in the 9-key thegraph pool
# (mtds market_interface config reads SHARD_INDEX via AliasChoices; key_number =
# SHARD_INDEX % 9 + 1, then the handler round-robins the full pool per request).
# Only export if non-empty (empty-string would break Pydantic int parsing).
SHARD_INDEX=$(_meta SHARD_INDEX)
if [[ -n "$SHARD_INDEX" ]]; then
  export SHARD_INDEX
fi
# DEPLOYMENT_ENV (env-tier for bucket-resolution per bucket_name_ssot Phase 0f,
# 2026-05-11). Every Phase-0f launcher propagates this via
# --metadata=DEPLOYMENT_ENV=<prod|staging|dev>. Default prod when absent so
# legacy launchers that haven't been migrated still target the prod-tier
# buckets without surprises. Export BEFORE any downstream env-aware code path
# fires (bucket-resolution, manifest writes, GCS-tee, heartbeat).
DEPLOYMENT_ENV=$(_meta DEPLOYMENT_ENV prod)
export DEPLOYMENT_ENV
# UTL service_runtime.py reads ENVIRONMENT (not DEPLOYMENT_ENV); keep in sync.
export ENVIRONMENT="$DEPLOYMENT_ENV"
case "$DEPLOYMENT_ENV" in
  prod)    DEPLOYMENT_ENV_SHORT="prd" ;;
  staging) DEPLOYMENT_ENV_SHORT="stg" ;;
  *)       DEPLOYMENT_ENV_SHORT="$DEPLOYMENT_ENV" ;;
esac
export DEPLOYMENT_ENV_SHORT
# DEPLOYMENT_REGISTRY_FIRESTORE_DUALWRITE (P1 registry-migration flag,
# unified_trading_library/config_interface/cloud_config.py — read by
# UnifiedCloudConfig via pydantic AliasChoices, so any process env var of this
# exact name is picked up automatically). Mirrors the DEPLOYMENT_ENV plumbing
# above: launchers pass it via --metadata=DEPLOYMENT_REGISTRY_FIRESTORE_DUALWRITE=true,
# this reads it off GCE metadata and exports it into the process env BEFORE
# the heartbeat daemon (which constructs DeploymentsRegistry) starts. Default
# "false" when absent — matches UTL's own field default, so unmigrated
# launchers keep writing GCS-only until explicitly opted in (see
# plans/active/deployment_registry_firestore_p0_unblock_2026_07_14.md "Link 2").
DEPLOYMENT_REGISTRY_FIRESTORE_DUALWRITE=$(_meta DEPLOYMENT_REGISTRY_FIRESTORE_DUALWRITE false)
export DEPLOYMENT_REGISTRY_FIRESTORE_DUALWRITE
# Stall-watchdog timeout override. Sports MDPS processes long empty-date
# stretches (no betting events → no log output) that would falsely trigger
# the default 1800s threshold and SIGKILL the process before the manifest
# shard is flushed to GCS. Launchers set STALL_TIMEOUT_SEC in metadata to
# raise the threshold for asset_groups where empty-date gaps are expected.
STALL_TIMEOUT_SEC=$(_meta STALL_TIMEOUT_SEC)
[[ -n "$STALL_TIMEOUT_SEC" ]] && export STALL_TIMEOUT_SEC
# Per-shard PROGRESS-marker watchdog (backfill_vm_silent_worker_stall_watchdog P1). When a
# launcher sets STALL_PROGRESS_REGEX, vm-exec-with-gcs-tee.sh resets the stall timer ONLY on a
# NEW line matching it (a real progress marker — a date advanced / a shard written), instead of
# on any raw log growth. This catches a worker HUNG on a network call while the log still emits
# noise (the silent-stall blind spot the blunt STALL_TIMEOUT_SEC override could not close: a
# raised threshold lets a genuine hang idle for hours). Unset ⇒ size-based behavior (unchanged).
STALL_PROGRESS_REGEX=$(_meta STALL_PROGRESS_REGEX)
[[ -n "$STALL_PROGRESS_REGEX" ]] && export STALL_PROGRESS_REGEX
# Manifest consolidated staleness threshold override. CeFi MTDS bucket has
# 34M+ rows (mostly Deribit options) and 1700+ per-VM shards — loading all
# shards at startup OOM-kills the VM on any machine size. Setting this to
# 86400s (24h) makes the ManifestReader use the consolidated availability_index
# even when it is hours old, instead of falling back to per-VM shard merge.
# Launchers pass MANIFEST_CONSOLIDATED_STALENESS_SEC=86400 for large buckets.
MANIFEST_CONSOLIDATED_STALENESS_SEC=$(_meta MANIFEST_CONSOLIDATED_STALENESS_SEC)
[[ -n "$MANIFEST_CONSOLIDATED_STALENESS_SEC" ]] && export MANIFEST_CONSOLIDATED_STALENESS_SEC
# Pair with the shell-level OOM preflight in section 5b. When opt-in, UTL
# read_availability_index raises ManifestConsolidatorStaleError on stale-fallback
# instead of OOM-killing at the per-VM shard merge (2026-05-28).
MANIFEST_FAIL_ON_STALE_FALLBACK=$(_meta MANIFEST_FAIL_ON_STALE_FALLBACK)
[[ -n "$MANIFEST_FAIL_ON_STALE_FALLBACK" ]] && export MANIFEST_FAIL_ON_STALE_FALLBACK
# TARDIS_FREE_ONLY: when 1, TickDataHandler skips paid-tier dates (non-1st-of-month
# outside the rolling 7-day window) for CEFI/TRADFI asset groups — avoids 100%
# CPU spin on 401 responses when the Tardis key is expired.
# Set by launch-cefi-sharded-backfill.sh when FREE_ONLY=1 + key expired.
TARDIS_FREE_ONLY=$(_meta TARDIS_FREE_ONLY)
[[ -n "$TARDIS_FREE_ONLY" ]] && export TARDIS_FREE_ONLY
TARDIS_DERIBIT_BOOK_MAX_CONCURRENT=$(_meta TARDIS_DERIBIT_BOOK_MAX_CONCURRENT)
[[ -n "$TARDIS_DERIBIT_BOOK_MAX_CONCURRENT" ]] && export TARDIS_DERIBIT_BOOK_MAX_CONCURRENT
TARDIS_BOOK_SNAPSHOT_MAX_CONCURRENT=$(_meta TARDIS_BOOK_SNAPSHOT_MAX_CONCURRENT)
[[ -n "$TARDIS_BOOK_SNAPSHOT_MAX_CONCURRENT" ]] && export TARDIS_BOOK_SNAPSHOT_MAX_CONCURRENT
# tardis_concurrent_ip_lockout_2026_07_12 (operator course-correction 2026-07-13):
# one big VM (one egress IP) can safely run several concurrent (non-book_snapshot_5)
# Tardis download streams — the 403 lockout is per-IP, not per-connection (see
# TARDIS_CONCURRENCY_LEASE above). This dials that intra-process concurrency
# (default 32 — see MarketTickDataServiceConfig.tardis_max_concurrent_downloads)
# down for a conservative first smoke wave. Do NOT dial it UP past ~32: measured
# 2026-07-17, Tardis plateaus at ~33 MB/s by 24 streams, so there is no throughput
# left to win, and each extra in-flight download costs a tardis-parse thread holding
# an 8 MiB pyarrow block. Leave unset to inherit the 32/8 default.
TARDIS_MAX_CONCURRENT_DOWNLOADS=$(_meta TARDIS_MAX_CONCURRENT_DOWNLOADS)
[[ -n "$TARDIS_MAX_CONCURRENT_DOWNLOADS" ]] && export TARDIS_MAX_CONCURRENT_DOWNLOADS
# Generic passthrough for ad-hoc `MTDS_*`-prefixed diagnostic env toggles (e.g.
# MTDS_CEFI_INCLUDE_NON_MVP) that don't warrant their own named _meta() read
# above — previously a launcher's --metadata=MTDS_...=value was silently
# dropped unless this script already had a matching hardcoded _meta line
# (cefi_deribit_combo_and_okx_bare_venue_gaps_2026_07_12 gotcha). The GCE
# metadata server lists every custom attribute key (one per line) when queried
# without a specific key name, so auto-export anything in the MTDS_ namespace
# instead of erroring or requiring a script change per new toggle. Named
# VM_*/TARDIS_*/etc. keys above stay explicit constructs (self-documenting,
# easy to grep) — this only covers the ad-hoc MTDS_ namespace.
for _attr_key in $(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/" 2>/dev/null || echo ""); do
  case "$_attr_key" in
    MTDS_*)
      _attr_val=$(_meta "$_attr_key")
      [[ -n "$_attr_val" ]] && export "$_attr_key=$_attr_val"
      ;;
  esac
done
unset _attr_key _attr_val
log "VM metadata: SERVICE=$VM_SERVICE TASK=$VM_TASK ASSET_GROUP=$VM_ASSET_GROUP PROVIDER=$VM_SPORTS_PROVIDER"
log "VM metadata: STRATEGY=$VM_STRATEGY PIPELINE_MODE=$VM_PIPELINE_MODE DEPLOYMENT_ENV=$DEPLOYMENT_ENV"

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
  # features_service is the consolidated post-2026-05-08 module (features_repo_consolidation_2026_05_08.md).
  # Without this mapping the script falls through to "install all" which pulls
  # execution-service + e2e-testing transitive deps and hits the unsatisfiable
  # betfairlightweight/requests resolve. Added 2026-05-16 after attempts 3-5 of
  # features-onchain DeFi VM all failed at uv pip install with conflicting pins.
  ["features_service"]="features-service-code"
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
  ["batch_live_reconciliation_service"]="batch-live-reconciliation-service-code"
  ["alerting_service"]="alerting-service-code"
)
# NOTE: unified-events-interface entry intentionally removed 2026-04-17 —
# UEI was folded into unified-trading-library.events. No repo/pyproject depends
# on it anymore; the stale 0-byte tarball in GCS broke VM setup. Keep it out.
declare -A TARBALL_DIRS=(
  ["unified-api-contracts-code"]="uac"
  ["unified-trading-library-code"]="utl"
  ["mtds-code"]="mtds"
  ["instruments-service-code"]="instruments"
  ["features-service-code"]="features"
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
  ["batch-live-reconciliation-service-code"]="blr"
  ["alerting-service-code"]="alerting"
  # e2e-testing scripts (run-paper.sh / run-live.sh / colocated_engine.py) for
  # strategy paper/live VMs. No editable install (no pyproject.toml Python package
  # to install from e2e-testing root — strategy-service + execution-service packages
  # are installed from their own tarballs and colocated_engine.py imports from those).
  ["e2e-testing-code"]="e2e-testing"
)

# Transitive sibling dependency: MDPS + features-* services declare
# `market-tick-data-service>=0.1.0,<1.0.0` in their pyproject.toml but MTDS
# is not on PyPI, so without MTDS installed as a sibling editable install
# the whole resolve fails with "requirements are unsatisfiable". Two MDPS
# backfill VMs died this way 2026-04-19. Keep this list in lockstep with
# pyproject deps of each downstream service.
# Declared here (before NEEDED_TARBALLS resolution) rather than after it, so the
# compound-VM_SERVICE branch below can also consult it per-component — moved
# 2026-07-26, see issues/mdps_features_live_launcher_shared_venv_dependency_conflict_2026_07_26.md.
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
  "features_service"
)

# Always install core (UAC + UTL + deployment-service) + the service
# tarball for VM_SERVICE.
# UEI was folded into unified-trading-library.events 2026-04-17 — removed from
# here so the stale 0-byte UEI tarball in GCS doesn't hang VM setup.
# deployment-service-code added 2026-04-18 so deployment_heartbeat.py
# can import deployment_service.bom + deployment_service.deployment_classification
# (the registry itself relocated to UTL 2026-07-13 — unified-trading-library-code
# covers that half) — without it every VM silently drops
# DEPLOYMENT_STARTED/PROGRESS/COMPLETED events.
NEEDED_TARBALLS=("unified-api-contracts-code" "unified-trading-library-code" "deployment-service-code")
# synthetic-benchmark VMs (Phase 5 of mock_data_pipeline_benchmarking_2026_05_10):
# the harness shells out to all 6 cutover-pipeline service CLIs in subprocess
# mode (mtds_read → mdps_compute → features → ml_inference → strategy →
# matching_engine), so every per-service tarball must land in $WORKSPACE
# before the benchmark CLI runs. VM_TASK=synthetic-benchmark + VM_SERVICE=synthetic_benchmark
# triggers the multi-service install path here instead of the single-service
# default.
if [[ "$VM_TASK" == "strategy-paper" || "$VM_TASK" == "strategy-live" || "$VM_TASK" == "defi-paper" ]]; then
  # Paper/live strategy VMs (incl. defi-paper — launch-defi-paper-trading-vm.sh,
  # same run-paper.sh path as strategy-paper) run colocated_engine.py from
  # e2e-testing via run-paper.sh / run-live.sh. colocated_engine.py imports:
  #   strategy_service, execution_service (core logic)
  #   position_balance_monitor_service (treasury state, line 195)
  #   pnl_attribution_service (P&L breakdown, line 558)
  #   risk_and_exposure_service (risk metrics, line 635)
  # (promote_workflow_may23_cli_path_2026_05_10.md Phase 1)
  log "VM_TASK=${VM_TASK} — installing strategy/execution/pbm/pnl/risk + e2e-testing"
  NEEDED_TARBALLS+=(
    "strategy-service-code"
    "execution-service-code"
    "position-balance-monitor-service-code"
    "pnl-attribution-service-code"
    "risk-and-exposure-service-code"
    "e2e-testing-code"
  )
elif [[ "$VM_TASK" == "synthetic-benchmark" || "$VM_SERVICE" == "synthetic_benchmark" ]]; then
  log "VM_TASK=synthetic-benchmark — installing all 6 pipeline service tarballs"
  NEEDED_TARBALLS+=(
    "mtds-code"
    "market-data-processing-service-code"
    "features-service-code"
    "ml-inference-service-code"
    "strategy-service-code"
    "execution-service-code"
  )
elif [[ "$VM_SERVICE" == *"+"* ]]; then
  # Compound VM_SERVICE (e.g. "market_data_processing_service+features_service",
  # launch-mdps-features-live.sh's co-located MDPS+features VM): SERVICE_TARBALLS
  # only has single-service keys, so an unsplit lookup here always misses and used
  # to fall through to "install all" below — the exact 2026-05-16 features_service
  # failure mode this table's own comment describes, just for a "+"-joined key
  # instead of a plain one (found 2026-07-26,
  # issues/mdps_features_live_launcher_shared_venv_dependency_conflict_2026_07_26.md:
  # the "install all" fallback pulled in the archived/stale position-balance-
  # monitor-service tarball, an unrelated unsatisfiable dependency conflict).
  # Resolve each "+"-joined part individually instead of failing the whole lookup.
  IFS='+' read -ra _compound_services <<<"$VM_SERVICE"
  for _svc in "${_compound_services[@]}"; do
    _svc_tarball="${SERVICE_TARBALLS[$_svc]:-}"
    if [ -n "$_svc_tarball" ]; then
      NEEDED_TARBALLS+=("$_svc_tarball")
    else
      log "WARNING: unknown component '$_svc' in compound VM_SERVICE=$VM_SERVICE — no tarball added for it"
    fi
    # The MTDS_DEPENDENT_SERVICES check below only compares the whole (unsplit)
    # $VM_SERVICE against each dep_svc, so it never matches a compound value —
    # mirror it here per-component or a compound MDPS/features VM silently
    # loses mtds-code (the exact "requirements are unsatisfiable" failure two
    # MDPS backfill VMs hit 2026-04-19, per the comment below).
    for _dep_svc in "${MTDS_DEPENDENT_SERVICES[@]}"; do
      if [[ "$_svc" == "$_dep_svc" ]]; then
        case " ${NEEDED_TARBALLS[*]} " in
          *" mtds-code "*) ;;
          *) NEEDED_TARBALLS+=("mtds-code"); log "  (added mtds-code — $_svc depends on MTDS)" ;;
        esac
        break
      fi
    done
  done
else
  SERVICE_TARBALL="${SERVICE_TARBALLS[$VM_SERVICE]:-}"
  if [ -n "$SERVICE_TARBALL" ]; then
    NEEDED_TARBALLS+=("$SERVICE_TARBALL")
  else
    log "WARNING: Unknown VM_SERVICE=$VM_SERVICE — installing all available tarballs"
    for k in "${!TARBALL_DIRS[@]}"; do NEEDED_TARBALLS+=("$k"); done
  fi
fi

# Transitive sibling dependency: MTDS_DEPENDENT_SERVICES is declared earlier
# (before NEEDED_TARBALLS resolution) so the compound-VM_SERVICE branch above
# can also consult it — this loop covers the plain single-VM_SERVICE case.
for dep_svc in "${MTDS_DEPENDENT_SERVICES[@]}"; do
  if [[ "$VM_SERVICE" == "$dep_svc" ]]; then
    case " ${NEEDED_TARBALLS[*]} " in
      *" mtds-code "*) ;;
      *) NEEDED_TARBALLS+=("mtds-code"); log "  (added mtds-code — $VM_SERVICE depends on MTDS)" ;;
    esac
    break
  fi
done

# Sports scheduler needs instruments-service for subprocess dispatches
if [[ "$VM_TASK" == "sports-scheduler-poll" ]]; then
  case " ${NEEDED_TARBALLS[*]} " in
    *" instruments-service-code "*) ;;
    *) NEEDED_TARBALLS+=("instruments-service-code"); log "  (added instruments-service-code — sports scheduler dispatches instruments commands)" ;;
  esac
fi

log "Tarballs to install: ${NEEDED_TARBALLS[*]}"

INSTALLED_DIRS=()

if gsutil ls "gs://${CODE_BUCKET}/code/" >/dev/null 2>&1; then
  for tarball_name in "${NEEDED_TARBALLS[@]}"; do
    dir="${TARBALL_DIRS[$tarball_name]}"
    tarball_path="/tmp/${tarball_name}.tar.gz"
    # Resolve per-tarball SHA pin if set (prevents race with concurrent tarball rebuilds)
    _tarball_pin_sha=""
    case "$tarball_name" in
      unified-trading-library-code)        _tarball_pin_sha="${UTL_TARBALL_SHA:-}" ;;
      unified-api-contracts-code)          _tarball_pin_sha="${UAC_TARBALL_SHA:-}" ;;
      market-data-processing-service-code) _tarball_pin_sha="${MDPS_TARBALL_SHA:-}" ;;
      mtds-code)                           _tarball_pin_sha="${MTDS_TARBALL_SHA:-}" ;;
    esac
    if [[ -n "$_tarball_pin_sha" ]]; then
      _tarball_gcs_src="gs://${CODE_BUCKET}/code/${tarball_name}@${_tarball_pin_sha}.tar.gz"
      # A pinned pull verifies the PINNED manifest (not the floating one, which a
      # concurrent rebuild can move out from under us).
      _tarball_manifest_src="gs://${CODE_BUCKET}/code/${tarball_name}@${_tarball_pin_sha}.manifest.json"
      log "  Using SHA-pinned tarball: ${tarball_name}@${_tarball_pin_sha:0:12}"
    else
      _tarball_gcs_src="gs://${CODE_BUCKET}/code/${tarball_name}.tar.gz"
      _tarball_manifest_src="gs://${CODE_BUCKET}/code/${tarball_name}.manifest.json"
    fi
    if gsutil -q cp "$_tarball_gcs_src" "$tarball_path" 2>/dev/null; then
      mkdir -p "$WORKSPACE/$dir"
      tar xzf "$tarball_path" -C "$WORKSPACE/$dir"
      INSTALLED_DIRS+=("$WORKSPACE/$dir")
      log "Deployed $tarball_name → $WORKSPACE/$dir"

      # Validate tarball manifest.json if present (Phase 3 SHA discipline)
      _tarball_manifest_path="/tmp/${tarball_name}.manifest.json"
      if gsutil -q cp "$_tarball_manifest_src" "$_tarball_manifest_path" 2>/dev/null; then
        _tarball_actual_sha=$(python3 -c "import json; d=json.load(open('$_tarball_manifest_path')); print(d.get('commit_sha','unknown'))" 2>/dev/null || echo "unknown")
        _tarball_pyproject_version=$(python3 -c "import json; d=json.load(open('$_tarball_manifest_path')); print(d.get('pyproject_version','unknown'))" 2>/dev/null || echo "unknown")
        log "  manifest: sha=${_tarball_actual_sha:0:12} version=$_tarball_pyproject_version"

        # Self-verify: a SHA-pinned pull MUST carry that exact sha in its own manifest
        # (prefix-compare so short pins match full manifest shas). Mismatch = loud fail,
        # never run wrong code.
        if [[ -n "$_tarball_pin_sha" && "$_tarball_actual_sha" != "unknown" ]]; then
          _cmp_n=${#_tarball_pin_sha}
          [[ ${#_tarball_actual_sha} -lt $_cmp_n ]] && _cmp_n=${#_tarball_actual_sha}
          if [[ "${_tarball_actual_sha:0:$_cmp_n}" != "${_tarball_pin_sha:0:$_cmp_n}" ]]; then
            log "ERROR: pinned tarball $tarball_name@${_tarball_pin_sha:0:12} carries manifest sha=${_tarball_actual_sha:0:12} — pin/manifest mismatch; refusing to run."
            exit 1
          fi
        fi

        # Assert against launcher-supplied expected SHA (activated when TARBALL_EXPECTED_SHA metadata is set)
        if [[ -n "${TARBALL_EXPECTED_SHA:-}" && "$_tarball_actual_sha" != "$TARBALL_EXPECTED_SHA" ]]; then
          log "ERROR: Tarball SHA drift — $tarball_name expected=${TARBALL_EXPECTED_SHA:0:12} actual=${_tarball_actual_sha:0:12}"
          log "ERROR: Deploy the correct tarball (run create-code-tarballs.sh from the expected commit) and retry."
          exit 1
        fi
      elif [[ -n "$_tarball_pin_sha" ]]; then
        log "ERROR: pinned tarball $tarball_name@${_tarball_pin_sha:0:12} has no manifest — cannot verify provenance; refusing to run unverified code."
        exit 1
      fi
    elif [[ -n "$_tarball_pin_sha" ]]; then
      # A SHA pin was explicitly requested but the pinned object is absent. Never
      # silently fall back to floating/stale code (the exit-2 silent-stale hazard).
      log "ERROR: SHA-pinned tarball not found: $_tarball_gcs_src"
      log "ERROR: rebuild it (create-code-tarballs.sh @ ${_tarball_pin_sha:0:12}) before launch; refusing floating fallback."
      exit 1
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

# ── Internal-package wheel-cache poisoning fix (P0 — contract propagation) ──
# The GCS wheel cache exists ONLY to skip recompiling SLOW EXTERNAL deps (web3,
# pandas, pyarrow, ... — the C-extension builds). It must NEVER serve an INTERNAL
# workspace package (unified_api_contracts / unified_trading_library / the service
# packages): those hold a STATIC 0.x.y version across commits (SETUPTOOLS_SCM_PRETEND_VERSION
# below pins them to 0.99.0), so a wheel built at an OLD sha satisfies the version
# constraint and SHADOWS the "-e, always fresh" editable install — a same-version
# contract change (e.g. a UAC nullable_ohlcv flip) then never reaches the VM. That
# is the exact silent, correctness-critical deployment gap in
# plans/active/issues/mdps_vm_stale_uac_contract_propagation_2026_07_20.md. Compute
# the internal packages' normalized wheel prefixes from the editable dirs we install
# (plus the two contract anchors as a hard safety net) and purge any matching wheel
# from the find-links dir so the editable SOURCE is the only source for them.
# EXTERNAL wheels are untouched — the cache still does its job for the slow builds.
_wheel_dist() { printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -s '._-' '_'; }
INTERNAL_WHEEL_NAMES=" $(_wheel_dist unified-api-contracts) $(_wheel_dist unified-trading-library) "
for _idir in "${INSTALLED_DIRS[@]}"; do
  _ipn=$(python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1],"rb")).get("project",{}).get("name",""))' "$_idir/pyproject.toml" 2>/dev/null || true)
  [[ -n "$_ipn" ]] && INTERNAL_WHEEL_NAMES="${INTERNAL_WHEEL_NAMES}$(_wheel_dist "$_ipn") "
done
log "Internal packages excluded from wheel cache:${INTERNAL_WHEEL_NAMES}"
_purge_internal_wheels() {
  # Delete any internal-package wheel from the given dir so the editable source
  # (not a stale same-version cached wheel) is authoritative. No-op on external
  # wheels and on an empty dir.
  local _wdir="$1" _whl _wbase _wdist _removed=0
  [[ -d "$_wdir" ]] || return 0
  for _whl in "$_wdir"/*.whl; do
    [[ -e "$_whl" ]] || continue
    _wbase="$(basename "$_whl")"
    _wdist="$(_wheel_dist "${_wbase%%-*}")"
    case "$INTERNAL_WHEEL_NAMES" in
      *" $_wdist "*) rm -f "$_whl"; _removed=$((_removed + 1)) ;;
    esac
  done
  [[ "$_removed" -gt 0 ]] && log "  purged $_removed internal-package wheel(s) from $(basename "$_wdir") (editable source is authoritative)"
  return 0
}

# Try to download cached wheels
if gsutil -q ls "$WHEEL_GCS/" >/dev/null 2>&1; then
  log "Downloading cached wheels from GCS..."
  # timeout-guard: a deadlocked `gsutil -m` (parallel-mode hang, observed
  # 2026-05-25 bricking bybit/hyperliquid/kraken at boot) never returns to hit
  # `|| true`, blocking the whole startup script forever. Bound it so boot
  # proceeds (falls back to building wheels from source if the cache is missing).
  timeout 180 gsutil -m -q cp "$WHEEL_GCS/*.whl" "$WHEEL_CACHE/" 2>/dev/null || true
  WHEEL_COUNT=$(ls "$WHEEL_CACHE"/*.whl 2>/dev/null | wc -l)
  log "Downloaded $WHEEL_COUNT cached wheels"
fi
# Remediate an already-poisoned cache: drop internal-package wheels BEFORE the
# editable install below so the find-links dir can never shadow the fresh source.
_purge_internal_wheels "$WHEEL_CACHE"

log "Installing Python dependencies..."
# --no-sources: ignore [tool.uv.sources] in pyproject.toml which points to
# sibling paths like ../unified-api-contracts that don't exist in our
# tarball layout (we use short names: uac, utl, instruments).
# Instead, editable installs resolve deps from each other since all are
# installed in the same call.
# Two-pass install. deployment-service declares deployment-api + fastapi
# + functions-framework as hard deps — none of which are needed by the VM
# heartbeat helper (which only touches deployment_service.bom +
# deployment_service.deployment_classification, stdlib + UTL StorageClient
# via unified_trading_library.deployment_registry). Install it with --no-deps
# to avoid a resolve failure that stops the whole VM. Everything else installs
# normally.
#
# hatch-vcs fallback: tarballs have no .git history; without this env var,
# hatch-vcs calls setuptools_scm.get_version() which exits non-zero when
# no git repo is found. Setting SETUPTOOLS_SCM_PRETEND_VERSION makes every
# hatch-vcs-based package resolve to "0.99.0" on the VM (D13 fleet rollout
# moved workspace repos to version_source=git-tag — 2026-06-27).
# Must be <1.0.0 and >= the highest lower-bound in any cross-package constraint
# (e.g. features-service requires unified-trading-library>=0.13.0,<1.0.0 — 0.99.0
# satisfies both bounds; 0.0.0 would fail the >=0.13.0 floor).
export SETUPTOOLS_SCM_PRETEND_VERSION="0.99.0"
INSTALL_ARGS_STD=("--no-sources")
INSTALL_ARGS_NODEPS=("--no-sources" "--no-deps")
# For synthetic-benchmark VMs, install every service package (mdps / features /
# ml-infer / strategy / execution + deployment) via --no-deps. Reason: the
# 6 service pyproject.tomls each pin transitive deps (e.g. execution-service
# pins requests<2.33.0; another pyproject in the union wants >=2.33.0) and the
# combined resolve raises `requirements are unsatisfiable`. The harness only
# needs the service modules importable (it shells out to their CLIs which
# run in the same venv UAC + UTL + MTDS set up); --no-deps gives that without
# trying to satisfy every transitive pin. Anchor deps (UAC + UTL + MTDS) still
# install with full deps in STD — they're the SSOT for what the workspace
# expects in the venv.
# e2e-testing routes to NODEPS too (paper/benchmark VMs): e2e-testing's pyproject declares
# the service packages (execution-service / strategy-service) as deps, which --no-sources
# CANNOT resolve from PyPI → the STD resolve fails before the NODEPS services install. Those
# deps are the other editables (installed separately), so --no-deps installs the e2e-testing
# scripts without the unsatisfiable resolution. (fix 2026-06-19 — funding-ensemble paper VM.)
_SVC_BENCH_NODEPS=(deployment mdps features ml-infer strategy execution pbm pnl risk e2e-testing)
# strategy-paper / strategy-live / defi-paper: execution-service's transitive betfairlightweight
# dep pins requests<2.33.0, conflicting with the workspace canonical requests>=2.33.0
# (CVE-2026-25645) floor. execution-service's OWN pyproject.toml already declares the fix
# ([tool.uv] override-dependencies = ["requests>=2.33.0,<3.0.0", ...]) — but `uv pip install`
# (the pip-compatible interface used here, not `uv sync`) does not read project-level [tool.uv]
# config automatically, so that override was silently never applied, forcing strategy/execution/
# e2e-testing onto --no-deps (which then also skipped every OTHER real dependency —
# nautilus-trader, solana, solders, ... — a "found via VM crash" whack-a-mole). Fix (2026-07-13):
# pass the SAME override explicitly via --overrides so `uv pip install`'s plain resolve respects
# it too. Verified: a full combined `uv pip install -e deployment-service -e strategy-service
# -e execution-service -e e2e-testing ... --overrides <this file>` resolves cleanly (252 pkgs,
# betfairlightweight/nautilus-trader/solana/solders all genuinely present). pbm/pnl/risk stay on
# --no-deps below (documented separate UAC version-pinning conflict, not re-verified here).
UV_OVERRIDE_ARGS=()
if [[ "$VM_TASK" == "strategy-paper" || "$VM_TASK" == "strategy-live" || "$VM_TASK" == "defi-paper" ]]; then
  UV_OVERRIDE_FILE=$(mktemp)
  echo "requests>=2.33.0,<3.0.0" > "$UV_OVERRIDE_FILE"
  UV_OVERRIDE_ARGS=("--overrides" "$UV_OVERRIDE_FILE")
fi
for dir in "${INSTALLED_DIRS[@]}"; do
  _base="$(basename "$dir")"
  _route_to_nodeps=false
  for _bn in "${_SVC_BENCH_NODEPS[@]}"; do
    if [[ "$_base" == "$_bn" ]]; then _route_to_nodeps=true; break; fi
  done
  # Outside synthetic-benchmark, only `deployment` historically routes to NODEPS — preserve that
  # by checking VM_TASK so other VMs aren't affected. strategy-paper/strategy-live/defi-paper
  # route pbm/pnl/risk to NODEPS too (separate unverified UAC pinning conflict); strategy/
  # execution/e2e-testing now go through STD below (--overrides fix, see comment above).
  if [[ "$VM_TASK" != "synthetic-benchmark" && "$_base" != "deployment" ]]; then
    if [[ "$VM_TASK" == "strategy-paper" || "$VM_TASK" == "strategy-live" || "$VM_TASK" == "defi-paper" ]]; then
      _route_to_nodeps=false
      for _bn in pbm pnl risk; do
        if [[ "$_base" == "$_bn" ]]; then _route_to_nodeps=true; break; fi
      done
    else
      _route_to_nodeps=false
    fi
  fi
  if $_route_to_nodeps; then
    INSTALL_ARGS_NODEPS+=("-e" "$dir")
  else
    INSTALL_ARGS_STD+=("-e" "$dir")
  fi
done
log "  uv pip install ${INSTALL_ARGS_STD[*]}"
uv pip install --find-links "$WHEEL_CACHE" "${UV_OVERRIDE_ARGS[@]}" "${INSTALL_ARGS_STD[@]}" 2>&1 | tail -5
if [[ "${#INSTALL_ARGS_NODEPS[@]}" -gt 2 ]]; then
  log "  uv pip install ${INSTALL_ARGS_NODEPS[*]}"
  uv pip install --find-links "$WHEEL_CACHE" "${INSTALL_ARGS_NODEPS[@]}" 2>&1 | tail -5
else
  log "  (skipping --no-deps install — no packages routed to NODEPS for VM_TASK=${VM_TASK})"
fi

# deployment_service/__init__.py eagerly imports the whole package
# (monitor/orchestrator/backends), which transitively needs jinja2 +
# pyyaml for template rendering in backends/services/vm_config.py and
# yaml parsing in config_loader.py. Importing the heartbeat helper
# (`from deployment_service.bom import ...` / `from deployment_service.deployment_classification
# import ...`) therefore evaluates the parent __init__ and fails without
# these. Install just the two minimal runtime extras needed by the init chain.
log "  uv pip install jinja2 pyyaml  (deployment_service __init__ chain extras)"
uv pip install --find-links "$WHEEL_CACHE" jinja2 pyyaml 2>&1 | tail -3
# position_balance_monitor_service.storage.database eagerly imports sqlalchemy at module load
# time; pbm/pnl/risk still route to --no-deps above (separate unverified UAC pinning conflict).
# sqlalchemy/plotly/nautilus-trader no longer need an explicit standalone install here (fix
# 2026-07-13, superseding the two prior interim workarounds landed the same day): strategy-
# service (declares sqlalchemy + plotly) and execution-service (declares sqlalchemy +
# nautilus-trader) now install via the STD --overrides path above, in the SAME shared venv PBM
# lands in, so PBM's sqlalchemy need is satisfied as a byproduct.
# Use STD args for the wheel-cache step below (deployment-service's
# heavyweight deps shouldn't be cached either).
INSTALL_ARGS=("${INSTALL_ARGS_STD[@]}")

# Upload any newly compiled wheels to GCS for next VM
NEW_WHEELS=$(find "$VENV/lib" -name "*.whl" -newer "$WHEEL_CACHE" 2>/dev/null | wc -l)
if [[ "$NEW_WHEELS" -gt 0 ]] || [[ ! -f "$WHEEL_CACHE/.uploaded" ]]; then
  log "Caching compiled wheels to GCS..."
  # Build wheels for all installed packages (captures compiled C extensions)
  uv pip wheel --wheel-dir "$WHEEL_CACHE" "${INSTALL_ARGS[@]}" -q 2>/dev/null || true
  # `uv pip wheel` also builds wheels for the -e internal packages — never upload
  # those (they would re-poison the cache with a static-version stale wheel that
  # shadows a future contract change). Purge them again before the upload glob.
  _purge_internal_wheels "$WHEEL_CACHE"
  # timeout-guard the upload too — this is the exact step that deadlocked and
  # left 3 CeFi VMs hung at boot (gsutil -m parallel-upload hang). Bounded so
  # the workload still launches even if the cache refresh wedges.
  timeout 180 gsutil -m -q cp "$WHEEL_CACHE"/*.whl "$WHEEL_GCS/" 2>/dev/null || true
  touch "$WHEEL_CACHE/.uploaded"
  log "Wheels cached to $WHEEL_GCS"
fi

python -c 'from unified_api_contracts.sports import LEAGUE_REGISTRY; print(f"UAC OK: {len(LEAGUE_REGISTRY)} leagues")'

# ── Contract-freshness assertion (P0 — mdps_vm_stale_uac_contract_propagation_2026_07_20) ──
# The unified_api_contracts just verified importable MUST resolve from the editable
# tarball source under $WORKSPACE, NOT from a wheel in the venv site-packages. A
# site-packages resolution means the GCS wheel cache shadowed the "-e, always fresh"
# install — the exact stale-contract propagation bug (a VM validated market-data
# writes against a STALE non-nullable deriv_ohlcv schema even though LDR AND the
# current UAC tarball both carried nullable_ohlcv=True). Fail LOUD so a stale
# contract can never again validate silently. UAC is a core tarball (always installed
# editable), so this is unconditional. When UAC_TARBALL_SHA was pinned, the per-tarball
# manifest-sha self-verify in step 4 already asserted the tarball's provenance; this
# closes the remaining gap between "correct tarball on disk" and "correct code imported".
_uac_file=$(python -c 'import unified_api_contracts as m; print(m.__file__ or "")' 2>/dev/null || echo "")
log "  unified_api_contracts.__file__ = ${_uac_file:-<none>}"
case "$_uac_file" in
  "$WORKSPACE"/*)
    log "  UAC contract source OK: editable install under $WORKSPACE (not a shadowing cached wheel)" ;;
  *)
    log "ERROR: unified_api_contracts resolved from '${_uac_file:-<none>}', NOT the editable source under $WORKSPACE."
    log "ERROR: a cached wheel shadowed the editable install — the VM would validate against a STALE contract schema"
    log "ERROR: (mdps_vm_stale_uac_contract_propagation_2026_07_20). Refusing to run on an unverified contract."
    exit 1 ;;
esac

# Verify whichever service is installed
python -c 'import market_tick_data_service; print("MTDS OK")' 2>/dev/null || true
python -c 'import instruments_service; print("instruments-service OK")' 2>/dev/null || true
log "Dependencies installed successfully (${#INSTALLED_DIRS[@]} packages)"

# ── 5. Auto-launch task (metadata already read in step 2b) ──
export GCP_PROJECT_ID="${GCP_PROJECT_ID:-central-element-323112}"
export CLOUD_PROVIDER="${CLOUD_PROVIDER:-gcp}"
export CLOUD_MOCK_MODE="${CLOUD_MOCK_MODE:-false}"
# Manifest-429 per-VM sharding (manifest_429_per_vm_sharding_2026_04_25):
# every backfill / forward-poll VM writes to `_index/per_vm/{VM_NAME}.parquet`
# instead of CAS-fighting the canonical blob. The minutely consolidator
# (deployment-service/terraform/gcp/manifest_consolidator_scheduler.tf)
# merges shards back into `_index/availability_index.parquet`.
export MANIFEST_PER_VM_SHARDS="${MANIFEST_PER_VM_SHARDS:-true}"

# ── 5a. VM identity + observability setup (all task modes, including backtest) ──
VM_NAME_SELF=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/name" || echo "unknown-vm")
GCS_LOG_DIR="gs://deployment-scripts-central-element-323112/vm-logs/${VM_NAME_SELF}"
GCS_LOG_URI="${GCS_LOG_DIR}/run.log"
TEE_WRAPPER="/tmp/vm-exec-with-gcs-tee.sh"
HEARTBEAT_HELPER="/tmp/deployment_heartbeat.py"
HEARTBEAT_DAEMON="/tmp/heartbeat_daemon.py"

# GCS-blob heartbeat sidecar (2026-05-01) — independent of Pub/Sub. Writes
# gs://deployment-scripts-{pid}/vm-heartbeat/{vm-name}.txt every 60s. Read by
# the external vm-zombie-watchdog daemon to detect zombie VMs whose network
# namespace died (the 2026-04-29 cefi rollout failure mode where 12 VMs lost
# link-local metadata routes; in-VM Pub/Sub heartbeats can't help because they
# also can't reach Pub/Sub when the L2 network is gone). GCS endpoint is
# usually reachable via private google access even when metadata is gone — and
# if even GCS is unreachable, the watchdog detects the stale blob and kills.
HEARTBEAT_SIDECAR="/tmp/vm_heartbeat_sidecar.sh"
if gsutil -q cp "gs://${CODE_BUCKET}/vm/vm_heartbeat_sidecar.sh" "$HEARTBEAT_SIDECAR" 2>/dev/null; then
  chmod +x "$HEARTBEAT_SIDECAR"
  nohup bash "$HEARTBEAT_SIDECAR" "$VM_NAME_SELF" >/dev/null 2>&1 &
  log "GCS-blob heartbeat sidecar started (60s interval, gs://${CODE_BUCKET}/vm-heartbeat/${VM_NAME_SELF}.txt)"
else
  log "WARNING: vm_heartbeat_sidecar.sh not found in GCS — external zombie-watchdog will fall back to manifest shard staleness"
fi

# Download the debug-log wrapper (tees stdout+stderr to GCS every 30s so we can
# monitor any VM task from outside even when SSH is broken).
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
  # them. Without this the heartbeat registers every VM as asset_group=UNKNOWN
  # task=vm-exec mode=full (the defaults in vm-exec-with-gcs-tee.sh), which
  # is what the 2026-04-19 Playwright audit caught across all 14 VMs.
  export VM_NAME="$VM_NAME_SELF"
  export VM_TASK="${VM_TASK:-}"
  export VM_ASSET_GROUP="${VM_ASSET_GROUP:-UNKNOWN}"
  export VM_MODE="${VM_MODE:-${VM_BACKFILL_MODE:-full}}"
  export VM_START_DATE="${VM_START_DATE:-}"
  export VM_END_DATE="${VM_END_DATE:-}"
  # Phase 3c (artifact_pipeline_observability plan) — stamp the tarball commit this VM booted, for
  # deployment-registry provenance. `_tarball_actual_sha` is already computed by the download loop
  # above (the LAST tarball's manifest `commit_sha`, "unknown" if unparseable) — this reads it, never
  # re-derives it. Deliberately mid-block, never a trailing `[[ ]] && export` (under `set -euo
  # pipefail`, that form's failure would abort EVERY VM boot). Never abort on a missing/garbage SHA —
  # degrade to the pre-existing "" (resolve_deployment_bom() already treats that as honestly unknown).
  # Semantics: this is "the manifest commit_sha this VM read at boot", NOT an attestation of the
  # running bytes (a floating pull can race the */30 refresh cron between the tarball and manifest
  # `gsutil cp` calls) — the value is not sent at all when unknown, same absence as today.
  if [[ -n "${_tarball_actual_sha:-}" && "$_tarball_actual_sha" != "unknown" ]]; then
    export GIT_COMMIT="$_tarball_actual_sha"
  fi
  export PYTHON_BIN="$VENV/bin/python"
  # VM-life PIPELINE_HEARTBEAT marker (BUG1b, 2026-06-22). The in-process UTL
  # PipelineHeartbeatTimer publishes a PIPELINE_HEARTBEAT *event* to PubSub, but
  # (a) a chunked backfill re-execs python ONCE PER CHUNK so the timer is born+dies
  # per chunk (a sub-60s chunk emitted nothing), and (b) the published event never
  # reaches the GCS-tee'd run.log the fleet watcher reads. This single, centralized
  # 60s emitter (covers EVERY data task — chunked backfill AND single-process live)
  # echoes a parseable PIPELINE_HEARTBEAT marker INSIDE the tee'd command so it
  # flows to stdout → run.log → GCS for the VM's whole life, giving the
  # heartbeat-stall watcher a worker-heartbeat signal DECOUPLED from the always-fresh
  # infra ``vm-heartbeat`` sidecar (which ticked even when the data worker was dead →
  # the "zero alerts" blind spot). The backgrounded loop is a child of the tee'd
  # bash, so it dies when the task ends / VM self-deletes. Skip infra VMs
  # (asset_group UNKNOWN) — they run no data worker, so must not look like one.
  local _hb_prefix=""
  if [[ "${VM_ASSET_GROUP:-UNKNOWN}" != "UNKNOWN" ]]; then
    _hb_prefix="( while true; do echo \"PIPELINE_HEARTBEAT vm=${VM_NAME_SELF} ag=${VM_ASSET_GROUP} task=${VM_TASK:-} source=vm-life-emitter ts=\$(date -u +%Y-%m-%dT%H:%M:%SZ)\"; sleep 60; done ) & __DP_HB_PID=\$!; trap 'kill \"\$__DP_HB_PID\" 2>/dev/null || true' EXIT; "
    cmd="${_hb_prefix}${cmd}"
    log "VM-life PIPELINE_HEARTBEAT marker emitter wired into tee'd command (60s, → run.log)"
  fi
  if [[ -n "$TEE_WRAPPER" ]]; then
    log "Launching with GCS tee: $cmd"
    log "  VM_NAME=$VM_NAME VM_ASSET_GROUP=$VM_ASSET_GROUP VM_TASK=$VM_TASK VM_MODE=$VM_MODE"
    nohup bash "$TEE_WRAPPER" "$GCS_LOG_URI" bash -c "$cmd" > "$fallback_log" 2>&1 &
  else
    # No TEE_WRAPPER (download failed) — add inline VM_SHUTDOWN_ON_COMPLETION
    # handling so the VM self-deletes even without the tee wrapper.
    # Uses setsid + trap '' HUP TERM to survive systemd cgroup teardown
    # (mirrors the same strategy in vm-exec-with-gcs-tee.sh lines 59-63).
    log "Launching plain (no TEE_WRAPPER): $cmd"
    nohup setsid bash -c "
      trap '' HUP TERM
      $cmd
      _PLAIN_RC=\$?
      _SD=\$(curl -sf -H 'Metadata-Flavor: Google' \
          'http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_SHUTDOWN_ON_COMPLETION' \
          2>/dev/null || echo '')
      if [[ \"\$_SD\" == 'true' ]]; then
        _NM=\$(curl -sf -H 'Metadata-Flavor: Google' \
            'http://metadata.google.internal/computeMetadata/v1/instance/name' 2>/dev/null || echo '')
        _ZN=\$(curl -sf -H 'Metadata-Flavor: Google' \
            'http://metadata.google.internal/computeMetadata/v1/instance/zone' 2>/dev/null | awk -F/ '{print \$NF}')
        if [[ -n \"\$_NM\" && -n \"\$_ZN\" ]]; then
          sleep 10
          gcloud compute instances delete \"\$_NM\" --zone=\"\$_ZN\" --quiet --delete-disks=all \
            || sudo shutdown -h now
        fi
      fi
      exit \$_PLAIN_RC
    " > "$fallback_log" 2>&1 &
  fi
  log "Task launched PID: $!"
  # Disarm the early-bootstrap EXIT trap — from this point on, the wrapped
  # vm-exec-with-gcs-tee.sh (or the plain setsid wrapper above) owns lifecycle.
  # A non-zero exit of this setup script AFTER successful launch must NOT delete
  # the VM, or we'd wipe the running pipeline.
  trap - EXIT
}

# ── 5b. OOM preflight: availability_index.parquet mtime check ──
# 2026-05-28 defense-in-depth: when manifest-consolidator is degraded, the
# consolidated availability_index.parquet goes stale. UTL's read_availability_index
# falls back to merging all per-VM shards (1700+ on cefi) → ~12GB+ Python heap
# → OOM-kill at startup before vm-exec-with-gcs-tee.sh's wrapped lifecycle
# triggers. Catch this before Python runs: if the index is staler than the
# launcher-supplied MANIFEST_CONSOLIDATED_STALENESS_SEC budget (default 86400s),
# exit 78 (EX_CONFIG). The EXIT trap above catches and self-deletes the VM
# (forensics: vm-logs/<vm>/vm-setup.log + SETUP_EXIT_STATUS).
#
# Composes with todo (b) in vm_zombie_watchdog_diagnosis_2026_05_28.md:
# this is the "option (b) shell preflight" variant; option (a) (in-Python
# fail-fast at UTL ManifestReader) remains a future hardening.
if [[ "${VM_SERVICE:-}" == "market_tick_data_service" && "${VM_OPERATION:-}" == "download" ]]; then
    _AG_LOWER=$(echo "${VM_ASSET_GROUP:-}" | tr '[:upper:]' '[:lower:]')
    # cloud-providers.yaml uses 'pred' (not 'prediction') in the bucket short name.
    [[ "$_AG_LOWER" == "prediction" ]] && _AG_LOWER="pred"
    case "$_AG_LOWER" in
        cefi|defi|tradfi|sports|pred)
            _BUDGET_SEC="${MANIFEST_CONSOLIDATED_STALENESS_SEC:-86400}"
            _BUCKET="market-data-tick-${_AG_LOWER}-${DEPLOYMENT_ENV_SHORT:-prd}-${GCP_PROJECT_ID:-central-element-323112}"
            _INDEX_URI="gs://${_BUCKET}/_index/availability_index.parquet"
            log "OOM preflight: checking ${_INDEX_URI} mtime against budget ${_BUDGET_SEC}s"
            _INDEX_UPDATED=$(gsutil ls -L "${_INDEX_URI}" 2>/dev/null | awk -F': +' '/Update time/{print $2; exit}')
            if [[ -z "${_INDEX_UPDATED}" ]]; then
                log "OOM preflight WARNING: ${_INDEX_URI} not found — consolidator hasn't materialised the index yet (proceeding; reader will use per-VM fallback)"
            else
                _INDEX_EPOCH=$(date -d "${_INDEX_UPDATED}" +%s 2>/dev/null || echo 0)
                _NOW_EPOCH=$(date +%s)
                _AGE_SEC=$(( _NOW_EPOCH - _INDEX_EPOCH ))
                if (( _AGE_SEC > _BUDGET_SEC )); then
                    log "OOM preflight FAIL: ${_INDEX_URI} is ${_AGE_SEC}s stale (budget ${_BUDGET_SEC}s) — exiting 78 to skip Python startup; EXIT trap will self-delete VM."
                    log "  Diagnosis: manifest-consolidator for asset_group=${_AG_LOWER} is degraded. Reader would fall back to merging per-VM shards → OOM at startup. Fix consolidator + relaunch."
                    exit 78
                fi
                log "OOM preflight OK: index is ${_AGE_SEC}s fresh (budget ${_BUDGET_SEC}s)"
            fi
            ;;
    esac
fi

if [[ "$VM_PIPELINE_MODE" == "backtest" ]]; then
  # Full L1-L7 pipeline for the asset_group — uses backfill-cluster.sh from
  # deployment-service (uploaded alongside this script).
  # Routes through _launch_with_tee so DEPLOYMENT_STARTED/COMPLETED/FAILED
  # events are emitted and GCS log is streamed (fixes 2026-05-15 audit gap:
  # original path used bare nohup + exit 0 before heartbeat setup).
  BACKFILL_SCRIPT="$WORKSPACE/deployment/scripts/vm/backfill-cluster.sh"
  BACKFILL_ARGS="--cluster ${VM_ASSET_GROUP,,} --start-date $VM_START_DATE --end-date $VM_END_DATE"
  [[ -n "$VM_STRATEGY" ]] && BACKFILL_ARGS="$BACKFILL_ARGS --strategy $VM_STRATEGY"

  if [[ -f "$BACKFILL_SCRIPT" ]]; then
    log "Backtest mode: running full pipeline via backfill-cluster.sh"
    log "  Args: $BACKFILL_ARGS"
    _launch_with_tee "bash $BACKFILL_SCRIPT $BACKFILL_ARGS" "$LOGS/backtest-pipeline.log"
  else
    log "WARNING: backfill-cluster.sh not found at $BACKFILL_SCRIPT — falling back to e2e-testing"
    E2E_SCRIPT="$WORKSPACE/e2e/scripts/${VM_ASSET_GROUP,,}/run-full-pipeline.sh"
    if [[ -f "$E2E_SCRIPT" ]]; then
      _launch_with_tee "bash $E2E_SCRIPT --start-date $VM_START_DATE --end-date $VM_END_DATE" "$LOGS/backtest-pipeline.log"
    else
      log "ERROR: No pipeline script found for asset_group $VM_ASSET_GROUP"
    fi
  fi
  exit 0  # skip generic VM_TASK routing — backtest handled above via _launch_with_tee
fi

if [[ "$VM_TASK" == "canonical-migration" ]]; then
  # Phase 3.4 migration scripts: MIGRATION_CMD metadata carries the full
  # command (e.g. "python -m market_tick_data_service.scripts.migrate_defi_canonical ...").
  # The migration script's OWN module lives wherever VM_SERVICE's tarball was
  # extracted — this task was originally MTDS-only (hardcoded cd into
  # $WORKSPACE/mtds), but a later instruments-service migration script
  # (reclassify_defi_curve_optimism_subgraph_deindexed_2026_07_24.py) hit
  # "ERROR: $WORKSPACE/mtds missing" launched with VM_SERVICE=instruments_service
  # because this branch never consulted VM_SERVICE at all. Derive the
  # workspace dir via the SAME SERVICE_TARBALLS -> TARBALL_DIRS mapping the
  # tarball-install step above already uses (never hand-roll a second
  # service->dir mapping) — this generalises the branch to whichever
  # VM_SERVICE the launcher set, not just MTDS.
  VM_MIGRATION_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_MIGRATION_CMD" || echo "")
  if [[ -n "$VM_MIGRATION_CMD" ]]; then
    FULL_CMD="${VM_MIGRATION_CMD/python /$VENV/bin/python }"
    _MIGRATION_TARBALL="${SERVICE_TARBALLS[$VM_SERVICE]:-mtds-code}"
    _MIGRATION_DIR="${TARBALL_DIRS[$_MIGRATION_TARBALL]:-mtds}"
    cd "$WORKSPACE/$_MIGRATION_DIR" || { log "ERROR: $WORKSPACE/$_MIGRATION_DIR missing (VM_SERVICE=$VM_SERVICE)"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/canonical-migration.log"
  else
    log "ERROR: canonical-migration task without VM_MIGRATION_CMD metadata"
  fi
elif [[ "$VM_TASK" == "sports-v9-migration" ]]; then
  # E4 (sports_manifest_canonicalisation_2026_06_01.md) — year-sharded sports
  # v9 migration. launch-sports-v9-migration-vm.sh sets VM_TASK=sports-v9-migration
  # + VM_MIGRATION_CMD carrying the two-phase sequential command
  # ("migrate_sports_canonical_v9 ... && rebuild_sports_manifest_v9 ...").
  # Root-caused 2026-07-12: this VM_TASK value had no dispatch branch, so it
  # fell through to the generic elif [ -n "$VM_TASK" ] fallback below, which
  # built --operation "${VM_OPERATION}" (="migrate-sports-v9-{surface}") — not
  # a registered market-tick-data-service CLI operation — argparse error,
  # exit_code=2 on all 16 fleet VMs of the first E4 attempt. Mirrors the
  # canonical-migration dispatch above (VM_MIGRATION_CMD-driven, both phases
  # live in market_tick_data_service so cd into $WORKSPACE/mtds once).
  VM_MIGRATION_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_MIGRATION_CMD" || echo "")
  if [[ -n "$VM_MIGRATION_CMD" ]]; then
    FULL_CMD="${VM_MIGRATION_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/mtds" || { log "ERROR: $WORKSPACE/mtds missing"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/sports-v9-migration.log"
  else
    log "ERROR: sports-v9-migration task without VM_MIGRATION_CMD metadata"
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
elif [[ "$VM_TASK" == "sports-gap-fill" ]]; then
  # 2026-05-06 — targeted (date, league_id) gap-fill for sports data types.
  # Runs an instruments-service/scripts/fill_missing_*.py script that reads
  # the canonical manifest, computes the missing-shard set, and calls the
  # adapter directly — bypassing the orchestrator's chronological date
  # iteration that wastes wall-clock on _should_skip_shard checks for
  # already-captured dates. Mirrors the sports-manifest-rescan dispatch
  # shape; VM_MIGRATION_CMD carries the full python invocation so the
  # host-side launcher controls --concurrency / --start-date / --end-date
  # / --limit flags. First use: PLAYER_STATS gap-fill (23k shards across
  # 1.6k dates) replacing the slow chronological af-backfill VM.
  VM_MIGRATION_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_MIGRATION_CMD" || echo "")
  if [[ -n "$VM_MIGRATION_CMD" ]]; then
    FULL_CMD="${VM_MIGRATION_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/instruments" || { log "ERROR: $WORKSPACE/instruments missing"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/sports-gap-fill.log"
  else
    log "ERROR: sports-gap-fill task without VM_MIGRATION_CMD metadata"
  fi
elif [[ "$VM_TASK" == "mdps-sports-bucket" ]]; then
  # Pass K of sports_predictions_e2e_2026_05_05 (2026-05-05) — MDPS bucket
  # adapter pass over canonical Odds-API data. Runs
  # market-data-processing-service/scripts/reprocess_sports_odds.py to
  # produce per-(league_id, horizon) bucketed parquets and per-shard
  # manifest entries (the user directive: "the manifest for MDPS need to
  # record properly per shard league and day and time bucket"). Like the
  # canonical-migration dispatch above, ``VM_MIGRATION_CMD`` carries the
  # full python invocation so the host-side launcher controls the date
  # slice + ``--workers`` / ``--force`` flags.
  VM_MIGRATION_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_MIGRATION_CMD" || echo "")
  if [[ -n "$VM_MIGRATION_CMD" ]]; then
    FULL_CMD="${VM_MIGRATION_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/mdps" || { log "ERROR: $WORKSPACE/mdps missing"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/mdps-sports-bucket.log"
  else
    log "ERROR: mdps-sports-bucket task without VM_MIGRATION_CMD metadata"
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
elif [[ "$VM_TASK" == "manifest-consolidator-poll" ]]; then
  # Long-lived daemon that periodically consolidates per-VM manifest shards
  # into the canonical _index/availability_index.parquet for each sports /
  # cefi / defi / tradfi / prediction bucket. Pre-requisite: UTL bug fix
  # 2026-04-29 (BlobMetadata path filter) — without it, consolidator reports
  # shards_scanned: 0 and is a no-op. SSOT:
  # codex/02-data/sports-schema-paths.md "Manifest consolidator + per_vm shard
  # merge mechanics" + memory:reference_manifest_consolidator_per_vm_merge.md.
  POLL_INTERVAL=$(_meta VM_POLL_INTERVAL)
  POLL_INTERVAL="${POLL_INTERVAL:-60}"
  BUCKETS_RAW=$(_meta VM_BUCKETS)
  # Launcher encodes buckets with ':' separator (gcloud --metadata can't
  # nest ',' inside a value without ^|^ escape gymnastics). Convert back
  # to space-separated for the bash loop. Default = all asset_group buckets.
  if [[ -z "$BUCKETS_RAW" ]]; then
    # Default = every bucket family that uses ManifestWriter v5: reference
    # data (instruments-store-*), market tick (market-data-tick-*), derived
    # features (features-sports-*), strategy outputs (strategy-store-*).
    # Uses env-tiered names (Phase 0f canonical form). Cloud Run crons in
    # manifest_consolidator_scheduler.tf handle the legacy (no-env-suffix)
    # counterparts written by MDPS scripts not yet migrated.
    BUCKETS_RAW="instruments-store-sports-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:instruments-store-cefi-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:instruments-store-defi-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:instruments-store-tradfi-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:instruments-store-pred-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:market-data-tick-sports-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:market-data-tick-cefi-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:market-data-tick-defi-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:market-data-tick-tradfi-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:market-data-tick-pred-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:features-sports-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:strategy-store-cefi-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:strategy-store-sports-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
    BUCKETS_RAW="${BUCKETS_RAW}:strategy-store-defi-${DEPLOYMENT_ENV_SHORT}-${GCP_PROJECT_ID}"
  fi
  BUCKETS_SPACE="${BUCKETS_RAW//:/ }"
  log "manifest-consolidator-poll: interval=${POLL_INTERVAL}s buckets=[$BUCKETS_SPACE]"
  # Bash poll loop: every $POLL_INTERVAL, call consolidator --once for each bucket.
  # The consolidator's sentinel-lock prevents concurrent races within a bucket.
  cat >"$WORKSPACE/manifest_consolidator_loop.sh" <<EOF_LOOP
#!/usr/bin/env bash
set -uo pipefail
POLL=${POLL_INTERVAL}
BUCKETS="${BUCKETS_SPACE}"
while true; do
  for bucket in \$BUCKETS; do
    echo "[\$(date -u +%FT%TZ)] consolidating \$bucket" >&2
    "$VENV/bin/python" -m unified_trading_library.manifest_consolidator --bucket "\$bucket" --once 2>&1 | tail -3
  done
  sleep "\$POLL"
done
EOF_LOOP
  chmod +x "$WORKSPACE/manifest_consolidator_loop.sh"
  _launch_with_tee \
    "$WORKSPACE/manifest_consolidator_loop.sh" \
    "$LOGS/manifest-consolidator.log"
elif [[ "$VM_TASK" == "strategy-backtest-grid" ]]; then
  # 2026-05-10 — 2-yr config-grid backtest runner for the May-23 live-DeFi
  # cutover (audit Item #2 + master Group F Item 18). VM_BACKFILL_CMD carries
  # the full strategy_service.scripts.run_2yr_config_grid_backtest invocation
  # so the host-side launcher controls --archetype / --start / --end /
  # --grid-density / --smoke flags. Runs in $WORKSPACE/strategy.
  # Writes per-config + summary parquets to
  # gs://strategy-store-{pid}/backtests/config_grid_2yr/{archetype}/{run_id}/
  # via the runner's internal GCS writer. Self-deletes on completion via
  # the launcher-attached shutdown-script (VM_SHUTDOWN_ON_COMPLETION=true).
  VM_BACKFILL_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$VM_BACKFILL_CMD" ]]; then
    FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/strategy" || { log "ERROR: $WORKSPACE/strategy missing"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/strategy-backtest-grid.log"
  else
    log "ERROR: strategy-backtest-grid task without VM_BACKFILL_CMD metadata"
  fi
elif [[ "$VM_TASK" == "strategy-paper" || "$VM_TASK" == "strategy-live" || "$VM_TASK" == "defi-paper" ]]; then
  # Strategy paper/live trading VMs (incl. defi-paper — launch-defi-paper-trading-vm.sh)
  # — run colocated_engine.py via run-paper.sh / run-live.sh from
  # $WORKSPACE/e2e-testing/scripts/defi/.
  # (promote_workflow_may23_cli_path_2026_05_10.md Phase 1)
  # Found 2026-07-13: VM_TASK=defi-paper had NO dispatch branch here, so it fell
  # through to the generic `elif [ -n "$VM_TASK" ]` fallback below, which built
  # `--operation $VM_OPERATION` literally (="paper") — but strategy-service's CLI
  # has no such choice (paper-run/paper-stream only) — an immediate argparse crash
  # before any archetype code ever ran. launch-defi-paper-trading-vm.sh already
  # prepares the correct VM_BACKFILL_CMD; this VM_TASK just needed to route to it,
  # same as strategy-paper/strategy-live.
  # VM_BACKFILL_CMD carries the full run-paper.sh / run-live.sh invocation set
  # by the launcher (e.g. "bash scripts/defi/run-paper.sh --strategy carry_staked_basis
  # --tick-interval 3600 --continuous").
  VM_BACKFILL_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$VM_BACKFILL_CMD" ]]; then
    E2E_DIR="$WORKSPACE/e2e-testing"
    if [[ ! -d "$E2E_DIR" ]]; then
      log "ERROR: $E2E_DIR not found — e2e-testing-code tarball may be missing"
      exit 1
    fi
    # run-paper.sh / run-live.sh look for ${WORKSPACE}/.venv-workspace/bin/python
    # (local-dev convention). On VM the venv lives at $VENV. Symlink so the scripts
    # find Python without modification. (promote_workflow Phase 1 fix 2026-05-12)
    ln -sfn "$VENV" "${WORKSPACE}/.venv-workspace"
    log "Symlinked ${WORKSPACE}/.venv-workspace → $VENV for run-{paper,live}.sh"
    cd "$E2E_DIR" || { log "ERROR: cannot cd into $E2E_DIR"; exit 1; }
    # Self-delete: resolve zone at launch so the delete works even if metadata
    # server is unavailable at engine-exit time. Chain with ';' so delete
    # runs regardless of whether the engine exits 0 or non-zero.
    _VM_ZONE=$(curl -sf -H "Metadata-Flavor: Google" \
      "http://metadata.google.internal/computeMetadata/v1/instance/zone" | awk -F/ '{print $NF}')
    # `log` is a function in this script's shell, NOT available in the _launch_with_tee
    # `bash -c` subshell — and the self-delete frequently kills its own process mid-delete
    # (the VM removes itself while gcloud is still running) → a non-zero, which `|| log`
    # turned into a `log: command not found` rc=127 → a FALSE DEPLOYMENT_FAILED even when the
    # task exited rc=0. Use `echo` (always available) + `|| true` so the self-delete race can
    # never mask a successful run as failed. (fix 2026-06-19 — funding-ensemble paper VM.)
    _SELF_DELETE="gcloud compute instances delete '$VM_NAME_SELF' --zone='$_VM_ZONE' --quiet 2>&1 || echo 'WARNING: VM self-delete returned nonzero (often the self-delete killing its own process)' || true"
    _launch_with_tee "$VM_BACKFILL_CMD; $_SELF_DELETE" "$LOGS/${VM_TASK}.log"
  else
    log "ERROR: ${VM_TASK} task without VM_BACKFILL_CMD metadata"
  fi
elif [[ "$VM_TASK" == "mdps-backfill" || "$VM_TASK" == "features-backfill" || "$VM_TASK" == "phantom-recon" || "$VM_TASK" == "cross-asset-rescan" || "$VM_TASK" == "synthetic-benchmark" ]]; then
  # Phase 5b/5c backfill + phantom-recon (2026-05-07) +
  # cross-asset-rescan (Phase 3.D of
  # manifest_schema_final_gate_2026_05_09, fix shipped 2026-05-11 after
  # `cross-asset-rescan-20260511-153940` failed at `python -m instruments_service`
  # CLI dispatch — no `cross_asset_rescan` operation registered) +
  # synthetic-benchmark (Phase 4.A-tail of mock_data_pipeline_benchmarking_2026_05_10,
  # 2026-05-12: launch-synthetic-benchmark-vm.sh passes the UTL benchmark CLI via
  # VM_BACKFILL_CMD = "python -m unified_trading_library.synthetic --archetype <X>
  # --date-start <date> --date-end <date> --input-uri gs://{pid}-synthetic-input
  # --report-uri gs://{pid}-benchmark-reports --mode stub|subprocess ..."):
  # BACKFILL_CMD metadata carries the full command (e.g. "python /workspace/instruments-service/scripts/
  # reconcile_phantom_manifest_rows_all.py --asset-group defi --dry-run").
  # This route lets one-off-script launchers (phantom-recon,
  # cross-asset-rescan, synthetic-benchmark, future) reuse
  # the workspace tarball-pull + venv setup without bespoke startup scripts.
  VM_BACKFILL_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$VM_BACKFILL_CMD" ]]; then
    FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
    _launch_with_tee "$FULL_CMD" "$LOGS/${VM_TASK}.log"
  else
    log "ERROR: ${VM_TASK} task without VM_BACKFILL_CMD metadata"
  fi
elif [[ "$VM_TASK" == "mtds-backfill" ]]; then
  # Chunked MTDS backfill — Tardis API requires ≤7-day download windows per request
  # (per-IP rate limits; wider windows return 429). VM_CHUNK_DAYS default 7.
  # Writes a self-contained chunk-loop script at boot so _launch_with_tee wraps
  # the full loop (streaming GCS log + heartbeat + self-delete on completion).
  VM_CHUNK_DAYS=$(_meta VM_CHUNK_DAYS 7)
  VM_TIER=$(_meta VM_TIER "")
  [[ -n "$VM_TIER" ]] && log "mtds-backfill architecture tier (informational only, not a CLI flag): $VM_TIER"

  # DeFi fix (data_pipeline_check_mtds_cannot_fetch_defi_2026_07_20.md): the
  # generic `op=download` orchestrator DELIBERATELY skips all 98 DeFi venues
  # ("use collect-* handlers") — DeFi is instrument/subgraph-driven, not a
  # Tardis-style bulk download, so a DeFi `mtds-backfill` shard fetched NOTHING
  # (0 rows, every cell no_parquet) regardless of day/venue. Scope this branch
  # ONLY to VM_ASSET_GROUP=defi (case-insensitive) — every other asset_group
  # (cefi/tradfi/sports/prediction) falls through to the untouched `else` below,
  # byte-identical to the prior single-branch behavior. Solana-protocol venues
  # route to collect-solana-defi (--solana-protocols, lowercased, mirrors the
  # existing VM_TASK=solana-defi-backfill branch incl. --solana-lending-backfill
  # for historical-date support); everything else is EVM DeFi → collect-evm-defi
  # (--venues, same generic flag the non-DeFi path already uses).
  _AG_LOWER_MTDS=$(echo "${VM_ASSET_GROUP:-}" | tr '[:upper:]' '[:lower:]')
  if [[ "$_AG_LOWER_MTDS" == "defi" ]]; then
    case "${VM_VENUE^^}" in
      ORCA | RAYDIUM | KAMINO | PHOENIX | METEORA | LIFINITY | MARINADE | JITO | SOLEND | MARGINFI | SANCTUM | SOLBLAZE | JITORESTAKING)
        BASE_CLI="--operation collect-solana-defi --mode batch --asset-group $VM_ASSET_GROUP --solana-lending-backfill"
        [[ -n "$VM_VENUE" ]] && BASE_CLI="$BASE_CLI --solana-protocols ${VM_VENUE,,}"
        ;;
      *)
        BASE_CLI="--operation collect-evm-defi --mode batch --asset-group $VM_ASSET_GROUP"
        [[ -n "$VM_VENUE" ]] && BASE_CLI="$BASE_CLI --venues $VM_VENUE"
        ;;
    esac
  else
    BASE_CLI="--operation download --mode batch --asset-group $VM_ASSET_GROUP"
    [[ -n "$VM_VENUE" ]] && BASE_CLI="$BASE_CLI --venues $VM_VENUE"
  fi
  # NOTE: VM_TIER is NOT a CLI flag — the MTDS download CLI has no `--tier` option
  # (argparse rejects it: "unrecognized arguments: --tier 1"). "Tier" is an
  # ARCHITECTURE label only (e.g. sports Tier-1 = Odds API), selected by the
  # asset_group + instrument universe (venue=odds_api auto-routed by the
  # orchestrator); the Odds-API paid-plan tier is encoded in the Secret-Manager
  # API key, not passed per-run. VM_TIER stays in metadata for documentation/logs.
  [[ -n "$VM_DATA_TYPES" ]] && BASE_CLI="$BASE_CLI --data-types ${VM_DATA_TYPES//[,;]/ }"
  # --source: REQUIRED for a TradFi OHLCV download (selects fetcher + stamps
  # provenance); the CLI ignores it for non-tradfi venue-fixed runs.
  [[ -n "$VM_SOURCE" ]] && BASE_CLI="$BASE_CLI --source $VM_SOURCE"
  [[ -n "$VM_INSTRUMENT_IDS" ]] && BASE_CLI="$BASE_CLI --instrument-ids ${VM_INSTRUMENT_IDS//[,;]/ }"
  # --league (sports only): the CLI flag takes ONE comma-separated string. Metadata
  # uses ';' between leagues (like VM_INSTRUMENT_IDS above — gcloud
  # --metadata=K=V,K=V splits on ',' at the key level, so a literal comma in the
  # value breaks parsing), converted back to ',' here for the actual CLI arg.
  # Lets a scoped VM target specific leagues (e.g. a Reference/Features league
  # with real odds_api coverage that a full unscoped run would never reach)
  # instead of always fetching every league.
  VM_LEAGUE=$(_meta VM_LEAGUE)
  [[ -n "$VM_LEAGUE" ]] && BASE_CLI="$BASE_CLI --league ${VM_LEAGUE//;/,}"
  [[ "$VM_FORCE" == "true" ]] && BASE_CLI="$BASE_CLI --force"
  # --batch-date-concurrency: OPT-IN, DEFAULT-OFF. The flag is a UTL ServiceCLI
  # addition shipped separately — the deployed UTL on a given tarball may not yet
  # recognize it, and passing it unconditionally would error "unrecognized
  # arguments: --batch-date-concurrency N" and abort the whole chunk loop. Only
  # append when a launch explicitly opts in via VM_BATCH_DATE_CONCURRENCY
  # metadata (a numeric > 1); absent/empty metadata is a no-op — identical CLI to
  # today.
  VM_BATCH_DATE_CONCURRENCY=$(_meta VM_BATCH_DATE_CONCURRENCY)
  [[ -n "$VM_BATCH_DATE_CONCURRENCY" ]] && BASE_CLI="$BASE_CLI --batch-date-concurrency $VM_BATCH_DATE_CONCURRENCY"

  CHUNK_SCRIPT="$WORKSPACE/mtds_chunk_loop.sh"
  cat >"$CHUNK_SCRIPT" <<MTDS_CHUNK_LOOP_EOF
#!/usr/bin/env bash
set -uo pipefail
CHUNKS=\$("$VENV/bin/python" -c "
from datetime import datetime, timedelta
start = datetime.strptime('$VM_START_DATE', '%Y-%m-%d')
end   = datetime.strptime('$VM_END_DATE',   '%Y-%m-%d')
chunk_days = int($VM_CHUNK_DAYS)
cur = start
while cur <= end:
    cend = min(cur + timedelta(days=chunk_days - 1), end)
    print(cur.strftime('%Y-%m-%d') + ' ' + cend.strftime('%Y-%m-%d'))
    cur = cend + timedelta(days=1)
")
TOTAL=\$(echo "\$CHUNKS" | wc -l | tr -d ' ')
CHUNK_NUM=0
echo "\$CHUNKS" | while IFS=' ' read -r CS CE; do
  CHUNK_NUM=\$((CHUNK_NUM + 1))
  echo "--- Chunk \${CHUNK_NUM}/\${TOTAL}: \${CS} → \${CE} ---"
  _CHUNK_LOG_TMP=\$(mktemp)
  CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
    "$VENV/bin/python" -m market_tick_data_service \\
      $BASE_CLI \\
      --start-date "\${CS}" --end-date "\${CE}" 2>&1 | tee "\$_CHUNK_LOG_TMP"
  CHUNK_RC=\${PIPESTATUS[0]}
  # Fail loud instead of silently swallowing a killed/failed chunk (was \`|| true\`
  # with no exit-code capture — a child OOM-kill (exit 137) or any other non-zero
  # exit left no log signal at all beyond staleness, see
  # mtds_backfill_vm_memory_hang_large_chunk_2026_07_22.md). Log a clear, greppable
  # marker either way and CONTINUE to the next chunk (shard-level failure isolation —
  # one bad chunk must not silently wedge the whole multi-chunk run).
  if [[ \$CHUNK_RC -eq 137 ]]; then
    echo "CHUNK_FAILED: chunk=\${CHUNK_NUM}/\${TOTAL} range=\${CS}→\${CE} exit=137 reason=OOM_KILLED time=\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  elif [[ \$CHUNK_RC -ne 0 ]]; then
    echo "CHUNK_FAILED: chunk=\${CHUNK_NUM}/\${TOTAL} range=\${CS}→\${CE} exit=\${CHUNK_RC} reason=NONZERO_EXIT time=\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  fi
  # SPOT resume checkpoint (infra_satellite_ao_dispatch_batch1 P2 PROGRESS.json
  # rollout, spot-vms-for-backfill.md). mtds_chunk_loop.sh re-execs python once
  # PER CHUNK, and manifest_finalize.py's "Manifest updated: date=... total_records=N
  # complete=..." line is the only per-date completion signal this loop can see
  # directly, so scan THIS chunk's own captured output for it and emit a
  # [[VM_PROGRESS]] marker for the latest date with total_records>0 — artifact-gated
  # (never advances past a date whose capture produced zero real rows), mirroring
  # UTL record_vm_progress's own frontier semantics. vm-exec-with-gcs-tee.sh's
  # stall-watchdog already scans run.log for this exact marker and persists
  # vm-logs/{vm}/PROGRESS.json — no wrapper change needed.
  _ckpt_date=""
  while IFS= read -r _ml; do
    _d="\${_ml#*date=}"; _d="\${_d%% *}"
    _tr="\${_ml#*total_records=}"; _tr="\${_tr%% *}"
    [[ "\$_tr" =~ ^[0-9]+\$ ]] || continue
    (( _tr > 0 )) || continue
    if [[ -z "\$_ckpt_date" || "\$_d" > "\$_ckpt_date" ]]; then
      _ckpt_date="\$_d"
    fi
  done < <(grep -a 'Manifest updated: date=' "\$_CHUNK_LOG_TMP" 2>/dev/null)
  [[ -n "\$_ckpt_date" ]] && echo "[[VM_PROGRESS]] last_completed_date=\${_ckpt_date} monotonic=true"
  rm -f "\$_CHUNK_LOG_TMP"
  echo "PROGRESS: chunk=\${CHUNK_NUM}/\${TOTAL} range=\${CS}→\${CE} time=\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
done
echo "mtds-backfill loop complete: \$(date -u)"
MTDS_CHUNK_LOOP_EOF
  chmod +x "$CHUNK_SCRIPT"
  _launch_with_tee "bash $CHUNK_SCRIPT" "$LOGS/mtds-backfill.log"
elif [[ "$VM_TASK" == "cefi-hl-aster-backfill" ]]; then
  # CeFi HYPERLIQUID/ASTER on-chain-perp historical backfill via the dedicated
  # OnchainPerpBatchHandler (--operation collect-onchain-perp-batch). Drives the
  # HyperliquidS3Downloader (requester-pays S3) + AsterAdapter (REST) DIRECTLY,
  # bypassing the orchestrator DeFi-strip that no-ops VM_OPERATION=download for
  # HL/ASTER. Writes cefi canonical parquet + manifest (source=hyperliquid/aster,
  # pipeline_mode=batch_<source>). Day-by-day loop (HL S3 is per-day; no Tardis
  # ≤7d rate-limit applies here, but per-day keeps shard granularity + progress).
  # SSOT: market-tick-data-service OnchainPerpBatchHandler
  # (live_tardis_machine_and_hl_aster_s3_batch_2026_06_21 §2).
  VM_CHUNK_DAYS=$(_meta VM_CHUNK_DAYS 1)
  BASE_CLI="--operation collect-onchain-perp-batch --mode batch --asset-group $VM_ASSET_GROUP"
  [[ -n "$VM_VENUE" ]] && BASE_CLI="$BASE_CLI --venues $VM_VENUE"
  [[ -n "$VM_DATA_TYPES" ]] && BASE_CLI="$BASE_CLI --onchain-perp-data-types ${VM_DATA_TYPES//[,;]/ }"
  [[ -n "$VM_INSTRUMENT_IDS" ]] && BASE_CLI="$BASE_CLI --onchain-perp-symbols ${VM_INSTRUMENT_IDS//[,;]/ }"

  CHUNK_SCRIPT="$WORKSPACE/cefi_hl_aster_loop.sh"
  cat >"$CHUNK_SCRIPT" <<HL_ASTER_LOOP_EOF
#!/usr/bin/env bash
set -uo pipefail
CHUNKS=\$("$VENV/bin/python" -c "
from datetime import datetime, timedelta
start = datetime.strptime('$VM_START_DATE', '%Y-%m-%d')
end   = datetime.strptime('$VM_END_DATE',   '%Y-%m-%d')
chunk_days = int($VM_CHUNK_DAYS)
cur = start
while cur <= end:
    cend = min(cur + timedelta(days=chunk_days - 1), end)
    print(cur.strftime('%Y-%m-%d') + ' ' + cend.strftime('%Y-%m-%d'))
    cur = cend + timedelta(days=1)
")
TOTAL=\$(echo "\$CHUNKS" | wc -l | tr -d ' ')
CHUNK_NUM=0
echo "\$CHUNKS" | while IFS=' ' read -r CS CE; do
  CHUNK_NUM=\$((CHUNK_NUM + 1))
  echo "--- Chunk \${CHUNK_NUM}/\${TOTAL}: \${CS} → \${CE} ---"
  _CHUNK_LOG_TMP=\$(mktemp)
  CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
    "$VENV/bin/python" -m market_tick_data_service \\
      $BASE_CLI \\
      --start-date "\${CS}" --end-date "\${CE}" 2>&1 | tee "\$_CHUNK_LOG_TMP"
  # SPOT resume checkpoint (infra_satellite_ao_dispatch_batch1 P2 PROGRESS.json
  # rollout, spot-vms-for-backfill.md). OnchainPerpBatchHandler._record_captured
  # writes via ManifestWriter.add() (not record_captured()), so the UTL
  # record_vm_progress hook never fires on its own. Scan this chunk's own output
  # for its own completion log ("OnchainPerpBatch: VENUE/data_type/symbol/DATE
  # captured N rows") and emit [[VM_PROGRESS]] for the latest date with N>0 —
  # artifact-gated, same frontier semantics as record_vm_progress itself.
  _ckpt_date=""
  while IFS= read -r _ml; do
    _rest="\${_ml#*OnchainPerpBatch: }"
    _datepart="\${_rest%% captured*}"
    _d="\${_datepart##*/}"
    _cnt="\${_ml#*captured }"; _cnt="\${_cnt%% rows*}"
    [[ "\$_d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\$ && "\$_cnt" =~ ^[0-9]+\$ ]] || continue
    (( _cnt > 0 )) || continue
    if [[ -z "\$_ckpt_date" || "\$_d" > "\$_ckpt_date" ]]; then
      _ckpt_date="\$_d"
    fi
  done < <(grep -a 'OnchainPerpBatch: .* captured .* rows' "\$_CHUNK_LOG_TMP" 2>/dev/null)
  [[ -n "\$_ckpt_date" ]] && echo "[[VM_PROGRESS]] last_completed_date=\${_ckpt_date} monotonic=true"
  rm -f "\$_CHUNK_LOG_TMP"
  echo "PROGRESS: chunk=\${CHUNK_NUM}/\${TOTAL} range=\${CS}→\${CE} time=\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
done
echo "cefi-hl-aster-backfill loop complete: \$(date -u)"
HL_ASTER_LOOP_EOF
  chmod +x "$CHUNK_SCRIPT"
  _launch_with_tee "bash $CHUNK_SCRIPT" "$LOGS/cefi-hl-aster-backfill.log"
elif [[ "$VM_TASK" == "instruments-backfill" ]]; then
  # Chunked instruments-service backfill. VM_CHUNK_DAYS default 30 (no strict
  # Tardis rate limit, but wide windows can exhaust per-API-key quotas for
  # football/odds providers). All optional flags follow generic handler convention.
  VM_CHUNK_DAYS=$(_meta VM_CHUNK_DAYS 30)

  BASE_CLI="--operation instruments --mode batch --asset-group $VM_ASSET_GROUP"
  [[ -n "$VM_VENUE" ]] && BASE_CLI="$BASE_CLI --venues $VM_VENUE"
  [[ -n "$VM_SPORTS_PROVIDER" ]] && BASE_CLI="$BASE_CLI --sports-provider $VM_SPORTS_PROVIDER"
  [[ -n "$VM_SPORTS_ENTITY" ]] && BASE_CLI="$BASE_CLI --sports-entity $VM_SPORTS_ENTITY"
  [[ -n "$VM_DATA_TYPES" ]] && BASE_CLI="$BASE_CLI --data-types ${VM_DATA_TYPES//[,;]/ }"
  [[ "$VM_FORCE" == "true" ]] && BASE_CLI="$BASE_CLI --force"

  CHUNK_SCRIPT="$WORKSPACE/instruments_chunk_loop.sh"
  cat >"$CHUNK_SCRIPT" <<INSTR_CHUNK_LOOP_EOF
#!/usr/bin/env bash
set -uo pipefail
CHUNKS=\$("$VENV/bin/python" -c "
from datetime import datetime, timedelta
start = datetime.strptime('$VM_START_DATE', '%Y-%m-%d')
end   = datetime.strptime('$VM_END_DATE',   '%Y-%m-%d')
chunk_days = int($VM_CHUNK_DAYS)
cur = start
while cur <= end:
    cend = min(cur + timedelta(days=chunk_days - 1), end)
    print(cur.strftime('%Y-%m-%d') + ' ' + cend.strftime('%Y-%m-%d'))
    cur = cend + timedelta(days=1)
")
TOTAL=\$(echo "\$CHUNKS" | wc -l | tr -d ' ')
CHUNK_NUM=0
# Bounded per-chunk retry: a chunk's process dying mid-range (OOM/signal) must
# retry the SAME range (skip-if-fresh resumes past whatever it already
# captured) rather than silently advancing to the next chunk's start — the
# prior bare "|| true" swallowed every such failure and left a permanent,
# undetected date-range gap (see
# plans/active/issues/per_vm_shard_growth_oom_long_running_backfills_2026_07_27.md).
CHUNK_MAX_ATTEMPTS=4
echo "\$CHUNKS" | while IFS=' ' read -r CS CE; do
  CHUNK_NUM=\$((CHUNK_NUM + 1))
  echo "--- Chunk \${CHUNK_NUM}/\${TOTAL}: \${CS} → \${CE} ---"
  ATTEMPT=0
  RC=1
  while [[ \$ATTEMPT -lt \$CHUNK_MAX_ATTEMPTS ]]; do
    ATTEMPT=\$((ATTEMPT + 1))
    # Per-chunk VM_NAME suffix (subprocess-scoped only — the outer VM_NAME the
    # tee-wrapper/heartbeat use for vm-logs/PROGRESS.json is untouched) bounds
    # unified-trading-library ManifestWriter's per-VM shard
    # (_index/per_vm/{VM_NAME}.parquet) to just THIS chunk's rows instead of
    # accumulating the whole multi-year backfill into one ever-growing shard —
    # the confirmed root cause of the repeated OOM-kills (same issue doc above;
    # the manifest consolidator merges every per_vm/*.parquet shard generically
    # regardless of naming, and prunes on mtime/generation, never VM-liveness
    # name-matching, so this is safe).
    VM_NAME="\${VM_NAME}-c\${CHUNK_NUM}" CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      "$VENV/bin/python" -m instruments_service \\
        $BASE_CLI \\
        --start-date "\${CS}" --end-date "\${CE}" 2>&1
    RC=\$?
    if [[ \$RC -eq 0 ]]; then
      break
    fi
    echo "CHUNK_RETRY chunk=\${CHUNK_NUM}/\${TOTAL} attempt=\${ATTEMPT}/\${CHUNK_MAX_ATTEMPTS} rc=\${RC} range=\${CS}→\${CE} — process died, retrying same range (skip-if-fresh resumes past captured dates)"
  done
  if [[ \$RC -ne 0 ]]; then
    echo "CHUNK_EXHAUSTED chunk=\${CHUNK_NUM}/\${TOTAL} range=\${CS}→\${CE} after \${CHUNK_MAX_ATTEMPTS} attempts (rc=\${RC}) — moving on; this range may be INCOMPLETE"
  else
    # SPOT resume checkpoint (infra_satellite_ao_dispatch_batch1 P2 PROGRESS.json
    # rollout, spot-vms-for-backfill.md). instruments-service's manifest writes
    # (manifest.record_captured() calls scattered per-league/per-provider) have no
    # single per-date completion summary this loop can grep the way MTDS's
    # "Manifest updated: date=..." line allows, so this checkpoint is coarser:
    # mark the chunk's END date only once the chunk's own retry loop reports
    # RC==0 (the CLI ran the whole [CS,CE] window to completion without dying).
    # A resume that blindly restarts from an earlier date still costs only a
    # cheap presence-skip re-scan, never a re-fetch — the same bounded-risk
    # reasoning already accepted for canonical-migration-defi-rebuild.
    echo "[[VM_PROGRESS]] last_completed_date=\${CE} monotonic=true"
  fi
  echo "PROGRESS: chunk=\${CHUNK_NUM}/\${TOTAL} range=\${CS}→\${CE} time=\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
done
echo "instruments-backfill loop complete: \$(date -u)"
INSTR_CHUNK_LOOP_EOF
  chmod +x "$CHUNK_SCRIPT"
  _launch_with_tee "bash $CHUNK_SCRIPT" "$LOGS/instruments-backfill.log"
elif [[ "$VM_TASK" == "solana-defi-backfill" ]]; then
  # Multi-protocol Solana DeFi backfill (collect-solana-defi op).
  # VM_SOLANA_PROTOCOLS uses ';' as separator (gcloud metadata uses ',' for
  # key separator).  Empty = handler default (all 10 protocols).
  CLI_ARGS="--operation collect-solana-defi --mode batch --asset-group $VM_ASSET_GROUP"
  [[ -n "$VM_START_DATE" ]] && CLI_ARGS="$CLI_ARGS --start-date $VM_START_DATE"
  [[ -n "$VM_END_DATE" ]] && CLI_ARGS="$CLI_ARGS --end-date $VM_END_DATE"
  VM_SOLANA_PROTOCOLS=$(_meta VM_SOLANA_PROTOCOLS)
  if [[ -n "$VM_SOLANA_PROTOCOLS" ]]; then
    CLI_ARGS="$CLI_ARGS --solana-protocols ${VM_SOLANA_PROTOCOLS//[,;]/ }"
  fi
  # --solana-lending-backfill enables historical-aware DeFiLlama paths for
  # marginfi (api.llama.fi/protocol/marginfi) + solend (yields.llama.fi/chart/{pool_id}).
  # Required for any historical date.
  CLI_ARGS="$CLI_ARGS --solana-lending-backfill"
  _launch_with_tee "$VENV/bin/python -m $VM_SERVICE $CLI_ARGS" "$LOGS/solana-defi-backfill.log"
elif [[ "$VM_TASK" == "solana-gas-backfill" ]]; then
  export GAS_FEE_SOLANA=true
  CLI_ARGS="--operation collect-gas-fees --mode batch --asset-group $VM_ASSET_GROUP"
  [[ -n "$VM_START_DATE" ]] && CLI_ARGS="$CLI_ARGS --start-date $VM_START_DATE"
  [[ -n "$VM_END_DATE" ]] && CLI_ARGS="$CLI_ARGS --end-date $VM_END_DATE"
  # Bug-G fix 2026-05-29: Solana doesn't have an EVM numeric chain_id. The
  # gas_fees handler accepts the sentinel string "solana" via --gas-fee-chains
  # and routes the entry through its _collect_solana_historical branch
  # (SolanaGasFeeClient → getBlock historical sampling). Previously this
  # passed 99999 → handler logged "Unknown chain_id 99999, skipping" for every
  # date, producing 0 rows.
  CLI_ARGS="$CLI_ARGS --gas-fee-chains solana"
  _launch_with_tee "$VENV/bin/python -m $VM_SERVICE $CLI_ARGS" "$LOGS/solana-gas-backfill.log"
elif [[ "$VM_TASK" == "alerting-quietness-baseline" ]]; then
  # Phase 7 of alerting_service_live_rules_2026_05_07: 48h quietness run.
  # PagerDuty disabled; all alerts route to Telegram staging-noise channel only.
  # VM_SERVICE must be alerting_service for tarball install to work.
  VM_SERVICE="alerting_service"
  _DURATION="${VM_DURATION_HOURS:-48}"
  # alerting_service uses Pydantic settings — quietness flags are env vars, not CLI args.
  # Fetch Telegram credentials from Secret Manager metadata keys.
  _TG_TOKEN_SECRET=$(_meta TELEGRAM_BOT_TOKEN_SECRET)
  _TG_CHAT_SECRET=$(_meta TELEGRAM_CHAT_ID_SECRET)
  if [[ -n "$_TG_TOKEN_SECRET" ]]; then
    export TELEGRAM_BOT_TOKEN
    TELEGRAM_BOT_TOKEN=$(gcloud secrets versions access latest --secret="$_TG_TOKEN_SECRET" --project="$(_meta PROJECT_ID central-element-323112)" 2>/dev/null || echo "")
  fi
  if [[ -n "$_TG_CHAT_SECRET" ]]; then
    export TELEGRAM_CHAT_ID
    TELEGRAM_CHAT_ID=$(gcloud secrets versions access latest --secret="$_TG_CHAT_SECRET" --project="$(_meta PROJECT_ID central-element-323112)" 2>/dev/null || echo "")
  fi
  export QUIETNESS_BASELINE_MODE=true
  export PAGERDUTY_DISABLED=true
  export RUN_DURATION_HOURS="$_DURATION"
  _launch_with_tee "$VENV/bin/python -m alerting_service --mode live" "$LOGS/alerting-quietness.log"
elif [[ "$VM_TASK" == "cefi-durability-force-converge" ]]; then
  # 2026-07-10 — launch-cefi-durability-force-converge-vm.sh. VM_BACKFILL_CMD
  # carries the full cefi_durability_force_converge_2026_07_10.py invocation
  # (--quarantine-backups / --fix-by-date / --apply / --workers all controlled
  # host-side by the launcher). GCP_PROJECT_ID is already exported above.
  # Self-deletes on completion via VM_SHUTDOWN_ON_COMPLETION (always set by
  # the launcher).
  VM_BACKFILL_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$VM_BACKFILL_CMD" ]]; then
    FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/instruments" || { log "ERROR: $WORKSPACE/instruments missing"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/cefi-durability-force-converge.log"
  else
    log "ERROR: cefi-durability-force-converge task without VM_BACKFILL_CMD metadata"
  fi
elif [[ "$VM_TASK" == "expected-universe-v2" ]]; then
  # 2026-07-10 — launch-expected-universe-v2-vm.sh's ad-hoc one-shot / full-history
  # backfill path. VM_BACKFILL_CMD carries the full enumerate_expected_universe.py
  # --enumerator-version v2 invocation (asset-group/catalog-path/apply-write/
  # max-writes-per-run all controlled host-side by the launcher). Found broken
  # 2026-07-10 (real DeFi backlog launch crashed: VM_OPERATION="expected-universe-v2"
  # fell through to the generic --operation dispatch below, which has no such CLI
  # choice) — this branch was simply missing, so VM_BACKFILL_CMD was always ignored
  # for this task. Self-deletes on completion via the launcher-attached
  # shutdown-script (VM_SHUTDOWN_ON_COMPLETION=true, always set by the launcher).
  VM_BACKFILL_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$VM_BACKFILL_CMD" ]]; then
    FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/instruments" || { log "ERROR: $WORKSPACE/instruments missing"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/expected-universe-v2.log"
  else
    log "ERROR: expected-universe-v2 task without VM_BACKFILL_CMD metadata"
  fi
elif [[ "$VM_TASK" == "qg-snapshot" ]]; then
  # B-018 Phase 4.A: daily QG snapshot → GCS parquet.
  # Runs snapshot.sh piped into snapshot_to_parquet.py then self-deletes.
  VM_BACKFILL_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$VM_BACKFILL_CMD" ]]; then
    _VM_ZONE=$(curl -sf -H "Metadata-Flavor: Google" \
      "http://metadata.google.internal/computeMetadata/v1/instance/zone" | awk -F/ '{print $NF}')
    _SELF_DELETE="gcloud compute instances delete '$VM_NAME_SELF' --zone='$_VM_ZONE' --quiet 2>&1 || log 'WARNING: qg-snapshot VM self-delete failed'"
    # Rewrite bare `python ` -> `$VENV/bin/python ` (matches strategy-backtest-grid /
    # synthetic-benchmark branches above) — snapshot_to_parquet.py needs
    # unified_trading_library + pyarrow, which only live in $VENV, not system python3.
    FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
    _launch_with_tee "$FULL_CMD; $_SELF_DELETE" "$LOGS/qg-snapshot.log"
  else
    log "ERROR: qg-snapshot task without VM_BACKFILL_CMD metadata"
  fi
elif [[ "$VM_TASK" == "prediction-arb-detect" ]]; then
  # Live cross-venue (Kalshi ↔ Polymarket) prediction arb DETECTOR (paper mode).
  # Runs the features-service cross_instrument CLI as a long-lived poll loop that
  # reads both venues' live book_snapshot_5, flags PURE_ARB / QUOTABLE_ARB, and
  # streams opportunities to the GCS arb store. Launcher: launch-prediction-arb-detector.sh.
  # Design SSOT: codex/04-architecture/cross-venue-prediction-arb-detection.md.
  _ARB_SCAN_DAYS=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_ARB_SCAN_DAYS" || echo "")
  _ARB_POLL=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_ARB_POLL_INTERVAL_SECONDS" || echo "")
  _ARB_MAX_DUR=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_ARB_MAX_DURATION_SECONDS" || echo "")
  _ARB_THRESH=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_ARB_ENTRY_THRESHOLD" || echo "")
  ARB_ARGS="--operation arb-detect --mode ${VM_MODE_LIVE:-live} --asset-group ${VM_ASSET_GROUP:-PREDICTION}"
  [[ -n "$_ARB_SCAN_DAYS" ]] && ARB_ARGS="$ARB_ARGS --scan-days $_ARB_SCAN_DAYS"
  [[ -n "$_ARB_POLL" ]] && ARB_ARGS="$ARB_ARGS --poll-interval-seconds $_ARB_POLL"
  [[ -n "$_ARB_MAX_DUR" ]] && ARB_ARGS="$ARB_ARGS --max-duration-seconds $_ARB_MAX_DUR"
  [[ -n "$_ARB_THRESH" ]] && ARB_ARGS="$ARB_ARGS --entry-threshold $_ARB_THRESH"
  cd "$WORKSPACE/features" || { log "ERROR: $WORKSPACE/features missing — features-service tarball not extracted"; exit 1; }
  _launch_with_tee "$VENV/bin/python -m features_service.cross_instrument $ARB_ARGS" "$LOGS/arb-detect.log"
elif [[ "$VM_TASK" == "datapoint-validation" ]]; then
  # Tier-2 per-datapoint validation (id + schema) — launch-datapoint-validation-vm.sh
  # prepares the correct validate_datapoint_schema_id.py invocation in VM_BACKFILL_CMD.
  # Found 2026-07-21 (first real launch-run): this VM_TASK had NO dispatch branch here,
  # so it fell through to the generic `elif [ -n "$VM_TASK" ]` fallback below, which built
  # `--operation datapoint-validation` literally — instruments-service's CLI has no such
  # --operation choice (only `instruments`), an immediate argparse crash (rc=2) before any
  # validation ever ran. Same root-cause class as the 2026-07-12 sports-v9-migration and
  # 2026-07-13 defi-paper VM_TASK gaps above — a new launcher's VM_TASK needs its own
  # dispatch branch here even when all it does is run VM_BACKFILL_CMD as-is.
  VM_BACKFILL_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$VM_BACKFILL_CMD" ]]; then
    FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/instruments" || { log "ERROR: $WORKSPACE/instruments missing — instruments-service tarball not extracted"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/datapoint-validation.log"
  else
    log "ERROR: datapoint-validation task without VM_BACKFILL_CMD metadata"
  fi
elif [[ "$VM_TASK" == "orphan-sweep" ]]; then
  # GCS→manifest orphan sweep — launch-orphan-sweep-vm.sh prepares the correct
  # migration_orphan_sweep.py invocation in VM_BACKFILL_CMD (same VM_BACKFILL_CMD
  # dispatch shape as datapoint-validation above). Found 2026-07-22 (first real
  # launch-run, all 4 asset_groups): this VM_TASK had NO dispatch branch here either
  # — same root-cause class as the datapoint-validation gap one block up (all 4 VMs
  # crashed rc=2 within ~3 minutes on the generic `--operation orphan-sweep` fallback).
  VM_BACKFILL_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$VM_BACKFILL_CMD" ]]; then
    FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/instruments" || { log "ERROR: $WORKSPACE/instruments missing — instruments-service tarball not extracted"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/orphan-sweep.log"
  else
    log "ERROR: orphan-sweep task without VM_BACKFILL_CMD metadata"
  fi
elif [[ "$VM_TASK" == "backfill-orphan-e" ]]; then
  # class-E orphan record_captured backfill — launch-backfill-orphan-e-vm.sh prepares
  # the correct backfill_orphan_class_e.py invocation in VM_BACKFILL_CMD (same
  # VM_BACKFILL_CMD dispatch shape as orphan-sweep above — sibling launcher, added
  # 2026-07-22 so this VM_TASK gets its OWN branch from day one rather than repeating
  # the recurring no-dispatch-branch bug class documented on orphan-sweep above).
  VM_BACKFILL_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$VM_BACKFILL_CMD" ]]; then
    FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/instruments" || { log "ERROR: $WORKSPACE/instruments missing — instruments-service tarball not extracted"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/backfill-orphan-e.log"
  else
    log "ERROR: backfill-orphan-e task without VM_BACKFILL_CMD metadata"
  fi
elif [[ "$VM_TASK" == "backfill-candle-manifest" ]]; then
  # candle-corpus class-E/F record_captured backfill —
  # launch-backfill-candle-manifest-vm.sh prepares the correct
  # backfill_candle_manifest.py invocation in VM_BACKFILL_CMD (same
  # VM_BACKFILL_CMD dispatch shape as backfill-orphan-e above — sibling
  # launcher, added 2026-07-27 so this VM_TASK gets its OWN branch from day
  # one rather than repeating the recurring no-dispatch-branch bug class
  # documented on orphan-sweep/backfill-orphan-e above). Unlike those two
  # (instruments-service), the target script lives in market-data-processing-
  # service's mdps workspace dir.
  VM_BACKFILL_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$VM_BACKFILL_CMD" ]]; then
    FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/mdps" || { log "ERROR: $WORKSPACE/mdps missing — market-data-processing-service tarball not extracted"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/backfill-candle-manifest.log"
  else
    log "ERROR: backfill-candle-manifest task without VM_BACKFILL_CMD metadata"
  fi
elif [[ "$VM_TASK" == "sports-derived-features-census" ]]; then
  # Sports derived_features post-floor residue census —
  # launch-sports-derived-features-census-vm.sh prepares the correct
  # purge_sports_derived_features_post_floor_residue_2026_07_27.py invocation
  # (no --apply) in VM_BACKFILL_CMD (same VM_BACKFILL_CMD dispatch shape as
  # datapoint-validation/orphan-sweep above). Found 2026-07-27 (first real
  # launch-run): this VM_TASK had NO dispatch branch here — same root-cause
  # class as those two (VM self-deleted rc=1 within ~3 minutes on the generic
  # fallback guard below, before the census ever ran). Unlike instruments-
  # service tasks above, the target script lives in features-service's
  # workspace dir.
  VM_BACKFILL_CMD=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$VM_BACKFILL_CMD" ]]; then
    FULL_CMD="${VM_BACKFILL_CMD/python /$VENV/bin/python }"
    cd "$WORKSPACE/features" || { log "ERROR: $WORKSPACE/features missing — features-service tarball not extracted"; exit 1; }
    _launch_with_tee "$FULL_CMD" "$LOGS/sports-derived-features-census.log"
  else
    log "ERROR: sports-derived-features-census task without VM_BACKFILL_CMD metadata"
  fi
elif [ -n "$VM_TASK" ]; then
  # GUARD (added after the 3rd occurrence of this exact bug class: 2026-07-12
  # sports-v9-migration, 2026-07-13 defi-paper, 2026-07-21 datapoint-validation —
  # see issues/datapoint_validation_results_bucket_missing_2026_07_21.md todo 4).
  # VM_BACKFILL_CMD is ONLY ever set by launchers whose VM_TASK has its own dedicated
  # dispatch branch above (each of those branches curls it and runs it directly) — so
  # reaching this generic fallback with VM_BACKFILL_CMD metadata present is BY
  # CONSTRUCTION a missing-dispatch-branch bug: this VM_TASK's launcher prepared a
  # specific command for the VM to run, but no `elif [[ "$VM_TASK" == "..." ]]` exists
  # to route to it, so this generic branch would silently ignore VM_BACKFILL_CMD and
  # build an unrelated `--operation $VM_OPERATION` invocation instead — which then
  # crashes minutes later, deep inside a task-specific CLI's argparse, with no signal
  # pointing back at the real cause. Fail loud and immediately instead.
  _VM_BACKFILL_CMD_PRESENT=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_BACKFILL_CMD" || echo "")
  if [[ -n "$_VM_BACKFILL_CMD_PRESENT" ]]; then
    log "ERROR: VM_TASK=${VM_TASK} has no dedicated dispatch branch in this script, but VM_BACKFILL_CMD metadata IS present (${_VM_BACKFILL_CMD_PRESENT:0:120}...). This launcher expects VM_BACKFILL_CMD to be run directly — add an 'elif [[ \"\$VM_TASK\" == \"${VM_TASK}\" ]]' branch here that curls VM_BACKFILL_CMD and runs it via _launch_with_tee (mirror the datapoint-validation/orphan-sweep branches above). Refusing to fall through to the generic --operation dispatch, which would silently ignore VM_BACKFILL_CMD and crash deep in an unrelated CLI's argparse."
    exit 1
  fi
  _OP="$VM_OPERATION"
  # Translate metadata op name → CLI op name for live mode.
  [[ "$_OP" == "live_websocket" ]] && _OP="websocket-streaming"
  if [[ "$VM_SERVICE" == "instruments_service" && "$_OP" == "download" ]]; then
    _OP="instruments"
  fi
  # Live websocket mode: install Redis locally as the streaming pipeline backbone.
  # websocket-streaming handler requires MTDS_STREAMING_REDIS_URL for the
  # Redis Stream that MDPS consumes (CandleBoundaryCrossedEvent).
  if [[ "${VM_OPERATION:-}" == "live_websocket" ]]; then
    log "live_websocket: installing Redis for websocket-streaming pipeline..."
    apt-get install -y -qq redis-server
    systemctl start redis-server 2>/dev/null || service redis-server start 2>/dev/null || true
    export MTDS_STREAMING_REDIS_URL="redis://127.0.0.1:6379"
    log "Redis started; MTDS_STREAMING_REDIS_URL=redis://127.0.0.1:6379"
  fi
  # tardis-machine live source: install Node + the tardis-machine sidecar and
  # start its stream-normalized endpoint (FREE — no API key for live). MTDS then
  # connects to ws://localhost:8002/ws-stream-normalized for uniform normalised
  # trade/book_snapshot_5/derivative_ticker. Default native source needs none of this.
  if [[ "${VM_OPERATION:-}" == "live_websocket" && "${VM_LIVE_SOURCE:-native}" == "tardis-machine" ]]; then
    log "live_websocket: VM_LIVE_SOURCE=tardis-machine — installing Node + tardis-machine sidecar..."
    if ! command -v node >/dev/null 2>&1; then
      curl -fsSL https://deb.nodesource.com/setup_20.x | bash - >/dev/null 2>&1 || true
      apt-get install -y -qq nodejs
    fi
    npm install -g tardis-machine >/dev/null 2>&1 || npm install -g tardis-machine
    # stream-normalized needs no API key; TM_API_KEY left unset for the free live endpoint.
    nohup tardis-machine --port 8001 > "$LOGS/tardis-machine.log" 2>&1 &
    sleep 3
    export MTDS_TARDIS_MACHINE_WS_URL="ws://localhost:8002/ws-stream-normalized"
    export MTDS_LIVE_SOURCE="tardis-machine"
    log "tardis-machine started: HTTP :8001 / WS :8002; MTDS_TARDIS_MACHINE_WS_URL=$MTDS_TARDIS_MACHINE_WS_URL"
  fi
  # Use VM_MODE_LIVE (set by live launchers via VM_MODE metadata) when present;
  # fall back to batch for all historical/backfill launchers.
  _MODE="${VM_MODE_LIVE:-batch}"
  # Multi-process fan-out gate (TIGHT: only a batch MTDS download with VM_NUM_WORKERS>1
  # and >=2 venues). Nothing else in this shared generic branch is affected, and the
  # default VM_NUM_WORKERS=1 makes this a no-op for every existing launcher.
  _FANOUT=0; _NW=1
  if [[ "$VM_SERVICE" == "market_tick_data_service" && "$_OP" == "download" && "$_MODE" == "batch" && "${VM_NUM_WORKERS:-1}" -gt 1 && -n "$VM_VENUE" ]]; then
    _NVEN=$(echo $VM_VENUE | wc -w)
    # Ceiling on workers (default 4, override VM_MAX_WORKERS). Aggregate sockets = N x
    # per-process (~15) and RAM = N x ~7-8GB, and per-process Tardis caps are NOT divided
    # by N — so an unbounded N would silently approach the ~100-200 socket 403 band and
    # 128GB RAM. N=2-3 is the measured sweet spot (38-57 sockets); 4 leaves headroom.
    _MAXW="${VM_MAX_WORKERS:-4}"
    _NW=$(( VM_NUM_WORKERS < _NVEN ? VM_NUM_WORKERS : _NVEN ))
    if [[ "$_NW" -gt "$_MAXW" ]]; then
      log "cefi fan-out: clamping workers $_NW -> $_MAXW (VM_MAX_WORKERS; keeps aggregate sockets/RAM in the safe band)"
      _NW="$_MAXW"
    fi
    if [[ "$_NW" -gt 1 ]]; then
      _FANOUT=1
      log "cefi fan-out: $_NW worker process(es) over $_NVEN venues (VM_NUM_WORKERS=$VM_NUM_WORKERS)"
      # Fan-out + the single-IP Tardis LEASE is a footgun: each process is a distinct lease
      # holder, so one wins and the other N-1 block up to 1800s then fail-open — a ~30min
      # startup stall per extra worker, for zero benefit (a single IP never 403s itself).
      if [[ "${TARDIS_CONCURRENCY_LEASE:-}" == "1" ]]; then
        log "cefi fan-out WARNING: TARDIS_CONCURRENCY_LEASE=1 with $_NW workers — the N processes will SERIALISE on the single-IP lease (~1800s stall each). The lease is redundant on one VM; leave it OFF for fan-out."
      fi
    fi
  fi
  CLI_ARGS="--operation $_OP --mode $_MODE --asset-group $VM_ASSET_GROUP"
  [[ -n "$VM_VENUE" && "$_FANOUT" != "1" ]] && CLI_ARGS="$CLI_ARGS --venues $VM_VENUE"  # fan-out: per-worker --venues added below
  [[ -n "$VM_START_DATE" ]] && CLI_ARGS="$CLI_ARGS --start-date $VM_START_DATE"
  [[ -n "$VM_END_DATE" ]] && CLI_ARGS="$CLI_ARGS --end-date $VM_END_DATE"
  [[ -n "$VM_LOOKBACK_DAYS" ]] && CLI_ARGS="$CLI_ARGS --lookback-days $VM_LOOKBACK_DAYS"
  [[ -n "$VM_LOOKAHEAD_DAYS" ]] && CLI_ARGS="$CLI_ARGS --lookahead-days $VM_LOOKAHEAD_DAYS"
  [[ "$VM_FORCE_WINDOW" == "true" ]] && CLI_ARGS="$CLI_ARGS --force-window"
  [[ "$VM_FORCE" == "true" ]] && CLI_ARGS="$CLI_ARGS --force"
  [[ -n "$VM_SPORTS_PROVIDER" ]] && CLI_ARGS="$CLI_ARGS --sports-provider $VM_SPORTS_PROVIDER"
  [[ -n "$VM_SPORTS_ENTITY" ]] && CLI_ARGS="$CLI_ARGS --sports-entity $VM_SPORTS_ENTITY"
  [[ -n "$VM_RECOVERY_FIXTURE_IDS" ]] && CLI_ARGS="$CLI_ARGS --recovery-fixture-ids $VM_RECOVERY_FIXTURE_IDS"
  [[ -n "$VM_STRATEGY" ]] && CLI_ARGS="$CLI_ARGS --strategy $VM_STRATEGY"
  [[ -n "$VM_LENDING_PROTOCOLS" ]] && CLI_ARGS="$CLI_ARGS --lending-protocols ${VM_LENDING_PROTOCOLS//[,;]/ }"
  [[ -n "$VM_DEX_POOLS_PROTOCOLS" ]] && CLI_ARGS="$CLI_ARGS --dex-pools-protocols ${VM_DEX_POOLS_PROTOCOLS//[,;]/ }"
  [[ -n "$VM_DEX_SWAPS_PROTOCOLS" ]] && CLI_ARGS="$CLI_ARGS --dex-swaps-protocols ${VM_DEX_SWAPS_PROTOCOLS//[,;]/ }"
  # CLI expects nargs='+' → space-separated. Metadata values arrive with
  # semicolons (see VM_INSTRUMENT_IDS comment above) to avoid collision with
  # gcloud's comma key-separator. Transform semicolons → spaces. VM_DATA_TYPES
  # historically used commas but we harmonise both on ; going forward; the
  # //,/ fallback keeps older launchers working.
  [[ -n "$VM_DATA_TYPES" ]] && CLI_ARGS="$CLI_ARGS --data-types ${VM_DATA_TYPES//[,;]/ }"
  # VM_SHARD_SPEC: required for websocket-streaming ("asset_group:venue:data_type").
  [[ -n "$VM_SHARD_SPEC" ]] && CLI_ARGS="$CLI_ARGS --shard-spec ${VM_SHARD_SPEC//[,;]/ }"
  # VM_LIVE_SOURCE: live source selector (native | tardis-machine). Only meaningful
  # for websocket-streaming; native is the CLI default so pass only when non-native.
  [[ -n "$VM_LIVE_SOURCE" && "$VM_LIVE_SOURCE" != "native" ]] && CLI_ARGS="$CLI_ARGS --live-source $VM_LIVE_SOURCE"
  [[ -n "$VM_INSTRUMENT_IDS" ]] && CLI_ARGS="$CLI_ARGS --instrument-ids ${VM_INSTRUMENT_IDS//[,;]/ }"
  [[ -n "$VM_GAS_FEE_CHAINS" ]] && CLI_ARGS="$CLI_ARGS --gas-fee-chains $VM_GAS_FEE_CHAINS"
  [[ -n "$VM_GAS_FEE_SAMPLE_INTERVAL" ]] && CLI_ARGS="$CLI_ARGS --gas-fee-sample-interval $VM_GAS_FEE_SAMPLE_INTERVAL"
  # VM_MAX_DURATION_SECONDS: bounds a live_websocket run (launch-mtds-live.sh --test-run
  # --max-duration-seconds N) so it actually terminates instead of running forever — only
  # meaningful for websocket-streaming; a real (non-test) live producer never sets this.
  [[ -n "$VM_MAX_DURATION_SECONDS" ]] && CLI_ARGS="$CLI_ARGS --max-duration-seconds $VM_MAX_DURATION_SECONDS"
  _LAUNCH_LOG="$LOGS/backfill.log"
  [[ "${VM_OPERATION:-}" == "live_websocket" ]] && _LAUNCH_LOG="$LOGS/live.log"
  if [[ "$_FANOUT" == "1" ]]; then
    # Write a self-contained fan-out supervisor and hand it to _launch_with_tee as the SINGLE
    # cmd. The tee wrapper waits on this one PID; the supervisor backgrounds N workers, waits
    # each, ORs their REAL exit codes and exit $AGG — so a dead worker propagates FAILED
    # rather than a false COMPLETED self-delete (the `|| true` BUG-4 in the mtds-backfill loop
    # must NOT be copied here). Each worker gets VM_NAME=<vm>-pK (distinct manifest shard,
    # load-bearing — a shared VM_NAME would clobber the per-VM shard) and a disjoint
    # round-robin --venues slice (round-robin separates the -SPOT/-FUTURES pairs so the big
    # BINANCE-FUTURES/BINANCE-SPOT land in different groups). Workers write to the SHARED
    # stdout so their 'uploaded' lines still reset the tee wrapper's stall watchdog.
    _FANOUT_SCRIPT="$WORKSPACE/cefi_fanout.sh"
    {
      echo '#!/usr/bin/env bash'
      echo 'set -uo pipefail  # NOT -e: a false `[[ rc -ne 0 ]] &&` must not exit the supervisor'
      echo 'PIDS=()'
      for (( _k=0; _k<_NW; _k++ )); do
        _slice=""; _i=0
        for _v in $VM_VENUE; do (( _i % _NW == _k )) && _slice="$_slice $_v"; _i=$((_i+1)); done
        _slice="${_slice# }"
        [[ -z "$_slice" ]] && continue
        printf 'echo "[fanout] worker %s (VM_NAME=%s-p%s) venues: %s"\n' "$_k" "$VM_NAME_SELF" "$_k" "$_slice"
        printf 'VM_NAME="%s-p%s" "%s" -m "%s" %s --venues %s &\n' "$VM_NAME_SELF" "$_k" "$VENV/bin/python" "$VM_SERVICE" "$CLI_ARGS" "$_slice"
        echo 'PIDS+=($!)'
      done
      echo 'AGG=0'
      echo 'for _pid in "${PIDS[@]}"; do wait "$_pid"; _rc=$?; echo "[fanout] pid=$_pid rc=$_rc"; [[ $_rc -ne 0 ]] && AGG=$_rc; done'
      echo 'echo "[fanout] all ${#PIDS[@]} worker(s) done aggregate_rc=$AGG"'
      echo 'exit $AGG'
    } > "$_FANOUT_SCRIPT"
    chmod +x "$_FANOUT_SCRIPT"
    _launch_with_tee "bash $_FANOUT_SCRIPT" "$_LAUNCH_LOG"
  else
    _launch_with_tee "$VENV/bin/python -m $VM_SERVICE $CLI_ARGS" "$_LAUNCH_LOG"
  fi
else
  log "No VM_TASK metadata — setup complete, ready for manual launch"
fi

log "=== VM setup complete ==="
echo "READY" > /tmp/vm_ready
