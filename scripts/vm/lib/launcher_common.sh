#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# VM launcher shared library — source this file from launch-*.sh scripts.
#
# Provides:
#   lc_validate_env <env>                  — validate prod|staging|dev; exit 1 on bad value
#   lc_tier_service_account <env> <project> [test_run=false]
#                                          — emit the per-tier runtime SA email for <env>
#                                            (uts-prd-sa for prod, uts-test-sa for staging/dev);
#                                            test_run=true FORCES uts-test-sa regardless of <env>
#                                            (a --test-run/IS_TEST_RUN launch routes GCS writes to
#                                            the -test- bucket sibling, which uts-prd-sa's IAM
#                                            condition does NOT cover — see DP-VM-002 fix note on
#                                            the function itself); env-overridable via LC_RUNTIME_SA=
#                                            for an instant revert, mirrors
#                                            scripts/cloud-run/deploy-shared.sh's RUNTIME_SA=
#                                            pattern (bucket_iam_write_protection_per_tier P2.2d)
#   lc_singleton_check <prefix> <zone> <project> [force=false]
#                                          — refuse duplicate launch unless force=true
#   lc_acquire_singleton_lock <lock_key> <project> [ttl_seconds=300] [force=false]
#                                          — ATOMIC GCS create-if-absent mutex closing
#                                            the TOCTOU window lc_singleton_check's own
#                                            list-then-create pattern cannot close on
#                                            its own (see the function docstring below
#                                            for the confirmed live incident this
#                                            fixes). No release call — the lock is
#                                            TTL-bounded and self-clears; call this
#                                            once, right before your existing
#                                            singleton/RUNNING-VM check.
#   lc_gcloud_create <vm_name> <project> <zone> <machine_type> <disk_gb> <metadata> <labels>
#                    [service_account]
#                                          — standard gcloud compute instances create wrapper;
#                                            optional 8th arg sets --service-account (omit/empty
#                                            preserves the prior default-compute-SA behavior)
#   lc_code_bucket <project>               — emit deployment-scripts-<project>
#   lc_run_ts                              — emit YYYYmmdd-HHMMSS timestamp
#   lc_tarball_name_for_repo <repo>        — canonical GCS code-tarball basename
#   lc_verify_tarball_freshness <bucket> <repo>...
#                                          — PRE-LAUNCH guard: refuse/warn/auto-
#                                            republish when a code tarball the VM
#                                            will fetch is stale vs the repo HEAD
#   lc_verify_setup_script_freshness <bucket> <metadata_str>
#                                          — PRE-LAUNCH guard (called automatically
#                                            by lc_gcloud_create): refuse/warn/auto-
#                                            republish when the GCS startup-script
#                                            the VM's startup-script-url points at
#                                            is stale vs the local script
#   lc_gcs_object_age_seconds <gcs_uri>    — seconds since a GCS object's Update time
#   lc_refuse_if_vm_alive <vm_name> <zone> <project> <log_base_gcs_dir> [force] [max_age_seconds]
#                                          — refuse to delete a VM of this EXACT name
#                                            if it's RUNNING with a fresh run.log
#                                            (a different job's live work landed on
#                                            a reused name — see
#                                            features_sports_parallel_backfill_vm_name_collision_2026_07_13.md)
#   lc_write_preemption_signal_file [vm_name] [project]
#                                          — write a shutdown-script (sets
#                                            PREEMPTION_SIGNAL_FILE) that marks a
#                                            SPOT preemption in GCS for fleet
#                                            monitors; observability only, does
#                                            NOT relaunch the VM. Optional
#                                            vm_name/project bake the identity in
#                                            at launch time (skips 2 of the 3
#                                            metadata round-trips the shutdown
#                                            script would otherwise need inside
#                                            the ~30s preemption grace period);
#                                            omit both to fall back to the
#                                            original live-metadata-query form.
#   lc_write_launch_params <vm_name> <project> <launcher> <K=V>...
#                                          — persist the launcher name + the exact
#                                            env vars this VM was launched with to
#                                            GCS (LAUNCH_PARAMS.json), so a later
#                                            preemption-aware relaunch (the
#                                            exit_code_fleet_monitor auto_recover
#                                            actuator) can reproduce the SAME
#                                            venues/START_DATE/concurrency/lease
#                                            instead of falling back to the
#                                            launcher's bare defaults. Best-effort
#                                            (never fails the launch).
#   lc_write_tarball_pin_record <vm_name> <project> <launcher> <K=V>...
#                                          — persist this VM's *_TARBALL_SHA code
#                                            pins to GCS (TARBALL_PINS.json) so the
#                                            retention sweep never reaps a tarball a
#                                            live/relaunchable VM needs, and so a
#                                            preemption relaunch can still find the
#                                            pin AFTER the instance (and its
#                                            metadata) is gone. Best-effort.
#
# Usage:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "${SCRIPT_DIR}/lib/launcher_common.sh"
#
# Conventions:
#   - All functions are prefixed lc_ (launcher_common) to avoid namespace collisions.
#   - Functions print errors to >&2 and return/exit non-zero on failure.
#   - No global state is mutated (functions are pure or emit to stdout).
#
# Compatibility: this file supports bash 3.2+ (macOS default) and bash 4+/5
# (Linux / homebrew). The lc_log_upload_trap_block helper emits shell snippets
# that run on Ubuntu VMs (bash 5) — runtime targets are bash 5, but the launcher
# itself runs on the operator's local machine, which on macOS is bash 3.2.
# When adding new logic, validate with:
#   /bin/bash -n scripts/vm/lib/launcher_common.sh
#   /bin/bash -c 'source scripts/vm/lib/launcher_common.sh && <function-call>'
# before committing. Avoid bash-4+ idioms (${var,,}, ${var^^}, declare -A,
# mapfile/readarray) — use tr-based / while-read alternatives instead.

set -euo pipefail

# ---------------------------------------------------------------------------
# lc_validate_env <env>
# ---------------------------------------------------------------------------
# Verify DEPLOYMENT_ENV is one of prod|staging|dev.
# Exits 1 with a clear error if invalid.
lc_validate_env() {
    local env="${1:-}"
    case "$env" in
        prod|staging|dev) ;;
        *) echo "ERROR: --env must be one of prod/staging/dev (got: ${env})" >&2; exit 1 ;;
    esac
}

# ---------------------------------------------------------------------------
# lc_tier_service_account <env> <project> [test_run=false]
# ---------------------------------------------------------------------------
# Emit the per-tier runtime SA email for <env>: uts-prd-sa for prod,
# uts-test-sa for staging/dev (bucket_iam_write_protection_per_tier_2026_06_09.md
# P2.2d — mirrors scripts/cloud-run/deploy-shared.sh's RUNTIME_SA= pattern on
# the VM-launcher side). Env-overridable via LC_RUNTIME_SA= for an instant
# per-launcher revert to the prior default-compute-SA behavior (pass "" through
# to lc_gcloud_create's service_account arg to opt out entirely).
#
# test_run=true FORCES uts-test-sa regardless of <env> (fix for DP-VM-002,
# 2026-08-01: a --test-run/IS_TEST_RUN launch routes the VM's GCS writes to the
# -test- bucket sibling via get_write_bucket_name(), but every caller of this
# helper still passed the launcher's own DEPLOYMENT_ENV — which defaults to
# "prod" and is never overridden by the pipeline_e2e_check.py smoke-check
# harnesses that drive --test-run. uts-prd-sa's storage.objectAdmin grant is an
# IAM CONDITION scoped to Group A/B `*-prd-*` bucket-name prefixes ONLY
# (bucket_iam_per_tier_sa.tf) — by design, not an oversight — so every such run
# 403'd on its manifest/event writes while still self-deleting with exit_code=0,
# which is exactly the DP-VM-002 "self-delete masking a 0-row run" shape.
# Root-cause incident: instr-backfill-sports-pchk-0801100312-f-betfair (agt-e2fffb).
lc_tier_service_account() {
    local env="${1:?lc_tier_service_account: env required}"
    local project="${2:?lc_tier_service_account: project required}"
    local test_run="${3:-false}"
    if [[ -n "${LC_RUNTIME_SA:-}" ]]; then
        echo "$LC_RUNTIME_SA"
        return 0
    fi
    if [[ "$test_run" == "true" ]]; then
        echo "uts-test-sa@${project}.iam.gserviceaccount.com"
        return 0
    fi
    case "$env" in
        prod) echo "uts-prd-sa@${project}.iam.gserviceaccount.com" ;;
        staging|dev) echo "uts-test-sa@${project}.iam.gserviceaccount.com" ;;
        *) echo "ERROR: lc_tier_service_account: env must be one of prod/staging/dev (got: ${env})" >&2; exit 1 ;;
    esac
}

# ---------------------------------------------------------------------------
# lc_singleton_check <prefix> <zone> <project> [force]
# ---------------------------------------------------------------------------
# If any VM whose name starts with <prefix> is RUNNING in <zone>/<project>,
# print a clear error and exit 1 — unless force="true" (case-insensitive).
#
# Example:
#   lc_singleton_check "qg-snapshot-" "asia-northeast1-c" "$PROJECT" "$FORCE"
lc_singleton_check() {
    local prefix="${1:?lc_singleton_check: prefix required}"
    local zone="${2:?lc_singleton_check: zone required}"
    local project="${3:?lc_singleton_check: project required}"
    local force="${4:-false}"

    # bash-3.2 safe lowercase (macOS default bash lacks ${var,,})
    local force_lc
    force_lc="$(printf '%s' "$force" | tr '[:upper:]' '[:lower:]')"
    if [[ "$force_lc" == "true" ]]; then
        return 0
    fi

    local existing
    existing="$(gcloud compute instances list \
        --filter="name~\"^${prefix}\" AND status=RUNNING" \
        --zones="$zone" \
        --project="$project" \
        --format='value(name)' 2>/dev/null | head -1 || true)"

    if [[ -n "$existing" ]]; then
        local code_bucket="deployment-scripts-${project}"
        cat >&2 <<EOF
ERROR: VM with prefix '${prefix}' already running in ${zone}: ${existing}
Refusing duplicate launch (singleton lock).

Options:
  Inspect: gsutil cat gs://${code_bucket}/vm-logs/${existing}/run.log | tail -50
  Force:   bash \$0 --force

CAUTION — do NOT delete ${existing} unless you have confirmed via Inspect
above that it is genuinely stale. It may be another dispatch's actively
progressing VM; deleting a live VM destroys hours of in-progress work (see
zombie_watchdog_relaunch_reaped_live_backfills_2026_06_23.md "Incident 2
correction" — a raw copy-pasteable delete suggestion in this exact refusal
path is the documented root cause of prior agent-deleted-own-fleet
incidents). If confirmed stale:
  gcloud compute instances delete ${existing} --zone=${zone} --project=${project} --quiet
EOF
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# lc_acquire_singleton_lock <lock_key> <project> [ttl_seconds=300] [force=false]
# ---------------------------------------------------------------------------
# Closes the TOCTOU race lc_singleton_check's own `list --filter=...RUNNING`
# pattern CANNOT close on its own: a freshly-created VM takes tens of seconds
# to reach RUNNING, so two near-simultaneous invocations of the SAME launcher
# can both see "nothing running" and both proceed to `gcloud compute
# instances create`. Confirmed LIVE twice in the same incident window
# (`cefi-fwd-*`, `unified-trading-sa`): two `instances.insert` calls 13s
# apart on the original launch, then 46s apart on its own relaunch — see
# plans/active/issues/cefi_fwd_vm_preempted_false_positive_standard_provisioning_2026_08_06.md
# P2 follow-up. A list-then-create check can NEVER close this on its own no
# matter how the status filter is widened — the only fix is an operation that
# is atomic AT THE STORAGE LAYER, so exactly one of two simultaneous callers
# wins deterministically.
#
# Mechanism: a create-if-absent conditional PUT (`gcloud storage cp
# --if-generation-match=0`) to a small sentinel blob at
# gs://deployment-scripts-<project>/vm-launch-locks/<lock_key>.lock — GCS
# guarantees this is atomic server-side, so of two callers racing to write the
# SAME object, exactly one succeeds; the other's conditional write fails.
# This is the SAME sentinel-blob-lock shape
# unified_trading_library.manifest_consolidator already uses for its own
# "skip this cron tick if a sibling cycle is in flight" guard (`_LOCK_PATH` /
# `_is_lock_fresh` / TTL so a crashed holder's lock self-expires rather than
# jamming every future run forever) — re-implemented bash-native here since
# this runs inside a launcher script, not the `deployment_service` python
# package, and TTL-only (no explicit release/delete call — a `gcloud storage
# rm` is blocked for autonomous workers by
# agent-orchestrator/scripts/hooks/block_destructive_commands.py regardless,
# and isn't needed for correctness: a STALE lock is reclaimed atomically via
# `--if-generation-match=<its own generation>`, the same CAS contract UTL's
# own `gcs_conditional_put` documents for a non-zero match value).
#
# Returns 0 (lock acquired or force=true — nothing further to do, no release
# call) or 1 (a FRESH lock is already held by another in-flight launch, or the
# conditional write lost the race — refuse; the caller should exit 1).
# force="true" bypasses entirely without touching GCS (mirrors every other
# launcher's --force semantics — an explicit operator override).
#
# Example (call once, right before your existing RUNNING-VM singleton check):
#   lc_acquire_singleton_lock "cefi-fwd-launch" "$PROJECT" 300 "$FORCE" || exit 1
lc_acquire_singleton_lock() {
    local lock_key="${1:?lc_acquire_singleton_lock: lock_key required}"
    local project="${2:?lc_acquire_singleton_lock: project required}"
    local ttl_seconds="${3:-300}"
    local force="${4:-false}"

    # bash-3.2 safe lowercase (macOS default bash lacks ${var,,}).
    local force_lc
    force_lc="$(printf '%s' "$force" | tr '[:upper:]' '[:lower:]')"
    if [[ "$force_lc" == "true" ]]; then
        return 0
    fi

    local code_bucket lock_uri
    code_bucket="deployment-scripts-${project}"
    lock_uri="gs://${code_bucket}/vm-launch-locks/${lock_key}.lock"

    local match_generation existing_gen existing_age
    match_generation="0"
    existing_gen="$(gcloud storage objects describe "$lock_uri" --format='value(generation)' 2>/dev/null || true)"
    if [[ -n "$existing_gen" ]]; then
        existing_age="$(lc_gcs_object_age_seconds "$lock_uri" 2>/dev/null || true)"
        if [[ -n "$existing_age" ]] && (( existing_age < ttl_seconds )); then
            cat >&2 <<EOF
ERROR: another launch for '${lock_key}' is already in flight (lock ${existing_age}s old, TTL ${ttl_seconds}s): ${lock_uri}

Refusing to launch a duplicate — this is the atomic GCS lock that closes the
list-then-create TOCTOU race a plain "any VM RUNNING?" singleton check cannot
close on its own (two near-simultaneous launches can both see "nothing
running" before either instance reaches RUNNING). See
plans/active/issues/cefi_fwd_vm_preempted_false_positive_standard_provisioning_2026_08_06.md.

If you are CERTAIN the other launch crashed without releasing its lock (rare
— the TTL self-clears within ${ttl_seconds}s on its own, usually the right
move is just to wait), re-run with --force.
EOF
            return 1
        fi
        # Stale (or age unreadable — fail toward reclaim, not toward a
        # permanently-jammed launcher) — reclaim atomically against the exact
        # generation just read; a concurrent reclaimer racing us here loses
        # THIS conditional write, never silently double-acquires.
        match_generation="$existing_gen"
    fi

    local tmp_dir tmp_file
    tmp_dir="$(mktemp -d)"
    tmp_file="${tmp_dir}/lock.txt"
    printf 'held_at=%s\npid=%s\nhost=%s\nlock_key=%s\n' \
        "$(date -u +%FT%TZ)" "$$" "$(hostname 2>/dev/null || echo unknown)" "$lock_key" >"$tmp_file"

    if ! gcloud storage cp "$tmp_file" "$lock_uri" --if-generation-match="$match_generation" >/dev/null 2>&1; then
        rm -rf "$tmp_dir"
        cat >&2 <<EOF
ERROR: could not acquire the singleton lock for '${lock_key}' (${lock_uri}) —
a concurrent launcher most likely just won the same race (the INTENDED
outcome: refuse rather than double-launch). Re-run once any concurrent launch
has settled, or --force to bypass.
EOF
        return 1
    fi
    rm -rf "$tmp_dir"
    return 0
}

# ---------------------------------------------------------------------------
# lc_gcs_object_age_seconds <gcs_uri>
# ---------------------------------------------------------------------------
# Seconds elapsed since <gcs_uri>'s update time (per `gcloud storage objects
# describe`). Prints the integer to stdout; returns non-zero if the object
# doesn't exist, gcloud is unavailable, or the timestamp can't be parsed. Uses
# python3 (already a hard dependency everywhere gcloud runs) for portable ISO
# 8601 parsing instead of GNU-only `date -d` (this file targets bash 3.2+/
# macOS too).
#
# `gcloud storage`, not `gsutil` — gsutil resolves creds from the CLI's active
# account (a short-lived WIF token in an interactive AO slot can't refresh
# unattended), while `gcloud storage` resolves via ADC, which stays valid. See
# plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
lc_gcs_object_age_seconds() {
    local gcs_uri="${1:?lc_gcs_object_age_seconds: gcs_uri required}"
    command -v gcloud >/dev/null 2>&1 || return 1
    local updated
    updated="$(gcloud storage objects describe "$gcs_uri" --format='value(update_time)' 2>/dev/null)"
    [[ -n "$updated" ]] || return 1
    python3 -c "
import sys, time
from datetime import datetime
try:
    ts = datetime.strptime(sys.argv[1], '%Y-%m-%dT%H:%M:%S%z').timestamp()
    print(int(time.time() - ts))
except Exception:
    sys.exit(1)
" "$updated"
}

# ---------------------------------------------------------------------------
# lc_refuse_if_vm_alive <vm_name> <zone> <project> <log_base_gcs_dir> [force] [max_age_seconds]
# ---------------------------------------------------------------------------
# Liveness/collision guard for launchers that relaunch onto a REUSED, numbered
# VM name (e.g. a `--vms 1` gap-fill relaunch always landing on the same freed
# slot number — the exact footgun that silently deleted a live, unrelated VM's
# in-progress work: features_sports_parallel_backfill_vm_name_collision_2026_07_13.md).
#
# Before an unconditional delete-then-create, this refuses (exit 1) the launch
# if a VM of this EXACT name is currently RUNNING **and** its durable run.log
# (written by lc_log_upload_trap_block at <log_base_gcs_dir>/<vm_name>/run.log)
# was updated within the last <max_age_seconds> (default 300s / 5min) — the
# signature of LIVE, in-progress work, not a stale leftover from a
# completed/dead prior run of the SAME logical job. A stopped/terminated VM,
# or one whose log has gone stale, is treated as safe to delete+relaunch (the
# case this delete step was originally written for). Fails CLOSED (refuses)
# when the log's age can't be determined at all — a brand-new VM that hasn't
# written its first log line yet is exactly the case that must not be killed.
#
# Example:
#   LOG_BASE="gs://deployment-scripts-${PROJECT_ID}/vm-logs"
#   lc_refuse_if_vm_alive "$VM_NAME" "$ZONE" "$PROJECT_ID" "$LOG_BASE" "$FORCE_REPLACE"
#   gcloud compute instances delete "$VM_NAME" ... --quiet 2>/dev/null || true
lc_refuse_if_vm_alive() {
    local vm_name="${1:?lc_refuse_if_vm_alive: vm_name required}"
    local zone="${2:?lc_refuse_if_vm_alive: zone required}"
    local project="${3:?lc_refuse_if_vm_alive: project required}"
    local log_base="${4:?lc_refuse_if_vm_alive: log_base_gcs_dir required}"
    local force="${5:-false}"
    local max_age_seconds="${6:-300}"

    # bash-3.2 safe lowercase (macOS default bash lacks ${var,,}).
    local force_lc
    force_lc="$(printf '%s' "$force" | tr '[:upper:]' '[:lower:]')"
    if [[ "$force_lc" == "true" ]]; then
        return 0
    fi

    local status
    status="$(gcloud compute instances describe "$vm_name" \
        --zone="$zone" --project="$project" \
        --format='value(status)' 2>/dev/null || true)"
    [[ "$status" == "RUNNING" ]] || return 0 # gone / stopped / never existed → safe to replace

    local age
    age="$(lc_gcs_object_age_seconds "${log_base}/${vm_name}/run.log" 2>/dev/null || true)"
    if [[ -z "$age" ]] || (( age < max_age_seconds )); then
        local age_label="unreadable/very-fresh"
        [[ -n "$age" ]] && age_label="${age}s old"
        cat >&2 <<EOF
ERROR: VM '${vm_name}' is RUNNING and its run.log is ${age_label} (< ${max_age_seconds}s) — refusing to delete a
LIVE VM. This is almost certainly a DIFFERENT job's live work that happens to have landed on this name (the classic
"--vms 1 relaunch reuses a freed slot number" collision — see
plans/active/issues/features_sports_parallel_backfill_vm_name_collision_2026_07_13.md).

Options:
  Inspect: gsutil cat ${log_base}/${vm_name}/run.log | tail -50
  If genuinely safe to replace: re-run with --force-replace
EOF
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# lc_gcloud_create <vm_name> <project> <zone> <machine_type> <disk_gb>
#                  <metadata_str> <labels_str> [service_account]
# ---------------------------------------------------------------------------
# Standard gcloud compute instances create call with the workspace's canonical
# image (ubuntu-2404-lts-amd64), cloud-platform scope, and no-restart-on-failure.
#
# <metadata_str>    : comma-separated key=value pairs (e.g. "VM_TASK=foo,ENV=prod")
# <labels_str>      : comma-separated key=value pairs (e.g. "purpose=foo,env=prod")
# [service_account] : optional runtime SA email — pass the output of
#                      lc_tier_service_account to run as a tier SA instead of
#                      the GCP default compute SA. Omit/empty preserves the
#                      prior default-compute-SA behavior (no --service-account
#                      flag at all, exactly as before this arg was added).
#
# Example:
#   lc_gcloud_create "$VM_NAME" "$PROJECT" "$ZONE" "e2-standard-4" "50" \
#       "VM_TASK=foo,DEPLOYMENT_ENV=prod" \
#       "purpose=foo,env=prod" \
#       "$(lc_tier_service_account "$DEPLOYMENT_ENV" "$PROJECT")"
lc_gcloud_create() {
    local vm_name="${1:?lc_gcloud_create: vm_name required}"
    local project="${2:?lc_gcloud_create: project required}"
    local zone="${3:?lc_gcloud_create: zone required}"
    local machine_type="${4:?lc_gcloud_create: machine_type required}"
    local disk_gb="${5:?lc_gcloud_create: disk_gb required}"
    local metadata_str="${6:?lc_gcloud_create: metadata_str required}"
    local labels_str="${7:?lc_gcloud_create: labels_str required}"
    local service_account="${8:-}"

    # Every deployment-service launch carries a standardized managed-by label so the deployments
    # cockpit can PROVE provenance: a live GCE instance WITHOUT this label is provably ad-hoc (WS-D
    # provenance robustness — deployment_full_estate_cost_provenance). Appended centrally here so all
    # ~80 launchers inherit it without a per-copy edit.
    local final_labels="${labels_str},managed-by=deployment-service"

    # Centralised dry-run safety net (codified 2026-05-20 after the
    # launch-tradfi-forward-poll.sh incident — see
    # plans/active/issues/launcher_dry_run_support_gap_2026_05_20.md).
    # Any caller (launcher or wrapper) that exports LC_DRY_RUN=true short-circuits
    # the real `gcloud compute instances create` call. Per-launcher --dry-run
    # parsing should set + export LC_DRY_RUN=true before invoking this helper.
    local lc_dry_run_lc
    lc_dry_run_lc="$(printf '%s' "${LC_DRY_RUN:-false}" | tr '[:upper:]' '[:lower:]')"
    if [[ "$lc_dry_run_lc" == "true" ]]; then
        echo "[DRY-RUN] Would create VM: ${vm_name}"
        echo "[DRY-RUN]   project=${project} zone=${zone} machine=${machine_type} disk=${disk_gb}GB"
        echo "[DRY-RUN]   metadata=${metadata_str}"
        echo "[DRY-RUN]   labels=${final_labels}"
        echo "[DRY-RUN]   service_account=${service_account:-<default-compute-sa>}"
        return 0
    fi

    # PRE-LAUNCH guard against the setup-script publish race (see
    # lc_verify_setup_script_freshness below) — checked here, not per-launcher,
    # so every caller of lc_gcloud_create (4 launchers, measured 2026-07-31; the
    # other ~139 launchers call `gcloud compute instances create` directly and do
    # NOT inherit this guard — see
    # plans/active/issues/vm_launcher_setup_script_freshness_gap_2026_07_31.md)
    # inherits it automatically.
    if ! lc_verify_setup_script_freshness "$(lc_code_bucket "$project")" "$metadata_str"; then
        return 1
    fi

    if [[ -n "$service_account" ]]; then
        gcloud compute instances create "$vm_name" \
            --project="$project" \
            --zone="$zone" \
            --machine-type="$machine_type" \
            --image-family=ubuntu-2404-lts-amd64 \
            --image-project=ubuntu-os-cloud \
            --boot-disk-size="${disk_gb}GB" \
            --scopes=cloud-platform \
            --no-restart-on-failure \
            --service-account="$service_account" \
            --metadata="$metadata_str" \
            --labels="$final_labels"
    else
        gcloud compute instances create "$vm_name" \
            --project="$project" \
            --zone="$zone" \
            --machine-type="$machine_type" \
            --image-family=ubuntu-2404-lts-amd64 \
            --image-project=ubuntu-os-cloud \
            --boot-disk-size="${disk_gb}GB" \
            --scopes=cloud-platform \
            --no-restart-on-failure \
            --metadata="$metadata_str" \
            --labels="$final_labels"
    fi
}

# ---------------------------------------------------------------------------
# lc_write_startup_file <script_content>
# ---------------------------------------------------------------------------
# Write <script_content> to a mktemp file, set STARTUP_FILE, and register
# a trap to delete it on EXIT. Centralises temp-file lifecycle management.
#
# After calling this function, pass STARTUP_FILE to gcloud:
#   gcloud compute instances create ... \
#       --metadata-from-file="startup-script=${STARTUP_FILE}"
#
# Example:
#   STARTUP_SCRIPT=$(cat << 'STARTUP_EOF'
#   #!/bin/bash
#   echo "hello from VM"
#   STARTUP_EOF
#   )
#   lc_write_startup_file "$STARTUP_SCRIPT"
#   # STARTUP_FILE is now set; cleanup registered via trap EXIT
lc_write_startup_file() {
    local content="${1:?lc_write_startup_file: script content required}"
    # mktemp -d + a fixed filename inside (portable): a template with a
    # non-X suffix ("-XXXX.sh") only randomizes on GNU coreutils — BSD/macOS
    # mktemp requires the trailing chars to be ALL 'X's, so on macOS it
    # silently returns the LITERAL path unrandomized. Two concurrent
    # invocations (e.g. pytest-xdist workers exercising different launcher
    # tests at once) then collide on the exact same file ("mkstemp failed:
    # File exists"), non-deterministically breaking any caller — found via
    # this exact xdist collision in test_vm_launcher_scripts.py. `mktemp -d`
    # (no template) is genuinely portable and creates a mode-0700 directory,
    # so a fixed name inside it is exclusive to this process.
    STARTUP_FILE="$(mktemp -d)/startup.sh"
    printf '%s' "$content" > "$STARTUP_FILE"
    # Register cleanup — if caller already has an EXIT trap, this appends to it.
    # shellcheck disable=SC2064
    trap "rm -f '${STARTUP_FILE}'" EXIT
    export STARTUP_FILE
}

# ---------------------------------------------------------------------------
# lc_write_preemption_signal_file
# ---------------------------------------------------------------------------
# Write a GCE shutdown-script to a mktemp file that, ONLY when the instance
# metadata server reports preempted=true, writes a "preempted" marker blob to
# gs://deployment-scripts-<project>/vm-logs/<vm_name>/PREEMPTED. This is an
# OBSERVABILITY signal only — it does NOT relaunch the VM. Its purpose is so
# fleet monitors can classify a SPOT preemption as an expected, benign
# shutdown (worth relaunching) rather than an unexplained DP_VM_GONE_NO_CAPTURE
# failure. Mirrors the pattern already shipped in
# launch-transfermarkt-backfill-vm.sh; centralised here so other SPOT
# launchers (e.g. the cefi/tardis family, which has none today — see
# cefi_completion_program_2026_07_15.md's "SPOT preemption DELETES waves"
# finding) can opt in with one line instead of re-deriving the heredoc.
#
# Sets PREEMPTION_SIGNAL_FILE and registers EXIT cleanup, same lifecycle
# contract as lc_write_startup_file. Pass the result to gcloud via:
#   --metadata-from-file="shutdown-script=${PREEMPTION_SIGNAL_FILE}"
#
# NOTE: gcloud only accepts ONE shutdown-script per instance. A caller that
# also needs its own shutdown-script cannot use this helper as-is — merge the
# preemption check into that script instead (see the transfermarkt launcher
# for the inline pattern).
#
# Hardened 2026-07-31 (issues/vm_billing_waste_first_audit_and_preflight_gate_design_2026_07_24.md
# + issues/session_bound_vm_monitoring_reliability_gap_2026_07_26.md): a live sweep of every
# af-backfill preemption confirmed via `gcloud compute operations list` found the marker missing
# on 5/5 — the write was consistently losing the race against the GCE ~30s preemption grace
# period, not a rare one-off. Two changes close most of that gap: (1) skip the VM_NAME/PROJECT
# metadata round-trips when the caller already knows them (bake in at launch time — 2 fewer
# HTTP round-trips inside the grace window); (2) replace the `gcloud storage cp` subprocess
# (multi-second Python-CLI cold start) with a direct curl PUT to the GCS JSON API using the
# instance's own metadata-server OAuth token — same identity/permissions, far less startup
# overhead — plus a short retry loop for a transient failure within the remaining window.
lc_write_preemption_signal_file() {
    local vm_name_baked="${1:-}"
    local project_baked="${2:-}"
    # Portable mktemp (see lc_write_startup_file's comment above for the full
    # rationale): a non-X-terminal template silently fails to randomize on
    # BSD/macOS mktemp, colliding across concurrent invocations.
    PREEMPTION_SIGNAL_FILE="$(mktemp -d)/preempt-shutdown.sh"
    local resolve_ids
    if [[ -n "$vm_name_baked" && -n "$project_baked" ]]; then
        resolve_ids="$(printf 'VM_NAME=%q\nPROJECT=%q' "$vm_name_baked" "$project_baked")"
    else
        resolve_ids='VM_NAME=$(curl -sf -m 2 -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/name" 2>/dev/null || echo "")
PROJECT=$(curl -sf -m 2 -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/project/project-id" 2>/dev/null || echo "")
[[ -n "$VM_NAME" && -n "$PROJECT" ]] || exit 0'
    fi
    cat > "$PREEMPTION_SIGNAL_FILE" <<EOF
#!/usr/bin/env bash
PREEMPTED=\$(curl -sf -m 2 -H 'Metadata-Flavor: Google' \\
  'http://metadata.google.internal/computeMetadata/v1/instance/preempted' 2>/dev/null || echo 'false')
[[ "\$PREEMPTED" == "true" ]] || exit 0
${resolve_ids}
TOKEN=\$(curl -sf -m 2 -H 'Metadata-Flavor: Google' \\
  'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token' 2>/dev/null \\
  | grep -o '"access_token"[[:space:]]*:[[:space:]]*"[^"]*"' | cut -d'"' -f4)
[[ -n "\$TOKEN" ]] || exit 0
OBJECT="vm-logs%2F\${VM_NAME}%2FPREEMPTED"
for attempt in 1 2 3; do
  curl -sf -m 3 -X POST -H "Authorization: Bearer \${TOKEN}" \\
    -H 'Content-Type: application/octet-stream' --data-binary 'preempted' \\
    "https://storage.googleapis.com/upload/storage/v1/b/deployment-scripts-\${PROJECT}/o?uploadType=media&name=\${OBJECT}" \\
    >/dev/null 2>&1 && break
done
echo "[preemption-shutdown] wrote PREEMPTED signal for \${VM_NAME}" >&2
EOF
    # shellcheck disable=SC2064
    trap "rm -f '${PREEMPTION_SIGNAL_FILE}'" EXIT
    export PREEMPTION_SIGNAL_FILE
}

# ---------------------------------------------------------------------------
# lc_write_launch_params <vm_name> <project> <launcher> [K=V ...]
# ---------------------------------------------------------------------------
# Persist this VM's exact launch parameters to
# gs://deployment-scripts-<project>/vm-logs/<vm_name>/LAUNCH_PARAMS.json —
# {"launcher": "<launcher>", "env": {"K": "V", ...}}.
#
# Why this exists: closing the SPOT-preemption relaunch gap
# (cefi_completion_program_2026_07_15.md P0 "Close the SPOT-preemption relaunch
# gap") requires the relauncher to reproduce the SAME venues/START_DATE/
# concurrency/lease the preempted VM was launched with — a blind re-invocation
# of the launcher with only its bare defaults would launch a DIFFERENT (often
# far bigger, e.g. full-history-since-genesis) job than the one that died.
# Called by the launcher itself (the local/orchestrator side, NOT a VM-side
# shutdown-script — unlike lc_write_preemption_signal_file, this runs BEFORE
# `gcloud compute instances create`/`aws ec2 run-instances`, so it needs no
# metadata-server round-trip and no 30s-preemption-notice race).
#
# Consumed by `deployment_service.data_pipeline_monitors._gcs.read_launch_params`
# (exit_code_fleet_monitor's PREEMPTED handling) → replayed as the relaunch
# subprocess's env by `scripts.recovery.relaunch_backfill_vm.RelaunchPreemptedVm`,
# which invokes the SAME launcher — so the launcher's OWN
# tardis_concurrency_guard (sourced inside it) still binds; this helper never
# bypasses that guard, it only lets the relaunch aim at the right shard.
#
# Best-effort: a missing gcloud/python3, or a write failure, only WARNS — it
# must never fail the launch (the VM being created is the real deliverable; a
# lost launch-params record just means a future preemption relaunch falls back
# to the launcher's bare defaults instead of an exact replay).
#
# Empty-string values are written verbatim (bash's `${VAR:-}` reads an empty
# env var the same as unset for every `[[ -n ... ]]`/`[[ "$X" == "1" ]]` guard
# in these launchers, so persisting "" for an unset optional is safe).
lc_write_launch_params() {
    local vm_name="${1:?lc_write_launch_params: vm_name required}"
    local project="${2:?lc_write_launch_params: project required}"
    local launcher="${3:?lc_write_launch_params: launcher required}"
    shift 3
    if ! command -v gcloud >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
        echo "WARNING: lc_write_launch_params — gcloud/python3 unavailable, skipping (relaunch will use launcher defaults)" >&2
        return 0
    fi
    local uri="gs://$(lc_code_bucket "$project")/vm-logs/${vm_name}/LAUNCH_PARAMS.json"
    # `gcloud storage`, not `gsutil` — gsutil resolves creds from the CLI's active
    # account (a short-lived WIF token in an interactive AO slot can't refresh
    # unattended), while `gcloud storage` resolves via ADC, which stays valid. See
    # plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
    if ! python3 -c '
import json, sys

launcher = sys.argv[1]
env = {}
for pair in sys.argv[2:]:
    if "=" in pair:
        key, _, value = pair.partition("=")
        env[key] = value
print(json.dumps({"launcher": launcher, "env": env}))
' "$launcher" "$@" | gcloud storage cp - "$uri" --quiet 2>/dev/null; then
        echo "WARNING: lc_write_launch_params — failed to write ${uri} (best-effort, non-fatal; a preemption relaunch will use launcher defaults)" >&2
    fi
}

# ---------------------------------------------------------------------------
# lc_write_tarball_pin_record <vm_name> <project> <launcher> [K=V ...]
# ---------------------------------------------------------------------------
# Persist this VM's SHA code-tarball pins to
# gs://deployment-scripts-<project>/vm-logs/<vm_name>/TARBALL_PINS.json —
# {"launcher": "...", "vm_name": "...", "written_at": "<iso8601 UTC>",
#  "pins": {"UAC_TARBALL_SHA": "<sha>", ...}, "floating": ["UTL_TARBALL_SHA"]}
#
# Why this exists, given the pins are ALREADY on the instance as GCE metadata
# (which the retention sweep now reads — see the UTL `metadata` passthrough):
# instance metadata dies WITH the instance. The 2026-07-20 incident is precisely
# the window where the VM no longer exists — SPOT-preempted, or self-deleted
# after a failed setup — and `RelaunchPreemptedVm` still needs the exact tarball
# it was pinned to. Metadata covers RUNNING VMs (day-one, no launcher rollout);
# this record covers the DEAD-but-relaunchable ones. Neither alone closes the
# incident; the protected set is the UNION.
#
# `floating` is not decoration: it is the explicit, machine-readable assertion
# that a repo was left unpinned ON PURPOSE. The retention sweep fails CLOSED on
# a running pinning-launcher VM it can find no record for, so a launcher that
# silently floats would otherwise be indistinguishable from one whose record
# failed to write — and the sweep would block forever. Recording the intent is
# what keeps fail-closed affordable.
#
# TTL/grace: consumers only honour a record whose object mtime is inside the
# retention grace window (`DEFAULT_PIN_GRACE_DAYS`, 14d) — the registry is
# bounded by that window plus the code bucket's own lifecycle rules, so it
# cannot grow without limit. A VM idle longer than the grace window is past any
# realistic relaunch horizon.
#
# Best-effort, exactly like lc_write_launch_params: a write failure WARNs and
# never fails the launch. The VM being created is the deliverable; a lost record
# degrades to metadata-only protection (still correct while the VM runs).
lc_write_tarball_pin_record() {
    local vm_name="${1:?lc_write_tarball_pin_record: vm_name required}"
    local project="${2:?lc_write_tarball_pin_record: project required}"
    local launcher="${3:?lc_write_tarball_pin_record: launcher required}"
    shift 3
    if ! command -v gcloud >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
        echo "WARNING: lc_write_tarball_pin_record — gcloud/python3 unavailable, skipping (retention falls back to instance metadata only)" >&2
        return 0
    fi
    local uri="gs://$(lc_code_bucket "$project")/vm-logs/${vm_name}/TARBALL_PINS.json"
    # `gcloud storage`, not `gsutil` — see the ADC-vs-active-account note on
    # lc_write_launch_params above; same WIF-token gap.
    if ! python3 -c '
import datetime, json, sys

launcher, vm_name = sys.argv[1], sys.argv[2]
pins, floating = {}, []
for pair in sys.argv[3:]:
    if "=" not in pair:
        continue
    key, _, value = pair.partition("=")
    value = value.strip()
    # An empty value is NOT a pin. Every `[[ -n ... ]]` guard in these launchers
    # treats "" and unset identically, and both mean a floating pull — so record
    # it as a deliberate float rather than a pin to nothing.
    if value:
        pins[key] = value
    else:
        floating.append(key)
print(
    json.dumps(
        {
            "launcher": launcher,
            "vm_name": vm_name,
            "written_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "pins": pins,
            "floating": floating,
        }
    )
)
' "$launcher" "$vm_name" "$@" | gcloud storage cp - "$uri" --quiet 2>/dev/null; then
        echo "WARNING: lc_write_tarball_pin_record — failed to write ${uri} (best-effort, non-fatal; retention falls back to instance metadata for as long as this VM runs)" >&2
    fi
}

# ---------------------------------------------------------------------------
# lc_code_bucket <project>
# ---------------------------------------------------------------------------
# Emit the canonical code/tarballs GCS bucket name for a given project.
lc_code_bucket() {
    local project="${1:?lc_code_bucket: project required}"
    echo "deployment-scripts-${project}"
}

# ---------------------------------------------------------------------------
# lc_run_ts
# ---------------------------------------------------------------------------
# Emit a sortable timestamp suitable for VM name suffixes: YYYYmmdd-HHMMSS
lc_run_ts() {
    date +%Y%m%d-%H%M%S
}

# ---------------------------------------------------------------------------
# lc_tarball_name_for_repo <repo>
# ---------------------------------------------------------------------------
# Emit the canonical GCS code-tarball basename create-code-tarballs.sh produces
# for a given workspace repo dir. Keep this the SINGLE source of truth for the
# repo→tarball mapping — it must stay in lockstep with CORE_REPOS in
# create-code-tarballs.sh (the one special case is market-tick-data-service →
# mtds-code; everything else is <repo>-code).
lc_tarball_name_for_repo() {
    local repo="${1:?lc_tarball_name_for_repo: repo required}"
    case "$repo" in
        market-tick-data-service) echo "mtds-code" ;;
        *)                        echo "${repo}-code" ;;
    esac
}

# ---------------------------------------------------------------------------
# lc_resolve_tarball_sha <code_bucket> <repo>
# ---------------------------------------------------------------------------
# Echo the commit_sha of the CURRENTLY-DEPLOYED (floating) code tarball for
# <repo> — but ONLY when its SHA-pinned copy provably resolves, i.e. both
# gs://<bucket>/code/<tarball>@<sha>.tar.gz AND its .manifest.json exist. Echoes
# nothing (and returns 0) on any failure, so a caller's `${VAR:-$(...)}` default
# degrades to "leave it floating" (today's behaviour) rather than pinning a SHA
# that would brick boot at setup-data-pipeline-vm.sh's "refusing floating
# fallback" hard-fail.
#
# Why the floating MANIFEST's sha (not the local clone HEAD): create-code-tarballs.sh
# writes <tarball>.tar.gz, <tarball>.manifest.json AND <tarball>@<sha>.{tar.gz,manifest.json}
# in the SAME run, so the floating manifest's commit_sha is exactly the sha whose
# @<sha> pair exists in the bucket — the freshest code that will ACTUALLY be
# installed. Pinning HEAD instead can name a sha that was never tarballed → a
# guaranteed boot failure. This reads the SAME manifest.json lc_verify_tarball_freshness
# compares against.
#
# Purpose: close the launcher half of
# plans/active/issues/mdps_vm_stale_uac_contract_propagation_2026_07_20.md — the
# service launchers pin UTL/MDPS but NOT UAC, so a UAC schema/contract change is
# not version-pinned into VM metadata and the VM falls to the unpinned floating
# pull. An auto-resolved UAC pin makes the contract deterministic, exempts the
# tarball from the retention sweep for the VM's life (tarball_pins.py), and lets
# setup's per-tarball manifest-sha self-verify run for UAC.
lc_resolve_tarball_sha() {
    local code_bucket="${1:?lc_resolve_tarball_sha: code_bucket required}"
    local repo="${2:?lc_resolve_tarball_sha: repo required}"
    command -v gcloud >/dev/null 2>&1 || return 0
    command -v python3 >/dev/null 2>&1 || return 0
    local tarball manifest_uri manifest_tmp sha
    tarball="$(lc_tarball_name_for_repo "$repo")"
    manifest_uri="gs://${code_bucket}/code/${tarball}.manifest.json"
    # Portable mktemp — see lc_write_startup_file's comment for the full
    # BSD/macOS-vs-GNU rationale (a non-X-terminal template doesn't randomize
    # on macOS, colliding across concurrent invocations/xdist workers).
    manifest_tmp="$(mktemp -d)/manifest.json"
    # `gcloud storage`, not `gsutil` — gsutil resolves creds from the CLI's active
    # account (a short-lived WIF token in an interactive AO slot can't refresh
    # unattended), while `gcloud storage` resolves via ADC, which stays valid. See
    # plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
    if ! gcloud storage cp "$manifest_uri" "$manifest_tmp" --quiet 2>/dev/null; then
        rm -f "$manifest_tmp"
        return 0
    fi
    sha="$(python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get('commit_sha',''))" "$manifest_tmp" 2>/dev/null || echo "")"
    rm -f "$manifest_tmp"
    [[ -n "$sha" ]] || return 0
    # Only emit the pin if the @<sha> tarball AND its manifest BOTH exist — setup
    # refuses a pinned tarball whose manifest is absent ("cannot verify
    # provenance") just as hard as a missing tarball.
    gcloud storage objects describe "gs://${code_bucket}/code/${tarball}@${sha}.tar.gz" >/dev/null 2>&1 || return 0
    gcloud storage objects describe "gs://${code_bucket}/code/${tarball}@${sha}.manifest.json" >/dev/null 2>&1 || return 0
    printf '%s' "$sha"
}

# ---------------------------------------------------------------------------
# lc_verify_tarball_freshness <code_bucket> <repo> [repo2 ...]
# ---------------------------------------------------------------------------
# PRE-LAUNCH guard against the silent-stale-tarball class of bug: a VM boots and
# fetches gs://<bucket>/code/<repo>-code.tar.gz that was NEVER republished after
# a fix landed on live-defi-rollout, so it silently runs days-old code despite
# correct metadata. Root-caused 2026-07-12 (morpho lending_indices: mtds-code
# was 4 days stale; the VM ran the pre-fix _DEFAULT_PROTOCOLS list and wrote 0
# rows for hours). SSOT:
# plans/active/issues/defi_morpho_lending_indices_never_wired_2026_07_12.md.
#
# For each repo, compares the floating tarball's .manifest.json commit_sha
# against the workspace clone's current git SHA (the exact SHA
# create-code-tarballs.sh would tar right now) and acts per LC_TARBALL_FRESHNESS:
#
#   off      — skip the check entirely (return 0).
#   warn     — (DEFAULT) print a loud WARNING + the exact republish remedy for
#              each stale repo, then return 0 (never blocks a launch).
#   enforce  — return 1 (block the launch) if ANY repo is stale or its tarball
#              manifest is missing/unreadable — "never launch a VM onto stale
#              code", mirroring setup-data-pipeline-vm.sh's TARBALL_EXPECTED_SHA
#              gate one step earlier (before the VM even boots).
#   auto     — republish each stale repo via create-code-tarballs.sh --include
#              <repo>, re-verify, then return 0 (or 1 if a republish failed).
#
# Comparison ref: the local workspace clone HEAD by default (kept fresh by
# slot-cron-ff-pull). Set LC_TARBALL_FRESHNESS_FETCH=true to `git fetch` first
# and compare against origin/<LC_TARBALL_FRESHNESS_REF> (default
# live-defi-rollout) for a network-authoritative check.
#
# Env knobs:
#   LC_TARBALL_FRESHNESS        off|warn|enforce|auto  (default: warn)
#   LC_TARBALL_FRESHNESS_FETCH  true|false             (default: false)
#   LC_TARBALL_FRESHNESS_REF    branch                 (default: live-defi-rollout)
#   WORKSPACE_ROOT              workspace root holding the sibling repo clones
#                               (default: auto-detected from this lib's location)
#
# Robustness: git/gcloud unavailable, or a repo dir that isn't a git clone, is a
# "can't verify" — warn+skip in every mode EXCEPT enforce with a genuinely
# stale/missing tarball. A transient tooling absence never blocks a launch.
#
# Usage (in a launcher, before the gcloud create):
#   source "${SCRIPT_DIR}/lib/launcher_common.sh"
#   lc_verify_tarball_freshness "$CODE_BUCKET" market-tick-data-service \
#       unified-api-contracts unified-trading-library deployment-service
lc_verify_tarball_freshness() {
    local code_bucket="${1:?lc_verify_tarball_freshness: code_bucket required}"
    shift
    if [[ $# -eq 0 ]]; then
        echo "lc_verify_tarball_freshness: at least one repo required" >&2
        return 2
    fi

    # bash-3.2 safe lowercase.
    local mode
    mode="$(printf '%s' "${LC_TARBALL_FRESHNESS:-auto}" | tr '[:upper:]' '[:lower:]')"
    if [[ "$mode" == "off" ]]; then
        return 0
    fi
    case "$mode" in
        warn|enforce|auto) ;;
        *) echo "WARNING: unknown LC_TARBALL_FRESHNESS='$mode' — treating as 'warn'" >&2; mode="warn" ;;
    esac

    # Tooling preconditions — a missing tool means we can't verify anything;
    # warn once and let the launch proceed (never block on a tooling gap).
    if ! command -v gcloud >/dev/null 2>&1 || ! command -v git >/dev/null 2>&1; then
        echo "WARNING: lc_verify_tarball_freshness — gcloud/git unavailable, skipping tarball-freshness check" >&2
        return 0
    fi

    # Resolve workspace root: env override, else up 4 dirs from this lib
    # (<ws>/deployment-service/scripts/vm/lib/launcher_common.sh).
    local ws_root lib_dir
    lib_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    ws_root="${WORKSPACE_ROOT:-$(cd "$lib_dir/../../../.." && pwd)}"

    local fetch_ref fetch_first
    fetch_ref="${LC_TARBALL_FRESHNESS_REF:-live-defi-rollout}"
    fetch_first="$(printf '%s' "${LC_TARBALL_FRESHNESS_FETCH:-false}" | tr '[:upper:]' '[:lower:]')"

    local stale_repos="" stale_count=0
    local repo tarball repo_path expected_sha actual_sha manifest_uri manifest_tmp cmp_n

    for repo in "$@"; do
        tarball="$(lc_tarball_name_for_repo "$repo")"
        repo_path="$ws_root/$repo"

        if [[ ! -d "$repo_path/.git" ]]; then
            echo "WARNING: lc_verify_tarball_freshness — $repo not a git clone at $repo_path, skipping" >&2
            continue
        fi

        # The SHA create-code-tarballs.sh would tar right now (or the fetched
        # origin ref when LC_TARBALL_FRESHNESS_FETCH=true).
        if [[ "$fetch_first" == "true" ]]; then
            git -C "$repo_path" fetch --quiet origin "$fetch_ref" 2>/dev/null || true
            expected_sha="$(git -C "$repo_path" rev-parse "origin/$fetch_ref" 2>/dev/null || echo "")"
        else
            expected_sha="$(git -C "$repo_path" rev-parse HEAD 2>/dev/null || echo "")"
        fi
        if [[ -z "$expected_sha" ]]; then
            echo "WARNING: lc_verify_tarball_freshness — cannot resolve current SHA for $repo, skipping" >&2
            continue
        fi

        # Read the floating tarball's manifest commit_sha from GCS.
        manifest_uri="gs://${code_bucket}/code/${tarball}.manifest.json"
        # Portable mktemp — see lc_write_startup_file's comment for the full
        # BSD/macOS-vs-GNU rationale (a non-X-terminal template doesn't
        # randomize on macOS, colliding across concurrent invocations/xdist
        # workers — this exact call is what test_stale_tarball_warn_does_not_block
        # flaked on under xdist before this fix).
        manifest_tmp="$(mktemp -d)/manifest.json"
        # `gcloud storage`, not `gsutil` — see the ADC-vs-active-account note on
        # lc_resolve_tarball_sha above; the same WIF-token gap made this read a
        # false-MISSING signal in every interactive AO slot.
        if ! gcloud storage cp "$manifest_uri" "$manifest_tmp" --quiet 2>/dev/null; then
            rm -f "$manifest_tmp"
            echo "WARNING: tarball manifest MISSING for $repo ($manifest_uri) — VM would fetch an absent/stale tarball" >&2
            stale_repos="${stale_repos}${repo} "
            stale_count=$((stale_count + 1))
            continue
        fi
        actual_sha="$(python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get('commit_sha',''))" "$manifest_tmp" 2>/dev/null || echo "")"
        rm -f "$manifest_tmp"
        if [[ -z "$actual_sha" ]]; then
            echo "WARNING: tarball manifest for $repo has no commit_sha — cannot verify freshness" >&2
            stale_repos="${stale_repos}${repo} "
            stale_count=$((stale_count + 1))
            continue
        fi

        # Prefix-safe compare (manifest carries full 40-char sha; either side may
        # be short in practice).
        cmp_n=${#expected_sha}
        [[ ${#actual_sha} -lt $cmp_n ]] && cmp_n=${#actual_sha}
        if [[ "${expected_sha:0:$cmp_n}" == "${actual_sha:0:$cmp_n}" ]]; then
            echo "  tarball fresh: $repo ($tarball @ ${actual_sha:0:12})"
        else
            echo "WARNING: STALE tarball for $repo — ${tarball} manifest=${actual_sha:0:12} but repo=${expected_sha:0:12}" >&2
            stale_repos="${stale_repos}${repo} "
            stale_count=$((stale_count + 1))
        fi
    done

    if [[ "$stale_count" -eq 0 ]]; then
        echo "lc_verify_tarball_freshness: all ${#} tarball(s) current."
        return 0
    fi

    # Trim trailing space for clean output.
    stale_repos="${stale_repos% }"

    local republish_cmd="bash ${lib_dir}/../create-code-tarballs.sh"
    local r
    for r in $stale_repos; do
        republish_cmd="${republish_cmd} --include ${r}"
    done

    case "$mode" in
        warn)
            {
                echo "WARNING: ${stale_count} STALE code tarball(s): ${stale_repos}"
                echo "WARNING: the VM you are launching may fetch pre-fix code. Republish before launch:"
                echo "WARNING:   ${republish_cmd}"
                echo "WARNING: (set LC_TARBALL_FRESHNESS=enforce to block, or =auto to republish automatically)"
            } >&2
            return 0
            ;;
        enforce)
            {
                echo "ERROR: ${stale_count} STALE code tarball(s): ${stale_repos}"
                echo "ERROR: refusing to launch a VM onto stale code. Republish first:"
                echo "ERROR:   ${republish_cmd}"
            } >&2
            return 1
            ;;
        auto)
            echo "lc_verify_tarball_freshness: auto-republishing stale tarball(s): ${stale_repos}"
            if $republish_cmd; then
                echo "lc_verify_tarball_freshness: republish complete — re-verifying"
                # Re-verify once in warn mode (no infinite loop) so the operator
                # sees confirmation the republish took.
                LC_TARBALL_FRESHNESS=warn lc_verify_tarball_freshness "$code_bucket" $stale_repos
                return 0
            fi
            echo "ERROR: auto-republish failed for: ${stale_repos} — not launching onto unverified code" >&2
            return 1
            ;;
    esac
}

# ---------------------------------------------------------------------------
# lc_verify_setup_script_freshness <code_bucket> <metadata_str>
# ---------------------------------------------------------------------------
# PRE-LAUNCH guard against the setup-script publish race: create-code-tarballs.sh
# publishes scripts/vm/setup-data-pipeline-vm.sh (the file every Pattern-A VM's
# startup-script-url points at) with no manifest/checksum companion — unlike the
# code tarballs (see lc_verify_tarball_freshness above, which has a
# .manifest.json to compare against). A VM can boot and fetch that GCS object
# BEFORE a fix-then-launch turn's publish has actually landed, silently running
# stale startup logic despite correct instance metadata. Root-caused 2026-07-12
# (morpho lending_indices first backfill VM ran the pre-fix _DEFAULT_PROTOCOLS
# list for ~17 min before the race was noticed). SSOT:
# plans/active/issues/defi_morpho_lending_indices_never_wired_2026_07_12.md.
#
# Called automatically by lc_gcloud_create (not per-launcher) so every caller
# inherits the guard without a per-file wiring pass. Parses <metadata_str> for
# a `startup-script-url=gs://<bucket>/vm/<name>.sh` entry — a no-op (return 0)
# if none is present (e.g. launchers using --metadata-from-file startup-script
# instead of Pattern A). Compares the local script's content hash
# (`gcloud storage hash`, same base64 md5 encoding `gcloud storage objects
# describe` reports) against the live GCS object's md5 before the VM is
# created.
#
# Acts per LC_SETUP_SCRIPT_FRESHNESS (same semantics as LC_TARBALL_FRESHNESS):
#   off      — skip the check entirely.
#   warn     — (DEFAULT) print a loud WARNING + the exact republish remedy,
#              then return 0 (never blocks a launch).
#   enforce  — return 1 (block the launch) if the script is stale or missing.
#   auto     — republish via `gcloud storage cp` the local script over the GCS
#              object, then return 0 (or 1 if the republish itself failed).
#
# Robustness: gcloud unavailable, or the local script file not found, is a
# "can't verify" — warn+skip in every mode (a tooling/path gap never blocks a
# launch; only a genuinely-confirmed stale/missing object blocks in enforce).
lc_verify_setup_script_freshness() {
    local code_bucket="${1:?lc_verify_setup_script_freshness: code_bucket required}"
    local metadata_str="${2:?lc_verify_setup_script_freshness: metadata_str required}"

    local mode
    mode="$(printf '%s' "${LC_SETUP_SCRIPT_FRESHNESS:-warn}" | tr '[:upper:]' '[:lower:]')"
    if [[ "$mode" == "off" ]]; then
        return 0
    fi
    case "$mode" in
        warn|enforce|auto) ;;
        *) echo "WARNING: unknown LC_SETUP_SCRIPT_FRESHNESS='$mode' — treating as 'warn'" >&2; mode="warn" ;;
    esac

    # metadata_str is comma-separated key=value; the GCS URL itself has no commas.
    local gcs_url
    gcs_url="$(printf '%s' "$metadata_str" | grep -oE 'startup-script-url=gs://[^,]+' | sed 's/^startup-script-url=//' || true)"
    if [[ -z "$gcs_url" ]]; then
        return 0
    fi

    local script_name
    script_name="$(basename "$gcs_url")"

    if ! command -v gcloud >/dev/null 2>&1; then
        echo "WARNING: lc_verify_setup_script_freshness — gcloud unavailable, skipping freshness check for $script_name" >&2
        return 0
    fi

    local lib_dir local_path
    lib_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local_path="$lib_dir/../${script_name}"
    if [[ ! -f "$local_path" ]]; then
        echo "WARNING: lc_verify_setup_script_freshness — local script $local_path not found, skipping" >&2
        return 0
    fi

    # `gcloud storage`, not `gsutil` — gsutil resolves creds from the CLI's active
    # account (a short-lived WIF token in an interactive AO slot can't refresh
    # unattended), while `gcloud storage` resolves via ADC, which stays valid. Both
    # sides use the same base64 md5 encoding `gsutil hash`/`gsutil stat` used, so
    # the direct string comparison below is unchanged. See
    # plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
    local local_md5 remote_md5
    local_md5="$(gcloud storage hash "$local_path" --skip-crc32c --format='value(md5_hash)' 2>/dev/null)"
    remote_md5="$(gcloud storage objects describe "$gcs_url" --format='value(md5_hash)' 2>/dev/null)"

    if [[ -z "$local_md5" ]]; then
        echo "WARNING: lc_verify_setup_script_freshness — could not hash local $local_path, skipping" >&2
        return 0
    fi

    local republish_cmd="gcloud storage cp ${local_path} ${gcs_url} --quiet"

    if [[ -z "$remote_md5" ]]; then
        echo "WARNING: setup script MISSING on GCS: $gcs_url — the VM would fail to fetch its startup-script-url" >&2
        case "$mode" in
            enforce)
                echo "ERROR: refusing to launch. Republish first: ${republish_cmd}" >&2
                return 1
                ;;
            auto)
                echo "lc_verify_setup_script_freshness: publishing $script_name"
                if ! $republish_cmd; then
                    echo "ERROR: publish failed for $gcs_url — not launching onto an unverified startup script" >&2
                    return 1
                fi
                ;;
            *) echo "WARNING: republish before launch: ${republish_cmd}" >&2 ;;
        esac
        return 0
    fi

    if [[ "$local_md5" == "$remote_md5" ]]; then
        echo "  setup script fresh: $script_name"
        return 0
    fi

    local msg="STALE setup script on GCS: $gcs_url content does not match local $local_path — a VM booting now would silently run stale startup logic despite a landed fix"
    case "$mode" in
        warn)
            {
                echo "WARNING: ${msg}"
                echo "WARNING: republish before launch: ${republish_cmd}"
                echo "WARNING: (set LC_SETUP_SCRIPT_FRESHNESS=enforce to block, or =auto to republish automatically)"
            } >&2
            return 0
            ;;
        enforce)
            {
                echo "ERROR: ${msg}"
                echo "ERROR: refusing to launch. Republish first: ${republish_cmd}"
            } >&2
            return 1
            ;;
        auto)
            echo "lc_verify_setup_script_freshness: auto-republishing $script_name"
            if $republish_cmd; then
                echo "lc_verify_setup_script_freshness: republish complete"
                return 0
            fi
            echo "ERROR: auto-republish failed for $gcs_url — not launching onto an unverified startup script" >&2
            return 1
            ;;
    esac
}

# ---------------------------------------------------------------------------
# lc_log_upload_trap_block <vm_name> <project_id> [asset_group] [task]
# ---------------------------------------------------------------------------
# Emit a bash snippet to inject AT THE TOP of an inline VM startup-script
# heredoc body. The snippet gives a bespoke (non-setup-data-pipeline-vm.sh)
# launcher the SAME durable-observability contract the canonical
# `vm-exec-with-gcs-tee.sh` path provides — without the heavyweight
# tarball+venv+heartbeat-daemon install. It:
#
#   1. Tees stdout+stderr to /var/log/run.log (so every echo is captured).
#   2. Starts a CONTINUOUS background streamer that uploads the growing log
#      to the canonical GCS path
#        gs://deployment-scripts-<project>/vm-logs/<vm-name>/run.log
#      every LC_LOG_STREAM_INTERVAL seconds (default 30), AND writes a
#      liveness/progress heartbeat blob
#        gs://deployment-scripts-<project>/vm-heartbeat/<vm-name>.txt
#      every tick. So a dead/hung VM's log is queryable from GCS WITHOUT
#      SSH (≤30 s loss), and the external vm-zombie-watchdog (which reads
#      that heartbeat blob) can reap a wedged VM. The blob also carries the
#      asset_group/task/start-epoch so liveness is attributable.
#   3. Installs an EXIT trap that ALWAYS does a final upload of the log to
#      GCS regardless of exit status (success, error, signal, `set -e`
#      propagation), writes the terminal EXIT_STATUS marker blob
#        gs://deployment-scripts-<project>/vm-logs/<vm-name>/EXIT_STATUS
#      (the durable STOPPED/FAILED signal: rc=0 → completed, rc!=0 →
#      failed), prints a final marker, stops the streamer, then schedules
#      `shutdown -h +1` to flush before the VM goes away.
#
#   4. BEFORE any of the above, immediately overwrites EXIT_STATUS with a
#      "RUNNING" sentinel. This closes a false-success gap found 2026-07-13
#      (features_sports_unbounded_memory_early_history_dates issue doc): a
#      SIGKILL that takes down the WHOLE systemd unit (wrapper shell + tee +
#      workload child all at once, e.g. a fast OOM) bypasses bash EXIT traps
#      entirely — nothing can trap SIGKILL — so `trap _lc_final_upload EXIT`
#      never runs and the real rc is never written. Without this sentinel, a
#      relaunch that reuses the same `vm_name` (the norm for backfill shards)
#      would leave the STALE terminal EXIT_STATUS from that vm_name's PRIOR
#      run — e.g. an earlier successful "0" — silently masquerading as this
#      run's result. `RUNNING` is not a valid integer, so
#      `data_pipeline_monitors._gcs.read_terminal_exit_code()` (the canonical
#      reader) fails its `int()` parse and correctly falls through to
#      `None` ("never terminated, never a success") instead of misreading a
#      stale 0 — matching its own documented contract. The trap itself still
#      cannot survive a whole-unit SIGKILL (no bash mechanism can), but the
#      OBSERVABLE terminal signal now can never falsely read "completed" for
#      a run that never got the chance to finish.
#
# Why this exists: inline startup scripts that put `gsutil cp` only at the
# END of the script body lose ALL logs if anything fails before that line,
# AND show a FROZEN log for the whole run (no progress visibility) — both
# observed 2026-05-19 (mtds-solana-drift TERMINATED after 7min, no run.log)
# and 2026-06 (SFI + gas-fees uploaders stalled, logs froze + were lost on
# termination, forcing serial-port/SSH diagnosis). The canonical pattern is
# `vm-exec-with-gcs-tee.sh` via `setup-data-pipeline-vm.sh` (30s stream +
# lifecycle events + log-archive backup); this helper is the SSOT
# lightweight equivalent for launchers that inline their own startup script.
# SSOT: codex/05-infrastructure/vm-tarball-deployment.md § "VM observability".
#
# Lifecycle/cleanup: the vm-logs/ + vm-heartbeat/ prefixes have GCS lifecycle
# delete rules (14d / 15d) + a daily log-archive Cloud Run job
# (terraform/gcp/main.tf deployment_scripts bucket +
# vm_log_archival_scheduler.tf) — logs never accumulate forever.
#
# CALLER MUST emit this snippet inside the heredoc BEFORE any `set -e`
# or workload commands. Inside the heredoc body the snippet's bash $-vars
# are pre-escaped with \$ — caller does NOT need to double-escape.
# Override the stream cadence with LC_LOG_STREAM_INTERVAL in the launcher env.
#
# Usage (inside a launcher):
#     STARTUP_FILE=$(mktemp)
#     LOG_TRAP="$(lc_log_upload_trap_block "$VM_NAME" "$PROJECT_ID" defi gas-fees)"
#     cat > "$STARTUP_FILE" <<STARTUP_EOF
#     #!/bin/bash
#     ${LOG_TRAP}
#     set -euo pipefail
#     # ... rest of script ...
#     STARTUP_EOF
lc_log_upload_trap_block() {
    local vm_name="${1:?lc_log_upload_trap_block: vm_name required}"
    local project_id="${2:?lc_log_upload_trap_block: project_id required}"
    local asset_group="${3:-UNKNOWN}"
    local task="${4:-vm-exec}"
    local code_bucket="deployment-scripts-${project_id}"
    local stream_interval="${LC_LOG_STREAM_INTERVAL:-30}"
    cat <<TRAPSNIPPET
# --- lc_log_upload_trap_block (deployment-service/scripts/vm/lib/launcher_common.sh) ---
# Durable observability for an inline startup script: continuous GCS log
# stream (every ${stream_interval}s) + liveness/progress heartbeat blob +
# terminal EXIT_STATUS marker + guaranteed final upload on any exit.
# Canonical run.log path: gs://${code_bucket}/vm-logs/${vm_name}/run.log
LOG_LOCAL="/var/log/run.log"
GCS_LOG_URI="gs://${code_bucket}/vm-logs/${vm_name}/run.log"
GCS_HEARTBEAT_URI="gs://${code_bucket}/vm-heartbeat/${vm_name}.txt"
GCS_EXIT_URI="gs://${code_bucket}/vm-logs/${vm_name}/EXIT_STATUS"
LC_STREAM_INTERVAL=${stream_interval}
LC_VM_ASSET_GROUP="${asset_group}"
LC_VM_TASK="${task}"
LC_START_EPOCH=\$(date +%s)
mkdir -p "\$(dirname "\$LOG_LOCAL")" 2>/dev/null || true
# Stamp a non-terminal RUNNING sentinel FIRST, before anything else — so a
# same-named relaunch never leaves a PRIOR run's stale terminal EXIT_STATUS
# (e.g. a stale "0") readable as this run's result if a whole-unit SIGKILL
# (e.g. a fast OOM) kills this script before the EXIT trap below ever fires.
echo "RUNNING" | timeout 30 gcloud storage cp - "\$GCS_EXIT_URI" --quiet 2>/dev/null || true
exec > >(tee -a "\$LOG_LOCAL") 2>&1
echo "=== VM STARTED \$(date -u +'%Y-%m-%dT%H:%M:%SZ') vm=${vm_name} asset_group=${asset_group} task=${task} ==="
# Continuous streamer: ship the growing log to GCS + write a heartbeat blob
# every \$LC_STREAM_INTERVAL seconds so progress is visible WITHOUT SSH and a
# hung VM's log is never lost. Runs detached; killed by the EXIT trap.
_lc_stream_loop() {
    while true; do
        timeout 60 gcloud storage cp "\$LOG_LOCAL" "\$GCS_LOG_URI" --quiet 2>/dev/null || true
        printf 'vm=%s asset_group=%s task=%s alive_at=%s uptime_s=%s\n' \
            "${vm_name}" "\$LC_VM_ASSET_GROUP" "\$LC_VM_TASK" \
            "\$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "\$(( \$(date +%s) - LC_START_EPOCH ))" \
            | timeout 30 gcloud storage cp - "\$GCS_HEARTBEAT_URI" --quiet 2>/dev/null || true
        sleep "\$LC_STREAM_INTERVAL"
    done
}
_lc_stream_loop &
_LC_STREAM_PID=\$!
_lc_final_upload() {
    local rc=\$?
    kill "\$_LC_STREAM_PID" 2>/dev/null || true
    echo ""
    echo "=== VM EXIT rc=\$rc \$(date -u +'%Y-%m-%dT%H:%M:%SZ') ==="
    # Terminal STOPPED/FAILED signal — durable in GCS even if the VM dies.
    echo "\$rc" | timeout 30 gcloud storage cp - "\$GCS_EXIT_URI" --quiet 2>/dev/null || true
    for _i in 1 2 3; do
        if timeout 60 gcloud storage cp "\$LOG_LOCAL" "\$GCS_LOG_URI" --quiet 2>/dev/null; then
            echo "log uploaded to \$GCS_LOG_URI (attempt \$_i)"
            break
        fi
        echo "log upload attempt \$_i failed, retrying in 5s..."
        sleep 5
    done
    sleep 2
    timeout 60 gcloud storage cp "\$LOG_LOCAL" "\$GCS_LOG_URI" --quiet 2>/dev/null || true
    # On completion: self-DELETE iff the launcher set VM_SHUTDOWN_ON_COMPLETION=true
    # (ephemeral batch / validation VMs), else just STOP (recurring cron / persistent
    # VMs that omit the flag). Mirrors the proven self-delete in
    # vm-exec-with-gcs-tee.sh / setup-data-pipeline-vm.sh — without this a completed
    # VM only halts (TERMINATED) and orphans its boot disk as a recurring billing cost.
    _lc_sd=\$(curl -sf -H 'Metadata-Flavor: Google' \
        'http://metadata.google.internal/computeMetadata/v1/instance/attributes/VM_SHUTDOWN_ON_COMPLETION' 2>/dev/null || echo '')
    if [[ "\$_lc_sd" == "true" ]]; then
        _lc_zone=\$(curl -sf -H 'Metadata-Flavor: Google' \
            'http://metadata.google.internal/computeMetadata/v1/instance/zone' 2>/dev/null | awk -F/ '{print \$NF}')
        if [[ -n "\$_lc_zone" ]]; then
            gcloud compute instances delete '${vm_name}' --zone="\$_lc_zone" --quiet --delete-disks=all \
                || sudo shutdown -h +1 2>/dev/null || true
        else
            sudo shutdown -h +1 2>/dev/null || true
        fi
    else
        shutdown -h +1 2>/dev/null || true
    fi
    return \$rc
}
trap _lc_final_upload EXIT
# --- end lc_log_upload_trap_block ---
TRAPSNIPPET
}
