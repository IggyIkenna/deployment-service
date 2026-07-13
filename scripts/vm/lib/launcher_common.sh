#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# VM launcher shared library — source this file from launch-*.sh scripts.
#
# Provides:
#   lc_validate_env <env>                  — validate prod|staging|dev; exit 1 on bad value
#   lc_singleton_check <prefix> <zone> <project> [force=false]
#                                          — refuse duplicate launch unless force=true
#   lc_gcloud_create <vm_name> <project> <zone> <machine_type> <disk_gb> <metadata> <labels>
#                                          — standard gcloud compute instances create wrapper
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
  Stop:    gcloud compute instances delete ${existing} --zone=${zone} --project=${project} --quiet
  Force:   bash \$0 --force
EOF
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# lc_gcs_object_age_seconds <gcs_uri>
# ---------------------------------------------------------------------------
# Seconds elapsed since <gcs_uri>'s "Update time" (per `gsutil stat`). Prints
# the integer to stdout; returns non-zero if the object doesn't exist, gsutil
# is unavailable, or the timestamp can't be parsed. Uses python3 (already a
# hard dependency everywhere gcloud/gsutil run) for portable RFC-2822 parsing
# instead of GNU-only `date -d` (this file targets bash 3.2+/macOS too).
lc_gcs_object_age_seconds() {
    local gcs_uri="${1:?lc_gcs_object_age_seconds: gcs_uri required}"
    command -v gsutil >/dev/null 2>&1 || return 1
    local updated
    updated="$(gsutil stat "$gcs_uri" 2>/dev/null | sed -n 's/^[[:space:]]*Update time:[[:space:]]*//p')"
    [[ -n "$updated" ]] || return 1
    python3 -c "
import sys, time
from email.utils import parsedate_to_datetime
try:
    ts = parsedate_to_datetime(sys.argv[1]).timestamp()
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
#                  <metadata_str> <labels_str>
# ---------------------------------------------------------------------------
# Standard gcloud compute instances create call with the workspace's canonical
# image (ubuntu-2404-lts-amd64), cloud-platform scope, and no-restart-on-failure.
#
# <metadata_str> : comma-separated key=value pairs (e.g. "VM_TASK=foo,ENV=prod")
# <labels_str>   : comma-separated key=value pairs (e.g. "purpose=foo,env=prod")
#
# Example:
#   lc_gcloud_create "$VM_NAME" "$PROJECT" "$ZONE" "e2-standard-4" "50" \
#       "VM_TASK=foo,DEPLOYMENT_ENV=prod" \
#       "purpose=foo,env=prod"
lc_gcloud_create() {
    local vm_name="${1:?lc_gcloud_create: vm_name required}"
    local project="${2:?lc_gcloud_create: project required}"
    local zone="${3:?lc_gcloud_create: zone required}"
    local machine_type="${4:?lc_gcloud_create: machine_type required}"
    local disk_gb="${5:?lc_gcloud_create: disk_gb required}"
    local metadata_str="${6:?lc_gcloud_create: metadata_str required}"
    local labels_str="${7:?lc_gcloud_create: labels_str required}"

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
        return 0
    fi

    # PRE-LAUNCH guard against the setup-script publish race (see
    # lc_verify_setup_script_freshness below) — checked here, not per-launcher,
    # so every caller of lc_gcloud_create (~80 launchers) inherits it automatically.
    if ! lc_verify_setup_script_freshness "$(lc_code_bucket "$project")" "$metadata_str"; then
        return 1
    fi

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
    STARTUP_FILE="$(mktemp /tmp/startup-XXXX.sh)"
    printf '%s' "$content" > "$STARTUP_FILE"
    # Register cleanup — if caller already has an EXIT trap, this appends to it.
    # shellcheck disable=SC2064
    trap "rm -f '${STARTUP_FILE}'" EXIT
    export STARTUP_FILE
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
# Robustness: git/gsutil unavailable, or a repo dir that isn't a git clone, is a
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
    mode="$(printf '%s' "${LC_TARBALL_FRESHNESS:-warn}" | tr '[:upper:]' '[:lower:]')"
    if [[ "$mode" == "off" ]]; then
        return 0
    fi
    case "$mode" in
        warn|enforce|auto) ;;
        *) echo "WARNING: unknown LC_TARBALL_FRESHNESS='$mode' — treating as 'warn'" >&2; mode="warn" ;;
    esac

    # Tooling preconditions — a missing tool means we can't verify anything;
    # warn once and let the launch proceed (never block on a tooling gap).
    if ! command -v gsutil >/dev/null 2>&1 || ! command -v git >/dev/null 2>&1; then
        echo "WARNING: lc_verify_tarball_freshness — gsutil/git unavailable, skipping tarball-freshness check" >&2
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
        manifest_tmp="$(mktemp /tmp/lc-manifest-XXXX.json)"
        if ! gsutil -q cp "$manifest_uri" "$manifest_tmp" 2>/dev/null; then
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
# startup-script-url points at) via a plain, unversioned `gsutil cp` with no
# manifest/checksum companion — unlike the code tarballs (see
# lc_verify_tarball_freshness above, which has a .manifest.json to compare
# against). A VM can boot and fetch that GCS object BEFORE a fix-then-launch
# turn's `gsutil cp` has actually landed, silently running stale startup logic
# despite correct instance metadata. Root-caused 2026-07-12 (morpho
# lending_indices first backfill VM ran the pre-fix _DEFAULT_PROTOCOLS list for
# ~17 min before the race was noticed). SSOT:
# plans/active/issues/defi_morpho_lending_indices_never_wired_2026_07_12.md.
#
# Called automatically by lc_gcloud_create (not per-launcher) so every caller
# inherits the guard without a per-file wiring pass. Parses <metadata_str> for
# a `startup-script-url=gs://<bucket>/vm/<name>.sh` entry — a no-op (return 0)
# if none is present (e.g. launchers using --metadata-from-file startup-script
# instead of Pattern A). Compares the local script's content hash
# (`gsutil hash -m`, matches `gsutil stat`'s reporting format) against the live
# GCS object's md5 before the VM is created.
#
# Acts per LC_SETUP_SCRIPT_FRESHNESS (same semantics as LC_TARBALL_FRESHNESS):
#   off      — skip the check entirely.
#   warn     — (DEFAULT) print a loud WARNING + the exact republish remedy,
#              then return 0 (never blocks a launch).
#   enforce  — return 1 (block the launch) if the script is stale or missing.
#   auto     — republish via `gsutil cp` the local script over the GCS object,
#              then return 0 (or 1 if the republish itself failed).
#
# Robustness: gsutil unavailable, or the local script file not found, is a
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

    if ! command -v gsutil >/dev/null 2>&1; then
        echo "WARNING: lc_verify_setup_script_freshness — gsutil unavailable, skipping freshness check for $script_name" >&2
        return 0
    fi

    local lib_dir local_path
    lib_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local_path="$lib_dir/../${script_name}"
    if [[ ! -f "$local_path" ]]; then
        echo "WARNING: lc_verify_setup_script_freshness — local script $local_path not found, skipping" >&2
        return 0
    fi

    local local_md5 remote_md5
    local_md5="$(gsutil hash -m "$local_path" 2>/dev/null | awk -F': ' '/Hash \(md5\):/ {gsub(/^ +| +$/, "", $2); print $2}')"
    remote_md5="$(gsutil stat "$gcs_url" 2>/dev/null | awk -F': ' '/Hash \(md5\):/ {gsub(/^ +| +$/, "", $2); print $2}')"

    if [[ -z "$local_md5" ]]; then
        echo "WARNING: lc_verify_setup_script_freshness — could not hash local $local_path, skipping" >&2
        return 0
    fi

    local republish_cmd="gsutil cp ${local_path} ${gcs_url}"

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
exec > >(tee -a "\$LOG_LOCAL") 2>&1
echo "=== VM STARTED \$(date -u +'%Y-%m-%dT%H:%M:%SZ') vm=${vm_name} asset_group=${asset_group} task=${task} ==="
# Continuous streamer: ship the growing log to GCS + write a heartbeat blob
# every \$LC_STREAM_INTERVAL seconds so progress is visible WITHOUT SSH and a
# hung VM's log is never lost. Runs detached; killed by the EXIT trap.
_lc_stream_loop() {
    while true; do
        gsutil -q cp "\$LOG_LOCAL" "\$GCS_LOG_URI" 2>/dev/null || true
        printf 'vm=%s asset_group=%s task=%s alive_at=%s uptime_s=%s\n' \
            "${vm_name}" "\$LC_VM_ASSET_GROUP" "\$LC_VM_TASK" \
            "\$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "\$(( \$(date +%s) - LC_START_EPOCH ))" \
            | gsutil -q cp - "\$GCS_HEARTBEAT_URI" 2>/dev/null || true
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
    echo "\$rc" | gsutil -q cp - "\$GCS_EXIT_URI" 2>/dev/null || true
    for _i in 1 2 3; do
        if gsutil -q cp "\$LOG_LOCAL" "\$GCS_LOG_URI" 2>/dev/null; then
            echo "log uploaded to \$GCS_LOG_URI (attempt \$_i)"
            break
        fi
        echo "log upload attempt \$_i failed, retrying in 5s..."
        sleep 5
    done
    sleep 2
    gsutil -q cp "\$LOG_LOCAL" "\$GCS_LOG_URI" 2>/dev/null || true
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
