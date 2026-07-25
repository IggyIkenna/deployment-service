#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# Reap zombie VMs — instances whose run.log has a terminal rc=NNN line but
# whose GCE instance is still RUNNING (or whose run.log has gone silent for
# longer than a heartbeat window).
#
# History (2026-04-22, L1 in
# ``unified-trading-pm/plans/active/manifest_schema_v6_quote_margin_combo_2026_04_23.md``):
# The first-generation Fleet Monitor used ``tail -1`` on ``run.log``, which
# missed the CMD's ``rc=NNN`` line because the heartbeat daemon appends
# ``DEPLOYMENT_COMPLETED`` AFTER. Result: Monitor reported "0 zombies / 95
# running" for 8+ hours while 49/95 VMs had actually crashed.
#
# This reaper uses ``tail -10`` and catches the rc= line regardless of where
# the heartbeat / trailing lines land. It is idempotent — running twice is
# harmless.
#
# Usage:
#   bash reap-zombies.sh --project <gcp-project> --zone <zone> [--prefix <name>] [--bucket <log-bucket>]
#       [--dry-run] [--silence-threshold-sec <N>]
#
# Exit code 0 = success (even if zero zombies found).
# Exit code 2 = one or more reap attempts failed — inspect STDERR.

set -euo pipefail

PROJECT=""
ZONE=""
PREFIX=""
BUCKET=""
DRY_RUN="false"
SILENCE_SEC=600  # 10 minutes — if run.log hasn't been touched in this long, treat as zombie

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project) PROJECT="$2"; shift 2 ;;
        --zone) ZONE="$2"; shift 2 ;;
        --prefix) PREFIX="$2"; shift 2 ;;
        --bucket) BUCKET="$2"; shift 2 ;;
        --dry-run) DRY_RUN="true"; shift ;;
        --silence-threshold-sec) SILENCE_SEC="$2"; shift 2 ;;
        -h|--help)
            sed -n '1,30p' "$0"
            exit 0
            ;;
        *) echo "unknown flag: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$PROJECT" || -z "$ZONE" ]]; then
    echo "error: --project and --zone are required" >&2
    exit 1
fi

if [[ -z "$BUCKET" ]]; then
    # Best-effort convention: deployment-scripts-{project}.
    BUCKET="deployment-scripts-${PROJECT}"
fi

log() { echo "[reap-zombies] $*"; }

# Enumerate RUNNING instances that match prefix.
filter="status=RUNNING"
if [[ -n "$PREFIX" ]]; then
    filter="${filter} AND name~^${PREFIX}"
fi

log "Enumerating instances — project=$PROJECT zone=$ZONE filter='${filter}'"
instances=$(gcloud compute instances list \
    --project="$PROJECT" \
    --zones="$ZONE" \
    --filter="$filter" \
    --format="value(name)" \
    2>/dev/null) || {
    echo "error: gcloud compute instances list failed" >&2
    exit 2
}

if [[ -z "$instances" ]]; then
    log "No running instances matched — nothing to reap."
    exit 0
fi

reaped=0
failed=0
now_epoch=$(date +%s)

while IFS= read -r instance; do
    [[ -z "$instance" ]] && continue
    log_path="gs://${BUCKET}/logs/${instance}/run.log"

    # Read the last 10 lines (L1 correct pattern — NOT tail -1).
    # `gcloud storage`, not `gsutil` — gsutil resolves creds from the CLI's
    # active account (a short-lived WIF token in an interactive AO slot can't
    # refresh unattended), while `gcloud storage` resolves via ADC, which stays
    # valid. See
    # plans/active/issues/vm_tarball_upload_expired_wif_token_interactive_slot_2026_07_25.md.
    tail_text=$(gcloud storage cat "$log_path" 2>/dev/null | tail -10 || true)

    is_zombie="false"
    reason=""

    if [[ -z "$tail_text" ]]; then
        # No run.log at all — could be a VM that hasn't started yet. Check
        # creation time: if older than SILENCE_SEC, reap.
        ct=$(gcloud compute instances describe "$instance" \
            --project="$PROJECT" --zone="$ZONE" \
            --format="value(creationTimestamp)" 2>/dev/null || true)
        if [[ -n "$ct" ]]; then
            ct_epoch=$(date -d "$ct" +%s 2>/dev/null || date -j -f "%Y-%m-%dT%H:%M:%S" "${ct%%.*}" +%s 2>/dev/null || echo 0)
            if [[ $((now_epoch - ct_epoch)) -gt $SILENCE_SEC ]]; then
                is_zombie="true"
                reason="no run.log + created ${SILENCE_SEC}s+ ago"
            fi
        fi
    elif echo "$tail_text" | grep -qE "rc=[0-9]+"; then
        # CMD exited. Reap regardless of rc.
        is_zombie="true"
        rc_line=$(echo "$tail_text" | grep -oE "rc=[0-9]+" | tail -1)
        reason="terminal rc present (${rc_line})"
    else
        # run.log exists but no rc= — check last-modified age.
        last_update=$(gcloud storage objects describe "$log_path" --format='value(update_time)' 2>/dev/null || true)
        if [[ -n "$last_update" ]]; then
            lu_epoch=$(date -d "$last_update" +%s 2>/dev/null || echo 0)
            if [[ $((now_epoch - lu_epoch)) -gt $SILENCE_SEC ]]; then
                is_zombie="true"
                reason="run.log silent for ${SILENCE_SEC}s+"
            fi
        fi
    fi

    if [[ "$is_zombie" != "true" ]]; then
        log "skip $instance — healthy"
        continue
    fi

    log "REAP $instance — ${reason}"
    if [[ "$DRY_RUN" == "true" ]]; then
        log "  (dry-run) would run: gcloud compute instances delete $instance --zone=$ZONE"
        continue
    fi

    if gcloud compute instances delete "$instance" \
        --project="$PROJECT" \
        --zone="$ZONE" \
        --quiet \
        >/dev/null 2>&1; then
        reaped=$((reaped + 1))
    else
        echo "error: failed to delete $instance" >&2
        failed=$((failed + 1))
    fi
done <<< "$instances"

log "Done — reaped=$reaped failed=$failed dry_run=$DRY_RUN"

if [[ $failed -gt 0 ]]; then
    exit 2
fi
