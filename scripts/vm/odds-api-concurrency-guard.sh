#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
#
# ODDS API CONCURRENT-VM / COST GUARD — sourced by launch-mtds-sports-odds-backfill-vm.sh.
#
# Root cause this closes (unified-trading-pm/plans/active/issues/
# odds_api_key_quota_exhausted_4_days_after_provisioning_2026_08_02.md): 5+ uncoordinated
# mtds-backfill-odds-* VM relaunches within a 4-day window (several after OOM crashes, 5
# explicitly parallel via --allow-parallel bypassing the launcher's own singleton lock)
# burned an entire 5,000,000-credit/month odds-api-key allocation to negative
# (x-requests-remaining: -772) with no aggregate cost accounting anywhere in the launch path.
#
# Unlike the Tardis venues (tardis-concurrency-guard.sh — single authenticated IP, so N>1
# VMs cause a mutual-403 storm), The Odds API has no IP lock: the risk here is CREDIT
# BUDGET, not connection contention. --allow-parallel exists for a real, legitimate use (5
# parallel `split1..split5` shards, 2026-07-29) so it stays available — it was simply
# UNGATED on any count. This guard keeps --allow-parallel working but caps the concurrent
# fleet instead of leaving it unlimited, mirroring the Tardis guard's fail-closed contract
# and slot-reservation prose without inheriting its single-IP-specific cap of 1.
#
# Usage (from the launcher):
#   source "$(dirname "$0")/odds-api-concurrency-guard.sh"
#   odds_api_concurrency_guard "<planned_vm_count>" "<zone>" "<project>" "<cap>"
#
# FAIL-CLOSED: if the fleet cannot be enumerated (gcloud missing, an API/auth error,
# unparseable output), the guard REFUSES rather than reading the failure as "0 running" — an
# unenumerable fleet is exactly the moment an unbounded relaunch wave would otherwise sail
# through. ODDS_API_GUARD_FORCE=1 is the explicit operator override (deliberately a
# DIFFERENT variable than the launcher's own --force/$FORCE, which controls VM_FORCE
# reprocessing metadata and used to ALSO silently bypass the old singleton lock as a side
# effect — that collision was part of this incident's root cause, so the guard override is
# now a separate, explicit opt-in).
ODDS_API_VM_NAME_PATTERN='^mtds-backfill-odds-'

odds_api_running_vm_count() { # $1=zone $2=project -> echoes count; returns 1 if undeterminable
  local zone="$1" project="$2" raw rc count
  if ! command -v gcloud >/dev/null 2>&1; then
    echo "ERROR: [odds-api-guard] gcloud not found on PATH -- cannot enumerate the odds-api backfill fleet (fail-closed)." >&2
    return 1
  fi
  raw="$(gcloud compute instances list \
    --filter="name~\"${ODDS_API_VM_NAME_PATTERN}\" AND (status=RUNNING OR status=PROVISIONING OR status=STAGING)" \
    --zones="$zone" --project="$project" \
    --format='value(name)' 2>&1)"
  rc=$?
  if (( rc != 0 )); then
    echo "ERROR: [odds-api-guard] 'gcloud compute instances list' failed (exit ${rc}) for zone=${zone} project=${project} -- fail-closed. gcloud said: ${raw}" >&2
    return 1
  fi
  count="$(printf '%s\n' "$raw" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')"
  echo "$count"
}

odds_api_concurrency_guard() { # $1=planned_vm_count $2=zone $3=project $4=cap
  local planned="$1" zone="$2" project="$3" cap="$4"
  local existing total
  if ! existing="$(odds_api_running_vm_count "$zone" "$project")"; then
    if [[ "${ODDS_API_GUARD_FORCE:-0}" == "1" ]]; then
      echo "[odds-api-guard] WARN: could not enumerate the running odds-api backfill fleet, but ODDS_API_GUARD_FORCE=1 -- proceeding on operator override." >&2
      return 0
    fi
    cat >&2 <<EOF
ERROR: [odds-api-guard] REFUSING to launch: the number of running mtds-backfill-odds-* VMs
could not be determined (see the error above). Fail-closed -- an unknown count is treated
as over-cap.

Fix the probe, then re-run:
  gcloud auth application-default login      # expired / missing ADC
  gcloud compute instances list --zones=$zone --project=$project   # confirm it returns
Operator override: ODDS_API_GUARD_FORCE=1 (launches WITHOUT knowing the current fleet size).
EOF
    return 1
  fi
  total=$(( existing + planned ))
  if (( total <= cap )); then
    echo "[odds-api-guard] OK: $existing running + $planned planned = $total <= cap $cap"
    return 0
  fi
  if [[ "${ODDS_API_GUARD_FORCE:-0}" == "1" ]]; then
    echo "[odds-api-guard] WARN: cap $cap exceeded ($existing running + $planned planned) but ODDS_API_GUARD_FORCE=1 -- proceeding on operator override." >&2
    return 0
  fi
  cat >&2 <<EOF
ERROR: [odds-api-guard] REFUSING to launch: $existing mtds-backfill-odds-* VM(s) already
running/coming up + $planned planned = $total, over the cap of $cap.

Root cause this guard exists for (odds_api_key_quota_exhausted_4_days_after_provisioning_2026_08_02.md):
5+ uncoordinated/parallel backfill relaunches against the same shared, unbudgeted
odds-api-key burned the entire 5,000,000-credit/month allocation to negative in under 4
days. --allow-parallel legitimately bypasses the singleton lock for sharded runs but was
previously UNGATED on any count.

Options:
  Inspect running odds-api backfill VMs:
    gcloud compute instances list --filter='name~"${ODDS_API_VM_NAME_PATTERN}"' --zones=$zone --project=$project
  Wait for a running VM to finish, then launch the next shard
  Raise the cap deliberately (only if you have verified remaining credit budget covers it)
  Operator override: ODDS_API_GUARD_FORCE=1 (accepts the credit-burn risk this guard exists to prevent)
EOF
  return 1
}
