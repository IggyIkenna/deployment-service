#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
#
# TARDIS CONCURRENT-VM CAP — shared guard sourced by every launcher that creates a
# Tardis-consuming VM (cefi sharded backfill GCP/AWS, mtds cefi backfill/pipelinecheck).
#
# HARD RULE (operator, 2026-07-16): at most **1** Tardis-consuming VM runs at a time
# (was 3 — see the cap comment below for the measured reason it changed), and every
# launcher MUST count the existing fleet before creating new ones.
#
# Why 1 (empirical — SSOT:
# unified-trading-pm/plans/active/cefi_completion_program_2026_07_15.md, and the superseded
# N=3 reading in issues/tardis_concurrent_ip_lockout_2026_07_12.md): the shared academic key
# allows ONE active IP. Every N>1 datapoint is a mutual-403 storm once VMs do REAL fetching:
# N=6 lease-OFF (2026-07-13) — all six died on the stall watchdog, zero progress.
# N=3 lease-ON (2026-07-16, in the real gap) — 10,300x403/912 ok and 15,034x403/0 ok,
#   +37,212 FALSE attempted_failed in 8h, coverage 52.13 -> 48.38 (BACKWARD).
# N=1 (2026-07-16) — ZERO 403s, clean fetching, cpu 104%/1600%, rss 7.8GB/128GB.
# The earlier "N=3 grinds at 50-70%" reading is SUPERSEDED: it was measured while the VMs
# re-walked already-captured 2020 data (skip-scans barely touch Tardis, so contention looked
# mild). It never held for real gap fetching.
#
# Behaviour:
#   - counts RUNNING GCP VMs matching TARDIS_VM_NAME_PATTERN in the fleet zone
#     (+ running/pending AWS sharded-backfill instances when AWS_REGION is set and
#     the aws CLI is available — both clouds share the ONE Tardis key);
#   - refuses when existing + planned > TARDIS_MAX_CONCURRENT_VMS (default 1),
#     lease or no lease (the cap is the operator's hard rule);
#   - warns (but proceeds) when launching under the cap WITHOUT the lease enabled;
#   - FORCE=1 downgrades refusal to a warning (operator override, matches the
#     launchers' existing FORCE semantics).
#
# Usage (from a launcher):
#   source "$(dirname "$0")/tardis-concurrency-guard.sh"
#   tardis_concurrency_guard "<planned_vm_count>" "<zone>" "<project>"   # exits 1 on refusal

# CAP = 1 (operator, 2026-07-16). Was 3 (operator 2026-07-14) — that figure was calibrated
# on the WRONG regime: the 3-VM wave it was measured against was re-walking already-captured
# 2020 data, where skip-scans need few real Tardis calls so contention looked survivable.
# In the REAL gap every cell needs a live fetch, and N=3 measured 2026-07-16 produced a
# mutual-403 storm: 10,300 x 403 / 912 successes on one VM, 15,034 x 403 / ZERO successes on
# another, +37,212 FALSE attempted_failed rows in 8h, and coverage went BACKWARD 52.13->48.38.
# The lease AMPLIFIES it: its fail-open path ("could not acquire within 1800s — proceeding
# WITHOUT the single-IP lock") means at N>1 every VM waits 30 min then they all proceed
# unlocked simultaneously. At N=1, measured: ZERO 403s, cpu=104%/1600%, rss=7.8GB/128GB.
# Scale THROUGHPUT with intra-VM concurrency on the one IP (TARDIS_MAX_CONCURRENT_DOWNLOADS,
# TARDIS_BOOK_SNAPSHOT_MAX_CONCURRENT) + SINGLE_VM_QUEUE=1 bundling, NEVER with more VMs.
TARDIS_MAX_CONCURRENT_VMS="${TARDIS_MAX_CONCURRENT_VMS:-1}"
# Name-pattern FALLBACK (kept for backward-compat during rollout — see the
# self-declaring metadata model below). Every VM shape that holds Tardis
# connections: sharded backfills (heavy/light), SINGLE_VM_QUEUE combined VMs,
# and mtds cefi backfill/pipelinecheck VMs.
TARDIS_VM_NAME_PATTERN='^(cefi|tradfi)-.*-(heavy|light)-|^cefi-queue-|^mtds-backfill-cefi-'

# Self-declaring metadata model (operator-approved design, 2026-07-16 —
# cefi_completion_program_2026_07_15.md "Scope the Tardis cap to AUTHENTICATED
# batch consumers only"): a name-regex can never stay in sync with every
# Tardis-consuming launcher (83+ launchers), and — proven the hard way — it can
# ALSO wrongly catch VMs that do NOT hold the licensed slot (live MTDS
# tardis-machine is an unauthenticated local sidecar; IS Tardis hits public
# api.tardis.dev metadata; neither contends). So every launcher that opens an
# AUTHENTICATED Tardis (datasets.tardis.dev) connection now stamps
# VM_TARDIS_CONSUMER=1 into its VM metadata (GCP) / instance tags (AWS) at
# create time, and THIS is what the guard counts — the name pattern above stays
# ONLY as an OR-clause fallback so an already-running VM launched by
# pre-rollout code (no stamp yet) is still counted while the fleet migrates.
# Counts RUNNING + PROVISIONING + STAGING (not RUNNING alone). A VM that is still coming up
# ALREADY holds the single Tardis IP slot (or is about to), so a concurrent launch during that
# ~40s window must see it. Real incident 2026-07-16T00:58Z: a keeper relaunch (PROVISIONING) and
# a manual launch fired 40s apart, both passed the RUNNING-only count, and TWO VMs ran = the
# 403 storm the cap exists to prevent. Widening the status set closes that race at the guard.
tardis_running_vm_count() { # $1=zone $2=project -> echoes count (GCP union of name-pattern + VM_TARDIS_CONSUMER=1 metadata, + best-effort AWS)
  local zone="$1" project="$2" gcp=0 aws_n=0
  if command -v gcloud >/dev/null 2>&1; then
    if command -v python3 >/dev/null 2>&1; then
      # One list call (name + metadata), one union-count pass in python — avoids a
      # fragile/unsupported `--filter metadata.<key>=<value>` server-side expression
      # (verified 2026-07-16: GCE's list API rejects that filter shape outright,
      # "Invalid list filter expression") and avoids double-counting a VM that
      # matches BOTH the name pattern and the metadata stamp.
      gcp="$(gcloud compute instances list \
        --filter='status=RUNNING OR status=PROVISIONING OR status=STAGING' \
        --zones="$zone" --project="$project" \
        --format='json(name,metadata.items)' 2>/dev/null | python3 -c '
import json, re, sys

pattern = re.compile(sys.argv[1])
try:
    instances = json.load(sys.stdin)
except (json.JSONDecodeError, ValueError):
    instances = []
count = 0
for inst in instances:
    name = inst.get("name", "")
    items = (inst.get("metadata") or {}).get("items") or []
    stamped = any(
        item.get("key") == "VM_TARDIS_CONSUMER" and str(item.get("value")) == "1" for item in items
    )
    if stamped or pattern.search(name):
        count += 1
print(count)
' "$TARDIS_VM_NAME_PATTERN" 2>/dev/null)"
      [[ -z "$gcp" ]] && gcp=0
    else
      # python3 unavailable — degrade to the name-pattern-only count (best-effort;
      # a forward-poll/mtds-backfill VM stamped ONLY via metadata would be missed,
      # but that is strictly no worse than the pre-rollout behavior).
      echo "WARNING: [tardis-guard] python3 unavailable — counting by VM-name pattern only (metadata-stamped VMs may be undercounted)" >&2
      gcp="$(gcloud compute instances list \
        --filter="name~\"${TARDIS_VM_NAME_PATTERN}\" AND (status=RUNNING OR status=PROVISIONING OR status=STAGING)" \
        --zones="$zone" --project="$project" \
        --format='value(name)' 2>/dev/null | grep -c . || true)"
    fi
  fi
  if [[ -n "${AWS_REGION:-}" ]] && command -v aws >/dev/null 2>&1; then
    # Union of the legacy purpose-tag match + the new VM_TARDIS_CONSUMER=1 tag
    # (AWS `--filters` ANDs across different Names, so two calls + a dedup pass
    # is the union — mirrors the GCP name-pattern-OR-metadata union above).
    aws_n="$(
      {
        aws ec2 describe-instances --region "${AWS_REGION}" \
          --filters "Name=tag:purpose,Values=cefi-sharded-backfill,tradfi-sharded-backfill" \
                    "Name=instance-state-name,Values=running,pending" \
          --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null
        aws ec2 describe-instances --region "${AWS_REGION}" \
          --filters "Name=tag:VM_TARDIS_CONSUMER,Values=1" \
                    "Name=instance-state-name,Values=running,pending" \
          --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null
      } | tr '\t' '\n' | sort -u | grep -c . || true
    )"
  fi
  echo $(( gcp + aws_n ))
}

tardis_concurrency_guard() { # $1=planned_vm_count $2=zone $3=project
  local planned="$1" zone="$2" project="$3"
  local existing total
  existing="$(tardis_running_vm_count "$zone" "$project")"
  total=$(( existing + planned ))
  if (( total <= TARDIS_MAX_CONCURRENT_VMS )); then
    echo "[tardis-guard] OK: $existing running + $planned planned = $total <= cap $TARDIS_MAX_CONCURRENT_VMS"
    if [[ "${TARDIS_CONCURRENCY_LEASE:-}" != "1" && "$total" -gt 1 ]]; then
      echo "[tardis-guard] WARN: TARDIS_CONCURRENCY_LEASE is not enabled. Every surviving multi-VM wave ran lease-ON; the only lease-OFF multi-VM wave (2026-07-13, N=6) collapsed entirely. Strongly recommend TARDIS_CONCURRENCY_LEASE=1." >&2
    fi
    return 0
  fi
  if [[ "${FORCE:-0}" == "1" ]]; then
    echo "[tardis-guard] WARN: cap $TARDIS_MAX_CONCURRENT_VMS exceeded ($existing running + $planned planned) but FORCE=1 — proceeding on operator override." >&2
    return 0
  fi
  cat >&2 <<EOF
ERROR: Tardis concurrent-VM cap would be exceeded: $existing running + $planned planned = $total > $TARDIS_MAX_CONCURRENT_VMS.

HARD RULE (operator, 2026-07-16): at most $TARDIS_MAX_CONCURRENT_VMS Tardis-consuming VM(s)
at a time — the lease does NOT lift the cap; it AMPLIFIES the problem (fail-open after 1800s
means every waiting VM then proceeds UNLOCKED at once). Measured in the real gap: N=3 lease-ON
produced ~94% failures (10,300x403/912 ok; 15,034x403/0 ok), +37,212 FALSE attempted_failed
rows in 8h, and coverage went BACKWARD 52.13 -> 48.38. N=1 produced ZERO 403s.

DO NOT scale by adding VMs. Scale on the ONE IP instead:
  SINGLE_VM_QUEUE=1                       bundle every venue/shard onto the single VM
  TARDIS_MAX_CONCURRENT_DOWNLOADS=<n>     intra-VM trade streams (default 16; the box is
                                          typically ~93% idle at that — cpu 104%/1600%,
                                          rss 7.8GB/128GB, so there is large headroom)
  TARDIS_BOOK_SNAPSHOT_MAX_CONCURRENT=<n> book_snapshot_5 streams (default 4 — its own cap)
Keep total concurrent connections well under Tardis's tolerance (~100-200 ok; ~2k is not).

Options:
  Inspect running Tardis VMs:
    gcloud compute instances list --filter='name~"${TARDIS_VM_NAME_PATTERN}" AND status=RUNNING' --zones=$zone --project=$project
  Wait for the running VM to finish, then launch the next slice
  Operator override: FORCE=1 (accepts the 403-storm + false-af-row risk)
EOF
  return 1
}
