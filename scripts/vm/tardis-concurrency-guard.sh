#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
#
# TARDIS CONCURRENT-VM CAP — shared guard sourced by every launcher that creates a
# Tardis-consuming VM (cefi sharded backfill GCP/AWS, mtds cefi backfill/pipelinecheck).
#
# HARD RULE (operator, 2026-07-14): at most **3** Tardis-consuming VMs run at a time,
# and every launcher MUST count the existing fleet before creating new ones.
#
# Why 3 (empirical, 2026-07-14 — SSOT:
# unified-trading-pm/plans/active/issues/tardis_concurrent_ip_lockout_2026_07_12.md
# § "EMPIRICAL CONCURRENCY TEST"): the shared academic key enforces a single REVOLVING
# active-IP slot. N=3 VMs grind through at ~50-70% request efficiency (net ~1.5-2x one
# serialized VM); N=6 collapsed — per-VM win rate fell below the 1800s stall watchdog
# threshold and ALL SIX died (2026-07-13T23:15Z wave, DEPLOYMENT_FAILED exit_code=137).
#
# Evidence nuance (2026-07-14T11:15Z metadata check): the surviving N=3 wave runs WITH
# TARDIS_CONCURRENCY_LEASE=1 — the 403 churn is lease-rotation handoff, not unmanaged
# contention. The N=6 collapse ran WITHOUT the lease. There is NO datapoint showing
# lease-off works at any N>1, so the lease does NOT waive the cap — it's required kit
# alongside it, and the guard warns when it's missing.
#
# Behaviour:
#   - counts RUNNING GCP VMs matching TARDIS_VM_NAME_PATTERN in the fleet zone
#     (+ running/pending AWS sharded-backfill instances when AWS_REGION is set and
#     the aws CLI is available — both clouds share the ONE Tardis key);
#   - refuses when existing + planned > TARDIS_MAX_CONCURRENT_VMS (default 3),
#     lease or no lease (the cap is the operator's hard rule);
#   - warns (but proceeds) when launching under the cap WITHOUT the lease enabled;
#   - FORCE=1 downgrades refusal to a warning (operator override, matches the
#     launchers' existing FORCE semantics).
#
# Usage (from a launcher):
#   source "$(dirname "$0")/tardis-concurrency-guard.sh"
#   tardis_concurrency_guard "<planned_vm_count>" "<zone>" "<project>"   # exits 1 on refusal

TARDIS_MAX_CONCURRENT_VMS="${TARDIS_MAX_CONCURRENT_VMS:-3}"
# Every VM shape that holds Tardis connections: sharded backfills (heavy/light),
# SINGLE_VM_QUEUE combined VMs, and mtds cefi backfill/pipelinecheck VMs.
TARDIS_VM_NAME_PATTERN='^(cefi|tradfi)-.*-(heavy|light)-|^cefi-queue-|^mtds-backfill-cefi-'

tardis_running_vm_count() { # $1=zone $2=project -> echoes count (GCP + best-effort AWS)
  local zone="$1" project="$2" gcp=0 aws_n=0
  if command -v gcloud >/dev/null 2>&1; then
    gcp="$(gcloud compute instances list \
      --filter="name~\"${TARDIS_VM_NAME_PATTERN}\" AND status=RUNNING" \
      --zones="$zone" --project="$project" \
      --format='value(name)' 2>/dev/null | grep -c . || true)"
  fi
  if [[ -n "${AWS_REGION:-}" ]] && command -v aws >/dev/null 2>&1; then
    aws_n="$(aws ec2 describe-instances --region "${AWS_REGION}" \
      --filters "Name=tag:purpose,Values=cefi-sharded-backfill,tradfi-sharded-backfill" \
                "Name=instance-state-name,Values=running,pending" \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null \
      | tr '\t' '\n' | grep -c . || true)"
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

HARD RULE (operator, 2026-07-14): at most $TARDIS_MAX_CONCURRENT_VMS Tardis-consuming VMs
at a time — the lease does NOT lift the cap. The shared key allows a single revolving
active slot: 3 lease-serialized VMs grind at ~50-70% request efficiency; the 2026-07-13
lease-OFF 6-VM wave starved below the stall watchdog and ALL SIX died with zero usable
progress (DEPLOYMENT_FAILED exit_code=137). SSOT:
unified-trading-pm/plans/active/issues/tardis_concurrent_ip_lockout_2026_07_12.md

Options:
  Inspect running Tardis VMs:
    gcloud compute instances list --filter='name~"${TARDIS_VM_NAME_PATTERN}" AND status=RUNNING' --zones=$zone --project=$project
  Reduce this launch (fewer VENUES/YEARS) so existing + planned <= $TARDIS_MAX_CONCURRENT_VMS
  Wait for the running wave to finish, then launch the next slice (lease-enabled)
  Operator override: FORCE=1 (accepts the collapse risk)
EOF
  return 1
}
