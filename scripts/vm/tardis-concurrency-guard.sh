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
#   - FAILS CLOSED: if the fleet cannot be enumerated (gcloud/python3/aws missing,
#     an API/auth error, unparseable output) the guard REFUSES rather than reading
#     the failure as "0 running" (see the fail-closed block below);
#   - warns (but proceeds) when launching under the cap WITHOUT the lease enabled;
#   - FORCE=1 downgrades refusal to a warning (operator override, matches the
#     launchers' existing FORCE semantics).
#
# Usage (from a launcher) — BOTH calls are mandatory:
#   source "$(dirname "$0")/tardis-concurrency-guard.sh"
#   tardis_concurrency_guard "<planned_vm_count>" "<zone>" "<project>"   # pre-flight, exits 1 on refusal
#   ...then immediately before EVERY `gcloud compute instances create` of a Tardis VM:
#   tardis_guard_reserve_slot "<zone>" "<project>" "<vm_name>"           # exits 1 on refusal
#
# WHY the second call exists (the 2026-07-20 latent-breach fix). The pre-flight
# count is an ESTIMATE the launcher derives by mirroring its own launch loop, and
# an estimate can silently drift from reality. It DID: launch-cefi-sharded-backfill.sh
# in SINGLE_VM_QUEUE=1 mode buckets shards by "group|data_types", and DERIBIT's light
# group (DATA_LIGHT_DERIBIT, carrying options_chain) is a DIFFERENT data_types string
# from every other derivatives venue's (DATA_LIGHT_PERPS, carrying liquidations) — so
# a DERIBIT+perp-venue light launch produces TWO buckets = TWO VMs, while the estimator
# clamped the planned count to ONE (it counted "light" once and broke at the first
# derivatives venue). Measured 2026-07-20 with LAUNCH_GROUPS="light"
# VENUES="DERIBIT BINANCE-FUTURES": guard saw "0 running + 1 planned = 1 <= cap 1 OK",
# then the flush created 2 Tardis VMs. Cap-1 breached with the guard reporting green.
# tardis_guard_reserve_slot binds the cap to ACTUAL VM CREATION instead of to a
# predicted count, so no naming scheme, bundling shape, or future estimator drift can
# evade it — the launcher can only create as many Tardis VMs as it can reserve slots for.
#
# CAP-EXEMPT venues: HYPERLIQUID / ASTER / LIGHTER-ZKSYNC / EXTENDED-STARKNET do NOT
# fetch from datasets.tardis.dev (S3 requester-pays + venue REST), so their launcher
# (launch-cefi-hl-aster-historical-backfill.sh) neither sources this guard nor stamps
# VM_TARDIS_CONSUMER=1, and its VM names (cefi-<venue>-<year|YYYYMMDD>-<ts>) do not
# match TARDIS_VM_NAME_PATTERN. Do NOT add the guard there and do NOT widen the name
# pattern to catch them — over-restricting them starves work that never contends.

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

# CAP-EXEMPT venues (see the prose above): venues whose URDI adapter is NOT
# "tardis" (unified-api-contracts registry/venue_adapter_keys.py
# VENUE_TO_ADAPTER_KEY is the SSOT — grep it, don't guess, before editing this
# list), so they never open an authenticated datasets.tardis.dev connection
# and do not contend for the single-IP slot. A small, rarely-changing set —
# kept as a bash mirror here (rather than shelling to python3 to import UAC)
# because every caller of this guard is bash and the set changes only on a
# deliberate operator-ruled venue add/remove.
TARDIS_CAP_EXEMPT_VENUES=(HYPERLIQUID ASTER LIGHTER-ZKSYNC EXTENDED-STARKNET COINBASE-CDE)

# tardis_venue_list_needs_guard <space-separated venue list, or empty>
# Returns 0 (true — guard IS needed) if the list is empty (== "all venues for
# the asset group", which includes real Tardis venues) OR contains at least
# one venue NOT in TARDIS_CAP_EXEMPT_VENUES. Returns 1 (false — guard can be
# skipped) only when every listed venue is cap-exempt. Matching is
# case-insensitive (venue casing on the CLI is not guaranteed uppercase).
tardis_venue_list_needs_guard() {
  local venues="$1" v e exempt
  [[ -z "$venues" ]] && return 0
  for v in $venues; do
    exempt=false
    for e in "${TARDIS_CAP_EXEMPT_VENUES[@]}"; do
      [[ "${v^^}" == "$e" ]] && { exempt=true; break; }
    done
    $exempt || return 0
  done
  return 1
}

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
# FAIL-CLOSED CONTRACT (2026-07-20). This function returns NON-ZERO and echoes
# nothing when it cannot determine the count. Every "could not enumerate" path used
# to collapse to 0 (`2>/dev/null` everywhere + `[[ -z "$gcp" ]] && gcp=0` + a skipped
# branch when gcloud was absent), which made the guard report "OK: 0 running" and wave
# every launch through at exactly the moment the environment was broken — expired ADC,
# a compute API error, a quota error, or a gcloud/python3 that isn't on PATH. A safety
# control that disappears when its probe fails is not a safety control. Callers MUST
# treat a non-zero return as REFUSE (tardis_concurrency_guard and
# tardis_guard_reserve_slot both do; FORCE=1 remains the operator override).
#
# TARDIS_GUARD_SKIP_AWS=1 is the documented escape hatch for a GCP-only launch on a
# host that exports AWS_REGION but has no aws CLI — it narrows the count to GCP
# deliberately instead of silently.
tardis_running_vm_count() { # $1=zone $2=project -> echoes count (GCP union of name-pattern + VM_TARDIS_CONSUMER=1 metadata, + AWS); returns 1 if undeterminable
  local zone="$1" project="$2" gcp=0 aws_n=0 raw rc
  if ! command -v gcloud >/dev/null 2>&1; then
    echo "ERROR: [tardis-guard] gcloud not found on PATH — cannot enumerate the Tardis fleet (fail-closed)." >&2
    return 1
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    # Previously this degraded to a name-pattern-only count. That silently drops every
    # VM that declares itself ONLY via VM_TARDIS_CONSUMER=1 metadata (forward-poll,
    # mtds-backfill, targeted options-chain), i.e. it undercounts the exact self-declaring
    # model the cap now relies on. Undercounting the Tardis cap is the failure mode that
    # corrupts the manifest — refuse instead.
    echo "ERROR: [tardis-guard] python3 not found on PATH — cannot evaluate the VM_TARDIS_CONSUMER metadata stamp, and a name-pattern-only count undercounts metadata-declared VMs (fail-closed)." >&2
    return 1
  fi
  # One list call (name + metadata), one union-count pass in python — avoids a
  # fragile/unsupported `--filter metadata.<key>=<value>` server-side expression
  # (verified 2026-07-16: GCE's list API rejects that filter shape outright,
  # "Invalid list filter expression") and avoids double-counting a VM that
  # matches BOTH the name pattern and the metadata stamp.
  raw="$(gcloud compute instances list \
    --filter='status=RUNNING OR status=PROVISIONING OR status=STAGING' \
    --zones="$zone" --project="$project" \
    --format='json(name,metadata.items)' 2>&1)"
  rc=$?
  if (( rc != 0 )); then
    echo "ERROR: [tardis-guard] 'gcloud compute instances list' failed (exit ${rc}) for zone=${zone} project=${project} — fail-closed. gcloud said: ${raw}" >&2
    return 1
  fi
  gcp="$(printf '%s' "$raw" | python3 -c '
import json, re, sys

pattern = re.compile(sys.argv[1])
try:
    instances = json.load(sys.stdin)
except (json.JSONDecodeError, ValueError) as exc:
    sys.stderr.write("unparseable gcloud JSON: %s\n" % exc)
    raise SystemExit(1)
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
' "$TARDIS_VM_NAME_PATTERN")" || {
    echo "ERROR: [tardis-guard] could not parse the gcloud instance list — fail-closed." >&2
    return 1
  }
  if [[ -z "$gcp" ]]; then
    echo "ERROR: [tardis-guard] empty count from the instance-list parse — fail-closed." >&2
    return 1
  fi
  if [[ -n "${AWS_REGION:-}" && "${TARDIS_GUARD_SKIP_AWS:-0}" != "1" ]]; then
    # Both clouds share the ONE Tardis key, so an unreadable AWS side means an
    # unknown total — refuse rather than count the GCP side alone.
    if ! command -v aws >/dev/null 2>&1; then
      echo "ERROR: [tardis-guard] AWS_REGION=${AWS_REGION} is set but the aws CLI is not on PATH — the AWS half of the shared-key fleet is unknowable (fail-closed). Set TARDIS_GUARD_SKIP_AWS=1 to deliberately count GCP only." >&2
      return 1
    fi
    # Union of the legacy purpose-tag match + the new VM_TARDIS_CONSUMER=1 tag
    # (AWS `--filters` ANDs across different Names, so two calls + a dedup pass
    # is the union — mirrors the GCP name-pattern-OR-metadata union above).
    local aws_legacy aws_stamped
    aws_legacy="$(aws ec2 describe-instances --region "${AWS_REGION}" \
      --filters "Name=tag:purpose,Values=cefi-sharded-backfill,tradfi-sharded-backfill" \
                "Name=instance-state-name,Values=running,pending" \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>&1)" || {
      echo "ERROR: [tardis-guard] AWS describe-instances (purpose tag) failed — fail-closed. aws said: ${aws_legacy}" >&2
      return 1
    }
    aws_stamped="$(aws ec2 describe-instances --region "${AWS_REGION}" \
      --filters "Name=tag:VM_TARDIS_CONSUMER,Values=1" \
                "Name=instance-state-name,Values=running,pending" \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>&1)" || {
      echo "ERROR: [tardis-guard] AWS describe-instances (VM_TARDIS_CONSUMER tag) failed — fail-closed. aws said: ${aws_stamped}" >&2
      return 1
    }
    aws_n="$(printf '%s\n%s\n' "$aws_legacy" "$aws_stamped" | tr '\t' '\n' | sed '/^[[:space:]]*$/d' | sort -u | grep -c . || true)"
    [[ -z "$aws_n" ]] && aws_n=0
  fi
  echo $(( gcp + aws_n ))
}

# In-process slot accounting for tardis_guard_reserve_slot (see the header). The
# baseline is the fleet count observed by the FIRST guard/reserve call in this process;
# TARDIS_SLOTS_RESERVED counts what this process has since committed to creating.
TARDIS_SLOTS_RESERVED=0
TARDIS_GUARD_BASELINE=""

# tardis_guard_reserve_slot <zone> <project> [label] — call immediately before EVERY
# `gcloud compute instances create` / `aws ec2 run-instances` of a Tardis-consuming VM.
# Returns 0 (and books a slot) when the VM is within cap, non-zero when it is not.
#
# The effective occupancy is max(live_recount, baseline + reserved_this_process):
#   - `baseline + reserved` is authoritative right after a create, because a VM launched
#     with `--async` is not reliably visible in `instances list` yet — counting live alone
#     would let the very next reserve re-use the slot we just took (this is the same
#     ~40s visibility race that the RUNNING+PROVISIONING+STAGING status widening was
#     added for on 2026-07-16, now closed on the launcher side too);
#   - `live_recount` is authoritative when ANOTHER process launched meanwhile, which
#     `baseline + reserved` cannot see.
# Taking the max means whichever signal shows more contention wins — the conservative
# direction, and the only one that cannot silently over-launch.
tardis_guard_reserve_slot() { # $1=zone $2=project [$3=label]
  local zone="$1" project="$2" label="${3:-<unnamed-vm>}"
  local live baseline effective total
  if ! live="$(tardis_running_vm_count "$zone" "$project")"; then
    if [[ "${FORCE:-0}" == "1" ]]; then
      echo "[tardis-guard] WARN: could not enumerate the Tardis fleet before creating '${label}', but FORCE=1 — proceeding on operator override." >&2
      TARDIS_SLOTS_RESERVED=$(( TARDIS_SLOTS_RESERVED + 1 ))
      return 0
    fi
    echo "ERROR: [tardis-guard] REFUSING to create '${label}': the running Tardis VM count could not be determined (see the error above). Fail-closed — an unknown count is treated as over-cap. Operator override: FORCE=1." >&2
    return 1
  fi
  baseline="${TARDIS_GUARD_BASELINE:-$live}"
  TARDIS_GUARD_BASELINE="$baseline"
  effective=$(( baseline + TARDIS_SLOTS_RESERVED ))
  (( live > effective )) && effective="$live"
  total=$(( effective + 1 ))
  if (( total <= TARDIS_MAX_CONCURRENT_VMS )); then
    TARDIS_SLOTS_RESERVED=$(( TARDIS_SLOTS_RESERVED + 1 ))
    echo "[tardis-guard] slot reserved for '${label}' (${total}/${TARDIS_MAX_CONCURRENT_VMS} Tardis VMs; ${TARDIS_SLOTS_RESERVED} created by this launcher)"
    return 0
  fi
  if [[ "${FORCE:-0}" == "1" ]]; then
    echo "[tardis-guard] WARN: creating '${label}' would put ${total} Tardis VMs over cap ${TARDIS_MAX_CONCURRENT_VMS}, but FORCE=1 — proceeding on operator override." >&2
    TARDIS_SLOTS_RESERVED=$(( TARDIS_SLOTS_RESERVED + 1 ))
    return 0
  fi
  cat >&2 <<EOF
ERROR: [tardis-guard] REFUSING to create Tardis VM '${label}' — it would be number ${total}, over the cap of ${TARDIS_MAX_CONCURRENT_VMS}.

This check runs at ACTUAL VM-CREATION time, so it fires even when the launcher's
pre-flight planned-count estimate said otherwise (that estimate has been wrong before:
the SINGLE_VM_QUEUE DERIBIT-options_chain bucket split — see this file's header).
Occupancy: baseline=${baseline} live=${live} reserved_by_this_launcher=${TARDIS_SLOTS_RESERVED}.

Any VMs this launcher already created remain running and are within cap. Re-run the
remaining scope once they finish. DO NOT scale by adding VMs — scale on the ONE IP
(SINGLE_VM_QUEUE=1, TARDIS_MAX_CONCURRENT_DOWNLOADS, TARDIS_BOOK_SNAPSHOT_MAX_CONCURRENT).
Operator override: FORCE=1 (accepts the 403-storm + false-attempted_failed-row risk).
EOF
  return 1
}

tardis_concurrency_guard() { # $1=planned_vm_count $2=zone $3=project
  local planned="$1" zone="$2" project="$3"
  local existing total
  if ! existing="$(tardis_running_vm_count "$zone" "$project")"; then
    if [[ "${FORCE:-0}" == "1" ]]; then
      echo "[tardis-guard] WARN: could not enumerate the running Tardis fleet, but FORCE=1 — proceeding on operator override." >&2
      return 0
    fi
    cat >&2 <<EOF
ERROR: [tardis-guard] REFUSING to launch: the number of running Tardis VMs could not be
determined (see the error above). Fail-closed by design — an unknown count is treated as
over-cap, because reading a broken probe as "0 running" is what lets an unbounded wave
through at exactly the moment the environment is broken.

Fix the probe, then re-run:
  gcloud auth application-default login      # expired / missing ADC
  gcloud compute instances list --zones=$zone --project=$project   # confirm it returns
Operator override: FORCE=1 (launches WITHOUT knowing the current fleet size).
EOF
    return 1
  fi
  TARDIS_GUARD_BASELINE="$existing"
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
