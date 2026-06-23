#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# allocate-orchestrator-eips.sh — Phase 11
#
# Allocates an Elastic IP per epic VM in the AWS fleet, tags it, and associates
# it with the running instance. Idempotent — re-running on an already-tagged EIP
# is a no-op. Re-running on an instance that already has the requested EIP is
# also a no-op.
#
# Why EIPs:
#   - Fleet VMs currently use dynamic public IPs. They change on stop/start,
#     forcing manual edits to data/config/backends.json + DNS records each
#     time. With EIPs the public IP is stable per-VM for the life of the
#     fleet.
#   - Per-VM DNS (api-<vm>.agent-orchestrator.odum-research.com) requires a
#     stable target. EIPs are the prerequisite for the DNS A records
#     documented in codex/05-infrastructure/agent-orchestrator-dns-cutover.md.
#
# Cost: an attached EIP is free; an unattached one is $0.005/hr (~$3.65/mo).
# This script attaches every EIP it allocates, so steady-state cost is zero.
# Unattached EIPs (e.g. after instance termination) need manual release —
# see "Release recipe" at the bottom of this file.
#
# Prereqs:
#   - AWS CLI configured with credentials that can ec2:AllocateAddress +
#     ec2:AssociateAddress + ec2:CreateTags + ec2:DescribeInstances
#   - Fleet VMs already launched and tagged Name=agent-orch-<vm-id>-<date>
#
# Usage:
#   bash allocate-orchestrator-eips.sh --vm-id vm-defi
#   bash allocate-orchestrator-eips.sh --all
#   bash allocate-orchestrator-eips.sh --all --dry-run
#
# Post-allocation:
#   - Updates agent-orchestrator/data/config/backends.json with the new EIPs
#     (operator must commit + push the update)
#   - Run DNS records per codex/05-infrastructure/agent-orchestrator-dns-cutover.md

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_REGION="${AWS_DEFAULT_REGION:-ap-northeast-1}"
DRY_RUN=false
VM_ID=""
ALL=false
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
BACKENDS_JSON="${WORKSPACE_ROOT}/agent-orchestrator/data/config/backends.json"

# Fleet VM ids — matches the 10 epic VMs in backends.json (excludes the central
# "ikenna-vm" since it already has its EIP at 13.113.200.22).
FLEET_VMS=(
  vm-defi vm-cefi vm-tradfi vm-sports vm-prediction
  vm-ml vm-trading-core vm-operator-ops vm-cross-cutting vm-orchestrator
)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vm-id)   VM_ID="$2"; shift 2 ;;
    --all)     ALL=true;   shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --region)  AWS_REGION="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,38p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "ERROR: unknown arg: $1" >&2; exit 2 ;;
  esac
done

if ! $ALL && [[ -z "${VM_ID}" ]]; then
  echo "ERROR: --vm-id <id> or --all required." >&2
  exit 1
fi

log() { printf '[eip] %s\n' "$*"; }

# Resolve a single VM's running instance-id by Name-tag prefix.
resolve_instance() {
  local vm="$1"
  aws ec2 describe-instances \
    --region "${AWS_REGION}" \
    --filters "Name=tag:Name,Values=agent-orch-${vm}-*" \
              "Name=instance-state-name,Values=running,pending" \
    --query "Reservations[].Instances[].InstanceId" \
    --output text 2>/dev/null | head -1
}

# Check if an instance already has an EIP allocated (vs the auto-assigned
# public IP). EIPs show as AssociationId on the network interface.
existing_eip_alloc_id() {
  local instance_id="$1"
  aws ec2 describe-addresses \
    --region "${AWS_REGION}" \
    --filters "Name=instance-id,Values=${instance_id}" \
    --query "Addresses[0].AllocationId" \
    --output text 2>/dev/null | grep -v '^None$' || true
}

# Allocate + tag + associate one EIP for one VM.
allocate_for_vm() {
  local vm="$1"
  local instance_id
  instance_id="$(resolve_instance "${vm}")"
  if [[ -z "${instance_id}" ]]; then
    log "  ${vm}: no running instance found — skipping"
    return 0
  fi

  local existing
  existing="$(existing_eip_alloc_id "${instance_id}")"
  if [[ -n "${existing}" ]]; then
    local existing_ip
    existing_ip="$(aws ec2 describe-addresses --region "${AWS_REGION}" \
      --allocation-ids "${existing}" --query "Addresses[0].PublicIp" --output text)"
    log "  ${vm}: ${instance_id} already has EIP ${existing_ip} (alloc ${existing}) — skipping"
    return 0
  fi

  if $DRY_RUN; then
    log "  ${vm}: [dry-run] would allocate + associate EIP to ${instance_id}"
    return 0
  fi

  log "  ${vm}: allocating EIP..."
  local alloc_json
  alloc_json="$(aws ec2 allocate-address \
    --region "${AWS_REGION}" \
    --domain vpc \
    --tag-specifications "ResourceType=elastic-ip,Tags=[{Key=Name,Value=agent-orch-${vm}-eip},{Key=Project,Value=agent-orchestrator},{Key=VmId,Value=${vm}}]" \
    --output json)"
  local alloc_id public_ip
  alloc_id="$(echo "${alloc_json}" | python3 -c "import json,sys; print(json.load(sys.stdin)['AllocationId'])")"
  public_ip="$(echo "${alloc_json}" | python3 -c "import json,sys; print(json.load(sys.stdin)['PublicIp'])")"

  log "  ${vm}: associating ${public_ip} (${alloc_id}) → ${instance_id}"
  aws ec2 associate-address \
    --region "${AWS_REGION}" \
    --allocation-id "${alloc_id}" \
    --instance-id "${instance_id}" >/dev/null

  log "  ${vm}: DONE — public IP is now ${public_ip} (stable across stop/start)"
  printf '  RESULT %s\t%s\t%s\n' "${vm}" "${public_ip}" "${alloc_id}" >> /tmp/eip_alloc_results.txt
}

# ── Main ──
> /tmp/eip_alloc_results.txt
if $ALL; then
  log "Allocating EIPs for entire fleet (${#FLEET_VMS[@]} VMs)..."
  for v in "${FLEET_VMS[@]}"; do
    allocate_for_vm "${v}"
  done
else
  allocate_for_vm "${VM_ID}"
fi

echo ""
log "Summary (also written to /tmp/eip_alloc_results.txt):"
if [[ -s /tmp/eip_alloc_results.txt ]]; then
  cat /tmp/eip_alloc_results.txt
fi

echo ""
log "NEXT STEPS:"
log "  1. Update ${BACKENDS_JSON} — replace each VM's 'url' public IP with the EIP."
log "  2. git add + commit + push to live-defi-rollout."
log "  3. Add DNS A records per codex/05-infrastructure/agent-orchestrator-dns-cutover.md"
log "     (operator-side action on the odum-research.com zone)."
log ""
log "Release recipe (per VM, if/when an EIP becomes orphan after instance terminate):"
log "  aws ec2 release-address --region ${AWS_REGION} --allocation-id <alloc-id>"
log "  An unattached EIP costs \$0.005/hr (~\$3.65/mo)."
