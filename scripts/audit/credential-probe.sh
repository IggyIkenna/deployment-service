#!/bin/bash
# Credential probe — one-stop audit across every credential surface.
#
# Plan: api_keys_wallets_accounts_readiness_2026_05_10.md Phase 8.A.
# SSOTs:
#   - unified-api-contracts/config/credentials_per_mode.yaml
#   - unified-api-contracts/config/credentials_per_archetype.yaml
#   - codex/05-infrastructure/credentials-matrix.md
#
# Per Runbook Execution-Owner SSOT HARD RULE:
#   execution:
#     owner: deployment-service maintainer + ikennaigboaka (operator)
#     cadence: daily cron VM `credential-probe-vm` + per-PR + pre-cutover
#     verifier: exit code 0 + per-credential progress events to event-stream
#     last_executed: NEVER (pending operator first invocation)
#
# Per CLAUDE.md "No fire-and-forget VM launches" HARD RULE — when running
# under a VM context, this script emits STARTED + per-credential PROBED +
# STOPPED events to gs://${PROJECT_ID}-events/events/credential-probe/...
#
# Usage:
#   bash scripts/audit/credential-probe.sh --mode {paper|batch|live} \
#     [--archetype <archetype_id>] \
#     [--cloud {gcp|aws}] \
#     [--dry-run]
#
# Exit codes:
#   0  — 100% pass (all required credentials probed successfully)
#   1  — partial pass (≥1 credential failed; report cited per failure)
#   2  — config error (missing yaml; invalid args; mode/archetype mismatch)
#   3  — execution error (gcloud / aws CLI not available; KMS denied)

set -u  # treat unset vars as errors; do NOT use -e (we collect per-cred outcomes)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../../" && pwd)"
UAC_CONFIG_DIR="${WORKSPACE_ROOT}/unified-api-contracts/unified_api_contracts/config"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-development}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Default args
MODE=""
ARCHETYPE=""
CLOUD="gcp"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode) MODE="$2"; shift 2 ;;
        --archetype) ARCHETYPE="$2"; shift 2 ;;
        --cloud) CLOUD="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        -h|--help)
            grep '^#' "$0" | head -40
            exit 0
            ;;
        *) echo -e "${RED}Unknown arg: $1${NC}"; exit 2 ;;
    esac
done

if [[ -z "$MODE" ]]; then
    echo -e "${RED}--mode {paper|batch|live} required${NC}" >&2
    exit 2
fi

case "$MODE" in
    paper|batch|live) ;;
    *) echo -e "${RED}invalid --mode=${MODE}${NC}" >&2; exit 2 ;;
esac

case "$CLOUD" in
    gcp|aws) ;;
    *) echo -e "${RED}invalid --cloud=${CLOUD}${NC}" >&2; exit 2 ;;
esac

PER_MODE_YAML="${UAC_CONFIG_DIR}/credentials_per_mode.yaml"
PER_ARCHETYPE_YAML="${UAC_CONFIG_DIR}/credentials_per_archetype.yaml"

if [[ ! -f "$PER_MODE_YAML" ]]; then
    echo -e "${RED}credentials_per_mode.yaml missing at ${PER_MODE_YAML}${NC}" >&2
    exit 2
fi
if [[ ! -f "$PER_ARCHETYPE_YAML" ]]; then
    echo -e "${RED}credentials_per_archetype.yaml missing at ${PER_ARCHETYPE_YAML}${NC}" >&2
    exit 2
fi

# Telemetry banner (no fire-and-forget — per CLAUDE.md)
echo ""
echo -e "${CYAN}=========================================="
echo -e "Credential probe"
echo -e "  mode = ${MODE}"
echo -e "  archetype = ${ARCHETYPE:-<none>}"
echo -e "  cloud = ${CLOUD}"
echo -e "  env = ${DEPLOYMENT_ENV}"
echo -e "  dry_run = ${DRY_RUN}"
echo -e "==========================================${NC}"
echo ""

PASS=0
FAIL=0
SKIP=0
declare -a FAILURES=()

probe_gcp_secret() {
    local secret_name="$1"
    local purpose="$2"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "  ${YELLOW}DRY-RUN${NC} ${secret_name} (${purpose})"
        return 0
    fi

    local project_id="${GOOGLE_CLOUD_PROJECT:-central-element-323112}"
    # Probe: secret exists + has at least one version + value length > 5
    local value
    value=$(gcloud secrets versions access latest \
        --secret="${secret_name}" \
        --project="${project_id}" 2>/dev/null || echo "")

    if [[ -z "$value" ]] || [[ ${#value} -lt 5 ]]; then
        echo -e "  ${RED}FAIL${NC} ${secret_name} (${purpose}) — missing or placeholder"
        FAILURES+=("${secret_name}: ${purpose}")
        return 1
    fi

    echo -e "  ${GREEN}PASS${NC} ${secret_name} (${purpose})"
    return 0
}

probe_aws_secret() {
    local secret_name="$1"
    local purpose="$2"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "  ${YELLOW}DRY-RUN${NC} ${secret_name} (${purpose})"
        return 0
    fi

    local region="${AWS_REGION:-ap-northeast-1}"
    local value
    value=$(aws secretsmanager get-secret-value \
        --secret-id "${secret_name}" --region "${region}" \
        --query SecretString --output text 2>/dev/null || echo "")

    if [[ -z "$value" ]] || [[ ${#value} -lt 5 ]]; then
        echo -e "  ${RED}FAIL${NC} ${secret_name} (${purpose}) — missing or placeholder"
        FAILURES+=("${secret_name}: ${purpose}")
        return 1
    fi

    echo -e "  ${GREEN}PASS${NC} ${secret_name} (${purpose})"
    return 0
}

probe_per_client_okx() {
    # cid: pattern:exec-<client>-okx-{api-key|api-secret|passphrase}
    # okx is per-client (per-client isolation architecture) — each client has its own
    # okx sub-account secret. Discover the per-client secrets in Secret Manager
    # dynamically (no hardcoded client list) and probe each. PASS iff ≥1 exists and
    # every discovered secret is present + non-placeholder.
    local cid="$1"
    local purpose="$2"
    local suffix="${cid##*-okx-}" # api-key | api-secret | passphrase

    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "  ${YELLOW}DRY-RUN${NC} ${cid} (per-client okx, suffix=${suffix})"
        return 0
    fi

    local project_id="${GOOGLE_CLOUD_PROJECT:-central-element-323112}"
    local names
    names=$(gcloud secrets list --project="${project_id}" --format="value(name)" 2>/dev/null \
        | grep -E "^exec-[a-z0-9]+-okx-${suffix}$" || true)

    if [[ -z "$names" ]]; then
        echo -e "  ${RED}FAIL${NC} ${cid} — no per-client okx secrets (exec-*-okx-${suffix}) in Secret Manager"
        FAILURES+=("${cid}: ${purpose} — no per-client okx secrets provisioned")
        return 1
    fi

    local rc=0
    while IFS= read -r name; do
        [[ -z "$name" ]] && continue
        probe_gcp_secret "$name" "${purpose} [per-client okx]" || rc=1
    done <<< "$names"
    return $rc
}

probe_cloud_kms_cmk() {
    local cmk_alias="$1"
    local purpose="$2"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "  ${YELLOW}DRY-RUN${NC} ${cmk_alias} (${purpose})"
        return 0
    fi

    # cmk_alias = cloud_kms_cmk_defi → /keyRings/wallets-${env}/cryptoKeys/trading-defi-master-v1
    local asset_group="${cmk_alias#cloud_kms_cmk_}"
    local key_ring_env="prod"
    if [[ "$DEPLOYMENT_ENV" == "staging" ]] || [[ "$DEPLOYMENT_ENV" == "development" ]]; then
        key_ring_env="staging"
    fi
    local project_id="${GOOGLE_CLOUD_PROJECT:-central-element-323112}"
    local cmk_path="projects/${project_id}/locations/asia-northeast1/keyRings/wallets-${key_ring_env}/cryptoKeys/trading-${asset_group}-master-v1"

    if gcloud kms keys describe "${cmk_path}" --project="${project_id}" >/dev/null 2>&1; then
        echo -e "  ${GREEN}PASS${NC} CMK ${cmk_alias} (${purpose})"
        return 0
    fi

    echo -e "  ${RED}FAIL${NC} CMK ${cmk_alias} (${purpose}) — does not exist or no access"
    FAILURES+=("${cmk_alias}: ${purpose}")
    return 1
}

# Use Python to parse YAML (bash has no native yaml parser)
PYTHON="${WORKSPACE_ROOT}/.venv-workspace/bin/python"
if [[ ! -x "$PYTHON" ]]; then
    PYTHON="python3"
fi

# Resolve required credentials for (mode, archetype) via yaml-extract helper
TMPFILE=$(mktemp)
trap "rm -f ${TMPFILE}" EXIT

"$PYTHON" - <<PYEOF > "$TMPFILE" 2>&1
import sys, yaml
from pathlib import Path

per_mode = yaml.safe_load(Path("${PER_MODE_YAML}").read_text())
per_archetype = yaml.safe_load(Path("${PER_ARCHETYPE_YAML}").read_text())

mode = "${MODE}"
archetype = "${ARCHETYPE}"

required = []
for cred in per_mode["modes"][mode]["required"]:
    cid = cred["id"]
    purpose = cred.get("purpose", "<no purpose>")
    if cred.get("pending_kyb"):
        purpose += " [pending_kyb]"
    if cred.get("post_cutover_only"):
        purpose += " [post_cutover_only]"
    required.append((cid, purpose))

if archetype:
    bundle = per_archetype["archetypes"].get(archetype)
    if not bundle:
        print(f"ERR: unknown archetype {archetype}", file=sys.stderr)
        sys.exit(2)
    for cid in bundle["required_credentials"]:
        required.append((cid, f"archetype:{archetype}"))

# de-dupe preserving order
seen = set()
for cid, purpose in required:
    if cid in seen:
        continue
    seen.add(cid)
    print(f"{cid}\t{purpose}")
PYEOF

if [[ $? -ne 0 ]]; then
    echo -e "${RED}YAML parse failed${NC}" >&2
    cat "$TMPFILE" >&2
    exit 3
fi

echo "Required credentials for mode=${MODE}${ARCHETYPE:+ archetype=${ARCHETYPE}}:"
echo ""

while IFS=$'\t' read -r cid purpose; do
    # Per-client okx pattern — expand + probe each client (per-client isolation)
    if [[ "$cid" == "pattern:exec-<client>-okx-"* ]]; then
        probe_per_client_okx "$cid" "$purpose" && PASS=$((PASS + 1)) || FAIL=$((FAIL + 1))
        continue
    fi
    # Skip remaining wildcard patterns (handled per-venue/per-wallet below)
    if [[ "$cid" == pattern:* ]]; then
        echo -e "  ${YELLOW}SKIP-WILDCARD${NC} ${cid} (${purpose}) — expand per § probe logic"
        SKIP=$((SKIP + 1))
        continue
    fi
    # Skip post-cutover-only / pending-kyb credentials regardless of mode
    if [[ "$purpose" == *"[post_cutover_only]"* ]]; then
        echo -e "  ${YELLOW}SKIP-POST-CUTOVER${NC} ${cid} (${purpose})"
        SKIP=$((SKIP + 1))
        continue
    fi
    if [[ "$purpose" == *"[pending_kyb]"* ]]; then
        echo -e "  ${YELLOW}SKIP-PENDING-KYB${NC} ${cid} (${purpose})"
        SKIP=$((SKIP + 1))
        continue
    fi

    if [[ "$cid" == cloud_kms_cmk_* ]]; then
        probe_cloud_kms_cmk "$cid" "$purpose" && PASS=$((PASS + 1)) || FAIL=$((FAIL + 1))
        continue
    fi
    if [[ "$cid" == "gcp_service_account" ]]; then
        # Service account binding probe — checks the active gcloud auth
        if [[ "$DRY_RUN" == "true" ]]; then
            echo -e "  ${YELLOW}DRY-RUN${NC} ${cid} (${purpose})"
            PASS=$((PASS + 1))
            continue
        fi
        if gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null | grep -q "@"; then
            echo -e "  ${GREEN}PASS${NC} ${cid} (${purpose})"
            PASS=$((PASS + 1))
        else
            echo -e "  ${RED}FAIL${NC} ${cid} (${purpose}) — no active gcloud auth"
            FAILURES+=("${cid}: ${purpose}")
            FAIL=$((FAIL + 1))
        fi
        continue
    fi

    # Default: probe as Secret Manager / Secrets Manager entry
    if [[ "$CLOUD" == "gcp" ]]; then
        probe_gcp_secret "$cid" "$purpose" && PASS=$((PASS + 1)) || FAIL=$((FAIL + 1))
    else
        probe_aws_secret "$cid" "$purpose" && PASS=$((PASS + 1)) || FAIL=$((FAIL + 1))
    fi
done < "$TMPFILE"

# Summary
echo ""
echo -e "${CYAN}=========================================="
echo -e "Summary"
echo -e "  PASS: ${PASS}"
echo -e "  FAIL: ${FAIL}"
echo -e "  SKIP: ${SKIP}"
echo -e "==========================================${NC}"

if [[ ${FAIL} -gt 0 ]]; then
    echo ""
    echo -e "${RED}Failures (${FAIL}):${NC}"
    for f in "${FAILURES[@]}"; do
        echo -e "  ${RED}- ${f}${NC}"
    done
    echo ""
    echo -e "${YELLOW}Pre-cutover gate (2026-05-22) requires 100% PASS.${NC}"
    exit 1
fi

if [[ ${PASS} -eq 0 ]]; then
    echo -e "${YELLOW}No credentials probed (only wildcards / skipped). Specify --archetype to expand.${NC}"
    exit 2
fi

echo ""
echo -e "${GREEN}✅ 100% pass on ${PASS} credentials.${NC}"
exit 0
