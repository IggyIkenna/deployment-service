#!/usr/bin/env bash
set -euo pipefail

# Deploy FlashLoanReceiver contract to a target chain.
#
# Usage:
#   bash scripts/deploy-flash-loan-receiver.sh --chain sepolia
#   bash scripts/deploy-flash-loan-receiver.sh --rpc-url https://... --chain-id 73571 --private-key 0x...
#
# Chains: mainnet, sepolia, tenderly (requires --rpc-url)
#
# For named chains (mainnet, sepolia), credentials are fetched from
# Google Secret Manager automatically. Override with --alchemy-key and
# --private-key if running outside GCP.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults
CHAIN=""
RPC_URL=""
CHAIN_ID=""
PRIVATE_KEY=""
ALCHEMY_KEY=""
POOL_ADDRESS=""
OUTPUT="/tmp/receiver-address.txt"
EVM_VERSION="paris"
VERBOSE=""
PROJECT_ID="central-element-323112"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --chain <name>        Named chain: mainnet, sepolia, tenderly
  --rpc-url <url>       Explicit RPC URL (required for tenderly/custom)
  --chain-id <id>       Explicit chain ID (required with --rpc-url)
  --private-key <key>   Deployer wallet private key (fetched from SM if omitted)
  --alchemy-key <key>   Alchemy API key (fetched from SM if omitted)
  --pool-address <addr> Override Aave V3 Pool address
  --evm-version <ver>   EVM target for solc (default: paris). Use 'paris' for
                        Tenderly forks and maximum compatibility (avoids PUSH0).
  --output <path>       Output file for deployed address (default: /tmp/receiver-address.txt)
  --verbose             Enable verbose logging
  -h, --help            Show this help

Examples:
  # Deploy to Sepolia (auto-fetch credentials from Secret Manager):
  bash scripts/deploy-flash-loan-receiver.sh --chain sepolia

  # Deploy with explicit credentials:
  bash scripts/deploy-flash-loan-receiver.sh --chain sepolia --alchemy-key <key> --private-key <key>
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --chain) CHAIN="$2"; shift 2 ;;
        --rpc-url) RPC_URL="$2"; shift 2 ;;
        --chain-id) CHAIN_ID="$2"; shift 2 ;;
        --private-key) PRIVATE_KEY="$2"; shift 2 ;;
        --alchemy-key) ALCHEMY_KEY="$2"; shift 2 ;;
        --pool-address) POOL_ADDRESS="$2"; shift 2 ;;
        --evm-version) EVM_VERSION="$2"; shift 2 ;;
        --output) OUTPUT="$2"; shift 2 ;;
        --verbose) VERBOSE="--verbose"; shift ;;
        -h|--help) usage ;;
        *) echo "ERROR: Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$CHAIN" && -z "$RPC_URL" ]]; then
    echo "ERROR: Must specify --chain or --rpc-url" >&2
    exit 1
fi

# Fetch credentials from Secret Manager if not provided
if [[ -n "$CHAIN" && "$CHAIN" != "tenderly" ]]; then
    if [[ -z "$ALCHEMY_KEY" ]]; then
        echo "Fetching Alchemy API key from Secret Manager..."
        ALCHEMY_KEY=$(gcloud secrets versions access latest \
            --secret=alchemy-api-key \
            --project="${PROJECT_ID}" 2>/dev/null) || {
            echo "ERROR: Failed to fetch alchemy-api-key from Secret Manager." >&2
            echo "Pass --alchemy-key explicitly or configure gcloud auth." >&2
            exit 1
        }
    fi

    if [[ -z "$PRIVATE_KEY" ]]; then
        echo "Fetching wallet private key from Secret Manager..."
        PRIVATE_KEY=$(gcloud secrets versions access latest \
            --secret=defi-wallet-private-key \
            --project="${PROJECT_ID}" 2>/dev/null) || {
            echo "ERROR: Failed to fetch defi-wallet-private-key from Secret Manager." >&2
            echo "Pass --private-key explicitly." >&2
            exit 1
        }
    fi
fi

# Build Python command
PYTHON_CMD=(
    python "${SCRIPT_DIR}/deploy_contract.py"
)

if [[ -n "$CHAIN" ]]; then
    PYTHON_CMD+=(--chain "$CHAIN")
fi
if [[ -n "$RPC_URL" ]]; then
    PYTHON_CMD+=(--rpc-url "$RPC_URL")
fi
if [[ -n "$CHAIN_ID" ]]; then
    PYTHON_CMD+=(--chain-id "$CHAIN_ID")
fi
if [[ -n "$PRIVATE_KEY" ]]; then
    PYTHON_CMD+=(--private-key "$PRIVATE_KEY")
fi
if [[ -n "$ALCHEMY_KEY" ]]; then
    PYTHON_CMD+=(--alchemy-key "$ALCHEMY_KEY")
fi
if [[ -n "$POOL_ADDRESS" ]]; then
    PYTHON_CMD+=(--pool-address "$POOL_ADDRESS")
fi
PYTHON_CMD+=(--evm-version "$EVM_VERSION")
PYTHON_CMD+=(--output "$OUTPUT")
if [[ -n "$VERBOSE" ]]; then
    PYTHON_CMD+=("$VERBOSE")
fi

echo "Deploying FlashLoanReceiver..."
"${PYTHON_CMD[@]}"

echo ""
echo "Deployed address: $(cat "$OUTPUT")"
echo "Address saved to: $OUTPUT"
