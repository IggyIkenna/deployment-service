#!/usr/bin/env bash
set -euo pipefail

# Deploy RecursiveLeverageReceiver contract to a target chain.
#
# Usage:
#   bash scripts/deploy-recursive-leverage-receiver.sh --chain sepolia
#   bash scripts/deploy-recursive-leverage-receiver.sh --rpc-url https://... --chain-id 73571 --private-key 0x...
#
# Chains: ethereum, base, sepolia, tenderly (requires --rpc-url)
#
# For named chains (ethereum, base, sepolia), credentials are fetched from
# Google Secret Manager automatically. Override with --alchemy-key and
# --private-key if running outside GCP.
#
# Plan:
#   unified-trading-pm/plans/active/defi_recursive_borrow_archetypes_2026_05_10.md
#   Phase 4 — RecursiveLeverageReceiver.sol deploy script.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults
CHAIN=""
RPC_URL=""
CHAIN_ID=""
PRIVATE_KEY=""
ALCHEMY_KEY=""
POOL_ADDRESS=""
WETH_ADDRESS=""
SWAP_ROUTER_ADDRESS=""
OUTPUT="/tmp/recursive-leverage-receiver-address.txt"
EVM_VERSION="paris"
VERBOSE=""
PROJECT_ID="central-element-323112"

# Aave V3 Pool addresses per chain (per Phase 4 design SSOT)
AAVE_V3_POOL_ETHEREUM="0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
AAVE_V3_POOL_BASE="0xA238Dd80C259a72e81d7e4664a9801593F98d1c5"
AAVE_V3_POOL_SEPOLIA="0x6Ae43d3271ff6888e7Fc43Fd7321a503ff738951"

# Uniswap SwapRouter02 (same address all EVM chains)
SWAP_ROUTER_DEFAULT="0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45"

# WETH9 addresses per chain
WETH_ETHEREUM="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
WETH_BASE="0x4200000000000000000000000000000000000006"
WETH_SEPOLIA="0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Deploy RecursiveLeverageReceiver.sol (action-encoder pattern) to a target chain.

Options:
  --chain <name>           Named chain: ethereum, base, sepolia, tenderly
  --rpc-url <url>          Explicit RPC URL (required for tenderly/custom)
  --chain-id <id>          Explicit chain ID (required with --rpc-url)
  --private-key <key>      Deployer wallet private key (fetched from SM if omitted)
  --alchemy-key <key>      Alchemy API key (fetched from SM if omitted)
  --pool-address <addr>    Override Aave V3 Pool address
  --weth-address <addr>    Override WETH9 address
  --swap-router <addr>     Override Uniswap SwapRouter02 address
  --evm-version <ver>      EVM target for solc (default: paris)
  --output <path>          Output file for deployed address
  --verbose                Enable verbose logging
  -h, --help               Show this help

Examples:
  # Deploy to Sepolia testnet (auto-fetch credentials from Secret Manager):
  bash scripts/deploy-recursive-leverage-receiver.sh --chain sepolia

  # Deploy to Ethereum mainnet:
  bash scripts/deploy-recursive-leverage-receiver.sh --chain ethereum

  # Deploy to Base mainnet:
  bash scripts/deploy-recursive-leverage-receiver.sh --chain base

  # Deploy to Tenderly fork with explicit credentials:
  bash scripts/deploy-recursive-leverage-receiver.sh \\
    --rpc-url "\${TENDERLY_FORK_RPC_URL}" \\
    --chain-id 1 \\
    --private-key "\${DEPLOYER_PRIVATE_KEY}"
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
        --weth-address) WETH_ADDRESS="$2"; shift 2 ;;
        --swap-router) SWAP_ROUTER_ADDRESS="$2"; shift 2 ;;
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

# Resolve chain-specific defaults
if [[ -n "$CHAIN" ]]; then
    case "$CHAIN" in
        ethereum)
            [[ -z "$POOL_ADDRESS" ]] && POOL_ADDRESS="$AAVE_V3_POOL_ETHEREUM"
            [[ -z "$WETH_ADDRESS" ]] && WETH_ADDRESS="$WETH_ETHEREUM"
            ;;
        base)
            [[ -z "$POOL_ADDRESS" ]] && POOL_ADDRESS="$AAVE_V3_POOL_BASE"
            [[ -z "$WETH_ADDRESS" ]] && WETH_ADDRESS="$WETH_BASE"
            ;;
        sepolia)
            [[ -z "$POOL_ADDRESS" ]] && POOL_ADDRESS="$AAVE_V3_POOL_SEPOLIA"
            [[ -z "$WETH_ADDRESS" ]] && WETH_ADDRESS="$WETH_SEPOLIA"
            ;;
        tenderly)
            if [[ -z "$RPC_URL" ]]; then
                echo "ERROR: --rpc-url required for tenderly chain" >&2
                exit 1
            fi
            if [[ -z "$POOL_ADDRESS" ]]; then
                echo "ERROR: --pool-address required for tenderly (fork may differ from mainnet)" >&2
                exit 1
            fi
            [[ -z "$WETH_ADDRESS" ]] && WETH_ADDRESS="$WETH_ETHEREUM"
            ;;
        *)
            echo "ERROR: Unknown chain: $CHAIN. Use: ethereum, base, sepolia, tenderly" >&2
            exit 1
            ;;
    esac
fi

[[ -z "$SWAP_ROUTER_ADDRESS" ]] && SWAP_ROUTER_ADDRESS="$SWAP_ROUTER_DEFAULT"

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

# Build Python deploy command — reuses deploy_contract.py with contract override
PYTHON_CMD=(
    python "${SCRIPT_DIR}/deploy_contract.py"
    --contract RecursiveLeverageReceiver
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
PYTHON_CMD+=(--weth-address "$WETH_ADDRESS")
PYTHON_CMD+=(--swap-router-address "$SWAP_ROUTER_ADDRESS")
PYTHON_CMD+=(--evm-version "$EVM_VERSION")
PYTHON_CMD+=(--output "$OUTPUT")
if [[ -n "$VERBOSE" ]]; then
    PYTHON_CMD+=("$VERBOSE")
fi

echo "Deploying RecursiveLeverageReceiver to chain=${CHAIN:-custom}..."
echo "  Aave V3 Pool:       ${POOL_ADDRESS}"
echo "  WETH9:              ${WETH_ADDRESS}"
echo "  SwapRouter02:       ${SWAP_ROUTER_ADDRESS}"
"${PYTHON_CMD[@]}"

echo ""
echo "Deployed address: $(cat "$OUTPUT")"
echo "Address saved to: $OUTPUT"
echo ""
echo "Next steps:"
echo "  1. Update unified-api-contracts FLASH_LOAN_RECEIVER_REGISTRY with deployed address"
echo "  2. Update testnet_contracts.yaml with recursive_leverage_receiver address for chain_id"
echo "  3. Verify: cast call <address> 'owner()' --rpc-url <rpc>"
