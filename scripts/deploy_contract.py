"""Deploy FlashLoanReceiver contract to target chain.

Compiles the Solidity source with py-solc-x and deploys via Web3.
Supports mainnet, Sepolia testnet, and custom RPC endpoints (Tenderly forks).

Usage:
    python scripts/deploy_contract.py --chain sepolia --alchemy-key <key> --private-key <key>
    python scripts/deploy_contract.py --rpc-url https://... --chain-id 73571 --private-key <key> --pool-address 0x...
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import solcx
from web3 import Web3

logger = logging.getLogger(__name__)

# Aave V3 Pool addresses per chain (SSOT for deploy script)
AAVE_POOL_ADDRESSES: dict[int, str] = {
    1: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",  # mainnet
    11155111: "0x6Ae43d3271ff6888e7Fc43Fd7321a503ff738951",  # sepolia
}

# Chain name to chain_id mapping
CHAIN_NAME_MAP: dict[str, int] = {
    "mainnet": 1,
    "sepolia": 11155111,
}

# Chain RPC URL templates (Alchemy)
CHAIN_RPC_TEMPLATES: dict[int, str] = {
    1: "https://eth-mainnet.g.alchemy.com/v2/{api_key}",
    11155111: "https://eth-sepolia.g.alchemy.com/v2/{api_key}",
}


def compile_contract(sol_path: Path, evm_version: str = "paris") -> tuple[str, str]:
    """Compile the Solidity source and return (abi_json, bytecode).

    Args:
        sol_path: Path to the .sol file.
        evm_version: EVM target version for solc. Default ``paris`` avoids
            the PUSH0 opcode (introduced in Shanghai) which is unsupported
            by some fork/testnet environments (e.g. older Tenderly forks,
            certain L2 chains). Use ``shanghai`` only when deploying to
            networks known to support EIP-3855.

    Returns:
        Tuple of (ABI as JSON string, bytecode hex string).

    Raises:
        FileNotFoundError: If the Solidity source doesn't exist.
        RuntimeError: If compilation fails.
    """
    if not sol_path.exists():
        raise FileNotFoundError(f"Solidity source not found: {sol_path}")

    source = sol_path.read_text()
    contract_name = "FlashLoanReceiver"

    solcx.set_solc_version("0.8.20")
    compiled = solcx.compile_source(
        source,
        output_values=["abi", "bin"],
        solc_version="0.8.20",
        evm_version=evm_version,
    )
    logger.info("Compiled with solc 0.8.20, evm_version=%s", evm_version)

    # solcx returns keys like '<stdin>:ContractName'
    contract_key = f"<stdin>:{contract_name}"
    if contract_key not in compiled:
        available = list(compiled.keys())
        raise RuntimeError(
            f"Contract '{contract_name}' not found in compiled output. Available: {available}"
        )

    contract_data = compiled[contract_key]
    abi = json.dumps(contract_data["abi"])
    bytecode: str = contract_data["bin"]
    return abi, bytecode


def deploy_contract(
    rpc_url: str,
    private_key: str,
    pool_address: str,
    abi_json: str,
    bytecode: str,
) -> str:
    """Deploy FlashLoanReceiver to the target chain.

    Args:
        rpc_url: Fully-resolved RPC endpoint URL.
        private_key: Deployer wallet private key (hex string).
        pool_address: Aave V3 Pool address for the target chain.
        abi_json: Contract ABI as JSON string.
        bytecode: Contract bytecode as hex string.

    Returns:
        Deployed contract address (checksummed).

    Raises:
        RuntimeError: If deployment fails or bytecode verification fails.
    """
    w3 = Web3(Web3.HTTPProvider(rpc_url))

    if not w3.is_connected():
        raise RuntimeError(f"Cannot connect to RPC endpoint: {rpc_url}")

    chain_id = w3.eth.chain_id
    account = w3.eth.account.from_key(private_key)
    wallet_address = account.address

    balance_wei = w3.eth.get_balance(wallet_address)
    balance_eth = w3.from_wei(balance_wei, "ether")
    logger.info(
        "Deployer: %s | Chain: %d | Balance: %s ETH",
        wallet_address,
        chain_id,
        balance_eth,
    )

    if balance_wei == 0:
        raise RuntimeError(
            f"Deployer wallet {wallet_address} has zero balance on chain {chain_id}. "
            "Fund the wallet before deploying."
        )

    # Build the deploy transaction
    abi = json.loads(abi_json)
    contract = w3.eth.contract(abi=abi, bytecode=bytecode)

    pool_checksum = Web3.to_checksum_address(pool_address)

    # Estimate gas for the constructor call
    nonce = w3.eth.get_transaction_count(wallet_address, "pending")
    base_tx: dict[str, int | str] = {
        "from": wallet_address,
        "nonce": nonce,
        "chainId": chain_id,
    }

    # Use EIP-1559 pricing on chains that support it (Sepolia, mainnet)
    latest_block = w3.eth.get_block("latest")
    base_fee_raw = (
        latest_block.get("baseFeePerGas")
        if isinstance(latest_block, dict)
        else getattr(latest_block, "baseFeePerGas", None)
    )
    if base_fee_raw is not None:
        base_fee = int(str(base_fee_raw))
        max_priority = w3.to_wei(2, "gwei")
        max_fee = base_fee * 2 + max_priority
        base_tx["maxFeePerGas"] = max_fee
        base_tx["maxPriorityFeePerGas"] = max_priority
        logger.info(
            "Using EIP-1559: baseFee=%d, maxFee=%d, maxPriority=%d", base_fee, max_fee, max_priority
        )
    else:
        base_tx["gasPrice"] = w3.eth.gas_price

    constructor_tx = contract.constructor(pool_checksum).build_transaction(base_tx)

    # Estimate gas with safety margin
    estimated_gas = w3.eth.estimate_gas(constructor_tx)
    gas_limit = int(estimated_gas * 1.3)
    constructor_tx["gas"] = gas_limit
    logger.info("Estimated gas: %d, using limit: %d", estimated_gas, gas_limit)

    # Sign and send
    signed_tx = w3.eth.account.sign_transaction(constructor_tx, private_key=private_key)
    raw_tx = getattr(signed_tx, "raw_transaction", None) or signed_tx.rawTransaction
    tx_hash = w3.eth.send_raw_transaction(raw_tx)
    tx_hash_hex = tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash)
    logger.info("Deploy tx sent: %s", tx_hash_hex)

    # Wait for receipt
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
    status = int(
        receipt.get("status", 0) if isinstance(receipt, dict) else getattr(receipt, "status", 0)
    )
    if status == 0:
        raise RuntimeError(f"Deploy transaction reverted: tx_hash={tx_hash_hex}")

    contract_address_raw = (
        receipt.get("contractAddress")
        if isinstance(receipt, dict)
        else getattr(receipt, "contractAddress", None)
    )
    if not contract_address_raw:
        raise RuntimeError(f"No contract address in receipt: tx_hash={tx_hash_hex}")

    contract_address = Web3.to_checksum_address(str(contract_address_raw))
    gas_used = int(
        receipt.get("gasUsed", 0) if isinstance(receipt, dict) else getattr(receipt, "gasUsed", 0)
    )
    logger.info(
        "Contract deployed at %s (gas_used=%d, tx=%s)",
        contract_address,
        gas_used,
        tx_hash_hex,
    )

    # Verify bytecode is present on-chain
    code = w3.eth.get_code(contract_address)
    code_hex = code.hex() if hasattr(code, "hex") else str(code)
    if not code_hex or code_hex in ("0x", "0x0", ""):
        raise RuntimeError(
            f"Bytecode verification failed: no code at {contract_address} on chain {chain_id}"
        )
    logger.info(
        "Bytecode verified: %d bytes at %s",
        len(code_hex) // 2,
        contract_address,
    )

    return contract_address


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Deploy FlashLoanReceiver contract to target chain."
    )
    parser.add_argument(
        "--chain",
        choices=["mainnet", "sepolia", "tenderly"],
        help="Named chain (mainnet, sepolia, tenderly). Resolves RPC URL automatically.",
    )
    parser.add_argument(
        "--rpc-url",
        help="Explicit RPC URL (required for --chain=tenderly or custom chains).",
    )
    parser.add_argument(
        "--chain-id",
        type=int,
        help="Explicit chain ID (required with --rpc-url for custom chains).",
    )
    parser.add_argument(
        "--private-key",
        required=True,
        help="Deployer wallet private key (hex string).",
    )
    parser.add_argument(
        "--alchemy-key",
        help="Alchemy API key (required for named chains without --rpc-url).",
    )
    parser.add_argument(
        "--pool-address",
        help="Override Aave V3 Pool address (auto-resolved for known chains).",
    )
    parser.add_argument(
        "--output",
        default="/tmp/receiver-address.txt",
        help="File to write the deployed address (default: /tmp/receiver-address.txt).",
    )
    parser.add_argument(
        "--evm-version",
        default="paris",
        help=(
            "EVM target version for solc compilation (default: paris). "
            "Use 'paris' for maximum compatibility (avoids PUSH0 opcode). "
            "Use 'shanghai' only for networks known to support EIP-3855."
        ),
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Main entry point for deploy script."""
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Resolve chain_id
    if args.chain and args.chain != "tenderly":
        chain_id = CHAIN_NAME_MAP[args.chain]
    elif args.chain_id:
        chain_id = args.chain_id
    else:
        logger.error("Must specify --chain (mainnet|sepolia) or --chain-id.")
        return 1

    # Resolve RPC URL
    rpc_url: str = ""
    if args.rpc_url:
        rpc_url = args.rpc_url
    elif chain_id in CHAIN_RPC_TEMPLATES:
        if not args.alchemy_key:
            logger.error(
                "Alchemy API key required for chain %d. Pass --alchemy-key or --rpc-url.",
                chain_id,
            )
            return 1
        rpc_url = CHAIN_RPC_TEMPLATES[chain_id].format(api_key=args.alchemy_key)
    else:
        logger.error("No RPC URL template for chain %d. Pass --rpc-url.", chain_id)
        return 1

    # Resolve Pool address
    pool_address: str = ""
    if args.pool_address:
        pool_address = args.pool_address
    elif chain_id in AAVE_POOL_ADDRESSES:
        pool_address = AAVE_POOL_ADDRESSES[chain_id]
    else:
        logger.error(
            "No known Aave V3 Pool address for chain %d. Pass --pool-address.",
            chain_id,
        )
        return 1

    # Compile
    sol_path = Path(__file__).resolve().parent.parent / "contracts" / "FlashLoanReceiver.sol"
    evm_version: str = args.evm_version
    logger.info("Compiling %s (evm_version=%s) ...", sol_path, evm_version)
    abi_json, bytecode = compile_contract(sol_path, evm_version=evm_version)
    logger.info("Compilation successful (bytecode: %d bytes)", len(bytecode) // 2)

    # Deploy
    logger.info(
        "Deploying FlashLoanReceiver to chain %d (pool=%s) ...",
        chain_id,
        pool_address,
    )
    contract_address = deploy_contract(
        rpc_url=rpc_url,
        private_key=args.private_key,
        pool_address=pool_address,
        abi_json=abi_json,
        bytecode=bytecode,
    )

    # Save address
    output_path = Path(args.output)
    output_path.write_text(contract_address)
    logger.info("Address written to %s", output_path)

    # Print to stdout for CI consumption
    print(contract_address)
    return 0


if __name__ == "__main__":
    sys.exit(main())
