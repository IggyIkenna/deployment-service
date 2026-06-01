// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

// Plan: unified-trading-pm/plans/active/defi_recursive_borrow_archetypes_2026_05_10.md
// Phase 4 — Extended RecursiveLeverageReceiver (action-encoder pattern, Option A).
//
// Parallel deploy alongside existing FlashLoanReceiver.sol (passthrough) — does NOT replace it.
// Consumed by execution-service/execution_service/defi_execution/orchestrators/recursive_loop_orchestrator.py
// via RecursiveLoopOrchestrator.open(mode=FLASH) + .unwind().

// ---------------------------------------------------------------------------
// Minimal interfaces (inline — no external dependency manager required)
// ---------------------------------------------------------------------------

interface IERC20 {
    function approve(address spender, uint256 amount) external returns (bool);
    function transfer(address to, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
}

interface IAavePool {
    function supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode) external;
    function borrow(address asset, uint256 amount, uint256 interestRateMode, uint16 referralCode, address onBehalfOf) external;
    function repay(address asset, uint256 amount, uint256 interestRateMode, address onBehalfOf) external returns (uint256);
    function withdraw(address asset, uint256 amount, address to) external returns (uint256);
}

interface ISwapRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }
    struct ExactOutputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 amountOut;
        uint256 amountInMaximum;
        uint160 sqrtPriceLimitX96;
    }
    function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);
    function exactOutputSingle(ExactOutputSingleParams calldata params) external payable returns (uint256 amountIn);
}

interface IWETH9 {
    function deposit() external payable;
    function withdraw(uint256 amount) external;
}

// ---------------------------------------------------------------------------
// Action struct — generic action-encoder pattern (Phase 4 AD: Option A)
// ---------------------------------------------------------------------------

struct Action {
    address target;
    bytes data;
    uint256 value;
}

// ---------------------------------------------------------------------------
// RecursiveLeverageReceiver
// ---------------------------------------------------------------------------

contract RecursiveLeverageReceiver {
    address public immutable POOL;
    address public immutable OWNER;

    // Re-entrancy guard (1 = unlocked, 2 = locked)
    uint256 private _lock = 1;

    // Whitelisted targets: {AavePool, UniswapV3SwapRouter02, WETH9}
    mapping(address => bool) private _allowedTargets;

    // Whitelisted selectors from the allowed target interfaces.
    // Closed set per Phase 4 design: supply, borrow, repay, withdraw (Aave);
    // exactInputSingle, exactOutputSingle (Uniswap V3); deposit, withdraw (WETH9); approve (ERC20).
    mapping(bytes4 => bool) private _allowedSelectors;

    // ---------------------------------------------------------------------------
    // Named errors (no string reverts per Phase 4 design)
    // ---------------------------------------------------------------------------
    error UnauthorizedCaller(address caller, address expected);
    error UnauthorizedInitiator(address initiator, address expected);
    error TargetNotAllowed(address target);
    error SelectorNotAllowed(bytes4 selector);
    error ActionFailed(uint256 idx);
    error InsufficientRepaymentBalance(uint256 owed, uint256 balance);
    error ReentrancyDetected();

    // ---------------------------------------------------------------------------
    // Constructor
    // ---------------------------------------------------------------------------

    constructor(address pool, address uniswapRouter, address weth9) {
        POOL = pool;
        OWNER = msg.sender;

        // Whitelist targets
        _allowedTargets[pool] = true;
        _allowedTargets[uniswapRouter] = true;
        _allowedTargets[weth9] = true;

        // Aave V3 IPool selectors
        _allowedSelectors[IAavePool.supply.selector] = true;
        _allowedSelectors[IAavePool.borrow.selector] = true;
        _allowedSelectors[IAavePool.repay.selector] = true;
        _allowedSelectors[IAavePool.withdraw.selector] = true;

        // UniswapV3 SwapRouter02 selectors
        _allowedSelectors[ISwapRouter.exactInputSingle.selector] = true;
        _allowedSelectors[ISwapRouter.exactOutputSingle.selector] = true;

        // WETH9 selectors
        _allowedSelectors[IWETH9.deposit.selector] = true;
        _allowedSelectors[IWETH9.withdraw.selector] = true;

        // ERC20 approve — needed for per-action inline approval (no infinite approvals)
        _allowedSelectors[IERC20.approve.selector] = true;
    }

    // ---------------------------------------------------------------------------
    // executeOperation — called by Aave V3 pool during flash loan
    // ---------------------------------------------------------------------------

    function executeOperation(
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata premiums,
        address initiator,
        bytes calldata params
    ) external returns (bool) {
        // Non-reentrant guard
        if (_lock != 1) revert ReentrancyDetected();
        _lock = 2;

        // Caller + initiator checks
        if (msg.sender != POOL) revert UnauthorizedCaller(msg.sender, POOL);
        if (initiator != OWNER) revert UnauthorizedInitiator(initiator, OWNER);

        // Decode action list + correlation_id (correlation_id threaded to Python events side)
        (Action[] memory actions, ) = abi.decode(params, (Action[], bytes32));

        // Execute each action in sequence
        for (uint256 i = 0; i < actions.length; i++) {
            Action memory action = actions[i];

            // Target whitelist check
            if (!_allowedTargets[action.target]) revert TargetNotAllowed(action.target);

            // Selector whitelist check (first 4 bytes of calldata)
            if (action.data.length < 4) revert SelectorNotAllowed(bytes4(0));
            bytes4 sel = bytes4(action.data[0]) | (bytes4(action.data[1]) >> 8) | (bytes4(action.data[2]) >> 16) | (bytes4(action.data[3]) >> 24);
            if (!_allowedSelectors[sel]) revert SelectorNotAllowed(sel);

            // Execute with exact value — no infinite approval carried across actions
            (bool ok, ) = action.target.call{value: action.value}(action.data);
            if (!ok) revert ActionFailed(i);
        }

        // Repayment check: contract must hold enough to repay flash loan + premium
        // Simple single-asset flash loan (assets[0]) — multi-asset not used in recursive patterns.
        uint256 owed = amounts[0] + premiums[0];
        uint256 bal = IERC20(assets[0]).balanceOf(address(this));
        if (bal < owed) revert InsufficientRepaymentBalance(owed, bal);

        // Approve pool for exact repayment amount (NOT infinite)
        IERC20(assets[0]).approve(POOL, owed);

        _lock = 1;
        return true;
    }

    // ---------------------------------------------------------------------------
    // sweep — OWNER escape hatch to recover stranded tokens
    // ---------------------------------------------------------------------------

    function sweep(address token) external {
        if (msg.sender != OWNER) revert UnauthorizedCaller(msg.sender, OWNER);
        uint256 bal = IERC20(token).balanceOf(address(this));
        if (bal > 0) {
            IERC20(token).transfer(OWNER, bal);
        }
    }

    // Accept ETH (needed for WETH9 unwrap flows)
    receive() external payable {}
}
