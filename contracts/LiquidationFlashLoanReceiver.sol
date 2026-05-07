// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @notice Atomic-bundle MEV liquidation receiver.
///         Borrows debt asset via Aave V3 flash loan -> liquidates an
///         under-collateralized position -> swaps seized collateral back
///         to the debt asset on Uniswap V3 -> repays flash loan + premium.
///         Net of all four legs in a single transaction; reverts if any step
///         fails (zero capital exposure to the liquidation proper).
///
/// @dev    Encodes the flow described in
///         strategy_service.engine.strategies.v2.mev.liquidation_bundle:
///           leg 0 BORROW   - flashLoanSimple(debtAsset, debtAmount, this, params, 0)
///           leg 1 TRADE    - pool.liquidationCall(collateral, debt, borrower, debt, false)
///           leg 2 SWAP     - swapRouter.exactInputSingle(seizedCol -> debtAsset)
///           leg 3 (implicit) - approve(pool, debtAmount + premium) for repayment
///
/// @dev    Compiled with solc 0.8.20 to match the simple FlashLoanReceiver
///         used elsewhere in the workspace fork test infra.

interface IERC20 {
    function approve(address spender, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
    function transfer(address to, uint256 amount) external returns (bool);
}

interface IPool {
    function liquidationCall(
        address collateralAsset,
        address debtAsset,
        address user,
        uint256 debtToCover,
        bool receiveAToken
    ) external;
}

interface ISwapRouter02 {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    function exactInputSingle(ExactInputSingleParams calldata params)
        external
        payable
        returns (uint256 amountOut);
}

contract LiquidationFlashLoanReceiver {
    address public immutable POOL;
    address public immutable OWNER;
    address public immutable SWAP_ROUTER;

    error UnauthorizedCaller(address caller, address expected);
    error UnauthorizedInitiator(address initiator, address expected);
    error UnsupportedCallback();

    /// @param pool        Aave V3 Pool address (chain-specific).
    /// @param swapRouter  Uniswap V3 SwapRouter02 address (same on most chains).
    constructor(address pool, address swapRouter) {
        POOL = pool;
        OWNER = msg.sender;
        SWAP_ROUTER = swapRouter;
    }

    /// @notice Trigger the bundle. The owner submits with the bundle params
    ///         encoded; AAVE will invoke executeOperation with this data.
    function triggerLiquidationBundle(
        address debtAsset,
        uint256 debtAmount,
        address collateralAsset,
        address borrower,
        uint24 swapFeeTier
    ) external {
        if (msg.sender != OWNER) revert UnauthorizedCaller(msg.sender, OWNER);
        bytes memory params = abi.encode(
            collateralAsset,
            borrower,
            swapFeeTier
        );
        // Use the simpler flashLoanSimple variant so executeOperation runs
        // a single asset/amount tuple — matches the receiver's interface.
        (bool ok,) = POOL.call(
            abi.encodeWithSignature(
                "flashLoanSimple(address,address,uint256,bytes,uint16)",
                address(this),
                debtAsset,
                debtAmount,
                params,
                uint16(0)
            )
        );
        require(ok, "flashLoanSimple call failed");
    }

    /// @notice AAVE V3 flashLoanSimple callback. Called once with the
    ///         borrowed funds in this contract's balance; must end with
    ///         this contract approving POOL to pull (amount + premium).
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external returns (bool) {
        if (msg.sender != POOL) revert UnauthorizedCaller(msg.sender, POOL);
        if (initiator != address(this)) revert UnauthorizedInitiator(initiator, address(this));

        (
            address collateralAsset,
            address borrower,
            uint24 swapFeeTier
        ) = abi.decode(params, (address, address, uint24));

        // Approve POOL to pull debt for the liquidationCall step.
        IERC20(asset).approve(POOL, amount);

        // Step 2 — liquidate. Pool pulls 'amount' of asset (debt), gives us
        // collateral worth amount * (1 + liq_bonus_pct/100) at oracle prices.
        IPool(POOL).liquidationCall(
            collateralAsset,
            asset,
            borrower,
            amount,
            false  // receiveAToken=false → seize underlying collateral
        );

        // Step 3 — swap the seized collateral back to the debt asset on
        // Uniswap V3. Use balanceOf so we sweep whatever liquidationCall
        // actually paid out (minus any rounding dust).
        uint256 collateralBalance = IERC20(collateralAsset).balanceOf(address(this));
        if (collateralBalance > 0) {
            IERC20(collateralAsset).approve(SWAP_ROUTER, collateralBalance);
            ISwapRouter02.ExactInputSingleParams memory swapParams =
                ISwapRouter02.ExactInputSingleParams({
                    tokenIn: collateralAsset,
                    tokenOut: asset,
                    fee: swapFeeTier,
                    recipient: address(this),
                    amountIn: collateralBalance,
                    amountOutMinimum: 0,  // accept any output; oracle vs DEX risk
                    sqrtPriceLimitX96: 0
                });
            ISwapRouter02(SWAP_ROUTER).exactInputSingle(swapParams);
        }

        // Step 4 — approve POOL to pull amount + premium for flash repayment.
        IERC20(asset).approve(POOL, amount + premium);
        return true;
    }

    /// @notice Sweep any remaining tokens to the owner (post-bundle profit).
    function sweep(address token) external {
        if (msg.sender != OWNER) revert UnauthorizedCaller(msg.sender, OWNER);
        uint256 bal = IERC20(token).balanceOf(address(this));
        if (bal > 0) IERC20(token).transfer(OWNER, bal);
    }

    /// @dev Reject the more-complex executeOperation(arrays) signature —
    ///      this receiver is wired for flashLoanSimple only.
    function executeOperation(
        address[] calldata,
        uint256[] calldata,
        uint256[] calldata,
        address,
        bytes calldata
    ) external pure returns (bool) {
        revert UnsupportedCallback();
    }
}
