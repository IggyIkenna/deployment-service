// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

interface IERC20 {
    function approve(address spender, uint256 amount) external returns (bool);
}

contract FlashLoanReceiver {
    address public immutable POOL;
    address public immutable OWNER;

    error UnauthorizedCaller(address caller, address expected);
    error UnauthorizedInitiator(address initiator, address expected);

    constructor(address pool) {
        POOL = pool;
        OWNER = msg.sender;
    }

    function executeOperation(
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata premiums,
        address initiator,
        bytes calldata
    ) external returns (bool) {
        if (msg.sender != POOL) revert UnauthorizedCaller(msg.sender, POOL);
        if (initiator != OWNER) revert UnauthorizedInitiator(initiator, OWNER);

        for (uint256 i = 0; i < assets.length; i++) {
            IERC20(assets[i]).approve(POOL, amounts[i] + premiums[i]);
        }
        return true;
    }
}
