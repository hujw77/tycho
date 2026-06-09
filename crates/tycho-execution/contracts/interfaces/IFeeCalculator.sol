// SPDX-License-Identifier: BUSL-1.1
pragma solidity ^0.8.26;

import {FeeRecipient} from "../lib/FeeStructs.sol";

/**
 * @notice Per-client custom fee configuration
 * @dev All fields pack into a single storage slot (6 bytes total)
 */
struct CustomFees {
    bool hasCustomFeeOnOutput; // 1 byte
    uint16 feeBpsOnOutput; // 2 bytes
    bool hasCustomFeeOnClientFee; // 1 byte
    uint16 feeBpsOnClientFee; // 2 bytes
}

interface IFeeCalculator {
    /**
     * @notice Calculates fees from the swap output amount
     * @dev Called from TychoRouter. Does not perform any accounting.
     *      Router fee parameters are retrieved from contract storage based on the user address.
     *      Client fee parameters are passed as function arguments.
     * @param amountIn The amount before fee deduction
     * @param client The client address to look up custom router fees for and to receive fees
     * @param clientFeeBps Client fee in basis points
     * @return amountOut The amount remaining after all fee deductions
     * @return feeRecipients Array of (address, feeAmount) tuples for fee distribution
     */
    function calculateFee(
        uint256 amountIn,
        address client,
        uint16 clientFeeBps
    )
        external
        view
        returns (uint256 amountOut, FeeRecipient[] memory feeRecipients);

    /**
     * @dev Returns the effective router fee on output amount for a specific client
     * @param client The client address to check
     * @return The fee in basis points (custom if set, otherwise default)
     */
    function getEffectiveRouterFeeOnOutput(address client)
        external
        view
        returns (uint16);

    /**
     * @notice Returns all clients with custom fee overrides and their current settings
     * @return clients Addresses of all clients with at least one custom fee
     * @return fees Custom fee configuration for each client (parallel array)
     */
    function getAllClientFees()
        external
        view
        returns (address[] memory clients, CustomFees[] memory fees);
}
