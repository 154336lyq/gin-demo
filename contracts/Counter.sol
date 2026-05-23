// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @title Counter —— 本地演示用极简合约，便于练习 eth_call 与 ABI 解码。
contract Counter {
    uint256 public number;

    function setNumber(uint256 newNumber) external {
        number = newNumber;
    }

    function increment() external {
        number++;
    }
}
