pragma solidity ^0.8.11;

// 1. Store the current publisher set.
// 2. Have a verifyProof function that can verify proofs (cheaply if already done).
// 3. Ignore anything that's stale.
// 4. Function to return the latest proven price.
// 5. Not allow older proofs to override newer ones.

// TODO: suspect the contract is reverting due to dynamic memory access when constructing arrays in getPoints().
// Either initialize statically sized arrays or use .push

import "./CircomVerifier.sol";

contract ZKPyth is Verifier {
    // Array of Public Keys.
    uint256 keysetVersion;

    // Offsets of data items in the proof
    mapping(string => uint256) offsets;

    // Cache of price and confidence
    uint256 price;
    uint256 confidence;

    function getPrice() public view returns (uint256, uint256) {
        return (price, confidence);
    }

    // Function taking Circom verify arguments, will forward to the on-chain.
    function submitProof(
        uint256[2] memory a,
        uint256[2][2] memory b,
        uint256[2] memory c,
        uint256[3891] memory input
    ) public {
        // Verify the proof
        require(Verifier.verifyProof(a, b, c, input), "proof is invalid");

        // Update the cache
        price = input[offsets["p50"]];
        confidence = input[offsets["confidence"]];
    }

    constructor() {
        // Size of a single signature component
        uint256 signature_size = 256;

        // Circuit compile parameters
        uint256 max = 5;

        // Input offsets
        offsets["price_model"] = 2 * max * 3;
        offsets["prices"] = offsets["price_model"] + max;
        offsets["confs"] = offsets["prices"] + max;
        offsets["timestamps"] = offsets["confs"] + max;
        offsets["A"] = offsets["timestamps"] + (max * signature_size);
        offsets["R"] = offsets["A"] + (max * signature_size);
        offsets["S"] = offsets["R"] + (max * signature_size);
        offsets["fee"] = offsets["S"] + 1;

        // Output offsets
        offsets["p25"] = offsets["fee"] + 1;
        offsets["p50"] = offsets["p25"] + 1;
        offsets["p75"] = offsets["p50"] + 1;
        offsets["confidence"] = offsets["p75"] + 1;
    }
}
