#!/usr/bin/env bash
set -e

snarkjs zkey export solidityverifier /workspaces/agent/src/prover/circom/build/pyth_0001.zkey /workspaces/agent/src/prover/circom/verifier_contract/contracts/CircomVerifier.sol
cd /workspaces/agent/src/prover/circom/verifier_contract && /workspaces/agent/src/prover/circom/verifier_contract/node_modules/.bin/truffle migrate --network development
