#!/bin/bash

snarkjs zkey export soliditycalldata $1 $2 > /workspaces/agent/src/prover/circom/calldata.txt
