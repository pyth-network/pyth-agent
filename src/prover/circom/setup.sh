#!/usr/bin/env bash

# Build the circuit
rm -rf build/ && mkdir -p build && cd build
circom ../pyth.circom --r1cs --wasm --sym --c -o .

# Enter the Pyth_cpp witness generation code produced by Circom and
# generate a Groth16 Powers of Tau setup. These files are colocated in the
# build/pyth_cpp directory because everything Circom assumes relative paths.
echo 'Powers of Tau: Init'
time snarkjs powersoftau new bn128 12 pot12_0000.ptau -v

echo 'Powers of Tau: Round 1'
openssl rand -base64 21 | time snarkjs powersoftau contribute pot12_0000.ptau pot12_0001.ptau --name="First contribution" -v

echo 'Powers of Tau: Phase 2'
time snarkjs powersoftau prepare phase2 pot12_0001.ptau pot12_final.ptau -v
time snarkjs groth16 setup pyth.r1cs pot12_final.ptau pyth_0000.zkey

echo 'Powers of Tau: Phase 2, Round 1'
openssl rand -base64 21 | time snarkjs zkey contribute pyth_0000.zkey pyth_0001.zkey --name="1st Contributor Name" -v

echo 'Powers of Tau: Generate Public Key'
time snarkjs zkey export verificationkey pyth_0001.zkey verification_key.json

# Build the witness generator
cd pyth_cpp && make
