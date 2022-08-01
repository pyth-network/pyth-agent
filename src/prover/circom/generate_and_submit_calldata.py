import sys
import subprocess
import web3
import json
import ast
from subprocess import Popen
import time
import os

contract_address = "0x76Ea767C8e94Cb772e4D91308c503B4269f41b2C"
transaction_signer_private_key = "0x419e044186bb4469fd63a7b58826a47c0ef46c8119a13c19f52f2b52b05a5fee"
calldata_file = "/workspaces/agent/src/prover/circom/calldata.txt"

def main():

    public_path = sys.argv[1]
    proof_path = sys.argv[2]

    if os.path.exists(calldata_file):
        os.remove(calldata_file)

    # Generate the calldata and read in the data
    subprocess.run(['/workspaces/agent/src/prover/circom/generate_calldata.sh', public_path, proof_path])
    calldata = ast.literal_eval(open(calldata_file, "r").read().strip())

    # Submit the proof to the contract
    with open("/workspaces/agent/src/prover/circom/verifier_contract/build/contracts/PythZK.json") as f:
        info_json = json.load(f)
    abi = info_json["abi"]
    w3 = web3.Web3(web3.HTTPProvider('http://127.0.0.1:8545', request_kwargs={'timeout': 1200}))
    account = w3.eth.account.privateKeyToAccount(transaction_signer_private_key)
    contract = w3.eth.contract(address=contract_address, abi=abi)

    # Extract the parameters
    a = [int(x, 16) for x in calldata[0]]
    b = [[int(y, 16) for y in x] for x in calldata[1]]
    c = [int(x, 16) for x in calldata[2]]
    i = [int(x, 16) for x in calldata[3]]

    # Submit the proof and update the price
    contract.functions.submitProof(a, b, c, i).transact({'from': account.address, 'gas': 30000000})

if __name__ == "__main__":
    main()
