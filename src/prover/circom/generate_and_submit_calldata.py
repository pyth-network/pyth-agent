import sys
import subprocess
import web3
import json
import ast
from subprocess import Popen
import time
import os
from web3.gas_strategies.rpc import rpc_gas_price_strategy

contract_address = "0x15cD1Ca94333f269d02994776FEA32cE19e5534b"
transaction_signer_private_key = "2ee8524deed7967e7eabd4d7b6459f58312c8597c51de90e314ab9c299a94ed6"
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
    with open("/workspaces/agent/src/prover/circom/verifier_contract/build/contracts/ZKPyth.json") as f:
        info_json = json.load(f)
    abi = info_json["abi"]
    w3 = web3.Web3(web3.HTTPProvider('https://rinkeby.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161', request_kwargs={'timeout': 1200}))
    account = w3.eth.account.privateKeyToAccount(transaction_signer_private_key)
    contract = w3.eth.contract(address=contract_address, abi=abi)
    w3.eth.set_gas_price_strategy(rpc_gas_price_strategy)
    
    

    # Extract the parameters
    a = [int(x, 16) for x in calldata[0]]
    b = [[int(y, 16) for y in x] for x in calldata[1]]
    c = [int(x, 16) for x in calldata[2]]
    i = [int(x, 16) for x in calldata[3]]

    # Submit the proof and update the price
    nonce = w3.eth.getTransactionCount(
        account.address,
        'pending'
    );
    print(nonce)
    print(account.address)
    transaction = contract.functions.submitProof(a, b, c, i).build_transaction({
        'from': account.address,
        'gas': 10000000,
        'nonce': nonce,
        'chainId': 4,
        'gasPrice': w3.eth.generate_gas_price(),
    })
    tx_hash = w3.eth.account.sign_transaction(transaction, private_key=transaction_signer_private_key)
    w3.eth.send_raw_transaction(tx_hash.rawTransaction)
    # tx_receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    # print(f"Transaction successful with hash: { tx_receipt.transactionHash.hex() }")

if __name__ == "__main__":
    main()
