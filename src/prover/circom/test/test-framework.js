const { expect } = require("chai");
const fs = require("fs");
const path = require("path");
const c_tester = require("circom_tester").c;
const { randomBytes } = require("crypto");
const { buildEddsa } = require("circomlibjs");
const { assert } = require("console");

/*
Test cases:

- Valid signature
    - Good case

- Invalid signature
    - Private and public key mismatch
    - Data included in signature is the wrong size


*/

// Utility Functions
        const toBinString = (bytes) =>
            bytes.reduce(
                (str, byte) =>
                str + byte.toString(2).padStart(8, "0"),
                ""
            );

        const fromBinString = (binString) =>
            new Uint8Array(binString.match(/.{1,8}/g).map((byte) => parseInt(byte, 2)));

        const conv = num => {
            let b = new ArrayBuffer(8);
            new DataView(b).setBigUint64(0, num);
            return Array.from(new Uint8Array(b));
        };

        const toCircomBin = (bytes) =>
            bytes.reduce(
                (str, byte) =>
                str + byte.toString(2).padStart(8, "0").split("").reverse().join(""),
                ""
        ).split('').map(v => Number(v));

        const pad64BitArray = arr =>
            arr.concat(Array(64 - arr.length).fill(0));

describe("Test Input Verifier", function () {

    //boiler plate 
    this.timeout(0);
    let circuitPath = path.join(__dirname, "input-verifier.test.circom");
    let circuit;
    let eddsa;

    /*
    Paramaters of the circuit:
    timestampThreshold: staleness threshold for data feed aggregation
    max: maximum number of components included in the proof
    */

    /*
    Inputs to proof
    N: number of data items in this proof
    price_model: 2d array of "votes"
    prices: signed prices
    confs: signed confs
    timestamps: signed timestamps
    observed_online: signed observed online
    A/R/S: ed25519 signature components
    fee: fee prover recieves when this proof is verified
    */
    
    before ( async() => {
        const circuitCode = `pragma circom 2.0.0;

include "../InputVerifier.circom"; 
component main = InputVerifier(); 
`;

        fs.writeFileSync(circuitPath, circuitCode, "utf8");


        circuit = await c_tester(circuitPath);
    });

    beforeEach ( async() => {
        eddsa = await buildEddsa();
    });

    //tests    
    it ('there should be a correct number of signatures', async() => {        
        /*
        Inputs to InputVerifier:
        A/R/S: one set of ed25519 signature components
        price: 64-bit signed binary price input
        confidence: 64-bit signed binary confidence input
        timestamp: 64-bit signed binary timestamp input
        online: 64-bit signed binary online input
        */

        // Generate signature
        privateKey = randomBytes(32);  

        // Generate some random input values
        let price = [6]
        let conf = [10];
        let timestamp = [75463746];
                
        // Sign components
        // Message payload: 
        // packed array of price (64-bit), conf (64-bit), timestamp (64-bit), online (64-bit)
        const payload = 
            toBinString(conv(BigInt(price)).reverse()) +
            toBinString(conv(BigInt(conf)).reverse()) +
            toBinString(conv(BigInt(timestamp)).reverse());

        // Generate A (public key)
        const pubKey = eddsa.prv2pub(privateKey);
        const packedPubKey = eddsa.babyJub.packPoint(pubKey);  
        const A = packedPubKey;
                
        // Generate signature component (R S)
        const signature = eddsa.signPedersen(privateKey, fromBinString(payload));
        const packedSignature = eddsa.packSignature(signature);
        const R = packedSignature.slice(0, 32);
        const S = packedSignature.slice(32, 64);

        // Input object
        let input = {
            'A': toCircomBin(A),
            'R': toCircomBin(R),
            'S': toCircomBin(S),
            'price': pad64BitArray(toCircomBin(price)),
            'confidence': pad64BitArray(toCircomBin(conf)),
            'timestamp': pad64BitArray(toCircomBin(timestamp)),
        }

        console.log('----- Input ------')
        console.log(input)

        const witness = await circuit.calculateWitness(input, true);

        console.log(witness);

        await circuit.assertOut(witness, {});
    });
    
it ('there should fail if public key is invalid', async() => {        
    /*
    Inputs to InputVerifier:
    A/R/S: one set of ed25519 signature components
    price: 64-bit signed binary price input
    confidence: 64-bit signed binary confidence input
    timestamp: 64-bit signed binary timestamp input
    online: 64-bit signed binary online input
    */

    // Generate signature
    privateKey = randomBytes(32);

    // Generate some random input values
    let price = [6]
    let conf = [10];
    let timestamp = [75463746];
            
    // Sign components
    // Message payload: 
    // packed array of price (64-bit), conf (64-bit), timestamp (64-bit), online (64-bit)
    const payload = 
        toBinString(conv(BigInt(price)).reverse()) +
        toBinString(conv(BigInt(conf)).reverse()) +
        toBinString(conv(BigInt(timestamp)).reverse());

        // Generate A (public key)
        const pubKey = eddsa.prv2pub(privateKey);
        const pubKeyInvalid = randomBytes(32);
        assert(pubKey != pubKeyInvalid);

        const packedPubKey = eddsa.babyJub.packPoint(pubKey);  
        const A = packedPubKey;
            
    // Generate signature component (R S)
    const signature = eddsa.signPedersen(privateKey, fromBinString(payload));
    const packedSignature = eddsa.packSignature(signature);
    const R = packedSignature.slice(0, 32);
    const S = packedSignature.slice(32, 64);

    // Input object
    let input = {
        'A': toCircomBin(A),
        'R': toCircomBin(R),
        'S': toCircomBin(S),
        'price': pad64BitArray(toCircomBin(price)),
        'confidence': pad64BitArray(toCircomBin(conf)),
        'timestamp': pad64BitArray(toCircomBin(timestamp)),
    }

    console.log('----- Input ------')
    console.log(input)

    // Constraints should 
    assert.throws(await circuit.calculateWitness(input, true), Error, "msg")
});
    
    // it ('should verify the signatures correctly passed in', async() => {
         
    // }); 
});   

