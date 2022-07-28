const { buildEddsa } = require("circomlibjs");
const { randomBytes } = require("crypto");

/*
Paramaters:
- n
- prices
- confs
- timestamps

Input data should be a JSON object of the form
{
    'n': 3,
    'prices': [5, 2, 8],
    'confs': [10, 2, 8],
    'timestamps': [9, 6, 0],
    'fee': 56,
}

Example minified:
{"n":3,"prices":[5,2,8],"confs":[10,2,8],"timestamps":[9,6,0],"fee":56}
*/

// Hardcoded max length of Pyth input
const max = 2;
const empty = 0;

// Utility functions
const toBinString = (bytes) =>
    bytes.reduce(
        (str, byte) =>
        str + byte.toString(2).padStart(8, "0"),
        ""
    );

const conv = num => {
    let b = new ArrayBuffer(8);
    new DataView(b).setBigUint64(0, num);
    return Array.from(new Uint8Array(b));
};

const fromBinString = (binString) =>
    new Uint8Array(binString.match(/.{1,8}/g).map((byte) => parseInt(byte, 2)));

const toCircomBin = (bytes) =>
    bytes.reduce(
            (str, byte) =>
            str + byte.toString(2).padStart(8, "0").split("").reverse().join(""),
            ""
    ).split('').map(v => Number(v));

const padArray = (arr, size, fill) =>
    arr.concat(Array(size - arr.length).fill(fill));

(async function () {

    // Parse the JSON from the CLI argument
    const data = JSON.parse(process.argv.slice(2)[0]);
    
    // Create a proof input object which we will populate
    var input = {
        N: data.n,
        price_model: [],
        prices: data.prices,
        confs: data.confs,
        timestamps: data.timestamps,
        A: [],
        R: [],
        S: [],
        fee: data.fee,
    }

    // Generate the price model votes
    input.price_model = input.prices.flatMap((_, i) => [
        [i, 0], // Price - Confidence
        [i, 1], // Price
        [i, 2], // Price + Confidence
    ]);

    // Sort the price model by computing the real values and sorting them
    input.price_model.sort((a, b) => {
        const priceA = input.prices[a[0]] + (input.confs[a[0]] * (a[1]-1));
        const priceB = input.prices[b[0]] + (input.confs[b[0]] * (b[1]-1));
        return priceA - priceB;
    });

    const eddsa = await buildEddsa();

    // For each data input, generate a random signature and add to the price model
    for (let i = 0; i < data.n; i++) {
        
        // Generate the random signature
        const privateKey = randomBytes(32);

        // Generate A (public key)
        const pubKey = eddsa.prv2pub(privateKey);
        const packedPubKey = eddsa.babyJub.packPoint(pubKey);  
        const A = packedPubKey;

        // Generate the payload
        // Encode payload with bytes reversed to satisfy Circom's
        // idiom of using left to right bits.
        const price = data.prices[i];
        const conf = data.confs[i];
        const timestamp = data.timestamps[i];
        const payload = 
            toBinString(conv(BigInt(price)).reverse()) +
            toBinString(conv(BigInt(conf)).reverse()) +
            toBinString(conv(BigInt(timestamp)).reverse());

        // Sign the payload
        // Message payload: 
        // Packed array of price (64-bit); conf (64-bit); timestamp (64-bit)
        const signature = eddsa.signPedersen(privateKey, fromBinString(payload));
        const packedSignature = eddsa.packSignature(signature);
        const R = packedSignature.slice(0, 32);
        const S = packedSignature.slice(32, 64);

        // Append the signature to the input
        input.A[i] = toCircomBin(A);
        input.R[i] = toCircomBin(R);
        input.S[i] = toCircomBin(S);
    }

    // Pad the result to the length the Pyth contract expects
    input.price_model = padArray(input.price_model, max*3, Array(2).fill(empty));
    input.prices = padArray(input.prices, max, empty);
    input.confs = padArray(input.confs, max, empty);
    input.timestamps = padArray(input.timestamps, max, empty);
    input.A = padArray(input.A, max, Array(256).fill(empty));
    input.R = padArray(input.R, max, Array(256).fill(empty));
    input.S = padArray(input.S, max, Array(256).fill(empty));

    // Output the result
    console.log(JSON.stringify(input));

})();
