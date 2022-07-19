pragma circom 2.0.0;

// TODO: use poseidon hash function instead (eddsaposeidon)
include "node_modules/circomlib/circuits/eddsa.circom";

// InputVerifier receives a list of signatures, prices, and components, and
// verifies that the prices and components have been correctly signed by the
// corresponding signature public keys.
//
// The price and component at position `i`, must correspond to the signature
// at position `i`.
template InputVerifier() { 
    //tie the sigature to the pubkey of the caller 
    // Components of the input ed25519 signatures. Used by ed25519.circom
    signal input A[256];
    signal input R[256];
    signal input S[256];

    // // Price and confidence components are encoded as 64 bit binary integers as
    // ed25519 requires binary inputs.
    signal input price[64];
    signal input confidence[64];
    signal input timestamp[64];

    // Publishers sign price and confidence with the following code:
    //
    //   `signature = ed25519::sign(price || confidence || timestamp || online || secret_key)`
    //
    // Therefore we must also create a verifier that can verify the signature
    // of these 256 bit messages.
    component verifier;
    verifier = EdDSAVerifier(256);

    // Each component verifies one signature. If verification fails the
    // component will violate a constraint.

    // Create and assign signature messages to the EdDSA verifier.
    for(var i = 0; i < 64; i++) verifier.msg[i]     <== price[i];
    for(var i = 0; i < 64; i++) verifier.msg[64+i] <== confidence[i];
    for(var i = 0; i < 64; i++) verifier.msg[128+i] <== timestamp[i];

    // Assign the expected signature to the verifier.
    for(var j = 0; j < 256; j++) {
        verifier.A[j]  <== A[j];
        verifier.R8[j] <== R[j];
        verifier.S[j]  <== S[j];
    }
}
