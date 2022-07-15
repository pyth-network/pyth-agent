pragma circom 2.0.0;

include "../node_modules/circomlib/circuits/eddsa.circom";

// InputVerifier receives a list of signatures, prices, and components, and
// verifies that the prices and components have been correctly signed by the
// corresponding signature public keys.
//
// The price and component at position `i`, must correspond to the signature
// at position `i`.
//
// The `N` parameter refers to the number of price components the circuit is
// able to process, and is fixed at compile time.
template InputVerifier(N) {
    // Components of the input ed25519 signatures. Used by ed25519.circom
    signal input A[N][256];
    signal input R[N][256];
    signal input S[N][256];

    // Price and confidence components are encoded as 64 bit binary integers as
    // ed25519 requires binary inputs.
    signal input price[N][64];
    signal input confidence[N][64];

    // Publishers sign price and confidence with the following code:
    //
    //   `signature = ed25519::sign(price || confidence, secret_key)`
    //
    // Therefore we must also create a verifier that can verify the signature
    // of these 128 bit messages.
    component verifiers[N];

    // Each component verifies one signature. If verification fails the
    // component will violate a constraint.
    for(var i = 0; i < N; i++) {
        // Create and assign signature messages to the EdDSA verifier.
        verifiers[i] = EdDSAVerifier(128);
        for(var i = 0; i < 64; i++) verifiers.msg[i]     = price[i];
        for(var i = 0; i < 64; i++) verifiers.msg[64+ i] = confidence[i];

        // Assign the expected signature to the verifier.
        for(var i = 0; i < 256; i++) {
                verifiers[i].A[j]  <== A[i][j];
                verifiers[i].R8[j] <== R[i][j];
                verifiers[i].S[j]  <== S[i][j];
        }
    }
}
