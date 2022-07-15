pragma circom 2.0.0;

// Tricks&Notes on Circom:
// - Scalar size errors happen when not providing all signals.
// - Power of Tau ceremony must choose an exponent e such that 2**e > constraint count.
// - C++ errors will fail constraints even before proof is produced. Asserts.
// - At least one quadratic output seems to be needed, so a squared input is used here.
// - TODO: Security; understand constraining outputs and when needed. See operators page on Circom.
//
// Goals
// - Faster Proving Times
// - Dig more into Curve support and what our limitations are (currently BN254).
// - Proof of concept P2P Protocol (even without P2P is fine).

// - [ ] Timestamp oracle as a median.
// - [x] Fee for proving.
// - [ ] Staleness threshold on price inputs.
// - [ ] Publishers commit to timestamps.
// - [ ] Publishers commit to observed online amount.
// - [ ] Min pub, required.

include "node_modules/circomlib/circuits/bitify.circom";
include "node_modules/circomlib/circuits/comparators.circom";

include "lib/SortedArray.circom";
include "InputVerifier.circom";
include "PriceModel.circom";

function calc_price(price_model, prices, confs, i) {
    var price = prices[price_model[i][0]];
    var conf  = confs[price_model[i][1]];
    var op    = price_model[i][2];

    if(op == 0) {
        return price - conf;
    } else if (op == 1) {
        return price;
    } else {
        return price + conf;
    }
}

// Proof is per-price
template Pyth(N) {
    // Publisher Controlled Inputs
    signal input  price_model[N*3][3];
    signal input  prices[N];
    signal input  confs[N];
    signal input  timestamps[N];
    signal input  observed_online[N];

    // Return fee input as output for verification contracts to charge users.
    signal input  fee;

    // Signatures: A/R/S components are part of the ed25519 signature scheme.
    // 
    // NOTE: The hash used in ed25519 in this contract uses the MiMC hash
    //       function rather than SHA256 as in standard ed25519. You can use
    //       circomlibjs to produce signatures that match this algorithm.
    signal input  A[N][256];
    signal input  R[N][256];
    signal input  S[N][256];

    // Output p-values for aggregattion.
    signal output p25;
    signal output p50;
    signal output p75;

    // Width of the confidence interval around the p50 aggregate.
    signal output confidence;

    // Convert Price/Confidence pairs into binary encoded values for signatur
    // verification.
    component Num2Bits_price_components[N];
    component Num2Bits_conf_components[N];
    for(var i=0; i<N; i++) {
        Num2Bits_price_components[i] = Num2Bits(64);
        Num2Bits_conf_components[i]  = Num2Bits(64);
        Num2Bits_price_components[i].in <== prices[i];
        Num2Bits_conf_components[i].in  <== confs[i];
    }

    // Verify the encoded data against incoming signatures.
    component verifiers[N];
    for(var i = 0; i < N; i++) {
        verifiers[i] = InputVerifier();

        // Assign output of binary conversion to signature verifier.
        for (var j = 0; j < 64; j++) {
            verifiers[i].price[j]      <== Num2Bits_price_components[i].out[j];
            verifiers[i].confidence[j] <== Num2Bits_conf_components[i].out[j];
        }

        // Assign Signature Components.
        for (var j = 0; j < 256; j++) {
            verifiers[i].A[j] <== A[i][j];
            verifiers[i].R[j] <== R[i][j];
            verifiers[i].S[j] <== S[i][j];
        }
    }

    // We verify that the price_model has been given to us in order by iterating
    // over the signal set and checking that every element is smaller than its
    // successor. I.E: all(map(lambda a, b: a <= b, prices))
    signal sort_checks[N*3];
    for(var i=1; i<N*3; i++) {
        var a = calc_price(price_model, prices, confs, i-1);
        var b = calc_price(price_model, prices, confs, i);

        // Constrain r1 < r2
        sort_checks[i] <-- a <= b;
        sort_checks[i] === 1;
    }

    component price_calc = PriceModelCore(N*3);
    for(var i=0; i<N*3; i++) {
        // TODO: Constraint missing, do we need one? <-- dangerous.
        price_calc.prices[i] <-- calc_price(price_model, prices, confs, i);
    }

    // Calculate confidence from aggregation result.
    var agg_conf_left  = price_calc.agg_p50 - price_calc.agg_p25;
    var agg_conf_right = price_calc.agg_p75 - price_calc.agg_p50;
    var agg_conf       =
        agg_conf_right > agg_conf_left
            ? agg_conf_right
            : agg_conf_left;

    signal confidence_1 <-- agg_conf_right > agg_conf_left;
    signal confidence_2 <== confidence_1*agg_conf_right;
    signal confidence_3 <== (1-confidence_1)*agg_conf_left;

    confidence <== confidence_2 + confidence_3;
    p25        <== price_calc.agg_p25;
    p50        <== price_calc.agg_p50;
    p75        <== price_calc.agg_p75;
 }

component main = Pyth(10);
