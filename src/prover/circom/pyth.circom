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

// - [x] Timestamp oracle as a median.
// - [x] Fee for proving.
// - [x] Staleness threshold on price inputs.
// - [x] Publishers commit to timestamps.
// - [x] Check the signatures are from different publishers 
// - [x] refactor code to template / functions
// - [x] checks for subgroup order 
// - [x] Contract with N prices must work for <N. Dynamic N.
// - [x] Min pub, required.
// - [ ] Generate and deploy verfication contract
// - [ ] Make simulator generate proof
// - [ ] Make simulator submit proof to verification contract 
// - [ ] Constraint assignment when calling templates, through the whole circuit
// - [ ] BinSum output length
// - [ ] Unit tests for each logic block

include "node_modules/circomlib/circuits/comparators.circom";
include "node_modules/circomlib/circuits/gates.circom";
include "node_modules/circomlib/circuits/binsum.circom";
include "node_modules/circomlib/circuits/binsub.circom";
include "node_modules/circomlib/circuits/bitify.circom";
include "node_modules/circomlib/circuits/mux1.circom";

include "SortedArray.circom";
include "Median.circom";
include "InputVerifier.circom";
include "PriceModel.circom"; 
include "ElementAt.circom";

function calc_price(price_model, prices, confs, i) {
    var price = prices[price_model[i][0]];
    var conf  = confs[price_model[i][0]];
    var op    = price_model[i][1];

    if(op == 0) {
        return price - conf;
    } else if (op == 1) {
        return price;
    } else {
        return price + conf;
    }
}

// Proof is per-price
template Pyth(max, timestampThreshold, minPublishers) {

    /*
        Template Inputs 
        max - maximum number of components included in the proof
        timestampThreshold - staleness threshold for data feed aggregation
        // callers public key is equal to the pubkey passed into the template 
    */

    // Publisher Controlled Inputs:
    //
    // Requirements:
    // Check all array elements are sorted.
    // Check all array elements are non-zero up to N. (0 indicates NULL).
    // Check all array elements are zero (NULL) >= N.
    //
    // These requirements allow us to prove aggregations for a variable number
    // of elements. 
    signal input    N;

    // TODO: better name than vote
    // price_model:
    // 2-D array. Tuple of (price, conf and op) for each vote. These represent the provers
    // input to the aggregation for each data feed. A single data feed's (price, conf) is
    // converted to a tuple of (price-conf, price, price+conf) representing the 3 votes.
    // Each vote is represented in the price model as a tuple of (index, op) where
    // that x is the index into prices and confs arrays and op represents the operation
    // (+-NOP) calc_price uses to reconstruct the values.
    signal input  price_model[max*3][2];
    signal input  prices[max];
    signal input  confs[max];
    signal input  timestamps[max];

    // Signatures: A/R/S components are part of the ed25519 signature scheme.
    // 
    // NOTE: The hash used in ed25519 in this contract uses the MiMC hash
    //       function rather than SHA256 as in standard ed25519. You can use
    //       circomlibjs to produce signatures that match this algorithm.
    signal input  A[max][256];
    signal input  R[max][256];
    signal input  S[max][256];

    // Return fee input as output for verification contracts to charge users.
    signal input  fee;

    // Output p-values for aggregattion.
    signal output p25;
    signal output p50;
    signal output p75;

    // Width of the confidence interval around the p50 aggregate.
    signal output confidence; 

    // Check that we have the minimum amount of publishers
    // TODO: double-check size of input
    component enoughPublishers = GreaterThan(64);
    enoughPublishers.in[0] <== N;
    enoughPublishers.in[1] <== minPublishers;
    enoughPublishers.out === 1;

    // In order to prevent the prover from choosing 
    component timestamp_median = Median(max);
    timestamp_median.n <== N;
    for(var i = 0; i < max; i++) {
        timestamp_median.list[i] <== timestamps[i];
    }

    // All timestamps must be within a certain range of the median.
    // TODO: Does it have to be bits?
    component timestamp_gt[max];
    component timestamp_lt[max];
    for(var i = 0; i < max; i++) timestamp_lt[i] = LessThan(64);
    for(var i = 0; i < max; i++) timestamp_gt[i]  = GreaterThan(64);

    component valid_timestamp[max];
    component timestamp_in_range[max];
    for(var i = 0; i < max; i++) {
        timestamp_lt[i].in[0] <== timestamps[i];
        timestamp_lt[i].in[1] <== timestamp_median.result + timestampThreshold;

        timestamp_gt[i].in[0]  <== timestamps[i];
        timestamp_gt[i].in[1]  <== timestamp_median.result - timestampThreshold;

        timestamp_in_range[i] = AND();
        timestamp_in_range[i].a <== timestamp_lt[i].out;
        timestamp_in_range[i].b <== timestamp_gt[i].out;
        
        valid_timestamp[i] = OR();
        var absent = i >= N;
        valid_timestamp[i].a <== timestamp_in_range[i].out;
        valid_timestamp[i].b <-- absent;
        valid_timestamp[i].out === 1;
    }


    // Convert Price/Confidence pairs into binary encoded values for signature
    // verification.
    component Num2Bits_price_components[max];
    component Num2Bits_conf_components[max];
    component Num2Bits_timestamp_components[max];
    for(var i=0; i<max; i++) {
        Num2Bits_price_components[i] = Num2Bits(64);
        Num2Bits_conf_components[i]  = Num2Bits(64);
        Num2Bits_timestamp_components[i] = Num2Bits(64);

        Num2Bits_price_components[i].in <== prices[i];
        Num2Bits_conf_components[i].in  <== confs[i];
        Num2Bits_timestamp_components[i].in <== timestamps[i];
    }
    
    // We need to check that each signature given corresponds to a unique public key.
    // This is so a single publisher cannot submit multiple entries to a proof.
    // A is the public key component of the signature, so we need to check that all
    // the given A's are unique.
    //
    // We do this by requiring that A is sorted in ascending order, then iterating over
    // the array and checking that a[i + 1] > a[i].
    //
    // A less-than check can be implemented by performing `A - B` and checking if the
    // operation overflowed. In other words, if `A - B < 0` then B must have been larger
    // than A. Circom however does not have signed bit operations. It's possible to still
    // perform this check however by creating our own range by shifting all the numbers
    // up by half of the integer range.
    //
    // For example, with an 8 bit range, shifting everything by 128 (1<<8), we get the
    // following representation:
    //
    // 00000000 = -128
    // 10000000 =    0
    // 11111111 =  128
    //
    // We can now achieve the same less-than check by performing
    //
    // 0 + A - B
    //
    // This generalizes to n-bit representations - we just shift everything by n (1 << 256) 
    // As all positive numbers now have a 1 MSB, and all negative numbers have a 0 MSB,
    // we can check if an overflow occured by checking if the MSB has become 0.  
    // Note that LSB is index 0, and bits go left-to-right.
    // component binsum_pubkey[max];
    // component binsub_pubkey[max]; 
    // component unique_pubkeys[max];
    // for (var i = 0; i < (max - 1); i++) {   
    //     binsum_pubkey[i] = BinSum(267, 2);  // TODO: check nbits logic in binsum
    //     binsub_pubkey[i] = BinSub(257); 
        
    //     // Perform 0 + A - B to check equality

    //     // Add 1 << n to A[i]
    //     for (var j = 0; j < 255; j++) { 
    //         binsum_pubkey[i].in[0][j] <-- A[i][j]; 
    //         binsum_pubkey[i].in[1][j] <-- 0;
    //     }
    //     binsum_pubkey[i].in[0][255] <-- A[i][255];
    //     binsum_pubkey[i].in[1][255] <-- 1;

    //     // pad BinSum with extra zeros
    //     // TODO: why is this necessary
    //     for (var j = 256; j < 267; j++) {
    //         binsum_pubkey[i].in[0][j] <-- 0;
    //         binsum_pubkey[i].in[1][j] <-- 0;
    //     }

    //     var result = binsum_pubkey[0].out[255];

    //     // Subtract A[i+1] from the result of (A[i] + (1 << n))
    //     // TODO: double-check this
    //     for (var j = 0; j < 255; j++) {
    //         binsub_pubkey[i].in[0][j] <-- binsum_pubkey[i].out[j]; 
    //         binsub_pubkey[i].in[1][j] <-- A[i+1][j];     
    //     }
    //     binsub_pubkey[i].in[0][255] <-- 0;
    //     binsub_pubkey[i].in[1][255] <-- 0;
    //     binsub_pubkey[i].in[0][256] <-- 0;
    //     binsub_pubkey[i].in[1][256] <-- 0;

    //     // Check that the MSB is 0, indicating that the subtraction "overflowed", so A[i+1] > A[i]
    //     unique_pubkeys[i] = OR();
    //     var absent = i >= N;
    //     var unique = binsub_pubkey[i].out[255] == 0;
    //     unique_pubkeys[i].a <-- unique;
    //     unique_pubkeys[i].b <-- absent;
    //     unique_pubkeys[i].out === 1;
    // }

    // // Verify the encoded data against incoming signatures.
    // component verifiers[max];
    // for(var i = 0; i < max; i++) {
    //     verifiers[i] = InputVerifier();

    //     // Pass in first signature if input is absent
    //     var present = i < N;
    //     var absent = i >= N;

    //     // Assign output of binary conversion to signature verifier.
    //     for (var j = 0; j < 64; j++) {

    //         // Use this selecting trick to avoid dynamic array accesses
    //         verifiers[i].price[j]      <-- (Num2Bits_price_components[i].out[j] * present) + (Num2Bits_price_components[0].out[j] * absent);
    //         verifiers[i].confidence[j] <-- (Num2Bits_conf_components[i].out[j] * present) + (Num2Bits_conf_components[0].out[j] * absent);
    //         verifiers[i].timestamp[j]  <-- (Num2Bits_timestamp_components[i].out[j] * present) + (Num2Bits_timestamp_components[0].out[j] * absent);
    //     }

    //     // Assign Signature Components.
    //     for (var j = 0; j < 256; j++) {
    //         verifiers[i].A[j] <-- (A[i][j] * present) + (A[0][j] * absent);
    //         verifiers[i].R[j] <-- (R[i][j] * present) + (R[0][j] * present);
    //         verifiers[i].S[j] <-- (S[i][j] * present) + (S[0][j] * present);
    //     }
    // }

    // // We verify that the price_model has been given to us in order by iterating
    // // over the signal set and checking that every element is smaller than its
    // // successor. I.E: all(map(lambda a, b: a <= b, prices))
    // component sort_checks[max*3];
    // for(var i=1; i<max*3; i++) {    
    //     var a = calc_price(price_model, prices, confs, i-1);
    //     var b = calc_price(price_model, prices, confs, i);

    //     // Constrain r1 < r2
    //     var absent = i >= (N * 3);
    //     sort_checks[i] = OR();
    //     sort_checks[i].a <-- a <= b;
    //     sort_checks[i].b <-- absent; 
    //     sort_checks[i].out === 1;
    // }

    // component price_calc = PriceModelCore(max*3);
    // price_calc.n <-- N*3;
    // for(var i=0; i<max*3; i++) {
    //     // TODO: Constraint missing, do we need one? <-- dangerous.
    //     price_calc.prices[i] <-- calc_price(price_model, prices, confs, i);
    // }

    // // Calculate confidence from aggregation result.
    // var agg_conf_left  = price_calc.agg_p50 - price_calc.agg_p25;
    // var agg_conf_right = price_calc.agg_p75 - price_calc.agg_p50;
    // var agg_conf       =
    //     agg_conf_right > agg_conf_left
    //         ? agg_conf_right
    //         : agg_conf_left;

    // signal confidence_1 <-- agg_conf_right > agg_conf_left;
    // signal confidence_2 <== confidence_1*agg_conf_right;
    // signal confidence_3 <== (1-confidence_1)*agg_conf_left;

    // confidence <== confidence_2 + confidence_3;
    // p25        <== price_calc.agg_p25;
    // p50        <== price_calc.agg_p50;
    // p75        <== price_calc.agg_p75;
 }

component main = Pyth(10, 10, 3);
