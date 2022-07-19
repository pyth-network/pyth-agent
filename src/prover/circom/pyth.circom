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
// - [x] Publishers commit to observed online amount.
// - [x] Check the signatures are from different publishers 
// - [ ] refactor code to template / functions
// - [ ] checks for subgroup order 
// - [ ] Min pub, required.
// - [ ] Contract with N prices must work for <N. Dynamic N.

include "node_modules/circomlib/circuits/comparators.circom";
include "node_modules/circomlib/circuits/gates.circom";

include "lib/SortedArray.circom";
include "lib/Median.circom";
include "InputVerifier.circom";
include "PriceModel.circom"; 

//check conditions to ensure that elements up to the expected length 
//are nonempty and everything above is empty 
function checkLength(arr, MAX, N) {
    var EMPTY = -1; 
    component xor_cond[MAX]; 
    component and_cond[MAX]; 
    component and_cond2[MAX];   

    for (var i = 0; i < MAX: i++) {
        var p = i >= N; 
        var q = arr[i] == EMPTY;   

        var p1 = i < N; 
        var q1 = arr[i] != EMPTY;   

        xor_cond[i] = XOR(); 
        and_cond[i] = AND(); 
        and_cond2[i] = AND();

        and_cond[i].a <-- p; 
        and_cond[i].b <-- q; 
        and_cond.out <== 1; 
        
        and_cond2[i].a <-- p1; 
        and_cond2[i].b <-- q1; 
        and_cond2.out <== 1; 

        xor_cond[i].a <-- and_cond[i]; 
        xor_cond[i].b <-- and_cond2[i] 
        xor_cond.out <== 1; 
    }

}

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
template Pyth(Max, timestampThreshold) {

    /*
        Template Inputs 
        Max - maximum number of components included in the proof
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
    signal input  price_model[Max*3][2];
    signal input  prices[Max];
    signal input  confs[Max];
    signal input  timestamps[Max];
    signal input  observed_online[Max]; 

    // Signatures: A/R/S components are part of the ed25519 signature scheme.
    // 
    // NOTE: The hash used in ed25519 in this contract uses the MiMC hash
    //       function rather than SHA256 as in standard ed25519. You can use
    //       circomlibjs to produce signatures that match this algorithm.
    signal input  A[Max][256];
    signal input  R[Max][256];
    signal input  S[Max][256];

    // Output p-values for aggregattion.
    signal output p25;
    signal output p50;
    signal output p75;

    // Width of the confidence interval around the p50 aggregate.
    signal output confidence; 
    // Return fee input as output for verification contracts to charge users.
    signal input  fee;
    
    // Checks that each input array has the expected (N) number of elements.
    checkLength(prices, MAX, N); 
    checkLength(confs, MAX, N); 
    checkLength(timestamps, MAX, N); 
    checkLength(observed_online, MAX, N);  

    // We use the last bit of the S component of each ED25519 signature 
    // as a flag which represents if that value is present.
    // In a valid signature, the last 3 bits need to be 0 for the signature 
    // to be non-malleable (curve order size ~< last 3 bits).
    // We therefore extract the last bit of each S component into an array,
    // and check that this has the expected length.
    LastBitsSignatures[MAX]; 
    for (var i = 0; i < MAX; i++) {      
        LastBitsSignatures[i] <-- S[i][255];     
    }
    checkLength(LastBitsSignatures, MAX, N);


    
    
    // In order to prevent the prover from choosing 
    component timestamp_median = Median(Max);
    for(var i = 0; i < Max; i++) {
        timestamp_median.list[i] <== timestamps[i];
    }


    // All timestamps must be within a certain range of the median.
    // TODO: Does it have to be bits?
    component timestamp_gt[Max];
    component timestamp_lte[Max];
    for(var i = 0; i < Max; i++) timestamp_lte[i] = LessEqThan(64);
    for(var i = 0; i < Max; i++) timestamp_gt[i]  = GreaterThan(64);

    for(var i = 0; i < Max; i++) {
        timestamp_lte[i].in[0] <== timestamps[i];
        timestamp_lte[i].in[1] <== timestamp_median.result;
        timestamp_gt[i].in[0]  <== timestamps[i];
        timestamp_gt[i].in[1]  <== timestamp_median.result - TimestampThreshold;
    }


    // Convert Price/Confidence pairs into binary encoded values for signature
    // verification.
    component Num2Bits_price_components[Max];
    component Num2Bits_conf_components[Max];
    component Num2Bits_timestamp_components[Max];
    component Num2Bits_online_components[Max];
    for(var i=0; i<Max; i++) {
        Num2Bits_price_components[i] = Num2Bits(64);
        Num2Bits_conf_components[i]  = Num2Bits(64);
        Num2Bits_timestamp_components[i] = Num2Bits(64);
        Num2Bits_online_components[i] = Num2Bits(64);

        Num2Bits_price_components[i].in <== prices[i];
        Num2Bits_conf_components[i].in  <== confs[i];
        Num2Bits_timestamp_components[i].in <== timestamps[i];
        Num2Bits_online_components[i].in <== observed_online[i];
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
    component binsub_pubkey[MAX]; 
    component binadd_pubkey[MAX]; 
    for (var i = 0; i < MAX; i++) {   
        
        binsub_pubkey[i] = BinSub(257);  
        binadd_pubkey[i] = BinAdd(257); // TODO: use BinSum
        
        // Perform 0 + A - B to check equality
        for (var j = 0; j < 256; j++) { 
            binadd_pubkey[i].in[0][j] <-- A[i][j]; 
            binadd_pubkey[i].in[1][255] <-- 1;     
            
            binsub_pubkey[i].in[0][j] <-- binadd_pubkey[i].out[j]; 
            binsub_pubkey[i].in[1][j] <-- A[i+1][j];     
        }

        // Check that the MSB is 0, indicating that the subtraction "overflowed", so A[i+1] > A[i]
        binsub_pubkey[i].out[255] === 0; 
    }

    // Verify the encoded data against incoming signatures.
    component verifiers[Max];
    for(var i = 0; i < Max; i++) {
        verifiers[i] = InputVerifier();

        // Assign output of binary conversion to signature verifier.
        for (var j = 0; j < 64; j++) {
            verifiers[i].price[j]      <== Num2Bits_price_components[i].out[j];
            verifiers[i].confidence[j] <== Num2Bits_conf_components[i].out[j];
            verifiers[i].timestamp[j]  <== Num2Bits_timestamp_components[i].out[j];
            verifiers[i].online[j]     <== Num2Bits_online_components[i].out[j];
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
    signal sort_checks[Max*3];
    for(var i=1; i<Max*3; i++) {
        var a = calc_price(price_model, prices, confs, i-1);
        var b = calc_price(price_model, prices, confs, i);

        // Constrain r1 < r2
        sort_checks[i] <-- a <= b;
        sort_checks[i] === 1;
    }

    component price_calc = PriceModelCore(Max*3);
    for(var i=0; i<Max*3; i++) {
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

component main = Pyth(10, 10);
