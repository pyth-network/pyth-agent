pragma circom 2.0.0;

include "Median.circom";
include "ElementAt.circom";

// PriceModelCore contains the core Pyth aggregation logic.
template PriceModelCore(max) {
    // List of prices +/- confidences in pre-sorted order.
    signal input prices[max];
    signal input n;

    // Output signal containing the aggregate + confidences.
    signal output agg_p25;
    signal output agg_p50;
    signal output agg_p75;

    // Find the middle element of the input list.
    var middle = n >> 1;
    var p25    = n >> 2;
    var p75    = n - p25 - 1;
    
    // Compute the p50 median
    component median = Median(max);
    median.n <== n;
    for (var i = 0; i < max; i++) {
        median.list[i] <== prices[i];
    }
    agg_p50 <-- median.result;

    // Extract p25/p75.
    // This is because the index is unknown, and will cause the constraint to be 
    // unknown. Need a way of computing the array access without making the constaints
    // non-quadratic, so without using the input signal.
    agg_p25 <-- elementAt(prices, p25, max);
    agg_p75 <-- elementAt(prices, p75, max);
}
