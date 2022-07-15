pragma circom 2.0.0;

// PriceModelCore contains the core Pyth aggregation logic.
template PriceModelCore(N) {
    // List of prices +/- confidences in pre-sorted order.
    signal input  prices[N];

    // Output signal containing the aggregate + confidences.
    signal output agg_p25;
    signal output agg_p50;
    signal output agg_p75;

    // Find the middle element of the input list.
    var middle = N >> 1;
    var p25    = N >> 2;
    var p75    = N - p25 - 1;

    // If N is an odd number of elements, it means the `middle` is in fact the
    // actual midpoint, we can simply return. When even the `middle` is the
    // right element of the middle pair, the result is then the average of the
    // two elements.
    if (N & 1) {
        agg_p50 <== prices[middle];
    } else {
        var left  = prices[middle - 1];
        var right = prices[middle];
        agg_p50 <== (left + right) / 2;
    }

    // Extract p25/p75.
    agg_p25 <== prices[p25];
    agg_p75 <== prices[p75];
}
