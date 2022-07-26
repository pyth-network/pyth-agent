pragma circom 2.0.0;

include "node_modules/circomlib/circuits/mux1.circom";

include "SortedArray.circom";

// Median computes the median of the given array, which has
// size Max and n elements.
template Median(Max) {
    signal input  list[Max];
    signal input  n;
    signal output result;

    // Check the input array is sorted before taking the median.
    component sorted = SortedArray(Max);
    sorted.n <== n;
    for(var i = 0; i<Max; i++) sorted.a[i] <== list[i];
    
    // Median in case n is odd
    var middle = n >> 1;
    var odd_median = list[middle];
    
    // Median in case n is even
    var left = list[middle - 1];
    var right = list[middle];
    var even_median = (left + right) / 2;

    // Select the output depending on whether n is even or odd
    component mux = Mux1();
    mux.c[0] <-- odd_median;
    mux.c[1] <-- even_median;
    var n_odd = n & 1;
    mux.s <-- n_odd;

    result <== mux.out;
}
