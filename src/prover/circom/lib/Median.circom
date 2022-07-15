pragma circom 2.0.0;

include "SortedArray.circom";

template Median(N) {
    signal input  list[N];
    signal output result;

    // Check the input array is sorted before taking the median.
    component sorted = SortedArray(N);
    for(var i = 0; i<N; i++) sorted.a[i] <== list[i];

    // Return the middle list input as output.
    var middle = N >> 1;
    if (N & 1) {
        result <== list[middle];
    } else {
        var left  = list[middle - 1];
        var right = list[middle];
        result <== (left + right) / 2;
    }
}
