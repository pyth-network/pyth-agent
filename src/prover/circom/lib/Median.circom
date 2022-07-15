pragma circom 2.0.0;

include "SortedArray.circom";

template Median(Max) {
    signal input  list[Max];
    signal output result;

    // Check the input array is sorted before taking the median.
    component sorted = SortedArray(Max);
    for(var i = 0; i<Max; i++) sorted.a[i] <== list[i];

    // Return the middle list input as output.
    var middle = Max >> 1;
    if (Max & 1) {
        result <== list[middle];
    } else {
        var left = list[middle - 1];
        var right = list[middle];
        result <== (left + right) / 2;
    }
}
