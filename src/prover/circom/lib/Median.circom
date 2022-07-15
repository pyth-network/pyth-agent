pragma circom 2.0.0;

include "lib/SortedArray.circom";

// TODO: Take a real median when the list is odd or even.
template Median(N) {
    signal input  list[N];
    signal output result;

    // Check the input array is sorted before taking the median.
    component sorted = SortedArray(N);
    for(var i = 0; i<N; i++) sorted.a[i] = list[i];
    sorted.b === 1;

    // Return the middle list input as output.
    output <== list[N/2];
}
