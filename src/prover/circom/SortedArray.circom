// SortedArray consumes a list of signals and outputs 1 if the list is sorted.

pragma circom 2.0.0;

include "node_modules/circomlib/circuits/gates.circom";

// By checking an existing sort is correct instead of sorting an unsorted input
// list of signals, we save on intermediate signals and move the work to the
// prover instead.
//
//     a[Max] -- List of signals to be checked for sort, with n elements.
//     b      -- Intermediate list of signals, all are 1 if the input is sorted.
template SortedArray(max) {
    signal input a[max];
    signal input n;

    // TODO: output signal 

    component valid[max];
    for(var i=1; i<max; i++) {
        var absent = i >= n;
        var sorted = a[i-1] <= a[i];

        valid[i] = OR();
        valid[i].a <-- absent;
        valid[i].b <-- sorted;

        valid[i].out === 1;
    }
}
