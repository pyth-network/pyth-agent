pragma circom 2.0.0;

include "node_modules/circomlib/circuits/gates.circom";

// Ensures that the given arr is populated by n elements:
// all elements below index n are nonempty and all above are empty 
template CheckLength(max) {
    signal input arr[max];
    signal input n;
    
    // TODO: output signal

    var EMPTY = -1; 
    component xor_cond[max]; 
    component and_cond[max]; 
    component and_cond2[max];

    for (var i = 0; i < max; i++) {
        var p = i >= n; 
        var q = arr[i] == EMPTY;   

        var p1 = i < n; 
        var q1 = arr[i] != EMPTY;   

        xor_cond[i] = XOR(); 
        and_cond[i] = AND(); 
        and_cond2[i] = AND();

        and_cond[i].a <-- p; 
        and_cond[i].b <-- q; 
        and_cond[i].out === 1; 
        
        and_cond2[i].a <-- p1; 
        and_cond2[i].b <-- q1; 
        and_cond2[i].out === 1; 

        xor_cond[i].a <-- and_cond[i].out; 
        xor_cond[i].b <-- and_cond2[i].out;
        xor_cond[i].out === 1; 
    }
}
