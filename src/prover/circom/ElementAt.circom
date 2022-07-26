pragma circom 2.0.0;

function elementAt(arr, i, length) {
    var result = 0;
    for (var j = 0; j < length; j++) {
        result += arr[j] * (j == i);
    }   
    return result;
}
