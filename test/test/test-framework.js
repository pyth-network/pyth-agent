const { expect } = require("chai");
const fs = require("fs");
const path = require("path");
const tester = require("circom").tester; 

describe("Test Input Verifier", function () {

    //boiler plate 
    this.timeout(0);
    let circuitPath = path.join(__dirname, "input-verifier.test.circom");
    let circuit;

    //initialize the inputs 
    let Max = 10; 
    let fee = 1; 
    let timestampThreshold = 10; // in seconds
    let thresholdDataFeed = 7;  
    let N = 8; //number of publishers online

    //setup accounts 
    const account1 = new Account(1);
    const account2 = new Account(2);
    const account3 = new Account(3);

    const accounts = []; 
    
    before ( async() => {
        const circuitCode = `
        'include "..src/InputVerifier.circom'; 
        'component main = InputVerifier(N)'; 
        `; 
    }); 

    fs.writeFileSync(circuitPath, circuitCode, "utf8");

    circuit = await tester(circuitPath, {reduceConstraints:false});
    await circuit.loadConstraints();
    console.log("Constraints: " + circuit.constraints.length + "\n");    

    //tests    
    it ('there should be a correct number of signatures', async() => {
        
    });
    
    
    it ('should verify the signatures correctly passed in', async() => {
         
    }); 
});   

