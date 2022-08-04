var ZKPyth = artifacts.require("ZKPyth");
var Pairing = artifacts.require("Pairing");
var Verifier = artifacts.require("Verifier");

module.exports = function(deployer) {

  // Deploy all the Points
  for (var i = 0; i < 1; i++) {
    var G1Point = artifacts.require("G1Points"+i);
    deployer.deploy(G1Point);
    deployer.link(G1Point, [Pairing, Verifier, ZKPyth]);
  }

  // Deploy the Pairing library
  deployer.deploy(Pairing);
  deployer.link(Pairing, [Verifier, ZKPyth]);

  // Deploy the Verifier library
  deployer.deploy(Verifier);
  deployer.link(Verifier, ZKPyth);

  // Deploy the ZKPyth library
  deployer.deploy(ZKPyth);
};
