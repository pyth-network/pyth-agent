{ pkgs ? import <nixpkgs> { }
, ...
}:
let
  solana-validator-bumped = pkgs.callPackage ./nix/solana-validator.nix {
    inherit (pkgs.apple_sdk.frameworks) IOKit Security AppKit;
    rustPlatform = pkgs.rustPackages_1_66.rustPlatform;
  };

in
with pkgs;
mkShell rec {
  buildInputs = [
    clang
    llvmPackages.libclang
    git # the shell may be using a different glibc which upsets common CLI tools
    openssl
    pkgconfig
    rustup
    python3
    poetry
    solana-validator-bumped
    stdenv.cc
    stdenv.cc.cc.lib # libstdc++.so.6
  ];

  # Explicitly tell the environment about all *.so's (libstdcxx, libclang end up in the path this way)
  LD_LIBRARY_PATH = lib.makeLibraryPath buildInputs;

  # Tell the Rust build where libclang lives
  LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
}
