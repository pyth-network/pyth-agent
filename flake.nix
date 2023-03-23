# Note:
#
# This file provides a Flake for Nix/NixOs users. It allows users to
# build or work with the project without having to worry about which
# dependencies are required. It is not required to build the project
# and can be ignored by users who do not use Nix/NixOs.
#
# See README for instructions on building the project.
#
#
# Usage:
#
# ```bash
# $ nix build                # Build
# $ nix develop              # Instant Dev Environment
# $ nix run . -- <args...>   # Run pyth-agent without installing.
# ```
#
# You can still run `nix-shell` if you prefer to not use flakes.

{
  description                         = "Pyth Agent";
  nixConfig.bash-prompt               = "\[nix@pyth-agent\]$ ";
  inputs.nixpkgs.url                  = github:NixOS/nixpkgs/release-22.11;
  inputs.flake-utils.url              = "github:numtide/flake-utils";
  inputs.fenix.url                    = "github:nix-community/fenix";
  inputs.fenix.inputs.nixpkgs.follows = "nixpkgs";

  outputs =
    { self
    , nixpkgs
    , fenix
    , flake-utils
    }:

    # Generate a Flake Configuration for each supported system.
    flake-utils.lib.eachDefaultSystem (system:
      let
      pkgs  = nixpkgs.legacyPackages.${system};
      shell = import ./shell.nix { inherit pkgs; };
      rust  = pkgs.makeRustPlatform {
        inherit (fenix.packages.${system}.minimal)
          rustc
          cargo;
      };

      in
      {
        devShells.default = shell;
        packages.default  = rust.buildRustPackage {
          pname       = "pyth-agent";
          version     = "0.0.1";
          src         = ./.;
          cargoLock   = { lockFile = ./Cargo.lock; };
        };
      }
    );
}
