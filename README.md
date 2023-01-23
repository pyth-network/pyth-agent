# Pyth Agent
Publish data to the [Pyth Network](https://pyth.network/).

## Overview
This software runs a JRPC API server, which data providers should use to publish data. Publishing using this intermediate API server provides greater reliability, usability and security than sending transactions directly to an RPC node.

Note that only permissioned publishers can publish data to the network. Please read the [publisher guidelines](https://docs.pyth.network/publish-data) before getting started.

## Build

Prerequisites: Rust 1.68 or higher. A Unix system is recommended.

```bash
# Install OpenSSL
apt install libssl-dev

# Build the project. This will produce a binary at target/release/agent
cargo build --release
```

## Configure
Configuration is managed through a configuration file. An example configuration file with sensible defaults can be found at [config/config.toml](config/config.toml).

The logging level can be configured at runtime through the `RUST_LOG` environment variable, using [env_logger](https://docs.rs/env_logger/latest/env_logger/)'s scheme. For example, to log at `debug` instead of the default `info` level, set `RUST_LOG=debug`.

## Run

### Key Store
If you already have a key store set up, you can skip this step. If you haven't, you will need to create one before publishing data. A key store contains the cryptographic keys needed to publish data. Once you have a key store set up, please ensure that the configuration file mentioned above contains the correct path to your key store.

```bash
# Install the Solana Tool Suite, needed for creating the key used to sign your transactions.
sh -c "$(curl -sSfL https://release.solana.com/v1.14.13/install)"

# Create the key store directory. This can be any location that is convenient for you.
PYTH_KEY_STORE=$HOME/.pythd

# Create your keypair (pair of private/public keys) that will be used to sign your transactions.
# Pyth Network will need to permission this key, so reach out to us once you have created it.
solana-keygen new --no-bip39-passphrase --outfile $PYTH_KEY_STORE/publish_key_pair.json

# Initialize the key store with the public keys of the Pyth Oracle Program on the network you wish to publish to.
PYTH_KEY_ENV=devnet # Can be devnet, testnet or mainnet
./scripts/init_key_store.sh $PYTH_KEY_ENV $PYTH_KEY_STORE
```

### API Server
```bash
# Run the agent binary, which will start a JRPC websocket API server.
./target/release/agent --config config/config.toml
```

### Publish Data
You can now publish data to the Pyth Network using the JRPC websocket API documented [here](https://docs.pyth.network/publish-data/pyth-client-websocket-api).
