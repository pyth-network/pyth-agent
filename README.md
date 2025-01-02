# Pyth Agent
Publish data to the [Pyth Network](https://pyth.network/).

## Overview
This software runs a JRPC API server, which data providers should use to publish data. Publishing using this intermediate API server provides greater reliability, usability and security than sending transactions directly to an RPC node.

Note that only permissioned publishers can publish data to the network. Please read the [publisher guidelines](https://docs.pyth.network/documentation/publish-data) before getting started.

## Build

Prerequisites: Rust 1.68 or higher. A Unix system is recommended.

```shell
# Install dependencies (Debian-based systems)
$ apt install libssl-dev build-essential

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
$ rustup default 1.68 # Optional

# Build the project. This will produce a binary at target/release/agent
$ cargo build --release
```

## Configure
The agent takes a single `--config` CLI option, pointing at
`config/config.toml` by default. An example configuration is provided
there, containing a minimal set of mandatory options and documentation
comments for optional settings. **The config file must exist.**

### Logging
The logging level can be configured at runtime
through the `RUST_LOG` environment variable using the standard
`error|warn|info|debug|trace`.

#### Plain/JSON logging
Pyth agent will print logs in plaintext in terminal and JSON format in non-terminal environments (e.g. when writing to a file).

## Run
### From Source
The preferred way to run Pyth Agent is by compiling from source. You can run the below command to build and run the agent in a single step.

```bash
cargo run --release -- --config <your_config.toml>
```

### Container
For convenience, a minimal container image is also published to [ECR Public](https://gallery.ecr.aws/pyth-network/agent). An example command for running this container can be found below. Make sure to update the image version to the latest release of Pyth Agent.

```bash
docker run -v /path/to/configdir:/config:z,ro public.ecr.aws/pyth-network/agent:v2.12.0-minimal
```

## Publishing API
A running agent will expose a WebSocket serving the JRPC publishing API documented [here](https://docs.pyth.network/documentation/publish-data/pyth-client-websocket-api). See `config/config.toml` for related settings.

## Best practices
If your publisher is publishing updates to more than 50 price feeds, it is recommended that you do the following to reduce the connection overhead to the agent:
- Batch your messages together and send them as a single request to the agent (as an array of messages). The agent will respond to the batch messages
  with a single response containing an array of individual responses (in the same order). If batching is not possible, you can disable the `instant_flush` option
  in the configuration file to let agent send the responses every `flush_interval` seconds.
- Do not use subscribe to the price schedule. Instead, define a schedule on the client side and send the messages based on your own schedule. Ideally
  you should send price updates as soon as you have them to increase the latency of the data on the Pyth Network.

# Development
## Unit Testing
A collection of Rust unit tests is provided, ran with `cargo test`.

## Integration Testing
In `integration-tests`, we provide end-to-end tests for the Pyth
`agent` binary against a running `solana-test-validator` with Pyth
oracle deployed to it. Optionally, accumulator message buffer program
can be deployed and used to validate accumulator CPI correctness
end-to-end (see configuration options below). Prebuilt binaries are
provided manually in `integration-tests/program-binaries` - see below
for more context.

### Running Integration Tests
The tests are implemented as a Python package containing a `pytest`
test suite, managed with [Poetry](https://python-poetry.org/) under
Python >3.10. Use following commands to install and run them:

```bash
cd integration-tests/
poetry install
poetry run pytest -s --log-cli-level=debug
```

### Optional Integration Test Configuration
* `USE_ACCUMULATOR`, off by default - when this env is set, the test
  framework also deploys the accumulator program
  (`message_buffer.so`), initializes it and configures the agent to
  make accumulator-enabled calls into the oracle
* `SOLANA_TEST_VALIDATOR`, systemwide `solana-test-validator` by
  default - when this env is set, the specified binary is used as the
  test validator. This is especially useful with `USE_ACCUMULATOR`,
  enabling life-like accumulator output from the `pythnet` validator.

### Testing Setup Overview
For each test's setup in `integration-tests/tests/test_integration.py`, we:
* Start `solana-test-validator` with prebuilt Solana programs deployed
* Generate and fund test Solana keypairs
* Initialize the oracle program - allocate test price feeds, assign
  publishing permissions. This is done using the dedicated [`program-admin`](https://github.com/pyth-network/program-admin) Python package.
* (Optionally) Initialize accumulator message buffer program
  initialize test authority, preallocate message buffers, assign
  allowed program permissions to the oracle - this is done using a
  generated client package in
  `integration-tests/message_buffer_client_codegen`, created using
  [AnchorPy](https://github.com/kevinheavey/anchorpy).
* Build and run the agent

This is followed by a specific test scenario,
e.g. `test_update_price_simple` - a couple publishing attempts with
assertions of expected on-chain state.

### Prebuilt Artifact Safety
In `integration-tests/program-binaries` we store oracle and
accumulator `*.so`s as well as accumulator program's Anchor IDL JSON
file. These artifacts are guarded against unexpected updates with a
commit hook verifying `md5sum --check canary.md5sum`. Changes to the
`integration-tests/message_buffer_client_codegen` package are much
harder to miss in review and tracked manually.

### Updating Artifacts
While you are free to experiment with the contents of
`program-binaries`, commits for new or changed artifacts must include
updated checksums in `canary.md5sum`. This can be done
by running `md5sum` in repository root:
```shell
$ md5sum integration-tests/program-binaries/*.json > canary.md5sum
$ md5sum integration-tests/program-binaries/*.so >> canary.md5sum # NOTE: Mind the ">>" for appending
```

### Updating `message_buffer_client_codegen`
After obtaining an updated `message_buffer.so` and `message_buffer_idl.json`, run:
```shell
$ cd integration-tests/
$ poetry install # If you haven't run this already
$ poetry run anchorpy client-gen --pdas program-binaries/message_buffer_idl.json message_buffer_client_codegen
```
