# Pyth Agent End-To-End Integration Tests

This project provides end-to-end tests for the [Pyth `agent`](../) binary. Each test:
- Starts an instance of [`solana-test-validator`](https://docs.solana.com/developing/test-validator).
- Deploys the [Pyth Oracle program](https://github.com/pyth-network/pyth-client/tree/main/program). Note that this deploys a [pre-compiled binary](../integration-tests/oracle.so), which should be updated when the Oracle program significantly changes.
- Creates [Mapping; Product and Price accounts](https://docs.pyth.network/design-overview/account-structure) using the [`program-admin`](https://github.com/pyth-network/program-admin) library.
- Builds and runs an instance of the Pyth `agent`, configured with the accounts set up in the previous step.
- Uses the [JRPC Websocket API](https://docs.pyth.network/publish-data/pyth-client-websocket-api) to test publishing to and retrieving data from the network.

## Installing
The test framework uses [Poetry](https://python-poetry.org/) and requires Python >3.10. To install the project:

```bash
poetry install
```

## Running Tests
```bash
pytest -s --log-cli-level=debug
```
