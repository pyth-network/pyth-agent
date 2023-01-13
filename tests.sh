#!/bin/sh -ev

# Run Rust unit tests
cargo test --workspace

# Run Python integration tests
cd integration-tests
poetry install
poetry run pytest -s --log-cli-level=debug
