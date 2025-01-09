FROM python:3.10-slim-bullseye

# Install Rust
RUN apt update && apt install -y curl pkg-config libssl-dev build-essential
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup toolchain install nightly

# Install poetry
RUN pip install poetry
ENV PATH="${PATH}:/root/.local/bin"
RUN poetry config virtualenvs.in-project true

# Install Solana Tool Suite
RUN sh -c "$(curl -sSfL https://release.solana.com/v1.14.17/install)"
ENV PATH="${PATH}:/root/.local/share/solana/install/active_release/bin"

ADD . /agent
WORKDIR /agent

RUN cargo build --release
