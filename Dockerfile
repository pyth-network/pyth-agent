FROM python:3.10-bullseye

# Install Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup toolchain install nightly

# Install poetry
RUN pip install poetry
ENV PATH="${PATH}:/{$HOME}/.local/bin"
RUN poetry config virtualenvs.in-project true

# Install Solana Tool Suite
RUN sh -c "$(curl -sSfL https://release.solana.com/v1.14.11/install)"

ADD . /agent
WORKDIR /agent

RUN apt update && apt install -y pkg-config libssl-dev
RUN cargo build --release
