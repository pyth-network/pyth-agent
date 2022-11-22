FROM rust:1.64.0-slim-bullseye

ADD . /agent
WORKDIR /agent

RUN apt update && apt install -y pkg-config libssl-dev
RUN cargo build --release
