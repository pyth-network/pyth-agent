FROM rust:1.61.0-slim-bullseye

ADD . /agent
WORKDIR /agent

RUN cargo build --release
