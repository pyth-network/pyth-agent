FROM rust:1.58.1-slim-bullseye

ADD . /agent
WORKDIR /agent

RUN cargo build --release
