FROM rust:1.61.0-slim-bullseye

ADD . /agent
WORKDIR /agent

RUN apt-get update && apt-get install pkg-config
RUN cargo build --release
