FROM rust:slim-bookworm as builder

RUN apt update && apt install -y curl libssl-dev pkg-config build-essential && apt clean all

ADD . /agent
WORKDIR /agent

RUN cargo build --release

FROM debian:12-slim

RUN apt update && apt install -y libssl-dev ca-certificates && apt clean all

COPY --from=builder /agent/target/release/agent /agent/
COPY --from=builder /agent/config/* /agent/config/

ENTRYPOINT ["/agent/agent"]
