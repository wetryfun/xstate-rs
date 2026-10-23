# The xstate-kafka service. Build context: the repository root
# (docker compose does this; by hand:
#   docker build -f crates/xstate-kafka/deploy/Dockerfile .).

FROM rust:1.97.0-trixie AS build
WORKDIR /src
# librdkafka, OpenSSL, zlib and zstd are built from source by the -sys
# crates: the full Debian image has the C toolchain, perl and make.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p xstate-kafka --features kafka --bin xstate-kafka \
    && cp target/release/xstate-kafka /usr/local/bin/xstate-kafka

FROM debian:trixie-slim
COPY --from=build /usr/local/bin/xstate-kafka /usr/local/bin/xstate-kafka
RUN useradd --system --home-dir /var/lib/xstate --create-home xstate
USER xstate
# Prometheus metrics (metricsAddress in the config).
EXPOSE 9464
ENTRYPOINT ["xstate-kafka"]
CMD ["/etc/xstate/service.json"]
