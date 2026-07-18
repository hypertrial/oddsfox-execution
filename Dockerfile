FROM rust:1.93.1-bookworm AS build

WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY migrations ./migrations
COPY src ./src
RUN cargo build --locked --release --no-default-features --features paper

FROM debian:bookworm-slim

ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="OddsFox Execution" \
      org.opencontainers.image.description="Paper-only risk-controlled Polymarket intent executor" \
      org.opencontainers.image.source="https://github.com/hypertrial/oddsfox-execution" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="MIT" \
      io.oddsfox.execution-mode="paper-only"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 oddsfox \
    && useradd --system --uid 10001 --gid oddsfox --home /var/lib/oddsfox oddsfox \
    && install -d -o oddsfox -g oddsfox /var/lib/oddsfox /var/lib/oddsfox/backups

COPY --from=build /src/target/release/oddsfox-exec /usr/local/bin/oddsfox-exec

USER 10001:10001
WORKDIR /var/lib/oddsfox
EXPOSE 8787 9090
ENTRYPOINT ["oddsfox-exec"]
CMD ["serve", "--config", "/etc/oddsfox/oddsfox.toml", "--risk-policy", "/etc/oddsfox/risk-policy.json"]
