# syntax=docker/dockerfile:1

FROM rust:1.93.1-bookworm@sha256:7c4ae649a84014c467d79319bbf17ce2632ae8b8be123ac2fb2ea5be46823f31 AS source

WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY migrations ./migrations
COPY src ./src
RUN test "$(dpkg --print-architecture)" = "amd64"

FROM source AS paper-build
RUN cargo build --locked --release

FROM source AS live-build
RUN cargo build --locked --release --features live

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS runtime-base

ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="OddsFox Execution" \
      org.opencontainers.image.source="https://github.com/hypertrial/oddsfox-execution" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="MIT"

RUN test "$(dpkg --print-architecture)" = "amd64" \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 oddsfox \
    && useradd --system --uid 10001 --gid oddsfox --home /var/lib/oddsfox oddsfox \
    && install -d -o oddsfox -g oddsfox /var/lib/oddsfox /var/lib/oddsfox/backups \
    && install -d /usr/share/licenses/oddsfox-execution

COPY LICENSE THIRD_PARTY_NOTICES.md /usr/share/licenses/oddsfox-execution/

USER 10001:10001
WORKDIR /var/lib/oddsfox
EXPOSE 8787 9090
ENTRYPOINT ["oddsfox-exec"]
CMD ["serve", "--config", "/etc/oddsfox/oddsfox.toml", "--risk-policy", "/etc/oddsfox/risk-policy.json"]

FROM runtime-base AS live-local
LABEL org.opencontainers.image.description="Locally signed risk-controlled Polymarket intent executor" \
      io.oddsfox.execution-mode="live-local"
COPY --from=live-build /src/target/release/oddsfox-exec /usr/local/bin/oddsfox-exec

FROM runtime-base AS paper
LABEL org.opencontainers.image.description="Paper-only risk-controlled Polymarket intent executor" \
      io.oddsfox.execution-mode="paper-only"
COPY --from=paper-build /src/target/release/oddsfox-exec /usr/local/bin/oddsfox-exec
