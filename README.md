# OddsFox Execution

`oddsfox-execution` is a self-hosted, risk-controlled execution service for
prediction markets. It accepts explicit order intents from separate strategy
systems and starts with Polymarket's V2 CLOB.

This repository is execution infrastructure only. It does not generate
signals, discover markets, ingest research data, manage deposits, create
wallets, or expose the retired OddsFox dashboard API.

## Safety model

- Paper mode is the default and uses a database that can never be reopened as
  live.
- Every admitted request is canonicalized, hashed, and durably journaled.
- Intent processing is serialized so concurrent requests cannot oversubscribe
  the same risk limits.
- A signed live order is persisted before the single network submission.
- A crash or transport failure after `SUBMITTING` becomes `UNKNOWN`, latches
  halt, retains worst-case exposure, and requires positive venue evidence.
- Live mode requires all three gates: the `live` Cargo feature,
  `mode = "live"`, and `ODDSFOX_ENABLE_LIVE_TRADING=YES`.
- The HTTP listener defaults to loopback, CORS is not enabled, and Prometheus
  metrics use a separate listener.
- The official container is compiled with the `paper` feature only. Its
  Dockerfile has no build argument capable of selecting `live` or `aws-kms`,
  and CI proves that `--mode live` is rejected even when the live environment
  acknowledgement is set.

No live order or real-capital action is authorized merely by building or
running this repository.

## Build and test

Rust `1.93.1` is pinned in `rust-toolchain.toml`.

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

Build the supported paper binary:

```bash
cargo build --locked --release
```

The source retains separately gated `live` and `aws-kms` features for future
security review and conformance testing. They are unsupported for deployment
in this release and are never selected by the official Dockerfile or release
workflow.

## Configure and run paper mode

```bash
cp config/oddsfox.toml.example config/oddsfox.toml
cp config/risk-policy.example.json config/risk-policy.json
cargo run --locked -- serve \
  --config config/oddsfox.toml \
  --risk-policy config/risk-policy.json
```

Generate a bearer-token digest without storing the token in configuration:

```bash
printf '%s' "$ODDSFOX_NEW_TOKEN" | oddsfox-exec token-digest
```

Replace the example `token_sha256`, keep the original token in the calling
client's secret store, and send it as `Authorization: Bearer ...`.

## Interfaces

- Control API: `http://127.0.0.1:8787`
- Metrics: `http://127.0.0.1:9090/metrics`
- Liveness/readiness: `/health/live`, `/health/ready`
- Durable API and resumable SSE: `/v1/*`
- Operator CLI: `oddsfox-exec submit|cancel|halt|resume|reconcile|backup|doctor`

All mutations require a caller-controlled `Idempotency-Key`. CLI mutations
accept `--idempotency-key`; when omitted, the CLI creates one for a single
attempt.

See:

- [Product specification](docs/product-spec.md)
- [Architecture](docs/architecture.md)
- [Operator runbook](docs/operator-runbook.md)
- [Security policy](SECURITY.md)
- [OpenAPI contract](openapi/oddsfox-execution-v1.json)

## Live mode

Live deployment is intentionally unavailable in the first release. The
official image cannot load a signer or enter live mode, regardless of runtime
environment variables. The source-level implementation remains behind Cargo
features for future review; it is not an authorized or supported deployment
artifact.

Before any funded deployment, complete the paper soak, restore rehearsal,
read-only venue conformance, independent security review, and explicitly
authorized canary described in the product specification.

## License and compliance

Original code is MIT licensed. The pinned official Polymarket Rust SDK is also
MIT licensed, so it does not prevent this infrastructure from being licensed
under MIT. MIT licensing does not grant access to Polymarket, waive its terms,
or make trading lawful in a particular jurisdiction. Operators are responsible
for current venue terms, geographic eligibility, financial regulation, tax,
wallet custody, and security.

OddsFox is independent and is not affiliated with or endorsed by Polymarket.
The executor refuses new orders when geographic eligibility is blocked or
indeterminate and contains no bypass-routing feature.

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) and
[COMPLIANCE.md](COMPLIANCE.md).
