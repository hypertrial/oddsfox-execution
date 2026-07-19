# Security policy

## Reporting

Do not open a public issue for a suspected vulnerability involving signing,
authentication, order duplication, risk bypass, secrets, or fund safety.
Contact the repository owner privately with reproduction details and affected
revision.

## Secrets

- Persist only SHA-256 hashes of high-entropy API bearer tokens.
- Pass API clients a token-file path; token values are never accepted in
  command arguments or environment variables.
- Store a local wallet key in a regular, non-symlink mounted file with exactly
  `0400` or `0600` permissions. The loader supports Unix permission semantics;
  the supported deployment runs it only inside a Linux container. Live
  configuration does not accept environment private keys.
- CLOB API credentials and passphrases are derived in memory and must not be
  logged, returned, or stored.
- Do not bake configuration, keys, database files, `.env`, or credentials into
  container layers.
- Treat panic output, tracing fields, metrics labels, backup manifests, and
  support bundles as disclosure surfaces.

## Network

The control API and metrics bind to loopback by default. A non-loopback live
control bind requires explicit acknowledgement and must be protected by an
operator-owned private network or authenticated TLS-terminating proxy. CORS is
not enabled. Never expose the metrics listener publicly.

## Supply chain

`Cargo.lock` is committed. CI runs formatting, all-feature lint/test,
full-history and working-tree secret scanning, `cargo audit`, `cargo deny`,
OpenAPI drift checks, Windows paper/live-gate tests, both container builds, and
paper smoke tests. CI does not publish container images. The Polymarket SDK is
exactly pinned and upgrades require contract/conformance review. Images carry
the project license and third-party notice in
`/usr/share/licenses/oddsfox-execution/`.

The Dockerfile's final/default `paper` target is structurally paper-only. The
separate `live-local` target compiles the local-file signer. CI checks each
target's declared capabilities and proves that a live-mode command fails in
the paper image even when the runtime acknowledgement variable is present.

## Supported security posture

The service is designed for one operator-controlled account and host. It is not
a custody platform, multi-tenant service, public API, or active-active system.
Production use requires host hardening, encrypted storage/backups, restricted
egress, audit retention, alerting, and independent security review.
