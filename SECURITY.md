# Security policy

## Reporting

Do not open a public issue for a suspected vulnerability involving signing,
authentication, order duplication, risk bypass, secrets, or fund safety.
Contact the repository owner privately with reproduction details and affected
revision.

## Secrets

- Persist only SHA-256 hashes of high-entropy API bearer tokens.
- Store local wallet keys in an owner-readable mounted file (`0600` or
  stricter). Live configuration does not accept environment private keys.
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
OpenAPI drift checks, container build, and paper smoke tests. Release images
are multi-architecture, SBOM- and provenance-bearing, and keylessly signed.
The Polymarket SDK is exactly pinned and upgrades require contract/conformance
review.

The official image is structurally paper-only: the Dockerfile hardcodes the
`paper` Cargo feature and CI proves that a live-mode command fails even when
the runtime acknowledgement variable is present.

## Supported security posture

The service is designed for one operator-controlled account and host. It is not
a custody platform, multi-tenant service, public API, or active-active system.
Production use requires host hardening, encrypted storage/backups, restricted
egress, audit retention, alerting, and independent security review.
