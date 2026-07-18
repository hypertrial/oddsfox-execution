# Public release audit

Audit date: 2026-07-18

This gate covers the transition from the private `hypertrial/oddsfox-live`
repository to public `hypertrial/oddsfox-execution`.

## Scope and evidence

- Audited every reachable commit on every branch and tag. The source repository
  contains one branch (`main`), one tag (`v0.1.0`), and twelve reachable
  commits at the audit baseline (`4285adda`).
- Inspected unreachable local Git objects. The only unreachable blob was a
  draft product specification and contained no credential or confidential
  data. Unreachable local objects are not part of a clone or the published
  repository.
- Searched reachable history and the working tree with Gitleaks 8.30.1 plus
  repository-specific credential patterns. The only match was the literal
  `cancel-operation-0001`, a non-secret idempotency key in a cancellation unit
  test. The exact literal is narrowly allowlisted in `.gitleaks.toml`.
- Found no Git LFS pointers in reachable history, so there are no referenced
  LFS objects requiring separate publication review.
- Reviewed historical deployment files, URLs, fixtures, SQLite exclusions,
  container context exclusions, licenses, and third-party notices.

## Dependency and license gate

- `cargo deny --locked check` passes advisories, bans, licenses, and sources.
- `cargo audit` reports `RUSTSEC-2023-0071` for the `rsa` crate retained in the
  lockfile through optional SQLx macro metadata. The repository gate proves
  that crate is absent from every compiled `oddsfox-execution` dependency graph
  before applying the documented advisory exception.
- Allowed unmaintained transitive crates are reported by the audit and must be
  reconsidered on dependency upgrades.
- Project code is MIT licensed; allowed dependency licenses and notices are
  recorded in `deny.toml` and `THIRD_PARTY_NOTICES.md`.

## Release controls

- The Dockerfile hardcodes `--no-default-features --features paper`; no build
  argument can add `live` or `aws-kms`.
- CI runs all-feature source tests but publishes only the hardcoded paper
  binary. It also executes the built image with a live request and live
  acknowledgement to prove startup is rejected.
- The initial risk policy admits only FAK and GTD.
- Published `linux/amd64` and `linux/arm64` images include SBOM and provenance,
  receive a GitHub build attestation, and are signed keylessly with GitHub
  OIDC.

Public visibility is permitted only after `scripts/full_history_audit.sh`, the
Rust job, the paper-only container assertion, and the paper smoke test all
pass at the release commit.
