# Operator runbook

This runbook does not authorize live trading. A funded canary requires a
separate explicit decision, dedicated wallet, allowlisted market, and reviewed
risk policy.

## Start paper mode

1. Create a high-entropy strategy token and operator token.
2. Store only their SHA-256 digests in `config/oddsfox.toml`.
3. Review the risk policy and give it a unique immutable version.
4. Run `oddsfox-exec doctor --config config/oddsfox.toml`.
5. Start `oddsfox-exec serve ...`.
6. Require `/health/ready` to return 200 before sending work.

The database lock is exclusive. Do not copy or inspect an active SQLite file
directly.

## Halt

```bash
oddsfox-exec halt \
  --token "$ODDSFOX_API_TOKEN" \
  --idempotency-key "operator-halt-20260718-0001" \
  --reason "operator initiated"
```

Halt is durable. If `cancel_on_halt` is enabled, the coordinator journals an
all-open cancellation and continues reconciliation. Verify orders and venue
state; do not infer cancellation from a disconnected stream.

## Resume

Resolve the root cause first. Resume runs a reconciliation and heartbeat and
fails if a critical finding is unresolved.

```bash
oddsfox-exec resume \
  --token "$ODDSFOX_API_TOKEN" \
  --idempotency-key "operator-resume-20260718-0001" \
  --reason "root cause resolved and venue state reviewed"
```

Never resume merely to make readiness green.

## Graceful shutdown and restart

Send `SIGTERM` or `SIGINT` and wait for the process to exit. The service stops
admitting new risk, performs configured protective cancellation, reconciles,
persists a durable `HALTED` completion marker, and checkpoints SQLite. A
successful restart remains not-ready until an operator reviews state and uses
the resume API. If shutdown is interrupted before the completion marker,
startup converts the persisted `SHUTTING_DOWN` state to a latched halt and
requires the same review.

Treat a non-zero exit as unsafe even if the process is no longer running.
Inspect the journal and venue before resume; do not delete the lock, WAL, or
database to make startup succeed.

## Unknown submission

1. Keep the service halted.
2. Record the intent, local order, prepared-order hash, and attempt.
3. Run explicit reconciliation.
4. Confirm positive venue evidence for the deterministic order ID.
5. If no positive evidence exists, leave the order `UNKNOWN`; v1 has no
   “assume not submitted” override.
6. Escalate before changing code or database state.

## Heartbeat or WebSocket failure

Heartbeat failure latches halt, marks readiness false, assumes venue orders may
have been cancelled, requests configured protective cancellation, and performs
full reconciliation. A reconnect is not sufficient: REST convergence and a
successful heartbeat are required before resume.

## Matching-engine restart or protocol mismatch

HTTP 425 or an order-version mismatch halts opening submissions. Cancellation
and reads remain available. Verify `/version` reports V2 and wait for venue
health. Do not rebuild or re-sign an ambiguous attempt. During a venue
post-only restart window, only a separately reviewed valid post-only GTC/GTD
order may be attempted after the previous request is definitively rejected.

## Backup

```bash
oddsfox-exec backup \
  --token "$ODDSFOX_API_TOKEN" \
  --idempotency-key "backup-20260718-0001"
```

The CLI calls `POST /v1/backups`; it never opens the active database. Query the
returned backup ID until it is complete, then copy both `.sqlite3` and
`.manifest.json`.

Verify offline:

```bash
oddsfox-exec doctor \
  --backup data/backups/<backup-id>.manifest.json
```

## Restore

1. Halt and stop the service.
2. Preserve the failed database, WAL, SHM, lock metadata, and logs.
3. Run offline backup verification.
4. Restore to a new path; never downgrade an execution database in place.
5. Configure the exact stored mode/account identity.
6. Start with no strategy traffic.
7. Require startup reconciliation and inspect every finding.
8. Resume only through the control API.

## Key rotation and KMS outage

Signer/funder identity is database-bound. A signer change requires a new
database and controlled cutover after cancelling and reconciling the prior
instance. During a KMS outage, halt and retain ambiguous reservations; never
fall back to an unconfigured local or environment key.

## Unsafe database condition

Disk-full, corruption, failed `synchronous=FULL` write, or checkpoint failure
is unsafe. Stop accepting risk, preserve evidence, restore a verified backup,
and reconcile against the venue before operation. Do not repair projections
with ad hoc SQL.

## SDK upgrade

Keep `polymarket_client_sdk_v2` exactly pinned. An upgrade requires:

- recorded request/response and WebSocket fixture review;
- signed-order serialization/reconstruction tests;
- V2 order-hash comparison;
- read-only production conformance;
- license/dependency review; and
- a new paper soak before live enablement.
