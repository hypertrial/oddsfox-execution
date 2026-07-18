# Architecture

## Trust boundary

Strategy repositories may identify a condition/token, side, quantity, price
protection, time in force, and cancellation target. They never receive venue
credentials and never submit venue-native signed orders.

```text
strategy client
    |
    | bearer auth + idempotent /v1 intent
    v
control API -> canonical intent journal -> validation -> risk coordinator
                                                        |
                                      +-----------------+-----------------+
                                      |                                   |
                                paper venue                       Polymarket V2
                                                                    REST + WS
                                      |                                   |
                                      +---------- reconciliation ----------+
                                                        |
                                               SQLite audit/projections
```

The executor runs one account and one mode per process. An adjacent advisory
lock excludes a second process from the database. Instance metadata binds a
database permanently to paper/live mode, Polygon chain ID, V2 protocol,
signer, and funder.

## Submission boundary

The sequence is intentionally fixed:

1. Commit the canonical intent and event.
2. Verify condition/token metadata and obtain a fresh book.
3. Evaluate risk and commit its policy-versioned capacity reservation in one
   serialized transaction.
4. Construct and sign exactly one venue order while the durable reservation
   prevents later intents from consuming the same capacity.
5. Commit the reconstructable signed payload, canonical bytes hash, signer,
   funder, protocol, SDK version, policy version, and attempt.
6. Atomically transition the intent, order, and attempt to `SUBMITTING`.
7. Send that persisted order exactly once.
8. Atomically commit the response, fill projection, or `UNKNOWN`.

A process interruption after step 6 is ambiguous even if the process cannot
prove that bytes left the host. Recovery converts the affected projections to
`UNKNOWN`, preserves their maximum reservation, latches halt, and queries the
venue by the V2 EIP-712 order identifier. It never signs a replacement while
the prior attempt is unresolved.

The pinned SDK may perform read-only trade lookups for up to 30 seconds after
a successful immediate-order POST to resolve transaction hashes. The
submission deadline allows that documented polling window; it still sends the
persisted order only once, and a deadline before the response commit remains
ambiguous.

## Persistence

SQLite uses WAL, foreign keys, `synchronous=FULL`, a bounded busy timeout, a
small read pool, and a process-wide serialized write gate. Projection changes
and their audit events share one transaction at safety-critical boundaries.
Risk reservations are durable before signing, are consumed atomically when the
prepared order is stored, and are released if preparation fails or pre-signing
work is recovered after a restart. All trading decimals are exact
`rust_decimal` values and remain strings at public and storage boundaries.
For quote-denominated immediate buys, the reservation keeps the full quote
budget plus maximum fees and separately reserves the maximum share count at
the market's minimum tick; the order projection retains the caller's price
protection value.

Every mutating API operation records its canonical request hash and original
`202` response in the same transaction as its intent, cancellation,
reconciliation, backup, or control record. Interrupted cancellations resume at
startup; interrupted reconciliation and backup resources become durably
`FAILED` before startup reconciliation.

Online backups are requested through the running service. The writer is
quiesced, SQLite `VACUUM INTO` writes a partial file, `PRAGMA integrity_check`
runs before publication, and the database plus manifest are renamed into
place. `doctor --backup` checks the checksum, SQLite integrity, schema, mode,
and event sequence without opening the active database.

## Readiness and halt

`HALTED` is durable and latched. New risk is rejected in the same serialized
transaction that checks the control state while the service is not `READY`;
an exact replay of a previously admitted idempotency key still returns its
original response. Reads, cancellations, backup, and reconciliation remain
available. Halt and cancellation do not wait behind a resume check, and a
monotonic control-state revision prevents a newer halt from being overwritten
by a resume that started earlier. The
`SUBMITTING` transition rechecks `READY` transactionally: if halt commits
first, no request is sent; if submission commits first, protective
cancellation waits for that order lifecycle to produce a cancellable result.
Resume performs reconciliation, checks the supervised heartbeat, and refuses
while a critical finding remains unresolved.

Live readiness additionally requires the compile-time `live` feature,
configuration mode, explicit environment gate, signer/funder match, protocol
V2, geographic eligibility, authenticated streams, heartbeat, and a clean
startup reconciliation. Production V2 uses `https://clob.polymarket.com`; the
former `clob-v2.polymarket.com` pre-cutover host is not a production target.

## Deliberate v1 limits

- One active process, account, funder, host, and SQLite database.
- HTTP plus resumable SSE; no broker, gRPC, PostgreSQL, or HA.
- Polymarket only; venue SDK types remain inside the adapter.
- `POLY_1271` only; wallet provisioning and on-chain allowance changes are
  external.
- No builder fee and no geographic bypass behavior.
- No automatic position-closing exception while halted.
