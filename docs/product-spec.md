# oddsfox-execution Product Specification

Status: Draft for implementation
Product: `oddsfox-execution`
Initial venue: Polymarket CLOB
Implementation language: Rust
License: MIT
Last updated: 2026-07-18

## 1. Executive summary

`oddsfox-execution` is a self-hosted service that receives explicit trade
intents from strategy systems and executes them safely on prediction markets.
It starts with Polymarket and is designed around four guarantees:

1. The same intent is never submitted twice accidentally.
2. An order is never submitted unless it passes current market, balance, and
   operator-defined risk checks.
3. Every accepted intent and venue-side outcome is durably auditable.
4. When the service cannot prove what happened, it stops opening risk and
   reconciles instead of guessing.

The service is execution infrastructure only. Strategies, forecasts, market
discovery, historical data, feature generation, optimization, and research
remain in other repositories.

This product replaces the current `oddsfox-live` read-only dashboard backend.
The existing dashboard, graph-artifact, sports, and replay APIs are retired
rather than carried into the new service.

## 2. Context

The current repository:

- consumes public Polymarket market data;
- merges that data with OddsFox graph and World Cup artifacts;
- serves dashboard-oriented JSON and SSE endpoints; and
- cannot authenticate, sign, submit, cancel, or reconcile orders.

That architecture is useful for visualization but is the wrong trust boundary
for trade execution. A production executor needs durable intent handling,
strict numeric semantics, isolated credentials, explicit risk policy,
idempotency, recovery after ambiguous network failures, and continuous
venue reconciliation.

## 3. Product definition

### 3.1 Mission

Provide a small, reliable, venue-aware control plane that turns an explicit
order intent into a safely managed prediction-market order.

### 3.2 Primary user

The initial user is a technical operator running one funded Polymarket account
and one or more separate strategy services.

The operator:

- owns the wallet and venue account;
- chooses whether the executor is in paper or live mode;
- defines the risk policy;
- supplies and rotates credentials;
- monitors health and reconciliation; and
- can halt, cancel, reconcile, and resume execution.

### 3.3 Upstream clients

Upstream clients are trusted strategy or orchestration services. They decide:

- what market and outcome token to trade;
- buy or sell;
- order size;
- price protection;
- time in force; and
- when to cancel.

They do not receive wallet credentials and do not construct or sign
venue-native orders.

### 3.4 Product principles

- **Safety before availability.** Reject or halt when required state is stale,
  inconsistent, or unknown.
- **Durability before side effects.** Persist intent and signed order material
  before making a venue request.
- **Reconciliation over inference.** Venue state is authoritative for orders
  and trades; the local journal explains how the service reached that state.
- **Explicit numeric semantics.** Monetary values and quantities use decimal
  strings at boundaries and fixed-precision decimal types internally. Binary
  floating point is prohibited for trading calculations.
- **One mode per process.** Paper and live execution never share a process,
  database, or request-level switch.
- **Narrow venue boundary.** Polymarket is the only v1 venue. Venue-specific
  types do not leak into the public API or core domain.
- **No silent behavior.** Rejections, degraded state, reconciliation changes,
  and operator actions produce durable events.

## 4. Goals and non-goals

### 4.1 Goals for v1

- Accept authenticated, idempotent order intents over a private HTTP API.
- Validate every intent against market metadata, order book freshness, account
  state, geographic availability, and a local risk policy.
- Sign, submit, observe, cancel, and reconcile Polymarket orders.
- Support GTC, GTD, FOK, and FAK orders, including post-only where supported.
- Track partial fills and Polymarket trade finality.
- Provide a conservative paper-execution mode using live market data.
- Recover safely from process crashes, WebSocket gaps, venue errors, and
  ambiguous HTTP outcomes.
- Expose an operator CLI, structured logs, metrics, health endpoints, and a
  resumable event stream.
- Run as a single non-root Linux container with SQLite-backed local durability.
- Keep original project code MIT licensed and document third-party licenses.

### 4.2 Non-goals for v1

- Forecasting, alpha generation, or strategy logic.
- Historical data ingestion, feature computation, or research notebooks.
- General market browsing or discovery beyond validating supplied identifiers.
- Portfolio construction or capital allocation across strategies.
- A browser UI or public internet API.
- Multi-user custody, role-based account administration, or SaaS tenancy.
- Deposits, withdrawals, bridging, wrapping, splitting, merging, or redeeming
  conditional tokens.
- Automated allowance transactions.
- Smart order routing or simultaneous multi-venue execution.
- Active-active high availability.
- Copy trading, social features, or builder-fee monetization.
- Compatibility with the old `/api/v0` dashboard API.

## 5. Initial operating model

v1 runs one process per:

- environment (`paper` or `live`);
- Polymarket account/funder; and
- SQLite database.

Only one process may own a database at a time. Startup fails if another writer
holds the instance lock.

The default bind address is loopback. Remote access requires an operator-owned
private network or authenticated TLS reverse proxy. The service does not enable
CORS.

Paper mode is the default. Live mode requires an explicit startup flag, live
credentials, a complete risk policy, a successful geographic-availability
check, and a clean startup reconciliation.

## 6. Core user journeys

### 6.1 Submit an order intent

1. A strategy sends an authenticated request with an `Idempotency-Key`.
2. The service durably records the canonical request and request hash.
3. The service validates the request and current operating state.
4. The risk engine approves or rejects the intent with machine-readable reasons.
5. In paper mode, the simulator processes the approved intent.
6. In live mode, the service builds and signs the venue order, persists the
   exact signed payload and deterministic order identifier, then submits it.
7. The service returns the durable intent representation. Subsequent state
   changes are available through reads and the event stream.

### 6.2 Retry a request safely

1. A strategy repeats a request with the same `Idempotency-Key`.
2. If the canonical body matches, the service returns the original intent and
   does not repeat any side effect.
3. If the body differs, the service returns `409 IDEMPOTENCY_CONFLICT`.

### 6.3 Cancel risk

The operator or strategy submits an idempotent cancellation targeting one
order, one intent, one market, or all open orders. The service records the
request before contacting the venue and keeps reconciling until each target
is terminal or explicitly `UNKNOWN`.

### 6.4 Halt and recover

1. A critical condition latches the service into `HALTED`.
2. New opening intents are rejected.
3. Configured protective cancellation runs.
4. The service continues observing venue state and permits reconciliation and
   risk-reducing actions.
5. The operator resolves the cause, runs reconciliation, and explicitly resumes.

### 6.5 Restart after failure

1. The service verifies database integrity and reads the latched control state.
2. It does not submit pending work automatically.
3. It checks geographic eligibility, venue health, credentials, balances,
   allowances, open orders, and recent trades.
4. It resolves or isolates locally ambiguous orders.
5. It starts market and user streams.
6. It becomes ready only after reconciliation succeeds.

## 7. Functional requirements

### 7.1 Intent admission

**FR-001** The service must require bearer authentication for every endpoint
except minimal liveness/readiness responses and Prometheus metrics when those
endpoints are bound to a private listener. Unauthenticated health responses
must not expose account, market, position, or failure details.

**FR-002** Every mutating request must include an `Idempotency-Key` of 16 to
128 printable ASCII characters.

**FR-003** The service must persist the canonical request body, its hash,
caller identity, receive time, and idempotency key before validation.

**FR-004** Reuse of a key with an identical body must return the original
resource. Reuse with a different body must return HTTP 409.

**FR-005** The service must enforce bounded body size, field lengths, decimal
precision, and request deadlines before processing.

### 7.2 Market validation

**FR-010** Every intent must identify both a Polymarket condition ID and outcome
token ID. The executor must verify their relationship.

**FR-011** Before approval, the service must verify that the market:

- exists;
- is active and accepting orders;
- has not resolved or closed;
- exposes the expected tick size and minimum order size;
- has the expected negative-risk configuration; and
- is allowed by local policy.

**FR-012** Market metadata must be refreshed on a bounded TTL. Submission must
fail closed when required metadata is unavailable or older than policy permits.

**FR-013** Price and size must be exact multiples of the venue tick and size
rules. The service must reject rather than silently round.

### 7.3 Order support

**FR-020** v1 must support:

| Time in force | Behavior | Quantity unit |
| --- | --- | --- |
| `GTC` | Rest until filled or cancelled | Shares |
| `GTD` | Rest until expiration, fill, or cancellation | Shares |
| `FOK` buy | Fill quote amount completely or cancel | Quote currency |
| `FOK` sell | Fill share amount completely or cancel | Shares |
| `FAK` buy | Fill available quote amount, cancel remainder | Quote currency |
| `FAK` sell | Fill available shares, cancel remainder | Shares |

**FR-021** GTC and GTD require a limit price. FOK and FAK require a worst
acceptable price as slippage protection.

**FR-022** Post-only is supported only for GTC and GTD. Invalid combinations are
rejected locally.

**FR-023** GTD requests use an RFC 3339 UTC timestamp externally. The adapter
must account for Polymarket's expiration safety threshold and reject an
expiration that cannot meet venue requirements.

**FR-024** The public API must use decimal strings for quote amounts, shares,
prices, fees, and exposure.

**FR-025** The adapter must query current venue fee parameters and include all
applicable fees in balance and risk checks. v1 does not attach a builder code
or charge a builder fee.

### 7.4 Risk

**FR-030** Live mode requires a versioned risk-policy file. Missing or invalid
required fields prevent readiness.

**FR-031** The risk engine must support:

- allowlisted condition IDs and token IDs;
- allowed sides and time-in-force values;
- maximum quote amount per order;
- maximum shares per order;
- maximum open order count;
- maximum open notional per market;
- maximum net position per token;
- maximum gross exposure across the account;
- maximum matched notional per UTC day;
- maximum worst-price distance from the current book;
- minimum visible depth for immediate orders;
- maximum market metadata age;
- maximum order book age;
- maximum user-stream age;
- maximum reconciliation age; and
- configurable cancel-on-halt behavior.

**FR-032** Risk checks must use worst-case exposure, including all open,
partially filled, submitting, and unknown orders.

**FR-033** Risk decisions must be deterministic for the same persisted inputs
and policy version.

**FR-034** Every rejection must include a stable reason code, human-readable
message, policy version, and relevant observed/allowed values.

**FR-035** Geographic availability must be checked from the executor's egress
IP at startup and periodically in live mode. A blocked or indeterminate result
halts new opening risk. The service must not implement or document bypass
routing.

### 7.5 Signing and submission

**FR-040** Venue authentication and order signing occur only inside the
executor.

**FR-041** The private key must never be stored in SQLite, returned by an API,
or written to logs.

**FR-042** Before network submission, the service must persist:

- the normalized venue order;
- the exact signed payload;
- the deterministic order hash or identifier;
- the signer and funder addresses;
- the venue protocol version;
- the SDK version; and
- the associated intent and policy version.

**FR-043** The service must never generate a different signed order when
retrying an ambiguous submission.

**FR-044** A timeout or connection loss after submission begins transitions the
order to `UNKNOWN`. The service must reconcile by deterministic identifier
before permitting further risk for the affected account and token.

**FR-045** Automatic submission retries are permitted only when the service can
prove that no request bytes reached the venue. HTTP response errors must be
classified as terminal, retryable-before-submit, or ambiguous.

### 7.6 Observation and reconciliation

**FR-050** The executor must consume Polymarket's authenticated user stream for
order and trade changes and the market stream for execution-relevant book state.

**FR-051** WebSocket events are low-latency signals, not the sole source of
truth. The service must perform REST reconciliation:

- at startup;
- at least every 60 seconds;
- after reconnect;
- after sequence gaps or malformed events;
- after submission or cancellation timeouts;
- when an order becomes `UNKNOWN`; and
- on explicit operator request.

**FR-052** Reconciliation must compare local open orders, recent trades,
balances, allowances, and positions with venue state.

**FR-053** Reconciliation may advance local state from venue evidence but must
never erase the local event history.

**FR-054** Unexpected venue orders, missing local orders, balance mismatches,
and unresolved trades must produce durable reconciliation findings and latch
the configured halt behavior.

**FR-055** Trade settlement must preserve the venue states `MATCHED`, `MINED`,
`CONFIRMED`, `RETRYING`, and `FAILED`. A fill is not final until the venue
reports its terminal settlement state.

### 7.7 Heartbeat and cancellation safety

**FR-060** Live mode must maintain the Polymarket order-safety heartbeat at the
venue-recommended interval.

**FR-061** Failure to send or validate heartbeats within the safety budget must:

- mark readiness false;
- latch `HALTED`;
- assume open orders may have been cancelled;
- reconcile all open orders; and
- prohibit new submissions until an operator resumes.

**FR-062** Cancellation endpoints must be idempotent and persist their target
set before contacting the venue.

**FR-063** Graceful shutdown must stop accepting intents, request cancellation
according to policy, stop heartbeats, perform a final bounded reconciliation,
checkpoint the database, and exit non-zero if safe shutdown cannot be verified.

### 7.8 Paper execution

**FR-070** Paper mode must require no wallet or API credentials.

**FR-071** Paper mode must use the same intent validation, risk engine, state
machines, journal, API, and event model as live mode.

**FR-072** Paper FOK and FAK fills must walk a fresh observed order book and
respect price protection, depth, tick size, size, and fees.

**FR-073** Resting paper orders must fill conservatively. The default model
must not award a fill merely because the book touched the order price; it
requires observed trading through the price or an explicitly configured queue
model.

**FR-074** Paper events must be visibly labeled and must never be written to a
live database.

**FR-075** The simulator must support a deterministic clock and event fixture
input for repeatable tests.

### 7.9 Operator controls

**FR-080** The service must expose latched `HALT` and explicit `RESUME`.

**FR-081** Resume must require:

- authenticated operator action;
- a reason;
- a successful recent reconciliation;
- healthy required streams and heartbeat; and
- no unresolved critical finding.

**FR-082** Risk-reducing cancellations and position-closing orders may be
allowed while halted only when policy explicitly permits them and the service
can prove they do not increase worst-case exposure.

**FR-083** Every operator action must include actor, reason, time, prior state,
new state, and correlation ID in the audit journal.

## 8. Control API

The API prefix is `/v1`. JSON uses `snake_case`. Timestamps use RFC 3339 UTC.
All resources include `id`, `created_at`, `updated_at`, and `mode`.

Bearer credentials map to a configured actor ID and scopes. v1 scopes are
`read`, `submit`, `cancel`, and `operate`. Strategy clients normally receive
`read`, `submit`, and `cancel`; only operator credentials receive `operate`.
Credential files are mounted secrets, and only one-way token digests may be
stored in non-secret configuration.

### 8.1 Submit intent

`POST /v1/intents`

Required headers:

```text
Authorization: Bearer <token>
Idempotency-Key: <caller-generated-key>
Content-Type: application/json
```

Example request:

```json
{
  "condition_id": "0x...",
  "token_id": "52114319501245...",
  "side": "BUY",
  "time_in_force": "GTD",
  "quantity": {
    "unit": "shares",
    "value": "25.0000"
  },
  "limit_price": "0.5400",
  "worst_price": null,
  "expires_at": "2026-07-18T18:00:00Z",
  "post_only": true,
  "client_context": {
    "strategy": "example_strategy",
    "strategy_order_id": "signal-20260718-0042",
    "correlation_id": "run-20260718"
  }
}
```

The response is `202 Accepted` after durable admission. It does not imply venue
acceptance.

### 8.2 Read resources

- `GET /v1/intents/{intent_id}`
- `GET /v1/intents?state=&condition_id=&created_after=&cursor=`
- `GET /v1/orders/{order_id}`
- `GET /v1/orders?state=&condition_id=&token_id=&cursor=`
- `GET /v1/trades?status=&condition_id=&token_id=&cursor=`
- `GET /v1/positions`
- `GET /v1/reconciliations/{reconciliation_id}`

List endpoints use opaque cursor pagination and stable creation-time ordering.

### 8.3 Cancel

`POST /v1/cancellations`

Exactly one selector is allowed:

```json
{
  "order_id": "ord_...",
  "intent_id": null,
  "condition_id": null,
  "all_open_orders": false,
  "reason": "strategy_withdrawn"
}
```

### 8.4 Control

- `POST /v1/control/halt`
- `POST /v1/control/resume`
- `GET /v1/control/state`
- `POST /v1/reconciliations`

Halt and resume require an `Idempotency-Key` and a non-empty reason.

### 8.5 Events

`GET /v1/events` returns Server-Sent Events.

- Every event has a monotonically increasing local sequence.
- Clients resume using `Last-Event-ID`.
- The server replays retained events from SQLite before switching to live flow.
- If the requested sequence has been pruned, the server returns a reset event
  directing the client to query current resources.
- Slow clients are disconnected rather than allowed to exhaust memory.

### 8.6 Health and metrics

- `GET /health/live`: process is running.
- `GET /health/ready`: service may safely accept work.
- `GET /metrics`: Prometheus text format on a configurable private listener.

Readiness is false during startup reconciliation, halt, stale required state,
database write failure, heartbeat failure, or unresolved critical ambiguity.

### 8.7 Error envelope

```json
{
  "error": {
    "code": "RISK_MAX_MARKET_EXPOSURE",
    "message": "Worst-case market exposure would exceed policy",
    "retryable": false,
    "correlation_id": "req_...",
    "details": {
      "observed": "950.00",
      "requested": "100.00",
      "limit": "1000.00",
      "policy_version": "risk-2026-07-18"
    }
  }
}
```

Stable error codes are part of the v1 API contract. Error messages are not.

## 9. State model

### 9.1 Service state

- `STARTING`
- `RECONCILING`
- `READY`
- `DEGRADED`
- `HALTED`
- `SHUTTING_DOWN`

`HALTED` is latched across restarts.

### 9.2 Intent state

```text
RECEIVED
  -> VALIDATING
    -> REJECTED
    -> APPROVED
      -> PREPARING
        -> PREPARED
          -> SUBMITTING
            -> SUBMITTED
            -> UNKNOWN
```

The intent state describes executor processing. Order and trade resources
describe venue lifecycle and settlement.

### 9.3 Order state

- `PREPARED`
- `SUBMITTING`
- `LIVE`
- `PARTIALLY_FILLED`
- `FILLED`
- `CANCEL_PENDING`
- `CANCELLED`
- `EXPIRED`
- `REJECTED`
- `UNKNOWN`

Terminal states are `FILLED`, `CANCELLED`, `EXPIRED`, and `REJECTED`.
`UNKNOWN` is non-terminal and blocks affected risk until reconciled.

### 9.4 Trade state

- `MATCHED`
- `MINED`
- `CONFIRMED`
- `RETRYING`
- `FAILED`

`CONFIRMED` and `FAILED` are terminal.

## 10. Persistence and audit

SQLite runs in WAL mode with foreign keys enabled, `synchronous=FULL`, bounded
busy timeouts, and explicit transactions. There is one serialized writer and a
small read pool.

Required logical tables:

- `intents`
- `idempotency_keys`
- `risk_decisions`
- `risk_reservations`
- `prepared_orders`
- `submission_attempts`
- `orders`
- `order_transitions`
- `trades`
- `venue_trade_finality`
- `positions`
- `cancellation_requests`
- `execution_events`
- `reconciliation_runs`
- `reconciliation_findings`
- `control_state`
- `operator_actions`
- `instance_metadata`
- `backups`
- paper-only account, inventory, venue-order, and market-event projections
- SQLx schema migrations

The append-only `execution_events` table is the audit source for SSE. Mutable
projection tables make current-state queries efficient.

Retention is operator-configurable but live audit events may not be pruned
until a verified backup exists. Database backup uses SQLite's online backup
mechanism and produces a manifest with schema version, mode, last event
sequence, size, and checksum.

## 11. Architecture

### 11.1 Components

```text
Strategy repositories
        |
        | authenticated HTTP intents
        v
Control API -> Intent journal -> Validation -> Risk engine
                                            |
                                            v
                                     Execution coordinator
                                      /                 \
                              Paper venue          Polymarket adapter
                                                       |
                                      REST + market/user WebSockets
                                                       |
                                                 Polymarket CLOB

Venue observations -> Reconciler -> State projections -> SSE / metrics / CLI
```

### 11.2 Rust implementation

The implementation target is stable Rust with:

- Tokio for asynchronous runtime;
- Axum and Tower for the private HTTP service;
- Serde for API and persisted payloads;
- SQLx with SQLite;
- `rust_decimal` for price, size, fee, and exposure arithmetic;
- `tracing` for structured logs;
- `secrecy` for secret-bearing values;
- Clap for the operator CLI; and
- Polymarket's official Rust SDK behind an internal adapter.

The Polymarket SDK version must be pinned exactly. SDK request/response types
must not appear in core domain types or public APIs. Recorded contract fixtures
and live read-only conformance tests gate SDK upgrades.

The initial codebase has one internal `ExecutionVenue` boundary with two
implementations: `PolymarketVenue` and `PaperVenue`. v1 does not implement a
dynamic plugin system.

### 11.3 Polymarket protocol policy

The initial live target is Polymarket CLOB V2. The endpoint is configurable,
but startup must query and verify the expected protocol and chain before
signing. The process must refuse an unexpected protocol rather than silently
switching between V1 and V2.

Negative-risk routing, tick size, fee parameters, funder type, and exchange
contract selection are derived from verified venue metadata, not caller input.

### 11.4 Signers

The core depends on a narrow signer interface. Initial supported backends:

1. a local private key loaded from a mounted secret file; and
2. an Alloy-compatible remote signer when enabled at build time.

Environment-variable private keys may be supported for local development but
must produce a live-mode warning. Secrets are redacted at type and logging
boundaries.

## 12. Configuration

Configuration is loaded once at startup from:

1. command-line paths and mode selection;
2. a non-secret TOML configuration file;
3. a versioned JSON risk-policy file; and
4. mounted secret files or the configured remote signer.

Unknown configuration fields are errors. Live mode refuses placeholder values,
wildcard allowlists, missing authentication, writable-by-group secret files,
or a paper database path.

Representative configuration:

```toml
mode = "paper"

[server]
bind = "127.0.0.1:8787"
metrics_bind = "127.0.0.1:9090"
max_body_bytes = 65536
request_timeout_ms = 45000

[storage]
database_path = "/data/paper.sqlite3"
backup_dir = "/data/backups"
event_retention = 1000000

[polymarket]
clob_url = "https://clob.polymarket.com"
expected_protocol = 2
chain_id = 137
reconciliation_interval_seconds = 60
heartbeat_interval_seconds = 5
```

## 13. Operator CLI

The binary is `oddsfox-exec`.

Required commands:

```text
oddsfox-exec serve
oddsfox-exec doctor
oddsfox-exec submit
oddsfox-exec cancel
oddsfox-exec halt
oddsfox-exec resume
oddsfox-exec orders
oddsfox-exec trades
oddsfox-exec positions
oddsfox-exec reconcile
oddsfox-exec backup
```

Mutating CLI commands call the running control API. They do not open the
database directly. `doctor` performs non-trading configuration, connectivity,
permission, protocol, clock, and credential checks.

## 14. Security, compliance, and licensing

### 14.1 Security requirements

- Run as a dedicated non-root user in a read-only container filesystem.
- Mount only `/data` and required secret files.
- Create database and backup files with owner-only permissions.
- Use Rustls-based TLS clients where supported.
- Disable CORS and bind to loopback by default.
- Require constant-time bearer-token verification.
- Apply request size, concurrency, and timeout limits.
- Redact secrets, signatures where appropriate, auth headers, and private key
  material from logs and errors.
- Never include secrets in panic output, metrics labels, or support bundles.
- Provide `SECURITY.md` and a private vulnerability-reporting path.
- Run dependency, license, and secret scans in CI.

### 14.2 Geographic and platform rules

The executor must use Polymarket's geographic-availability endpoint from the
same egress path used for trading. It must honor blocked and close-only
behavior, fail closed when eligibility is indeterminate, and preserve the
result in the audit log without storing more IP data than needed.

The software must not claim that its MIT license grants access to Polymarket or
overrides Polymarket's Terms of Service, builder rules, geographic controls, or
applicable law.

### 14.3 Licensing

Original OddsFox execution code remains under MIT.

The repository must include:

- `LICENSE`;
- `THIRD_PARTY_NOTICES.md`;
- dependency license checks in CI;
- a non-affiliation statement;
- a trading-risk disclaimer; and
- documentation that users are responsible for venue eligibility and legal
  compliance.

Polymarket's official clients are open source and MIT licensed. Their use does
not require changing this project's MIT license, but their notices and exact
dependency versions must be recorded.

## 15. Reliability and performance

### 15.1 Reliability invariants

- No venue side effect occurs before durable local intent admission.
- No submission occurs before the exact signed payload is durable.
- A process crash cannot turn an ambiguous order into a new order.
- No `UNKNOWN` order is excluded from worst-case exposure.
- No process reports ready before startup reconciliation.
- No operator resume clears a halt without a durable audit event.
- Paper and live records cannot share a database.

### 15.2 Initial service objectives

These are product targets, not guarantees about venue performance:

- p99 intent admission under 100 ms on supported local storage;
- p99 approved-intent-to-submission-start under 100 ms when all required state
  is cached and fresh;
- p99 local event publication under 250 ms after venue observation;
- readiness within 30 seconds when storage and venue dependencies are healthy;
- scheduled reconciliation at least once every 60 seconds;
- no more than 5 seconds between healthy heartbeat attempts;
- zero acknowledged-but-uncommitted audit events; and
- zero silent duplicate venue submissions.

Initial capacity target:

- 25 admitted intents per second sustained, 100 burst;
- 10,000 locally tracked open orders;
- 1,000 execution-relevant token subscriptions; and
- 10 concurrent API clients.

Venue rate limits may impose lower operational limits. The executor must
backpressure and reject explicitly rather than build an unbounded queue.

## 16. Observability

Structured logs include timestamp, severity, mode, component, correlation ID,
intent ID, order ID, and stable event code where applicable.

Required metrics include:

- admitted, approved, rejected, submitted, and unknown intents;
- live, partially filled, terminal, and unknown orders;
- trades by settlement state;
- risk rejections by reason;
- venue request rate, latency, retry class, and errors;
- market and user WebSocket connection state and last-event age;
- heartbeat attempt age and failures;
- reconciliation duration, age, and findings;
- database transaction latency and failures;
- current service/control state;
- open notional and worst-case exposure; and
- event-stream clients and disconnects.

Metrics labels must be bounded. Raw condition IDs, token IDs, strategy IDs, and
error messages are prohibited as labels.

## 17. Testing and release gates

### 17.1 Automated tests

Unit and property tests must cover:

- decimal parsing, precision, overflow, and tick/size divisibility;
- all valid and invalid order-field combinations;
- risk thresholds at, below, and above every boundary;
- idempotency under concurrent duplicate requests;
- every allowed and forbidden state transition;
- fee-inclusive balance and exposure calculations;
- canonical request hashing;
- configuration validation and secret redaction; and
- paper fill determinism.

Integration tests must cover:

- crash after intent commit but before validation;
- crash after signed-payload commit but before submission;
- crash after venue response but before local response commit;
- submission timeout with eventual venue acceptance;
- submission timeout with no venue acceptance;
- duplicate venue response;
- partial fills and all trade finality states;
- cancellation timeout and cancellation/fill races;
- market and user WebSocket disconnects, gaps, duplicates, and reordering;
- heartbeat loss and venue-side cancellation;
- startup with unexpected open orders;
- stale balances, allowances, books, and metadata;
- geoblock blocked, close-only, malformed, and unavailable responses;
- SQLite busy, full disk, corruption, backup, and restore;
- halt persistence across restart;
- no CORS, auth enforcement, body limits, and SSE resume; and
- hard separation of paper and live databases.

### 17.2 CI gates

Every change must pass:

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo audit
dependency license policy
secret scan
container build
container smoke test in paper mode
```

SDK upgrades additionally require recorded Polymarket contract tests and a
read-only conformance run against the configured production endpoint.

### 17.3 Live release gate

Live mode remains compile-time or configuration-gated until:

- all critical tests pass;
- a restored backup passes integrity and reconciliation checks;
- paper mode runs continuously for at least 24 hours on live books;
- no unresolved `UNKNOWN` state exists;
- the operator runbook is rehearsed; and
- the live canary is explicitly approved.

## 18. Rollout

### Phase 0: Repository reset

- Preserve the existing `v0.1.0` tag.
- Rename the repository to `oddsfox-execution`.
- Remove the Go dashboard backend and its API contract.
- Update downstream OddsFox repositories so they no longer build or depend on
  `oddsfox-live`.
- Establish Rust workspace, CI, security policy, licensing notices, and
  architecture records.

### Phase 1: Paper foundation

- Implement domain types, SQLite journal, state machines, control API, auth,
  risk-policy parsing, operator controls, SSE, and conservative paper venue.
- Run deterministic fixtures and live-book shadow execution.

Exit criterion: 24-hour paper run with restart, backup/restore, and halt/resume
drills completed without invariant violations.

### Phase 2: Polymarket observation and reconciliation

- Add pinned official Rust SDK adapter.
- Add protocol verification, market metadata, order book, user stream,
  balances, allowances, open orders, trades, heartbeat, and reconciliation.
- Keep submission disabled.

Exit criterion: local projections remain consistent with a funded account
during disconnect and restart drills.

### Phase 3: Live submission

- Enable signing, durable prepared orders, submission, cancellation, ambiguous
  outcome handling, and full live risk checks.
- Start with one allowlisted market, one dedicated minimally funded wallet,
  post-only GTD orders, and a maximum order notional of 5 quote units.

Exit criterion: operator verifies place, observe, partial/full fill, cancel,
restart, reconcile, heartbeat cancellation, and kill-switch workflows.

### Phase 4: Production hardening

- Add remote signer support if required.
- Tune capacity and backpressure from observed workloads.
- Complete recovery runbook, dashboards, alerts, dependency update policy, and
  external security review.

## 19. Repository migration

The overhaul is intentionally incompatible.

Remove:

- `main.go`, `server.go`, `state.go`, `stream.go`, and their Go tests;
- graph and knockout artifact loading;
- sports WebSocket integration;
- dashboard SSE and replay JSONL;
- `/api/v0/*`;
- Go module files; and
- the current Go Docker image.

Retain or replace:

- retain the MIT license;
- replace the README with execution-product documentation;
- replace the Dockerfile with a multi-stage Rust build and non-root runtime;
- replace dashboard API docs with OpenAPI and operator docs; and
- preserve historical behavior through Git tags, not compatibility code.

Companion changes are required in `oddsfox-dash`, `oddsfox-graph`, and
`oddsfox-pipeline` to remove references to `oddsfox-live`.

The first preview release is `v0.2.0`. The first release authorized for
meaningful live capital is `v1.0.0`.

## 20. Acceptance criteria for v1

v1 is complete when:

1. A strategy can submit and cancel all supported order types through the
   documented API without access to venue credentials.
2. Concurrent duplicate requests demonstrably produce at most one venue order.
3. Every accepted intent, risk decision, signature preparation, venue response,
   state transition, reconciliation result, and operator action is durable and
   queryable.
4. The service recovers from every tested crash point without blind resubmission.
5. Unknown venue outcomes latch risk and reconcile to a proven result.
6. Paper and live execution pass the same contract suite.
7. Startup, WebSocket gaps, heartbeat failures, and database failures trigger
   the specified safe states.
8. Risk limits cannot be exceeded through concurrent intents or stale
   projections in the test model.
9. No secret appears in logs, metrics, API responses, panic output, container
   layers, or the database.
10. The live canary and recovery drills in Phase 3 pass.
11. The old dashboard API and its downstream deployment dependencies are
    removed.
12. The repository ships under MIT with third-party notices and the required
    compliance disclaimers.

## 21. Decisions to confirm before implementation

The specification assumes the following defaults. They should be confirmed
before Phase 1 is locked:

1. One Polymarket account and one active executor instance are sufficient for
   v1.
2. Strategy clients can use HTTP plus resumable SSE; a message broker is not
   required.
3. SQLite on one host is acceptable; PostgreSQL and active-active operation are
   out of scope.
4. The initial production wallet will use Polymarket's current recommended
   account/funder flow and a dedicated minimally funded signer.
5. The initial capacity target is 25 sustained intents per second, not
   high-frequency market-making scale.
6. v1 will not charge builder fees.
7. The old dashboard API can be removed completely once companion repository
   changes land.

## 22. External references

- [Polymarket API introduction](https://docs.polymarket.com/api-reference/introduction)
- [Polymarket authentication](https://docs.polymarket.com/api-reference/authentication)
- [Polymarket clients and SDKs](https://docs.polymarket.com/api-reference/clients-sdks)
- [Polymarket order overview](https://docs.polymarket.com/trading/orders/overview)
- [Polymarket geographic restrictions](https://docs.polymarket.com/api-reference/geoblock)
- [Polymarket builder fees](https://docs.polymarket.com/builders/fees)
- [Official Polymarket Rust SDK](https://github.com/Polymarket/rs-clob-client-v2)
