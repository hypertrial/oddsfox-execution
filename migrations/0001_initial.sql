CREATE TABLE instance_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    mode TEXT NOT NULL,
    chain_id INTEGER NOT NULL,
    protocol_version INTEGER NOT NULL,
    signer_address TEXT,
    funder_address TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE intents (
    id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    request_json TEXT NOT NULL,
    state TEXT NOT NULL,
    rejection_code TEXT,
    rejection_message TEXT,
    policy_version TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(actor_id, idempotency_key)
);

CREATE INDEX idx_intents_created ON intents(created_at, id);
CREATE INDEX idx_intents_state ON intents(state, created_at);

CREATE TABLE idempotency_keys (
    actor_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    response_status INTEGER NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(actor_id, idempotency_key)
);

CREATE TABLE risk_decisions (
    id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL REFERENCES intents(id),
    approved INTEGER NOT NULL,
    reason_code TEXT NOT NULL,
    observed_json TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE risk_reservations (
    intent_id TEXT PRIMARY KEY REFERENCES intents(id),
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL,
    price TEXT NOT NULL,
    quantity TEXT NOT NULL,
    fee_rate TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_risk_reservations_state
ON risk_reservations(state, condition_id, token_id);

CREATE TABLE prepared_orders (
    id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL REFERENCES intents(id),
    normalized_json TEXT NOT NULL,
    signed_payload_json TEXT,
    payload_sha256 TEXT,
    deterministic_order_id TEXT NOT NULL,
    signer_address TEXT,
    funder_address TEXT,
    protocol_version INTEGER NOT NULL,
    sdk_version TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE submission_attempts (
    id TEXT PRIMARY KEY,
    prepared_order_id TEXT NOT NULL REFERENCES prepared_orders(id),
    attempt_number INTEGER NOT NULL,
    state TEXT NOT NULL,
    request_started_at TEXT,
    response_class TEXT,
    response_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(prepared_order_id, attempt_number)
);

CREATE TABLE orders (
    id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL REFERENCES intents(id),
    mode TEXT NOT NULL,
    venue_order_id TEXT,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL,
    time_in_force TEXT NOT NULL,
    price TEXT NOT NULL,
    fee_rate TEXT NOT NULL,
    original_quantity TEXT NOT NULL,
    remaining_quantity TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_orders_state ON orders(state, created_at);
CREATE INDEX idx_orders_token ON orders(token_id, state);

CREATE TABLE order_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL REFERENCES orders(id),
    prior_state TEXT,
    new_state TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE trades (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES orders(id),
    mode TEXT NOT NULL,
    venue_trade_id TEXT NOT NULL UNIQUE,
    price TEXT NOT NULL,
    size TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE venue_trade_finality (
    venue_trade_id TEXT PRIMARY KEY,
    trade_id TEXT NOT NULL REFERENCES trades(id),
    status TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE positions (
    token_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    shares TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Durable simulator-side state. These tables exist in every database so the
-- schema is identical, but Store refuses to use them unless mode is PAPER.
CREATE TABLE paper_account (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    configuration_sha256 TEXT NOT NULL,
    quote_balance TEXT NOT NULL,
    reserved_quote TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE paper_inventory (
    token_id TEXT PRIMARY KEY,
    shares TEXT NOT NULL,
    reserved_shares TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE paper_venue_orders (
    venue_order_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL,
    state TEXT NOT NULL,
    price TEXT NOT NULL,
    fee_rate TEXT NOT NULL,
    original_quantity TEXT NOT NULL,
    remaining_quantity TEXT NOT NULL,
    filled_quantity TEXT NOT NULL,
    filled_price TEXT,
    quote_amount TEXT NOT NULL,
    fee_amount TEXT NOT NULL,
    reserved_quote TEXT NOT NULL,
    reserved_shares TEXT NOT NULL,
    venue_trade_ids_json TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE paper_market_events (
    event_id TEXT PRIMARY KEY,
    token_id TEXT NOT NULL,
    price TEXT NOT NULL,
    size TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE cancellation_requests (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL,
    selector_json TEXT NOT NULL,
    target_order_ids_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE execution_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE reconciliation_runs (
    id TEXT PRIMARY KEY,
    trigger TEXT NOT NULL,
    state TEXT NOT NULL,
    summary_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE reconciliation_findings (
    id TEXT PRIMARY KEY,
    reconciliation_id TEXT NOT NULL REFERENCES reconciliation_runs(id),
    severity TEXT NOT NULL,
    code TEXT NOT NULL,
    details_json TEXT NOT NULL,
    resolved_at TEXT
);

CREATE TABLE control_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    state TEXT NOT NULL,
    reason TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE operator_actions (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT NOT NULL,
    prior_state TEXT NOT NULL,
    new_state TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE backups (
    id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    database_path TEXT,
    manifest_path TEXT,
    checksum_sha256 TEXT,
    last_event_sequence INTEGER,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

INSERT INTO control_state(singleton, state, reason, updated_at)
VALUES(1, 'STARTING', 'initializing', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
