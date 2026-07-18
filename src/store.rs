use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use fs2::FileExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{
    QueryBuilder, Row, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use thiserror::Error;
use tokio::sync::{Mutex, OwnedMutexGuard, broadcast};
use uuid::Uuid;

use crate::{
    config::{PaperConfig, PolymarketConfig, StorageConfig},
    domain::{
        CancellationRequest, ExecutionEvent, IntentRecord, IntentState, Mode, OrderIntentRequest,
        OrderRecord, OrderState, PositionRecord, ServiceState, Side, TradeRecord, TradeState,
    },
    risk::{RiskDecision, RiskPolicy, conservative_exposure},
    venue::{MarketRules, OrderBook},
};

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    mode: Mode,
    write_gate: Arc<Mutex<()>>,
    risk_gate: Arc<Mutex<()>>,
    events: broadcast::Sender<ExecutionEvent>,
    event_retention: u64,
    backup_dir: Arc<PathBuf>,
    _lock: Arc<InstanceLock>,
}

struct InstanceLock {
    _file: File,
}

#[derive(Debug)]
pub enum Admission {
    Created(IntentRecord),
    Existing(IntentRecord),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskSnapshot {
    pub open_order_count: u64,
    pub market_open_notional: Decimal,
    pub token_position: Decimal,
    pub token_pending_buys: Decimal,
    pub token_pending_sells: Decimal,
    pub gross_exposure: Decimal,
    pub daily_matched_notional: Decimal,
}

#[derive(sqlx::FromRow)]
struct RiskExposureRow {
    condition_id: String,
    token_id: String,
    side: String,
    fee_rate: String,
    quote_exposure: String,
    original_quantity: String,
    quantity: String,
}

#[derive(sqlx::FromRow)]
struct RiskPositionRow {
    token_id: String,
    shares: String,
}

#[derive(sqlx::FromRow)]
struct RiskTradeRow {
    price: String,
    size: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupRecord {
    pub id: Uuid,
    pub state: String,
    pub database_path: Option<String>,
    pub manifest_path: Option<String>,
    pub checksum_sha256: Option<String>,
    pub last_event_sequence: Option<i64>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct CancellationRecord {
    pub id: Uuid,
    pub actor_id: String,
    pub selector_json: String,
    pub target_order_ids_json: String,
    pub reason: String,
    pub state: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ReconciliationRecord {
    pub id: Uuid,
    pub trigger: String,
    pub state: String,
    pub summary_json: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub backup_id: Uuid,
    pub schema_version: i64,
    pub mode: Mode,
    pub chain_id: u64,
    pub protocol_version: u32,
    pub signer_address: Option<String>,
    pub funder_address: Option<String>,
    pub last_event_sequence: i64,
    pub database_size_bytes: u64,
    pub checksum_sha256: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct IdempotentResponse {
    pub status: u16,
    pub body: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct UnknownSubmission {
    pub order_id: Uuid,
    pub intent_id: Uuid,
    pub deterministic_order_id: String,
}

#[derive(Debug, Clone)]
pub struct PreparedSubmission {
    pub order_id: Uuid,
    pub deterministic_order_id: String,
    pub normalized_json: String,
    pub signed_payload_json: Option<String>,
    pub signer_address: Option<String>,
    pub funder_address: Option<String>,
    pub protocol_version: u32,
    pub sdk_version: String,
}

#[derive(Debug, Clone)]
pub struct PaperOrderCommit {
    pub venue_order_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub side: Side,
    pub state: OrderState,
    pub price: Decimal,
    pub fee_rate: Decimal,
    pub original_quantity: Decimal,
    pub remaining_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub filled_price: Option<Decimal>,
    pub quote_amount: Decimal,
    pub fee_amount: Decimal,
    pub reserved_quote: Decimal,
    pub reserved_shares: Decimal,
    pub venue_trade_ids: Vec<String>,
    pub evidence: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct PaperOrderSnapshot {
    pub venue_order_id: String,
    pub state: OrderState,
    pub remaining_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub filled_price: Option<Decimal>,
    pub venue_trade_ids: Vec<String>,
    pub evidence: serde_json::Value,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperAccountSnapshot {
    pub quote_balance: Decimal,
    pub reserved_quote: Decimal,
    pub positions: std::collections::BTreeMap<String, (Decimal, Decimal)>,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("database migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("database is already locked by another executor: {0}")]
    AlreadyLocked(PathBuf),
    #[error("database identity mismatch: {0}")]
    IdentityMismatch(String),
    #[error("idempotency key reused with a different request")]
    IdempotencyConflict,
    #[error("new risk admission is disabled while service state is {0}")]
    NewRiskDisabled(String),
    #[error("resource not found")]
    NotFound,
    #[error("invalid stored decimal: {0}")]
    InvalidDecimal(String),
}

impl Store {
    pub async fn open(
        storage: &StorageConfig,
        mode: Mode,
        polymarket: &PolymarketConfig,
    ) -> Result<Self, StoreError> {
        let database_path = PathBuf::from(&storage.database_path);
        if let Some(parent) = database_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let lock_path = PathBuf::from(format!("{}.lock", database_path.display()));
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        set_owner_only_file(&lock_path)?;
        lock_file
            .try_lock_exclusive()
            .map_err(|_| StoreError::AlreadyLocked(lock_path))?;

        let options = SqliteConnectOptions::new()
            .filename(&database_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(4)
            .connect_with(options)
            .await?;
        sqlx::migrate!().run(&pool).await?;
        let legacy_active_exposure: i64 = sqlx::query_scalar(
            "SELECT \
                (SELECT COUNT(*) FROM risk_reservations \
                 WHERE state='ACTIVE' AND quote_exposure='0') + \
                (SELECT COUNT(*) FROM orders \
                 WHERE state IN \
                 ('PREPARED','SUBMITTING','LIVE','PARTIALLY_FILLED','CANCEL_PENDING','UNKNOWN') \
                 AND quote_exposure='0')",
        )
        .fetch_one(&pool)
        .await?;
        if legacy_active_exposure > 0 {
            return Err(StoreError::IdentityMismatch(
                "legacy active risk records do not contain conservative quote exposure; \
                 reconcile and retire the development database before starting this version"
                    .into(),
            ));
        }

        let (events, _) = broadcast::channel(1_024);
        let store = Self {
            pool,
            mode,
            write_gate: Arc::new(Mutex::new(())),
            risk_gate: Arc::new(Mutex::new(())),
            events,
            event_retention: storage.event_retention,
            backup_dir: Arc::new(PathBuf::from(&storage.backup_dir)),
            _lock: Arc::new(InstanceLock { _file: lock_file }),
        };
        store.bind_identity(polymarket).await?;
        harden_sqlite_files(&database_path)?;
        Ok(store)
    }

    #[must_use]
    pub const fn mode(&self) -> Mode {
        self.mode
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ExecutionEvent> {
        self.events.subscribe()
    }

    pub(crate) async fn lock_risk(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.risk_gate).lock_owned().await
    }

    async fn bind_identity(&self, polymarket: &PolymarketConfig) -> Result<(), StoreError> {
        let _guard = self.write_gate.lock().await;
        let existing = sqlx::query(
            "SELECT mode, chain_id, protocol_version, signer_address, funder_address \
             FROM instance_metadata WHERE singleton = 1",
        )
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = existing {
            let stored_mode: String = row.try_get("mode")?;
            if stored_mode != self.mode.to_string() {
                return Err(StoreError::IdentityMismatch(format!(
                    "mode is {stored_mode}, requested {}",
                    self.mode
                )));
            }
            let chain_id: i64 = row.try_get("chain_id")?;
            let protocol_version: i64 = row.try_get("protocol_version")?;
            let configured_chain_id = i64::try_from(polymarket.chain_id).map_err(|_| {
                StoreError::IdentityMismatch("configured chain ID exceeds SQLite integer".into())
            })?;
            if chain_id != configured_chain_id
                || protocol_version != i64::from(polymarket.expected_protocol)
            {
                return Err(StoreError::IdentityMismatch(
                    "chain or protocol changed".into(),
                ));
            }
            if self.mode == Mode::Live {
                let signer: Option<String> = row.try_get("signer_address")?;
                let funder: Option<String> = row.try_get("funder_address")?;
                if signer != polymarket.signer_address || funder != polymarket.funder_address {
                    return Err(StoreError::IdentityMismatch(
                        "signer or funder changed".into(),
                    ));
                }
            }
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO instance_metadata \
             (singleton, mode, chain_id, protocol_version, signer_address, funder_address, created_at) \
             VALUES(1, ?, ?, ?, ?, ?, ?)",
        )
        .bind(self.mode)
        .bind(i64::try_from(polymarket.chain_id).map_err(|_| {
            StoreError::IdentityMismatch("configured chain ID exceeds SQLite integer".into())
        })?)
        .bind(i64::from(polymarket.expected_protocol))
        .bind(&polymarket.signer_address)
        .bind(&polymarket.funder_address)
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn admit_intent(
        &self,
        actor_id: &str,
        idempotency_key: &str,
        request: &OrderIntentRequest,
    ) -> Result<Admission, StoreError> {
        let canonical = serde_jcs::to_vec(request)?;
        let request_hash = hex::encode(Sha256::digest(&canonical));
        let request_json =
            String::from_utf8(canonical).expect("canonical JSON serialization always emits UTF-8");
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;

        if let Some(existing) = sqlx::query_as::<_, IntentRecord>(
            "SELECT * FROM intents WHERE actor_id = ? AND idempotency_key = ?",
        )
        .bind(actor_id)
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        {
            if existing.request_hash != request_hash {
                return Err(StoreError::IdempotencyConflict);
            }
            let original_response: String = sqlx::query_scalar(
                "SELECT response_json FROM idempotency_keys \
                 WHERE actor_id=? AND idempotency_key=? AND resource_type='intent'",
            )
            .bind(actor_id)
            .bind(idempotency_key)
            .fetch_one(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(Admission::Existing(serde_json::from_str(
                &original_response,
            )?));
        }
        let service_state: String =
            sqlx::query_scalar("SELECT state FROM control_state WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await?;
        if service_state != "READY" {
            return Err(StoreError::NewRiskDisabled(service_state));
        }

        let now = Utc::now();
        let record = IntentRecord {
            id: Uuid::now_v7(),
            mode: self.mode,
            actor_id: actor_id.to_owned(),
            idempotency_key: idempotency_key.to_owned(),
            request_hash: request_hash.clone(),
            request_json,
            state: IntentState::Received,
            rejection_code: None,
            rejection_message: None,
            created_at: now,
            updated_at: now,
        };
        sqlx::query(
            "INSERT INTO intents \
             (id, mode, actor_id, idempotency_key, request_hash, request_json, state, created_at, updated_at) \
             VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.id)
        .bind(record.mode)
        .bind(&record.actor_id)
        .bind(&record.idempotency_key)
        .bind(&record.request_hash)
        .bind(&record.request_json)
        .bind(record.state)
        .bind(record.created_at)
        .bind(record.updated_at)
        .execute(&mut *tx)
        .await?;
        let response_json = serde_json::to_string(&record)?;
        sqlx::query(
            "INSERT INTO idempotency_keys \
             (actor_id, idempotency_key, request_hash, resource_type, resource_id, response_status, response_json, created_at) \
             VALUES(?, ?, ?, 'intent', ?, 202, ?, ?)",
        )
        .bind(actor_id)
        .bind(idempotency_key)
        .bind(&request_hash)
        .bind(record.id)
        .bind(response_json)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let event = append_event(
            &mut tx,
            self.mode,
            "intent.received",
            "intent",
            &record.id.to_string(),
            &serde_json::to_string(request)?,
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(Admission::Created(record))
    }

    pub async fn replay_intent(
        &self,
        actor_id: &str,
        idempotency_key: &str,
        request: &OrderIntentRequest,
    ) -> Result<Option<IntentRecord>, StoreError> {
        let request_hash = hex::encode(Sha256::digest(serde_jcs::to_vec(request)?));
        let existing = sqlx::query(
            "SELECT request_hash, response_json FROM idempotency_keys \
             WHERE actor_id=? AND idempotency_key=? AND resource_type='intent'",
        )
        .bind(actor_id)
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(existing) = existing {
            let existing_hash: String = existing.try_get("request_hash")?;
            if existing_hash != request_hash {
                return Err(StoreError::IdempotencyConflict);
            }
            let response_json: String = existing.try_get("response_json")?;
            return Ok(Some(serde_json::from_str(&response_json)?));
        }
        Ok(None)
    }

    pub async fn idempotent_response(
        &self,
        actor_id: &str,
        idempotency_key: &str,
        operation: &str,
        body: &serde_json::Value,
    ) -> Result<Option<IdempotentResponse>, StoreError> {
        let request_hash = operation_hash(operation, body)?;
        let row = sqlx::query(
            "SELECT request_hash, resource_type, response_status, response_json \
             FROM idempotency_keys WHERE actor_id = ? AND idempotency_key = ?",
        )
        .bind(actor_id)
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let stored_hash: String = row.try_get("request_hash")?;
        let stored_operation: String = row.try_get("resource_type")?;
        if stored_hash != request_hash || stored_operation != operation {
            return Err(StoreError::IdempotencyConflict);
        }
        Ok(Some(IdempotentResponse {
            status: row
                .try_get::<i64, _>("response_status")?
                .try_into()
                .map_err(|_| {
                    StoreError::IdentityMismatch("stored HTTP response status is invalid".into())
                })?,
            body: serde_json::from_str(row.try_get("response_json")?)?,
        }))
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn record_idempotent_response(
        &self,
        actor_id: &str,
        idempotency_key: &str,
        operation: &str,
        resource_id: &str,
        request_body: &serde_json::Value,
        status: u16,
        response_body: &serde_json::Value,
    ) -> Result<(), StoreError> {
        let request_hash = operation_hash(operation, request_body)?;
        let response_json = serde_jcs::to_string(response_body)?;
        let _guard = self.write_gate.lock().await;
        let result = sqlx::query(
            "INSERT INTO idempotency_keys \
             (actor_id, idempotency_key, request_hash, resource_type, resource_id, \
              response_status, response_json, created_at) \
             VALUES(?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(actor_id, idempotency_key) DO NOTHING",
        )
        .bind(actor_id)
        .bind(idempotency_key)
        .bind(&request_hash)
        .bind(operation)
        .bind(resource_id)
        .bind(i64::from(status))
        .bind(response_json)
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            let replay = self
                .idempotent_response(actor_id, idempotency_key, operation, request_body)
                .await?;
            if replay.is_none() {
                return Err(StoreError::IdempotencyConflict);
            }
        }
        Ok(())
    }

    pub async fn get_intent(&self, id: Uuid) -> Result<IntentRecord, StoreError> {
        sqlx::query_as("SELECT * FROM intents WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::NotFound)
    }

    pub async fn list_intents(&self, limit: u32) -> Result<Vec<IntentRecord>, StoreError> {
        Ok(
            sqlx::query_as("SELECT * FROM intents ORDER BY created_at, id LIMIT ?")
                .bind(i64::from(limit.min(500)))
                .fetch_all(&self.pool)
                .await?,
        )
    }

    pub async fn list_intents_page(
        &self,
        limit: u32,
        state: Option<IntentState>,
        condition_id: Option<&str>,
        created_after: Option<DateTime<Utc>>,
        cursor: Option<(DateTime<Utc>, Uuid)>,
    ) -> Result<Vec<IntentRecord>, StoreError> {
        let limit = limit.clamp(1, 500);
        let mut query = QueryBuilder::<Sqlite>::new("SELECT intents.* FROM intents WHERE 1=1");
        if let Some(state) = state {
            query.push(" AND state=").push_bind(state);
        }
        if let Some(condition_id) = condition_id {
            query
                .push(" AND json_extract(request_json, '$.condition_id')=")
                .push_bind(condition_id);
        }
        if let Some(created_after) = created_after {
            query.push(" AND created_at>=").push_bind(created_after);
        }
        if let Some((created_at, id)) = cursor {
            query
                .push(" AND (created_at>")
                .push_bind(created_at)
                .push(" OR (created_at=")
                .push_bind(created_at)
                .push(" AND id>")
                .push_bind(id)
                .push("))");
        }
        query
            .push(" ORDER BY created_at, id LIMIT ")
            .push_bind(i64::from(limit) + 1);
        Ok(query.build_query_as().fetch_all(&self.pool).await?)
    }

    pub async fn transition_intent(
        &self,
        id: Uuid,
        expected: IntentState,
        next: IntentState,
        rejection: Option<(&str, &str)>,
    ) -> Result<IntentRecord, StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let now = Utc::now();
        let result = sqlx::query(
            "UPDATE intents SET state = ?, rejection_code = ?, rejection_message = ?, updated_at = ? \
             WHERE id = ? AND state = ?",
        )
        .bind(next)
        .bind(rejection.map(|value| value.0))
        .bind(rejection.map(|value| value.1))
        .bind(now)
        .bind(id)
        .bind(expected)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(StoreError::NotFound);
        }
        let record = sqlx::query_as::<_, IntentRecord>("SELECT * FROM intents WHERE id = ?")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
        let payload = serde_json::to_string(&record)?;
        let event = append_event(
            &mut tx,
            self.mode,
            &format!("intent.{next:?}").to_ascii_lowercase(),
            "intent",
            &id.to_string(),
            &payload,
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(record)
    }

    pub async fn record_risk_decision(
        &self,
        intent_id: Uuid,
        approved: bool,
        reason_code: &str,
        observed: &serde_json::Value,
        policy_version: &str,
    ) -> Result<(), StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let decision_id = Uuid::now_v7();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO risk_decisions \
             (id, intent_id, approved, reason_code, observed_json, policy_version, created_at) \
             VALUES(?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(decision_id)
        .bind(intent_id)
        .bind(approved)
        .bind(reason_code)
        .bind(observed.to_string())
        .bind(policy_version)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let event = append_event(
            &mut tx,
            self.mode,
            "risk.decision",
            "risk_decision",
            &decision_id.to_string(),
            &serde_json::json!({
                "id": decision_id,
                "intent_id": intent_id,
                "approved": approved,
                "reason_code": reason_code,
                "observed": observed,
                "policy_version": policy_version,
                "mode": self.mode,
                "created_at": now
            })
            .to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(())
    }

    pub async fn risk_snapshot(
        &self,
        condition_id: &str,
        token_id: &str,
    ) -> Result<RiskSnapshot, StoreError> {
        let _risk_guard = self.lock_risk().await;
        self.risk_snapshot_locked(condition_id, token_id).await
    }

    pub(crate) async fn risk_snapshot_locked(
        &self,
        condition_id: &str,
        token_id: &str,
    ) -> Result<RiskSnapshot, StoreError> {
        let orders = sqlx::query_as::<_, RiskExposureRow>(
            "SELECT condition_id, token_id, side, fee_rate, quote_exposure, \
                    original_quantity, remaining_quantity AS quantity \
             FROM orders WHERE state IN \
             ('PREPARED','SUBMITTING','LIVE','PARTIALLY_FILLED','CANCEL_PENDING','UNKNOWN')",
        )
        .fetch_all(&self.pool)
        .await?;
        let reservations = sqlx::query_as::<_, RiskExposureRow>(
            "SELECT condition_id, token_id, side, fee_rate, quote_exposure, \
                    quantity AS original_quantity, quantity \
             FROM risk_reservations WHERE state='ACTIVE'",
        )
        .fetch_all(&self.pool)
        .await?;
        let positions =
            sqlx::query_as::<_, RiskPositionRow>("SELECT token_id, shares FROM positions")
                .fetch_all(&self.pool)
                .await?;
        let daily_rows = sqlx::query_as::<_, RiskTradeRow>(
            "SELECT price, size FROM trades \
             WHERE status IN ('MATCHED','MINED','CONFIRMED','RETRYING','FAILED') \
             AND created_at >= date('now')",
        )
        .fetch_all(&self.pool)
        .await?;
        build_risk_snapshot(
            condition_id,
            token_id,
            &orders,
            &reservations,
            positions,
            daily_rows,
        )
    }

    #[allow(clippy::too_many_lines)]
    pub async fn evaluate_and_reserve_risk(
        &self,
        intent_id: Uuid,
        request: &OrderIntentRequest,
        market: &MarketRules,
        book: &OrderBook,
        policy: &RiskPolicy,
    ) -> Result<RiskDecision, StoreError> {
        let _risk_guard = self.lock_risk().await;
        let _write_guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let snapshot =
            risk_snapshot_in_transaction(&mut tx, &request.condition_id, &request.token_id).await?;
        let decision = policy.evaluate(request, market, book, &snapshot);
        let now = Utc::now();
        let decision_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO risk_decisions \
             (id, intent_id, approved, reason_code, observed_json, policy_version, created_at) \
             VALUES(?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(decision_id)
        .bind(intent_id)
        .bind(decision.approved)
        .bind(decision.reason_code)
        .bind(decision.observed.to_string())
        .bind(&decision.policy_version)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let mut events = vec![
            append_event(
                &mut tx,
                self.mode,
                "risk.decision",
                "risk_decision",
                &decision_id.to_string(),
                &serde_json::json!({
                    "id": decision_id,
                    "intent_id": intent_id,
                    "approved": decision.approved,
                    "reason_code": decision.reason_code,
                    "observed": decision.observed,
                    "policy_version": decision.policy_version,
                    "mode": self.mode,
                    "created_at": now
                })
                .to_string(),
            )
            .await?,
        ];
        let next_state = if decision.approved {
            let price = request
                .protection_price()
                .map_err(|error| StoreError::InvalidDecimal(error.to_string()))?;
            let quantity = request
                .quantity_decimal()
                .map_err(|error| StoreError::InvalidDecimal(error.to_string()))?;
            let (shares, quote_exposure) = conservative_exposure(request, market, quantity, price)
                .ok_or_else(|| {
                    StoreError::IdentityMismatch(
                        "approved risk decision has an invalid market tick size".into(),
                    )
                })?;
            let fee_rate = market.maker_fee_rate.max(market.taker_fee_rate);
            sqlx::query(
                "INSERT INTO risk_reservations \
                 (intent_id, condition_id, token_id, side, price, quantity, quote_exposure, \
                  fee_rate, state, created_at, updated_at) \
                 VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)",
            )
            .bind(intent_id)
            .bind(&request.condition_id)
            .bind(&request.token_id)
            .bind(request.side)
            .bind(price.normalize().to_string())
            .bind(shares.normalize().to_string())
            .bind(quote_exposure.normalize().to_string())
            .bind(fee_rate.normalize().to_string())
            .bind(now)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            events.push(
                append_event(
                    &mut tx,
                    self.mode,
                    "risk.reserved",
                    "risk_reservation",
                    &intent_id.to_string(),
                    &serde_json::json!({
                        "intent_id": intent_id,
                        "condition_id": request.condition_id,
                        "token_id": request.token_id,
                        "side": request.side,
                        "price": price,
                        "quantity": shares,
                        "quote_exposure": quote_exposure,
                        "fee_rate": fee_rate,
                        "policy_version": policy.version,
                        "mode": self.mode
                    })
                    .to_string(),
                )
                .await?,
            );
            IntentState::Approved
        } else {
            IntentState::Rejected
        };
        let update = sqlx::query(
            "UPDATE intents SET state=?, rejection_code=?, rejection_message=?, \
             policy_version=?, updated_at=? WHERE id=? AND state='VALIDATING'",
        )
        .bind(next_state)
        .bind((!decision.approved).then_some(decision.reason_code))
        .bind((!decision.approved).then_some(decision.message.as_str()))
        .bind(&decision.policy_version)
        .bind(now)
        .bind(intent_id)
        .execute(&mut *tx)
        .await?;
        if update.rows_affected() != 1 {
            return Err(StoreError::IdentityMismatch(
                "intent was not VALIDATING during risk reservation".into(),
            ));
        }
        events.push(
            append_event(
                &mut tx,
                self.mode,
                &format!("intent.{next_state:?}").to_ascii_lowercase(),
                "intent",
                &intent_id.to_string(),
                &serde_json::json!({
                    "state": next_state,
                    "reason_code": (!decision.approved).then_some(decision.reason_code),
                    "policy_version": decision.policy_version
                })
                .to_string(),
            )
            .await?,
        );
        tx.commit().await?;
        for event in events {
            let _ = self.events.send(event);
        }
        Ok(decision)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn create_prepared_order(
        &self,
        intent: &IntentRecord,
        request: &OrderIntentRequest,
        normalized_venue_json: &str,
        deterministic_order_id: &str,
        signed_payload_json: Option<&str>,
        signer_address: Option<&str>,
        funder_address: Option<&str>,
        protocol_version: u32,
        sdk_version: &str,
        policy_version: &str,
        fee_rate: Decimal,
    ) -> Result<OrderRecord, StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let now = Utc::now();
        let prepared_id = Uuid::now_v7();
        let normalized_json: serde_json::Value = serde_json::from_str(normalized_venue_json)?;
        let normalized_json = serde_jcs::to_string(&normalized_json)?;
        let payload_sha256 =
            signed_payload_json.map(|value| hex::encode(Sha256::digest(value.as_bytes())));
        sqlx::query(
            "INSERT INTO prepared_orders \
             (id, intent_id, normalized_json, signed_payload_json, payload_sha256, deterministic_order_id, \
              signer_address, funder_address, protocol_version, sdk_version, policy_version, created_at) \
             VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(prepared_id)
        .bind(intent.id)
        .bind(&normalized_json)
        .bind(signed_payload_json)
        .bind(payload_sha256)
        .bind(deterministic_order_id)
        .bind(signer_address)
        .bind(funder_address)
        .bind(i64::from(protocol_version))
        .bind(sdk_version)
        .bind(policy_version)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO submission_attempts \
             (id, prepared_order_id, attempt_number, state, created_at, updated_at) \
             VALUES(?, ?, 1, 'PREPARED', ?, ?)",
        )
        .bind(Uuid::now_v7())
        .bind(prepared_id)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        let price = request
            .protection_price()
            .map_err(|error| StoreError::InvalidDecimal(error.to_string()))?;
        let reservation = sqlx::query(
            "SELECT quantity, quote_exposure FROM risk_reservations \
             WHERE intent_id=? AND state='ACTIVE'",
        )
        .bind(intent.id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| {
            StoreError::IdentityMismatch(
                "prepared order does not have an active risk reservation".into(),
            )
        })?;
        let share_quantity = parse_stored_decimal(reservation.try_get::<String, _>("quantity")?)?;
        let quote_exposure =
            parse_stored_decimal(reservation.try_get::<String, _>("quote_exposure")?)?;
        let order = OrderRecord {
            id: Uuid::now_v7(),
            intent_id: intent.id,
            mode: self.mode,
            venue_order_id: None,
            condition_id: request.condition_id.clone(),
            token_id: request.token_id.clone(),
            side: request.side,
            time_in_force: request.time_in_force,
            price: price.normalize().to_string(),
            original_quantity: share_quantity.normalize().to_string(),
            remaining_quantity: share_quantity.normalize().to_string(),
            state: OrderState::Prepared,
            created_at: now,
            updated_at: now,
        };
        insert_order(&mut tx, &order, fee_rate, quote_exposure).await?;
        let reservation_update = sqlx::query(
            "UPDATE risk_reservations SET state='CONSUMED', updated_at=? \
             WHERE intent_id=? AND state='ACTIVE'",
        )
        .bind(now)
        .bind(intent.id)
        .execute(&mut *tx)
        .await?;
        if reservation_update.rows_affected() != 1 {
            return Err(StoreError::IdentityMismatch(
                "prepared order does not have exactly one active risk reservation".into(),
            ));
        }
        let intent_update = sqlx::query(
            "UPDATE intents SET state = 'PREPARED', updated_at = ? \
             WHERE id = ? AND state = 'PREPARING'",
        )
        .bind(now)
        .bind(intent.id)
        .execute(&mut *tx)
        .await?;
        if intent_update.rows_affected() != 1 {
            return Err(StoreError::IdentityMismatch(
                "intent was not PREPARING while committing prepared order".into(),
            ));
        }
        let prepared_intent =
            sqlx::query_as::<_, IntentRecord>("SELECT * FROM intents WHERE id = ?")
                .bind(intent.id)
                .fetch_one(&mut *tx)
                .await?;
        let intent_event = append_event(
            &mut tx,
            self.mode,
            "intent.prepared",
            "intent",
            &intent.id.to_string(),
            &serde_json::to_string(&prepared_intent)?,
        )
        .await?;
        let order_event = append_event(
            &mut tx,
            self.mode,
            "order.prepared",
            "order",
            &order.id.to_string(),
            &serde_json::to_string(&order)?,
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(intent_event);
        let _ = self.events.send(order_event);
        Ok(order)
    }

    pub async fn reject_preparation(
        &self,
        intent_id: Uuid,
        code: &str,
        message: &str,
    ) -> Result<(), StoreError> {
        let _risk_guard = self.lock_risk().await;
        let _write_guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let now = Utc::now();
        let reservation = sqlx::query(
            "UPDATE risk_reservations SET state='RELEASED', updated_at=? \
             WHERE intent_id=? AND state='ACTIVE'",
        )
        .bind(now)
        .bind(intent_id)
        .execute(&mut *tx)
        .await?;
        if reservation.rows_affected() != 1 {
            return Err(StoreError::IdentityMismatch(
                "preparation rejection does not have an active risk reservation".into(),
            ));
        }
        let intent = sqlx::query(
            "UPDATE intents SET state='REJECTED', rejection_code=?, rejection_message=?, \
             updated_at=? WHERE id=? AND state='PREPARING'",
        )
        .bind(code)
        .bind(message)
        .bind(now)
        .bind(intent_id)
        .execute(&mut *tx)
        .await?;
        if intent.rows_affected() != 1 {
            return Err(StoreError::IdentityMismatch(
                "intent was not PREPARING during reservation release".into(),
            ));
        }
        let reservation_event = append_event(
            &mut tx,
            self.mode,
            "risk.released",
            "risk_reservation",
            &intent_id.to_string(),
            &serde_json::json!({
                "intent_id": intent_id,
                "reason": code,
                "mode": self.mode
            })
            .to_string(),
        )
        .await?;
        let intent_event = append_event(
            &mut tx,
            self.mode,
            "intent.rejected",
            "intent",
            &intent_id.to_string(),
            &serde_json::json!({
                "state": "REJECTED",
                "reason_code": code,
                "message": message
            })
            .to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(reservation_event);
        let _ = self.events.send(intent_event);
        Ok(())
    }

    pub async fn begin_submission(
        &self,
        intent_id: Uuid,
        order_id: Uuid,
    ) -> Result<(), StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let service_state: String =
            sqlx::query_scalar("SELECT state FROM control_state WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await?;
        if service_state != "READY" {
            return Err(StoreError::NewRiskDisabled(service_state));
        }
        let now = Utc::now();
        let intent_update = sqlx::query(
            "UPDATE intents SET state = 'SUBMITTING', updated_at = ? \
             WHERE id = ? AND state = 'PREPARED'",
        )
        .bind(now)
        .bind(intent_id)
        .execute(&mut *tx)
        .await?;
        let order_update = sqlx::query(
            "UPDATE orders SET state = 'SUBMITTING', updated_at = ? \
             WHERE id = ? AND intent_id = ? AND state = 'PREPARED'",
        )
        .bind(now)
        .bind(order_id)
        .bind(intent_id)
        .execute(&mut *tx)
        .await?;
        if intent_update.rows_affected() != 1 || order_update.rows_affected() != 1 {
            return Err(StoreError::IdentityMismatch(
                "prepared intent/order pair changed before submission".into(),
            ));
        }
        let attempt_update = sqlx::query(
            "UPDATE submission_attempts \
             SET state = 'SUBMITTING', request_started_at = ?, updated_at = ? \
             WHERE prepared_order_id = (
                 SELECT id FROM prepared_orders WHERE intent_id = ?
             ) AND state = 'PREPARED'",
        )
        .bind(now)
        .bind(now)
        .bind(intent_id)
        .execute(&mut *tx)
        .await?;
        if attempt_update.rows_affected() != 1 {
            return Err(StoreError::IdentityMismatch(
                "submission attempt was not PREPARED".into(),
            ));
        }
        sqlx::query(
            "INSERT INTO order_transitions \
             (order_id, prior_state, new_state, evidence_json, created_at) \
             VALUES(?, 'PREPARED', 'SUBMITTING', '{}', ?)",
        )
        .bind(order_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let intent_event = append_event(
            &mut tx,
            self.mode,
            "intent.submitting",
            "intent",
            &intent_id.to_string(),
            &serde_json::json!({"state": "SUBMITTING"}).to_string(),
        )
        .await?;
        let order_event = append_event(
            &mut tx,
            self.mode,
            "order.submitting",
            "order",
            &order_id.to_string(),
            &serde_json::json!({"state": "SUBMITTING"}).to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(intent_event);
        let _ = self.events.send(order_event);
        Ok(())
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn finish_submission(
        &self,
        intent_id: Uuid,
        order_id: Uuid,
        intent_state: IntentState,
        order_state: OrderState,
        venue_order_id: Option<&str>,
        response_class: &str,
        filled_quantity: Decimal,
        filled_price: Option<Decimal>,
        venue_trade_ids: &[String],
        evidence: &serde_json::Value,
    ) -> Result<(), StoreError> {
        if !matches!(intent_state, IntentState::Submitted | IntentState::Unknown) {
            return Err(StoreError::IdentityMismatch(
                "invalid terminal submission intent state".into(),
            ));
        }
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let now = Utc::now();
        let prior_order = sqlx::query_as::<_, OrderRecord>(
            "SELECT * FROM orders WHERE id=? AND intent_id=? AND state='SUBMITTING'",
        )
        .bind(order_id)
        .bind(intent_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| {
            StoreError::IdentityMismatch("submitting order changed before response commit".into())
        })?;
        let original_quantity = parse_stored_decimal(prior_order.original_quantity.clone())?;
        let remaining_quantity = if matches!(
            order_state,
            OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
        ) {
            Decimal::ZERO
        } else {
            (original_quantity - filled_quantity).max(Decimal::ZERO)
        };
        validate_venue_order_evidence(
            original_quantity,
            order_state,
            remaining_quantity,
            filled_quantity,
            filled_price,
        )?;
        let intent_update = sqlx::query(
            "UPDATE intents SET state = ?, updated_at = ? \
             WHERE id = ? AND state = 'SUBMITTING'",
        )
        .bind(intent_state)
        .bind(now)
        .bind(intent_id)
        .execute(&mut *tx)
        .await?;
        let order_update = sqlx::query(
            "UPDATE orders SET state = ?, venue_order_id = COALESCE(?, venue_order_id), \
             remaining_quantity = ?, updated_at = ? \
             WHERE id = ? AND intent_id = ? AND state = 'SUBMITTING'",
        )
        .bind(order_state)
        .bind(venue_order_id)
        .bind(remaining_quantity.normalize().to_string())
        .bind(now)
        .bind(order_id)
        .bind(intent_id)
        .execute(&mut *tx)
        .await?;
        let trade_event = if intent_state == IntentState::Submitted {
            apply_matched_fill(
                &mut tx,
                self.mode,
                &prior_order,
                venue_order_id,
                filled_quantity,
                filled_price,
                venue_trade_ids,
                now,
            )
            .await?
        } else {
            None
        };
        if intent_update.rows_affected() != 1 || order_update.rows_affected() != 1 {
            return Err(StoreError::IdentityMismatch(
                "submitting intent/order pair changed before response commit".into(),
            ));
        }
        let attempt_state = if intent_state == IntentState::Unknown {
            "UNKNOWN"
        } else if order_state == OrderState::Rejected {
            "REJECTED"
        } else {
            "ACCEPTED"
        };
        sqlx::query(
            "UPDATE submission_attempts SET state = ?, response_class = ?, response_json = ?, \
             updated_at = ? WHERE prepared_order_id = (
                 SELECT id FROM prepared_orders WHERE intent_id = ?
             ) AND state = 'SUBMITTING'",
        )
        .bind(attempt_state)
        .bind(response_class)
        .bind(evidence.to_string())
        .bind(now)
        .bind(intent_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO order_transitions \
             (order_id, prior_state, new_state, evidence_json, created_at) \
             VALUES(?, 'SUBMITTING', ?, ?, ?)",
        )
        .bind(order_id)
        .bind(order_state)
        .bind(evidence.to_string())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let intent_event = append_event(
            &mut tx,
            self.mode,
            &format!("intent.{intent_state:?}").to_ascii_lowercase(),
            "intent",
            &intent_id.to_string(),
            &serde_json::json!({
                "state": intent_state,
                "response_class": response_class,
            })
            .to_string(),
        )
        .await?;
        let order_event = append_event(
            &mut tx,
            self.mode,
            &format!("order.{order_state:?}").to_ascii_lowercase(),
            "order",
            &order_id.to_string(),
            &serde_json::json!({
                "state": order_state,
                "venue_order_id": venue_order_id,
                "evidence": evidence,
            })
            .to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(intent_event);
        let _ = self.events.send(order_event);
        if let Some(event) = trade_event {
            let _ = self.events.send(event);
        }
        Ok(())
    }

    pub async fn recover_interrupted_submissions(&self) -> Result<u64, StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let interrupted = sqlx::query(
            "SELECT orders.id AS order_id, orders.intent_id AS intent_id, \
                    prepared_orders.deterministic_order_id \
             FROM orders \
             JOIN intents ON intents.id = orders.intent_id \
             JOIN prepared_orders ON prepared_orders.intent_id=orders.intent_id \
             WHERE orders.state = 'SUBMITTING' OR intents.state = 'SUBMITTING'",
        )
        .fetch_all(&mut *tx)
        .await?;
        let now = Utc::now();
        let mut events = Vec::with_capacity(interrupted.len() * 2);
        for row in &interrupted {
            let order_id: Uuid = row.try_get("order_id")?;
            let intent_id: Uuid = row.try_get("intent_id")?;
            let deterministic_order_id: String = row.try_get("deterministic_order_id")?;
            sqlx::query(
                "UPDATE orders SET state='UNKNOWN', \
                 venue_order_id=COALESCE(venue_order_id, ?), updated_at=? WHERE id=?",
            )
            .bind(deterministic_order_id)
            .bind(now)
            .bind(order_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query("UPDATE intents SET state='UNKNOWN', updated_at=? WHERE id=?")
                .bind(now)
                .bind(intent_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "UPDATE submission_attempts SET state='UNKNOWN', \
                 response_class='PROCESS_INTERRUPTED', updated_at=? \
                 WHERE prepared_order_id=(
                     SELECT id FROM prepared_orders WHERE intent_id=?
                 ) AND state='SUBMITTING'",
            )
            .bind(now)
            .bind(intent_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO order_transitions \
                 (order_id, prior_state, new_state, evidence_json, created_at) \
                 VALUES(?, 'SUBMITTING', 'UNKNOWN', ?, ?)",
            )
            .bind(order_id)
            .bind(r#"{"reason":"process_interrupted_after_submitting_commit"}"#)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            events.push(
                append_event(
                    &mut tx,
                    self.mode,
                    "intent.unknown",
                    "intent",
                    &intent_id.to_string(),
                    r#"{"reason":"process_interrupted_after_submitting_commit"}"#,
                )
                .await?,
            );
            events.push(
                append_event(
                    &mut tx,
                    self.mode,
                    "order.unknown",
                    "order",
                    &order_id.to_string(),
                    r#"{"reason":"process_interrupted_after_submitting_commit"}"#,
                )
                .await?,
            );
        }
        tx.commit().await?;
        for event in events {
            let _ = self.events.send(event);
        }
        u64::try_from(interrupted.len())
            .map_err(|_| StoreError::IdentityMismatch("interrupted order count overflow".into()))
    }

    /// Restores purely local, pre-submission work after a process crash.
    ///
    /// Anything before PREPARED can be evaluated again because no venue bytes
    /// have been sent. PREPARED work keeps its exact persisted payload and is
    /// returned without rebuilding or re-signing it.
    pub async fn recover_pre_submission_work(&self) -> Result<Vec<Uuid>, StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(
            "SELECT id, state FROM intents \
             WHERE state IN ('RECEIVED','VALIDATING','APPROVED','PREPARING','PREPARED') \
             ORDER BY created_at, id",
        )
        .fetch_all(&mut *tx)
        .await?;
        let now = Utc::now();
        let mut events = Vec::new();
        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            let id: Uuid = row.try_get("id")?;
            let state: String = row.try_get("state")?;
            if state == "PREPARED" {
                let count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM orders o \
                     JOIN prepared_orders p ON p.intent_id=o.intent_id \
                     JOIN submission_attempts a ON a.prepared_order_id=p.id \
                     WHERE o.intent_id=? AND o.state='PREPARED' AND a.state='PREPARED'",
                )
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
                if count != 1 {
                    return Err(StoreError::IdentityMismatch(format!(
                        "prepared intent {id} does not have exactly one prepared order and attempt"
                    )));
                }
            } else if state != "RECEIVED" {
                let released = sqlx::query(
                    "UPDATE risk_reservations SET state='RELEASED', updated_at=? \
                     WHERE intent_id=? AND state='ACTIVE'",
                )
                .bind(now)
                .bind(id)
                .execute(&mut *tx)
                .await?;
                if released.rows_affected() > 0 {
                    events.push(
                        append_event(
                            &mut tx,
                            self.mode,
                            "risk.released",
                            "risk_reservation",
                            &id.to_string(),
                            r#"{"reason":"process_interrupted_before_prepared_order"}"#,
                        )
                        .await?,
                    );
                }
                sqlx::query(
                    "UPDATE intents SET state='RECEIVED', updated_at=? WHERE id=? AND state=?",
                )
                .bind(now)
                .bind(id)
                .bind(&state)
                .execute(&mut *tx)
                .await?;
                events.push(
                    append_event(
                        &mut tx,
                        self.mode,
                        "intent.recovered_before_submission",
                        "intent",
                        &id.to_string(),
                        &serde_json::json!({
                            "prior_state": state,
                            "new_state": "RECEIVED",
                            "reason": "no venue request had begun"
                        })
                        .to_string(),
                    )
                    .await?,
                );
            }
            ids.push(id);
        }
        tx.commit().await?;
        for event in events {
            let _ = self.events.send(event);
        }
        Ok(ids)
    }

    pub async fn prepared_submission(
        &self,
        intent_id: Uuid,
    ) -> Result<PreparedSubmission, StoreError> {
        let row = sqlx::query(
            "SELECT o.id AS order_id, p.deterministic_order_id, p.normalized_json, \
                    p.signed_payload_json, p.signer_address, p.funder_address, \
                    p.protocol_version, p.sdk_version \
             FROM orders o \
             JOIN prepared_orders p ON p.intent_id=o.intent_id \
             JOIN submission_attempts a ON a.prepared_order_id=p.id \
             WHERE o.intent_id=? AND o.state='PREPARED' AND a.state='PREPARED'",
        )
        .bind(intent_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(StoreError::NotFound)?;
        let protocol_version: i64 = row.try_get("protocol_version")?;
        Ok(PreparedSubmission {
            order_id: row.try_get("order_id")?,
            deterministic_order_id: row.try_get("deterministic_order_id")?,
            normalized_json: row.try_get("normalized_json")?,
            signed_payload_json: row.try_get("signed_payload_json")?,
            signer_address: row.try_get("signer_address")?,
            funder_address: row.try_get("funder_address")?,
            protocol_version: u32::try_from(protocol_version).map_err(|_| {
                StoreError::IdentityMismatch("stored protocol version is outside u32".into())
            })?,
            sdk_version: row.try_get("sdk_version")?,
        })
    }

    pub async fn unknown_submissions(&self) -> Result<Vec<UnknownSubmission>, StoreError> {
        let rows = sqlx::query(
            "SELECT orders.id AS order_id, orders.intent_id AS intent_id, \
             prepared_orders.deterministic_order_id \
             FROM orders \
             JOIN prepared_orders ON prepared_orders.intent_id = orders.intent_id \
             WHERE orders.state = 'UNKNOWN'",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(UnknownSubmission {
                    order_id: row.try_get("order_id")?,
                    intent_id: row.try_get("intent_id")?,
                    deterministic_order_id: row.try_get("deterministic_order_id")?,
                })
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn resolve_unknown_submission(
        &self,
        unknown: &UnknownSubmission,
        venue_order_id: &str,
        state: OrderState,
        remaining_quantity: Decimal,
        filled_quantity: Decimal,
        filled_price: Option<Decimal>,
        venue_trade_ids: &[String],
        evidence: &serde_json::Value,
    ) -> Result<(), StoreError> {
        if state == OrderState::Unknown {
            return Ok(());
        }
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let now = Utc::now();
        let prior_order =
            sqlx::query_as::<_, OrderRecord>("SELECT * FROM orders WHERE id=? AND state='UNKNOWN'")
                .bind(unknown.order_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| {
                    StoreError::IdentityMismatch(
                        "unknown order changed during reconciliation".into(),
                    )
                })?;
        let original = parse_stored_decimal(prior_order.original_quantity.clone())?;
        validate_venue_order_evidence(
            original,
            state,
            remaining_quantity,
            filled_quantity,
            filled_price,
        )?;
        let order_update = sqlx::query(
            "UPDATE orders SET state=?, venue_order_id=?, remaining_quantity=?, updated_at=? \
             WHERE id=? AND state='UNKNOWN'",
        )
        .bind(state)
        .bind(venue_order_id)
        .bind(remaining_quantity.normalize().to_string())
        .bind(now)
        .bind(unknown.order_id)
        .execute(&mut *tx)
        .await?;
        if order_update.rows_affected() != 1 {
            return Err(StoreError::IdentityMismatch(
                "unknown order changed during reconciliation".into(),
            ));
        }
        sqlx::query(
            "UPDATE intents SET state='SUBMITTED', updated_at=? \
             WHERE id=? AND state='UNKNOWN'",
        )
        .bind(now)
        .bind(unknown.intent_id)
        .execute(&mut *tx)
        .await?;
        let trade_event = apply_matched_fill(
            &mut tx,
            self.mode,
            &prior_order,
            Some(venue_order_id),
            filled_quantity,
            filled_price,
            venue_trade_ids,
            now,
        )
        .await?;
        sqlx::query(
            "UPDATE submission_attempts SET state='RESOLVED', response_class='RECONCILED', \
             response_json=?, updated_at=? WHERE prepared_order_id=(
                 SELECT id FROM prepared_orders WHERE intent_id=?
             ) AND state='UNKNOWN'",
        )
        .bind(evidence.to_string())
        .bind(now)
        .bind(unknown.intent_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO order_transitions \
             (order_id, prior_state, new_state, evidence_json, created_at) \
             VALUES(?, 'UNKNOWN', ?, ?, ?)",
        )
        .bind(unknown.order_id)
        .bind(state)
        .bind(evidence.to_string())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let order_event = append_event(
            &mut tx,
            self.mode,
            &format!("order.{state:?}").to_ascii_lowercase(),
            "order",
            &unknown.order_id.to_string(),
            &serde_json::json!({
                "state": state,
                "venue_order_id": venue_order_id,
                "reconciled_from": "UNKNOWN",
                "evidence": evidence,
            })
            .to_string(),
        )
        .await?;
        let intent_event = append_event(
            &mut tx,
            self.mode,
            "intent.submitted",
            "intent",
            &unknown.intent_id.to_string(),
            &serde_json::json!({"state": "SUBMITTED", "reconciled_from": "UNKNOWN"}).to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(order_event);
        let _ = self.events.send(intent_event);
        if let Some(event) = trade_event {
            let _ = self.events.send(event);
        }
        Ok(())
    }

    pub async fn transition_order(
        &self,
        id: Uuid,
        next: OrderState,
        venue_order_id: Option<&str>,
        evidence: &serde_json::Value,
    ) -> Result<OrderRecord, StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let prior_row = sqlx::query("SELECT state, intent_id FROM orders WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(StoreError::NotFound)?;
        let prior: OrderState = prior_row.try_get("state")?;
        let intent_id: Uuid = prior_row.try_get("intent_id")?;
        if next == OrderState::CancelPending
            && matches!(
                prior,
                OrderState::Filled
                    | OrderState::Cancelled
                    | OrderState::Expired
                    | OrderState::Rejected
            )
        {
            let order = sqlx::query_as::<_, OrderRecord>("SELECT * FROM orders WHERE id=?")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(order);
        }
        if !valid_order_transition(prior, next) {
            return Err(StoreError::IdentityMismatch(format!(
                "invalid order transition {prior:?} -> {next:?}"
            )));
        }
        let now = Utc::now();
        sqlx::query(
            "UPDATE orders SET state = ?, venue_order_id = COALESCE(?, venue_order_id), updated_at = ? \
             WHERE id = ?",
        )
        .bind(next)
        .bind(venue_order_id)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?;
        if prior == OrderState::Unknown
            && matches!(
                next,
                OrderState::Filled | OrderState::Cancelled | OrderState::Expired
            )
        {
            sqlx::query(
                "UPDATE intents SET state='SUBMITTED', updated_at=? \
                 WHERE id=? AND state='UNKNOWN'",
            )
            .bind(now)
            .bind(intent_id)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "INSERT INTO order_transitions(order_id, prior_state, new_state, evidence_json, created_at) \
             VALUES(?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(prior)
        .bind(next)
        .bind(evidence.to_string())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let order = sqlx::query_as::<_, OrderRecord>("SELECT * FROM orders WHERE id = ?")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
        let event = append_event(
            &mut tx,
            self.mode,
            &format!("order.{next:?}").to_ascii_lowercase(),
            "order",
            &id.to_string(),
            &serde_json::to_string(&order)?,
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(order)
    }

    pub async fn cancel_prepared_order(
        &self,
        order_id: Uuid,
        evidence: &serde_json::Value,
    ) -> Result<OrderRecord, StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let order = sqlx::query_as::<_, OrderRecord>(
            "SELECT * FROM orders WHERE id=? AND state='PREPARED'",
        )
        .bind(order_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::NotFound)?;
        let now = Utc::now();
        sqlx::query("UPDATE orders SET state='CANCELLED', updated_at=? WHERE id=?")
            .bind(now)
            .bind(order_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE intents SET state='REJECTED', rejection_code='CANCELLED_BEFORE_SUBMISSION', \
             rejection_message='cancelled before venue submission', updated_at=? \
             WHERE id=? AND state='PREPARED'",
        )
        .bind(now)
        .bind(order.intent_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE submission_attempts SET state='CANCELLED', \
             response_class='CANCELLED_BEFORE_SUBMISSION', response_json=?, updated_at=? \
             WHERE prepared_order_id=(SELECT id FROM prepared_orders WHERE intent_id=?) \
             AND state='PREPARED'",
        )
        .bind(evidence.to_string())
        .bind(now)
        .bind(order.intent_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO order_transitions \
             (order_id, prior_state, new_state, evidence_json, created_at) \
             VALUES(?, 'PREPARED', 'CANCELLED', ?, ?)",
        )
        .bind(order_id)
        .bind(evidence.to_string())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let cancelled = sqlx::query_as::<_, OrderRecord>("SELECT * FROM orders WHERE id=?")
            .bind(order_id)
            .fetch_one(&mut *tx)
            .await?;
        let order_event = append_event(
            &mut tx,
            self.mode,
            "order.cancelled",
            "order",
            &order_id.to_string(),
            &serde_json::to_string(&cancelled)?,
        )
        .await?;
        let intent_event = append_event(
            &mut tx,
            self.mode,
            "intent.rejected",
            "intent",
            &order.intent_id.to_string(),
            &serde_json::json!({
                "state": "REJECTED",
                "reason_code": "CANCELLED_BEFORE_SUBMISSION",
                "order_id": order_id
            })
            .to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(order_event);
        let _ = self.events.send(intent_event);
        Ok(cancelled)
    }

    pub async fn get_order(&self, id: Uuid) -> Result<OrderRecord, StoreError> {
        sqlx::query_as("SELECT * FROM orders WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::NotFound)
    }

    pub async fn reconciliation_orders(
        &self,
    ) -> Result<(Vec<OrderRecord>, std::collections::HashSet<String>), StoreError> {
        let known: Vec<OrderRecord> =
            sqlx::query_as("SELECT * FROM orders WHERE venue_order_id IS NOT NULL")
                .fetch_all(&self.pool)
                .await?;
        let mut known_ids: std::collections::HashSet<String> = known
            .iter()
            .filter_map(|order| order.venue_order_id.clone())
            .collect();
        known_ids.extend(
            sqlx::query_scalar::<_, String>("SELECT deterministic_order_id FROM prepared_orders")
                .fetch_all(&self.pool)
                .await?,
        );
        let active = known
            .into_iter()
            .filter(|order| {
                matches!(
                    order.state,
                    OrderState::Live | OrderState::PartiallyFilled | OrderState::CancelPending
                )
            })
            .collect();
        Ok((active, known_ids))
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn reconcile_order_projection(
        &self,
        order_id: Uuid,
        observed_state: OrderState,
        remaining_quantity: Decimal,
        total_filled_quantity: Decimal,
        filled_price: Option<Decimal>,
        venue_trade_ids: &[String],
        evidence: &serde_json::Value,
    ) -> Result<(), StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let order = sqlx::query_as::<_, OrderRecord>("SELECT * FROM orders WHERE id=?")
            .bind(order_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(StoreError::NotFound)?;
        let original = parse_stored_decimal(order.original_quantity.clone())?;
        validate_venue_order_evidence(
            original,
            observed_state,
            remaining_quantity,
            total_filled_quantity,
            filled_price,
        )?;
        let local_remaining = parse_stored_decimal(order.remaining_quantity.clone())?;
        let local_filled = (original - local_remaining).max(Decimal::ZERO);
        if total_filled_quantity < local_filled {
            return Err(StoreError::IdentityMismatch(format!(
                "venue fill quantity {total_filled_quantity} regressed below local {local_filled}"
            )));
        }
        let delta_fill = total_filled_quantity - local_filled;
        let next_state = if order.state == OrderState::CancelPending
            && matches!(
                observed_state,
                OrderState::Live | OrderState::PartiallyFilled
            ) {
            OrderState::CancelPending
        } else {
            observed_state
        };
        if next_state == order.state
            && remaining_quantity == local_remaining
            && delta_fill == Decimal::ZERO
        {
            tx.commit().await?;
            return Ok(());
        }
        let now = Utc::now();
        sqlx::query("UPDATE orders SET state=?, remaining_quantity=?, updated_at=? WHERE id=?")
            .bind(next_state)
            .bind(remaining_quantity.normalize().to_string())
            .bind(now)
            .bind(order_id)
            .execute(&mut *tx)
            .await?;
        let trade_event = apply_matched_fill(
            &mut tx,
            self.mode,
            &order,
            order.venue_order_id.as_deref(),
            delta_fill,
            filled_price,
            venue_trade_ids,
            now,
        )
        .await?;
        sqlx::query(
            "INSERT INTO order_transitions \
             (order_id, prior_state, new_state, evidence_json, created_at) \
             VALUES(?, ?, ?, ?, ?)",
        )
        .bind(order_id)
        .bind(order.state)
        .bind(next_state)
        .bind(evidence.to_string())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let order_event = append_event(
            &mut tx,
            self.mode,
            "order.reconciled",
            "order",
            &order_id.to_string(),
            &serde_json::json!({
                "prior_state": order.state,
                "new_state": next_state,
                "remaining_quantity": remaining_quantity,
                "delta_fill": delta_fill,
                "evidence": evidence
            })
            .to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(order_event);
        if let Some(event) = trade_event {
            let _ = self.events.send(event);
        }
        Ok(())
    }

    pub async fn order_ids_for_venue_ids(
        &self,
        venue_order_ids: &[String],
    ) -> Result<Vec<Uuid>, StoreError> {
        let mut ids = std::collections::BTreeSet::new();
        for venue_order_id in venue_order_ids {
            let order_id: Option<Uuid> =
                sqlx::query_scalar("SELECT id FROM orders WHERE venue_order_id=?")
                    .bind(venue_order_id)
                    .fetch_optional(&self.pool)
                    .await?;
            if let Some(order_id) = order_id {
                ids.insert(order_id);
            }
        }
        Ok(ids.into_iter().collect())
    }

    pub async fn record_venue_trade_finality(
        &self,
        order_id: Uuid,
        venue_trade_id: &str,
        status: TradeState,
        evidence: &serde_json::Value,
    ) -> Result<bool, StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let trades =
            sqlx::query_as::<_, TradeRecord>("SELECT * FROM trades WHERE order_id=? ORDER BY id")
                .bind(order_id)
                .fetch_all(&mut *tx)
                .await?;
        let Some(trade) = trades.into_iter().find(|trade| {
            trade_component_ids(&trade.venue_trade_id)
                .into_iter()
                .any(|component| component == venue_trade_id)
        }) else {
            tx.commit().await?;
            return Ok(false);
        };
        let prior_status: Option<TradeState> =
            sqlx::query_scalar("SELECT status FROM venue_trade_finality WHERE venue_trade_id=?")
                .bind(venue_trade_id)
                .fetch_optional(&mut *tx)
                .await?;
        if prior_status == Some(status) {
            tx.commit().await?;
            return Ok(true);
        }
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO venue_trade_finality \
             (venue_trade_id, trade_id, status, evidence_json, updated_at) \
             VALUES(?, ?, ?, ?, ?) \
             ON CONFLICT(venue_trade_id) DO UPDATE SET trade_id=excluded.trade_id, \
             status=excluded.status, evidence_json=excluded.evidence_json, \
             updated_at=excluded.updated_at",
        )
        .bind(venue_trade_id)
        .bind(trade.id)
        .bind(status)
        .bind(evidence.to_string())
        .bind(now)
        .execute(&mut *tx)
        .await?;

        let component_ids = trade_component_ids(&trade.venue_trade_id);
        let mut component_statuses = Vec::with_capacity(component_ids.len());
        for component_id in &component_ids {
            let component_status: Option<TradeState> = sqlx::query_scalar(
                "SELECT status FROM venue_trade_finality WHERE venue_trade_id=?",
            )
            .bind(component_id)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(component_status) = component_status {
                component_statuses.push(component_status);
            }
        }
        let aggregate_status = if component_statuses.len() == component_ids.len() {
            aggregate_trade_status(&component_statuses)
        } else {
            trade.status
        };
        if aggregate_status != trade.status {
            sqlx::query("UPDATE trades SET status=?, updated_at=? WHERE id=?")
                .bind(aggregate_status)
                .bind(now)
                .bind(trade.id)
                .execute(&mut *tx)
                .await?;
        }
        let event = append_event(
            &mut tx,
            self.mode,
            "trade.finality_observed",
            "trade",
            &trade.id.to_string(),
            &serde_json::json!({
                "venue_trade_id": venue_trade_id,
                "prior_status": prior_status,
                "status": status,
                "aggregate_status": aggregate_status,
                "evidence": evidence
            })
            .to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(true)
    }

    pub async fn reconcile_venue_positions(
        &self,
        observed: &[(String, String, Decimal)],
    ) -> Result<(), StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let current_rows = sqlx::query("SELECT token_id, condition_id, shares FROM positions")
            .fetch_all(&mut *tx)
            .await?;
        let mut current = std::collections::BTreeMap::new();
        for row in current_rows {
            current.insert(
                row.try_get::<String, _>("token_id")?,
                (
                    row.try_get::<String, _>("condition_id")?,
                    parse_stored_decimal(row.try_get("shares")?)?,
                ),
            );
        }
        let mut observed_tokens = std::collections::BTreeSet::new();
        let mut events = Vec::new();
        for (condition_id, token_id, shares) in observed {
            if condition_id.is_empty() || token_id.is_empty() || *shares < Decimal::ZERO {
                return Err(StoreError::IdentityMismatch(
                    "venue returned an invalid position".into(),
                ));
            }
            if !observed_tokens.insert(token_id.clone()) {
                return Err(StoreError::IdentityMismatch(format!(
                    "venue returned duplicate position token {token_id}"
                )));
            }
            let prior = current.get(token_id).map(|(_, shares)| *shares);
            if prior == Some(*shares) {
                continue;
            }
            let now = Utc::now();
            sqlx::query(
                "INSERT INTO positions(token_id, condition_id, mode, shares, updated_at) \
                 VALUES(?, ?, ?, ?, ?) \
                 ON CONFLICT(token_id) DO UPDATE SET condition_id=excluded.condition_id, \
                 mode=excluded.mode, shares=excluded.shares, updated_at=excluded.updated_at",
            )
            .bind(token_id)
            .bind(condition_id)
            .bind(self.mode)
            .bind(shares.normalize().to_string())
            .bind(now)
            .execute(&mut *tx)
            .await?;
            events.push(
                append_event(
                    &mut tx,
                    self.mode,
                    "position.reconciled",
                    "position",
                    token_id,
                    &serde_json::json!({
                        "condition_id": condition_id,
                        "token_id": token_id,
                        "prior_shares": prior,
                        "shares": shares
                    })
                    .to_string(),
                )
                .await?,
            );
        }
        for (token_id, (condition_id, prior_shares)) in current {
            if observed_tokens.contains(&token_id) || prior_shares == Decimal::ZERO {
                continue;
            }
            let now = Utc::now();
            sqlx::query("UPDATE positions SET shares='0', updated_at=? WHERE token_id=?")
                .bind(now)
                .bind(&token_id)
                .execute(&mut *tx)
                .await?;
            events.push(
                append_event(
                    &mut tx,
                    self.mode,
                    "position.reconciled",
                    "position",
                    &token_id,
                    &serde_json::json!({
                        "condition_id": condition_id,
                        "token_id": token_id,
                        "prior_shares": prior_shares,
                        "shares": Decimal::ZERO
                    })
                    .to_string(),
                )
                .await?,
            );
        }
        tx.commit().await?;
        for event in events {
            let _ = self.events.send(event);
        }
        Ok(())
    }

    pub async fn list_orders(&self, limit: u32) -> Result<Vec<OrderRecord>, StoreError> {
        Ok(
            sqlx::query_as("SELECT * FROM orders ORDER BY created_at, id LIMIT ?")
                .bind(i64::from(limit.min(500)))
                .fetch_all(&self.pool)
                .await?,
        )
    }

    pub async fn list_orders_page(
        &self,
        limit: u32,
        state: Option<OrderState>,
        condition_id: Option<&str>,
        token_id: Option<&str>,
        cursor: Option<(DateTime<Utc>, Uuid)>,
    ) -> Result<Vec<OrderRecord>, StoreError> {
        let limit = limit.clamp(1, 500);
        let mut query = QueryBuilder::<Sqlite>::new("SELECT * FROM orders WHERE 1=1");
        if let Some(state) = state {
            query.push(" AND state=").push_bind(state);
        }
        if let Some(condition_id) = condition_id {
            query.push(" AND condition_id=").push_bind(condition_id);
        }
        if let Some(token_id) = token_id {
            query.push(" AND token_id=").push_bind(token_id);
        }
        if let Some((created_at, id)) = cursor {
            query
                .push(" AND (created_at>")
                .push_bind(created_at)
                .push(" OR (created_at=")
                .push_bind(created_at)
                .push(" AND id>")
                .push_bind(id)
                .push("))");
        }
        query
            .push(" ORDER BY created_at, id LIMIT ")
            .push_bind(i64::from(limit) + 1);
        Ok(query.build_query_as().fetch_all(&self.pool).await?)
    }

    pub async fn list_trades(&self, limit: u32) -> Result<Vec<TradeRecord>, StoreError> {
        Ok(
            sqlx::query_as("SELECT * FROM trades ORDER BY created_at, id LIMIT ?")
                .bind(i64::from(limit.min(500)))
                .fetch_all(&self.pool)
                .await?,
        )
    }

    pub async fn list_trades_page(
        &self,
        limit: u32,
        status: Option<TradeState>,
        condition_id: Option<&str>,
        token_id: Option<&str>,
        cursor: Option<(DateTime<Utc>, Uuid)>,
    ) -> Result<Vec<TradeRecord>, StoreError> {
        let limit = limit.clamp(1, 500);
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT trades.* FROM trades JOIN orders ON orders.id=trades.order_id WHERE 1=1",
        );
        if let Some(status) = status {
            query.push(" AND trades.status=").push_bind(status);
        }
        if let Some(condition_id) = condition_id {
            query
                .push(" AND orders.condition_id=")
                .push_bind(condition_id);
        }
        if let Some(token_id) = token_id {
            query.push(" AND orders.token_id=").push_bind(token_id);
        }
        if let Some((created_at, id)) = cursor {
            query
                .push(" AND (trades.created_at>")
                .push_bind(created_at)
                .push(" OR (trades.created_at=")
                .push_bind(created_at)
                .push(" AND trades.id>")
                .push_bind(id)
                .push("))");
        }
        query
            .push(" ORDER BY trades.created_at, trades.id LIMIT ")
            .push_bind(i64::from(limit) + 1);
        Ok(query.build_query_as().fetch_all(&self.pool).await?)
    }

    pub async fn list_positions(&self) -> Result<Vec<PositionRecord>, StoreError> {
        Ok(sqlx::query_as("SELECT * FROM positions ORDER BY token_id")
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn service_state(&self) -> Result<(ServiceState, String, DateTime<Utc>), StoreError> {
        let (state, reason, updated_at, _) = self.service_state_with_revision().await?;
        Ok((state, reason, updated_at))
    }

    pub(crate) async fn service_state_with_revision(
        &self,
    ) -> Result<(ServiceState, String, DateTime<Utc>, i64), StoreError> {
        let row = sqlx::query(
            "SELECT state, reason, updated_at, revision FROM control_state WHERE singleton=1",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok((
            parse_service_state(&row.try_get::<String, _>("state")?)?,
            row.try_get("reason")?,
            row.try_get("updated_at")?,
            row.try_get("revision")?,
        ))
    }

    pub async fn set_service_state(
        &self,
        actor: &str,
        next: ServiceState,
        reason: &str,
    ) -> Result<(), StoreError> {
        self.set_service_state_inner(actor, next, reason, None, None)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn set_service_state_idempotent(
        &self,
        actor: &str,
        next: ServiceState,
        reason: &str,
        idempotency_key: &str,
        operation: &str,
        request_body: &serde_json::Value,
        response_body: &serde_json::Value,
    ) -> Result<(), StoreError> {
        self.set_service_state_inner(
            actor,
            next,
            reason,
            Some((idempotency_key, operation, request_body, response_body)),
            None,
        )
        .await
    }

    pub async fn set_service_state_if_unchanged(
        &self,
        actor: &str,
        next: ServiceState,
        reason: &str,
        expected_revision: i64,
    ) -> Result<(), StoreError> {
        self.set_service_state_inner(actor, next, reason, None, Some(expected_revision))
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn set_service_state_idempotent_if_unchanged(
        &self,
        actor: &str,
        next: ServiceState,
        reason: &str,
        idempotency_key: &str,
        operation: &str,
        request_body: &serde_json::Value,
        response_body: &serde_json::Value,
        expected_revision: i64,
    ) -> Result<(), StoreError> {
        self.set_service_state_inner(
            actor,
            next,
            reason,
            Some((idempotency_key, operation, request_body, response_body)),
            Some(expected_revision),
        )
        .await
    }

    async fn set_service_state_inner(
        &self,
        actor: &str,
        next: ServiceState,
        reason: &str,
        idempotency: Option<(&str, &str, &serde_json::Value, &serde_json::Value)>,
        expected_revision: Option<i64>,
    ) -> Result<(), StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT state, revision FROM control_state WHERE singleton=1")
            .fetch_one(&mut *tx)
            .await?;
        let prior: String = row.try_get("state")?;
        let prior_revision: i64 = row.try_get("revision")?;
        if expected_revision.is_some_and(|expected| prior != "HALTED" || prior_revision != expected)
        {
            return Err(StoreError::IdentityMismatch(
                "control state changed while resume checks were running".into(),
            ));
        }
        let next_revision = prior_revision.checked_add(1).ok_or_else(|| {
            StoreError::IdentityMismatch("control state revision exhausted".into())
        })?;
        let now = Utc::now();
        let next_text = service_state_text(next);
        sqlx::query(
            "UPDATE control_state SET state = ?, reason = ?, updated_at = ?, revision = ? \
             WHERE singleton=1",
        )
        .bind(next_text)
        .bind(reason)
        .bind(now)
        .bind(next_revision)
        .execute(&mut *tx)
        .await?;
        let action_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO operator_actions \
             (id, actor_id, action, reason, prior_state, new_state, correlation_id, created_at) \
             VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(action_id)
        .bind(actor)
        .bind(next_text.to_ascii_lowercase())
        .bind(reason)
        .bind(&prior)
        .bind(next_text)
        .bind(Uuid::now_v7().to_string())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if let Some((key, operation, request_body, response_body)) = idempotency {
            insert_idempotency_record(
                &mut tx,
                actor,
                key,
                operation,
                "control",
                request_body,
                202,
                response_body,
                now,
            )
            .await?;
        }
        let event = append_event(
            &mut tx,
            self.mode,
            "control.state_changed",
            "operator_action",
            &action_id.to_string(),
            &serde_json::json!({"prior_state": prior, "new_state": next_text, "reason": reason})
                .to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(())
    }

    pub async fn create_cancellation(
        &self,
        actor: &str,
        request: &CancellationRequest,
    ) -> Result<(Uuid, Vec<Uuid>), StoreError> {
        self.create_cancellation_inner(actor, request, None).await
    }

    pub async fn create_cancellation_idempotent(
        &self,
        actor: &str,
        idempotency_key: &str,
        request: &CancellationRequest,
    ) -> Result<(Uuid, Vec<Uuid>), StoreError> {
        let request_body = serde_json::to_value(request)?;
        self.create_cancellation_inner(actor, request, Some((idempotency_key, &request_body)))
            .await
    }

    async fn create_cancellation_inner(
        &self,
        actor: &str,
        request: &CancellationRequest,
        idempotency: Option<(&str, &serde_json::Value)>,
    ) -> Result<(Uuid, Vec<Uuid>), StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let targets = if let Some(order_id) = request.order_id {
            vec![order_id]
        } else if let Some(intent_id) = request.intent_id {
            sqlx::query_scalar("SELECT id FROM orders WHERE intent_id = ? AND state NOT IN ('FILLED','CANCELLED','EXPIRED','REJECTED')")
                .bind(intent_id)
                .fetch_all(&mut *tx)
                .await?
        } else if let Some(condition_id) = &request.condition_id {
            sqlx::query_scalar("SELECT id FROM orders WHERE condition_id = ? AND state NOT IN ('FILLED','CANCELLED','EXPIRED','REJECTED')")
                .bind(condition_id)
                .fetch_all(&mut *tx)
                .await?
        } else {
            sqlx::query_scalar(
                "SELECT id FROM orders WHERE state NOT IN ('FILLED','CANCELLED','EXPIRED','REJECTED')",
            )
            .fetch_all(&mut *tx)
            .await?
        };
        let id = Uuid::now_v7();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO cancellation_requests \
             (id, actor_id, selector_json, target_order_ids_json, reason, state, created_at, updated_at) \
             VALUES(?, ?, ?, ?, ?, 'PENDING', ?, ?)",
        )
        .bind(id)
        .bind(actor)
        .bind(serde_json::to_string(request)?)
        .bind(serde_json::to_string(&targets)?)
        .bind(&request.reason)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if let Some((key, request_body)) = idempotency {
            let response_body = serde_json::json!({
                "id": id,
                "target_order_ids": targets,
                "mode": self.mode
            });
            insert_idempotency_record(
                &mut tx,
                actor,
                key,
                "cancellation",
                &id.to_string(),
                request_body,
                202,
                &response_body,
                now,
            )
            .await?;
        }
        let event = append_event(
            &mut tx,
            self.mode,
            "cancellation.pending",
            "cancellation",
            &id.to_string(),
            &serde_json::json!({
                "id": id,
                "actor_id": actor,
                "target_order_ids": targets,
                "reason": request.reason,
                "state": "PENDING",
                "mode": self.mode
            })
            .to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok((id, targets))
    }

    pub async fn finish_cancellation(
        &self,
        id: Uuid,
        failed_order_ids: &[Uuid],
    ) -> Result<CancellationRecord, StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let state = if failed_order_ids.is_empty() {
            "COMPLETE"
        } else {
            "RECONCILIATION_REQUIRED"
        };
        let now = Utc::now();
        let result = sqlx::query(
            "UPDATE cancellation_requests SET state=?, updated_at=? \
             WHERE id=? AND state='PENDING'",
        )
        .bind(state)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(StoreError::NotFound);
        }
        let record = sqlx::query_as::<_, CancellationRecord>(
            "SELECT * FROM cancellation_requests WHERE id=?",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        let event = append_event(
            &mut tx,
            self.mode,
            "cancellation.completed",
            "cancellation",
            &id.to_string(),
            &serde_json::json!({
                "id": id,
                "state": state,
                "failed_order_ids": failed_order_ids,
                "mode": self.mode
            })
            .to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(record)
    }

    pub async fn get_cancellation(&self, id: Uuid) -> Result<CancellationRecord, StoreError> {
        sqlx::query_as("SELECT * FROM cancellation_requests WHERE id=?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::NotFound)
    }

    pub async fn pending_cancellations(&self) -> Result<Vec<CancellationRecord>, StoreError> {
        Ok(sqlx::query_as(
            "SELECT * FROM cancellation_requests WHERE state='PENDING' ORDER BY created_at, id",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn initialize_paper_account(&self, config: &PaperConfig) -> Result<(), StoreError> {
        if self.mode != Mode::Paper {
            return Err(StoreError::IdentityMismatch(
                "paper simulator state cannot be used by a live database".into(),
            ));
        }
        let configuration_json = serde_jcs::to_vec(config)?;
        let configuration_sha256 = hex::encode(Sha256::digest(configuration_json));
        let quote_balance = Decimal::from_str_exact(&config.starting_quote_balance)
            .map_err(|_| StoreError::InvalidDecimal(config.starting_quote_balance.clone()))?;
        if quote_balance < Decimal::ZERO {
            return Err(StoreError::InvalidDecimal(
                config.starting_quote_balance.clone(),
            ));
        }
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let existing: Option<String> =
            sqlx::query_scalar("SELECT configuration_sha256 FROM paper_account WHERE singleton=1")
                .fetch_optional(&mut *tx)
                .await?;
        if let Some(existing) = existing {
            if existing != configuration_sha256 {
                return Err(StoreError::IdentityMismatch(
                    "paper starting balance/inventory differs from the database binding".into(),
                ));
            }
            tx.commit().await?;
            return Ok(());
        }
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO paper_account \
             (singleton, configuration_sha256, quote_balance, reserved_quote, created_at, updated_at) \
             VALUES(1, ?, ?, '0', ?, ?)",
        )
        .bind(&configuration_sha256)
        .bind(quote_balance.normalize().to_string())
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        for position in &config.starting_positions {
            let shares = Decimal::from_str_exact(&position.shares)
                .map_err(|_| StoreError::InvalidDecimal(position.shares.clone()))?;
            if shares < Decimal::ZERO {
                return Err(StoreError::InvalidDecimal(position.shares.clone()));
            }
            let result = sqlx::query(
                "INSERT INTO paper_inventory(token_id, shares, reserved_shares, updated_at) \
                 VALUES(?, ?, '0', ?)",
            )
            .bind(&position.token_id)
            .bind(shares.normalize().to_string())
            .bind(now)
            .execute(&mut *tx)
            .await;
            if let Err(sqlx::Error::Database(error)) = &result
                && error.is_unique_violation()
            {
                return Err(StoreError::IdentityMismatch(format!(
                    "paper starting inventory contains duplicate token {}",
                    position.token_id
                )));
            }
            result?;
            sqlx::query(
                "INSERT INTO positions(token_id, condition_id, mode, shares, updated_at) \
                 VALUES(?, ?, ?, ?, ?)",
            )
            .bind(&position.token_id)
            .bind(&position.condition_id)
            .bind(self.mode)
            .bind(shares.normalize().to_string())
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        let event = append_event(
            &mut tx,
            self.mode,
            "paper.account_initialized",
            "paper_account",
            "paper",
            &serde_json::json!({
                "mode": self.mode,
                "configuration_sha256": configuration_sha256,
                "quote_balance": quote_balance,
                "position_count": config.starting_positions.len()
            })
            .to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    pub async fn commit_paper_order(
        &self,
        order: &PaperOrderCommit,
    ) -> Result<PaperOrderSnapshot, StoreError> {
        if self.mode != Mode::Paper {
            return Err(StoreError::IdentityMismatch(
                "paper order cannot be committed in live mode".into(),
            ));
        }
        let _risk_guard = self.lock_risk().await;
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        if let Some(existing) = load_paper_order(&mut tx, &order.venue_order_id).await? {
            if existing.evidence != order.evidence {
                return Err(StoreError::IdentityMismatch(
                    "paper deterministic order ID was reused with different evidence".into(),
                ));
            }
            tx.commit().await?;
            return Ok(existing);
        }
        let account = sqlx::query(
            "SELECT quote_balance, reserved_quote FROM paper_account WHERE singleton=1",
        )
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StoreError::IdentityMismatch("paper account is not initialized".into()))?;
        let mut quote_balance =
            parse_stored_decimal(account.try_get::<String, _>("quote_balance")?)?;
        let mut reserved_quote =
            parse_stored_decimal(account.try_get::<String, _>("reserved_quote")?)?;
        let inventory_row =
            sqlx::query("SELECT shares, reserved_shares FROM paper_inventory WHERE token_id=?")
                .bind(&order.token_id)
                .fetch_optional(&mut *tx)
                .await?;
        let (mut shares, mut reserved_shares) =
            inventory_row.map_or(Ok((Decimal::ZERO, Decimal::ZERO)), |row| {
                Ok::<_, StoreError>((
                    parse_stored_decimal(row.try_get::<String, _>("shares")?)?,
                    parse_stored_decimal(row.try_get::<String, _>("reserved_shares")?)?,
                ))
            })?;
        match order.side {
            Side::Buy => {
                let debit = order.quote_amount + order.fee_amount;
                if quote_balance - reserved_quote < debit + order.reserved_quote {
                    return Err(StoreError::IdentityMismatch(
                        "paper quote balance is insufficient".into(),
                    ));
                }
                quote_balance -= debit;
                reserved_quote += order.reserved_quote;
                shares += order.filled_quantity;
            }
            Side::Sell => {
                if shares - reserved_shares < order.filled_quantity + order.reserved_shares {
                    return Err(StoreError::IdentityMismatch(
                        "paper share balance is insufficient".into(),
                    ));
                }
                shares -= order.filled_quantity;
                reserved_shares += order.reserved_shares;
                quote_balance += order.quote_amount - order.fee_amount;
            }
        }
        let now = Utc::now();
        sqlx::query(
            "UPDATE paper_account SET quote_balance=?, reserved_quote=?, updated_at=? \
             WHERE singleton=1",
        )
        .bind(quote_balance.normalize().to_string())
        .bind(reserved_quote.normalize().to_string())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO paper_inventory(token_id, shares, reserved_shares, updated_at) \
             VALUES(?, ?, ?, ?) \
             ON CONFLICT(token_id) DO UPDATE SET shares=excluded.shares, \
             reserved_shares=excluded.reserved_shares, updated_at=excluded.updated_at",
        )
        .bind(&order.token_id)
        .bind(shares.normalize().to_string())
        .bind(reserved_shares.normalize().to_string())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO paper_venue_orders \
             (venue_order_id, condition_id, token_id, side, state, price, fee_rate, \
              original_quantity, remaining_quantity, filled_quantity, filled_price, quote_amount, fee_amount, \
              reserved_quote, reserved_shares, venue_trade_ids_json, evidence_json, \
              created_at, updated_at) \
             VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&order.venue_order_id)
        .bind(&order.condition_id)
        .bind(&order.token_id)
        .bind(order.side)
        .bind(order.state)
        .bind(order.price.normalize().to_string())
        .bind(order.fee_rate.normalize().to_string())
        .bind(order.original_quantity.normalize().to_string())
        .bind(order.remaining_quantity.normalize().to_string())
        .bind(order.filled_quantity.normalize().to_string())
        .bind(
            order
                .filled_price
                .map(|value| value.normalize().to_string()),
        )
        .bind(order.quote_amount.normalize().to_string())
        .bind(order.fee_amount.normalize().to_string())
        .bind(order.reserved_quote.normalize().to_string())
        .bind(order.reserved_shares.normalize().to_string())
        .bind(serde_json::to_string(&order.venue_trade_ids)?)
        .bind(order.evidence.to_string())
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let event = append_event(
            &mut tx,
            self.mode,
            "paper.venue_order_committed",
            "paper_order",
            &order.venue_order_id,
            &serde_json::json!({
                "mode": self.mode,
                "venue_order_id": order.venue_order_id,
                "state": order.state,
                "filled_quantity": order.filled_quantity,
                "quote_amount": order.quote_amount,
                "fee_amount": order.fee_amount
            })
            .to_string(),
        )
        .await?;
        let snapshot = load_paper_order(&mut tx, &order.venue_order_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(snapshot)
    }

    #[allow(clippy::too_many_lines)]
    pub async fn apply_paper_trade_through(
        &self,
        event_id: &str,
        token_id: &str,
        trade_price: Decimal,
        trade_size: Decimal,
        observed_at: DateTime<Utc>,
    ) -> Result<usize, StoreError> {
        if self.mode != Mode::Paper {
            return Err(StoreError::IdentityMismatch(
                "paper market event cannot be applied in live mode".into(),
            ));
        }
        if event_id.is_empty()
            || token_id.is_empty()
            || trade_price <= Decimal::ZERO
            || trade_price >= Decimal::ONE
            || trade_size <= Decimal::ZERO
        {
            return Err(StoreError::IdentityMismatch(
                "paper market event contains invalid fields".into(),
            ));
        }

        let _risk_guard = self.lock_risk().await;
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO paper_market_events \
             (event_id, token_id, price, size, observed_at, created_at) \
             VALUES(?, ?, ?, ?, ?, ?) ON CONFLICT(event_id) DO NOTHING",
        )
        .bind(event_id)
        .bind(token_id)
        .bind(trade_price.normalize().to_string())
        .bind(trade_size.normalize().to_string())
        .bind(observed_at)
        .bind(Utc::now())
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(0);
        }

        let account = sqlx::query(
            "SELECT quote_balance, reserved_quote FROM paper_account WHERE singleton=1",
        )
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StoreError::IdentityMismatch("paper account is not initialized".into()))?;
        let mut quote_balance =
            parse_stored_decimal(account.try_get::<String, _>("quote_balance")?)?;
        let mut account_reserved_quote =
            parse_stored_decimal(account.try_get::<String, _>("reserved_quote")?)?;
        let inventory =
            sqlx::query("SELECT shares, reserved_shares FROM paper_inventory WHERE token_id=?")
                .bind(token_id)
                .fetch_optional(&mut *tx)
                .await?;
        let (mut shares, mut account_reserved_shares) =
            inventory.map_or(Ok((Decimal::ZERO, Decimal::ZERO)), |row| {
                Ok::<_, StoreError>((
                    parse_stored_decimal(row.try_get::<String, _>("shares")?)?,
                    parse_stored_decimal(row.try_get::<String, _>("reserved_shares")?)?,
                ))
            })?;
        let rows = sqlx::query(
            "SELECT venue_order_id, side, price, fee_rate, remaining_quantity, \
                    filled_quantity, quote_amount, fee_amount, reserved_quote, reserved_shares, \
                    venue_trade_ids_json \
             FROM paper_venue_orders \
             WHERE token_id=? AND state IN ('LIVE', 'PARTIALLY_FILLED') AND created_at<=? \
             ORDER BY created_at, venue_order_id",
        )
        .bind(token_id)
        .bind(observed_at)
        .fetch_all(&mut *tx)
        .await?;

        let mut market_size_remaining = trade_size;
        let mut fill_count = 0_usize;
        let mut events = Vec::new();
        for row in rows {
            if market_size_remaining <= Decimal::ZERO {
                break;
            }
            let venue_order_id: String = row.try_get("venue_order_id")?;
            let side: Side = row.try_get("side")?;
            let limit_price = parse_stored_decimal(row.try_get("price")?)?;
            let crossed = match side {
                Side::Buy => trade_price < limit_price,
                Side::Sell => trade_price > limit_price,
            };
            if !crossed {
                continue;
            }
            let remaining_quantity =
                parse_stored_decimal(row.try_get::<String, _>("remaining_quantity")?)?;
            let fill_quantity = remaining_quantity.min(market_size_remaining);
            if fill_quantity <= Decimal::ZERO {
                continue;
            }
            let fee_rate = parse_stored_decimal(row.try_get("fee_rate")?)?;
            let prior_filled = parse_stored_decimal(row.try_get::<String, _>("filled_quantity")?)?;
            let prior_quote = parse_stored_decimal(row.try_get::<String, _>("quote_amount")?)?;
            let prior_fee = parse_stored_decimal(row.try_get::<String, _>("fee_amount")?)?;
            let prior_reserved_quote =
                parse_stored_decimal(row.try_get::<String, _>("reserved_quote")?)?;
            let prior_reserved_shares =
                parse_stored_decimal(row.try_get::<String, _>("reserved_shares")?)?;
            let quote_amount = fill_quantity * trade_price;
            let fee_amount = quote_amount * fee_rate;
            let next_filled = prior_filled + fill_quantity;
            let next_remaining = remaining_quantity - fill_quantity;
            let next_quote = prior_quote + quote_amount;
            let next_fee = prior_fee + fee_amount;
            let average_price = next_quote / next_filled;
            let (next_reserved_quote, next_reserved_shares) = match side {
                Side::Buy => {
                    let released = fill_quantity * limit_price * (Decimal::ONE + fee_rate);
                    account_reserved_quote = (account_reserved_quote - released).max(Decimal::ZERO);
                    quote_balance -= quote_amount + fee_amount;
                    shares += fill_quantity;
                    (
                        (prior_reserved_quote - released).max(Decimal::ZERO),
                        prior_reserved_shares,
                    )
                }
                Side::Sell => {
                    account_reserved_shares =
                        (account_reserved_shares - fill_quantity).max(Decimal::ZERO);
                    shares -= fill_quantity;
                    quote_balance += quote_amount - fee_amount;
                    (
                        prior_reserved_quote,
                        (prior_reserved_shares - fill_quantity).max(Decimal::ZERO),
                    )
                }
            };
            if quote_balance < Decimal::ZERO || shares < Decimal::ZERO {
                return Err(StoreError::IdentityMismatch(
                    "paper trade-through would overdraw the account".into(),
                ));
            }
            let state = if next_remaining == Decimal::ZERO {
                OrderState::Filled
            } else {
                OrderState::PartiallyFilled
            };
            let venue_trade_id = format!("paper:{event_id}:{venue_order_id}");
            let mut venue_trade_ids: Vec<String> =
                serde_json::from_str(row.try_get("venue_trade_ids_json")?)?;
            venue_trade_ids.push(venue_trade_id.clone());
            let evidence = serde_json::json!({
                "paper": true,
                "fill_model": "strict_trade_through",
                "market_event_id": event_id,
                "observed_at": observed_at,
                "trade_price": trade_price,
                "trade_size": trade_size,
                "fill_quantity": fill_quantity
            });
            let now = Utc::now();
            sqlx::query(
                "UPDATE paper_venue_orders SET state=?, remaining_quantity=?, \
                 filled_quantity=?, filled_price=?, quote_amount=?, fee_amount=?, \
                 reserved_quote=?, reserved_shares=?, venue_trade_ids_json=?, \
                 evidence_json=?, updated_at=? WHERE venue_order_id=?",
            )
            .bind(state)
            .bind(next_remaining.normalize().to_string())
            .bind(next_filled.normalize().to_string())
            .bind(average_price.normalize().to_string())
            .bind(next_quote.normalize().to_string())
            .bind(next_fee.normalize().to_string())
            .bind(next_reserved_quote.normalize().to_string())
            .bind(next_reserved_shares.normalize().to_string())
            .bind(serde_json::to_string(&venue_trade_ids)?)
            .bind(evidence.to_string())
            .bind(now)
            .bind(&venue_order_id)
            .execute(&mut *tx)
            .await?;

            if let Some(core_order) =
                sqlx::query_as::<_, OrderRecord>("SELECT * FROM orders WHERE venue_order_id=?")
                    .bind(&venue_order_id)
                    .fetch_optional(&mut *tx)
                    .await?
                && matches!(
                    core_order.state,
                    OrderState::Live | OrderState::PartiallyFilled | OrderState::CancelPending
                )
            {
                let next_core_state = if core_order.state == OrderState::CancelPending
                    && state != OrderState::Filled
                {
                    OrderState::CancelPending
                } else {
                    state
                };
                sqlx::query(
                    "UPDATE orders SET state=?, remaining_quantity=?, updated_at=? WHERE id=?",
                )
                .bind(next_core_state)
                .bind(next_remaining.normalize().to_string())
                .bind(now)
                .bind(core_order.id)
                .execute(&mut *tx)
                .await?;
                if let Some(event) = apply_matched_fill(
                    &mut tx,
                    self.mode,
                    &core_order,
                    Some(&venue_order_id),
                    fill_quantity,
                    Some(trade_price),
                    std::slice::from_ref(&venue_trade_id),
                    now,
                )
                .await?
                {
                    events.push(event);
                }
                sqlx::query(
                    "INSERT INTO order_transitions \
                     (order_id, prior_state, new_state, evidence_json, created_at) \
                     VALUES(?, ?, ?, ?, ?)",
                )
                .bind(core_order.id)
                .bind(core_order.state)
                .bind(next_core_state)
                .bind(evidence.to_string())
                .bind(now)
                .execute(&mut *tx)
                .await?;
                events.push(
                    append_event(
                        &mut tx,
                        self.mode,
                        "order.paper_filled",
                        "order",
                        &core_order.id.to_string(),
                        &serde_json::json!({
                            "state": next_core_state,
                            "remaining_quantity": next_remaining,
                            "fill_quantity": fill_quantity,
                            "market_event_id": event_id
                        })
                        .to_string(),
                    )
                    .await?,
                );
            }
            events.push(
                append_event(
                    &mut tx,
                    self.mode,
                    "paper.trade_through_fill",
                    "paper_order",
                    &venue_order_id,
                    &evidence.to_string(),
                )
                .await?,
            );
            fill_count += 1;
            market_size_remaining -= fill_quantity;
        }

        let now = Utc::now();
        sqlx::query(
            "UPDATE paper_account SET quote_balance=?, reserved_quote=?, updated_at=? \
             WHERE singleton=1",
        )
        .bind(quote_balance.normalize().to_string())
        .bind(account_reserved_quote.normalize().to_string())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO paper_inventory(token_id, shares, reserved_shares, updated_at) \
             VALUES(?, ?, ?, ?) \
             ON CONFLICT(token_id) DO UPDATE SET shares=excluded.shares, \
             reserved_shares=excluded.reserved_shares, updated_at=excluded.updated_at",
        )
        .bind(token_id)
        .bind(shares.normalize().to_string())
        .bind(account_reserved_shares.normalize().to_string())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        events.push(
            append_event(
                &mut tx,
                self.mode,
                "paper.market_trade_observed",
                "paper_market_event",
                event_id,
                &serde_json::json!({
                    "token_id": token_id,
                    "price": trade_price,
                    "size": trade_size,
                    "observed_at": observed_at,
                    "fill_count": fill_count
                })
                .to_string(),
            )
            .await?,
        );
        tx.commit().await?;
        for event in events {
            let _ = self.events.send(event);
        }
        Ok(fill_count)
    }

    pub async fn paper_active_tokens(&self) -> Result<Vec<String>, StoreError> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT token_id FROM paper_venue_orders \
             WHERE state IN ('LIVE', 'PARTIALLY_FILLED') ORDER BY token_id",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn paper_venue_positions(
        &self,
    ) -> Result<Vec<(String, String, Decimal)>, StoreError> {
        let rows = sqlx::query(
            "SELECT inventory.token_id, inventory.shares, \
                    COALESCE(positions.condition_id, ( \
                        SELECT condition_id FROM paper_venue_orders \
                        WHERE paper_venue_orders.token_id=inventory.token_id \
                        ORDER BY created_at LIMIT 1 \
                    )) AS condition_id \
             FROM paper_inventory AS inventory \
             LEFT JOIN positions ON positions.token_id=inventory.token_id \
             ORDER BY inventory.token_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let condition_id: Option<String> = row.try_get("condition_id")?;
                Ok((
                    condition_id.ok_or_else(|| {
                        StoreError::IdentityMismatch(
                            "paper inventory is missing its market condition".into(),
                        )
                    })?,
                    row.try_get("token_id")?,
                    parse_stored_decimal(row.try_get("shares")?)?,
                ))
            })
            .collect()
    }

    pub async fn cancel_paper_order(
        &self,
        venue_order_id: &str,
    ) -> Result<PaperOrderSnapshot, StoreError> {
        if self.mode != Mode::Paper {
            return Err(StoreError::IdentityMismatch(
                "paper cancellation cannot run in live mode".into(),
            ));
        }
        let _risk_guard = self.lock_risk().await;
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT token_id, state, reserved_quote, reserved_shares \
             FROM paper_venue_orders WHERE venue_order_id=?",
        )
        .bind(venue_order_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::NotFound)?;
        let state: OrderState = row.try_get("state")?;
        if !matches!(
            state,
            OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
        ) {
            let token_id: String = row.try_get("token_id")?;
            let released_quote = parse_stored_decimal(row.try_get::<String, _>("reserved_quote")?)?;
            let released_shares =
                parse_stored_decimal(row.try_get::<String, _>("reserved_shares")?)?;
            let now = Utc::now();
            let current_quote: String =
                sqlx::query_scalar("SELECT reserved_quote FROM paper_account WHERE singleton=1")
                    .fetch_one(&mut *tx)
                    .await?;
            let next_quote =
                (parse_stored_decimal(current_quote)? - released_quote).max(Decimal::ZERO);
            sqlx::query(
                "UPDATE paper_account SET reserved_quote=?, updated_at=? WHERE singleton=1",
            )
            .bind(next_quote.normalize().to_string())
            .bind(now)
            .execute(&mut *tx)
            .await?;
            let current_shares: Option<String> =
                sqlx::query_scalar("SELECT reserved_shares FROM paper_inventory WHERE token_id=?")
                    .bind(&token_id)
                    .fetch_optional(&mut *tx)
                    .await?;
            if let Some(current_shares) = current_shares {
                let next_shares =
                    (parse_stored_decimal(current_shares)? - released_shares).max(Decimal::ZERO);
                sqlx::query(
                    "UPDATE paper_inventory SET reserved_shares=?, updated_at=? WHERE token_id=?",
                )
                .bind(next_shares.normalize().to_string())
                .bind(now)
                .bind(&token_id)
                .execute(&mut *tx)
                .await?;
            }
            sqlx::query(
                "UPDATE paper_venue_orders SET state='CANCELLED', remaining_quantity='0', \
                 reserved_quote='0', reserved_shares='0', updated_at=? WHERE venue_order_id=?",
            )
            .bind(now)
            .bind(venue_order_id)
            .execute(&mut *tx)
            .await?;
        }
        let snapshot = load_paper_order(&mut tx, venue_order_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        let event = append_event(
            &mut tx,
            self.mode,
            "paper.venue_order_cancelled",
            "paper_order",
            venue_order_id,
            &serde_json::json!({
                "mode": self.mode,
                "venue_order_id": venue_order_id,
                "state": snapshot.state
            })
            .to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(snapshot)
    }

    pub async fn find_paper_order(
        &self,
        venue_order_id: &str,
    ) -> Result<Option<PaperOrderSnapshot>, StoreError> {
        let mut tx = self.pool.begin().await?;
        let result = load_paper_order(&mut tx, venue_order_id).await?;
        tx.commit().await?;
        Ok(result)
    }

    pub async fn paper_venue_counts(&self) -> Result<(usize, usize), StoreError> {
        let orders: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM paper_venue_orders")
            .fetch_one(&self.pool)
            .await?;
        let trades: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM paper_venue_orders WHERE filled_quantity != '0'",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok((
            usize::try_from(orders).map_err(|_| {
                StoreError::IdentityMismatch("paper order count is negative".into())
            })?,
            usize::try_from(trades).map_err(|_| {
                StoreError::IdentityMismatch("paper trade count is negative".into())
            })?,
        ))
    }

    pub async fn list_paper_orders(&self) -> Result<Vec<PaperOrderSnapshot>, StoreError> {
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT venue_order_id FROM paper_venue_orders ORDER BY created_at")
                .fetch_all(&self.pool)
                .await?;
        let mut orders = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(order) = self.find_paper_order(&id).await? {
                orders.push(order);
            }
        }
        Ok(orders)
    }

    #[cfg(test)]
    pub async fn paper_account_snapshot(&self) -> Result<PaperAccountSnapshot, StoreError> {
        let row = sqlx::query(
            "SELECT quote_balance, reserved_quote FROM paper_account WHERE singleton=1",
        )
        .fetch_one(&self.pool)
        .await?;
        let inventory =
            sqlx::query("SELECT token_id, shares, reserved_shares FROM paper_inventory")
                .fetch_all(&self.pool)
                .await?;
        let mut positions = std::collections::BTreeMap::new();
        for row in inventory {
            positions.insert(
                row.try_get("token_id")?,
                (
                    parse_stored_decimal(row.try_get("shares")?)?,
                    parse_stored_decimal(row.try_get("reserved_shares")?)?,
                ),
            );
        }
        Ok(PaperAccountSnapshot {
            quote_balance: parse_stored_decimal(row.try_get("quote_balance")?)?,
            reserved_quote: parse_stored_decimal(row.try_get("reserved_quote")?)?,
            positions,
        })
    }

    pub async fn replay_events(
        &self,
        after_sequence: i64,
        limit: u32,
    ) -> Result<Vec<ExecutionEvent>, StoreError> {
        Ok(sqlx::query_as(
            "SELECT * FROM execution_events WHERE sequence > ? ORDER BY sequence LIMIT ?",
        )
        .bind(after_sequence)
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn event_sequence_bounds(&self) -> Result<(Option<i64>, Option<i64>), StoreError> {
        let row = sqlx::query(
            "SELECT MIN(sequence) AS minimum, MAX(sequence) AS maximum FROM execution_events",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok((row.try_get("minimum")?, row.try_get("maximum")?))
    }

    pub async fn prune_events(&self) -> Result<u64, StoreError> {
        let retention = i64::try_from(self.event_retention).map_err(|_| {
            StoreError::IdentityMismatch("event retention exceeds SQLite integer".into())
        })?;
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let maximum: i64 =
            sqlx::query_scalar("SELECT COALESCE(MAX(sequence), 0) FROM execution_events")
                .fetch_one(&mut *tx)
                .await?;
        let cutoff = maximum.saturating_sub(retention);
        if cutoff <= 0 {
            tx.commit().await?;
            return Ok(0);
        }
        if self.mode == Mode::Live {
            let backed_up_through: i64 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(last_event_sequence), 0) FROM backups \
                 WHERE state='COMPLETE'",
            )
            .fetch_one(&mut *tx)
            .await?;
            if backed_up_through < cutoff {
                tx.commit().await?;
                return Ok(0);
            }
        }
        let result = sqlx::query("DELETE FROM execution_events WHERE sequence <= ?")
            .bind(cutoff)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(result.rows_affected())
    }

    pub async fn start_reconciliation(&self, trigger: &str) -> Result<Uuid, StoreError> {
        self.start_reconciliation_inner(trigger, None).await
    }

    pub async fn start_reconciliation_idempotent(
        &self,
        actor: &str,
        idempotency_key: &str,
        trigger: &str,
        request_body: &serde_json::Value,
    ) -> Result<Uuid, StoreError> {
        self.start_reconciliation_inner(trigger, Some((actor, idempotency_key, request_body)))
            .await
    }

    async fn start_reconciliation_inner(
        &self,
        trigger: &str,
        idempotency: Option<(&str, &str, &serde_json::Value)>,
    ) -> Result<Uuid, StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let id = Uuid::now_v7();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO reconciliation_runs(id, trigger, state, summary_json, started_at) \
             VALUES(?, ?, 'RUNNING', '{}', ?)",
        )
        .bind(id)
        .bind(trigger)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if let Some((actor, key, request_body)) = idempotency {
            let response_body = serde_json::json!({"id": id, "mode": self.mode});
            insert_idempotency_record(
                &mut tx,
                actor,
                key,
                "reconciliation",
                &id.to_string(),
                request_body,
                202,
                &response_body,
                now,
            )
            .await?;
        }
        let event = append_event(
            &mut tx,
            self.mode,
            "reconciliation.started",
            "reconciliation",
            &id.to_string(),
            &serde_json::json!({"trigger": trigger}).to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(id)
    }

    pub async fn fail_interrupted_reconciliations(&self) -> Result<u64, StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM reconciliation_runs WHERE state='RUNNING' ORDER BY started_at, id",
        )
        .fetch_all(&mut *tx)
        .await?;
        let now = Utc::now();
        let mut events = Vec::with_capacity(rows.len());
        for id in &rows {
            let summary = serde_json::json!({
                "error": "executor restarted before reconciliation completed"
            });
            sqlx::query(
                "UPDATE reconciliation_runs SET state='FAILED', summary_json=?, completed_at=? \
                 WHERE id=? AND state='RUNNING'",
            )
            .bind(summary.to_string())
            .bind(now)
            .bind(id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO reconciliation_findings \
                 (id, reconciliation_id, severity, code, details_json) \
                 VALUES(?, ?, 'CRITICAL', 'RECONCILIATION_INTERRUPTED', ?)",
            )
            .bind(Uuid::now_v7())
            .bind(id)
            .bind(summary.to_string())
            .execute(&mut *tx)
            .await?;
            events.push(
                append_event(
                    &mut tx,
                    self.mode,
                    "reconciliation.failed",
                    "reconciliation",
                    &id.to_string(),
                    &summary.to_string(),
                )
                .await?,
            );
        }
        tx.commit().await?;
        for event in events {
            let _ = self.events.send(event);
        }
        Ok(rows.len() as u64)
    }

    pub async fn finish_reconciliation(
        &self,
        id: Uuid,
        summary: &serde_json::Value,
    ) -> Result<(), StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE reconciliation_runs SET state='COMPLETE', summary_json=?, completed_at=? WHERE id=?",
        )
        .bind(summary.to_string())
        .bind(Utc::now())
        .bind(id)
        .execute(&mut *tx)
        .await?;
        if let Some(findings) = summary
            .get("critical_findings")
            .and_then(serde_json::Value::as_array)
        {
            if findings.is_empty() {
                sqlx::query(
                    "UPDATE reconciliation_findings SET resolved_at=? \
                     WHERE severity='CRITICAL' AND resolved_at IS NULL",
                )
                .bind(Utc::now())
                .execute(&mut *tx)
                .await?;
            }
            for finding in findings {
                sqlx::query(
                    "INSERT INTO reconciliation_findings \
                     (id, reconciliation_id, severity, code, details_json) \
                     VALUES(?, ?, 'CRITICAL', 'RECONCILIATION_MISMATCH', ?)",
                )
                .bind(Uuid::now_v7())
                .bind(id)
                .bind(serde_json::json!({"message": finding}).to_string())
                .execute(&mut *tx)
                .await?;
            }
        }
        let event = append_event(
            &mut tx,
            self.mode,
            "reconciliation.completed",
            "reconciliation",
            &id.to_string(),
            &summary.to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(())
    }

    pub async fn fail_reconciliation(&self, id: Uuid, error: &str) -> Result<(), StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let summary = serde_json::json!({"error": error});
        sqlx::query(
            "UPDATE reconciliation_runs SET state='FAILED', summary_json=?, completed_at=? \
             WHERE id=?",
        )
        .bind(summary.to_string())
        .bind(Utc::now())
        .bind(id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO reconciliation_findings \
             (id, reconciliation_id, severity, code, details_json) \
             VALUES(?, ?, 'CRITICAL', 'RECONCILIATION_FAILED', ?)",
        )
        .bind(Uuid::now_v7())
        .bind(id)
        .bind(summary.to_string())
        .execute(&mut *tx)
        .await?;
        let event = append_event(
            &mut tx,
            self.mode,
            "reconciliation.failed",
            "reconciliation",
            &id.to_string(),
            &summary.to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(())
    }

    pub async fn get_reconciliation(&self, id: Uuid) -> Result<ReconciliationRecord, StoreError> {
        sqlx::query_as("SELECT * FROM reconciliation_runs WHERE id=?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::NotFound)
    }

    pub async fn unresolved_critical_findings(&self) -> Result<u64, StoreError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM reconciliation_findings \
             WHERE severity='CRITICAL' AND resolved_at IS NULL",
        )
        .fetch_one(&self.pool)
        .await?;
        u64::try_from(count)
            .map_err(|_| StoreError::IdentityMismatch("critical finding count is negative".into()))
    }

    pub async fn latest_successful_reconciliation(
        &self,
    ) -> Result<Option<DateTime<Utc>>, StoreError> {
        Ok(sqlx::query_scalar(
            "SELECT MAX(completed_at) FROM reconciliation_runs WHERE state='COMPLETE'",
        )
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn create_backup(&self) -> Result<BackupRecord, StoreError> {
        let backup = self.start_backup_inner(None).await?;
        self.run_backup(backup.id).await?;
        self.get_backup(backup.id).await
    }

    pub async fn start_backup_idempotent(
        &self,
        actor: &str,
        idempotency_key: &str,
        request_body: &serde_json::Value,
    ) -> Result<BackupRecord, StoreError> {
        self.start_backup_inner(Some((actor, idempotency_key, request_body)))
            .await
    }

    async fn start_backup_inner(
        &self,
        idempotency: Option<(&str, &str, &serde_json::Value)>,
    ) -> Result<BackupRecord, StoreError> {
        fs::create_dir_all(self.backup_dir.as_ref())?;
        set_owner_only_directory(self.backup_dir.as_ref())?;
        let id = Uuid::now_v7();
        let created_at = Utc::now();
        {
            let _guard = self.write_gate.lock().await;
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                "INSERT INTO backups(id, state, created_at, updated_at) VALUES(?, 'RUNNING', ?, ?)",
            )
            .bind(id)
            .bind(created_at)
            .bind(created_at)
            .execute(&mut *tx)
            .await?;
            if let Some((actor, key, request_body)) = idempotency {
                let response_body = serde_json::json!({
                    "id": id,
                    "state": "RUNNING",
                    "mode": self.mode
                });
                insert_idempotency_record(
                    &mut tx,
                    actor,
                    key,
                    "backup",
                    &id.to_string(),
                    request_body,
                    202,
                    &response_body,
                    created_at,
                )
                .await?;
            }
            let event = append_event(
                &mut tx,
                self.mode,
                "backup.started",
                "backup",
                &id.to_string(),
                &serde_json::json!({"id": id, "state": "RUNNING", "mode": self.mode}).to_string(),
            )
            .await?;
            tx.commit().await?;
            let _ = self.events.send(event);
        }
        self.get_backup(id).await
    }

    pub async fn run_backup(&self, id: Uuid) -> Result<(), StoreError> {
        let created_at: DateTime<Utc> =
            sqlx::query_scalar("SELECT created_at FROM backups WHERE id=? AND state='RUNNING'")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?
                .ok_or(StoreError::NotFound)?;
        let result = self.create_backup_files(id, created_at).await;
        if let Err(error) = result {
            let _guard = self.write_gate.lock().await;
            let mut tx = self.pool.begin().await?;
            sqlx::query("UPDATE backups SET state='FAILED', error=?, updated_at=? WHERE id=?")
                .bind(error.to_string())
                .bind(Utc::now())
                .bind(id)
                .execute(&mut *tx)
                .await?;
            let event = append_event(
                &mut tx,
                self.mode,
                "backup.failed",
                "backup",
                &id.to_string(),
                &serde_json::json!({"id": id, "state": "FAILED", "error": error.to_string()})
                    .to_string(),
            )
            .await?;
            tx.commit().await?;
            let _ = self.events.send(event);
            return Err(error);
        }
        Ok(())
    }

    pub async fn fail_interrupted_backups(&self) -> Result<u64, StoreError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.pool.begin().await?;
        let ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM backups WHERE state='RUNNING' ORDER BY created_at, id",
        )
        .fetch_all(&mut *tx)
        .await?;
        let now = Utc::now();
        let mut events = Vec::with_capacity(ids.len());
        for id in &ids {
            let message = "executor restarted before backup completed";
            sqlx::query(
                "UPDATE backups SET state='FAILED', error=?, updated_at=? \
                 WHERE id=? AND state='RUNNING'",
            )
            .bind(message)
            .bind(now)
            .bind(id)
            .execute(&mut *tx)
            .await?;
            events.push(
                append_event(
                    &mut tx,
                    self.mode,
                    "backup.failed",
                    "backup",
                    &id.to_string(),
                    &serde_json::json!({
                        "id": id,
                        "state": "FAILED",
                        "error": message
                    })
                    .to_string(),
                )
                .await?,
            );
        }
        tx.commit().await?;
        for event in events {
            let _ = self.events.send(event);
        }
        Ok(ids.len() as u64)
    }

    async fn create_backup_files(
        &self,
        id: Uuid,
        created_at: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let _guard = self.write_gate.lock().await;
        let backup_path = self.backup_dir.join(format!("{id}.sqlite3"));
        let partial_backup_path = self.backup_dir.join(format!("{id}.sqlite3.partial"));
        let escaped = partial_backup_path.to_string_lossy().replace('\'', "''");
        sqlx::query(&format!("VACUUM INTO '{escaped}'"))
            .execute(&self.pool)
            .await?;
        set_owner_only_file(&partial_backup_path)?;
        sqlite_integrity_check(&partial_backup_path).await?;
        let checksum = sha256_file(&partial_backup_path)?;
        let size = fs::metadata(&partial_backup_path)?.len();
        let last_sequence: i64 =
            sqlx::query_scalar("SELECT COALESCE(MAX(sequence), 0) FROM execution_events")
                .fetch_one(&self.pool)
                .await?;
        let schema_version: i64 =
            sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM _sqlx_migrations")
                .fetch_one(&self.pool)
                .await?;
        let identity = sqlx::query(
            "SELECT chain_id, protocol_version, signer_address, funder_address \
             FROM instance_metadata WHERE singleton=1",
        )
        .fetch_one(&self.pool)
        .await?;
        let manifest = BackupManifest {
            backup_id: id,
            schema_version,
            mode: self.mode,
            chain_id: u64::try_from(identity.try_get::<i64, _>("chain_id")?)
                .map_err(|_| StoreError::IdentityMismatch("stored chain ID is negative".into()))?,
            protocol_version: u32::try_from(identity.try_get::<i64, _>("protocol_version")?)
                .map_err(|_| {
                    StoreError::IdentityMismatch("stored protocol version is invalid".into())
                })?,
            signer_address: identity.try_get("signer_address")?,
            funder_address: identity.try_get("funder_address")?,
            last_event_sequence: last_sequence,
            database_size_bytes: size,
            checksum_sha256: checksum.clone(),
            created_at,
        };
        let manifest_path = self.backup_dir.join(format!("{id}.manifest.json"));
        let partial_manifest_path = self.backup_dir.join(format!("{id}.manifest.json.partial"));
        fs::write(
            &partial_manifest_path,
            serde_json::to_vec_pretty(&manifest)?,
        )?;
        set_owner_only_file(&partial_manifest_path)?;
        fs::rename(&partial_backup_path, &backup_path)?;
        fs::rename(&partial_manifest_path, &manifest_path)?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE backups SET state='COMPLETE', database_path=?, manifest_path=?, \
             checksum_sha256=?, last_event_sequence=?, updated_at=? WHERE id=?",
        )
        .bind(backup_path.to_string_lossy().to_string())
        .bind(manifest_path.to_string_lossy().to_string())
        .bind(checksum)
        .bind(last_sequence)
        .bind(Utc::now())
        .bind(id)
        .execute(&mut *tx)
        .await?;
        let event = append_event(
            &mut tx,
            self.mode,
            "backup.completed",
            "backup",
            &id.to_string(),
            &serde_json::json!({
                "id": id,
                "state": "COMPLETE",
                "checksum_sha256": manifest.checksum_sha256,
                "last_event_sequence": manifest.last_event_sequence,
                "mode": self.mode
            })
            .to_string(),
        )
        .await?;
        tx.commit().await?;
        let _ = self.events.send(event);
        Ok(())
    }

    pub async fn get_backup(&self, id: Uuid) -> Result<BackupRecord, StoreError> {
        let row = sqlx::query(
            "SELECT id, state, database_path, manifest_path, checksum_sha256, \
             last_event_sequence, error, created_at, updated_at FROM backups WHERE id=?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(StoreError::NotFound)?;
        Ok(BackupRecord {
            id: row.try_get("id")?,
            state: row.try_get("state")?,
            database_path: row.try_get("database_path")?,
            manifest_path: row.try_get("manifest_path")?,
            checksum_sha256: row.try_get("checksum_sha256")?,
            last_event_sequence: row.try_get("last_event_sequence")?,
            error: row.try_get("error")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }

    pub async fn checkpoint(&self) -> Result<(), StoreError> {
        let _guard = self.write_gate.lock().await;
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}

async fn risk_snapshot_in_transaction(
    tx: &mut Transaction<'_, Sqlite>,
    condition_id: &str,
    token_id: &str,
) -> Result<RiskSnapshot, StoreError> {
    let orders = sqlx::query_as::<_, RiskExposureRow>(
        "SELECT condition_id, token_id, side, fee_rate, quote_exposure, \
                original_quantity, remaining_quantity AS quantity \
         FROM orders WHERE state IN \
         ('PREPARED','SUBMITTING','LIVE','PARTIALLY_FILLED','CANCEL_PENDING','UNKNOWN')",
    )
    .fetch_all(&mut **tx)
    .await?;
    let reservations = sqlx::query_as::<_, RiskExposureRow>(
        "SELECT condition_id, token_id, side, fee_rate, quote_exposure, \
                quantity AS original_quantity, quantity \
         FROM risk_reservations WHERE state='ACTIVE'",
    )
    .fetch_all(&mut **tx)
    .await?;
    let positions = sqlx::query_as::<_, RiskPositionRow>("SELECT token_id, shares FROM positions")
        .fetch_all(&mut **tx)
        .await?;
    let daily_rows = sqlx::query_as::<_, RiskTradeRow>(
        "SELECT price, size FROM trades \
         WHERE status IN ('MATCHED','MINED','CONFIRMED','RETRYING','FAILED') \
         AND created_at >= date('now')",
    )
    .fetch_all(&mut **tx)
    .await?;
    build_risk_snapshot(
        condition_id,
        token_id,
        &orders,
        &reservations,
        positions,
        daily_rows,
    )
}

fn build_risk_snapshot(
    condition_id: &str,
    token_id: &str,
    orders: &[RiskExposureRow],
    reservations: &[RiskExposureRow],
    positions: Vec<RiskPositionRow>,
    daily_rows: Vec<RiskTradeRow>,
) -> Result<RiskSnapshot, StoreError> {
    let open_order_count = u64::try_from(orders.len().saturating_add(reservations.len()))
        .map_err(|_| StoreError::IdentityMismatch("open order count overflow".into()))?;
    let mut market_open_notional = Decimal::ZERO;
    let mut token_pending_buys = Decimal::ZERO;
    let mut token_pending_sells = Decimal::ZERO;
    let mut gross_exposure = Decimal::ZERO;
    for row in orders.iter().chain(reservations) {
        let quantity = parse_stored_decimal(row.quantity.clone())?;
        let original_quantity = parse_stored_decimal(row.original_quantity.clone())?;
        if original_quantity <= Decimal::ZERO {
            return Err(StoreError::IdentityMismatch(
                "risk exposure has a non-positive original quantity".into(),
            ));
        }
        let quote_exposure = parse_stored_decimal(row.quote_exposure.clone())?;
        let notional = quote_exposure * quantity / original_quantity;
        if row.condition_id == condition_id {
            market_open_notional += notional;
        }
        if row.token_id == token_id {
            if row.side == "BUY" {
                token_pending_buys += quantity;
            } else if row.side == "SELL" {
                token_pending_sells += quantity;
            }
        }
        if row.side == "BUY" {
            let fee_rate = parse_stored_decimal(row.fee_rate.clone())?;
            gross_exposure += notional * (Decimal::ONE + fee_rate);
        }
    }
    let mut token_position = Decimal::ZERO;
    for row in positions {
        let shares = parse_stored_decimal(row.shares)?;
        gross_exposure += shares.abs();
        if row.token_id == token_id {
            token_position = shares;
        }
    }
    let mut daily_matched_notional = Decimal::ZERO;
    for row in daily_rows {
        let price = parse_stored_decimal(row.price)?;
        let size = parse_stored_decimal(row.size)?;
        daily_matched_notional += price * size;
    }
    Ok(RiskSnapshot {
        open_order_count,
        market_open_notional,
        token_position,
        token_pending_buys,
        token_pending_sells,
        gross_exposure,
        daily_matched_notional,
    })
}

async fn load_paper_order(
    tx: &mut Transaction<'_, Sqlite>,
    venue_order_id: &str,
) -> Result<Option<PaperOrderSnapshot>, StoreError> {
    let row = sqlx::query(
        "SELECT venue_order_id, state, remaining_quantity, filled_quantity, filled_price, \
                venue_trade_ids_json, evidence_json \
         FROM paper_venue_orders WHERE venue_order_id=?",
    )
    .bind(venue_order_id)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(|row| {
        let filled_price = row
            .try_get::<Option<String>, _>("filled_price")?
            .map(parse_stored_decimal)
            .transpose()?;
        Ok(PaperOrderSnapshot {
            venue_order_id: row.try_get("venue_order_id")?,
            state: row.try_get("state")?,
            remaining_quantity: parse_stored_decimal(row.try_get("remaining_quantity")?)?,
            filled_quantity: parse_stored_decimal(row.try_get("filled_quantity")?)?,
            filled_price,
            venue_trade_ids: serde_json::from_str(row.try_get("venue_trade_ids_json")?)?,
            evidence: serde_json::from_str(row.try_get("evidence_json")?)?,
        })
    })
    .transpose()
}

async fn append_event(
    tx: &mut Transaction<'_, Sqlite>,
    mode: Mode,
    event_type: &str,
    resource_type: &str,
    resource_id: &str,
    payload_json: &str,
) -> Result<ExecutionEvent, StoreError> {
    let created_at = Utc::now();
    let result = sqlx::query(
        "INSERT INTO execution_events \
         (event_type, resource_type, resource_id, mode, payload_json, created_at) \
         VALUES(?, ?, ?, ?, ?, ?)",
    )
    .bind(event_type)
    .bind(resource_type)
    .bind(resource_id)
    .bind(mode)
    .bind(payload_json)
    .bind(created_at)
    .execute(&mut **tx)
    .await?;
    Ok(ExecutionEvent {
        sequence: result.last_insert_rowid(),
        event_type: event_type.into(),
        resource_type: resource_type.into(),
        resource_id: resource_id.into(),
        mode,
        payload_json: payload_json.into(),
        created_at,
    })
}

async fn insert_order(
    tx: &mut Transaction<'_, Sqlite>,
    order: &OrderRecord,
    fee_rate: Decimal,
    quote_exposure: Decimal,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO orders \
         (id, intent_id, mode, venue_order_id, condition_id, token_id, side, time_in_force, \
          price, fee_rate, quote_exposure, original_quantity, remaining_quantity, state, \
          created_at, updated_at) \
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(order.id)
    .bind(order.intent_id)
    .bind(order.mode)
    .bind(&order.venue_order_id)
    .bind(&order.condition_id)
    .bind(&order.token_id)
    .bind(order.side)
    .bind(order.time_in_force)
    .bind(&order.price)
    .bind(fee_rate.normalize().to_string())
    .bind(quote_exposure.normalize().to_string())
    .bind(&order.original_quantity)
    .bind(&order.remaining_quantity)
    .bind(order.state)
    .bind(order.created_at)
    .bind(order.updated_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn apply_matched_fill(
    tx: &mut Transaction<'_, Sqlite>,
    mode: Mode,
    order: &OrderRecord,
    venue_order_id: Option<&str>,
    filled_quantity: Decimal,
    filled_price: Option<Decimal>,
    venue_trade_ids: &[String],
    now: DateTime<Utc>,
) -> Result<Option<ExecutionEvent>, StoreError> {
    if filled_quantity <= Decimal::ZERO {
        return Ok(None);
    }
    let trade_price = filled_price
        .unwrap_or(parse_stored_decimal(order.price.clone())?)
        .normalize();
    let venue_trade_id = match venue_trade_ids {
        [] => {
            let order_reference =
                venue_order_id.map_or_else(|| order.id.to_string(), str::to_owned);
            format!("submission:{order_reference}")
        }
        [venue_trade_id] => venue_trade_id.clone(),
        venue_trade_ids => format!("submission:{}", venue_trade_ids.join(",")),
    };
    let trade = TradeRecord {
        id: Uuid::now_v7(),
        order_id: order.id,
        mode,
        venue_trade_id,
        price: trade_price.to_string(),
        size: filled_quantity.normalize().to_string(),
        status: TradeState::Matched,
        created_at: now,
        updated_at: now,
    };
    let insert = sqlx::query(
        "INSERT INTO trades \
         (id, order_id, mode, venue_trade_id, price, size, status, created_at, updated_at) \
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(venue_trade_id) DO NOTHING",
    )
    .bind(trade.id)
    .bind(trade.order_id)
    .bind(trade.mode)
    .bind(&trade.venue_trade_id)
    .bind(&trade.price)
    .bind(&trade.size)
    .bind(trade.status)
    .bind(trade.created_at)
    .bind(trade.updated_at)
    .execute(&mut **tx)
    .await?;
    if insert.rows_affected() == 0 {
        return Ok(None);
    }
    let current_position =
        sqlx::query_scalar::<_, String>("SELECT shares FROM positions WHERE token_id=?")
            .bind(&order.token_id)
            .fetch_optional(&mut **tx)
            .await?
            .map_or(Ok(Decimal::ZERO), parse_stored_decimal)?;
    let next_position = match order.side {
        Side::Buy => current_position + filled_quantity,
        Side::Sell => current_position - filled_quantity,
    };
    sqlx::query(
        "INSERT INTO positions(token_id, condition_id, mode, shares, updated_at) \
         VALUES(?, ?, ?, ?, ?) \
         ON CONFLICT(token_id) DO UPDATE SET condition_id=excluded.condition_id, \
         mode=excluded.mode, shares=excluded.shares, updated_at=excluded.updated_at",
    )
    .bind(&order.token_id)
    .bind(&order.condition_id)
    .bind(mode)
    .bind(next_position.normalize().to_string())
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(Some(
        append_event(
            tx,
            mode,
            "trade.matched",
            "trade",
            &trade.id.to_string(),
            &serde_json::to_string(&trade)?,
        )
        .await?,
    ))
}

fn trade_component_ids(venue_trade_id: &str) -> Vec<&str> {
    venue_trade_id
        .strip_prefix("submission:")
        .map_or_else(|| vec![venue_trade_id], |ids| ids.split(',').collect())
}

fn aggregate_trade_status(statuses: &[TradeState]) -> TradeState {
    if statuses.contains(&TradeState::Failed) {
        TradeState::Failed
    } else if statuses.contains(&TradeState::Retrying) {
        TradeState::Retrying
    } else if statuses
        .iter()
        .all(|status| *status == TradeState::Confirmed)
    {
        TradeState::Confirmed
    } else if statuses
        .iter()
        .all(|status| matches!(status, TradeState::Mined | TradeState::Confirmed))
    {
        TradeState::Mined
    } else {
        TradeState::Matched
    }
}

fn valid_order_transition(prior: OrderState, next: OrderState) -> bool {
    prior == next
        || matches!(
            (prior, next),
            (
                OrderState::Live | OrderState::PartiallyFilled,
                OrderState::CancelPending
            ) | (
                OrderState::CancelPending | OrderState::Unknown,
                OrderState::Filled | OrderState::Cancelled | OrderState::Expired
            )
        )
}

fn validate_venue_order_evidence(
    original_quantity: Decimal,
    state: OrderState,
    remaining_quantity: Decimal,
    filled_quantity: Decimal,
    filled_price: Option<Decimal>,
) -> Result<(), StoreError> {
    if original_quantity <= Decimal::ZERO
        || remaining_quantity < Decimal::ZERO
        || filled_quantity < Decimal::ZERO
        || remaining_quantity > original_quantity
        || filled_quantity > original_quantity
        || remaining_quantity + filled_quantity > original_quantity
    {
        return Err(StoreError::IdentityMismatch(
            "venue order quantities violate the persisted order bounds".into(),
        ));
    }
    if !matches!(
        state,
        OrderState::Live
            | OrderState::PartiallyFilled
            | OrderState::Filled
            | OrderState::Cancelled
            | OrderState::Expired
            | OrderState::Rejected
            | OrderState::Unknown
    ) {
        return Err(StoreError::IdentityMismatch(
            "venue evidence contains a local-only order state".into(),
        ));
    }
    if (state == OrderState::Filled && (remaining_quantity != Decimal::ZERO))
        || (state == OrderState::Live && remaining_quantity == Decimal::ZERO)
        || (state == OrderState::PartiallyFilled
            && (remaining_quantity == Decimal::ZERO || filled_quantity == Decimal::ZERO))
        || (state == OrderState::Rejected && filled_quantity != Decimal::ZERO)
        || (state == OrderState::Unknown
            && (filled_quantity != Decimal::ZERO || remaining_quantity != original_quantity))
    {
        return Err(StoreError::IdentityMismatch(
            "venue order state is inconsistent with reported quantities".into(),
        ));
    }
    if filled_quantity > Decimal::ZERO {
        let Some(price) = filled_price else {
            return Err(StoreError::IdentityMismatch(
                "venue fill is missing its execution price".into(),
            ));
        };
        if price <= Decimal::ZERO || price >= Decimal::ONE {
            return Err(StoreError::IdentityMismatch(
                "venue fill price must be between zero and one".into(),
            ));
        }
    }
    Ok(())
}

fn parse_stored_decimal(raw: String) -> Result<Decimal, StoreError> {
    Decimal::from_str(&raw).map_err(|_| StoreError::InvalidDecimal(raw))
}

fn parse_service_state(raw: &str) -> Result<ServiceState, StoreError> {
    match raw {
        "STARTING" => Ok(ServiceState::Starting),
        "RECONCILING" => Ok(ServiceState::Reconciling),
        "READY" => Ok(ServiceState::Ready),
        "DEGRADED" => Ok(ServiceState::Degraded),
        "HALTED" => Ok(ServiceState::Halted),
        // SHUTTINGDOWN was written by v0.2.0 development builds before state
        // serialization was made explicit. Accept it so those databases fail
        // closed through normal startup recovery instead of becoming unreadable.
        "SHUTTING_DOWN" | "SHUTTINGDOWN" => Ok(ServiceState::ShuttingDown),
        _ => Err(StoreError::IdentityMismatch(format!(
            "unknown control state {raw}"
        ))),
    }
}

const fn service_state_text(state: ServiceState) -> &'static str {
    match state {
        ServiceState::Starting => "STARTING",
        ServiceState::Reconciling => "RECONCILING",
        ServiceState::Ready => "READY",
        ServiceState::Degraded => "DEGRADED",
        ServiceState::Halted => "HALTED",
        ServiceState::ShuttingDown => "SHUTTING_DOWN",
    }
}

pub async fn verify_backup(
    manifest_path: &Path,
    expected_mode: Mode,
) -> anyhow::Result<BackupManifest> {
    verify_backup_inner(manifest_path, Some(expected_mode)).await
}

pub async fn verify_backup_offline(manifest_path: &Path) -> anyhow::Result<BackupManifest> {
    verify_backup_inner(manifest_path, None).await
}

async fn verify_backup_inner(
    manifest_path: &Path,
    expected_mode: Option<Mode>,
) -> anyhow::Result<BackupManifest> {
    let manifest: BackupManifest =
        serde_json::from_slice(&fs::read(manifest_path)?).map_err(anyhow::Error::from)?;
    let database_path = manifest_path.with_file_name(format!("{}.sqlite3", manifest.backup_id));
    let actual = sha256_file(&database_path)?;
    anyhow::ensure!(
        actual == manifest.checksum_sha256,
        "backup checksum mismatch"
    );
    if let Some(expected_mode) = expected_mode {
        anyhow::ensure!(
            manifest.mode == expected_mode,
            "backup mode {} does not match configured mode {expected_mode}",
            manifest.mode
        );
    }
    sqlite_integrity_check(&database_path).await?;
    let options = SqliteConnectOptions::new()
        .filename(&database_path)
        .read_only(true)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let schema_version: i64 =
        sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await?;
    anyhow::ensure!(
        schema_version == manifest.schema_version,
        "backup schema version does not match manifest"
    );
    let mode: String = sqlx::query_scalar("SELECT mode FROM instance_metadata WHERE singleton=1")
        .fetch_one(&pool)
        .await?;
    anyhow::ensure!(
        mode == manifest.mode.to_string(),
        "backup database mode mismatch"
    );
    let last_sequence: i64 =
        sqlx::query_scalar("SELECT COALESCE(MAX(sequence), 0) FROM execution_events")
            .fetch_one(&pool)
            .await?;
    anyhow::ensure!(
        last_sequence == manifest.last_event_sequence,
        "backup event sequence does not match manifest"
    );
    pool.close().await;
    Ok(manifest)
}

async fn sqlite_integrity_check(path: &Path) -> Result<(), StoreError> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let result: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&pool)
        .await?;
    pool.close().await;
    if result != "ok" {
        return Err(StoreError::IdentityMismatch(format!(
            "SQLite integrity check failed: {result}"
        )));
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, std::io::Error> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(unix)]
fn set_owner_only_file(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_owner_only_file(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(unix)]
fn set_owner_only_directory(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_owner_only_directory(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

fn harden_sqlite_files(database_path: &Path) -> Result<(), std::io::Error> {
    set_owner_only_file(database_path)?;
    for suffix in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", database_path.display()));
        if sidecar.exists() {
            set_owner_only_file(&sidecar)?;
        }
    }
    Ok(())
}

fn operation_hash(operation: &str, body: &serde_json::Value) -> Result<String, serde_json::Error> {
    let canonical = serde_jcs::to_vec(&serde_json::json!({
        "operation": operation,
        "body": body,
    }))?;
    Ok(hex::encode(Sha256::digest(canonical)))
}

#[allow(clippy::too_many_arguments)]
async fn insert_idempotency_record(
    tx: &mut Transaction<'_, Sqlite>,
    actor_id: &str,
    idempotency_key: &str,
    operation: &str,
    resource_id: &str,
    request_body: &serde_json::Value,
    response_status: u16,
    response_body: &serde_json::Value,
    created_at: DateTime<Utc>,
) -> Result<(), StoreError> {
    let request_hash = operation_hash(operation, request_body)?;
    let response_json = serde_jcs::to_string(response_body)?;
    let result = sqlx::query(
        "INSERT INTO idempotency_keys \
         (actor_id, idempotency_key, request_hash, resource_type, resource_id, \
          response_status, response_json, created_at) \
         VALUES(?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(actor_id, idempotency_key) DO NOTHING",
    )
    .bind(actor_id)
    .bind(idempotency_key)
    .bind(request_hash)
    .bind(operation)
    .bind(resource_id)
    .bind(i64::from(response_status))
    .bind(response_json)
    .bind(created_at)
    .execute(&mut **tx)
    .await?;
    if result.rows_affected() != 1 {
        return Err(StoreError::IdempotencyConflict);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tempfile::tempdir;

    use super::*;
    use crate::{
        config::{PaperConfig, PolymarketConfig},
        domain::{ClientContext, Quantity, QuantityUnit, Side, TimeInForce},
        venue::PriceLevel,
    };

    async fn store() -> (tempfile::TempDir, Store) {
        let dir = tempdir().unwrap();
        let storage = StorageConfig {
            database_path: dir.path().join("paper.sqlite3").display().to_string(),
            backup_dir: dir.path().join("backups").display().to_string(),
            event_retention: 100,
        };
        let store = Store::open(&storage, Mode::Paper, &PolymarketConfig::default())
            .await
            .unwrap();
        mark_ready(&store).await;
        (dir, store)
    }

    async fn mark_ready(store: &Store) {
        sqlx::query("UPDATE control_state SET state='READY' WHERE singleton=1")
            .execute(&store.pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn shutting_down_control_state_round_trips_and_accepts_legacy_spelling() {
        let (_dir, store) = store().await;
        store
            .set_service_state("test", ServiceState::ShuttingDown, "fixture shutdown")
            .await
            .unwrap();
        assert_eq!(
            store.service_state().await.unwrap().0,
            ServiceState::ShuttingDown
        );

        sqlx::query("UPDATE control_state SET state='SHUTTINGDOWN' WHERE singleton=1")
            .execute(&store.pool)
            .await
            .unwrap();
        assert_eq!(
            store.service_state().await.unwrap().0,
            ServiceState::ShuttingDown
        );
    }

    #[tokio::test]
    async fn stale_resume_cannot_overwrite_a_newer_halt() {
        let (_dir, store) = store().await;
        store
            .set_service_state("operator", ServiceState::Halted, "initial halt")
            .await
            .unwrap();
        let (_, _, _, resume_revision) = store.service_state_with_revision().await.unwrap();
        store
            .set_service_state("operator", ServiceState::Halted, "emergency halt")
            .await
            .unwrap();

        assert!(
            store
                .set_service_state_if_unchanged(
                    "operator",
                    ServiceState::Ready,
                    "stale resume",
                    resume_revision,
                )
                .await
                .is_err()
        );
        let (state, reason, _) = store.service_state().await.unwrap();
        assert_eq!(state, ServiceState::Halted);
        assert_eq!(reason, "emergency halt");
    }

    #[test]
    fn venue_order_evidence_cannot_exceed_persisted_order_bounds() {
        assert!(
            validate_venue_order_evidence(
                Decimal::TEN,
                OrderState::PartiallyFilled,
                Decimal::new(6, 0),
                Decimal::new(5, 0),
                Some(Decimal::new(5, 1)),
            )
            .is_err()
        );
        assert!(
            validate_venue_order_evidence(
                Decimal::TEN,
                OrderState::Filled,
                Decimal::ZERO,
                Decimal::TEN,
                Some(Decimal::ONE),
            )
            .is_err()
        );
    }

    fn request() -> OrderIntentRequest {
        OrderIntentRequest {
            condition_id: "0xabc".into(),
            token_id: "123".into(),
            side: Side::Buy,
            time_in_force: TimeInForce::Gtc,
            quantity: Quantity {
                unit: QuantityUnit::Shares,
                value: "10".into(),
            },
            limit_price: Some("0.5".into()),
            worst_price: None,
            expires_at: None,
            post_only: true,
            client_context: ClientContext::default(),
        }
    }

    fn policy() -> RiskPolicy {
        RiskPolicy {
            version: "test-policy".into(),
            allowed_condition_ids: BTreeSet::new(),
            allowed_token_ids: BTreeSet::new(),
            allowed_sides: [Side::Buy, Side::Sell].into_iter().collect(),
            allowed_time_in_force: [
                TimeInForce::Gtc,
                TimeInForce::Gtd,
                TimeInForce::Fok,
                TimeInForce::Fak,
            ]
            .into_iter()
            .collect(),
            max_quote_per_order: "1000".into(),
            max_shares_per_order: "1000".into(),
            max_open_orders: 100,
            max_open_notional_per_market: "10000".into(),
            max_net_position_per_token: "10000".into(),
            max_gross_exposure: "10000".into(),
            max_daily_matched_notional: "10000".into(),
            max_worst_price_distance: "1".into(),
            min_visible_depth: "0".into(),
            max_market_metadata_age_seconds: 30,
            max_order_book_age_seconds: 30,
            max_user_stream_age_seconds: 30,
            max_reconciliation_age_seconds: 60,
            cancel_on_halt: false,
        }
    }

    async fn reserve_for_preparation(store: &Store, intent: &IntentRecord) {
        store
            .transition_intent(
                intent.id,
                IntentState::Received,
                IntentState::Validating,
                None,
            )
            .await
            .unwrap();
        let market = MarketRules::test_default("0xabc", "123");
        let book = OrderBook {
            bids: vec![PriceLevel::new("0.49", "1000")],
            asks: vec![PriceLevel::new("0.51", "1000")],
            observed_at: Utc::now(),
            hash: Some("test-book".into()),
        };
        assert!(
            store
                .evaluate_and_reserve_risk(intent.id, &request(), &market, &book, &policy())
                .await
                .unwrap()
                .approved
        );
        store
            .transition_intent(
                intent.id,
                IntentState::Approved,
                IntentState::Preparing,
                None,
            )
            .await
            .unwrap();
    }

    async fn commit_resting_paper_buy(store: &Store) {
        store
            .commit_paper_order(&PaperOrderCommit {
                venue_order_id: "paper-resting-buy".into(),
                condition_id: "0xabc".into(),
                token_id: "123".into(),
                side: Side::Buy,
                state: OrderState::Live,
                price: Decimal::new(5, 1),
                fee_rate: Decimal::ZERO,
                original_quantity: Decimal::TEN,
                remaining_quantity: Decimal::TEN,
                filled_quantity: Decimal::ZERO,
                filled_price: None,
                quote_amount: Decimal::ZERO,
                fee_amount: Decimal::ZERO,
                reserved_quote: Decimal::new(5, 0),
                reserved_shares: Decimal::ZERO,
                venue_trade_ids: Vec::new(),
                evidence: serde_json::json!({"paper": true}),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn idempotency_returns_existing_and_rejects_changed_body() {
        let (_dir, store) = store().await;
        let first = store
            .admit_intent("strategy", "abcdefghijklmnop", &request())
            .await;
        let intent_id = match first.unwrap() {
            Admission::Created(intent) => intent.id,
            Admission::Existing(_) => unreachable!(),
        };
        store
            .transition_intent(
                intent_id,
                IntentState::Received,
                IntentState::Validating,
                None,
            )
            .await
            .unwrap();
        let second = store
            .admit_intent("strategy", "abcdefghijklmnop", &request())
            .await;
        assert!(matches!(
            second,
            Ok(Admission::Existing(IntentRecord {
                state: IntentState::Received,
                ..
            }))
        ));
        let mut changed = request();
        changed.quantity.value = "11".into();
        assert!(matches!(
            store
                .admit_intent("strategy", "abcdefghijklmnop", &changed)
                .await,
            Err(StoreError::IdempotencyConflict)
        ));
    }

    #[tokio::test]
    async fn halted_service_refuses_new_risk_but_preserves_exact_intent_replay() {
        let (_dir, store) = store().await;
        let request = request();
        let first = store
            .admit_intent("strategy", "halt-replay-key-0001", &request)
            .await
            .unwrap();
        store
            .set_service_state("operator", ServiceState::Halted, "test halt")
            .await
            .unwrap();

        assert!(matches!(
            store
                .admit_intent("strategy", "halt-replay-key-0001", &request)
                .await,
            Ok(Admission::Existing(_))
        ));
        assert!(matches!(
            store
                .admit_intent("strategy", "halt-new-risk-key-0001", &request)
                .await,
            Err(StoreError::NewRiskDisabled(state)) if state == "HALTED"
        ));
        assert!(matches!(first, Admission::Created(_)));
    }

    #[tokio::test]
    async fn operational_idempotency_is_atomic_and_replays_original_admission() {
        let (_dir, store) = store().await;
        let cancellation = CancellationRequest {
            order_id: None,
            intent_id: None,
            condition_id: None,
            all_open_orders: true,
            reason: "operator test".into(),
        };
        let key = "cancel-operation-0001";
        let (id, targets) = store
            .create_cancellation_idempotent("operator", key, &cancellation)
            .await
            .unwrap();
        assert!(targets.is_empty());
        assert_eq!(store.get_cancellation(id).await.unwrap().state, "PENDING");
        let body = serde_json::to_value(&cancellation).unwrap();
        let replay = store
            .idempotent_response("operator", key, "cancellation", &body)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.status, 202);
        assert_eq!(replay.body["id"], id.to_string());

        let changed = serde_json::json!({
            "order_id": null,
            "intent_id": null,
            "condition_id": null,
            "all_open_orders": true,
            "reason": "changed"
        });
        assert!(matches!(
            store
                .idempotent_response("operator", key, "cancellation", &changed)
                .await,
            Err(StoreError::IdempotencyConflict)
        ));
    }

    #[tokio::test]
    async fn backup_admission_replay_is_stable_while_resource_completes() {
        let (_dir, store) = store().await;
        let body = serde_json::json!({});
        let key = "backup-operation-0001";
        let backup = store
            .start_backup_idempotent("operator", key, &body)
            .await
            .unwrap();
        assert_eq!(backup.state, "RUNNING");
        let admitted = store
            .idempotent_response("operator", key, "backup", &body)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(admitted.body["state"], "RUNNING");

        store.run_backup(backup.id).await.unwrap();
        assert_eq!(store.get_backup(backup.id).await.unwrap().state, "COMPLETE");
        let replay = store
            .idempotent_response("operator", key, "backup", &body)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.body, admitted.body);
    }

    #[tokio::test]
    async fn interrupted_operational_resources_fail_closed() {
        let (_dir, store) = store().await;
        let body = serde_json::json!({});
        let reconciliation_id = store
            .start_reconciliation_idempotent(
                "operator",
                "reconcile-operation-0001",
                "operator",
                &body,
            )
            .await
            .unwrap();
        let backup = store
            .start_backup_idempotent("operator", "backup-operation-0002", &body)
            .await
            .unwrap();

        assert_eq!(store.fail_interrupted_reconciliations().await.unwrap(), 1);
        assert_eq!(store.fail_interrupted_backups().await.unwrap(), 1);
        assert_eq!(
            store
                .get_reconciliation(reconciliation_id)
                .await
                .unwrap()
                .state,
            "FAILED"
        );
        assert_eq!(store.get_backup(backup.id).await.unwrap().state, "FAILED");
        assert_eq!(store.unresolved_critical_findings().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn durable_event_retention_preserves_the_newest_sequences() {
        let (_dir, mut store) = store().await;
        store.event_retention = 3;
        for index in 0..5 {
            store
                .admit_intent("strategy", &format!("retention-key-{index:04}"), &request())
                .await
                .unwrap();
        }
        assert_eq!(store.prune_events().await.unwrap(), 2);
        let retained = store.replay_events(0, 100).await.unwrap();
        assert_eq!(retained.len(), 3);
        assert_eq!(retained[0].sequence, 3);
        assert_eq!(retained[2].sequence, 5);
    }

    #[tokio::test]
    async fn live_event_retention_waits_for_a_covering_backup() {
        let dir = tempdir().unwrap();
        let storage = StorageConfig {
            database_path: dir.path().join("live.sqlite3").display().to_string(),
            backup_dir: dir.path().join("backups").display().to_string(),
            event_retention: 2,
        };
        let store = Store::open(&storage, Mode::Live, &PolymarketConfig::default())
            .await
            .unwrap();
        mark_ready(&store).await;
        for index in 0..5 {
            store
                .admit_intent(
                    "strategy",
                    &format!("live-retention-{index:04}"),
                    &request(),
                )
                .await
                .unwrap();
        }
        assert_eq!(store.prune_events().await.unwrap(), 0);
        assert_eq!(store.replay_events(0, 100).await.unwrap().len(), 5);

        store.create_backup().await.unwrap();
        assert!(store.prune_events().await.unwrap() > 0);
        assert_eq!(store.replay_events(0, 100).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn durable_risk_reservation_blocks_a_second_intent_before_signing() {
        let (_dir, store) = store().await;
        let first = match store
            .admit_intent("strategy", "risk-reservation-0001", &request())
            .await
            .unwrap()
        {
            Admission::Created(intent) => intent,
            Admission::Existing(_) => unreachable!(),
        };
        let second = match store
            .admit_intent("strategy", "risk-reservation-0002", &request())
            .await
            .unwrap()
        {
            Admission::Created(intent) => intent,
            Admission::Existing(_) => unreachable!(),
        };
        for intent in [&first, &second] {
            store
                .transition_intent(
                    intent.id,
                    IntentState::Received,
                    IntentState::Validating,
                    None,
                )
                .await
                .unwrap();
        }
        let market = MarketRules::test_default("0xabc", "123");
        let book = OrderBook {
            bids: vec![PriceLevel::new("0.49", "1000")],
            asks: vec![PriceLevel::new("0.51", "1000")],
            observed_at: Utc::now(),
            hash: Some("reservation-book".into()),
        };
        let mut policy = policy();
        policy.max_open_orders = 1;
        assert!(
            store
                .evaluate_and_reserve_risk(first.id, &request(), &market, &book, &policy)
                .await
                .unwrap()
                .approved
        );
        let rejected = store
            .evaluate_and_reserve_risk(second.id, &request(), &market, &book, &policy)
            .await
            .unwrap();
        assert!(!rejected.approved);
        assert_eq!(rejected.reason_code, "RISK_MAX_OPEN_ORDERS");
        assert_eq!(
            store.get_intent(second.id).await.unwrap().state,
            IntentState::Rejected
        );
    }

    #[tokio::test]
    async fn prepared_immediate_buy_uses_the_conservative_reserved_share_quantity() {
        let (_dir, store) = store().await;
        let mut immediate = request();
        immediate.time_in_force = TimeInForce::Fak;
        immediate.quantity = Quantity {
            unit: QuantityUnit::Quote,
            value: "2".into(),
        };
        immediate.limit_price = None;
        immediate.worst_price = Some("0.5".into());
        immediate.post_only = false;
        let intent = match store
            .admit_intent("strategy", "immediate-reservation-0001", &immediate)
            .await
            .unwrap()
        {
            Admission::Created(intent) => intent,
            Admission::Existing(_) => unreachable!(),
        };
        store
            .transition_intent(
                intent.id,
                IntentState::Received,
                IntentState::Validating,
                None,
            )
            .await
            .unwrap();
        let mut market = MarketRules::test_default("0xabc", "123");
        market.tick_size = Decimal::new(1, 2);
        let book = OrderBook {
            bids: vec![PriceLevel::new("0.49", "1000")],
            asks: vec![PriceLevel::new("0.50", "1000")],
            observed_at: Utc::now(),
            hash: Some("immediate-book".into()),
        };
        assert!(
            store
                .evaluate_and_reserve_risk(intent.id, &immediate, &market, &book, &policy())
                .await
                .unwrap()
                .approved
        );
        store
            .transition_intent(
                intent.id,
                IntentState::Approved,
                IntentState::Preparing,
                None,
            )
            .await
            .unwrap();
        let order = store
            .create_prepared_order(
                &intent,
                &immediate,
                "{}",
                "immediate-prepared-order",
                None,
                None,
                None,
                2,
                "paper-v1",
                "test-policy",
                Decimal::ZERO,
            )
            .await
            .unwrap();

        assert_eq!(order.original_quantity, "200");
        let snapshot = store.risk_snapshot("0xabc", "123").await.unwrap();
        assert_eq!(snapshot.market_open_notional, Decimal::new(2, 0));
        assert_eq!(snapshot.token_pending_buys, Decimal::new(200, 0));
    }

    #[tokio::test]
    async fn preparation_failure_releases_the_durable_risk_reservation() {
        let (_dir, store) = store().await;
        let intent = match store
            .admit_intent("strategy", "risk-release-0001", &request())
            .await
            .unwrap()
        {
            Admission::Created(intent) => intent,
            Admission::Existing(_) => unreachable!(),
        };
        reserve_for_preparation(&store, &intent).await;
        assert_eq!(
            store
                .risk_snapshot("0xabc", "123")
                .await
                .unwrap()
                .open_order_count,
            1
        );
        store
            .reject_preparation(intent.id, "SIGNER_UNAVAILABLE", "fixture failure")
            .await
            .unwrap();
        let snapshot = store.risk_snapshot("0xabc", "123").await.unwrap();
        assert_eq!(snapshot.open_order_count, 0);
        assert_eq!(snapshot.gross_exposure, Decimal::ZERO);
        assert_eq!(
            store.get_intent(intent.id).await.unwrap().state,
            IntentState::Rejected
        );
    }

    #[tokio::test]
    async fn mode_is_bound_to_database() {
        let dir = tempdir().unwrap();
        let storage = StorageConfig {
            database_path: dir.path().join("database.sqlite3").display().to_string(),
            backup_dir: dir.path().join("backups").display().to_string(),
            event_retention: 100,
        };
        let store = Store::open(&storage, Mode::Paper, &PolymarketConfig::default())
            .await
            .unwrap();
        store.close().await;
        assert!(
            Store::open(&storage, Mode::Live, &PolymarketConfig::default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn startup_rejects_active_exposure_without_a_conservative_quote_bound() {
        let dir = tempdir().unwrap();
        let storage = StorageConfig {
            database_path: dir.path().join("database.sqlite3").display().to_string(),
            backup_dir: dir.path().join("backups").display().to_string(),
            event_retention: 100,
        };
        let store = Store::open(&storage, Mode::Paper, &PolymarketConfig::default())
            .await
            .unwrap();
        mark_ready(&store).await;
        let intent = match store
            .admit_intent("strategy", "legacy-risk-record-0001", &request())
            .await
            .unwrap()
        {
            Admission::Created(intent) => intent,
            Admission::Existing(_) => unreachable!(),
        };
        reserve_for_preparation(&store, &intent).await;
        sqlx::query("UPDATE risk_reservations SET quote_exposure='0' WHERE intent_id=?")
            .bind(intent.id)
            .execute(&store.pool)
            .await
            .unwrap();
        store.close().await;
        drop(store);

        assert!(matches!(
            Store::open(&storage, Mode::Paper, &PolymarketConfig::default()).await,
            Err(StoreError::IdentityMismatch(message))
                if message.contains("conservative quote exposure")
        ));
    }

    #[tokio::test]
    async fn backup_manifest_verifies_database_identity_and_integrity() {
        let (_dir, store) = store().await;
        let admitted = store
            .admit_intent("strategy", "backup-test-key-0001", &request())
            .await
            .unwrap();
        assert!(matches!(admitted, Admission::Created(_)));
        let backup = store.create_backup().await.unwrap();
        assert_eq!(backup.state, "COMPLETE");
        let manifest_path = PathBuf::from(backup.manifest_path.unwrap());
        let manifest = verify_backup(&manifest_path, Mode::Paper).await.unwrap();
        assert_eq!(manifest.mode, Mode::Paper);
        assert!(manifest.last_event_sequence > 0);
        let offline = verify_backup_offline(&manifest_path).await.unwrap();
        assert_eq!(offline.backup_id, manifest.backup_id);
        assert_eq!(offline.checksum_sha256, manifest.checksum_sha256);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn interrupted_submitting_attempt_becomes_unknown() {
        let (_dir, store) = store().await;
        let intent = match store
            .admit_intent("strategy", "recovery-test-key-01", &request())
            .await
            .unwrap()
        {
            Admission::Created(intent) => intent,
            Admission::Existing(_) => unreachable!(),
        };
        reserve_for_preparation(&store, &intent).await;
        let order = store
            .create_prepared_order(
                &intent,
                &request(),
                "{}",
                "paper_recovery_order",
                None,
                None,
                None,
                2,
                "paper-v1",
                "test-policy",
                Decimal::ZERO,
            )
            .await
            .unwrap();
        store.begin_submission(intent.id, order.id).await.unwrap();
        assert_eq!(store.recover_interrupted_submissions().await.unwrap(), 1);
        assert_eq!(
            store.get_intent(intent.id).await.unwrap().state,
            IntentState::Unknown
        );
        assert_eq!(
            store.get_order(order.id).await.unwrap().state,
            OrderState::Unknown
        );
        let unknown = store.unknown_submissions().await.unwrap().remove(0);
        store
            .resolve_unknown_submission(
                &unknown,
                "paper_recovery_order",
                OrderState::Filled,
                Decimal::ZERO,
                Decimal::new(10, 0),
                Some(Decimal::new(5, 1)),
                &["paper_recovery_trade".into()],
                &serde_json::json!({"paper": true, "positive_evidence": true}),
            )
            .await
            .unwrap();
        assert_eq!(
            store.get_intent(intent.id).await.unwrap().state,
            IntentState::Submitted
        );
        assert_eq!(store.list_trades(10).await.unwrap().len(), 1);
        assert_eq!(store.list_positions().await.unwrap()[0].shares, "10");
        assert!(
            store
                .record_venue_trade_finality(
                    order.id,
                    "paper_recovery_trade",
                    TradeState::Mined,
                    &serde_json::json!({"source": "test"}),
                )
                .await
                .unwrap()
        );
        assert_eq!(
            store.list_trades(10).await.unwrap()[0].status,
            TradeState::Mined
        );
        assert!(
            store
                .record_venue_trade_finality(
                    order.id,
                    "paper_recovery_trade",
                    TradeState::Confirmed,
                    &serde_json::json!({"source": "test"}),
                )
                .await
                .unwrap()
        );
        assert_eq!(
            store.list_trades(10).await.unwrap()[0].status,
            TradeState::Confirmed
        );
    }

    #[tokio::test]
    async fn pre_submission_recovery_reuses_the_persisted_prepared_attempt() {
        let (_dir, store) = store().await;
        let intent = match store
            .admit_intent("strategy", "recovery-test-key-02", &request())
            .await
            .unwrap()
        {
            Admission::Created(intent) => intent,
            Admission::Existing(_) => unreachable!(),
        };
        reserve_for_preparation(&store, &intent).await;
        let signed = r#"{"order":{"salt":"42"},"signature":"0x1234"}"#;
        let order = store
            .create_prepared_order(
                &intent,
                &request(),
                "{}",
                "persisted-order-id",
                Some(signed),
                Some("0xsigner"),
                Some("0xfunder"),
                2,
                "0.7.0",
                "test-policy",
                Decimal::ZERO,
            )
            .await
            .unwrap();

        assert_eq!(
            store.recover_pre_submission_work().await.unwrap(),
            vec![intent.id]
        );
        let recovered = store.prepared_submission(intent.id).await.unwrap();
        assert_eq!(recovered.order_id, order.id);
        assert_eq!(recovered.deterministic_order_id, "persisted-order-id");
        assert_eq!(recovered.signed_payload_json.as_deref(), Some(signed));
        assert_eq!(recovered.sdk_version, "0.7.0");
    }

    #[tokio::test]
    async fn pre_signing_recovery_restarts_from_received() {
        let (_dir, store) = store().await;
        let intent = match store
            .admit_intent("strategy", "recovery-test-key-03", &request())
            .await
            .unwrap()
        {
            Admission::Created(intent) => intent,
            Admission::Existing(_) => unreachable!(),
        };
        store
            .transition_intent(
                intent.id,
                IntentState::Received,
                IntentState::Validating,
                None,
            )
            .await
            .unwrap();
        store
            .transition_intent(
                intent.id,
                IntentState::Validating,
                IntentState::Approved,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            store.recover_pre_submission_work().await.unwrap(),
            vec![intent.id]
        );
        assert_eq!(
            store.get_intent(intent.id).await.unwrap().state,
            IntentState::Received
        );
        assert!(
            store
                .replay_events(0, 100)
                .await
                .unwrap()
                .iter()
                .any(|event| event.event_type == "intent.recovered_before_submission")
        );
    }

    #[tokio::test]
    async fn paper_ledger_survives_restart_and_releases_reservations_on_cancel() {
        let dir = tempdir().unwrap();
        let storage = StorageConfig {
            database_path: dir.path().join("paper.sqlite3").display().to_string(),
            backup_dir: dir.path().join("backups").display().to_string(),
            event_retention: 100,
        };
        let config = PaperConfig {
            starting_quote_balance: "100".into(),
            starting_positions: Vec::new(),
        };
        let store = Store::open(&storage, Mode::Paper, &PolymarketConfig::default())
            .await
            .unwrap();
        store.initialize_paper_account(&config).await.unwrap();
        store
            .commit_paper_order(&PaperOrderCommit {
                venue_order_id: "paper-durable-order".into(),
                condition_id: "0xabc".into(),
                token_id: "123".into(),
                side: Side::Buy,
                state: OrderState::Live,
                price: Decimal::new(5, 1),
                fee_rate: Decimal::ZERO,
                original_quantity: Decimal::new(10, 0),
                remaining_quantity: Decimal::new(10, 0),
                filled_quantity: Decimal::ZERO,
                filled_price: None,
                quote_amount: Decimal::ZERO,
                fee_amount: Decimal::ZERO,
                reserved_quote: Decimal::new(5, 0),
                reserved_shares: Decimal::ZERO,
                venue_trade_ids: Vec::new(),
                evidence: serde_json::json!({"paper": true, "case": "durable"}),
            })
            .await
            .unwrap();
        assert_eq!(
            store.paper_account_snapshot().await.unwrap().reserved_quote,
            Decimal::new(5, 0)
        );
        store.close().await;
        drop(store);

        let reopened = Store::open(&storage, Mode::Paper, &PolymarketConfig::default())
            .await
            .unwrap();
        reopened.initialize_paper_account(&config).await.unwrap();
        assert_eq!(
            reopened
                .find_paper_order("paper-durable-order")
                .await
                .unwrap()
                .unwrap()
                .state,
            OrderState::Live
        );
        reopened
            .cancel_paper_order("paper-durable-order")
            .await
            .unwrap();
        assert_eq!(
            reopened
                .paper_account_snapshot()
                .await
                .unwrap()
                .reserved_quote,
            Decimal::ZERO
        );
    }

    #[tokio::test]
    async fn paper_database_rejects_changed_starting_account_configuration() {
        let (_dir, store) = store().await;
        let first = PaperConfig {
            starting_quote_balance: "100".into(),
            starting_positions: Vec::new(),
        };
        store.initialize_paper_account(&first).await.unwrap();
        let changed = PaperConfig {
            starting_quote_balance: "101".into(),
            starting_positions: Vec::new(),
        };
        assert!(matches!(
            store.initialize_paper_account(&changed).await,
            Err(StoreError::IdentityMismatch(_))
        ));
    }

    #[tokio::test]
    async fn paper_resting_buy_fills_only_after_trade_through_and_deduplicates_events() {
        let (_dir, store) = store().await;
        store
            .initialize_paper_account(&PaperConfig::default())
            .await
            .unwrap();
        commit_resting_paper_buy(&store).await;
        assert_eq!(
            store
                .apply_paper_trade_through(
                    "touch",
                    "123",
                    Decimal::new(5, 1),
                    Decimal::new(4, 0),
                    Utc::now(),
                )
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .find_paper_order("paper-resting-buy")
                .await
                .unwrap()
                .unwrap()
                .filled_quantity,
            Decimal::ZERO
        );

        assert_eq!(
            store
                .apply_paper_trade_through(
                    "through",
                    "123",
                    Decimal::new(4, 1),
                    Decimal::new(4, 0),
                    Utc::now(),
                )
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .apply_paper_trade_through(
                    "through",
                    "123",
                    Decimal::new(4, 1),
                    Decimal::new(4, 0),
                    Utc::now(),
                )
                .await
                .unwrap(),
            0
        );
        let order = store
            .find_paper_order("paper-resting-buy")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(order.state, OrderState::PartiallyFilled);
        assert_eq!(order.filled_quantity, Decimal::new(4, 0));
        assert_eq!(order.remaining_quantity, Decimal::new(6, 0));
        let account = store.paper_account_snapshot().await.unwrap();
        assert_eq!(account.quote_balance, Decimal::new(99_984, 1));
        assert_eq!(account.reserved_quote, Decimal::new(3, 0));
        assert_eq!(
            account.positions.get("123"),
            Some(&(Decimal::new(4, 0), Decimal::ZERO))
        );
    }

    #[tokio::test]
    async fn paper_trade_event_that_predates_an_order_cannot_fill_it() {
        let (_dir, store) = store().await;
        store
            .initialize_paper_account(&PaperConfig::default())
            .await
            .unwrap();
        let before_order = Utc::now() - chrono::Duration::seconds(1);
        commit_resting_paper_buy(&store).await;

        assert_eq!(
            store
                .apply_paper_trade_through(
                    "predates-order",
                    "123",
                    Decimal::new(4, 1),
                    Decimal::new(4, 0),
                    before_order,
                )
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .find_paper_order("paper-resting-buy")
                .await
                .unwrap()
                .unwrap()
                .filled_quantity,
            Decimal::ZERO
        );
    }

    #[tokio::test]
    async fn cancelling_prepared_work_releases_risk_without_venue_assumption() {
        let (_dir, store) = store().await;
        let intent = match store
            .admit_intent("strategy", "cancel-prepared-key-01", &request())
            .await
            .unwrap()
        {
            Admission::Created(intent) => intent,
            Admission::Existing(_) => unreachable!(),
        };
        reserve_for_preparation(&store, &intent).await;
        let order = store
            .create_prepared_order(
                &intent,
                &request(),
                "{}",
                "cancel-prepared-order",
                None,
                None,
                None,
                2,
                "paper-v1",
                "test-policy",
                Decimal::ZERO,
            )
            .await
            .unwrap();
        store
            .set_service_state("operator", ServiceState::Halted, "cancel prepared fixture")
            .await
            .unwrap();
        assert!(matches!(
            store.begin_submission(intent.id, order.id).await,
            Err(StoreError::NewRiskDisabled(state)) if state == "HALTED"
        ));
        store
            .cancel_prepared_order(order.id, &serde_json::json!({"operator": true}))
            .await
            .unwrap();
        assert_eq!(
            store.get_intent(intent.id).await.unwrap().state,
            IntentState::Rejected
        );
        assert_eq!(
            store.get_order(order.id).await.unwrap().state,
            OrderState::Cancelled
        );
        assert_eq!(
            store
                .risk_snapshot("0xabc", "123")
                .await
                .unwrap()
                .open_order_count,
            0
        );
    }
}
