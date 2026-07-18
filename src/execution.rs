use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::json;
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, watch},
    task::JoinHandle,
};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    domain::{
        CancellationRequest, IntentState, OrderIntentRequest, OrderState, ServiceState, Side,
        TimeInForce,
    },
    risk::RiskPolicy,
    store::{Store, StoreError},
    venue::{ExecutionVenue, MarketRules, OrderBook, PreparedVenueOrder, VenueError},
};

const VENUE_CALL_TIMEOUT: Duration = Duration::from_secs(10);
// polymarket_client_sdk_v2 0.7.0 may poll trade IDs for up to 30 seconds
// after a successful placement to backfill transaction hashes.
const SUBMISSION_CALL_TIMEOUT: Duration = Duration::from_secs(35);
const RECONCILIATION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct ExecutionCoordinator {
    store: Store,
    venue: Arc<dyn ExecutionVenue>,
    policy: Arc<RiskPolicy>,
    queue: mpsc::Sender<Uuid>,
    shutdown: watch::Sender<bool>,
    deferred_recovery: Arc<Mutex<Vec<Uuid>>>,
    safety_gate: Arc<Mutex<()>>,
    order_lifecycle_gate: Arc<Mutex<()>>,
    cancellation_gate: Arc<Mutex<()>>,
    operation_slots: Arc<Semaphore>,
    last_healthy_heartbeat: Arc<Mutex<Option<Instant>>>,
}

pub struct CoordinatorTasks {
    worker: JoinHandle<()>,
    reconciliation: JoinHandle<()>,
    heartbeat: Option<JoinHandle<()>>,
}

impl CoordinatorTasks {
    pub async fn shutdown(self) {
        let _ = self.worker.await;
        let _ = self.reconciliation.await;
        if let Some(heartbeat) = self.heartbeat {
            let _ = heartbeat.await;
        }
    }
}

impl ExecutionCoordinator {
    #[allow(clippy::too_many_lines)]
    pub fn start(
        store: Store,
        venue: Arc<dyn ExecutionVenue>,
        policy: RiskPolicy,
        reconciliation_interval: Duration,
        heartbeat_interval: Duration,
    ) -> (Self, CoordinatorTasks) {
        let (queue, mut receiver) = mpsc::channel::<Uuid>(100);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let coordinator = Self {
            store,
            venue,
            policy: Arc::new(policy),
            queue,
            shutdown,
            deferred_recovery: Arc::new(Mutex::new(Vec::new())),
            safety_gate: Arc::new(Mutex::new(())),
            order_lifecycle_gate: Arc::new(Mutex::new(())),
            cancellation_gate: Arc::new(Mutex::new(())),
            operation_slots: Arc::new(Semaphore::new(64)),
            last_healthy_heartbeat: Arc::new(Mutex::new(None)),
        };

        let worker_coordinator = coordinator.clone();
        let mut worker_shutdown = shutdown_rx.clone();
        let worker = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(intent_id) = receiver.recv() => {
                        if let Err(error) = worker_coordinator.process_intent(intent_id).await {
                            error!(%intent_id, error = %error, "intent processing failed");
                        }
                    }
                    result = worker_shutdown.changed() => {
                        if result.is_err() || *worker_shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
        });

        let reconciliation_coordinator = coordinator.clone();
        let mut reconciliation_shutdown = shutdown_rx.clone();
        let reconciliation = tokio::spawn(async move {
            let mut interval = tokio::time::interval(reconciliation_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(error) = reconciliation_coordinator.reconcile("scheduled").await {
                            warn!(error = %error, "scheduled reconciliation failed");
                        }
                    }
                    result = reconciliation_shutdown.changed() => {
                        if result.is_err() || *reconciliation_shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
        });

        let heartbeat = if coordinator.venue.requires_heartbeat() {
            let heartbeat_coordinator = coordinator.clone();
            let mut heartbeat_shutdown = shutdown_rx;
            Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(heartbeat_interval);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                interval.tick().await;
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let heartbeat = tokio::time::timeout(
                                heartbeat_interval,
                                heartbeat_coordinator.venue.heartbeat(),
                            )
                            .await;
                            match heartbeat {
                                Ok(Ok(())) => {
                                    *heartbeat_coordinator.last_healthy_heartbeat.lock().await =
                                        Some(Instant::now());
                                    metrics::counter!(
                                        "oddsfox_heartbeats_total",
                                        "result" => "success"
                                    )
                                    .increment(1);
                                    metrics::gauge!("oddsfox_heartbeat_healthy").set(1.0);
                                    if heartbeat_coordinator.venue.reconciliation_required()
                                        && let Err(error) = heartbeat_coordinator
                                            .reconcile("user_stream_event")
                                            .await
                                    {
                                        error!(%error, "user-stream reconciliation failed");
                                    }
                                }
                                Ok(Err(error)) => {
                                    metrics::counter!(
                                        "oddsfox_heartbeats_total",
                                        "result" => "failure"
                                    )
                                    .increment(1);
                                    metrics::gauge!("oddsfox_heartbeat_healthy").set(0.0);
                                    error!(error = %error, "venue heartbeat failed");
                                    if let Err(state_error) = heartbeat_coordinator
                                        .halt("system", "venue heartbeat failed")
                                        .await
                                    {
                                        error!(error = %state_error, "failed to persist heartbeat halt");
                                    }
                                    let _ = heartbeat_coordinator.reconcile("heartbeat_failure").await;
                                }
                                Err(_) => {
                                    metrics::counter!(
                                        "oddsfox_heartbeats_total",
                                        "result" => "timeout"
                                    )
                                    .increment(1);
                                    metrics::gauge!("oddsfox_heartbeat_healthy").set(0.0);
                                    error!("venue heartbeat deadline exceeded");
                                    if let Err(state_error) = heartbeat_coordinator
                                        .halt("system", "venue heartbeat deadline exceeded")
                                        .await
                                    {
                                        error!(error = %state_error, "failed to persist heartbeat timeout halt");
                                    }
                                    let _ = heartbeat_coordinator
                                        .reconcile("heartbeat_timeout")
                                        .await;
                                }
                            }
                        }
                        result = heartbeat_shutdown.changed() => {
                            if result.is_err() || *heartbeat_shutdown.borrow() {
                                break;
                            }
                        }
                    }
                }
            }))
        } else {
            None
        };

        (
            coordinator,
            CoordinatorTasks {
                worker,
                reconciliation,
                heartbeat,
            },
        )
    }

    pub fn enqueue(&self, intent_id: Uuid) -> Result<(), CoordinatorError> {
        self.queue
            .try_send(intent_id)
            .map_err(|_| CoordinatorError::QueueFull)
    }

    pub fn reserve_queue(&self) -> Result<tokio::sync::mpsc::OwnedPermit<Uuid>, CoordinatorError> {
        self.queue
            .clone()
            .try_reserve_owned()
            .map_err(|_| CoordinatorError::QueueFull)
    }

    pub fn reserve_operation(&self) -> Result<OwnedSemaphorePermit, CoordinatorError> {
        Arc::clone(&self.operation_slots)
            .try_acquire_owned()
            .map_err(|_| CoordinatorError::QueueFull)
    }

    async fn enqueue_recovered(&self, intent_id: Uuid) -> Result<(), CoordinatorError> {
        let state = self.store.get_intent(intent_id).await?.state;
        if !matches!(state, IntentState::Received | IntentState::Prepared) {
            return Ok(());
        }
        self.queue
            .send(intent_id)
            .await
            .map_err(|_| CoordinatorError::QueueFull)
    }

    async fn defer_recovery(&self, intent_id: Uuid) {
        let mut deferred = self.deferred_recovery.lock().await;
        if !deferred.contains(&intent_id) {
            deferred.push(intent_id);
        }
    }

    pub async fn startup(&self) -> Result<(), CoordinatorError> {
        let _safety_guard = self.safety_gate.lock().await;
        let (mut prior_state, _, _) = self.store.service_state().await?;
        if prior_state == ServiceState::ShuttingDown {
            self.store
                .set_service_state(
                    "system",
                    ServiceState::Halted,
                    "previous shutdown did not reach its durable completion marker",
                )
                .await?;
            prior_state = ServiceState::Halted;
        }
        if prior_state != ServiceState::Halted {
            self.store
                .set_service_state(
                    "system",
                    ServiceState::Reconciling,
                    "startup reconciliation",
                )
                .await?;
        }
        self.store.fail_interrupted_reconciliations().await?;
        self.store.fail_interrupted_backups().await?;
        let interrupted = self.store.recover_interrupted_submissions().await?;
        if interrupted > 0 {
            self.store
                .set_service_state(
                    "system",
                    ServiceState::Halted,
                    "interrupted submission requires positive venue reconciliation",
                )
                .await?;
        }
        let recoverable = self.store.recover_pre_submission_work().await?;
        for cancellation in self.store.pending_cancellations().await? {
            let targets: Vec<Uuid> = serde_json::from_str(&cancellation.target_order_ids_json)?;
            self.execute_cancellation(cancellation.id, &targets).await?;
        }
        if prior_state == ServiceState::Halted && self.policy.cancel_on_halt {
            let request = CancellationRequest {
                order_id: None,
                intent_id: None,
                condition_id: None,
                all_open_orders: true,
                reason: "startup_recovery_for_latched_halt".into(),
            };
            self.cancel_inner("system", &request).await?;
        }
        self.reconcile_inner("startup").await?;
        if self.venue.requires_heartbeat()
            && let Err(error) = self.supervised_heartbeat().await
        {
            warn!(%error, "startup heartbeat failed");
            self.halt_inner("system", "startup heartbeat failed")
                .await?;
        }
        let (state, _, _) = self.store.service_state().await?;
        if state == ServiceState::Halted {
            *self.deferred_recovery.lock().await = recoverable;
        } else {
            self.store
                .set_service_state("system", ServiceState::Ready, "startup checks passed")
                .await?;
            for intent_id in recoverable {
                self.enqueue_recovered(intent_id).await?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn process_intent(&self, intent_id: Uuid) -> Result<(), CoordinatorError> {
        let _safety_guard = self.safety_gate.lock().await;
        let _order_lifecycle_guard = self.order_lifecycle_gate.lock().await;
        self.process_intent_inner(intent_id).await
    }

    #[allow(clippy::too_many_lines)]
    async fn process_intent_inner(&self, intent_id: Uuid) -> Result<(), CoordinatorError> {
        if self.venue.reconciliation_required()
            && let Err(error) = self.reconcile_inner("pre_intent_user_stream_event").await
        {
            warn!(%intent_id, %error, "pre-intent user-stream reconciliation failed");
        }
        if let Some(reason) = self.execution_health_failure().await? {
            self.halt_inner("system", &reason).await?;
            self.reject_received(intent_id, "EXECUTION_HEALTH_STALE", &reason)
                .await?;
            return Ok(());
        }
        let (state, _, _) = self.store.service_state().await?;
        if state != ServiceState::Ready {
            self.reject_received(
                intent_id,
                "SERVICE_NOT_READY",
                "service is not ready for new risk",
            )
            .await?;
            return Ok(());
        }
        let intent = self.store.get_intent(intent_id).await?;
        if intent.state == IntentState::Prepared {
            let persisted = self.store.prepared_submission(intent_id).await?;
            let request = intent.request()?;
            let prepared = PreparedVenueOrder {
                deterministic_order_id: persisted.deterministic_order_id,
                normalized_json: persisted.normalized_json,
                signed_payload_json: persisted.signed_payload_json,
                signer_address: persisted.signer_address,
                funder_address: persisted.funder_address,
                protocol_version: persisted.protocol_version,
                sdk_version: persisted.sdk_version,
            };
            return self
                .submit_prepared(intent_id, persisted.order_id, &prepared, &request)
                .await;
        }
        if intent.state != IntentState::Received {
            return Err(CoordinatorError::Invalid(format!(
                "intent {intent_id} is not recoverable from state {:?}",
                intent.state
            )));
        }
        let request = intent.request()?;
        self.store
            .transition_intent(
                intent_id,
                IntentState::Received,
                IntentState::Validating,
                None,
            )
            .await?;
        if let Err(error) = request.validate(Utc::now()) {
            self.store
                .record_risk_decision(
                    intent_id,
                    false,
                    "ORDER_INVALID",
                    &json!({"error": error.to_string()}),
                    &self.policy.version,
                )
                .await?;
            self.store
                .transition_intent(
                    intent_id,
                    IntentState::Validating,
                    IntentState::Rejected,
                    Some(("ORDER_INVALID", &error.to_string())),
                )
                .await?;
            return Ok(());
        }

        let market = match tokio::time::timeout(
            VENUE_CALL_TIMEOUT,
            self.venue
                .market_rules(&request.condition_id, &request.token_id),
        )
        .await
        {
            Ok(Ok(market)) => market,
            Ok(Err(error)) => {
                self.reject_validating(intent_id, "MARKET_VALIDATION_FAILED", &error.to_string())
                    .await?;
                self.halt_for_venue_error(&error, "market_validation_safety_halt")
                    .await?;
                return Ok(());
            }
            Err(_) => {
                self.reject_validating(
                    intent_id,
                    "MARKET_VALIDATION_TIMEOUT",
                    "market validation deadline exceeded",
                )
                .await?;
                return Ok(());
            }
        };
        let book = match tokio::time::timeout(
            VENUE_CALL_TIMEOUT,
            self.venue.order_book(&request.token_id),
        )
        .await
        {
            Ok(Ok(book)) => book,
            Ok(Err(error)) => {
                self.reject_validating(intent_id, "ORDER_BOOK_UNAVAILABLE", &error.to_string())
                    .await?;
                self.halt_for_venue_error(&error, "order_book_safety_halt")
                    .await?;
                return Ok(());
            }
            Err(_) => {
                self.reject_validating(
                    intent_id,
                    "ORDER_BOOK_TIMEOUT",
                    "order book deadline exceeded",
                )
                .await?;
                return Ok(());
            }
        };
        if let Err((code, message)) =
            self.validate_freshness_and_increments(&request, &market, &book)
        {
            self.reject_validating(intent_id, code, &message).await?;
            return Ok(());
        }
        let decision = self
            .store
            .evaluate_and_reserve_risk(intent_id, &request, &market, &book, &self.policy)
            .await?;
        if !decision.approved {
            metrics::counter!(
                "oddsfox_risk_decisions_total",
                "result" => "rejected",
                "reason" => decision.reason_code
            )
            .increment(1);
            return Ok(());
        }
        metrics::counter!("oddsfox_risk_decisions_total", "result" => "approved").increment(1);
        self.store
            .transition_intent(
                intent_id,
                IntentState::Approved,
                IntentState::Preparing,
                None,
            )
            .await?;
        let prepared =
            match tokio::time::timeout(VENUE_CALL_TIMEOUT, self.venue.prepare(intent_id, &request))
                .await
            {
                Ok(Ok(prepared)) => prepared,
                Ok(Err(error)) => {
                    self.store
                        .reject_preparation(
                            intent_id,
                            "ORDER_PREPARATION_FAILED",
                            &error.to_string(),
                        )
                        .await?;
                    self.halt_for_venue_error(&error, "order_preparation_safety_halt")
                        .await?;
                    return Ok(());
                }
                Err(_) => {
                    self.store
                        .reject_preparation(
                            intent_id,
                            "ORDER_PREPARATION_TIMEOUT",
                            "order preparation deadline exceeded",
                        )
                        .await?;
                    return Ok(());
                }
            };
        let order = self
            .store
            .create_prepared_order(
                &intent,
                &request,
                &prepared.normalized_json,
                &prepared.deterministic_order_id,
                prepared.signed_payload_json.as_deref(),
                prepared.signer_address.as_deref(),
                prepared.funder_address.as_deref(),
                prepared.protocol_version,
                &prepared.sdk_version,
                &self.policy.version,
                market.maker_fee_rate.max(market.taker_fee_rate),
            )
            .await?;
        self.submit_prepared(intent_id, order.id, &prepared, &request)
            .await
    }

    async fn submit_prepared(
        &self,
        intent_id: Uuid,
        order_id: Uuid,
        prepared: &PreparedVenueOrder,
        request: &OrderIntentRequest,
    ) -> Result<(), CoordinatorError> {
        let (service_state, _, _) = self.store.service_state().await?;
        if service_state != ServiceState::Ready {
            if self.store.get_intent(intent_id).await?.state == IntentState::Prepared {
                self.defer_recovery(intent_id).await;
            }
            return Ok(());
        }
        match self.store.begin_submission(intent_id, order_id).await {
            Ok(()) => {}
            Err(StoreError::NewRiskDisabled(_)) => {
                self.defer_recovery(intent_id).await;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        }
        let submission = match tokio::time::timeout(
            SUBMISSION_CALL_TIMEOUT,
            self.venue.submit(prepared, request),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(VenueError::Ambiguous(
                "submission deadline exceeded after SUBMITTING commit".into(),
            )),
        };
        match submission {
            Ok(submission) => {
                self.store
                    .finish_submission(
                        intent_id,
                        order_id,
                        IntentState::Submitted,
                        submission.state,
                        Some(&submission.venue_order_id),
                        "VENUE_RESPONSE",
                        submission.filled_quantity,
                        submission.filled_price,
                        &submission.venue_trade_ids,
                        &submission.evidence,
                    )
                    .await?;
                metrics::counter!("oddsfox_intents_submitted_total").increment(1);
            }
            Err(error) if error.is_ambiguous() => {
                let evidence = json!({"error": error.to_string()});
                self.store
                    .finish_submission(
                        intent_id,
                        order_id,
                        IntentState::Unknown,
                        OrderState::Unknown,
                        Some(&prepared.deterministic_order_id),
                        "AMBIGUOUS",
                        Decimal::ZERO,
                        None,
                        &[],
                        &evidence,
                    )
                    .await?;
                self.halt_inner("system", "ambiguous venue submission")
                    .await?;
                metrics::counter!("oddsfox_intents_unknown_total").increment(1);
                let _ = self.reconcile_inner("ambiguous_submission").await;
            }
            Err(error) => {
                let evidence = json!({"error": error.to_string()});
                self.store
                    .finish_submission(
                        intent_id,
                        order_id,
                        IntentState::Submitted,
                        OrderState::Rejected,
                        None,
                        "DEFINITIVE_REJECTION",
                        Decimal::ZERO,
                        None,
                        &[],
                        &evidence,
                    )
                    .await?;
                if error.requires_halt() {
                    self.halt_inner("system", &error.to_string()).await?;
                    let _ = self.reconcile_inner("venue_safety_halt").await;
                }
            }
        }
        Ok(())
    }

    fn validate_freshness_and_increments(
        &self,
        request: &OrderIntentRequest,
        market: &MarketRules,
        book: &OrderBook,
    ) -> Result<(), (&'static str, String)> {
        let now = Utc::now();
        if market.condition_id != request.condition_id
            || market.token_id != request.token_id
            || market.tick_size <= Decimal::ZERO
            || market.tick_size >= Decimal::ONE
            || market.minimum_order_size <= Decimal::ZERO
            || market.maker_fee_rate < Decimal::ZERO
            || market.maker_fee_rate > Decimal::ONE
            || market.taker_fee_rate < Decimal::ZERO
            || market.taker_fee_rate > Decimal::ONE
        {
            return Err((
                "MARKET_RULES_INVALID",
                "venue market rules contain invalid identifiers, increments, or fees".into(),
            ));
        }
        if market.observed_at > now + chrono::Duration::seconds(5) {
            return Err((
                "MARKET_METADATA_INVALID_TIME",
                "market metadata timestamp is in the future".into(),
            ));
        }
        let market_age = now
            .signed_duration_since(market.observed_at)
            .num_seconds()
            .max(0)
            .cast_unsigned();
        if market_age > self.policy.max_market_metadata_age_seconds {
            return Err((
                "MARKET_METADATA_STALE",
                format!("market metadata is {market_age}s old"),
            ));
        }
        if book.observed_at > now + chrono::Duration::seconds(5) {
            return Err((
                "ORDER_BOOK_INVALID_TIME",
                "order book timestamp is in the future".into(),
            ));
        }
        let book_age = now
            .signed_duration_since(book.observed_at)
            .num_seconds()
            .max(0)
            .cast_unsigned();
        if book_age > self.policy.max_order_book_age_seconds {
            return Err(("ORDER_BOOK_STALE", format!("order book is {book_age}s old")));
        }
        let valid_levels = |levels: &[crate::venue::PriceLevel]| {
            levels.iter().all(|level| {
                level.price > Decimal::ZERO
                    && level.price < Decimal::ONE
                    && level.price % market.tick_size == Decimal::ZERO
                    && level.size > Decimal::ZERO
            })
        };
        if !valid_levels(&book.bids)
            || !valid_levels(&book.asks)
            || book
                .best_bid()
                .zip(book.best_ask())
                .is_some_and(|(bid, ask)| bid >= ask)
            || book
                .hash
                .as_ref()
                .is_some_and(|hash| hash.trim().is_empty())
        {
            return Err((
                "ORDER_BOOK_INVALID",
                "order book failed structural consistency checks".into(),
            ));
        }
        let price = request
            .protection_price()
            .map_err(|error| ("ORDER_INVALID_PRICE", error.to_string()))?;
        if price % market.tick_size != Decimal::ZERO {
            return Err((
                "ORDER_INVALID_TICK",
                format!("price must be a multiple of {}", market.tick_size),
            ));
        }
        let quantity = request
            .quantity_decimal()
            .map_err(|error| ("ORDER_INVALID_QUANTITY", error.to_string()))?;
        let shares = if request.side == Side::Buy
            && matches!(request.time_in_force, TimeInForce::Fok | TimeInForce::Fak)
        {
            quantity / price
        } else {
            quantity
        };
        if shares < market.minimum_order_size {
            return Err((
                "ORDER_BELOW_MINIMUM_SIZE",
                format!(
                    "share quantity {shares} is below {}",
                    market.minimum_order_size
                ),
            ));
        }
        Ok(())
    }

    async fn reject_received(
        &self,
        intent_id: Uuid,
        code: &str,
        message: &str,
    ) -> Result<(), CoordinatorError> {
        self.store
            .transition_intent(
                intent_id,
                IntentState::Received,
                IntentState::Rejected,
                Some((code, message)),
            )
            .await?;
        Ok(())
    }

    async fn reject_validating(
        &self,
        intent_id: Uuid,
        code: &str,
        message: &str,
    ) -> Result<(), CoordinatorError> {
        self.store
            .record_risk_decision(
                intent_id,
                false,
                code,
                &json!({"error": message}),
                &self.policy.version,
            )
            .await?;
        self.store
            .transition_intent(
                intent_id,
                IntentState::Validating,
                IntentState::Rejected,
                Some((code, message)),
            )
            .await?;
        Ok(())
    }

    async fn halt_for_venue_error(
        &self,
        error: &VenueError,
        reconciliation_trigger: &str,
    ) -> Result<(), CoordinatorError> {
        if error.requires_halt() {
            self.halt_inner("system", &error.to_string()).await?;
            let _ = self.reconcile_inner(reconciliation_trigger).await;
        }
        Ok(())
    }

    pub async fn cancel(
        &self,
        actor: &str,
        request: &CancellationRequest,
    ) -> Result<(Uuid, Vec<Uuid>), CoordinatorError> {
        let _cancellation_guard = self.cancellation_gate.lock().await;
        let _order_lifecycle_guard = self.order_lifecycle_gate.lock().await;
        self.cancel_inner(actor, request).await
    }

    async fn cancel_inner(
        &self,
        actor: &str,
        request: &CancellationRequest,
    ) -> Result<(Uuid, Vec<Uuid>), CoordinatorError> {
        request
            .validate()
            .map_err(|error| CoordinatorError::Invalid(error.to_string()))?;
        let (cancellation_id, targets) = self.store.create_cancellation(actor, request).await?;
        self.execute_cancellation(cancellation_id, &targets).await?;
        Ok((cancellation_id, targets))
    }

    pub async fn admit_cancellation(
        &self,
        actor: &str,
        idempotency_key: &str,
        request: &CancellationRequest,
    ) -> Result<(Uuid, Vec<Uuid>), CoordinatorError> {
        request
            .validate()
            .map_err(|error| CoordinatorError::Invalid(error.to_string()))?;
        Ok(self
            .store
            .create_cancellation_idempotent(actor, idempotency_key, request)
            .await?)
    }

    pub fn spawn_cancellation(
        &self,
        cancellation_id: Uuid,
        targets: Vec<Uuid>,
        permit: OwnedSemaphorePermit,
    ) {
        let coordinator = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _cancellation_guard = coordinator.cancellation_gate.lock().await;
            let _order_lifecycle_guard = coordinator.order_lifecycle_gate.lock().await;
            if let Err(error) = coordinator
                .execute_cancellation(cancellation_id, &targets)
                .await
            {
                error!(%cancellation_id, %error, "admitted cancellation failed");
            }
        });
    }

    async fn execute_cancellation(
        &self,
        cancellation_id: Uuid,
        targets: &[Uuid],
    ) -> Result<(), CoordinatorError> {
        let mut failed_order_ids = Vec::new();
        for order_id in targets {
            let order = self.store.get_order(*order_id).await?;
            if matches!(
                order.state,
                OrderState::Filled
                    | OrderState::Cancelled
                    | OrderState::Expired
                    | OrderState::Rejected
            ) {
                continue;
            }
            if order.state == OrderState::Prepared && order.venue_order_id.is_none() {
                self.store
                    .cancel_prepared_order(
                        *order_id,
                        &json!({
                            "cancellation_id": cancellation_id,
                            "reason": "cancelled_before_venue_submission"
                        }),
                    )
                    .await?;
                continue;
            }
            if order.state != OrderState::Unknown {
                let pending = self
                    .store
                    .transition_order(
                        *order_id,
                        OrderState::CancelPending,
                        None,
                        &json!({"cancellation_id": cancellation_id}),
                    )
                    .await?;
                if matches!(
                    pending.state,
                    OrderState::Filled
                        | OrderState::Cancelled
                        | OrderState::Expired
                        | OrderState::Rejected
                ) {
                    continue;
                }
            }
            if let Some(venue_order_id) = &order.venue_order_id {
                let cancellation = match tokio::time::timeout(
                    VENUE_CALL_TIMEOUT,
                    self.venue.cancel(venue_order_id),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(VenueError::Ambiguous(
                        "cancellation deadline exceeded".into(),
                    )),
                };
                match cancellation {
                    Ok(cancellation) => {
                        self.store
                            .transition_order(
                                *order_id,
                                cancellation.state,
                                None,
                                &cancellation.evidence,
                            )
                            .await?;
                    }
                    Err(error) => {
                        warn!(%order_id, error = %error, "cancellation requires reconciliation");
                        failed_order_ids.push(*order_id);
                        let _ = self.reconcile_inner("cancellation_failure").await;
                    }
                }
            } else {
                failed_order_ids.push(*order_id);
                warn!(
                    %order_id,
                    "non-prepared order has no venue identifier; cancellation requires reconciliation"
                );
                let _ = self.reconcile_inner("cancellation_missing_venue_id").await;
            }
        }
        self.store
            .finish_cancellation(cancellation_id, &failed_order_ids)
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    pub async fn reconcile(&self, trigger: &str) -> Result<Uuid, CoordinatorError> {
        let _safety_guard = self.safety_gate.lock().await;
        self.reconcile_inner(trigger).await
    }

    async fn reconcile_inner(&self, trigger: &str) -> Result<Uuid, CoordinatorError> {
        let id = self.store.start_reconciliation(trigger).await?;
        self.reconcile_started(id).await
    }

    pub async fn admit_reconciliation(
        &self,
        actor: &str,
        idempotency_key: &str,
        trigger: &str,
    ) -> Result<Uuid, CoordinatorError> {
        Ok(self
            .store
            .start_reconciliation_idempotent(
                actor,
                idempotency_key,
                trigger,
                &serde_json::json!({}),
            )
            .await?)
    }

    pub fn spawn_reconciliation(&self, id: Uuid, permit: OwnedSemaphorePermit) {
        let coordinator = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _safety_guard = coordinator.safety_gate.lock().await;
            if let Err(error) = coordinator.reconcile_started(id).await {
                error!(reconciliation_id = %id, %error, "admitted reconciliation failed");
            }
        });
    }

    async fn reconcile_started(&self, id: Uuid) -> Result<Uuid, CoordinatorError> {
        let result = self.reconcile_started_inner(id).await;
        if let Err(error) = &result {
            let error_message = error.to_string();
            let interrupted = self
                .store
                .get_reconciliation(id)
                .await
                .is_ok_and(|record| record.state == "RUNNING");
            if interrupted {
                if let Err(store_error) = self.store.fail_reconciliation(id, &error_message).await {
                    error!(reconciliation_id = %id, %store_error, "failed to close unsafe reconciliation record");
                }
                metrics::counter!(
                    "oddsfox_reconciliations_total",
                    "result" => "internal_failure"
                )
                .increment(1);
            }
            match self.store.service_state().await {
                Ok((ServiceState::Halted, _, _)) => {}
                Ok(_) => {
                    if let Err(store_error) = self
                        .store
                        .set_service_state(
                            "system",
                            ServiceState::Halted,
                            "reconciliation processing failed",
                        )
                        .await
                    {
                        error!(%store_error, "failed to persist reconciliation safety halt");
                    } else {
                        self.spawn_protective_cancellation("reconciliation processing failed");
                    }
                }
                Err(store_error) => {
                    error!(%store_error, "failed to inspect control state after reconciliation error");
                }
            }
        }
        result
    }

    #[allow(clippy::too_many_lines)]
    async fn reconcile_started_inner(&self, id: Uuid) -> Result<Uuid, CoordinatorError> {
        let started = Instant::now();
        let reconciliation =
            match tokio::time::timeout(RECONCILIATION_TIMEOUT, self.venue.reconcile()).await {
                Ok(result) => result,
                Err(_) => Err(VenueError::Unavailable(
                    "reconciliation deadline exceeded".into(),
                )),
            };
        match reconciliation {
            Ok(mut result) => {
                let (active_local_orders, known_venue_order_ids) =
                    self.store.reconciliation_orders().await?;
                let unexpected_order_ids: Vec<_> = result
                    .observed_orders
                    .iter()
                    .filter(|observed| !known_venue_order_ids.contains(&observed.venue_order_id))
                    .map(|observed| observed.venue_order_id.clone())
                    .collect();
                for venue_order_id in unexpected_order_ids {
                    result.critical_findings.push(format!(
                        "unexpected venue order {venue_order_id} has no local journal record"
                    ));
                    if self.policy.cancel_on_halt {
                        let cancellation = tokio::time::timeout(
                            VENUE_CALL_TIMEOUT,
                            self.venue.cancel(&venue_order_id),
                        )
                        .await;
                        match cancellation {
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) => result.critical_findings.push(format!(
                                "protective cancellation of unexpected venue order \
                                 {venue_order_id} failed: {error}"
                            )),
                            Err(_) => result.critical_findings.push(format!(
                                "protective cancellation of unexpected venue order \
                                 {venue_order_id} timed out"
                            )),
                        }
                    }
                }
                let observed_by_id: HashMap<_, _> = result
                    .observed_orders
                    .iter()
                    .cloned()
                    .map(|order| (order.venue_order_id.clone(), order))
                    .collect();
                for local in active_local_orders {
                    let venue_order_id = local
                        .venue_order_id
                        .as_deref()
                        .expect("reconciliation query returns venue IDs");
                    let observed = if let Some(observed) = observed_by_id.get(venue_order_id) {
                        Some(observed.clone())
                    } else {
                        self.find_venue_order(venue_order_id).await?
                    };
                    if let Some(observed) = observed {
                        if observed.state == OrderState::Unknown {
                            result.critical_findings.push(format!(
                                "venue order {venue_order_id} has an unrecognized state"
                            ));
                            continue;
                        }
                        self.store
                            .reconcile_order_projection(
                                local.id,
                                observed.state,
                                observed.remaining_quantity,
                                observed.filled_quantity,
                                observed.filled_price,
                                &observed.venue_trade_ids,
                                &observed.evidence,
                            )
                            .await?;
                    } else {
                        result.critical_findings.push(format!(
                            "local open order {} ({venue_order_id}) is missing at venue",
                            local.id
                        ));
                    }
                }
                for unknown in self.store.unknown_submissions().await? {
                    match self
                        .find_venue_order(&unknown.deterministic_order_id)
                        .await?
                    {
                        Some(observed) if observed.state != OrderState::Unknown => {
                            self.store
                                .resolve_unknown_submission(
                                    &unknown,
                                    &observed.venue_order_id,
                                    observed.state,
                                    observed.remaining_quantity,
                                    observed.filled_quantity,
                                    observed.filled_price,
                                    &observed.venue_trade_ids,
                                    &observed.evidence,
                                )
                                .await?;
                        }
                        _ => result.critical_findings.push(format!(
                            "unresolved ambiguous order {} ({})",
                            unknown.order_id, unknown.deterministic_order_id
                        )),
                    }
                }
                for observed_trade in &result.observed_trades {
                    let order_ids = self
                        .store
                        .order_ids_for_venue_ids(&observed_trade.venue_order_ids)
                        .await?;
                    match order_ids.as_slice() {
                        [order_id] => {
                            if !self
                                .store
                                .record_venue_trade_finality(
                                    *order_id,
                                    &observed_trade.venue_trade_id,
                                    observed_trade.status,
                                    &observed_trade.evidence,
                                )
                                .await?
                            {
                                result.critical_findings.push(format!(
                                    "venue trade {} is not linked to a local matched fill",
                                    observed_trade.venue_trade_id
                                ));
                            }
                        }
                        [] => result.critical_findings.push(format!(
                            "venue trade {} has no local order",
                            observed_trade.venue_trade_id
                        )),
                        _ => result.critical_findings.push(format!(
                            "venue trade {} maps to multiple local orders",
                            observed_trade.venue_trade_id
                        )),
                    }
                }
                let positions: Vec<_> = result
                    .observed_positions
                    .iter()
                    .map(|position| {
                        (
                            position.condition_id.clone(),
                            position.token_id.clone(),
                            position.shares,
                        )
                    })
                    .collect();
                self.store.reconcile_venue_positions(&positions).await?;
                let summary = serde_json::to_value(&result)?;
                self.store.finish_reconciliation(id, &summary).await?;
                self.store.prune_events().await?;
                if !result.critical_findings.is_empty() {
                    self.store
                        .set_service_state(
                            "system",
                            ServiceState::Halted,
                            "critical reconciliation finding",
                        )
                        .await?;
                    self.spawn_protective_cancellation("critical reconciliation finding");
                }
                metrics::histogram!("oddsfox_reconciliation_duration_seconds")
                    .record(started.elapsed().as_secs_f64());
                let critical_finding_count =
                    u32::try_from(result.critical_findings.len()).unwrap_or(u32::MAX);
                metrics::gauge!("oddsfox_reconciliation_critical_findings")
                    .set(f64::from(critical_finding_count));
                metrics::counter!("oddsfox_reconciliations_total", "result" => "success")
                    .increment(1);
                Ok(id)
            }
            Err(error) => {
                self.store
                    .fail_reconciliation(id, &error.to_string())
                    .await?;
                self.store
                    .set_service_state("system", ServiceState::Halted, "reconciliation failed")
                    .await?;
                self.spawn_protective_cancellation("reconciliation failed");
                metrics::histogram!("oddsfox_reconciliation_duration_seconds")
                    .record(started.elapsed().as_secs_f64());
                metrics::gauge!("oddsfox_reconciliation_critical_findings").set(1.0);
                metrics::counter!("oddsfox_reconciliations_total", "result" => "failure")
                    .increment(1);
                Err(error.into())
            }
        }
    }

    async fn find_venue_order(
        &self,
        deterministic_order_id: &str,
    ) -> Result<Option<crate::venue::ObservedVenueOrder>, CoordinatorError> {
        match tokio::time::timeout(
            VENUE_CALL_TIMEOUT,
            self.venue.find_order(deterministic_order_id),
        )
        .await
        {
            Ok(result) => Ok(result?),
            Err(_) => Err(CoordinatorError::Invalid(format!(
                "venue order lookup timed out for {deterministic_order_id}"
            ))),
        }
    }

    async fn supervised_heartbeat(&self) -> Result<(), CoordinatorError> {
        match tokio::time::timeout(VENUE_CALL_TIMEOUT, self.venue.heartbeat()).await {
            Ok(result) => result?,
            Err(_) => {
                return Err(CoordinatorError::Invalid(
                    "venue heartbeat deadline exceeded".into(),
                ));
            }
        }
        *self.last_healthy_heartbeat.lock().await = Some(Instant::now());
        Ok(())
    }

    async fn execution_health_failure(&self) -> Result<Option<String>, CoordinatorError> {
        let reconciliation = self.store.latest_successful_reconciliation().await?;
        let reconciliation_stale = reconciliation.is_none_or(|completed_at| {
            Utc::now()
                .signed_duration_since(completed_at)
                .num_seconds()
                .max(0)
                .cast_unsigned()
                > self.policy.max_reconciliation_age_seconds
        });
        if reconciliation_stale {
            return Ok(Some(
                "latest successful reconciliation is missing or stale".into(),
            ));
        }
        if self.venue.requires_heartbeat() {
            let heartbeat_stale =
                self.last_healthy_heartbeat
                    .lock()
                    .await
                    .is_none_or(|observed_at| {
                        observed_at.elapsed()
                            > Duration::from_secs(self.policy.max_user_stream_age_seconds)
                    });
            if heartbeat_stale {
                return Ok(Some("venue heartbeat is missing or stale".into()));
            }
        }
        Ok(None)
    }

    pub async fn resume(&self, actor: &str, reason: &str) -> Result<(), CoordinatorError> {
        let _safety_guard = self.safety_gate.lock().await;
        if reason.trim().is_empty() {
            return Err(CoordinatorError::Invalid("reason is required".into()));
        }
        let (initial_state, _, _, initial_revision) =
            self.store.service_state_with_revision().await?;
        if initial_state != ServiceState::Halted {
            return Err(CoordinatorError::Invalid(
                "resume requires the service to be halted".into(),
            ));
        }
        self.reconcile_inner("pre_resume").await?;
        self.supervised_heartbeat().await?;
        if self.store.unresolved_critical_findings().await? > 0 {
            return Err(CoordinatorError::Invalid(
                "unresolved critical reconciliation finding prevents resume".into(),
            ));
        }
        self.store
            .set_service_state_if_unchanged(actor, ServiceState::Ready, reason, initial_revision)
            .await?;
        let recoverable = std::mem::take(&mut *self.deferred_recovery.lock().await);
        for intent_id in recoverable {
            self.enqueue_recovered(intent_id).await?;
        }
        Ok(())
    }

    pub async fn resume_idempotent(
        &self,
        actor: &str,
        idempotency_key: &str,
        reason: &str,
    ) -> Result<(), CoordinatorError> {
        let _safety_guard = self.safety_gate.lock().await;
        if reason.trim().is_empty() {
            return Err(CoordinatorError::Invalid("reason is required".into()));
        }
        let (initial_state, _, _, initial_revision) =
            self.store.service_state_with_revision().await?;
        if initial_state != ServiceState::Halted {
            return Err(CoordinatorError::Invalid(
                "resume requires the service to be halted".into(),
            ));
        }
        self.reconcile_inner("pre_resume").await?;
        self.supervised_heartbeat().await?;
        if self.store.unresolved_critical_findings().await? > 0 {
            return Err(CoordinatorError::Invalid(
                "unresolved critical reconciliation finding prevents resume".into(),
            ));
        }
        let request_body = serde_json::json!({"reason": reason});
        let response_body = serde_json::json!({
            "state": "READY",
            "mode": self.store.mode()
        });
        self.store
            .set_service_state_idempotent_if_unchanged(
                actor,
                ServiceState::Ready,
                reason,
                idempotency_key,
                "control.resume",
                &request_body,
                &response_body,
                initial_revision,
            )
            .await?;
        let recoverable = std::mem::take(&mut *self.deferred_recovery.lock().await);
        for intent_id in recoverable {
            self.enqueue_recovered(intent_id).await?;
        }
        Ok(())
    }

    pub async fn halt(&self, actor: &str, reason: &str) -> Result<(), CoordinatorError> {
        if reason.trim().is_empty() {
            return Err(CoordinatorError::Invalid("reason is required".into()));
        }
        self.store
            .set_service_state(actor, ServiceState::Halted, reason)
            .await?;
        let _safety_guard = self.safety_gate.lock().await;
        self.cancel_on_halt_inner(reason).await
    }

    pub async fn halt_idempotent(
        &self,
        actor: &str,
        idempotency_key: &str,
        reason: &str,
    ) -> Result<(), CoordinatorError> {
        if reason.trim().is_empty() {
            return Err(CoordinatorError::Invalid("reason is required".into()));
        }
        let request_body = serde_json::json!({"reason": reason});
        let response_body = serde_json::json!({
            "state": "HALTED",
            "mode": self.store.mode()
        });
        self.store
            .set_service_state_idempotent(
                actor,
                ServiceState::Halted,
                reason,
                idempotency_key,
                "control.halt",
                &request_body,
                &response_body,
            )
            .await?;
        self.spawn_protective_cancellation(reason);
        Ok(())
    }

    async fn halt_inner(&self, actor: &str, reason: &str) -> Result<(), CoordinatorError> {
        if reason.trim().is_empty() {
            return Err(CoordinatorError::Invalid("reason is required".into()));
        }
        self.store
            .set_service_state(actor, ServiceState::Halted, reason)
            .await?;
        self.cancel_on_halt_inner(reason).await
    }

    async fn cancel_on_halt_inner(&self, reason: &str) -> Result<(), CoordinatorError> {
        if self.policy.cancel_on_halt {
            let request = CancellationRequest {
                order_id: None,
                intent_id: None,
                condition_id: None,
                all_open_orders: true,
                reason: format!("automatic_cancel_on_halt:{reason}"),
            };
            if let Err(error) = self.cancel_inner("system", &request).await {
                error!(%error, "automatic cancel-on-halt failed");
            }
        }
        Ok(())
    }

    fn spawn_protective_cancellation(&self, reason: &str) {
        if !self.policy.cancel_on_halt {
            return;
        }
        let coordinator = self.clone();
        let reason = reason.to_owned();
        tokio::spawn(async move {
            let request = CancellationRequest {
                order_id: None,
                intent_id: None,
                condition_id: None,
                all_open_orders: true,
                reason: format!("automatic_cancel_on_halt:{reason}"),
            };
            if let Err(error) = coordinator.cancel("system", &request).await {
                error!(%error, "deferred protective cancellation failed");
            }
        });
    }

    pub async fn shutdown(&self, cancel_on_halt: bool) -> Result<(), CoordinatorError> {
        self.store
            .set_service_state("system", ServiceState::ShuttingDown, "signal received")
            .await?;
        if cancel_on_halt {
            let request = CancellationRequest {
                order_id: None,
                intent_id: None,
                condition_id: None,
                all_open_orders: true,
                reason: "graceful_shutdown".into(),
            };
            self.cancel("system", &request).await?;
        }
        self.reconcile("shutdown").await?;
        self.store
            .set_service_state(
                "system",
                ServiceState::Halted,
                "graceful shutdown completed; explicit resume is required",
            )
            .await?;
        self.store.checkpoint().await?;
        let _ = self.shutdown.send(true);
        info!("execution coordinator stopped");
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Venue(#[from] VenueError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("execution queue is full")]
    QueueFull,
    #[error("invalid request: {0}")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        config::{PolymarketConfig, StorageConfig},
        domain::{ClientContext, Mode, Quantity, QuantityUnit},
        store::Admission,
        venue::{
            ObservedVenueOrder, PreparedVenueOrder, PriceLevel, ReconciliationResult,
            VenueCancellation, VenueSubmission,
        },
    };

    #[derive(Debug, Default)]
    struct FakeVenue {
        submissions: AtomicUsize,
        heartbeat_required: bool,
        heartbeat_fails: bool,
    }

    #[async_trait]
    impl ExecutionVenue for FakeVenue {
        async fn market_rules(
            &self,
            condition_id: &str,
            token_id: &str,
        ) -> Result<MarketRules, VenueError> {
            Ok(MarketRules::test_default(condition_id, token_id))
        }

        async fn order_book(&self, _token_id: &str) -> Result<OrderBook, VenueError> {
            Ok(OrderBook {
                bids: vec![PriceLevel::new("0.49", "1000")],
                asks: vec![PriceLevel::new("0.51", "1000")],
                observed_at: Utc::now(),
                hash: Some("fixture-book".into()),
            })
        }

        async fn prepare(
            &self,
            intent_id: Uuid,
            request: &OrderIntentRequest,
        ) -> Result<PreparedVenueOrder, VenueError> {
            Ok(PreparedVenueOrder {
                deterministic_order_id: format!("fake-{intent_id}"),
                normalized_json: serde_jcs::to_string(request).unwrap(),
                signed_payload_json: Some(format!(r#"{{"intent_id":"{intent_id}"}}"#)),
                signer_address: Some("0xsigner".into()),
                funder_address: Some("0xfunder".into()),
                protocol_version: 2,
                sdk_version: "fake-v1".into(),
            })
        }

        async fn submit(
            &self,
            prepared: &PreparedVenueOrder,
            _request: &OrderIntentRequest,
        ) -> Result<VenueSubmission, VenueError> {
            self.submissions.fetch_add(1, Ordering::SeqCst);
            Ok(VenueSubmission {
                venue_order_id: prepared.deterministic_order_id.clone(),
                state: OrderState::Live,
                filled_quantity: Decimal::ZERO,
                filled_price: None,
                venue_trade_ids: Vec::new(),
                evidence: json!({"fixture": true}),
            })
        }

        async fn cancel(&self, _venue_order_id: &str) -> Result<VenueCancellation, VenueError> {
            Ok(VenueCancellation {
                state: OrderState::Cancelled,
                evidence: json!({"fixture": true, "cancelled": true}),
            })
        }

        async fn find_order(
            &self,
            deterministic_order_id: &str,
        ) -> Result<Option<ObservedVenueOrder>, VenueError> {
            Ok(deterministic_order_id
                .starts_with("fake-")
                .then(|| ObservedVenueOrder {
                    venue_order_id: deterministic_order_id.into(),
                    state: OrderState::Live,
                    remaining_quantity: Decimal::ONE,
                    filled_quantity: Decimal::ZERO,
                    filled_price: None,
                    venue_trade_ids: Vec::new(),
                    evidence: json!({"fixture": true, "lookup": true}),
                }))
        }

        async fn reconcile(&self) -> Result<ReconciliationResult, VenueError> {
            Ok(ReconciliationResult {
                orders_observed: self.submissions.load(Ordering::SeqCst),
                trades_observed: 0,
                observed_orders: Vec::new(),
                observed_trades: Vec::new(),
                observed_positions: Vec::new(),
                critical_findings: Vec::new(),
            })
        }

        async fn heartbeat(&self) -> Result<(), VenueError> {
            if self.heartbeat_fails {
                Err(VenueError::Unavailable("fixture heartbeat failure".into()))
            } else {
                Ok(())
            }
        }

        fn requires_heartbeat(&self) -> bool {
            self.heartbeat_required
        }
    }

    fn request() -> OrderIntentRequest {
        OrderIntentRequest {
            condition_id: "0xabc".into(),
            token_id: "123".into(),
            side: Side::Buy,
            time_in_force: TimeInForce::Gtc,
            quantity: Quantity {
                unit: QuantityUnit::Shares,
                value: "1".into(),
            },
            limit_price: Some("0.50".into()),
            worst_price: None,
            expires_at: None,
            post_only: true,
            client_context: ClientContext::default(),
        }
    }

    fn policy() -> RiskPolicy {
        RiskPolicy {
            version: "serialized-risk-v1".into(),
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
            max_quote_per_order: "100".into(),
            max_shares_per_order: "100".into(),
            max_open_orders: 1,
            max_open_notional_per_market: "100".into(),
            max_net_position_per_token: "100".into(),
            max_gross_exposure: "100".into(),
            max_daily_matched_notional: "100".into(),
            max_worst_price_distance: "0.10".into(),
            min_visible_depth: "1".into(),
            max_market_metadata_age_seconds: 30,
            max_order_book_age_seconds: 30,
            max_user_stream_age_seconds: 30,
            max_reconciliation_age_seconds: 60,
            cancel_on_halt: false,
        }
    }

    #[tokio::test]
    async fn serialized_worker_prevents_concurrent_risk_oversubscription() {
        let dir = tempdir().unwrap();
        let storage = StorageConfig {
            database_path: dir.path().join("paper.sqlite3").display().to_string(),
            backup_dir: dir.path().join("backups").display().to_string(),
            event_retention: 100,
        };
        let store = Store::open(&storage, Mode::Paper, &PolymarketConfig::default())
            .await
            .unwrap();
        let venue = Arc::new(FakeVenue::default());
        let (coordinator, tasks) = ExecutionCoordinator::start(
            store.clone(),
            venue.clone(),
            policy(),
            Duration::from_secs(3_600),
            Duration::from_secs(5),
        );
        coordinator.startup().await.unwrap();
        let mut ids = Vec::new();
        for index in 0..8 {
            let key = format!("serialized-risk-{index:04}");
            let intent = match store
                .admit_intent("strategy", &key, &request())
                .await
                .unwrap()
            {
                Admission::Created(intent) => intent,
                Admission::Existing(_) => unreachable!(),
            };
            ids.push(intent.id);
            coordinator.enqueue(intent.id).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut terminal = true;
                for id in &ids {
                    terminal &= matches!(
                        store.get_intent(*id).await.unwrap().state,
                        IntentState::Submitted | IntentState::Rejected
                    );
                }
                if terminal {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(venue.submissions.load(Ordering::SeqCst), 1);
        assert_eq!(store.list_orders(100).await.unwrap().len(), 1);
        let mut rejected = 0;
        for id in &ids {
            if store.get_intent(*id).await.unwrap().state == IntentState::Rejected {
                rejected += 1;
            }
        }
        assert_eq!(rejected, 7);
        coordinator.shutdown(false).await.unwrap();
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn interrupted_shutdown_restarts_latched_halted() {
        let dir = tempdir().unwrap();
        let storage = StorageConfig {
            database_path: dir.path().join("paper.sqlite3").display().to_string(),
            backup_dir: dir.path().join("backups").display().to_string(),
            event_retention: 100,
        };
        let store = Store::open(&storage, Mode::Paper, &PolymarketConfig::default())
            .await
            .unwrap();
        store
            .set_service_state(
                "test",
                ServiceState::ShuttingDown,
                "simulated interrupted shutdown",
            )
            .await
            .unwrap();
        let (coordinator, tasks) = ExecutionCoordinator::start(
            store.clone(),
            Arc::new(FakeVenue::default()),
            policy(),
            Duration::from_secs(3_600),
            Duration::from_secs(5),
        );

        coordinator.startup().await.unwrap();

        let (state, reason, _) = store.service_state().await.unwrap();
        assert_eq!(state, ServiceState::Halted);
        assert!(reason.contains("shutdown"));
        coordinator.shutdown(false).await.unwrap();
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn startup_cannot_become_ready_without_a_required_heartbeat() {
        let dir = tempdir().unwrap();
        let storage = StorageConfig {
            database_path: dir.path().join("paper.sqlite3").display().to_string(),
            backup_dir: dir.path().join("backups").display().to_string(),
            event_retention: 100,
        };
        let store = Store::open(&storage, Mode::Paper, &PolymarketConfig::default())
            .await
            .unwrap();
        let venue = Arc::new(FakeVenue {
            heartbeat_required: true,
            heartbeat_fails: true,
            ..FakeVenue::default()
        });
        let (coordinator, tasks) = ExecutionCoordinator::start(
            store.clone(),
            venue,
            policy(),
            Duration::from_secs(3_600),
            Duration::from_secs(5),
        );

        coordinator.startup().await.unwrap();

        let (state, reason, _) = store.service_state().await.unwrap();
        assert_eq!(state, ServiceState::Halted);
        assert!(reason.contains("heartbeat"));
        coordinator.shutdown(false).await.unwrap();
        tasks.shutdown().await;
    }
}
