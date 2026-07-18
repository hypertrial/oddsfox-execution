use std::{
    collections::HashSet,
    convert::Infallible,
    fmt,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    BoxError, Extension, Json, Router,
    body::Bytes,
    error_handling::HandleErrorLayer,
    extract::{DefaultBodyLimit, FromRequest, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware,
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, stream};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{DeserializeOwned, Error as _, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_stream::wrappers::BroadcastStream;
use tower::ServiceBuilder;
use tower_http::ServiceBuilderExt;
use tower_http::{request_id::MakeRequestUuid, trace::TraceLayer};
use uuid::Uuid;

use crate::{
    auth::{Actor, AuthRegistry, require_auth},
    domain::{
        CancellationRequest, IntentRecord, IntentState, OrderIntentRequest, OrderRecord,
        OrderState, PositionRecord, ReasonRequest, ServiceState, TradeRecord, TradeState,
    },
    execution::{CoordinatorError, ExecutionCoordinator},
    store::{Admission, BackupRecord, CancellationRecord, ReconciliationRecord, Store, StoreError},
};

#[derive(Clone)]
pub struct ApiState {
    store: Store,
    coordinator: ExecutionCoordinator,
    admission_limiter: Arc<Mutex<TokenBucket>>,
    mutation_gate: Arc<Mutex<()>>,
    emergency_mutation_gate: Arc<Mutex<()>>,
}

impl ApiState {
    #[must_use]
    pub fn new(store: Store, coordinator: ExecutionCoordinator) -> Self {
        Self {
            store,
            coordinator,
            admission_limiter: Arc::new(Mutex::new(TokenBucket::new(25.0, 100.0))),
            mutation_gate: Arc::new(Mutex::new(())),
            emergency_mutation_gate: Arc::new(Mutex::new(())),
        }
    }
}

pub fn router(
    state: ApiState,
    auth: AuthRegistry,
    max_body_bytes: usize,
    request_timeout: Duration,
) -> Router {
    let protected = Router::new()
        .route("/v1/intents", post(submit_intent).get(list_intents))
        .route("/v1/intents/{intent_id}", get(get_intent))
        .route("/v1/orders", get(list_orders))
        .route("/v1/orders/{order_id}", get(get_order))
        .route("/v1/trades", get(list_trades))
        .route("/v1/positions", get(list_positions))
        .route("/v1/cancellations", post(cancel))
        .route("/v1/cancellations/{cancellation_id}", get(get_cancellation))
        .route("/v1/control/state", get(control_state))
        .route("/v1/control/halt", post(halt))
        .route("/v1/control/resume", post(resume))
        .route("/v1/reconciliations", post(reconcile))
        .route(
            "/v1/reconciliations/{reconciliation_id}",
            get(get_reconciliation),
        )
        .route("/v1/events", get(events))
        .route("/v1/backups", post(create_backup))
        .route("/v1/backups/{backup_id}", get(get_backup))
        .route_layer(middleware::from_fn_with_state(auth, require_auth));

    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/v1/openapi.json", get(openapi_handler))
        .merge(protected)
        .with_state(state)
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_timeout_error))
                .set_x_request_id(MakeRequestUuid)
                .layer(TraceLayer::new_for_http())
                .timeout(request_timeout),
        )
}

async fn openapi_handler() -> Json<Value> {
    Json(openapi_document())
}

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn openapi_document() -> Value {
    let mut paths = serde_json::Map::new();
    let operations = [
        ("get", "/health/live", "getLiveness", false, false),
        ("get", "/health/ready", "getReadiness", false, false),
        ("get", "/v1/openapi.json", "getOpenApi", false, false),
        ("post", "/v1/intents", "submitIntent", true, true),
        ("get", "/v1/intents", "listIntents", true, false),
        ("get", "/v1/intents/{intent_id}", "getIntent", true, false),
        ("get", "/v1/orders", "listOrders", true, false),
        ("get", "/v1/orders/{order_id}", "getOrder", true, false),
        ("get", "/v1/trades", "listTrades", true, false),
        ("get", "/v1/positions", "listPositions", true, false),
        (
            "post",
            "/v1/cancellations",
            "createCancellation",
            true,
            true,
        ),
        (
            "get",
            "/v1/cancellations/{cancellation_id}",
            "getCancellation",
            true,
            false,
        ),
        ("get", "/v1/control/state", "getControlState", true, false),
        ("post", "/v1/control/halt", "halt", true, true),
        ("post", "/v1/control/resume", "resume", true, true),
        (
            "post",
            "/v1/reconciliations",
            "createReconciliation",
            true,
            true,
        ),
        (
            "get",
            "/v1/reconciliations/{reconciliation_id}",
            "getReconciliation",
            true,
            false,
        ),
        ("get", "/v1/events", "streamEvents", true, false),
        ("post", "/v1/backups", "createBackup", true, true),
        ("get", "/v1/backups/{backup_id}", "getBackup", true, false),
    ];
    for (method, path, operation_id, authenticated, mutating) in operations {
        let status = if mutating { "202" } else { "200" };
        let mut operation = serde_json::json!({
            "operationId": operation_id,
            "responses": {
                status: {
                    "description": if mutating {
                        "Durably accepted; venue acceptance is not implied"
                    } else {
                        "Successful response"
                    }
                },
                "400": {"$ref": "#/components/responses/Error"},
                "401": {"$ref": "#/components/responses/Error"},
                "403": {"$ref": "#/components/responses/Error"},
                "409": {"$ref": "#/components/responses/Error"},
                "429": {"$ref": "#/components/responses/Error"},
                "503": {"$ref": "#/components/responses/Error"}
            }
        });
        if authenticated {
            operation["security"] = serde_json::json!([{"bearerAuth": []}]);
        }
        if mutating {
            operation["parameters"] = serde_json::json!([{
                "name": "Idempotency-Key",
                "in": "header",
                "required": true,
                "schema": {"type": "string", "minLength": 16, "maxLength": 128}
            }]);
        }
        let path_item = paths
            .entry(path.to_owned())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        path_item
            .as_object_mut()
            .expect("path item is always an object")
            .insert(method.to_owned(), operation);
    }
    paths
        .get_mut("/v1/intents")
        .and_then(Value::as_object_mut)
        .and_then(|item| item.get_mut("post"))
        .expect("intent operation exists")["requestBody"] = serde_json::json!({
        "required": true,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/OrderIntentRequest"}
            }
        }
    });
    paths
        .get_mut("/v1/cancellations")
        .and_then(Value::as_object_mut)
        .and_then(|item| item.get_mut("post"))
        .expect("cancellation operation exists")["requestBody"] = serde_json::json!({
        "required": true,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/CancellationRequest"}
            }
        }
    });
    for path in ["/v1/control/halt", "/v1/control/resume"] {
        paths
            .get_mut(path)
            .and_then(Value::as_object_mut)
            .and_then(|item| item.get_mut("post"))
            .expect("control operation exists")["requestBody"] = serde_json::json!({
            "required": true,
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ReasonRequest"}
                }
            }
        });
    }
    let pagination_parameters = serde_json::json!([
        {
            "name": "limit",
            "in": "query",
            "required": false,
            "schema": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100}
        },
        {
            "name": "cursor",
            "in": "query",
            "required": false,
            "schema": {"type": "string"},
            "description": "Opaque cursor returned by the previous page"
        }
    ]);
    for path in ["/v1/intents", "/v1/orders", "/v1/trades"] {
        paths
            .get_mut(path)
            .and_then(Value::as_object_mut)
            .and_then(|item| item.get_mut("get"))
            .expect("list operation exists")["parameters"] = pagination_parameters.clone();
    }
    serde_json::json!({
        "openapi": "3.1.0",
        "info": {
            "title": "OddsFox Execution API",
            "version": "1.0.0",
            "description": "Private, idempotent prediction-market execution control plane"
        },
        "servers": [{"url": "http://127.0.0.1:8787"}],
        "paths": paths,
        "components": {
            "securitySchemes": {
                "bearerAuth": {"type": "http", "scheme": "bearer"}
            },
            "responses": {
                "Error": {
                    "description": "Stable error envelope",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/ErrorEnvelope"}
                        }
                    }
                }
            },
            "schemas": {
                "DecimalString": {
                    "type": "string",
                    "pattern": "^[0-9]+(?:\\.[0-9]{1,6})?$"
                },
                "Quantity": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["unit", "value"],
                    "properties": {
                        "unit": {"type": "string", "enum": ["shares", "quote"]},
                        "value": {"$ref": "#/components/schemas/DecimalString"}
                    }
                },
                "OrderIntentRequest": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["condition_id", "token_id", "side", "time_in_force", "quantity"],
                    "properties": {
                        "condition_id": {"type": "string", "maxLength": 128},
                        "token_id": {"type": "string", "pattern": "^[0-9]+$", "maxLength": 128},
                        "side": {"type": "string", "enum": ["BUY", "SELL"]},
                        "time_in_force": {"type": "string", "enum": ["GTC", "GTD", "FOK", "FAK"]},
                        "quantity": {"$ref": "#/components/schemas/Quantity"},
                        "limit_price": {"oneOf": [
                            {"$ref": "#/components/schemas/DecimalString"},
                            {"type": "null"}
                        ]},
                        "worst_price": {"oneOf": [
                            {"$ref": "#/components/schemas/DecimalString"},
                            {"type": "null"}
                        ]},
                        "expires_at": {"type": ["string", "null"], "format": "date-time"},
                        "post_only": {"type": "boolean", "default": false},
                        "client_context": {"type": "object", "additionalProperties": false}
                    }
                },
                "CancellationRequest": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["reason"],
                    "properties": {
                        "order_id": {"type": ["string", "null"], "format": "uuid"},
                        "intent_id": {"type": ["string", "null"], "format": "uuid"},
                        "condition_id": {"type": ["string", "null"]},
                        "all_open_orders": {"type": "boolean", "default": false},
                        "reason": {"type": "string", "minLength": 1, "maxLength": 512}
                    }
                },
                "ReasonRequest": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["reason"],
                    "properties": {
                        "reason": {"type": "string", "minLength": 1, "maxLength": 512}
                    }
                },
                "ErrorEnvelope": {
                    "type": "object",
                    "required": ["error"],
                    "properties": {
                        "error": {
                            "type": "object",
                            "required": ["code", "message", "retryable", "correlation_id", "details"],
                            "properties": {
                                "code": {"type": "string"},
                                "message": {"type": "string"},
                                "retryable": {"type": "boolean"},
                                "correlation_id": {"type": "string", "format": "uuid"},
                                "details": {"type": "object"}
                            }
                        }
                    }
                }
            }
        }
    })
}

async fn handle_timeout_error(error: BoxError) -> ApiError {
    ApiError::new(
        StatusCode::REQUEST_TIMEOUT,
        "REQUEST_TIMEOUT",
        error.to_string(),
        true,
    )
}

pub fn metrics_router(handle: metrics_exporter_prometheus::PrometheusHandle) -> Router {
    Router::new().route(
        "/metrics",
        get(move || {
            let handle = handle.clone();
            async move { handle.render() }
        }),
    )
}

async fn liveness() -> Json<Value> {
    Json(json!({"status": "live"}))
}

async fn readiness(State(state): State<ApiState>) -> Response {
    match state.store.service_state().await {
        Ok((ServiceState::Ready, _, _)) => {
            (StatusCode::OK, Json(json!({"status": "ready"}))).into_response()
        }
        Ok(_) | Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "not_ready"})),
        )
            .into_response(),
    }
}

async fn submit_intent(
    State(state): State<ApiState>,
    Extension(actor): Extension<Actor>,
    headers: HeaderMap,
    StrictJson(request): StrictJson<OrderIntentRequest>,
) -> Result<(StatusCode, Json<IntentRecord>), ApiError> {
    let key = idempotency_key(&headers)?;
    if let Some(record) = state.store.replay_intent(&actor.id, key, &request).await? {
        return Ok((StatusCode::ACCEPTED, Json(record)));
    }
    enforce_admission_rate(&state).await?;
    let queue_permit = state.coordinator.reserve_queue()?;
    let admission = state.store.admit_intent(&actor.id, key, &request).await?;
    let record = match admission {
        Admission::Created(record) => {
            queue_permit.send(record.id);
            metrics::counter!("oddsfox_intents_admitted_total").increment(1);
            record
        }
        Admission::Existing(record) => {
            drop(queue_permit);
            record
        }
    };
    Ok((StatusCode::ACCEPTED, Json(record)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntentListQuery {
    #[serde(default = "default_limit")]
    limit: u32,
    state: Option<IntentState>,
    condition_id: Option<String>,
    created_after: Option<DateTime<Utc>>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderListQuery {
    #[serde(default = "default_limit")]
    limit: u32,
    state: Option<OrderState>,
    condition_id: Option<String>,
    token_id: Option<String>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TradeListQuery {
    #[serde(default = "default_limit")]
    limit: u32,
    status: Option<TradeState>,
    condition_id: Option<String>,
    token_id: Option<String>,
    cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct ListPage<T> {
    items: Vec<T>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceCursor {
    created_at: DateTime<Utc>,
    id: Uuid,
}

const fn default_limit() -> u32 {
    100
}

fn decode_cursor(raw: Option<&str>) -> Result<Option<ResourceCursor>, ApiError> {
    raw.map(|raw| {
        let bytes = URL_SAFE_NO_PAD.decode(raw).map_err(|_| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "PAGINATION_CURSOR_INVALID",
                "pagination cursor is invalid",
                false,
            )
        })?;
        serde_json::from_slice(&bytes).map_err(|_| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "PAGINATION_CURSOR_INVALID",
                "pagination cursor is invalid",
                false,
            )
        })
    })
    .transpose()
}

fn page<T>(
    mut items: Vec<T>,
    requested_limit: u32,
    cursor_for: impl FnOnce(&T) -> ResourceCursor,
) -> Result<ListPage<T>, ApiError> {
    let limit = usize::try_from(requested_limit.clamp(1, 500)).map_err(|_| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "PAGINATION_LIMIT_INVALID",
            "pagination limit is invalid",
            false,
        )
    })?;
    let has_more = items.len() > limit;
    items.truncate(limit);
    let next_cursor = if has_more {
        let cursor = cursor_for(items.last().expect("a page with more rows is non-empty"));
        Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor).map_err(ApiError::serialization)?))
    } else {
        None
    };
    Ok(ListPage { items, next_cursor })
}

async fn list_intents(
    State(state): State<ApiState>,
    Query(query): Query<IntentListQuery>,
) -> Result<Json<ListPage<IntentRecord>>, ApiError> {
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let rows = state
        .store
        .list_intents_page(
            query.limit,
            query.state,
            query.condition_id.as_deref(),
            query.created_after,
            cursor.map(|value| (value.created_at, value.id)),
        )
        .await?;
    Ok(Json(page(rows, query.limit, |record| ResourceCursor {
        created_at: record.created_at,
        id: record.id,
    })?))
}

async fn get_intent(
    State(state): State<ApiState>,
    Path(intent_id): Path<Uuid>,
) -> Result<Json<IntentRecord>, ApiError> {
    Ok(Json(state.store.get_intent(intent_id).await?))
}

async fn list_orders(
    State(state): State<ApiState>,
    Query(query): Query<OrderListQuery>,
) -> Result<Json<ListPage<OrderRecord>>, ApiError> {
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let rows = state
        .store
        .list_orders_page(
            query.limit,
            query.state,
            query.condition_id.as_deref(),
            query.token_id.as_deref(),
            cursor.map(|value| (value.created_at, value.id)),
        )
        .await?;
    Ok(Json(page(rows, query.limit, |record| ResourceCursor {
        created_at: record.created_at,
        id: record.id,
    })?))
}

async fn get_order(
    State(state): State<ApiState>,
    Path(order_id): Path<Uuid>,
) -> Result<Json<OrderRecord>, ApiError> {
    Ok(Json(state.store.get_order(order_id).await?))
}

async fn list_trades(
    State(state): State<ApiState>,
    Query(query): Query<TradeListQuery>,
) -> Result<Json<ListPage<TradeRecord>>, ApiError> {
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let rows = state
        .store
        .list_trades_page(
            query.limit,
            query.status,
            query.condition_id.as_deref(),
            query.token_id.as_deref(),
            cursor.map(|value| (value.created_at, value.id)),
        )
        .await?;
    Ok(Json(page(rows, query.limit, |record| ResourceCursor {
        created_at: record.created_at,
        id: record.id,
    })?))
}

async fn list_positions(
    State(state): State<ApiState>,
) -> Result<Json<Vec<PositionRecord>>, ApiError> {
    Ok(Json(state.store.list_positions().await?))
}

async fn cancel(
    State(state): State<ApiState>,
    Extension(actor): Extension<Actor>,
    headers: HeaderMap,
    StrictJson(request): StrictJson<CancellationRequest>,
) -> Result<Response, ApiError> {
    let key = idempotency_key(&headers)?.to_owned();
    let body = serde_json::to_value(&request).map_err(ApiError::serialization)?;
    let _guard = state.emergency_mutation_gate.lock().await;
    if let Some(response) = replay_operation(&state, &actor, &key, "cancellation", &body).await? {
        return Ok(response);
    }
    enforce_admission_rate(&state).await?;
    let operation_permit = state.coordinator.reserve_operation()?;
    let (id, targets) = state
        .coordinator
        .admit_cancellation(&actor.id, &key, &request)
        .await?;
    let response = json!({"id": id, "target_order_ids": targets, "mode": state.store.mode()});
    state
        .coordinator
        .spawn_cancellation(id, targets, operation_permit);
    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

async fn control_state(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    let (service_state, reason, updated_at) = state.store.service_state().await?;
    Ok(Json(json!({
        "state": service_state,
        "reason": reason,
        "updated_at": updated_at,
        "mode": state.store.mode(),
    })))
}

async fn halt(
    State(state): State<ApiState>,
    Extension(actor): Extension<Actor>,
    headers: HeaderMap,
    StrictJson(request): StrictJson<ReasonRequest>,
) -> Result<Response, ApiError> {
    let key = idempotency_key(&headers)?.to_owned();
    validate_reason(&request.reason)?;
    let body = serde_json::to_value(&request).map_err(ApiError::serialization)?;
    let _guard = state.emergency_mutation_gate.lock().await;
    if let Some(response) = replay_operation(&state, &actor, &key, "control.halt", &body).await? {
        return Ok(response);
    }
    enforce_admission_rate(&state).await?;
    state
        .coordinator
        .halt_idempotent(&actor.id, &key, &request.reason)
        .await?;
    let response = json!({"state": "HALTED", "mode": state.store.mode()});
    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

async fn resume(
    State(state): State<ApiState>,
    Extension(actor): Extension<Actor>,
    headers: HeaderMap,
    StrictJson(request): StrictJson<ReasonRequest>,
) -> Result<Response, ApiError> {
    let key = idempotency_key(&headers)?.to_owned();
    validate_reason(&request.reason)?;
    let body = serde_json::to_value(&request).map_err(ApiError::serialization)?;
    let _guard = state.mutation_gate.lock().await;
    if let Some(response) = replay_operation(&state, &actor, &key, "control.resume", &body).await? {
        return Ok(response);
    }
    enforce_admission_rate(&state).await?;
    state
        .coordinator
        .resume_idempotent(&actor.id, &key, &request.reason)
        .await?;
    let response = json!({"state": "READY", "mode": state.store.mode()});
    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

async fn reconcile(
    State(state): State<ApiState>,
    Extension(actor): Extension<Actor>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let key = idempotency_key(&headers)?.to_owned();
    let body = json!({});
    let _guard = state.mutation_gate.lock().await;
    if let Some(response) = replay_operation(&state, &actor, &key, "reconciliation", &body).await? {
        return Ok(response);
    }
    enforce_admission_rate(&state).await?;
    let operation_permit = state.coordinator.reserve_operation()?;
    let id = state
        .coordinator
        .admit_reconciliation(&actor.id, &key, "operator")
        .await?;
    let response = json!({"id": id, "mode": state.store.mode()});
    state.coordinator.spawn_reconciliation(id, operation_permit);
    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

async fn create_backup(
    State(state): State<ApiState>,
    Extension(actor): Extension<Actor>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let key = idempotency_key(&headers)?.to_owned();
    let body = json!({});
    let _guard = state.mutation_gate.lock().await;
    if let Some(response) = replay_operation(&state, &actor, &key, "backup", &body).await? {
        return Ok(response);
    }
    enforce_admission_rate(&state).await?;
    let operation_permit = state.coordinator.reserve_operation()?;
    let backup = state
        .store
        .start_backup_idempotent(&actor.id, &key, &body)
        .await?;
    let response = json!({
        "id": backup.id,
        "state": "RUNNING",
        "mode": state.store.mode()
    });
    let store = state.store.clone();
    tokio::spawn(async move {
        let _operation_permit = operation_permit;
        if let Err(error) = store.run_backup(backup.id).await {
            tracing::error!(backup_id = %backup.id, %error, "admitted backup failed");
        }
    });
    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

async fn get_reconciliation(
    State(state): State<ApiState>,
    Path(reconciliation_id): Path<Uuid>,
) -> Result<Json<ReconciliationRecord>, ApiError> {
    Ok(Json(
        state.store.get_reconciliation(reconciliation_id).await?,
    ))
}

async fn get_backup(
    State(state): State<ApiState>,
    Path(backup_id): Path<Uuid>,
) -> Result<Json<BackupRecord>, ApiError> {
    Ok(Json(state.store.get_backup(backup_id).await?))
}

async fn get_cancellation(
    State(state): State<ApiState>,
    Path(cancellation_id): Path<Uuid>,
) -> Result<Json<CancellationRecord>, ApiError> {
    Ok(Json(state.store.get_cancellation(cancellation_id).await?))
}

async fn events(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    // Subscribe before reading the journal so events committed between the
    // replay query and stream construction cannot be lost.
    let receiver = state.store.subscribe();
    let after = match headers.get("last-event-id") {
        Some(value) => value
            .to_str()
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value >= 0)
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "INVALID_EVENT_CURSOR",
                    "Last-Event-ID must be a non-negative integer",
                    false,
                )
            })?,
        None => 0,
    };
    let (minimum, maximum) = state.store.event_sequence_bounds().await?;
    let cursor_outside_retention = event_cursor_outside_retention(after, minimum, maximum);
    let mut replay = if cursor_outside_retention {
        Vec::new()
    } else {
        state.store.replay_events(after, 10_000).await?
    };
    let replay_watermark = replay.last().map_or(after, |event| event.sequence);
    let replay_truncated =
        event_replay_truncated(cursor_outside_retention, maximum, replay_watermark);
    if replay_truncated {
        replay.clear();
    }
    let reset_reason = if cursor_outside_retention {
        Some("cursor_outside_retention")
    } else if replay_truncated {
        Some("replay_limit_exceeded")
    } else {
        None
    };
    let live_watermark = if reset_reason.is_some() {
        maximum.unwrap_or(after)
    } else {
        replay_watermark
    };
    let reset = reset_reason.map(|reason| {
        Ok::<_, Infallible>(
            Event::default()
                .event("reset")
                .data(json!({"reason": reason, "action": "query_resources"}).to_string()),
        )
    });
    let replay_stream = stream::iter(
        reset
            .into_iter()
            .chain(replay.into_iter().map(|event| Ok(sse_event(event)))),
    );
    let live_stream = BroadcastStream::new(receiver).filter_map(move |result| async move {
        match result {
            Ok(event) if event.sequence > live_watermark => {
                Some(Ok::<_, Infallible>(sse_event(event)))
            }
            Ok(_) => None,
            Err(_) => Some(Ok(Event::default()
                .event("reset")
                .data(r#"{"reason":"client_lagged","action":"query_resources"}"#))),
        }
    });
    Ok(Sse::new(replay_stream.chain(live_stream)).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

fn event_cursor_outside_retention(after: i64, minimum: Option<i64>, maximum: Option<i64>) -> bool {
    after > 0
        && (minimum.is_some_and(|minimum| after < minimum.saturating_sub(1))
            || maximum.is_none_or(|maximum| after > maximum))
}

fn event_replay_truncated(
    cursor_outside_retention: bool,
    maximum: Option<i64>,
    replay_watermark: i64,
) -> bool {
    !cursor_outside_retention && maximum.is_some_and(|maximum| replay_watermark < maximum)
}

fn sse_event(event: crate::domain::ExecutionEvent) -> Event {
    let payload =
        serde_json::from_str(&event.payload_json).unwrap_or(Value::String(event.payload_json));
    Event::default()
        .id(event.sequence.to_string())
        .event(event.event_type)
        .data(
            json!({
                "sequence": event.sequence,
                "mode": event.mode,
                "resource_type": event.resource_type,
                "resource_id": event.resource_id,
                "created_at": event.created_at,
                "payload": payload
            })
            .to_string(),
        )
}

async fn replay_operation(
    state: &ApiState,
    actor: &Actor,
    key: &str,
    operation: &str,
    body: &Value,
) -> Result<Option<Response>, ApiError> {
    let replay = state
        .store
        .idempotent_response(&actor.id, key, operation, body)
        .await?;
    replay
        .map(|replay| {
            let status = StatusCode::from_u16(replay.status).map_err(|_| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_STORAGE_ERROR",
                    "stored idempotent response has an invalid status",
                    false,
                )
            })?;
            Ok((status, Json(replay.body)).into_response())
        })
        .transpose()
}

async fn enforce_admission_rate(state: &ApiState) -> Result<(), ApiError> {
    if state.admission_limiter.lock().await.take() {
        return Ok(());
    }
    Err(ApiError::new(
        StatusCode::TOO_MANY_REQUESTS,
        "RATE_LIMITED",
        "mutation admission rate exceeded",
        true,
    )
    .with_retry_after(1))
}

fn idempotency_key(headers: &HeaderMap) -> Result<&str, ApiError> {
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "IDEMPOTENCY_KEY_REQUIRED",
                "Idempotency-Key header is required",
                false,
            )
        })?;
    if !(16..=128).contains(&key.len())
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "IDEMPOTENCY_KEY_INVALID",
            "Idempotency-Key must contain 16 to 128 printable ASCII characters",
            false,
        ));
    }
    Ok(key)
}

fn validate_reason(reason: &str) -> Result<(), ApiError> {
    if reason.trim().is_empty() || reason.len() > 512 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "REASON_INVALID",
            "reason must contain 1 to 512 characters",
            false,
        ));
    }
    Ok(())
}

struct TokenBucket {
    tokens: f64,
    rate: f64,
    capacity: f64,
    last: Instant,
}

impl TokenBucket {
    fn new(rate: f64, capacity: f64) -> Self {
        Self {
            tokens: capacity,
            rate,
            capacity,
            last: Instant::now(),
        }
    }

    fn take(&mut self) -> bool {
        let now = Instant::now();
        self.tokens = (self.tokens + now.duration_since(self.last).as_secs_f64() * self.rate)
            .min(self.capacity);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

struct StrictJson<T>(T);

impl<S, T> FromRequest<S> for StrictJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(request, state)
            .await
            .map_err(|rejection| {
                let status = rejection.into_response().status();
                ApiError::new(
                    status,
                    if status == StatusCode::PAYLOAD_TOO_LARGE {
                        "REQUEST_BODY_TOO_LARGE"
                    } else {
                        "REQUEST_BODY_INVALID"
                    },
                    "request body could not be read",
                    false,
                )
            })?;
        let unique: UniqueJson = serde_json::from_slice(&bytes).map_err(|error| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "REQUEST_JSON_INVALID",
                error.to_string(),
                false,
            )
        })?;
        serde_json::from_value(unique.0).map(Self).map_err(|error| {
            ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "REQUEST_SCHEMA_INVALID",
                error.to_string(),
                false,
            )
        })
    }
}

#[derive(Debug)]
struct UniqueJson(Value);

impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueJsonVisitor(PhantomData))
    }
}

struct UniqueJsonVisitor(PhantomData<()>);

impl<'de> Visitor<'de> for UniqueJsonVisitor {
    type Value = UniqueJson;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON without duplicate object fields")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(UniqueJson)
            .ok_or_else(|| E::custom("JSON number is not finite"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        UniqueJson::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
        while let Some(value) = sequence.next_element::<UniqueJson>()? {
            values.push(value.0);
        }
        Ok(UniqueJson(Value::Array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = HashSet::with_capacity(map.size_hint().unwrap_or(0));
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(A::Error::custom(format!("duplicate JSON field `{key}`")));
            }
            values.insert(key, map.next_value::<UniqueJson>()?.0);
        }
        Ok(UniqueJson(Value::Object(values)))
    }
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    retryable: bool,
    retry_after_seconds: Option<u64>,
}

impl ApiError {
    fn new(
        status: StatusCode,
        code: &'static str,
        message: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            retryable,
            retry_after_seconds: None,
        }
    }

    const fn with_retry_after(mut self, seconds: u64) -> Self {
        self.retry_after_seconds = Some(seconds);
        self
    }

    #[allow(clippy::needless_pass_by_value)]
    fn serialization(error: serde_json::Error) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_SERIALIZATION_ERROR",
            error.to_string(),
            false,
        )
    }
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound => Self::new(
                StatusCode::NOT_FOUND,
                "RESOURCE_NOT_FOUND",
                error.to_string(),
                false,
            ),
            StoreError::IdempotencyConflict => Self::new(
                StatusCode::CONFLICT,
                "IDEMPOTENCY_KEY_CONFLICT",
                error.to_string(),
                false,
            ),
            StoreError::NewRiskDisabled(_) => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "NEW_RISK_DISABLED",
                error.to_string(),
                true,
            ),
            _ => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_STORAGE_ERROR",
                "storage operation failed",
                true,
            ),
        }
    }
}

impl From<CoordinatorError> for ApiError {
    fn from(error: CoordinatorError) -> Self {
        match error {
            CoordinatorError::QueueFull => Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "EXECUTION_QUEUE_FULL",
                error.to_string(),
                true,
            )
            .with_retry_after(1),
            CoordinatorError::Invalid(_) => Self::new(
                StatusCode::BAD_REQUEST,
                "REQUEST_INVALID",
                error.to_string(),
                false,
            ),
            _ => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "EXECUTION_UNAVAILABLE",
                error.to_string(),
                true,
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let retry_after_seconds = self.retry_after_seconds;
        let mut response = (
            self.status,
            Json(json!({
                "error": {
                    "code": self.code,
                    "message": self.message,
                    "retryable": self.retryable,
                    "correlation_id": Uuid::now_v7(),
                    "details": {},
                }
            })),
        )
            .into_response();
        if let Some(seconds) = retry_after_seconds
            && let Ok(value) = HeaderValue::from_str(&seconds.to_string())
        {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_honors_burst() {
        let mut bucket = TokenBucket::new(25.0, 100.0);
        assert!((0..100).all(|_| bucket.take()));
        assert!(!bucket.take());
    }

    #[test]
    fn duplicate_json_fields_are_rejected_recursively() {
        let duplicate = br#"{"condition_id":"a","nested":{"value":1,"value":2}}"#;
        let error = serde_json::from_slice::<UniqueJson>(duplicate).unwrap_err();
        assert!(error.to_string().contains("duplicate JSON field `value`"));
    }

    #[test]
    fn pagination_cursor_round_trips_and_rejects_malformed_input() {
        let expected = ResourceCursor {
            created_at: Utc::now(),
            id: Uuid::now_v7(),
        };
        let encoded = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&expected).unwrap());
        let decoded = decode_cursor(Some(&encoded)).unwrap().unwrap();
        assert_eq!(decoded.created_at, expected.created_at);
        assert_eq!(decoded.id, expected.id);
        assert!(decode_cursor(Some("not valid base64!")).is_err());
    }

    #[test]
    fn event_replay_resets_for_retention_gaps_and_oversized_backlogs() {
        assert!(event_cursor_outside_retention(5, Some(10), Some(20)));
        assert!(event_cursor_outside_retention(21, Some(10), Some(20)));
        assert!(!event_cursor_outside_retention(9, Some(10), Some(20)));
        assert!(event_replay_truncated(false, Some(20_000), 10_000));
        assert!(!event_replay_truncated(false, Some(10_000), 10_000));
        assert!(!event_replay_truncated(true, Some(20_000), 10_000));
    }
}
