use std::{fmt, str::FromStr};

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use thiserror::Error;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type, ToSchema, Default,
)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "TEXT", rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Paper,
    Live,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Paper => "paper",
            Self::Live => "live",
        })
    }
}

impl FromStr for Mode {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "paper" => Ok(Self::Paper),
            "live" => Ok(Self::Live),
            _ => Err(DomainError::InvalidField {
                field: "mode",
                message: "must be paper or live".into(),
            }),
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    sqlx::Type,
    ToSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[sqlx(type_name = "TEXT", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ServiceState {
    Starting,
    Reconciling,
    Ready,
    Degraded,
    Halted,
    ShuttingDown,
}

impl fmt::Display for ServiceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}").map(|()| ())
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    sqlx::Type,
    ToSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[sqlx(type_name = "TEXT", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IntentState {
    Received,
    Validating,
    Rejected,
    Approved,
    Preparing,
    Prepared,
    Submitting,
    Submitted,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[sqlx(type_name = "TEXT", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderState {
    Prepared,
    Submitting,
    Live,
    PartiallyFilled,
    Filled,
    CancelPending,
    Cancelled,
    Expired,
    Rejected,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[sqlx(type_name = "TEXT", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeState {
    Matched,
    Mined,
    Confirmed,
    Retrying,
    Failed,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    sqlx::Type,
    ToSchema,
)]
#[serde(rename_all = "UPPERCASE")]
#[sqlx(type_name = "TEXT", rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    sqlx::Type,
    ToSchema,
)]
#[serde(rename_all = "UPPERCASE")]
#[sqlx(type_name = "TEXT", rename_all = "UPPERCASE")]
pub enum TimeInForce {
    Gtc,
    Gtd,
    Fok,
    Fak,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QuantityUnit {
    Shares,
    Quote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct Quantity {
    pub unit: QuantityUnit,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ClientContext {
    pub strategy: Option<String>,
    pub strategy_order_id: Option<String>,
    pub correlation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct OrderIntentRequest {
    pub condition_id: String,
    pub token_id: String,
    pub side: Side,
    pub time_in_force: TimeInForce,
    pub quantity: Quantity,
    pub limit_price: Option<String>,
    pub worst_price: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub post_only: bool,
    #[serde(default)]
    pub client_context: ClientContext,
}

impl OrderIntentRequest {
    pub fn quantity_decimal(&self) -> Result<Decimal, DomainError> {
        parse_positive_decimal("quantity.value", &self.quantity.value)
    }

    pub fn protection_price(&self) -> Result<Decimal, DomainError> {
        let raw = match self.time_in_force {
            TimeInForce::Gtc | TimeInForce::Gtd => self.limit_price.as_deref(),
            TimeInForce::Fok | TimeInForce::Fak => self.worst_price.as_deref(),
        }
        .ok_or_else(|| DomainError::InvalidField {
            field: "price",
            message: "required for the selected time_in_force".into(),
        })?;
        let price = parse_positive_decimal("price", raw)?;
        if price >= Decimal::ONE {
            return Err(DomainError::InvalidField {
                field: "price",
                message: "must be less than 1".into(),
            });
        }
        Ok(price)
    }

    pub fn validate(&self, now: DateTime<Utc>) -> Result<(), DomainError> {
        if self.condition_id.is_empty() || self.condition_id.len() > 128 {
            return Err(DomainError::InvalidField {
                field: "condition_id",
                message: "must contain 1 to 128 characters".into(),
            });
        }
        if self.token_id.is_empty()
            || self.token_id.len() > 128
            || !self.token_id.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(DomainError::InvalidField {
                field: "token_id",
                message: "must be a decimal token identifier".into(),
            });
        }
        self.quantity_decimal()?;
        self.protection_price()?;

        match self.time_in_force {
            TimeInForce::Gtc => {
                if self.worst_price.is_some() || self.expires_at.is_some() {
                    return Err(DomainError::InvalidCombination(
                        "GTC accepts limit_price and no expiration".into(),
                    ));
                }
            }
            TimeInForce::Gtd => {
                if self.worst_price.is_some() {
                    return Err(DomainError::InvalidCombination(
                        "GTD does not accept worst_price".into(),
                    ));
                }
                let expires_at = self.expires_at.ok_or_else(|| DomainError::InvalidField {
                    field: "expires_at",
                    message: "required for GTD".into(),
                })?;
                if expires_at < now + Duration::minutes(3) {
                    return Err(DomainError::InvalidField {
                        field: "expires_at",
                        message: "must be at least three minutes in the future".into(),
                    });
                }
            }
            TimeInForce::Fok | TimeInForce::Fak => {
                if self.limit_price.is_some() || self.expires_at.is_some() || self.post_only {
                    return Err(DomainError::InvalidCombination(
                        "FOK/FAK accept worst_price and cannot be post_only or expiring".into(),
                    ));
                }
                let expected_unit = match self.side {
                    Side::Buy => QuantityUnit::Quote,
                    Side::Sell => QuantityUnit::Shares,
                };
                if self.quantity.unit != expected_unit {
                    return Err(DomainError::InvalidCombination(
                        "immediate buys use quote; immediate sells use shares".into(),
                    ));
                }
            }
        }

        if matches!(self.time_in_force, TimeInForce::Gtc | TimeInForce::Gtd)
            && self.quantity.unit != QuantityUnit::Shares
        {
            return Err(DomainError::InvalidCombination(
                "resting orders use share quantity".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct IntentRecord {
    pub id: Uuid,
    pub mode: Mode,
    pub actor_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub request_json: String,
    pub state: IntentState,
    pub rejection_code: Option<String>,
    pub rejection_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl IntentRecord {
    pub fn request(&self) -> Result<OrderIntentRequest, serde_json::Error> {
        serde_json::from_str(&self.request_json)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct OrderRecord {
    pub id: Uuid,
    pub intent_id: Uuid,
    pub mode: Mode,
    pub venue_order_id: Option<String>,
    pub condition_id: String,
    pub token_id: String,
    pub side: Side,
    pub time_in_force: TimeInForce,
    pub price: String,
    pub original_quantity: String,
    pub remaining_quantity: String,
    pub state: OrderState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct TradeRecord {
    pub id: Uuid,
    pub order_id: Uuid,
    pub mode: Mode,
    pub venue_trade_id: String,
    pub price: String,
    pub size: String,
    pub status: TradeState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct PositionRecord {
    pub token_id: String,
    pub condition_id: String,
    pub mode: Mode,
    pub shares: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct ExecutionEvent {
    pub sequence: i64,
    pub event_type: String,
    pub resource_type: String,
    pub resource_id: String,
    pub mode: Mode,
    pub payload_json: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ReasonRequest {
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CancellationRequest {
    pub order_id: Option<Uuid>,
    pub intent_id: Option<Uuid>,
    pub condition_id: Option<String>,
    #[serde(default)]
    pub all_open_orders: bool,
    pub reason: String,
}

impl CancellationRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        let selectors = usize::from(self.order_id.is_some())
            + usize::from(self.intent_id.is_some())
            + usize::from(self.condition_id.is_some())
            + usize::from(self.all_open_orders);
        if selectors != 1 {
            return Err(DomainError::InvalidCombination(
                "exactly one cancellation selector is required".into(),
            ));
        }
        if self.reason.trim().is_empty() || self.reason.len() > 512 {
            return Err(DomainError::InvalidField {
                field: "reason",
                message: "must contain 1 to 512 characters".into(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("invalid {field}: {message}")]
    InvalidField {
        field: &'static str,
        message: String,
    },
    #[error("invalid order combination: {0}")]
    InvalidCombination(String),
}

fn parse_positive_decimal(field: &'static str, raw: &str) -> Result<Decimal, DomainError> {
    let value = Decimal::from_str_exact(raw).map_err(|_| DomainError::InvalidField {
        field,
        message: "must be an exact decimal string".into(),
    })?;
    if value <= Decimal::ZERO {
        return Err(DomainError::InvalidField {
            field,
            message: "must be greater than zero".into(),
        });
    }
    if value.scale() > 6 {
        return Err(DomainError::InvalidField {
            field,
            message: "supports at most six decimal places".into(),
        });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(tif: TimeInForce) -> OrderIntentRequest {
        OrderIntentRequest {
            condition_id: "0xabc".into(),
            token_id: "123".into(),
            side: Side::Buy,
            time_in_force: tif,
            quantity: Quantity {
                unit: QuantityUnit::Shares,
                value: "10.0".into(),
            },
            limit_price: Some("0.5".into()),
            worst_price: None,
            expires_at: None,
            post_only: false,
            client_context: ClientContext::default(),
        }
    }

    #[test]
    fn validates_resting_order() {
        request(TimeInForce::Gtc).validate(Utc::now()).unwrap();
    }

    #[test]
    fn rejects_post_only_immediate_order() {
        let mut value = request(TimeInForce::Fok);
        value.quantity.unit = QuantityUnit::Quote;
        value.limit_price = None;
        value.worst_price = Some("0.6".into());
        value.post_only = true;
        assert!(value.validate(Utc::now()).is_err());
    }

    #[test]
    fn rejects_excess_decimal_precision() {
        let mut value = request(TimeInForce::Gtc);
        value.quantity.value = "1.0000001".into();
        assert!(value.validate(Utc::now()).is_err());
    }
}
