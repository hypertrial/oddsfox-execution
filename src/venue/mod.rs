mod paper;
mod polymarket;

use std::fmt;
#[cfg(test)]
use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::domain::{OrderIntentRequest, OrderState, TradeState};

pub use paper::PaperVenue;
pub use polymarket::PolymarketVenue;

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketRules {
    pub condition_id: String,
    pub token_id: String,
    pub active: bool,
    pub closed: bool,
    pub accepting_orders: bool,
    pub tick_size: Decimal,
    pub minimum_order_size: Decimal,
    pub negative_risk: bool,
    pub maker_fee_rate: Decimal,
    pub taker_fee_rate: Decimal,
    pub observed_at: DateTime<Utc>,
}

impl MarketRules {
    #[cfg(test)]
    #[must_use]
    pub fn test_default(condition_id: &str, token_id: &str) -> Self {
        Self {
            condition_id: condition_id.into(),
            token_id: token_id.into(),
            active: true,
            closed: false,
            accepting_orders: true,
            tick_size: Decimal::new(1, 2),
            minimum_order_size: Decimal::ONE,
            negative_risk: false,
            maker_fee_rate: Decimal::ZERO,
            taker_fee_rate: Decimal::ZERO,
            observed_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub size: Decimal,
}

impl PriceLevel {
    #[cfg(test)]
    #[must_use]
    pub fn new(price: &str, size: &str) -> Self {
        Self {
            price: Decimal::from_str(price).unwrap(),
            size: Decimal::from_str(size).unwrap(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub observed_at: DateTime<Utc>,
    pub hash: Option<String>,
}

impl OrderBook {
    #[must_use]
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.iter().map(|level| level.price).max()
    }

    #[must_use]
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.iter().map(|level| level.price).min()
    }

    #[must_use]
    pub fn ask_depth_through(&self, price: Decimal) -> Decimal {
        self.asks
            .iter()
            .filter(|level| level.price <= price)
            .map(|level| level.size)
            .sum()
    }

    #[must_use]
    pub fn bid_depth_through(&self, price: Decimal) -> Decimal {
        self.bids
            .iter()
            .filter(|level| level.price >= price)
            .map(|level| level.size)
            .sum()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedVenueOrder {
    pub deterministic_order_id: String,
    pub normalized_json: String,
    pub signed_payload_json: Option<String>,
    pub signer_address: Option<String>,
    pub funder_address: Option<String>,
    pub protocol_version: u32,
    pub sdk_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueSubmission {
    pub venue_order_id: String,
    pub state: OrderState,
    pub filled_quantity: Decimal,
    pub filled_price: Option<Decimal>,
    pub venue_trade_ids: Vec<String>,
    pub evidence: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueCancellation {
    pub state: OrderState,
    pub evidence: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationResult {
    pub orders_observed: usize,
    pub trades_observed: usize,
    pub observed_orders: Vec<ObservedVenueOrder>,
    pub observed_trades: Vec<ObservedVenueTrade>,
    pub observed_positions: Vec<ObservedVenuePosition>,
    pub critical_findings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedVenueOrder {
    pub venue_order_id: String,
    pub state: OrderState,
    pub remaining_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub filled_price: Option<Decimal>,
    pub venue_trade_ids: Vec<String>,
    pub evidence: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedVenueTrade {
    pub venue_trade_id: String,
    pub venue_order_ids: Vec<String>,
    pub status: TradeState,
    pub evidence: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedVenuePosition {
    pub condition_id: String,
    pub token_id: String,
    pub shares: Decimal,
    pub evidence: serde_json::Value,
}

#[derive(Debug, Error)]
pub enum VenueError {
    #[error("market validation failed: {0}")]
    Market(String),
    #[error("venue is stale: {0}")]
    Stale(String),
    #[error("venue rejected request: {0}")]
    Rejected(String),
    #[error("venue response is ambiguous: {0}")]
    Ambiguous(String),
    #[error("venue temporarily unavailable: {0}")]
    Unavailable(String),
    #[error("matching engine is restarting: {0}")]
    MatchingEngineRestart(String),
    #[error("venue protocol mismatch: {0}")]
    ProtocolMismatch(String),
    #[error("geographic policy prohibits new orders: {0}")]
    GeographicRestricted(String),
    #[error("live support is not compiled into this binary")]
    LiveNotCompiled,
    #[error("live trading is not configured: {0}")]
    LiveNotConfigured(String),
}

impl VenueError {
    #[must_use]
    pub const fn is_ambiguous(&self) -> bool {
        matches!(self, Self::Ambiguous(_))
    }

    #[must_use]
    pub const fn requires_halt(&self) -> bool {
        matches!(
            self,
            Self::Ambiguous(_)
                | Self::MatchingEngineRestart(_)
                | Self::ProtocolMismatch(_)
                | Self::GeographicRestricted(_)
        )
    }
}

#[async_trait]
pub trait ExecutionVenue: Send + Sync + fmt::Debug {
    async fn market_rules(
        &self,
        condition_id: &str,
        token_id: &str,
    ) -> Result<MarketRules, VenueError>;
    async fn order_book(&self, token_id: &str) -> Result<OrderBook, VenueError>;
    async fn prepare(
        &self,
        intent_id: Uuid,
        request: &OrderIntentRequest,
    ) -> Result<PreparedVenueOrder, VenueError>;
    async fn submit(
        &self,
        prepared: &PreparedVenueOrder,
        request: &OrderIntentRequest,
    ) -> Result<VenueSubmission, VenueError>;
    async fn cancel(&self, venue_order_id: &str) -> Result<VenueCancellation, VenueError>;
    async fn find_order(
        &self,
        deterministic_order_id: &str,
    ) -> Result<Option<ObservedVenueOrder>, VenueError>;
    async fn reconcile(&self) -> Result<ReconciliationResult, VenueError>;
    async fn heartbeat(&self) -> Result<(), VenueError>;
    fn requires_heartbeat(&self) -> bool;
    fn reconciliation_required(&self) -> bool {
        false
    }
}
