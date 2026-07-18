use std::{collections::HashSet, str::FromStr, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use polymarket_client_sdk_v2::{
    clob::ws::{ChannelType, Client as WsClient},
    types::U256,
    ws::config::Config as WsConfig,
};
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tracing::warn;
use uuid::Uuid;

use crate::{
    config::PaperConfig,
    domain::{OrderIntentRequest, OrderState, Side, TimeInForce},
    store::{PaperOrderCommit, Store},
};

use super::{
    ExecutionVenue, MarketRules, ObservedVenueOrder, ObservedVenuePosition, ObservedVenueTrade,
    OrderBook, PreparedVenueOrder, PriceLevel, ReconciliationResult, VenueCancellation, VenueError,
    VenueSubmission,
};

pub struct PaperVenue {
    client: Client,
    clob_url: String,
    store: Store,
    market_ws: WsClient,
    trade_subscriptions: Mutex<HashSet<String>>,
    stream_failures: Arc<Mutex<HashSet<String>>>,
}

impl std::fmt::Debug for PaperVenue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PaperVenue")
            .field("clob_url", &self.clob_url)
            .finish_non_exhaustive()
    }
}

impl PaperVenue {
    pub async fn new(
        config: &PaperConfig,
        clob_url: &str,
        websocket_url: &str,
        store: Store,
    ) -> Result<Self, VenueError> {
        store
            .initialize_paper_account(config)
            .await
            .map_err(store_error)?;
        let market_ws = WsClient::new(websocket_url, WsConfig::default())
            .map_err(|error| VenueError::Unavailable(error.to_string()))?;
        let venue = Self {
            client: Client::new(),
            clob_url: clob_url.trim_end_matches('/').into(),
            store,
            market_ws,
            trade_subscriptions: Mutex::new(HashSet::new()),
            stream_failures: Arc::new(Mutex::new(HashSet::new())),
        };
        for token_id in venue
            .store
            .paper_active_tokens()
            .await
            .map_err(store_error)?
        {
            venue.ensure_trade_subscription(&token_id).await?;
        }
        Ok(venue)
    }

    async fn market_json(&self, condition_id: &str) -> Result<Value, VenueError> {
        self.client
            .get(format!("{}/markets/{condition_id}", self.clob_url))
            .send()
            .await
            .map_err(|error| VenueError::Unavailable(error.to_string()))?
            .error_for_status()
            .map_err(|error| VenueError::Market(error.to_string()))?
            .json()
            .await
            .map_err(|error| VenueError::Market(error.to_string()))
    }

    fn parse_decimal(value: Option<&Value>, field: &str) -> Result<Decimal, VenueError> {
        let raw = value
            .and_then(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .or_else(|| Some(value.to_string()))
            })
            .ok_or_else(|| VenueError::Market(format!("missing {field}")))?;
        Decimal::from_str(raw.trim_matches('"'))
            .map_err(|error| VenueError::Market(format!("invalid {field}: {error}")))
    }

    async fn ensure_trade_subscription(&self, token_id: &str) -> Result<(), VenueError> {
        let mut subscriptions = self.trade_subscriptions.lock().await;
        if subscriptions.contains(token_id) {
            let healthy = !self.stream_failures.lock().await.contains(token_id)
                && self
                    .market_ws
                    .connection_state(ChannelType::Market)
                    .is_connected();
            if healthy {
                return Ok(());
            }
            subscriptions.remove(token_id);
        }
        let asset_id = U256::from_str(token_id)
            .map_err(|error| VenueError::Market(format!("invalid token ID: {error}")))?;
        let stream = self
            .market_ws
            .subscribe_last_trade_price(vec![asset_id])
            .map_err(|error| VenueError::Unavailable(error.to_string()))?;
        subscriptions.insert(token_id.to_owned());
        drop(subscriptions);

        let token_id = token_id.to_owned();
        let store = self.store.clone();
        let failures = Arc::clone(&self.stream_failures);
        tokio::spawn(async move {
            let mut stream = Box::pin(stream);
            while let Some(message) = stream.next().await {
                match message {
                    Ok(trade) => {
                        failures.lock().await.remove(&token_id);
                        let Some(size) = trade.size.filter(|size| *size > Decimal::ZERO) else {
                            continue;
                        };
                        let observed_at = DateTime::from_timestamp_millis(trade.timestamp)
                            .unwrap_or_else(Utc::now);
                        let event_payload = serde_json::json!({
                            "asset_id": trade.asset_id.to_string(),
                            "market": trade.market.to_string(),
                            "price": trade.price,
                            "side": trade.side.map(|side| format!("{side:?}")),
                            "size": size,
                            "timestamp": trade.timestamp
                        });
                        let event_id = match serde_jcs::to_vec(&event_payload) {
                            Ok(canonical) => hex::encode(Sha256::digest(canonical)),
                            Err(error) => {
                                failures.lock().await.insert(token_id.clone());
                                warn!(%error, %token_id, "paper trade event canonicalization failed");
                                continue;
                            }
                        };
                        if let Err(error) = store
                            .apply_paper_trade_through(
                                &event_id,
                                &token_id,
                                trade.price,
                                size,
                                observed_at,
                            )
                            .await
                        {
                            failures.lock().await.insert(token_id.clone());
                            warn!(%error, %token_id, "paper trade-through application failed");
                        }
                    }
                    Err(error) => {
                        failures.lock().await.insert(token_id.clone());
                        warn!(%error, %token_id, "paper market stream is unhealthy");
                    }
                }
            }
            failures.lock().await.insert(token_id.clone());
            warn!(%token_id, "paper market stream ended");
        });
        Ok(())
    }

    pub async fn apply_market_trade_fixture(
        &self,
        event_id: &str,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        observed_at: DateTime<Utc>,
    ) -> Result<usize, VenueError> {
        self.store
            .apply_paper_trade_through(event_id, token_id, price, size, observed_at)
            .await
            .map_err(store_error)
    }
}

#[derive(Debug, Default)]
struct PaperFill {
    shares: Decimal,
    quote: Decimal,
    fee: Decimal,
    complete: bool,
}

impl PaperFill {
    fn average_price(&self) -> Option<Decimal> {
        (self.shares > Decimal::ZERO).then(|| self.quote / self.shares)
    }
}

fn walk_book(
    book: &OrderBook,
    side: Side,
    requested: Decimal,
    worst_price: Decimal,
    fee_rate: Decimal,
) -> PaperFill {
    let mut levels = match side {
        Side::Buy => book.asks.clone(),
        Side::Sell => book.bids.clone(),
    };
    match side {
        Side::Buy => levels.sort_by_key(|level| level.price),
        Side::Sell => levels.sort_by(|left, right| right.price.cmp(&left.price)),
    }
    let mut fill = PaperFill::default();
    match side {
        Side::Buy => {
            let mut quote_remaining = requested;
            for level in levels
                .into_iter()
                .filter(|level| level.price <= worst_price)
            {
                let quote_per_share = level.price * (Decimal::ONE + fee_rate);
                let shares = level.size.min(quote_remaining / quote_per_share);
                let quote = shares * level.price;
                let fee = quote * fee_rate;
                fill.shares += shares;
                fill.quote += quote;
                fill.fee += fee;
                quote_remaining -= quote + fee;
                if quote_remaining <= Decimal::new(1, 6) {
                    fill.complete = true;
                    break;
                }
            }
        }
        Side::Sell => {
            let mut shares_remaining = requested;
            for level in levels
                .into_iter()
                .filter(|level| level.price >= worst_price)
            {
                let shares = level.size.min(shares_remaining);
                let quote = shares * level.price;
                fill.shares += shares;
                fill.quote += quote;
                fill.fee += quote * fee_rate;
                shares_remaining -= shares;
                if shares_remaining <= Decimal::new(1, 6) {
                    fill.complete = true;
                    break;
                }
            }
        }
    }
    fill
}

#[allow(clippy::needless_pass_by_value)]
fn store_error(error: crate::store::StoreError) -> VenueError {
    VenueError::Rejected(format!("paper ledger: {error}"))
}

#[allow(clippy::too_many_lines)]
#[async_trait]
impl ExecutionVenue for PaperVenue {
    async fn market_rules(
        &self,
        condition_id: &str,
        token_id: &str,
    ) -> Result<MarketRules, VenueError> {
        let value = self.market_json(condition_id).await?;
        let token_matches = value
            .get("tokens")
            .and_then(Value::as_array)
            .is_some_and(|tokens| {
                tokens.iter().any(|token| {
                    token
                        .get("token_id")
                        .or_else(|| token.get("tokenId"))
                        .is_some_and(|value| {
                            value.as_str().is_some_and(|raw| raw == token_id)
                                || value
                                    .as_u64()
                                    .is_some_and(|raw| raw.to_string() == token_id)
                        })
                })
            });
        if !token_matches {
            return Err(VenueError::Market(
                "token does not belong to condition".into(),
            ));
        }
        Ok(MarketRules {
            condition_id: condition_id.into(),
            token_id: token_id.into(),
            active: value
                .get("active")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            closed: value.get("closed").and_then(Value::as_bool).unwrap_or(true),
            accepting_orders: value
                .get("accepting_orders")
                .or_else(|| value.get("acceptingOrders"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            tick_size: Self::parse_decimal(
                value
                    .get("minimum_tick_size")
                    .or_else(|| value.get("minimumTickSize")),
                "minimum_tick_size",
            )?,
            minimum_order_size: Self::parse_decimal(
                value
                    .get("minimum_order_size")
                    .or_else(|| value.get("minimumOrderSize")),
                "minimum_order_size",
            )?,
            negative_risk: value
                .get("neg_risk")
                .or_else(|| value.get("negRisk"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            maker_fee_rate: Self::parse_decimal(
                value
                    .get("maker_base_fee")
                    .or_else(|| value.get("makerBaseFee")),
                "maker_base_fee",
            )
            .unwrap_or(Decimal::ZERO),
            taker_fee_rate: Self::parse_decimal(
                value
                    .get("taker_base_fee")
                    .or_else(|| value.get("takerBaseFee")),
                "taker_base_fee",
            )
            .unwrap_or(Decimal::ZERO),
            observed_at: Utc::now(),
        })
    }

    async fn order_book(&self, token_id: &str) -> Result<OrderBook, VenueError> {
        let value: Value = self
            .client
            .get(format!("{}/book", self.clob_url))
            .query(&[("token_id", token_id)])
            .send()
            .await
            .map_err(|error| VenueError::Unavailable(error.to_string()))?
            .error_for_status()
            .map_err(|error| VenueError::Unavailable(error.to_string()))?
            .json()
            .await
            .map_err(|error| VenueError::Unavailable(error.to_string()))?;
        let parse_levels = |name: &str| -> Result<Vec<PriceLevel>, VenueError> {
            value
                .get(name)
                .and_then(Value::as_array)
                .ok_or_else(|| VenueError::Market(format!("missing {name}")))?
                .iter()
                .map(|level| {
                    Ok(PriceLevel {
                        price: Self::parse_decimal(level.get("price"), "price")?,
                        size: Self::parse_decimal(level.get("size"), "size")?,
                    })
                })
                .collect()
        };
        Ok(OrderBook {
            bids: parse_levels("bids")?,
            asks: parse_levels("asks")?,
            observed_at: Utc::now(),
            hash: value.get("hash").and_then(Value::as_str).map(str::to_owned),
        })
    }

    async fn prepare(
        &self,
        intent_id: Uuid,
        request: &OrderIntentRequest,
    ) -> Result<PreparedVenueOrder, VenueError> {
        let normalized_json = serde_jcs::to_string(request)
            .map_err(|error| VenueError::Rejected(error.to_string()))?;
        let mut hasher = Sha256::new();
        hasher.update(b"oddsfox-paper-order-v1");
        hasher.update(intent_id.as_bytes());
        hasher.update(normalized_json.as_bytes());
        Ok(PreparedVenueOrder {
            deterministic_order_id: format!("paper_{}", hex::encode(hasher.finalize())),
            normalized_json,
            signed_payload_json: None,
            signer_address: None,
            funder_address: None,
            protocol_version: 2,
            sdk_version: "paper-v1".into(),
        })
    }

    async fn submit(
        &self,
        prepared: &PreparedVenueOrder,
        request: &OrderIntentRequest,
    ) -> Result<VenueSubmission, VenueError> {
        let book = self.order_book(&request.token_id).await?;
        let market = self
            .market_rules(&request.condition_id, &request.token_id)
            .await?;
        let price = request
            .protection_price()
            .map_err(|error| VenueError::Rejected(error.to_string()))?;
        let quantity = request
            .quantity_decimal()
            .map_err(|error| VenueError::Rejected(error.to_string()))?;
        let immediate = matches!(request.time_in_force, TimeInForce::Fok | TimeInForce::Fak);
        let fee_rate = market.maker_fee_rate.max(market.taker_fee_rate);
        let fill = if immediate {
            walk_book(&book, request.side, quantity, price, fee_rate)
        } else {
            self.ensure_trade_subscription(&request.token_id).await?;
            PaperFill::default()
        };
        let fok_unfilled = request.time_in_force == TimeInForce::Fok && !fill.complete;
        let mut reserved_quote = Decimal::ZERO;
        let mut reserved_shares = Decimal::ZERO;
        if !immediate {
            match request.side {
                Side::Buy => {
                    reserved_quote = quantity * price * (Decimal::ONE + fee_rate);
                }
                Side::Sell => {
                    reserved_shares = quantity;
                }
            }
        }
        let state = if fok_unfilled {
            OrderState::Cancelled
        } else if immediate {
            if fill.complete {
                OrderState::Filled
            } else {
                OrderState::Cancelled
            }
        } else {
            OrderState::Live
        };
        let applied_fill = if fok_unfilled {
            PaperFill::default()
        } else {
            fill
        };
        let original_quantity = if request.side == Side::Buy && immediate {
            quantity / market.tick_size
        } else {
            quantity
        };
        let remaining_quantity = if immediate {
            Decimal::ZERO
        } else {
            original_quantity
        };
        let venue_trade_ids: Vec<String> = (applied_fill.shares > Decimal::ZERO)
            .then(|| format!("paper_trade_{}", prepared.deterministic_order_id))
            .into_iter()
            .collect();
        let evidence = if fok_unfilled {
            serde_json::json!({"paper": true, "reason": "insufficient_depth"})
        } else {
            serde_json::json!({
                "paper": true,
                "filled_quantity": applied_fill.shares,
                "quote": applied_fill.quote,
                "fee": applied_fill.fee,
                "average_price": applied_fill.average_price(),
            })
        };
        let committed = self
            .store
            .commit_paper_order(&PaperOrderCommit {
                venue_order_id: prepared.deterministic_order_id.clone(),
                condition_id: request.condition_id.clone(),
                token_id: request.token_id.clone(),
                side: request.side,
                state,
                price,
                fee_rate,
                original_quantity,
                remaining_quantity,
                filled_quantity: applied_fill.shares,
                filled_price: applied_fill.average_price(),
                quote_amount: applied_fill.quote,
                fee_amount: applied_fill.fee,
                reserved_quote,
                reserved_shares,
                venue_trade_ids: venue_trade_ids.clone(),
                evidence: evidence.clone(),
            })
            .await
            .map_err(store_error)?;
        Ok(VenueSubmission {
            venue_order_id: committed.venue_order_id,
            state: committed.state,
            filled_quantity: committed.filled_quantity,
            filled_price: committed.filled_price,
            venue_trade_ids: committed.venue_trade_ids,
            evidence: committed.evidence,
        })
    }

    async fn cancel(&self, venue_order_id: &str) -> Result<VenueCancellation, VenueError> {
        let order = self
            .store
            .cancel_paper_order(venue_order_id)
            .await
            .map_err(store_error)?;
        Ok(VenueCancellation {
            state: order.state,
            evidence: serde_json::json!({"paper": true, "state": order.state}),
        })
    }

    async fn find_order(
        &self,
        deterministic_order_id: &str,
    ) -> Result<Option<ObservedVenueOrder>, VenueError> {
        Ok(self
            .store
            .find_paper_order(deterministic_order_id)
            .await
            .map_err(store_error)?
            .map(|order| ObservedVenueOrder {
                venue_order_id: order.venue_order_id,
                state: order.state,
                remaining_quantity: order.remaining_quantity,
                filled_quantity: order.filled_quantity,
                filled_price: order.filled_price,
                venue_trade_ids: order.venue_trade_ids,
                evidence: serde_json::json!({
                    "paper": true,
                    "recovered": true,
                    "original_evidence": order.evidence
                }),
            }))
    }

    async fn reconcile(&self) -> Result<ReconciliationResult, VenueError> {
        let failed_streams = self.stream_failures.lock().await;
        if !failed_streams.is_empty() {
            return Err(VenueError::Stale(format!(
                "paper trade streams unhealthy for tokens: {}",
                failed_streams.iter().cloned().collect::<Vec<_>>().join(",")
            )));
        }
        drop(failed_streams);
        let (orders_observed, _) = self.store.paper_venue_counts().await.map_err(store_error)?;
        let paper_orders = self.store.list_paper_orders().await.map_err(store_error)?;
        let observed_trades: Vec<_> = paper_orders
            .iter()
            .flat_map(|order| {
                order
                    .venue_trade_ids
                    .iter()
                    .map(|venue_trade_id| ObservedVenueTrade {
                        venue_trade_id: venue_trade_id.clone(),
                        venue_order_ids: vec![order.venue_order_id.clone()],
                        status: crate::domain::TradeState::Matched,
                        evidence: serde_json::json!({
                            "paper": true,
                            "reconciliation": true
                        }),
                    })
            })
            .collect();
        let trades_observed = observed_trades.len();
        let observed_orders = paper_orders
            .into_iter()
            .map(|order| ObservedVenueOrder {
                venue_order_id: order.venue_order_id,
                state: order.state,
                remaining_quantity: order.remaining_quantity,
                filled_quantity: order.filled_quantity,
                filled_price: order.filled_price,
                venue_trade_ids: order.venue_trade_ids,
                evidence: serde_json::json!({
                    "paper": true,
                    "reconciliation": true,
                    "original_evidence": order.evidence
                }),
            })
            .collect();
        let observed_positions = self
            .store
            .paper_venue_positions()
            .await
            .map_err(store_error)?
            .into_iter()
            .map(|(condition_id, token_id, shares)| ObservedVenuePosition {
                condition_id,
                token_id,
                shares,
                evidence: serde_json::json!({
                    "paper": true,
                    "source": "durable_paper_inventory"
                }),
            })
            .collect();
        Ok(ReconciliationResult {
            orders_observed,
            trades_observed,
            observed_orders,
            observed_trades,
            observed_positions,
            critical_findings: Vec::new(),
        })
    }

    async fn heartbeat(&self) -> Result<(), VenueError> {
        for token_id in self
            .store
            .paper_active_tokens()
            .await
            .map_err(store_error)?
        {
            self.ensure_trade_subscription(&token_id).await?;
        }
        let failed_streams = self.stream_failures.lock().await;
        if !failed_streams.is_empty() {
            return Err(VenueError::Stale(format!(
                "paper trade streams unhealthy for tokens: {}",
                failed_streams.iter().cloned().collect::<Vec<_>>().join(",")
            )));
        }
        Ok(())
    }

    fn requires_heartbeat(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use super::*;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[test]
    fn immediate_buy_walks_levels_and_charges_fee() {
        let book = OrderBook {
            bids: Vec::new(),
            asks: vec![
                PriceLevel {
                    price: decimal("0.4"),
                    size: decimal("10"),
                },
                PriceLevel {
                    price: decimal("0.5"),
                    size: decimal("10"),
                },
            ],
            observed_at: Utc::now(),
            hash: None,
        };
        let fill = walk_book(
            &book,
            Side::Buy,
            decimal("6.3"),
            decimal("0.5"),
            decimal("0.05"),
        );
        assert!(fill.complete);
        assert_eq!(fill.shares, decimal("14"));
        assert_eq!(fill.quote, decimal("6"));
        assert_eq!(fill.fee, decimal("0.3"));
    }

    #[test]
    fn price_protection_limits_sell_depth() {
        let book = OrderBook {
            bids: vec![
                PriceLevel {
                    price: decimal("0.6"),
                    size: decimal("2"),
                },
                PriceLevel {
                    price: decimal("0.5"),
                    size: decimal("10"),
                },
            ],
            asks: Vec::new(),
            observed_at: Utc::now(),
            hash: None,
        };
        let fill = walk_book(
            &book,
            Side::Sell,
            decimal("3"),
            decimal("0.55"),
            Decimal::ZERO,
        );
        assert!(!fill.complete);
        assert_eq!(fill.shares, decimal("2"));
        assert_eq!(fill.quote, decimal("1.2"));
    }
}
