use std::{collections::BTreeSet, fs, path::Path, str::FromStr};

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    domain::{OrderIntentRequest, Side, TimeInForce},
    store::RiskSnapshot,
    venue::{MarketRules, OrderBook},
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RiskPolicy {
    pub version: String,
    #[serde(default)]
    pub allowed_condition_ids: BTreeSet<String>,
    #[serde(default)]
    pub allowed_token_ids: BTreeSet<String>,
    #[serde(default = "default_sides")]
    pub allowed_sides: BTreeSet<Side>,
    #[serde(default = "default_time_in_force")]
    pub allowed_time_in_force: BTreeSet<TimeInForce>,
    pub max_quote_per_order: String,
    pub max_shares_per_order: String,
    pub max_open_orders: u64,
    pub max_open_notional_per_market: String,
    pub max_net_position_per_token: String,
    pub max_gross_exposure: String,
    pub max_daily_matched_notional: String,
    pub max_worst_price_distance: String,
    pub min_visible_depth: String,
    pub max_market_metadata_age_seconds: u64,
    pub max_order_book_age_seconds: u64,
    pub max_user_stream_age_seconds: u64,
    pub max_reconciliation_age_seconds: u64,
    #[serde(default = "default_cancel_on_halt")]
    pub cancel_on_halt: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RiskDecision {
    pub approved: bool,
    pub reason_code: &'static str,
    pub message: String,
    pub observed: serde_json::Value,
    pub policy_version: String,
}

impl RiskPolicy {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes =
            fs::read(path).with_context(|| format!("read risk policy {}", path.display()))?;
        let policy: Self =
            serde_json::from_slice(&bytes).context("parse strict risk policy JSON")?;
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.version.trim().is_empty(),
            "risk policy version is required"
        );
        anyhow::ensure!(self.max_open_orders > 0, "max_open_orders must be positive");
        anyhow::ensure!(
            self.max_market_metadata_age_seconds > 0
                && self.max_order_book_age_seconds > 0
                && self.max_user_stream_age_seconds > 0
                && self.max_reconciliation_age_seconds > 0,
            "risk freshness limits must be positive"
        );
        for (name, raw) in [
            ("max_quote_per_order", &self.max_quote_per_order),
            ("max_shares_per_order", &self.max_shares_per_order),
            (
                "max_open_notional_per_market",
                &self.max_open_notional_per_market,
            ),
            (
                "max_net_position_per_token",
                &self.max_net_position_per_token,
            ),
            ("max_gross_exposure", &self.max_gross_exposure),
            (
                "max_daily_matched_notional",
                &self.max_daily_matched_notional,
            ),
            ("max_worst_price_distance", &self.max_worst_price_distance),
            ("min_visible_depth", &self.min_visible_depth),
        ] {
            let value = Decimal::from_str_exact(raw)
                .with_context(|| format!("{name} must be an exact decimal string"))?;
            anyhow::ensure!(value >= Decimal::ZERO, "{name} may not be negative");
        }
        Ok(())
    }

    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn evaluate(
        &self,
        request: &OrderIntentRequest,
        market: &MarketRules,
        book: &OrderBook,
        snapshot: &RiskSnapshot,
    ) -> RiskDecision {
        let reject = |code, message: String, observed: serde_json::Value| RiskDecision {
            approved: false,
            reason_code: code,
            message,
            observed,
            policy_version: self.version.clone(),
        };

        if !self.allowed_condition_ids.is_empty()
            && !self.allowed_condition_ids.contains(&request.condition_id)
        {
            return reject(
                "RISK_CONDITION_NOT_ALLOWED",
                "condition is not allowlisted".into(),
                serde_json::json!({"condition_id": request.condition_id}),
            );
        }
        if !self.allowed_token_ids.is_empty() && !self.allowed_token_ids.contains(&request.token_id)
        {
            return reject(
                "RISK_TOKEN_NOT_ALLOWED",
                "token is not allowlisted".into(),
                serde_json::json!({"token_id": request.token_id}),
            );
        }
        if !self.allowed_sides.contains(&request.side) {
            return reject(
                "RISK_SIDE_NOT_ALLOWED",
                "side is not allowed".into(),
                serde_json::json!({"side": request.side}),
            );
        }
        if !self.allowed_time_in_force.contains(&request.time_in_force) {
            return reject(
                "RISK_TIF_NOT_ALLOWED",
                "time in force is not allowed".into(),
                serde_json::json!({"time_in_force": request.time_in_force}),
            );
        }
        if !market.active || market.closed || !market.accepting_orders {
            return reject(
                "MARKET_NOT_ACCEPTING_ORDERS",
                "market is not active and accepting orders".into(),
                serde_json::json!({
                    "active": market.active,
                    "closed": market.closed,
                    "accepting_orders": market.accepting_orders,
                }),
            );
        }
        let quantity = match request.quantity_decimal() {
            Ok(value) => value,
            Err(error) => {
                return reject(
                    "ORDER_INVALID_QUANTITY",
                    error.to_string(),
                    serde_json::json!({}),
                );
            }
        };
        let price = match request.protection_price() {
            Ok(value) => value,
            Err(error) => {
                return reject(
                    "ORDER_INVALID_PRICE",
                    error.to_string(),
                    serde_json::json!({}),
                );
            }
        };
        let Some((shares, quote)) = conservative_exposure(request, market, quantity, price) else {
            return reject(
                "MARKET_RULES_INVALID",
                "market tick size must be positive".into(),
                serde_json::json!({"tick_size": market.tick_size}),
            );
        };
        let max_shares = decimal(&self.max_shares_per_order);
        let max_quote = decimal(&self.max_quote_per_order);
        if shares > max_shares {
            return reject(
                "RISK_MAX_SHARES_PER_ORDER",
                "share quantity exceeds policy".into(),
                serde_json::json!({"observed": shares, "limit": max_shares}),
            );
        }
        if quote > max_quote {
            return reject(
                "RISK_MAX_QUOTE_PER_ORDER",
                "quote notional exceeds policy".into(),
                serde_json::json!({"observed": quote, "limit": max_quote}),
            );
        }
        if snapshot.open_order_count >= self.max_open_orders {
            return reject(
                "RISK_MAX_OPEN_ORDERS",
                "open order count exceeds policy".into(),
                serde_json::json!({
                    "observed": snapshot.open_order_count,
                    "limit": self.max_open_orders,
                }),
            );
        }
        let market_after = snapshot.market_open_notional + quote;
        let market_limit = decimal(&self.max_open_notional_per_market);
        if market_after > market_limit {
            return reject(
                "RISK_MAX_MARKET_EXPOSURE",
                "worst-case market exposure would exceed policy".into(),
                serde_json::json!({"observed": market_after, "limit": market_limit}),
            );
        }
        let long_after = snapshot.token_position
            + snapshot.token_pending_buys
            + if request.side == Side::Buy {
                shares
            } else {
                Decimal::ZERO
            };
        let short_after = snapshot.token_position
            - snapshot.token_pending_sells
            - if request.side == Side::Sell {
                shares
            } else {
                Decimal::ZERO
            };
        let worst_position_after = long_after.abs().max(short_after.abs());
        let position_limit = decimal(&self.max_net_position_per_token);
        if worst_position_after > position_limit {
            return reject(
                "RISK_MAX_TOKEN_POSITION",
                "worst-case token position would exceed policy".into(),
                serde_json::json!({
                    "observed": worst_position_after,
                    "long_case": long_after,
                    "short_case": short_after,
                    "limit": position_limit
                }),
            );
        }
        let gross_after = snapshot.gross_exposure
            + if request.side == Side::Buy {
                quote + quote * market.taker_fee_rate
            } else {
                Decimal::ZERO
            };
        let gross_limit = decimal(&self.max_gross_exposure);
        if gross_after > gross_limit {
            return reject(
                "RISK_MAX_GROSS_EXPOSURE",
                "worst-case gross exposure would exceed policy".into(),
                serde_json::json!({"observed": gross_after, "limit": gross_limit}),
            );
        }
        let daily_after = snapshot.daily_matched_notional + quote;
        let daily_limit = decimal(&self.max_daily_matched_notional);
        if daily_after > daily_limit {
            return reject(
                "RISK_MAX_DAILY_MATCHED_NOTIONAL",
                "daily matched notional would exceed policy".into(),
                serde_json::json!({"observed": daily_after, "limit": daily_limit}),
            );
        }
        if matches!(request.time_in_force, TimeInForce::Fok | TimeInForce::Fak) {
            let (reference, visible_depth) = match request.side {
                Side::Buy => (book.best_ask(), book.ask_depth_through(price)),
                Side::Sell => (book.best_bid(), book.bid_depth_through(price)),
            };
            let Some(reference) = reference else {
                return reject(
                    "RISK_EMPTY_BOOK",
                    "book has no executable liquidity".into(),
                    serde_json::json!({}),
                );
            };
            let distance = (price - reference).abs();
            let distance_limit = decimal(&self.max_worst_price_distance);
            if distance > distance_limit {
                return reject(
                    "RISK_WORST_PRICE_DISTANCE",
                    "worst price is too far from the current book".into(),
                    serde_json::json!({"observed": distance, "limit": distance_limit}),
                );
            }
            let depth_limit = decimal(&self.min_visible_depth);
            if visible_depth < depth_limit {
                return reject(
                    "RISK_MIN_VISIBLE_DEPTH",
                    "visible executable depth is below policy".into(),
                    serde_json::json!({"observed": visible_depth, "limit": depth_limit}),
                );
            }
        }

        RiskDecision {
            approved: true,
            reason_code: "RISK_APPROVED",
            message: "intent satisfies policy".into(),
            observed: serde_json::json!({
                "quote": quote,
                "shares": shares,
                "gross_after": gross_after,
                "market_after": market_after,
                "long_position_after": long_after,
                "short_position_after": short_after,
            }),
            policy_version: self.version.clone(),
        }
    }
}

pub(crate) fn conservative_exposure(
    request: &OrderIntentRequest,
    market: &MarketRules,
    quantity: Decimal,
    price: Decimal,
) -> Option<(Decimal, Decimal)> {
    if request.side == Side::Buy
        && matches!(request.time_in_force, TimeInForce::Fok | TimeInForce::Fak)
    {
        (market.tick_size > Decimal::ZERO).then(|| (quantity / market.tick_size, quantity))
    } else {
        Some((quantity, price * quantity))
    }
}

fn decimal(raw: &str) -> Decimal {
    Decimal::from_str(raw).expect("validated risk decimal")
}

fn default_sides() -> BTreeSet<Side> {
    [Side::Buy, Side::Sell].into_iter().collect()
}

fn default_time_in_force() -> BTreeSet<TimeInForce> {
    [
        TimeInForce::Gtc,
        TimeInForce::Gtd,
        TimeInForce::Fok,
        TimeInForce::Fak,
    ]
    .into_iter()
    .collect()
}

const fn default_cancel_on_halt() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::{
        domain::{ClientContext, Quantity, QuantityUnit},
        venue::PriceLevel,
    };

    fn policy() -> RiskPolicy {
        RiskPolicy {
            version: "test".into(),
            allowed_condition_ids: BTreeSet::new(),
            allowed_token_ids: BTreeSet::new(),
            allowed_sides: default_sides(),
            allowed_time_in_force: default_time_in_force(),
            max_quote_per_order: "100".into(),
            max_shares_per_order: "100".into(),
            max_open_orders: 10,
            max_open_notional_per_market: "1000".into(),
            max_net_position_per_token: "1000".into(),
            max_gross_exposure: "1000".into(),
            max_daily_matched_notional: "1000".into(),
            max_worst_price_distance: "0.10".into(),
            min_visible_depth: "1".into(),
            max_market_metadata_age_seconds: 30,
            max_order_book_age_seconds: 2,
            max_user_stream_age_seconds: 10,
            max_reconciliation_age_seconds: 60,
            cancel_on_halt: true,
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
                value: "10".into(),
            },
            limit_price: Some("0.5".into()),
            worst_price: None,
            expires_at: None,
            post_only: true,
            client_context: ClientContext::default(),
        }
    }

    #[test]
    fn accepts_at_boundary_and_rejects_above() {
        let market = MarketRules::test_default("0xabc", "123");
        let book = OrderBook {
            bids: vec![PriceLevel::new("0.49", "100")],
            asks: vec![PriceLevel::new("0.51", "100")],
            observed_at: Utc::now(),
            hash: None,
        };
        let snapshot = RiskSnapshot {
            open_order_count: 0,
            market_open_notional: Decimal::ZERO,
            token_position: Decimal::ZERO,
            token_pending_buys: Decimal::ZERO,
            token_pending_sells: Decimal::ZERO,
            gross_exposure: Decimal::ZERO,
            daily_matched_notional: Decimal::ZERO,
        };
        let mut value = request();
        value.quantity.value = "100".into();
        assert!(
            policy()
                .evaluate(&value, &market, &book, &snapshot)
                .approved
        );
        value.quantity.value = "100.000001".into();
        assert!(
            !policy()
                .evaluate(&value, &market, &book, &snapshot)
                .approved
        );
    }

    #[test]
    fn immediate_quote_buy_reserves_maximum_shares_at_the_tick_floor() {
        let mut market = MarketRules::test_default("0xabc", "123");
        market.tick_size = Decimal::new(1, 2);
        let book = OrderBook {
            bids: vec![PriceLevel::new("0.49", "1000")],
            asks: vec![PriceLevel::new("0.50", "1000")],
            observed_at: Utc::now(),
            hash: None,
        };
        let snapshot = RiskSnapshot {
            open_order_count: 0,
            market_open_notional: Decimal::ZERO,
            token_position: Decimal::ZERO,
            token_pending_buys: Decimal::ZERO,
            token_pending_sells: Decimal::ZERO,
            gross_exposure: Decimal::ZERO,
            daily_matched_notional: Decimal::ZERO,
        };
        let mut value = request();
        value.time_in_force = TimeInForce::Fak;
        value.quantity = Quantity {
            unit: QuantityUnit::Quote,
            value: "2".into(),
        };
        value.limit_price = None;
        value.worst_price = Some("0.50".into());
        value.post_only = false;
        let mut policy = policy();
        policy.max_shares_per_order = "150".into();

        let decision = policy.evaluate(&value, &market, &book, &snapshot);
        assert!(!decision.approved);
        assert_eq!(decision.reason_code, "RISK_MAX_SHARES_PER_ORDER");
    }
}
