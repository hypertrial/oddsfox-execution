#[cfg(feature = "live")]
mod implementation {
    #[cfg(unix)]
    use std::fs;
    use std::{
        borrow::Cow,
        collections::{HashMap, HashSet},
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };

    use alloy::{
        dyn_abi::Eip712Domain,
        signers::{Signer as _, local::PrivateKeySigner},
        sol_types::SolStruct as _,
    };
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use futures::StreamExt as _;
    use polymarket_client_sdk_v2::{
        auth::{Normal, state::Authenticated},
        clob::{
            Client, Config as SdkConfig,
            types::{
                Amount, AssetType, OrderPayload, OrderSignature, OrderStatusType,
                OrderType as SdkOrderType, OrderV2, Side as SdkSide, SignatureType, SignedOrder,
                TradeStatusType, TraderSide,
                request::{
                    BalanceAllowanceRequest, OrderBookSummaryRequest, OrdersRequest, TradesRequest,
                },
            },
            ws::{ChannelType, Client as WsClient},
        },
        contract_config,
        data::{Client as DataClient, types::request::PositionsRequest},
        error::{Kind as SdkErrorKind, Status as SdkStatus},
        types::{Address, B256, U256},
        ws::config::Config as WsConfig,
    };
    use rust_decimal::Decimal;
    #[cfg(unix)]
    use secrecy::{ExposeSecret as _, ExposeSecretMut as _, SecretBox};
    use serde_json::Value;
    use tokio::sync::{Mutex, RwLock};
    use uuid::Uuid;

    use crate::{
        config::PolymarketConfig,
        domain::{OrderIntentRequest, OrderState, Side, TimeInForce, TradeState},
    };

    use super::super::{
        ExecutionVenue, MarketRules, ObservedVenueOrder, ObservedVenuePosition, ObservedVenueTrade,
        OrderBook, PreparedVenueOrder, PriceLevel, ReconciliationResult, VenueCancellation,
        VenueError, VenueSubmission,
    };

    type AuthClient = Client<Authenticated<Normal>>;
    type AuthWsClient = WsClient<Authenticated<Normal>>;

    fn book_is_consistent(book: &OrderBook) -> bool {
        let valid_levels = |levels: &[PriceLevel]| {
            let mut prices = std::collections::BTreeSet::new();
            levels.iter().all(|level| {
                level.price > Decimal::ZERO
                    && level.price <= Decimal::ONE
                    && level.size > Decimal::ZERO
                    && prices.insert(level.price)
            })
        };
        if !valid_levels(&book.bids) || !valid_levels(&book.asks) {
            return false;
        }
        let best_bid = book.bids.iter().map(|level| level.price).max();
        let best_ask = book.asks.iter().map(|level| level.price).min();
        if best_bid.zip(best_ask).is_some_and(|(bid, ask)| bid >= ask) {
            return false;
        }
        book.hash
            .as_ref()
            .is_none_or(|hash| !hash.trim().is_empty())
    }

    pub struct PolymarketVenue {
        config: PolymarketConfig,
        client: AuthClient,
        data_client: DataClient,
        signer: PrivateKeySigner,
        ws: AuthWsClient,
        market_subscriptions: Mutex<HashSet<String>>,
        books: Arc<RwLock<HashMap<String, OrderBook>>>,
        last_user_event: Arc<RwLock<Option<DateTime<Utc>>>>,
        user_event_generation: Arc<AtomicU64>,
        reconciled_user_event_generation: AtomicU64,
        stream_gap_generation: Arc<AtomicU64>,
        reconciled_stream_generation: AtomicU64,
        heartbeat_id: Mutex<Option<Uuid>>,
    }

    impl std::fmt::Debug for PolymarketVenue {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PolymarketVenue")
                .field("clob_url", &self.config.clob_url)
                .field("signer_address", &self.config.signer_address)
                .field("funder_address", &self.config.funder_address)
                .finish_non_exhaustive()
        }
    }

    impl PolymarketVenue {
        #[allow(clippy::too_many_lines)]
        pub async fn new(config: PolymarketConfig) -> Result<Self, VenueError> {
            let signer = load_live_signer(&config)?;
            let funder = Address::from_str(config.funder_address.as_deref().ok_or_else(|| {
                VenueError::LiveNotConfigured("funder_address is required".into())
            })?)
            .map_err(|error| VenueError::LiveNotConfigured(error.to_string()))?;
            let geoblock_host = config
                .geoblock_url
                .strip_suffix("/api/geoblock")
                .unwrap_or(&config.geoblock_url)
                .to_owned();
            let sdk_config = SdkConfig::builder().geoblock_host(geoblock_host).build();
            let unauthenticated = Client::new(&config.clob_url, sdk_config)
                .map_err(|error| VenueError::LiveNotConfigured(error.to_string()))?;
            let version = unauthenticated
                .version()
                .await
                .map_err(classify_pre_submit_error)?;
            if version != config.expected_protocol {
                return Err(VenueError::ProtocolMismatch(format!(
                    "expected {}, venue reports {version}",
                    config.expected_protocol
                )));
            }
            let geoblock = unauthenticated
                .check_geoblock()
                .await
                .map_err(classify_pre_submit_error)?;
            if geoblock.blocked {
                return Err(VenueError::GeographicRestricted(format!(
                    "blocked from {}/{}",
                    geoblock.country, geoblock.region
                )));
            }
            let credentials = unauthenticated
                .create_or_derive_api_key(&signer, None)
                .await
                .map_err(classify_pre_submit_error)?;
            let ws = WsClient::new(&config.websocket_url, WsConfig::default())
                .map_err(classify_pre_submit_error)?
                .authenticate(credentials.clone(), signer.address())
                .map_err(classify_pre_submit_error)?;
            let client = unauthenticated
                .authentication_builder(&signer)
                .credentials(credentials)
                .funder(funder)
                .signature_type(SignatureType::Poly1271)
                .authenticate()
                .await
                .map_err(classify_pre_submit_error)?;
            let ban = client
                .closed_only_mode()
                .await
                .map_err(classify_pre_submit_error)?;
            if ban.closed_only {
                return Err(VenueError::GeographicRestricted(
                    "account is in close-only mode".into(),
                ));
            }
            let last_user_event = Arc::new(RwLock::new(None));
            let user_event_generation = Arc::new(AtomicU64::new(0));
            let stream_gap_generation = Arc::new(AtomicU64::new(0));
            let user_stream = ws
                .subscribe_user_events(Vec::new())
                .map_err(classify_pre_submit_error)?;
            let user_timestamp = Arc::clone(&last_user_event);
            let user_events = Arc::clone(&user_event_generation);
            let user_gaps = Arc::clone(&stream_gap_generation);
            tokio::spawn(async move {
                let mut user_stream = Box::pin(user_stream);
                while let Some(message) = user_stream.next().await {
                    match message {
                        Ok(_) => {
                            *user_timestamp.write().await = Some(Utc::now());
                            user_events.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(error) => {
                            user_gaps.fetch_add(1, Ordering::SeqCst);
                            tracing::error!(%error, "Polymarket user stream error");
                        }
                    }
                }
                user_gaps.fetch_add(1, Ordering::SeqCst);
                tracing::error!("Polymarket user stream ended");
            });
            wait_for_ws(&ws, ChannelType::User).await?;
            let data_client =
                DataClient::new(&config.data_api_url).map_err(classify_pre_submit_error)?;
            Ok(Self {
                config,
                client,
                data_client,
                signer,
                ws,
                market_subscriptions: Mutex::new(HashSet::new()),
                books: Arc::new(RwLock::new(HashMap::new())),
                last_user_event,
                user_event_generation,
                reconciled_user_event_generation: AtomicU64::new(0),
                stream_gap_generation,
                reconciled_stream_generation: AtomicU64::new(0),
                heartbeat_id: Mutex::new(None),
            })
        }

        #[allow(clippy::too_many_lines)]
        async fn ensure_market_subscription(&self, token_id: &str) -> Result<(), VenueError> {
            let mut subscriptions = self.market_subscriptions.lock().await;
            if subscriptions.contains(token_id) {
                let cache_is_healthy = self
                    .books
                    .read()
                    .await
                    .get(token_id)
                    .is_some_and(book_is_consistent)
                    && self.ws.connection_state(ChannelType::Market).is_connected();
                if cache_is_healthy {
                    return Ok(());
                }
                subscriptions.remove(token_id);
            }
            let token =
                U256::from_str(token_id).map_err(|error| VenueError::Market(error.to_string()))?;
            let book_stream = self
                .ws
                .subscribe_orderbook(vec![token])
                .map_err(classify_pre_submit_error)?;
            let price_stream = self
                .ws
                .subscribe_prices(vec![token])
                .map_err(classify_pre_submit_error)?;
            let books = Arc::clone(&self.books);
            let book_gaps = Arc::clone(&self.stream_gap_generation);
            let subscribed_token = token_id.to_owned();
            tokio::spawn(async move {
                let mut book_stream = Box::pin(book_stream);
                while let Some(message) = book_stream.next().await {
                    match message {
                        Ok(book) => {
                            let observed_at = DateTime::from_timestamp_millis(book.timestamp)
                                .unwrap_or_else(Utc::now);
                            let next = OrderBook {
                                bids: book
                                    .bids
                                    .into_iter()
                                    .map(|level| PriceLevel {
                                        price: level.price,
                                        size: level.size,
                                    })
                                    .collect(),
                                asks: book
                                    .asks
                                    .into_iter()
                                    .map(|level| PriceLevel {
                                        price: level.price,
                                        size: level.size,
                                    })
                                    .collect(),
                                observed_at,
                                hash: book.hash,
                            };
                            let asset_id = book.asset_id.to_string();
                            let mut cache = books.write().await;
                            let regressed = cache
                                .get(&asset_id)
                                .is_some_and(|prior| next.observed_at < prior.observed_at);
                            if regressed || !book_is_consistent(&next) {
                                cache.remove(&asset_id);
                                book_gaps.fetch_add(1, Ordering::SeqCst);
                                tracing::error!(
                                    token_id = %asset_id,
                                    "Polymarket book snapshot failed consistency checks"
                                );
                            } else {
                                cache.insert(asset_id, next);
                            }
                        }
                        Err(error) => {
                            books.write().await.remove(&subscribed_token);
                            book_gaps.fetch_add(1, Ordering::SeqCst);
                            tracing::error!(%error, "Polymarket market stream error");
                        }
                    }
                }
                books.write().await.remove(&subscribed_token);
                book_gaps.fetch_add(1, Ordering::SeqCst);
                tracing::error!(token_id = %subscribed_token, "Polymarket market stream ended");
            });
            let books = Arc::clone(&self.books);
            let price_gaps = Arc::clone(&self.stream_gap_generation);
            let subscribed_token = token_id.to_owned();
            tokio::spawn(async move {
                let mut price_stream = Box::pin(price_stream);
                while let Some(message) = price_stream.next().await {
                    match message {
                        Ok(change) => {
                            let observed_at = DateTime::from_timestamp_millis(change.timestamp)
                                .unwrap_or_else(Utc::now);
                            let mut books = books.write().await;
                            for entry in change.price_changes {
                                let asset_id = entry.asset_id.to_string();
                                let Some(book) = books.get_mut(&asset_id) else {
                                    continue;
                                };
                                if observed_at < book.observed_at {
                                    books.remove(&asset_id);
                                    price_gaps.fetch_add(1, Ordering::SeqCst);
                                    tracing::error!(
                                        token_id = %asset_id,
                                        "Polymarket price event timestamp regressed"
                                    );
                                    continue;
                                }
                                let levels = if entry.side == SdkSide::Buy {
                                    &mut book.bids
                                } else {
                                    &mut book.asks
                                };
                                levels.retain(|level| level.price != entry.price);
                                if let Some(size) = entry.size
                                    && size > Decimal::ZERO
                                {
                                    levels.push(PriceLevel {
                                        price: entry.price,
                                        size,
                                    });
                                }
                                book.observed_at = observed_at;
                                if let Some(hash) = entry.hash {
                                    book.hash = Some(hash);
                                }
                                if !book_is_consistent(book) {
                                    books.remove(&asset_id);
                                    price_gaps.fetch_add(1, Ordering::SeqCst);
                                    tracing::error!(
                                        token_id = %asset_id,
                                        "Polymarket price event produced an inconsistent book"
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            books.write().await.remove(&subscribed_token);
                            price_gaps.fetch_add(1, Ordering::SeqCst);
                            tracing::error!(%error, "Polymarket price stream error");
                        }
                    }
                }
                books.write().await.remove(&subscribed_token);
                price_gaps.fetch_add(1, Ordering::SeqCst);
                tracing::error!(token_id = %subscribed_token, "Polymarket price stream ended");
            });
            subscriptions.insert(token_id.to_owned());
            drop(subscriptions);
            wait_for_ws(&self.ws, ChannelType::Market).await
        }

        async fn ensure_trading_allowed(&self) -> Result<(), VenueError> {
            let version = self
                .client
                .version()
                .await
                .map_err(classify_pre_submit_error)?;
            if version != self.config.expected_protocol {
                return Err(VenueError::ProtocolMismatch(format!(
                    "expected {}, venue reports {version}",
                    self.config.expected_protocol
                )));
            }
            let geoblock = self
                .client
                .check_geoblock()
                .await
                .map_err(classify_pre_submit_error)?;
            if geoblock.blocked {
                return Err(VenueError::GeographicRestricted(format!(
                    "blocked from {}/{}",
                    geoblock.country, geoblock.region
                )));
            }
            let ban = self
                .client
                .closed_only_mode()
                .await
                .map_err(classify_pre_submit_error)?;
            if ban.closed_only {
                return Err(VenueError::GeographicRestricted(
                    "account is in close-only mode".into(),
                ));
            }
            Ok(())
        }

        async fn sign_order(
            &self,
            request: &OrderIntentRequest,
        ) -> Result<SignedOrder, VenueError> {
            let token_id = U256::from_str(&request.token_id)
                .map_err(|error| VenueError::Rejected(error.to_string()))?;
            let price = request
                .protection_price()
                .map_err(|error| VenueError::Rejected(error.to_string()))?;
            let quantity = request
                .quantity_decimal()
                .map_err(|error| VenueError::Rejected(error.to_string()))?;
            let side = match request.side {
                Side::Buy => SdkSide::Buy,
                Side::Sell => SdkSide::Sell,
            };
            let order_type = sdk_order_type(request.time_in_force);
            let signable = match request.time_in_force {
                TimeInForce::Gtc => {
                    self.client
                        .limit_order()
                        .token_id(token_id)
                        .size(quantity)
                        .price(price)
                        .side(side)
                        .order_type(order_type)
                        .post_only(request.post_only)
                        .defer_exec(false)
                        .build()
                        .await
                }
                TimeInForce::Gtd => {
                    self.client
                        .limit_order()
                        .token_id(token_id)
                        .size(quantity)
                        .price(price)
                        .side(side)
                        .order_type(order_type)
                        .post_only(request.post_only)
                        .defer_exec(false)
                        .expiration(request.expires_at.ok_or_else(|| {
                            VenueError::Rejected("GTD expiration is required".into())
                        })?)
                        .build()
                        .await
                }
                TimeInForce::Fok | TimeInForce::Fak => {
                    let amount = match request.side {
                        Side::Buy => Amount::usdc(quantity),
                        Side::Sell => Amount::shares(quantity),
                    }
                    .map_err(|error| VenueError::Rejected(error.to_string()))?;
                    self.client
                        .market_order()
                        .token_id(token_id)
                        .amount(amount)
                        .price(price)
                        .side(side)
                        .order_type(order_type)
                        .post_only(false)
                        .defer_exec(false)
                        .build()
                        .await
                }
            }
            .map_err(classify_pre_submit_error)?;
            self.client
                .sign(&self.signer, signable)
                .await
                .map_err(classify_pre_submit_error)
        }

        async fn ensure_balance_and_allowance(
            &self,
            request: &OrderIntentRequest,
            exchange: Address,
        ) -> Result<(), VenueError> {
            let token_id = U256::from_str(&request.token_id)
                .map_err(|error| VenueError::Rejected(error.to_string()))?;
            let balance_request = match request.side {
                Side::Buy => BalanceAllowanceRequest::builder()
                    .asset_type(AssetType::Collateral)
                    .build(),
                Side::Sell => BalanceAllowanceRequest::builder()
                    .asset_type(AssetType::Conditional)
                    .token_id(token_id)
                    .build(),
            };
            let balance = self
                .client
                .balance_allowance(balance_request)
                .await
                .map_err(classify_pre_submit_error)?;
            let quantity = request
                .quantity_decimal()
                .map_err(|error| VenueError::Rejected(error.to_string()))?;
            let price = request
                .protection_price()
                .map_err(|error| VenueError::Rejected(error.to_string()))?;
            let required = match request.side {
                Side::Buy
                    if matches!(request.time_in_force, TimeInForce::Fok | TimeInForce::Fak) =>
                {
                    quantity
                }
                Side::Buy => quantity * price,
                Side::Sell => quantity,
            };
            if balance.balance < required {
                return Err(VenueError::Rejected(format!(
                    "venue balance {} is below required {required}",
                    balance.balance
                )));
            }
            let allowance = balance
                .allowances
                .get(&exchange)
                .ok_or_else(|| {
                    VenueError::Rejected(format!(
                        "no venue allowance is recorded for exchange {exchange}"
                    ))
                })
                .and_then(|raw| {
                    Decimal::from_str_exact(raw).map_err(|error| {
                        VenueError::Rejected(format!("invalid venue allowance: {error}"))
                    })
                })?;
            if allowance < required {
                return Err(VenueError::Rejected(format!(
                    "venue allowance {allowance} is below required {required}"
                )));
            }
            Ok(())
        }
    }

    #[allow(clippy::too_many_lines)]
    #[async_trait]
    impl ExecutionVenue for PolymarketVenue {
        async fn market_rules(
            &self,
            condition_id: &str,
            token_id: &str,
        ) -> Result<MarketRules, VenueError> {
            self.ensure_trading_allowed().await?;
            self.ensure_market_subscription(token_id).await?;
            let market = self
                .client
                .market(condition_id)
                .await
                .map_err(classify_pre_submit_error)?;
            let token_matches = market
                .tokens
                .iter()
                .any(|token| token.token_id.to_string() == token_id);
            if !token_matches {
                return Err(VenueError::Market(
                    "token does not belong to condition".into(),
                ));
            }
            Ok(MarketRules {
                condition_id: condition_id.into(),
                token_id: token_id.into(),
                active: market.active,
                closed: market.closed,
                accepting_orders: market.accepting_orders,
                tick_size: market.minimum_tick_size,
                minimum_order_size: market.minimum_order_size,
                negative_risk: market.neg_risk,
                maker_fee_rate: market.maker_base_fee,
                taker_fee_rate: market.taker_base_fee,
                observed_at: Utc::now(),
            })
        }

        async fn order_book(&self, token_id: &str) -> Result<OrderBook, VenueError> {
            if let Some(book) = self.books.read().await.get(token_id).cloned()
                && Utc::now()
                    .signed_duration_since(book.observed_at)
                    .num_seconds()
                    <= 2
            {
                return Ok(book);
            }
            let token_id =
                U256::from_str(token_id).map_err(|error| VenueError::Market(error.to_string()))?;
            let request = OrderBookSummaryRequest::builder()
                .token_id(token_id)
                .build();
            let book = self
                .client
                .order_book(&request)
                .await
                .map_err(classify_pre_submit_error)?;
            let book = OrderBook {
                bids: book
                    .bids
                    .into_iter()
                    .map(|level| PriceLevel {
                        price: level.price,
                        size: level.size,
                    })
                    .collect(),
                asks: book
                    .asks
                    .into_iter()
                    .map(|level| PriceLevel {
                        price: level.price,
                        size: level.size,
                    })
                    .collect(),
                observed_at: book.timestamp,
                hash: book.hash,
            };
            if !book_is_consistent(&book) {
                return Err(VenueError::Stale(
                    "REST order book failed consistency checks".into(),
                ));
            }
            Ok(book)
        }

        async fn prepare(
            &self,
            _intent_id: Uuid,
            request: &OrderIntentRequest,
        ) -> Result<PreparedVenueOrder, VenueError> {
            self.ensure_trading_allowed().await?;
            let signed = self.sign_order(request).await?;
            let neg_risk = self
                .client
                .neg_risk(signed.order().tokenId)
                .await
                .map_err(classify_pre_submit_error)?
                .neg_risk;
            let exchange = contract_config(self.config.chain_id, neg_risk)
                .and_then(|contracts| contracts.exchange_v2)
                .ok_or_else(|| {
                    VenueError::ProtocolMismatch(format!(
                        "V2 exchange is not configured for chain {} and neg_risk={neg_risk}",
                        self.config.chain_id
                    ))
                })?;
            self.ensure_balance_and_allowance(request, exchange).await?;
            let domain = Eip712Domain {
                name: Some(Cow::Borrowed("Polymarket CTF Exchange")),
                version: Some(Cow::Borrowed("2")),
                chain_id: Some(U256::from(self.config.chain_id)),
                verifying_contract: Some(exchange),
                ..Eip712Domain::default()
            };
            let deterministic_order_id = signed.order().eip712_signing_hash(&domain).to_string();
            let signed_payload_json = serde_jcs::to_string(&signed)
                .map_err(|error| VenueError::Rejected(error.to_string()))?;
            Ok(PreparedVenueOrder {
                deterministic_order_id,
                normalized_json: serde_jcs::to_string(signed.order())
                    .map_err(|error| VenueError::Rejected(error.to_string()))?,
                signed_payload_json: Some(signed_payload_json),
                signer_address: self.config.signer_address.clone(),
                funder_address: self.config.funder_address.clone(),
                protocol_version: self.config.expected_protocol,
                sdk_version: "0.7.0".into(),
            })
        }

        async fn submit(
            &self,
            prepared: &PreparedVenueOrder,
            request: &OrderIntentRequest,
        ) -> Result<VenueSubmission, VenueError> {
            let payload = prepared.signed_payload_json.as_deref().ok_or_else(|| {
                VenueError::Rejected("signed payload is required in live mode".into())
            })?;
            let signed = reconstruct_signed_order(payload)?;
            match self.client.post_order(signed).await {
                Ok(response) => {
                    if !response.success {
                        return Err(VenueError::Rejected(
                            response
                                .error_msg
                                .clone()
                                .unwrap_or_else(|| "venue rejected the order".into()),
                        ));
                    }
                    let state = map_order_status(&response.status);
                    if state == OrderState::Unknown {
                        return Err(VenueError::Ambiguous(format!(
                            "venue returned unrecognized order status {} for {}",
                            response.status, response.order_id
                        )));
                    }
                    let (filled_quantity, filled_price) = match request.side {
                        Side::Buy => (
                            response.taking_amount,
                            (response.taking_amount > Decimal::ZERO)
                                .then(|| response.making_amount / response.taking_amount),
                        ),
                        Side::Sell => (
                            response.making_amount,
                            (response.making_amount > Decimal::ZERO)
                                .then(|| response.taking_amount / response.making_amount),
                        ),
                    };
                    Ok(VenueSubmission {
                        venue_order_id: response.order_id.clone(),
                        state,
                        filled_quantity,
                        filled_price,
                        venue_trade_ids: response.trade_ids.clone(),
                        evidence: serde_json::json!({
                            "success": response.success,
                            "order_id": response.order_id,
                            "status": response.status.to_string(),
                            "error": response.error_msg,
                            "trade_ids": response.trade_ids,
                            "transaction_hashes": response.transaction_hashes,
                        }),
                    })
                }
                Err(error) => Err(classify_submission_error(error)),
            }
        }

        async fn cancel(&self, venue_order_id: &str) -> Result<VenueCancellation, VenueError> {
            let response = self
                .client
                .cancel_order(venue_order_id)
                .await
                .map_err(classify_pre_submit_error)?;
            if !response.canceled.iter().any(|id| id == venue_order_id) {
                let reason = response
                    .not_canceled
                    .get(venue_order_id)
                    .cloned()
                    .unwrap_or_else(|| "venue did not positively acknowledge cancellation".into());
                return Err(VenueError::Ambiguous(reason));
            }
            Ok(VenueCancellation {
                state: OrderState::Cancelled,
                evidence: serde_json::json!({
                    "canceled": response.canceled,
                    "not_canceled": response.not_canceled,
                }),
            })
        }

        async fn find_order(
            &self,
            deterministic_order_id: &str,
        ) -> Result<Option<ObservedVenueOrder>, VenueError> {
            match self.client.order(deterministic_order_id).await {
                Ok(order) => Ok(Some(ObservedVenueOrder {
                    venue_order_id: order.id.clone(),
                    state: map_order_status(&order.status),
                    remaining_quantity: (order.original_size - order.size_matched)
                        .max(Decimal::ZERO),
                    filled_quantity: order.size_matched,
                    filled_price: Some(order.price),
                    venue_trade_ids: order.associate_trades.clone(),
                    evidence: serde_json::json!({
                        "source": "rest_order_lookup",
                        "venue_order_id": order.id,
                        "asset_id": order.asset_id.to_string(),
                        "status": order.status.to_string(),
                        "original_size": order.original_size,
                        "size_matched": order.size_matched,
                    }),
                })),
                Err(error) if is_not_found(&error) => Ok(None),
                Err(error) => Err(classify_pre_submit_error(error)),
            }
        }

        async fn reconcile(&self) -> Result<ReconciliationResult, VenueError> {
            let user_generation_at_start = self.user_event_generation.load(Ordering::SeqCst);
            let gap_generation_at_start = self.stream_gap_generation.load(Ordering::SeqCst);
            self.ensure_trading_allowed().await?;
            if !self.ws.connection_state(ChannelType::User).is_connected() {
                return Err(VenueError::Stale(
                    "authenticated user stream is disconnected".into(),
                ));
            }
            let mut orders_observed = 0_usize;
            let mut observed_orders = Vec::new();
            let mut order_cursor = None;
            for _ in 0..100 {
                let page = self
                    .client
                    .orders(&OrdersRequest::default(), order_cursor)
                    .await
                    .map_err(classify_pre_submit_error)?;
                orders_observed += page.data.len();
                observed_orders.extend(page.data.iter().map(|order| ObservedVenueOrder {
                    venue_order_id: order.id.clone(),
                    state: map_order_status(&order.status),
                    remaining_quantity:
                        (order.original_size - order.size_matched).max(Decimal::ZERO),
                    filled_quantity: order.size_matched,
                    filled_price: Some(order.price),
                    venue_trade_ids: order.associate_trades.clone(),
                    evidence: serde_json::json!({
                        "source": "open_orders_reconciliation",
                        "venue_order_id": order.id,
                        "asset_id": order.asset_id.to_string(),
                        "status": order.status.to_string(),
                        "original_size": order.original_size,
                        "size_matched": order.size_matched
                    }),
                }));
                if page.next_cursor.is_empty() || page.next_cursor == "LTE=" {
                    break;
                }
                order_cursor = Some(page.next_cursor);
            }
            let mut trades_observed = 0_usize;
            let mut observed_trades = Vec::new();
            let mut critical_findings = Vec::new();
            let mut trade_cursor = None;
            for _ in 0..100 {
                let page = self
                    .client
                    .trades(&TradesRequest::default(), trade_cursor)
                    .await
                    .map_err(classify_pre_submit_error)?;
                trades_observed += page.data.len();
                for trade in &page.data {
                    let Some(status) = map_trade_status(&trade.status) else {
                        critical_findings.push(format!(
                            "venue trade {} has an unrecognized status {}",
                            trade.id, trade.status
                        ));
                        continue;
                    };
                    let venue_order_ids = match &trade.trader_side {
                        TraderSide::Taker => vec![trade.taker_order_id.clone()],
                        TraderSide::Maker => trade
                            .maker_orders
                            .iter()
                            .map(|order| order.order_id.clone())
                            .collect(),
                        _ => std::iter::once(trade.taker_order_id.clone())
                            .chain(
                                trade
                                    .maker_orders
                                    .iter()
                                    .map(|order| order.order_id.clone()),
                            )
                            .collect(),
                    };
                    observed_trades.push(ObservedVenueTrade {
                        venue_trade_id: trade.id.clone(),
                        venue_order_ids,
                        status,
                        evidence: serde_json::json!({
                            "source": "trade_reconciliation",
                            "market": trade.market.to_string(),
                            "asset_id": trade.asset_id.to_string(),
                            "side": trade.side.to_string(),
                            "size": trade.size,
                            "price": trade.price,
                            "status": trade.status.to_string(),
                            "match_time": trade.match_time,
                            "last_update": trade.last_update,
                            "transaction_hash": trade.transaction_hash.to_string(),
                            "error": trade.error_msg
                        }),
                    });
                }
                if page.next_cursor.is_empty() || page.next_cursor == "LTE=" {
                    break;
                }
                trade_cursor = Some(page.next_cursor);
            }
            let collateral = self
                .client
                .balance_allowance(
                    BalanceAllowanceRequest::builder()
                        .asset_type(AssetType::Collateral)
                        .build(),
                )
                .await
                .map_err(classify_pre_submit_error)?;
            metrics::gauge!("oddsfox_venue_collateral_balance")
                .set(collateral.balance.to_string().parse::<f64>().unwrap_or(0.0));
            if collateral.allowances.is_empty() {
                critical_findings.push(
                    "venue returned no collateral allowances for the configured account".into(),
                );
            }
            let funder =
                Address::from_str(self.config.funder_address.as_deref().ok_or_else(|| {
                    VenueError::LiveNotConfigured("funder_address is required".into())
                })?)
                .map_err(|error| VenueError::LiveNotConfigured(error.to_string()))?;
            let mut observed_positions = Vec::new();
            for page_index in 0_i32..=20 {
                let request = PositionsRequest::builder()
                    .user(funder)
                    .size_threshold(Decimal::ZERO)
                    .limit(500)
                    .map_err(|error| VenueError::Unavailable(error.to_string()))?
                    .offset(page_index * 500)
                    .map_err(|error| VenueError::Unavailable(error.to_string()))?
                    .build();
                let page = self
                    .data_client
                    .positions(&request)
                    .await
                    .map_err(classify_pre_submit_error)?;
                let page_len = page.len();
                observed_positions.extend(page.into_iter().map(|position| ObservedVenuePosition {
                    condition_id: position.condition_id.to_string(),
                    token_id: position.asset.to_string(),
                    shares: position.size,
                    evidence: serde_json::json!({
                        "source": "data_api_positions",
                        "proxy_wallet": position.proxy_wallet.to_string(),
                        "average_price": position.avg_price,
                        "current_value": position.current_value,
                        "redeemable": position.redeemable,
                        "mergeable": position.mergeable
                    }),
                }));
                if page_len < 500 {
                    break;
                }
                if page_index == 20 {
                    critical_findings.push(
                        "position reconciliation exceeded the Data API pagination bound".into(),
                    );
                }
            }
            let token_ids: HashSet<_> = observed_orders
                .iter()
                .filter_map(|order| {
                    order
                        .evidence
                        .get("asset_id")
                        .and_then(Value::as_str)
                        .and_then(|value| U256::from_str(value).ok())
                })
                .chain(
                    observed_positions
                        .iter()
                        .filter_map(|position| U256::from_str(&position.token_id).ok()),
                )
                .collect();
            for token_id in token_ids {
                let conditional = self
                    .client
                    .balance_allowance(
                        BalanceAllowanceRequest::builder()
                            .asset_type(AssetType::Conditional)
                            .token_id(token_id)
                            .build(),
                    )
                    .await
                    .map_err(classify_pre_submit_error)?;
                if conditional.allowances.is_empty() {
                    critical_findings.push(format!(
                        "venue returned no conditional-token allowances for {token_id}"
                    ));
                }
            }
            if !self.market_subscriptions.lock().await.is_empty()
                && !self.ws.connection_state(ChannelType::Market).is_connected()
            {
                return Err(VenueError::Stale(
                    "market stream is disconnected during reconciliation".into(),
                ));
            }
            self.reconciled_stream_generation
                .store(gap_generation_at_start, Ordering::SeqCst);
            self.reconciled_user_event_generation
                .store(user_generation_at_start, Ordering::SeqCst);
            Ok(ReconciliationResult {
                orders_observed,
                trades_observed,
                observed_orders,
                observed_trades,
                observed_positions,
                critical_findings,
            })
        }

        async fn heartbeat(&self) -> Result<(), VenueError> {
            if !self.ws.connection_state(ChannelType::User).is_connected() {
                return Err(VenueError::Stale(
                    "authenticated user stream is disconnected".into(),
                ));
            }
            if !self.market_subscriptions.lock().await.is_empty()
                && !self.ws.connection_state(ChannelType::Market).is_connected()
            {
                return Err(VenueError::Stale("market stream is disconnected".into()));
            }
            if self.stream_gap_generation.load(Ordering::SeqCst)
                != self.reconciled_stream_generation.load(Ordering::SeqCst)
            {
                return Err(VenueError::Stale(
                    "a WebSocket gap requires reconciliation".into(),
                ));
            }
            if let Some(last_event) = *self.last_user_event.read().await {
                let age_seconds = Utc::now()
                    .signed_duration_since(last_event)
                    .to_std()
                    .map_or(0.0, |duration| duration.as_secs_f64());
                metrics::gauge!("oddsfox_user_stream_age_seconds").set(age_seconds);
            }
            let mut heartbeat_id = self.heartbeat_id.lock().await;
            let response = self
                .client
                .post_heartbeat(*heartbeat_id)
                .await
                .map_err(classify_pre_submit_error)?;
            if let Some(error) = response.error {
                return Err(VenueError::Unavailable(error));
            }
            *heartbeat_id = Some(response.heartbeat_id);
            Ok(())
        }

        fn requires_heartbeat(&self) -> bool {
            true
        }

        fn reconciliation_required(&self) -> bool {
            self.user_event_generation.load(Ordering::SeqCst)
                != self.reconciled_user_event_generation.load(Ordering::SeqCst)
        }
    }

    fn sdk_order_type(value: TimeInForce) -> SdkOrderType {
        match value {
            TimeInForce::Gtc => SdkOrderType::GTC,
            TimeInForce::Gtd => SdkOrderType::GTD,
            TimeInForce::Fok => SdkOrderType::FOK,
            TimeInForce::Fak => SdkOrderType::FAK,
        }
    }

    pub fn validate_live_signer(config: &PolymarketConfig) -> Result<(), VenueError> {
        load_live_signer(config).map(drop)
    }

    #[cfg(unix)]
    fn load_live_signer(config: &PolymarketConfig) -> Result<PrivateKeySigner, VenueError> {
        use std::{
            io::Read as _,
            os::unix::fs::{MetadataExt as _, PermissionsExt as _},
        };

        if config.chain_id != 137 {
            return Err(VenueError::LiveNotConfigured(
                "local signing supports only Polygon chain 137".into(),
            ));
        }
        let path = config
            .private_key_file
            .as_deref()
            .ok_or_else(|| VenueError::LiveNotConfigured("private_key_file is required".into()))?;
        let link_metadata = fs::symlink_metadata(path).map_err(|_| {
            VenueError::LiveNotConfigured("live key file could not be inspected".into())
        })?;
        if link_metadata.file_type().is_symlink() || !link_metadata.is_file() {
            return Err(VenueError::LiveNotConfigured(
                "live key path must be a regular, non-symlink file".into(),
            ));
        }
        let mut key_file = fs::File::open(path)
            .map_err(|_| VenueError::LiveNotConfigured("live key file could not be read".into()))?;
        let opened_metadata = key_file.metadata().map_err(|_| {
            VenueError::LiveNotConfigured("live key file could not be inspected".into())
        })?;
        let current_path_metadata = fs::symlink_metadata(path).map_err(|_| {
            VenueError::LiveNotConfigured("live key file changed while it was being opened".into())
        })?;
        if current_path_metadata.file_type().is_symlink()
            || !current_path_metadata.is_file()
            || current_path_metadata.dev() != link_metadata.dev()
            || current_path_metadata.ino() != link_metadata.ino()
            || !opened_metadata.is_file()
            || opened_metadata.dev() != link_metadata.dev()
            || opened_metadata.ino() != link_metadata.ino()
        {
            return Err(VenueError::LiveNotConfigured(
                "live key file changed while it was being opened".into(),
            ));
        }
        if !private_key_mode_is_allowed(opened_metadata.permissions().mode()) {
            return Err(VenueError::LiveNotConfigured(
                "live key file mode must be 0400 or 0600".into(),
            ));
        }
        let key_length = usize::try_from(opened_metadata.len())
            .map_err(|_| VenueError::LiveNotConfigured("live key file is too large".into()))?;
        let buffer_length = key_length
            .checked_add(1)
            .ok_or_else(|| VenueError::LiveNotConfigured("live key file is too large".into()))?;
        let mut private_key = SecretBox::<Vec<u8>>::default();
        private_key.expose_secret_mut().resize(buffer_length, 0);
        key_file
            .read_exact(&mut private_key.expose_secret_mut()[..key_length])
            .map_err(|_| VenueError::LiveNotConfigured("live key file could not be read".into()))?;
        if key_file
            .read(&mut private_key.expose_secret_mut()[key_length..])
            .map_err(|_| VenueError::LiveNotConfigured("live key file could not be read".into()))?
            != 0
        {
            return Err(VenueError::LiveNotConfigured(
                "live key file changed while it was being read".into(),
            ));
        }
        let private_key =
            std::str::from_utf8(&private_key.expose_secret()[..key_length]).map_err(|_| {
                VenueError::LiveNotConfigured(
                    "live key file does not contain a valid private key".into(),
                )
            })?;
        let signer = PrivateKeySigner::from_str(private_key.trim())
            .map_err(|_| {
                VenueError::LiveNotConfigured(
                    "live key file does not contain a valid private key".into(),
                )
            })?
            .with_chain_id(Some(137));
        let configured_signer =
            Address::from_str(config.signer_address.as_deref().ok_or_else(|| {
                VenueError::LiveNotConfigured("signer_address is required".into())
            })?)
            .map_err(|_| {
                VenueError::LiveNotConfigured("configured signer_address is invalid".into())
            })?;
        if signer.address() != configured_signer {
            return Err(VenueError::LiveNotConfigured(
                "configured signer_address does not match mounted key".into(),
            ));
        }
        Ok(signer)
    }

    #[cfg(unix)]
    fn private_key_mode_is_allowed(mode: u32) -> bool {
        matches!(mode & 0o7777, 0o400 | 0o600)
    }

    #[cfg(not(unix))]
    fn load_live_signer(_config: &PolymarketConfig) -> Result<PrivateKeySigner, VenueError> {
        Err(VenueError::LiveNotConfigured(
            "local live key files are supported only on Unix platforms".into(),
        ))
    }

    fn map_order_status(status: &OrderStatusType) -> OrderState {
        match status {
            OrderStatusType::Live | OrderStatusType::Unmatched | OrderStatusType::Delayed => {
                OrderState::Live
            }
            OrderStatusType::Matched => OrderState::Filled,
            OrderStatusType::Canceled => OrderState::Cancelled,
            _ => OrderState::Unknown,
        }
    }

    fn map_trade_status(status: &TradeStatusType) -> Option<TradeState> {
        match status {
            TradeStatusType::Matched => Some(TradeState::Matched),
            TradeStatusType::Mined => Some(TradeState::Mined),
            TradeStatusType::Confirmed => Some(TradeState::Confirmed),
            TradeStatusType::Retrying => Some(TradeState::Retrying),
            TradeStatusType::Failed => Some(TradeState::Failed),
            _ => None,
        }
    }

    fn is_not_found(error: &polymarket_client_sdk_v2::error::Error) -> bool {
        error
            .downcast_ref::<SdkStatus>()
            .is_some_and(|status| status.status_code.as_u16() == 404)
    }

    async fn wait_for_ws(client: &AuthWsClient, channel: ChannelType) -> Result<(), VenueError> {
        for _ in 0..100 {
            if client.connection_state(channel).is_connected() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(VenueError::Stale(format!(
            "{channel:?} WebSocket did not connect within 10 seconds"
        )))
    }

    #[allow(clippy::needless_pass_by_value)]
    fn classify_pre_submit_error(error: polymarket_client_sdk_v2::error::Error) -> VenueError {
        if let Some(status) = error.downcast_ref::<SdkStatus>() {
            if status.status_code.as_u16() == 425 {
                return VenueError::MatchingEngineRestart(status.message.clone());
            }
            if status.message.contains("order_version_mismatch") {
                return VenueError::ProtocolMismatch(status.message.clone());
            }
            return VenueError::Rejected(status.to_string());
        }
        match error.kind() {
            SdkErrorKind::Geoblock => VenueError::GeographicRestricted(error.to_string()),
            SdkErrorKind::Validation => VenueError::Rejected(error.to_string()),
            _ => VenueError::Unavailable(error.to_string()),
        }
    }

    fn classify_submission_error(error: polymarket_client_sdk_v2::error::Error) -> VenueError {
        if let Some(status) = error.downcast_ref::<SdkStatus>() {
            if status.status_code.as_u16() == 425 {
                return VenueError::MatchingEngineRestart(status.message.clone());
            }
            if status.message.contains("order_version_mismatch") {
                return VenueError::ProtocolMismatch(status.message.clone());
            }
            if status.status_code.is_server_error() {
                return VenueError::Ambiguous(status.to_string());
            }
            return VenueError::Rejected(status.to_string());
        }
        match error.kind() {
            SdkErrorKind::Validation | SdkErrorKind::Geoblock => classify_pre_submit_error(error),
            _ => VenueError::Ambiguous(error.to_string()),
        }
    }

    fn reconstruct_signed_order(payload: &str) -> Result<SignedOrder, VenueError> {
        let value: Value = serde_json::from_str(payload)
            .map_err(|error| VenueError::Rejected(error.to_string()))?;
        let body = value
            .get("order")
            .ok_or_else(|| VenueError::Rejected("signed order is missing order".into()))?;
        let mut order = OrderV2::default();
        order.salt = parse_u256(body, "salt")?;
        order.maker = parse_address(body, "maker")?;
        order.signer = parse_address(body, "signer")?;
        order.tokenId = parse_u256(body, "tokenId")?;
        order.makerAmount = parse_u256(body, "makerAmount")?;
        order.takerAmount = parse_u256(body, "takerAmount")?;
        order.side = match body.get("side").and_then(Value::as_str) {
            Some("BUY") => SdkSide::Buy as u8,
            Some("SELL") => SdkSide::Sell as u8,
            _ => {
                return Err(VenueError::Rejected(
                    "signed order contains an invalid side".into(),
                ));
            }
        };
        order.signatureType = u8::try_from(value_u64(body, "signatureType")?)
            .map_err(|_| VenueError::Rejected("signatureType exceeds u8".into()))?;
        order.timestamp = parse_u256(body, "timestamp")?;
        order.metadata = parse_b256(body, "metadata")?;
        order.builder = parse_b256(body, "builder")?;
        let expiration = parse_u256(body, "expiration")?;
        let signature = body
            .get("signature")
            .and_then(Value::as_str)
            .ok_or_else(|| VenueError::Rejected("signed order is missing signature".into()))?;
        let order_type = match value.get("orderType").and_then(Value::as_str) {
            Some("GTC") => SdkOrderType::GTC,
            Some("GTD") => SdkOrderType::GTD,
            Some("FOK") => SdkOrderType::FOK,
            Some("FAK") => SdkOrderType::FAK,
            _ => {
                return Err(VenueError::Rejected(
                    "signed order contains an invalid order type".into(),
                ));
            }
        };
        let owner = value
            .get("owner")
            .and_then(Value::as_str)
            .ok_or_else(|| VenueError::Rejected("signed order is missing owner".into()))?
            .parse::<Uuid>()
            .map_err(|error| VenueError::Rejected(error.to_string()))?;
        let post_only = value
            .get("postOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let defer_exec = value
            .get("deferExec")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        Ok(SignedOrder::builder()
            .payload(OrderPayload::new(order, expiration))
            .signature(OrderSignature::Wrapped(signature.to_owned()))
            .order_type(order_type)
            .owner(owner)
            .post_only(post_only)
            .defer_exec(defer_exec)
            .build())
    }

    fn parse_u256(value: &Value, field: &str) -> Result<U256, VenueError> {
        U256::from_str(&value_string(value, field)?)
            .map_err(|error| VenueError::Rejected(error.to_string()))
    }

    fn parse_address(value: &Value, field: &str) -> Result<Address, VenueError> {
        Address::from_str(&value_string(value, field)?)
            .map_err(|error| VenueError::Rejected(error.to_string()))
    }

    fn parse_b256(value: &Value, field: &str) -> Result<B256, VenueError> {
        B256::from_str(&value_string(value, field)?)
            .map_err(|error| VenueError::Rejected(error.to_string()))
    }

    fn value_u64(value: &Value, field: &str) -> Result<u64, VenueError> {
        value
            .get(field)
            .and_then(Value::as_u64)
            .ok_or_else(|| VenueError::Rejected(format!("signed order is missing {field}")))
    }

    fn value_string(value: &Value, field: &str) -> Result<String, VenueError> {
        let value = value
            .get(field)
            .ok_or_else(|| VenueError::Rejected(format!("signed order is missing {field}")))?;
        if let Some(raw) = value.as_str() {
            Ok(raw.to_owned())
        } else if value.is_number() {
            Ok(value.to_string())
        } else {
            Err(VenueError::Rejected(format!(
                "signed order has invalid {field}"
            )))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[cfg(unix)]
        fn signer_config(key_path: &std::path::Path, private_key: &str) -> PolymarketConfig {
            let signer = PrivateKeySigner::from_str(private_key).unwrap();
            PolymarketConfig {
                private_key_file: Some(key_path.to_string_lossy().into_owned()),
                signer_address: Some(signer.address().to_string()),
                ..PolymarketConfig::default()
            }
        }

        #[cfg(unix)]
        #[test]
        fn local_live_key_accepts_only_0400_or_0600_regular_files() {
            use std::{fs, os::unix::fs::PermissionsExt as _};

            use tempfile::tempdir;

            let directory = tempdir().unwrap();
            let key = directory.path().join("key");
            let test_key_hex = "11".repeat(32);
            fs::write(&key, &test_key_hex).unwrap();
            let config = signer_config(&key, &test_key_hex);

            fs::set_permissions(&key, fs::Permissions::from_mode(0o400)).unwrap();
            assert!(load_live_signer(&config).is_ok());
            fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
            assert!(load_live_signer(&config).is_ok());
            for mode in [0o000, 0o500, 0o644, 0o700] {
                fs::set_permissions(&key, fs::Permissions::from_mode(mode)).unwrap();
                assert!(load_live_signer(&config).is_err());
            }
            for mode in [0o1400, 0o2400, 0o4400] {
                assert!(!private_key_mode_is_allowed(mode));
            }

            let link = directory.path().join("key-link");
            std::os::unix::fs::symlink(&key, &link).unwrap();
            let link_config = signer_config(&link, &test_key_hex);
            assert!(load_live_signer(&link_config).is_err());

            let directory_config = signer_config(directory.path(), &test_key_hex);
            assert!(load_live_signer(&directory_config).is_err());
        }

        #[cfg(unix)]
        #[test]
        fn local_live_key_errors_do_not_expose_key_material() {
            use std::{fs, os::unix::fs::PermissionsExt as _};

            use tempfile::tempdir;

            let directory = tempdir().unwrap();
            let key = directory.path().join("key");
            let malformed = format!("invalid-sensitive-value-{}", "x".repeat(32));
            fs::write(&key, &malformed).unwrap();
            fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
            let mut config = PolymarketConfig {
                private_key_file: Some(key.to_string_lossy().into_owned()),
                signer_address: Some(format!("0x{}", "11".repeat(20))),
                ..PolymarketConfig::default()
            };
            let error = load_live_signer(&config).unwrap_err().to_string();
            assert!(!error.contains(&malformed));

            let test_key_hex = "22".repeat(32);
            fs::write(&key, &test_key_hex).unwrap();
            config.signer_address = Some(format!("0x{}", "11".repeat(20)));
            let mismatch = load_live_signer(&config).unwrap_err().to_string();
            assert!(mismatch.contains("does not match"));
            assert!(!mismatch.contains(&test_key_hex));
        }

        #[cfg(not(unix))]
        #[test]
        fn native_windows_live_signing_is_rejected() {
            let error = load_live_signer(&PolymarketConfig::default())
                .unwrap_err()
                .to_string();
            assert!(error.contains("supported only on Unix platforms"));
        }

        #[test]
        fn persisted_signed_order_reconstructs_without_changing_bytes() {
            let mut order = OrderV2::default();
            order.salt = U256::from(42);
            order.maker = Address::repeat_byte(0x11);
            order.signer = Address::repeat_byte(0x22);
            order.tokenId = U256::from(123);
            order.makerAmount = U256::from(500_000);
            order.takerAmount = U256::from(1_000_000);
            order.side = SdkSide::Buy as u8;
            order.signatureType = SignatureType::Poly1271 as u8;
            order.timestamp = U256::from(1_700_000_000);
            order.metadata = B256::ZERO;
            order.builder = B256::ZERO;
            let signed = SignedOrder::builder()
                .payload(OrderPayload::new(order, U256::ZERO))
                .signature(OrderSignature::Wrapped("0x1234".into()))
                .order_type(SdkOrderType::GTC)
                .owner(Uuid::nil())
                .post_only(true)
                .defer_exec(false)
                .build();
            let persisted = serde_jcs::to_string(&signed).unwrap();
            let reconstructed = reconstruct_signed_order(&persisted).unwrap();
            assert_eq!(serde_jcs::to_string(&reconstructed).unwrap(), persisted);
        }
    }
}

#[cfg(not(feature = "live"))]
mod implementation {
    use async_trait::async_trait;
    use uuid::Uuid;

    use crate::{config::PolymarketConfig, domain::OrderIntentRequest};

    use super::super::{
        ExecutionVenue, MarketRules, ObservedVenueOrder, OrderBook, PreparedVenueOrder,
        ReconciliationResult, VenueCancellation, VenueError, VenueSubmission,
    };

    #[derive(Debug)]
    pub struct PolymarketVenue;

    pub fn validate_live_signer(_config: &PolymarketConfig) -> Result<(), VenueError> {
        Err(VenueError::LiveNotCompiled)
    }

    impl PolymarketVenue {
        #[allow(clippy::unused_async)]
        pub async fn new(_config: PolymarketConfig) -> Result<Self, VenueError> {
            Err(VenueError::LiveNotCompiled)
        }
    }

    #[async_trait]
    impl ExecutionVenue for PolymarketVenue {
        async fn market_rules(
            &self,
            _condition_id: &str,
            _token_id: &str,
        ) -> Result<MarketRules, VenueError> {
            Err(VenueError::LiveNotCompiled)
        }

        async fn order_book(&self, _token_id: &str) -> Result<OrderBook, VenueError> {
            Err(VenueError::LiveNotCompiled)
        }

        async fn prepare(
            &self,
            _intent_id: Uuid,
            _request: &OrderIntentRequest,
        ) -> Result<PreparedVenueOrder, VenueError> {
            Err(VenueError::LiveNotCompiled)
        }

        async fn submit(
            &self,
            _prepared: &PreparedVenueOrder,
            _request: &OrderIntentRequest,
        ) -> Result<VenueSubmission, VenueError> {
            Err(VenueError::LiveNotCompiled)
        }

        async fn cancel(&self, _venue_order_id: &str) -> Result<VenueCancellation, VenueError> {
            Err(VenueError::LiveNotCompiled)
        }

        async fn find_order(
            &self,
            _deterministic_order_id: &str,
        ) -> Result<Option<ObservedVenueOrder>, VenueError> {
            Err(VenueError::LiveNotCompiled)
        }

        async fn reconcile(&self) -> Result<ReconciliationResult, VenueError> {
            Err(VenueError::LiveNotCompiled)
        }

        async fn heartbeat(&self) -> Result<(), VenueError> {
            Err(VenueError::LiveNotCompiled)
        }

        fn requires_heartbeat(&self) -> bool {
            true
        }
    }
}

pub use implementation::{PolymarketVenue, validate_live_signer};
