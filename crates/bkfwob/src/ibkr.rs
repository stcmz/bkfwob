//! Interactive Brokers, through IB Gateway or TWS, with the `ibapi` crate's blocking client.
//!
//! Two layers, deliberately separate:
//!
//! - **Mapping** (`map`): pure functions between ibapi's types and bkfwob's. Every rule about how
//!   an IB status, order or execution reads lives here, and is unit-tested without a gateway.
//! - **Connection** ([`IbkrBroker`]): requests and waits. Each placement waits on its own order
//!   subscription for the first decisive answer; a background thread turns IB's global order
//!   update stream into [`BrokerEvent`]s.
//!
//! IB specifics worth knowing:
//!
//! - Our client order id travels in IB's `orderRef`, which IB stores with the order and returns
//!   on open orders, completed orders and executions. That is what lets an order of unknown fate
//!   be looked up after a timeout.
//! - IB's own numeric order id is per *API client id*, and an order can only be modified or
//!   cancelled by the client id that placed it. Hence the stable configured client id.
//! - Paper accounts are `DU…` (or `DF…` for advisors); the mode is read from the account id, never
//!   from the port.
//! - Notional (`usd:`) orders and fractional shares are not offered: IB's API supports them only
//!   in narrow cases, and an order IB would silently resize is worse than one refused up front.

use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bkfwob_core::broker::{Broker, BrokerError, BrokerEvent, Capabilities};
use bkfwob_core::types::{
    Account, ClientOrderId, Fill, Instrument, Mode, Order, OrderRequest, OrderSpec, Position,
    Quote, Tif,
};
use ibapi::accounts::types::AccountGroup;
use ibapi::accounts::{AccountSummaryResult, PositionUpdate};
use ibapi::client::blocking::Client;
use ibapi::orders::{
    CancelOrder, ExecutionFilter, Executions, OrderData, OrderUpdate, Orders, PlaceOrder,
};
use ibapi::subscriptions::SubscriptionItem;
use jiff::Timestamp;

use crate::config::IbkrConfig;

pub mod map {
    //! ibapi <-> bkfwob. Pure; no I/O.

    use bkfwob_core::clock::EXCHANGE_TZ;
    use bkfwob_core::status::OrderStatus;
    use bkfwob_core::types::{
        ClientOrderId, Fill, Instrument, Mode, Order, OrderKind, OrderSpec, Qty, Quote, Session,
        Side, Tif, Trail,
    };
    use ibapi::contracts::tick_types::TickType;
    use ibapi::contracts::{Contract, Currency, Exchange, SecurityType, Symbol};
    use ibapi::market_data::realtime::TickTypes;
    use ibapi::orders::{
        Action, ExecutionData, ExecutionSide, OrderData, OrderStatusKind, TimeInForce,
    };
    use jiff::Timestamp;
    use jiff::civil::DateTime;
    use jiff::tz::TimeZone;

    pub fn contract(instrument: &Instrument) -> Contract {
        let mut contract = Contract {
            symbol: Symbol(instrument.symbol.replace('.', " ")),
            security_type: SecurityType::Stock,
            exchange: Exchange(instrument.exchange.clone()),
            currency: Currency(instrument.currency.clone()),
            ..Contract::default()
        };
        if let Some(primary) = &instrument.primary_exchange {
            contract.primary_exchange = Exchange(primary.clone());
        }
        contract
    }

    /// IB writes class shares with a space (`BRK B`); the toolchain writes `BRK.B`.
    pub fn instrument(contract: &Contract) -> Instrument {
        let mut instrument = Instrument::stock(contract.symbol.0.replace(' ', "."));
        instrument.currency = if contract.currency.0.is_empty() {
            "USD".into()
        } else {
            contract.currency.0.clone()
        };
        if !contract.primary_exchange.0.is_empty() {
            instrument.primary_exchange = Some(contract.primary_exchange.0.clone());
        }
        instrument
    }

    pub fn mode_of(account: &str) -> Mode {
        if account.starts_with('D') {
            Mode::Paper
        } else {
            Mode::Live
        }
    }

    /// An IB order for `spec`. The caller guarantees shares (capabilities refuse notional).
    pub fn order(
        spec: &OrderSpec,
        client_id: &ClientOrderId,
        account: &str,
    ) -> ibapi::orders::Order {
        let mut order = ibapi::orders::Order {
            action: match spec.side {
                Side::Buy => Action::Buy,
                Side::Sell => Action::Sell,
            },
            total_quantity: spec.qty.value(),
            tif: match spec.tif {
                Tif::Day => TimeInForce::Day,
                Tif::Gtc => TimeInForce::GoodTillCanceled,
                Tif::Ioc => TimeInForce::ImmediateOrCancel,
            },
            outside_rth: spec.session == Session::Extended,
            order_ref: client_id.to_string(),
            account: account.to_string(),
            transmit: true,
            ..ibapi::orders::Order::default()
        };
        match spec.kind {
            OrderKind::Market => order.order_type = "MKT".into(),
            OrderKind::Limit { limit } => {
                order.order_type = "LMT".into();
                order.limit_price = Some(limit);
            }
            OrderKind::Stop { stop } => {
                order.order_type = "STP".into();
                order.aux_price = Some(stop);
            }
            OrderKind::StopLimit { stop, limit } => {
                order.order_type = "STP LMT".into();
                order.aux_price = Some(stop);
                order.limit_price = Some(limit);
            }
            OrderKind::Trailing { trail } => {
                order.order_type = "TRAIL".into();
                match trail {
                    Trail::Amount(a) => order.aux_price = Some(a),
                    Trail::Percent(p) => order.trailing_percent = Some(p),
                }
            }
        }
        order
    }

    /// Our id for an IB order: the `orderRef` we set, or `ib-<permId>` for orders placed
    /// elsewhere (TWS by hand, another program).
    pub fn client_id(order_ref: &str, perm_id: i64) -> ClientOrderId {
        if order_ref.is_empty() {
            ClientOrderId::new(format!("ib-{perm_id}"))
        } else {
            ClientOrderId::new(order_ref)
        }
    }

    pub fn status(kind: &OrderStatusKind, filled: f64) -> OrderStatus {
        match kind {
            // IB's PendingSubmit means sent and not yet acknowledged by the venue: working, for us.
            OrderStatusKind::ApiPending
            | OrderStatusKind::PendingSubmit
            | OrderStatusKind::PreSubmitted
            | OrderStatusKind::Submitted => {
                if filled > 0.0 {
                    OrderStatus::PartiallyFilled
                } else {
                    OrderStatus::Submitted
                }
            }
            OrderStatusKind::PendingCancel => OrderStatus::PendingCancel,
            OrderStatusKind::ApiCancelled | OrderStatusKind::Cancelled => OrderStatus::Cancelled,
            OrderStatusKind::Filled => OrderStatus::Filled,
            // Inactive: rejected, or parked by IB pending something it will not do on its own.
            OrderStatusKind::Inactive => OrderStatus::Rejected,
            // A status this build does not know: assume it could still fill.
            OrderStatusKind::Unknown(_) => OrderStatus::Submitted,
        }
    }

    fn kind(order: &ibapi::orders::Order) -> Option<OrderKind> {
        let price = |p: Option<f64>| p.filter(|v| v.is_finite() && *v > 0.0);
        Some(match order.order_type.as_str() {
            "MKT" => OrderKind::Market,
            "LMT" => OrderKind::Limit {
                limit: price(order.limit_price)?,
            },
            "STP" => OrderKind::Stop {
                stop: price(order.aux_price)?,
            },
            "STP LMT" => OrderKind::StopLimit {
                stop: price(order.aux_price)?,
                limit: price(order.limit_price)?,
            },
            "TRAIL" => OrderKind::Trailing {
                trail: match price(order.trailing_percent) {
                    Some(p) => Trail::Percent(p),
                    None => Trail::Amount(price(order.aux_price)?),
                },
            },
            _ => return None,
        })
    }

    /// The bkfwob view of an IB order, or `None` for something bkfwob does not model (options,
    /// combos, algos). Fill progress comes from the separate status message when there is one.
    pub fn order_from(
        data: &OrderData,
        status: Option<&ibapi::orders::OrderStatus>,
        now: Timestamp,
    ) -> Option<Order> {
        if data.contract.security_type != SecurityType::Stock {
            return None;
        }
        let ib = &data.order;
        let spec = OrderSpec {
            instrument: instrument(&data.contract),
            side: match ib.action {
                Action::Buy => Side::Buy,
                _ => Side::Sell,
            },
            qty: Qty::Shares(ib.total_quantity),
            kind: kind(ib)?,
            tif: match ib.tif {
                TimeInForce::GoodTillCanceled => Tif::Gtc,
                TimeInForce::ImmediateOrCancel => Tif::Ioc,
                _ => Tif::Day,
            },
            session: if ib.outside_rth {
                Session::Extended
            } else {
                Session::Rth
            },
            tag: None,
        };
        let (kind_status, filled, avg) = match status {
            Some(s) => (&s.status, s.filled, s.average_fill_price),
            None => (&data.order_state.status, 0.0, None),
        };
        Some(Order {
            client_id: client_id(&ib.order_ref, ib.perm_id),
            broker_id: Some(data.order_id.to_string()),
            spec,
            status: self::status(kind_status, filled),
            filled_qty: filled,
            avg_fill_price: avg.filter(|p| *p > 0.0),
            reason: None,
            updated_at: now,
        })
    }

    fn quote_parts(ticks: &[TickTypes]) -> (Quote, bool, Option<Timestamp>) {
        let mut quote = Quote {
            instrument: Instrument::stock(""),
            bid: None,
            ask: None,
            last: None,
            time: None,
        };
        let mut delayed = false;
        let mut traded_at = None;
        let mut put = |tick: &TickType, price: f64| {
            if !(price.is_finite() && price > 0.0) {
                return;
            }
            let slot = match tick {
                TickType::Bid => &mut quote.bid,
                TickType::Ask => &mut quote.ask,
                TickType::Last => &mut quote.last,
                TickType::DelayedBid => {
                    delayed = true;
                    &mut quote.bid
                }
                TickType::DelayedAsk => {
                    delayed = true;
                    &mut quote.ask
                }
                TickType::DelayedLast => {
                    delayed = true;
                    &mut quote.last
                }
                _ => return,
            };
            *slot = Some(price);
        };
        for tick in ticks {
            match tick {
                TickTypes::Price(p) => put(&p.tick_type, p.price),
                TickTypes::PriceSize(p) => put(&p.price_tick_type, p.price),
                TickTypes::String(s)
                    if matches!(
                        s.tick_type,
                        TickType::LastTimestamp | TickType::DelayedLastTimestamp
                    ) =>
                {
                    traded_at = s
                        .value
                        .trim()
                        .parse::<i64>()
                        .ok()
                        .and_then(|secs| Timestamp::from_second(secs).ok());
                }
                _ => {}
            }
        }
        (quote, delayed, traded_at)
    }

    /// Whether a snapshot has everything [`quote`] uses, so collection can stop early.
    pub fn quote_complete(ticks: &[TickTypes]) -> bool {
        let (q, _, traded_at) = quote_parts(ticks);
        q.bid.is_some() && q.ask.is_some() && q.last.is_some() && traded_at.is_some()
    }

    /// A quote from snapshot ticks.
    ///
    /// Its time is the **last trade's** time when IB sends it, not the moment the snapshot
    /// arrived: on a Sunday IB still returns Friday's bid and ask, and stamping them "now" would
    /// let the freshness guard wave a market order through a closed market. Without a trade time,
    /// delayed data is stamped fifteen minutes old and live data now.
    pub fn quote(instrument: &Instrument, ticks: &[TickTypes], now: Timestamp) -> Option<Quote> {
        let (mut quote, delayed, traded_at) = quote_parts(ticks);
        if quote.bid.is_none() && quote.ask.is_none() && quote.last.is_none() {
            return None;
        }
        quote.instrument = instrument.clone();
        quote.time = traded_at.or_else(|| {
            let age = if delayed { 15 * 60 } else { 0 };
            now.checked_sub(jiff::SignedDuration::from_secs(age)).ok()
        });
        Some(quote)
    }

    /// IB execution times come as `20261005 10:28:43 US/Eastern` (zone named) or
    /// `20261005-14:28:43` (UTC).
    pub fn exec_time(text: &str) -> Option<Timestamp> {
        let text = text.trim();
        if let Some((date, time)) = text.split_once('-')
            && !time.contains(' ')
        {
            let dt = DateTime::strptime("%Y%m%d %H:%M:%S", format!("{date} {time}")).ok()?;
            return dt.to_zoned(TimeZone::UTC).ok().map(|z| z.timestamp());
        }
        let mut parts = text.split_whitespace();
        let date = parts.next()?;
        let time = parts.next()?;
        let tz = parts
            .next()
            .and_then(|name| TimeZone::get(name).ok())
            .unwrap_or_else(|| TimeZone::get(EXCHANGE_TZ).unwrap_or(TimeZone::UTC));
        let dt = DateTime::strptime("%Y%m%d %H:%M:%S", format!("{date} {time}")).ok()?;
        dt.to_zoned(tz).ok().map(|z| z.timestamp())
    }

    pub fn fill(data: &ExecutionData, commission: Option<f64>, now: Timestamp) -> Fill {
        let e = &data.execution;
        Fill {
            exec_id: e.execution_id.clone(),
            client_id: client_id(&e.order_reference, e.perm_id),
            broker_order_id: Some(e.order_id.to_string()),
            instrument: instrument(&data.contract),
            side: match e.side {
                ExecutionSide::Bought => Side::Buy,
                ExecutionSide::Sold => Side::Sell,
            },
            qty: e.shares,
            price: e.price,
            commission,
            time: exec_time(&e.time).unwrap_or(now),
        }
    }
}

/// Classifies an ibapi error by what it means for an order that may have been sent.
fn classify(error: ibapi::Error, sent: bool) -> BrokerError {
    use ibapi::Error as E;
    match error {
        E::Notice(notice) => BrokerError::Rejected(format!("{} ({})", notice.message, notice.code)),
        E::InvalidArgument(why) | E::ServerVersion(_, _, why) => BrokerError::Rejected(why),
        E::ConnectionFailed | E::ConnectionRejected(_) | E::Shutdown if !sent => {
            BrokerError::Disconnected(error.to_string())
        }
        E::Io(_) | E::ConnectionReset if !sent => BrokerError::Disconnected(error.to_string()),
        other if sent => BrokerError::Ambiguous(other.to_string()),
        other => BrokerError::Other(other.to_string()),
    }
}

pub struct IbkrBroker {
    client: Arc<Client>,
    account: String,
    mode: Mode,
    timeout: Duration,
    events: Receiver<BrokerEvent>,
    /// Execution id -> commission, filled in by the event thread as reports arrive.
    commissions: Arc<Mutex<HashMap<String, f64>>>,
}

impl IbkrBroker {
    pub fn connect(config: &IbkrConfig, mode: Mode) -> Result<Self, BrokerError> {
        let address = format!("{}:{}", config.host, config.port_for(mode));
        let client = Client::connect(&address, config.client_id).map_err(|e| {
            BrokerError::Disconnected(format!(
                "cannot reach IB Gateway/TWS at {address} (client id {}): {e}",
                config.client_id
            ))
        })?;
        let accounts = client.managed_accounts().map_err(|e| classify(e, false))?;
        let account = match (&config.account, accounts.as_slice()) {
            (Some(wanted), _) if accounts.iter().any(|a| a == wanted) => wanted.clone(),
            (Some(wanted), _) => {
                return Err(BrokerError::Other(format!(
                    "account {wanted} is not on this login (it has {})",
                    accounts.join(", ")
                )));
            }
            (None, [only]) => only.clone(),
            (None, many) => {
                return Err(BrokerError::Other(format!(
                    "this login has several accounts ({}); set [ibkr] account or --account",
                    many.join(", ")
                )));
            }
        };
        let client = Arc::new(client);
        let commissions = Arc::new(Mutex::new(HashMap::new()));
        let (sender, events) = mpsc::channel();
        spawn_event_thread(client.clone(), sender, commissions.clone())?;
        Ok(Self {
            mode: map::mode_of(&account),
            client,
            account,
            timeout: Duration::from_secs(config.timeout_secs.max(1)),
            events,
            commissions,
        })
    }

    fn now() -> Timestamp {
        Timestamp::now()
    }

    /// Every open order in the account (all API clients and TWS), as IB order data plus the
    /// latest status of each.
    fn open_order_data(
        &self,
    ) -> Result<Vec<(OrderData, Option<ibapi::orders::OrderStatus>)>, BrokerError> {
        let subscription = self
            .client
            .all_open_orders()
            .map_err(|e| classify(e, false))?;
        collect_orders(subscription.timeout_iter(self.timeout))
    }

    fn completed_order_data(
        &self,
    ) -> Result<Vec<(OrderData, Option<ibapi::orders::OrderStatus>)>, BrokerError> {
        let subscription = self
            .client
            .completed_orders(false)
            .map_err(|e| classify(e, false))?;
        collect_orders(subscription.timeout_iter(self.timeout))
    }

    /// IB's numeric id for one of our orders, checking it can be changed from this client.
    fn ib_order_id(&self, id: &ClientOrderId) -> Result<i32, BrokerError> {
        let open = self.open_order_data()?;
        let (data, _) = open
            .iter()
            .find(|(d, _)| map::client_id(&d.order.order_ref, d.order.perm_id) == *id)
            .ok_or_else(|| BrokerError::NotFound(id.clone()))?;
        if data.order.client_id != self.client.client_id() {
            return Err(BrokerError::Rejected(format!(
                "order {id} was placed by API client {} and IB only lets that client change it \
                 (this is client {}); cancel it there, or with `cancel all`",
                data.order.client_id,
                self.client.client_id()
            )));
        }
        Ok(data.order_id)
    }

    /// Sends `order` under IB id `order_id` and waits for the first decisive answer.
    fn send(
        &self,
        order_id: i32,
        spec: &OrderSpec,
        request_id: &ClientOrderId,
    ) -> Result<Order, BrokerError> {
        let contract = map::contract(&spec.instrument);
        let order = map::order(spec, request_id, &self.account);
        let subscription = self
            .client
            .place_order(order_id, &contract, &order)
            .map_err(|e| classify(e, false))?;
        let mut data: Option<OrderData> = None;
        let mut status = None;
        loop {
            match subscription.next_timeout(self.timeout) {
                Some(Ok(SubscriptionItem::Data(PlaceOrder::OpenOrder(d)))) => data = Some(d),
                Some(Ok(SubscriptionItem::Data(PlaceOrder::OrderStatus(s)))) => status = Some(s),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(classify(e, true)),
                None if data.is_none() && status.is_none() => {
                    return Err(BrokerError::Ambiguous(format!(
                        "no answer from IB within {:?}",
                        self.timeout
                    )));
                }
                None => {}
            }
            if let (Some(d), Some(s)) = (&data, &status) {
                return map::order_from(d, Some(s), Self::now()).ok_or_else(|| {
                    BrokerError::Other("IB returned an order bkfwob cannot read".into())
                });
            }
            if data.is_some() || status.is_some() {
                // One half arrived; give the other a moment, then go with what we have.
                if let Some(Ok(SubscriptionItem::Data(item))) =
                    subscription.next_timeout(Duration::from_millis(500))
                {
                    match item {
                        PlaceOrder::OpenOrder(d) => data = Some(d),
                        PlaceOrder::OrderStatus(s) => status = Some(s),
                        _ => {}
                    }
                }
                return match &data {
                    Some(d) => map::order_from(d, status.as_ref(), Self::now()).ok_or_else(|| {
                        BrokerError::Other("IB returned an order bkfwob cannot read".into())
                    }),
                    None => {
                        // Accepted (a status arrived) but the order details did not: the order
                        // exists; describe it from what we sent.
                        let s = status.expect("one half arrived");
                        Ok(Order {
                            client_id: request_id.clone(),
                            broker_id: Some(order_id.to_string()),
                            spec: spec.clone(),
                            status: map::status(&s.status, s.filled),
                            filled_qty: s.filled,
                            avg_fill_price: s.average_fill_price,
                            reason: None,
                            updated_at: Self::now(),
                        })
                    }
                };
            }
        }
    }
}

type OrderRows = Vec<(OrderData, Option<ibapi::orders::OrderStatus>)>;

fn collect_orders<I>(items: I) -> Result<OrderRows, BrokerError>
where
    I: Iterator<Item = Result<SubscriptionItem<Orders>, ibapi::Error>>,
{
    let mut data: Vec<OrderData> = Vec::new();
    let mut statuses: HashMap<i32, ibapi::orders::OrderStatus> = HashMap::new();
    for item in items {
        match item.map_err(|e| classify(e, false))? {
            SubscriptionItem::Data(Orders::OrderData(d)) => data.push(d),
            SubscriptionItem::Data(Orders::OrderStatus(s)) => {
                statuses.insert(s.order_id, s);
            }
            SubscriptionItem::Notice(_) => {}
        }
    }
    Ok(data
        .into_iter()
        .map(|d| {
            let s = statuses.remove(&d.order_id);
            (d, s)
        })
        .collect())
}

/// Turns IB's account-wide order update stream into events. Order details and statuses arrive
/// as separate messages; the thread keeps the latest details per IB order id so each status can
/// be reported as a whole order.
fn spawn_event_thread(
    client: Arc<Client>,
    sender: Sender<BrokerEvent>,
    commissions: Arc<Mutex<HashMap<String, f64>>>,
) -> Result<(), BrokerError> {
    let stream = client
        .order_update_stream()
        .map_err(|e| classify(e, false))?;
    std::thread::Builder::new()
        .name("ibkr-events".into())
        .spawn(move || {
            let mut known: HashMap<i32, OrderData> = HashMap::new();
            for item in stream.iter() {
                let now = Timestamp::now();
                let event = match item {
                    Ok(SubscriptionItem::Data(OrderUpdate::OpenOrder(d))) => {
                        let event = map::order_from(&d, None, now)
                            .map(|order| BrokerEvent::Order { order });
                        known.insert(d.order_id, d);
                        event
                    }
                    Ok(SubscriptionItem::Data(OrderUpdate::OrderStatus(s))) => known
                        .get(&s.order_id)
                        .and_then(|d| map::order_from(d, Some(&s), now))
                        .map(|order| BrokerEvent::Order { order }),
                    Ok(SubscriptionItem::Data(OrderUpdate::ExecutionData(e))) => {
                        let commission = commissions
                            .lock()
                            .ok()
                            .and_then(|c| c.get(&e.execution.execution_id).copied());
                        Some(BrokerEvent::Fill {
                            fill: map::fill(&e, commission, now),
                        })
                    }
                    Ok(SubscriptionItem::Data(OrderUpdate::CommissionReport(r))) => {
                        if let Ok(mut c) = commissions.lock() {
                            c.insert(r.execution_id, r.commission);
                        }
                        None
                    }
                    Ok(SubscriptionItem::Data(OrderUpdate::OrderBound(_))) => None,
                    Ok(SubscriptionItem::Notice(n)) => Some(BrokerEvent::Notice {
                        message: format!("{} ({})", n.message, n.code),
                    }),
                    Err(e) => Some(BrokerEvent::Disconnected {
                        reason: e.to_string(),
                    }),
                };
                if let Some(event) = event
                    && sender.send(event).is_err()
                {
                    return;
                }
            }
            let _ = sender.send(BrokerEvent::Disconnected {
                reason: "the IB order stream ended".into(),
            });
        })
        .map_err(|e| BrokerError::Other(e.to_string()))?;
    Ok(())
}

fn parse_money(value: &str) -> f64 {
    value.parse().unwrap_or(0.0)
}

impl Broker for IbkrBroker {
    fn name(&self) -> &str {
        "ibkr"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            market: true,
            limit: true,
            stop: true,
            stop_limit: true,
            trailing_amount: true,
            trailing_percent: true,
            tifs: vec![Tif::Day, Tif::Gtc, Tif::Ioc],
            extended_hours: true,
            market_outside_rth: false,
            notional: false,
            fractional: false,
        }
    }

    fn account(&mut self) -> Result<Account, BrokerError> {
        let tags = ["TotalCashValue", "BuyingPower", "NetLiquidation"];
        let subscription = self
            .client
            .account_summary(&AccountGroup("All".into()), &tags)
            .map_err(|e| classify(e, false))?;
        let mut account = Account {
            id: self.account.clone(),
            mode: self.mode,
            currency: "USD".into(),
            cash: 0.0,
            buying_power: 0.0,
            equity: 0.0,
        };
        for item in subscription.timeout_iter(self.timeout) {
            match item.map_err(|e| classify(e, false))? {
                SubscriptionItem::Data(AccountSummaryResult::Summary(s))
                    if s.account == self.account =>
                {
                    let v = parse_money(&s.value);
                    match s.tag.as_str() {
                        "TotalCashValue" => {
                            account.cash = v;
                            if !s.currency.is_empty() {
                                account.currency = s.currency.clone();
                            }
                        }
                        "BuyingPower" => account.buying_power = v,
                        "NetLiquidation" => account.equity = v,
                        _ => {}
                    }
                }
                SubscriptionItem::Data(AccountSummaryResult::End) => break,
                _ => {}
            }
        }
        subscription.cancel();
        Ok(account)
    }

    fn positions(&mut self) -> Result<Vec<Position>, BrokerError> {
        let subscription = self.client.positions().map_err(|e| classify(e, false))?;
        let mut out = Vec::new();
        for item in subscription.timeout_iter(self.timeout) {
            match item.map_err(|e| classify(e, false))? {
                SubscriptionItem::Data(PositionUpdate::Position(p)) => {
                    if p.account == self.account
                        && p.contract.security_type == ibapi::contracts::SecurityType::Stock
                        && p.position != 0.0
                    {
                        out.push(Position {
                            instrument: map::instrument(&p.contract),
                            qty: p.position,
                            avg_cost: p.average_cost,
                        });
                    }
                }
                SubscriptionItem::Data(PositionUpdate::PositionEnd) => break,
                SubscriptionItem::Notice(_) => {}
            }
        }
        subscription.cancel();
        Ok(out)
    }

    fn open_orders(&mut self) -> Result<Vec<Order>, BrokerError> {
        let now = Self::now();
        Ok(self
            .open_order_data()?
            .iter()
            .filter(|(d, _)| d.order.account.is_empty() || d.order.account == self.account)
            .filter_map(|(d, s)| map::order_from(d, s.as_ref(), now))
            .filter(|o| o.status.is_live())
            .collect())
    }

    fn find_order(&mut self, id: &ClientOrderId) -> Result<Option<Order>, BrokerError> {
        let now = Self::now();
        let matches = |d: &OrderData| map::client_id(&d.order.order_ref, d.order.perm_id) == *id;
        if let Some((d, s)) = self.open_order_data()?.iter().find(|(d, _)| matches(d)) {
            return Ok(map::order_from(d, s.as_ref(), now));
        }
        Ok(self
            .completed_order_data()?
            .iter()
            .find(|(d, _)| matches(d))
            .and_then(|(d, s)| map::order_from(d, s.as_ref(), now)))
    }

    fn fills(&mut self, since: Timestamp) -> Result<Vec<Fill>, BrokerError> {
        let filter = ExecutionFilter {
            account_code: self.account.clone(),
            time: since
                .to_zoned(jiff::tz::TimeZone::UTC)
                .strftime("%Y%m%d-%H:%M:%S")
                .to_string(),
            ..ExecutionFilter::default()
        };
        let subscription = self
            .client
            .executions(filter)
            .map_err(|e| classify(e, false))?;
        let mut executions = Vec::new();
        let mut commissions: HashMap<String, f64> = HashMap::new();
        for item in subscription.timeout_iter(self.timeout) {
            match item.map_err(|e| classify(e, false))? {
                SubscriptionItem::Data(Executions::ExecutionData(e)) => executions.push(e),
                SubscriptionItem::Data(Executions::CommissionReport(r)) => {
                    commissions.insert(r.execution_id, r.commission);
                }
                SubscriptionItem::Notice(_) => {}
            }
        }
        let known = self
            .commissions
            .lock()
            .map(|c| c.clone())
            .unwrap_or_default();
        let now = Self::now();
        Ok(executions
            .iter()
            .filter(|e| e.contract.security_type == ibapi::contracts::SecurityType::Stock)
            .map(|e| {
                let id = &e.execution.execution_id;
                let commission = commissions.get(id).or_else(|| known.get(id)).copied();
                map::fill(e, commission, now)
            })
            .filter(|f| f.time >= since)
            .collect())
    }

    fn quote(&mut self, instrument: &Instrument) -> Result<Quote, BrokerError> {
        let contract = map::contract(instrument);
        let subscription = self
            .client
            .market_data(&contract)
            .snapshot()
            .subscribe()
            .map_err(|e| classify(e, false))?;
        // IB holds a snapshot open for up to ~11 s before ending it; stop as soon as the parts
        // that matter are in.
        let ticks = subscription.collect_until(self.timeout, map::quote_complete);
        subscription.cancel();
        map::quote(instrument, &ticks, Self::now()).ok_or_else(|| {
            BrokerError::Other(format!(
                "no quote for {instrument} (market data permissions?)"
            ))
        })
    }

    fn place(&mut self, request: &OrderRequest) -> Result<Order, BrokerError> {
        let order_id = self.client.next_order_id();
        self.send(order_id, &request.spec, &request.client_id)
    }

    fn modify(&mut self, id: &ClientOrderId, spec: &OrderSpec) -> Result<Order, BrokerError> {
        let order_id = self.ib_order_id(id)?;
        self.send(order_id, spec, id)
    }

    fn cancel(&mut self, id: &ClientOrderId) -> Result<(), BrokerError> {
        let order_id = self.ib_order_id(id)?;
        let subscription = self
            .client
            .cancel_order(order_id, "")
            .map_err(|e| classify(e, false))?;
        match subscription.next_timeout(self.timeout) {
            Some(Ok(SubscriptionItem::Data(CancelOrder::OrderStatus(_)))) => Ok(()),
            Some(Ok(SubscriptionItem::Notice(_))) | None => Ok(()), // confirmation arrives as an event
            Some(Err(e)) => Err(classify(e, true)),
        }
    }

    fn cancel_all(&mut self) -> Result<(), BrokerError> {
        self.client.global_cancel().map_err(|e| classify(e, false))
    }

    fn next_event(&mut self, timeout: Duration) -> Result<Option<BrokerEvent>, BrokerError> {
        match self.events.recv_timeout(timeout) {
            Ok(event) => Ok(Some(event)),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => Err(BrokerError::Disconnected(
                "the IB event thread stopped".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::map;
    use bkfwob_core::status::OrderStatus;
    use bkfwob_core::types::{
        ClientOrderId, Instrument, Mode, OrderKind, OrderSpec, Qty, Session, Side, Tif, Trail,
    };
    use ibapi::orders::{OrderData, OrderStatusKind};

    fn spec(kind: OrderKind) -> OrderSpec {
        OrderSpec::new(
            Instrument::stock("BRK.B"),
            Side::Sell,
            Qty::Shares(7.0),
            kind,
        )
    }

    fn round_trip(spec: &OrderSpec) -> OrderSpec {
        let id = ClientOrderId::new("bk-20261005-0001");
        let data = OrderData {
            order_id: 42,
            contract: map::contract(&spec.instrument),
            order: map::order(spec, &id, "DU1234567"),
            ..OrderData::default()
        };
        let order = map::order_from(&data, None, jiff::Timestamp::UNIX_EPOCH).unwrap();
        assert_eq!(order.client_id, id, "the client id rides in orderRef");
        assert_eq!(order.broker_id.as_deref(), Some("42"));
        order.spec
    }

    #[test]
    fn every_order_kind_survives_the_trip_through_ib() {
        for kind in [
            OrderKind::Market,
            OrderKind::Limit { limit: 410.25 },
            OrderKind::Stop { stop: 400.0 },
            OrderKind::StopLimit {
                stop: 400.0,
                limit: 399.5,
            },
            OrderKind::Trailing {
                trail: Trail::Amount(2.5),
            },
            OrderKind::Trailing {
                trail: Trail::Percent(1.5),
            },
        ] {
            let mut s = spec(kind);
            s.tif = Tif::Gtc;
            s.session = Session::Extended;
            assert_eq!(round_trip(&s), s, "{kind}");
        }
    }

    #[test]
    fn ib_order_fields() {
        let mut s = spec(OrderKind::StopLimit {
            stop: 400.0,
            limit: 399.5,
        });
        s.session = Session::Extended;
        let o = map::order(&s, &ClientOrderId::new("bk-1"), "DU1");
        assert_eq!(o.order_type, "STP LMT");
        assert_eq!(o.aux_price, Some(400.0));
        assert_eq!(o.limit_price, Some(399.5));
        assert!(o.outside_rth);
        assert_eq!(o.order_ref, "bk-1");
        assert_eq!(o.account, "DU1");
        assert!(o.transmit);
        let regular = map::order(&spec(OrderKind::Market), &ClientOrderId::new("x"), "DU1");
        assert!(!regular.outside_rth, "regular hours unless asked");
    }

    #[test]
    fn class_shares_use_a_space_at_ib() {
        let c = map::contract(&Instrument::stock("BRK.B"));
        assert_eq!(c.symbol.0, "BRK B");
        assert_eq!(map::instrument(&c).symbol, "BRK.B");
    }

    #[test]
    fn statuses() {
        use OrderStatusKind as K;
        assert_eq!(map::status(&K::PreSubmitted, 0.0), OrderStatus::Submitted);
        assert_eq!(map::status(&K::PendingSubmit, 0.0), OrderStatus::Submitted);
        assert_eq!(
            map::status(&K::Submitted, 3.0),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(
            map::status(&K::PendingCancel, 0.0),
            OrderStatus::PendingCancel
        );
        assert_eq!(map::status(&K::ApiCancelled, 0.0), OrderStatus::Cancelled);
        assert_eq!(map::status(&K::Filled, 7.0), OrderStatus::Filled);
        assert_eq!(map::status(&K::Inactive, 0.0), OrderStatus::Rejected);
        assert_eq!(
            map::status(&K::Unknown("Weird".into()), 0.0),
            OrderStatus::Submitted,
            "an unknown status is assumed live"
        );
    }

    #[test]
    fn foreign_orders_get_a_perm_id_based_id() {
        assert_eq!(map::client_id("", 98765).as_str(), "ib-98765");
        assert_eq!(map::client_id("bk-x", 98765).as_str(), "bk-x");
    }

    #[test]
    fn paper_accounts_are_d_prefixed() {
        assert_eq!(map::mode_of("DU1234567"), Mode::Paper);
        assert_eq!(map::mode_of("DF1234567"), Mode::Paper);
        assert_eq!(map::mode_of("U1234567"), Mode::Live);
    }

    #[test]
    fn execution_times() {
        let named = map::exec_time("20261005 10:28:43 US/Eastern").unwrap();
        assert_eq!(named.to_string(), "2026-10-05T14:28:43Z");
        let utc = map::exec_time("20261005-14:28:43").unwrap();
        assert_eq!(utc, named);
        let bare = map::exec_time("20261005 10:28:43").unwrap();
        assert_eq!(bare, named, "no zone means New York");
        assert!(map::exec_time("garbage").is_none());
    }
}

#[cfg(test)]
mod quote_tests {
    use super::map;
    use bkfwob_core::types::Instrument;
    use ibapi::contracts::tick_types::TickType;
    use ibapi::market_data::realtime::{TickAttribute, TickPrice, TickString, TickTypes};
    use jiff::Timestamp;

    fn price(tick_type: TickType, price: f64) -> TickTypes {
        TickTypes::Price(TickPrice {
            tick_type,
            price,
            attributes: TickAttribute::default(),
        })
    }

    fn traded_at(secs: i64) -> TickTypes {
        TickTypes::String(TickString {
            tick_type: TickType::LastTimestamp,
            value: secs.to_string(),
        })
    }

    fn now() -> Timestamp {
        "2026-10-05T02:55:00Z".parse().unwrap() // Sunday evening, New York
    }

    /// Sunday night, IB still quotes Friday's book. The quote must carry Friday's time so a
    /// market order is refused as stale rather than sent into a closed market.
    #[test]
    fn a_weekend_quote_carries_the_last_trade_time_not_now() {
        let friday_close: Timestamp = "2026-10-02T20:00:00Z".parse().unwrap();
        let ticks = [
            price(TickType::Bid, 333.08),
            price(TickType::Ask, 333.5),
            price(TickType::Last, 333.37),
            traded_at(friday_close.as_second()),
        ];
        assert!(map::quote_complete(&ticks));
        let q = map::quote(&Instrument::stock("AAPL"), &ticks, now()).unwrap();
        assert_eq!(q.time, Some(friday_close));
        assert_eq!(
            (q.bid, q.ask, q.last),
            (Some(333.08), Some(333.5), Some(333.37))
        );
    }

    #[test]
    fn without_a_trade_time_delayed_data_is_aged_and_live_data_is_now() {
        let delayed = [price(TickType::DelayedLast, 100.0)];
        assert!(!map::quote_complete(&delayed));
        let q = map::quote(&Instrument::stock("X"), &delayed, now()).unwrap();
        assert_eq!(q.time.unwrap().as_second(), now().as_second() - 900);
        let live = [price(TickType::Bid, 100.0)];
        let q = map::quote(&Instrument::stock("X"), &live, now()).unwrap();
        assert_eq!(q.time, Some(now()));
    }

    #[test]
    fn no_prices_is_no_quote() {
        assert!(
            map::quote(
                &Instrument::stock("X"),
                &[price(TickType::Bid, -1.0)],
                now()
            )
            .is_none()
        );
        assert!(map::quote(&Instrument::stock("X"), &[], now()).is_none());
    }
}
