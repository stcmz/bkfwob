//! A deterministic, scriptable broker for tests and offline use.
//!
//! Not a paper account. A paper account is a real broker that happens to use pretend money; it
//! cannot be told "drop the connection right after accepting the next order" or "deliver that
//! execution twice". This can, which is what the reconciliation and guardrail tests need: every
//! failure a real broker produces rarely and at random, on demand and in order.
//!
//! Matching is deliberately simple: market orders fill at the touch when placed (if
//! `auto_fill`), everything else rests until the test calls [`FakeBroker::fill`]. Its state
//! serializes, so the CLI's `fake` broker persists between invocations.

use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::broker::{Broker, BrokerError, BrokerEvent, Capabilities};
use crate::clock::SharedClock;
use crate::status::OrderStatus;
use crate::types::{
    Account, ClientOrderId, Fill, Instrument, Mode, Order, OrderKind, OrderRequest, OrderSpec,
    Position, Qty, Quote,
};

/// A behavior queued for the next broker call that can take it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Script {
    /// The next `place` or `modify` is rejected with this reason.
    Reject(String),
    /// The next `place` is accepted by the broker, but the reply is lost.
    LoseReplyAfterAccept,
    /// The next `place` is lost in transit: the broker never sees it, and we time out.
    LoseRequest,
    /// The next call of any kind finds the connection down (until [`FakeBroker::reconnect`]).
    Disconnect,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FakeState {
    account_id: String,
    mode: Mode,
    cash: f64,
    positions: BTreeMap<String, Position>,
    orders: BTreeMap<ClientOrderId, Order>,
    fills: Vec<Fill>,
    quotes: BTreeMap<String, Quote>,
    events: VecDeque<BrokerEvent>,
    script: VecDeque<Script>,
    connected: bool,
    next_exec: u64,
    next_broker_id: u64,
    auto_fill: bool,
}

pub struct FakeBroker {
    state: FakeState,
    clock: SharedClock,
    caps: Capabilities,
}

impl FakeBroker {
    pub fn new(clock: SharedClock, cash: f64) -> Self {
        Self {
            state: FakeState {
                account_id: "FAKE0001".into(),
                mode: Mode::Paper,
                cash,
                connected: true,
                auto_fill: true,
                ..FakeState::default()
            },
            clock,
            caps: Capabilities::all(),
        }
    }

    /// Restores a saved fake, or starts a new one with `cash`.
    pub fn load_or_new(path: &Path, clock: SharedClock, cash: f64) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new(clock, cash));
        }
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let state = serde_json::from_str(&text)
            .with_context(|| format!("parsing fake broker state {}", path.display()))?;
        Ok(Self {
            state,
            clock,
            caps: Capabilities::all(),
        })
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        std::fs::write(path, serde_json::to_string_pretty(&self.state)?)
            .with_context(|| format!("writing {}", path.display()))
    }

    pub fn with_capabilities(mut self, caps: Capabilities) -> Self {
        self.caps = caps;
        self
    }

    pub fn set_mode(&mut self, mode: Mode, account_id: &str) {
        self.state.mode = mode;
        self.state.account_id = account_id.into();
    }

    /// Whether market orders fill on placement. Off, every order rests until [`Self::fill`].
    pub fn set_auto_fill(&mut self, on: bool) {
        self.state.auto_fill = on;
    }

    pub fn set_quote(&mut self, symbol: &str, bid: f64, ask: f64) {
        let quote = Quote {
            instrument: Instrument::stock(symbol),
            bid: Some(bid),
            ask: Some(ask),
            last: Some((bid + ask) / 2.0),
            time: Some(self.clock.now()),
        };
        self.state.quotes.insert(symbol.into(), quote);
    }

    /// Sets a position directly, as if it had been there all along (or traded elsewhere).
    pub fn set_position(&mut self, symbol: &str, qty: f64, avg_cost: f64) {
        self.state.positions.insert(
            symbol.into(),
            Position {
                instrument: Instrument::stock(symbol),
                qty,
                avg_cost,
            },
        );
    }

    pub fn script(&mut self, step: Script) {
        self.state.script.push_back(step);
    }

    pub fn reconnect(&mut self) {
        self.state.connected = true;
        self.state.events.push_back(BrokerEvent::Reconnected);
    }

    pub fn is_connected(&self) -> bool {
        self.state.connected
    }

    /// Queues an event as if the broker sent it; for out-of-order and duplicate delivery tests.
    pub fn push_event(&mut self, event: BrokerEvent) {
        self.state.events.push_back(event);
    }

    /// The broker's own record of an order, bypassing the connection; for assertions.
    pub fn order(&self, id: &ClientOrderId) -> Option<&Order> {
        self.state.orders.get(id)
    }

    pub fn order_count(&self) -> usize {
        self.state.orders.len()
    }

    /// The broker's position, bypassing the connection; for assertions.
    pub fn position(&self, symbol: &str) -> f64 {
        self.state.positions.get(symbol).map_or(0.0, |p| p.qty)
    }

    /// Executes `qty` of a working order at `price`, as the market would.
    pub fn fill(&mut self, id: &ClientOrderId, qty: f64, price: f64) -> Fill {
        let now = self.clock.now();
        let order = self.state.orders.get_mut(id).expect("fill: no such order");
        assert!(
            order.status.is_live(),
            "fill: order {id} is {}",
            order.status
        );
        let total = order.filled_qty + qty;
        order.avg_fill_price = Some(match order.avg_fill_price {
            Some(avg) => (avg * order.filled_qty + price * qty) / total,
            None => price,
        });
        order.filled_qty = total;
        let target = match order.spec.qty {
            Qty::Shares(q) => q,
            Qty::Notional(v) => v / price,
        };
        order.status = if total + 1e-9 >= target {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };
        order.updated_at = now;
        self.state.next_exec += 1;
        let fill = Fill {
            exec_id: format!("fx{:06}", self.state.next_exec),
            client_id: id.clone(),
            broker_order_id: order.broker_id.clone(),
            instrument: order.spec.instrument.clone(),
            side: order.spec.side,
            qty,
            price,
            commission: Some(0.0),
            time: now,
        };
        let snapshot = order.clone();
        self.book_fill(&fill);
        self.state.fills.push(fill.clone());
        self.state
            .events
            .push_back(BrokerEvent::Order { order: snapshot });
        self.state
            .events
            .push_back(BrokerEvent::Fill { fill: fill.clone() });
        fill
    }

    fn book_fill(&mut self, fill: &Fill) {
        let signed = fill.side.sign() * fill.qty;
        self.state.cash -= signed * fill.price;
        let position = self
            .state
            .positions
            .entry(fill.instrument.symbol.clone())
            .or_insert(Position {
                instrument: fill.instrument.clone(),
                qty: 0.0,
                avg_cost: 0.0,
            });
        let next = position.qty + signed;
        if position.qty == 0.0 || position.qty.signum() != next.signum() {
            position.avg_cost = fill.price;
        } else if position.qty.signum() == signed.signum() {
            position.avg_cost = (position.avg_cost * position.qty + fill.price * signed) / next;
        }
        position.qty = next;
        if position.qty.abs() < 1e-9 {
            self.state.positions.remove(&fill.instrument.symbol);
        }
    }

    /// A deterministic quote for symbols nobody set one for, so the offline CLI can price
    /// anything: somewhere in $20-$500 depending on the symbol, a cent wide.
    fn synthetic_quote(&self, instrument: &Instrument) -> Quote {
        let hash = instrument
            .symbol
            .bytes()
            .fold(17u64, |h, b| h.wrapping_mul(31).wrapping_add(b as u64));
        let mid = 20.0 + (hash % 48_000) as f64 / 100.0;
        Quote {
            instrument: instrument.clone(),
            bid: Some(mid - 0.01),
            ask: Some(mid + 0.01),
            last: Some(mid),
            time: Some(self.clock.now()),
        }
    }

    fn take_script(&mut self, pick: impl Fn(&Script) -> bool) -> Option<Script> {
        let index = self.state.script.iter().position(pick)?;
        self.state.script.remove(index)
    }

    fn check_connection(&mut self) -> Result<(), BrokerError> {
        if self.take_script(|s| *s == Script::Disconnect).is_some() {
            self.state.connected = false;
            self.state.events.push_back(BrokerEvent::Disconnected {
                reason: "scripted disconnect".into(),
            });
        }
        if self.state.connected {
            Ok(())
        } else {
            Err(BrokerError::Disconnected(
                "fake broker is disconnected".into(),
            ))
        }
    }

    fn working(&self, id: &ClientOrderId) -> Result<&Order, BrokerError> {
        let order = self
            .state
            .orders
            .get(id)
            .ok_or_else(|| BrokerError::NotFound(id.clone()))?;
        if order.status.is_terminal() {
            return Err(BrokerError::Rejected(format!(
                "order {id} is {}",
                order.status
            )));
        }
        Ok(order)
    }

    fn emit(&mut self, order: &Order) {
        self.state.events.push_back(BrokerEvent::Order {
            order: order.clone(),
        });
    }
}

impl Broker for FakeBroker {
    fn name(&self) -> &str {
        "fake"
    }

    fn capabilities(&self) -> Capabilities {
        self.caps.clone()
    }

    fn account(&mut self) -> Result<Account, BrokerError> {
        self.check_connection()?;
        let held: f64 = self
            .state
            .positions
            .values()
            .map(|p| {
                let mark = self
                    .state
                    .quotes
                    .get(&p.instrument.symbol)
                    .and_then(|q| q.last)
                    .unwrap_or(p.avg_cost);
                p.qty * mark
            })
            .fold(0.0, |a, b| a + b);
        Ok(Account {
            id: self.state.account_id.clone(),
            mode: self.state.mode,
            currency: "USD".into(),
            cash: self.state.cash,
            buying_power: self.state.cash.max(0.0),
            equity: self.state.cash + held,
        })
    }

    fn positions(&mut self) -> Result<Vec<Position>, BrokerError> {
        self.check_connection()?;
        Ok(self.state.positions.values().cloned().collect())
    }

    fn open_orders(&mut self) -> Result<Vec<Order>, BrokerError> {
        self.check_connection()?;
        Ok(self
            .state
            .orders
            .values()
            .filter(|o| o.status.is_live())
            .cloned()
            .collect())
    }

    fn find_order(&mut self, id: &ClientOrderId) -> Result<Option<Order>, BrokerError> {
        self.check_connection()?;
        Ok(self.state.orders.get(id).cloned())
    }

    fn fills(&mut self, since: Timestamp) -> Result<Vec<Fill>, BrokerError> {
        self.check_connection()?;
        Ok(self
            .state
            .fills
            .iter()
            .filter(|f| f.time >= since)
            .cloned()
            .collect())
    }

    fn quote(&mut self, instrument: &Instrument) -> Result<Quote, BrokerError> {
        self.check_connection()?;
        Ok(match self.state.quotes.get(&instrument.symbol) {
            Some(q) => q.clone(),
            None => self.synthetic_quote(instrument),
        })
    }

    fn place(&mut self, request: &OrderRequest) -> Result<Order, BrokerError> {
        self.check_connection()?;
        if self.state.orders.contains_key(&request.client_id) {
            return Err(BrokerError::Rejected(format!(
                "duplicate client order id {}",
                request.client_id
            )));
        }
        if let Some(Script::Reject(reason)) = self.take_script(|s| matches!(s, Script::Reject(_))) {
            return Err(BrokerError::Rejected(reason));
        }
        if self.take_script(|s| *s == Script::LoseRequest).is_some() {
            return Err(BrokerError::Ambiguous("request timed out".into()));
        }
        let now = self.clock.now();
        self.state.next_broker_id += 1;
        let mut order = Order::new(request, OrderStatus::Submitted, now);
        order.broker_id = Some(self.state.next_broker_id.to_string());
        self.state
            .orders
            .insert(request.client_id.clone(), order.clone());
        self.emit(&order);

        if self.state.auto_fill && request.spec.kind == OrderKind::Market {
            let quote = self.quote(&request.spec.instrument)?;
            if let Some(price) = quote.reference(request.spec.side) {
                let qty = match request.spec.qty {
                    Qty::Shares(q) => q,
                    Qty::Notional(v) => v / price,
                };
                self.fill(&request.client_id, qty, price);
            }
        }
        if self
            .take_script(|s| *s == Script::LoseReplyAfterAccept)
            .is_some()
        {
            return Err(BrokerError::Ambiguous("reply lost after send".into()));
        }
        Ok(self.state.orders[&request.client_id].clone())
    }

    fn modify(&mut self, id: &ClientOrderId, spec: &OrderSpec) -> Result<Order, BrokerError> {
        self.check_connection()?;
        self.working(id)?;
        if let Some(Script::Reject(reason)) = self.take_script(|s| matches!(s, Script::Reject(_))) {
            return Err(BrokerError::Rejected(reason));
        }
        let now = self.clock.now();
        let order = self.state.orders.get_mut(id).unwrap();
        order.spec = spec.clone();
        order.updated_at = now;
        let order = order.clone();
        self.emit(&order);
        Ok(order)
    }

    fn cancel(&mut self, id: &ClientOrderId) -> Result<(), BrokerError> {
        self.check_connection()?;
        self.working(id)?;
        let now = self.clock.now();
        let order = self.state.orders.get_mut(id).unwrap();
        order.status = OrderStatus::Cancelled;
        order.updated_at = now;
        let order = order.clone();
        self.emit(&order);
        Ok(())
    }

    fn cancel_all(&mut self) -> Result<(), BrokerError> {
        self.check_connection()?;
        let live: Vec<_> = self
            .state
            .orders
            .values()
            .filter(|o| o.status.is_live())
            .map(|o| o.client_id.clone())
            .collect();
        for id in live {
            self.cancel(&id)?;
        }
        Ok(())
    }

    fn next_event(&mut self, _timeout: Duration) -> Result<Option<BrokerEvent>, BrokerError> {
        Ok(self.state.events.pop_front())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;
    use crate::types::Side;

    fn broker() -> FakeBroker {
        let clock = ManualClock::new("2026-10-05T14:00:00Z".parse().unwrap());
        FakeBroker::new(clock, 100_000.0)
    }

    fn request(id: &str, side: Side, qty: f64, kind: OrderKind) -> OrderRequest {
        OrderRequest {
            client_id: ClientOrderId::new(id),
            spec: OrderSpec::new(Instrument::stock("AAPL"), side, Qty::Shares(qty), kind),
        }
    }

    #[test]
    fn a_market_order_fills_at_the_touch_and_moves_cash_and_position() {
        let mut b = broker();
        b.set_quote("AAPL", 99.0, 101.0);
        let order = b
            .place(&request("a", Side::Buy, 10.0, OrderKind::Market))
            .unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.avg_fill_price, Some(101.0));
        assert_eq!(b.position("AAPL"), 10.0);
        assert_eq!(b.account().unwrap().cash, 100_000.0 - 1_010.0);
    }

    #[test]
    fn partial_fills_accumulate_to_filled() {
        let mut b = broker();
        let id = ClientOrderId::new("a");
        b.place(&request(
            "a",
            Side::Buy,
            100.0,
            OrderKind::Limit { limit: 50.0 },
        ))
        .unwrap();
        b.fill(&id, 30.0, 50.0);
        assert_eq!(b.order(&id).unwrap().status, OrderStatus::PartiallyFilled);
        b.fill(&id, 70.0, 49.0);
        let order = b.order(&id).unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert!((order.avg_fill_price.unwrap() - 49.3).abs() < 1e-9);
    }

    #[test]
    fn a_lost_reply_still_leaves_the_order_at_the_broker() {
        let mut b = broker();
        b.script(Script::LoseReplyAfterAccept);
        let err = b
            .place(&request(
                "a",
                Side::Buy,
                1.0,
                OrderKind::Limit { limit: 1.0 },
            ))
            .unwrap_err();
        assert!(matches!(err, BrokerError::Ambiguous(_)));
        assert!(b.find_order(&ClientOrderId::new("a")).unwrap().is_some());

        b.script(Script::LoseRequest);
        b.place(&request(
            "b",
            Side::Buy,
            1.0,
            OrderKind::Limit { limit: 1.0 },
        ))
        .unwrap_err();
        assert!(b.find_order(&ClientOrderId::new("b")).unwrap().is_none());
    }

    #[test]
    fn a_disconnect_lasts_until_reconnected() {
        let mut b = broker();
        b.script(Script::Disconnect);
        assert!(matches!(b.positions(), Err(BrokerError::Disconnected(_))));
        assert!(matches!(b.account(), Err(BrokerError::Disconnected(_))));
        b.reconnect();
        assert!(b.account().is_ok());
    }

    #[test]
    fn state_survives_a_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fake.json");
        let clock = ManualClock::new("2026-10-05T14:00:00Z".parse().unwrap());
        let mut b = FakeBroker::new(clock.clone(), 1_000.0);
        b.set_position("MSFT", 5.0, 400.0);
        b.save(&path).unwrap();
        let mut again = FakeBroker::load_or_new(&path, clock, 0.0).unwrap();
        assert_eq!(again.positions().unwrap()[0].qty, 5.0);
        assert_eq!(again.account().unwrap().cash, 1_000.0);
    }

    #[test]
    fn going_through_zero_resets_the_cost_basis() {
        let mut b = broker();
        b.set_auto_fill(false);
        let id = ClientOrderId::new("a");
        b.place(&request("a", Side::Buy, 10.0, OrderKind::Market))
            .unwrap();
        b.fill(&id, 10.0, 100.0);
        let id2 = ClientOrderId::new("b");
        b.place(&request("b", Side::Sell, 15.0, OrderKind::Market))
            .unwrap();
        b.fill(&id2, 15.0, 110.0);
        let p = &b.positions().unwrap()[0];
        assert_eq!(p.qty, -5.0);
        assert_eq!(p.avg_cost, 110.0);
    }
}
