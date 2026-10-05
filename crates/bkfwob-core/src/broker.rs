//! The seam every broker adapter implements.

use std::time::Duration;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::refusal::{Refusal, RefusalCode};
use crate::types::{
    Account, ClientOrderId, Fill, Instrument, Order, OrderKind, OrderRequest, OrderSpec, Position,
    Qty, Quote, Session, Tif, Trail,
};

/// Why a broker call failed — classified by what it means for the order, because that decides
/// what is safe to do next.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum BrokerError {
    /// The broker refused the request. Definitive: for a `place`, no order exists.
    #[error("rejected by the broker: {0}")]
    Rejected(String),
    /// The request was sent and the answer was lost. The order may or may not exist; it must be
    /// resolved by looking it up, never by sending it again.
    #[error("outcome unknown: {0}")]
    Ambiguous(String),
    /// Not connected, so nothing was sent. Safe to retry once connected.
    #[error("not connected: {0}")]
    Disconnected(String),
    #[error("no such order at the broker: {0}")]
    NotFound(ClientOrderId),
    #[error("not supported by this broker: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Other(String),
}

/// Something the broker told us without being asked.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "kebab-case")]
pub enum BrokerEvent {
    /// An order's state changed; carries the whole order as the broker now sees it.
    Order {
        order: Order,
    },
    Fill {
        fill: Fill,
    },
    Account {
        account: Account,
    },
    Disconnected {
        reason: String,
    },
    /// The connection is back. Anything could have happened meanwhile: reconcile.
    Reconnected,
    Notice {
        message: String,
    },
}

/// What an adapter can actually do. Orders outside this are refused before they reach the broker,
/// never silently translated into something close.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    pub market: bool,
    pub limit: bool,
    pub stop: bool,
    pub stop_limit: bool,
    pub trailing_amount: bool,
    pub trailing_percent: bool,
    pub tifs: Vec<Tif>,
    /// Orders may be flagged to execute outside regular hours.
    pub extended_hours: bool,
    /// Market orders may execute outside regular hours (IB: no).
    pub market_outside_rth: bool,
    /// `usd:` quantities.
    pub notional: bool,
    pub fractional: bool,
}

impl Capabilities {
    /// Everything; for tests and the fake broker.
    pub fn all() -> Self {
        Self {
            market: true,
            limit: true,
            stop: true,
            stop_limit: true,
            trailing_amount: true,
            trailing_percent: true,
            tifs: vec![Tif::Day, Tif::Gtc, Tif::Ioc],
            extended_hours: true,
            market_outside_rth: false,
            notional: true,
            fractional: true,
        }
    }

    /// Whether this broker can take `spec` as written, and whether `spec` is well formed at all:
    /// positive finite quantities, prices on the tick grid. A price off the grid is refused rather
    /// than rounded — rounding a limit is changing the order.
    pub fn check(&self, spec: &OrderSpec) -> Result<(), Refusal> {
        let unsupported = |what: String| Err(Refusal::new(RefusalCode::Unsupported, what));
        let kind_ok = match spec.kind {
            OrderKind::Market => self.market,
            OrderKind::Limit { .. } => self.limit,
            OrderKind::Stop { .. } => self.stop,
            OrderKind::StopLimit { .. } => self.stop_limit,
            OrderKind::Trailing {
                trail: Trail::Amount(_),
            } => self.trailing_amount,
            OrderKind::Trailing {
                trail: Trail::Percent(_),
            } => self.trailing_percent,
        };
        if !kind_ok {
            return unsupported(format!("{} orders", spec.kind.label()));
        }
        if !self.tifs.contains(&spec.tif) {
            return unsupported(format!("time in force {}", spec.tif));
        }
        if spec.session == Session::Extended {
            if !self.extended_hours {
                return unsupported("extended-hours orders".into());
            }
            if spec.kind == OrderKind::Market && !self.market_outside_rth {
                return unsupported(
                    "market orders outside regular hours; use a limit (`lmt:P eth`)".into(),
                );
            }
        }
        match spec.qty {
            Qty::Notional(_) if !self.notional => {
                return unsupported("notional (`usd:`) quantities".into());
            }
            Qty::Shares(q) if !self.fractional && q.fract() != 0.0 => {
                return unsupported(format!("fractional shares ({q})"));
            }
            _ => {}
        }
        validate(spec)
    }
}

fn invalid(detail: String) -> Result<(), Refusal> {
    Err(Refusal::new(RefusalCode::Invalid, detail))
}

/// US equity tick: a cent at or above $1, a hundredth of a cent below.
pub fn tick_size(price: f64) -> f64 {
    if price >= 1.0 { 0.01 } else { 0.0001 }
}

pub fn on_tick(price: f64) -> bool {
    let steps = price / tick_size(price);
    (steps - steps.round()).abs() < 1e-6
}

/// Broker-independent sanity: what no broker should ever be sent.
pub fn validate(spec: &OrderSpec) -> Result<(), Refusal> {
    let qty = spec.qty.value();
    if !(qty.is_finite() && qty > 0.0) {
        return invalid(format!("quantity must be positive, got {qty}"));
    }
    let mut prices = Vec::new();
    match spec.kind {
        OrderKind::Market => {}
        OrderKind::Limit { limit } => prices.push(("limit", limit)),
        OrderKind::Stop { stop } => prices.push(("stop", stop)),
        OrderKind::StopLimit { stop, limit } => {
            prices.push(("stop", stop));
            prices.push(("limit", limit));
        }
        OrderKind::Trailing {
            trail: Trail::Amount(a),
        } => prices.push(("trailing amount", a)),
        OrderKind::Trailing {
            trail: Trail::Percent(p),
        } => {
            if !(p.is_finite() && p > 0.0 && p < 100.0) {
                return invalid(format!("trailing percent must be in (0, 100), got {p}"));
            }
        }
    }
    for (what, price) in prices {
        if !(price.is_finite() && price > 0.0) {
            return invalid(format!("{what} price must be positive, got {price}"));
        }
        if !on_tick(price) {
            return Err(Refusal::new(
                RefusalCode::NotOnTick,
                format!(
                    "{what} price {price} is not a multiple of the {} tick",
                    tick_size(price)
                ),
            ));
        }
    }
    Ok(())
}

/// A brokerage connection.
///
/// Blocking by design, like mdfwob's IB client: a call returns when the broker has answered (or
/// failed to). Unsolicited updates are pulled with [`Broker::next_event`].
///
/// Adapters implement this raw; callers never use it raw. Everything reaches it through
/// [`crate::desk::Desk`], which adds the journal, the guardrails and reconciliation.
pub trait Broker: Send {
    /// Short name, e.g. `ibkr`.
    fn name(&self) -> &str;
    fn capabilities(&self) -> Capabilities;
    fn account(&mut self) -> Result<Account, BrokerError>;
    fn positions(&mut self) -> Result<Vec<Position>, BrokerError>;
    fn open_orders(&mut self) -> Result<Vec<Order>, BrokerError>;
    /// One order by our id, including orders that finished today. `Ok(None)` means the broker has
    /// no record of it — which, for an order of unknown fate, means it was never placed.
    fn find_order(&mut self, id: &ClientOrderId) -> Result<Option<Order>, BrokerError>;
    fn fills(&mut self, since: Timestamp) -> Result<Vec<Fill>, BrokerError>;
    fn quote(&mut self, instrument: &Instrument) -> Result<Quote, BrokerError>;
    fn place(&mut self, request: &OrderRequest) -> Result<Order, BrokerError>;
    /// Replace a working order's terms with `spec` (the whole new spec, not a delta).
    fn modify(&mut self, id: &ClientOrderId, spec: &OrderSpec) -> Result<Order, BrokerError>;
    fn cancel(&mut self, id: &ClientOrderId) -> Result<(), BrokerError>;
    /// Cancel every working order in the account, including ones this process did not place.
    fn cancel_all(&mut self) -> Result<(), BrokerError>;
    /// The next unsolicited event, waiting up to `timeout`.
    fn next_event(&mut self, timeout: Duration) -> Result<Option<BrokerEvent>, BrokerError>;
}

impl<B: Broker + ?Sized> Broker for Box<B> {
    fn name(&self) -> &str {
        (**self).name()
    }
    fn capabilities(&self) -> Capabilities {
        (**self).capabilities()
    }
    fn account(&mut self) -> Result<Account, BrokerError> {
        (**self).account()
    }
    fn positions(&mut self) -> Result<Vec<Position>, BrokerError> {
        (**self).positions()
    }
    fn open_orders(&mut self) -> Result<Vec<Order>, BrokerError> {
        (**self).open_orders()
    }
    fn find_order(&mut self, id: &ClientOrderId) -> Result<Option<Order>, BrokerError> {
        (**self).find_order(id)
    }
    fn fills(&mut self, since: Timestamp) -> Result<Vec<Fill>, BrokerError> {
        (**self).fills(since)
    }
    fn quote(&mut self, instrument: &Instrument) -> Result<Quote, BrokerError> {
        (**self).quote(instrument)
    }
    fn place(&mut self, request: &OrderRequest) -> Result<Order, BrokerError> {
        (**self).place(request)
    }
    fn modify(&mut self, id: &ClientOrderId, spec: &OrderSpec) -> Result<Order, BrokerError> {
        (**self).modify(id, spec)
    }
    fn cancel(&mut self, id: &ClientOrderId) -> Result<(), BrokerError> {
        (**self).cancel(id)
    }
    fn cancel_all(&mut self) -> Result<(), BrokerError> {
        (**self).cancel_all()
    }
    fn next_event(&mut self, timeout: Duration) -> Result<Option<BrokerEvent>, BrokerError> {
        (**self).next_event(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Instrument, Side};

    fn spec(qty: Qty, kind: OrderKind) -> OrderSpec {
        OrderSpec::new(Instrument::stock("AAPL"), Side::Buy, qty, kind)
    }

    fn code(r: Result<(), Refusal>) -> Option<RefusalCode> {
        r.err().map(|r| r.code)
    }

    #[test]
    fn ticks() {
        assert!(on_tick(185.5));
        assert!(on_tick(185.51));
        assert!(!on_tick(185.505));
        assert!(on_tick(0.1234));
        assert!(!on_tick(0.12345));
        assert!(on_tick(1.0));
    }

    #[test]
    fn malformed_orders_are_refused_whatever_the_broker() {
        let caps = Capabilities::all();
        assert_eq!(
            code(caps.check(&spec(Qty::Shares(0.0), OrderKind::Market))),
            Some(RefusalCode::Invalid)
        );
        assert_eq!(
            code(caps.check(&spec(Qty::Shares(1.0), OrderKind::Limit { limit: 185.505 }))),
            Some(RefusalCode::NotOnTick)
        );
        assert_eq!(
            code(caps.check(&spec(
                Qty::Shares(1.0),
                OrderKind::Trailing {
                    trail: Trail::Amount(f64::NAN)
                }
            ))),
            Some(RefusalCode::Invalid)
        );
        assert_eq!(
            code(caps.check(&spec(Qty::Shares(1.0), OrderKind::Limit { limit: 185.5 }))),
            None
        );
    }

    #[test]
    fn capabilities_refuse_rather_than_translate() {
        let mut caps = Capabilities::all();
        caps.notional = false;
        caps.fractional = false;
        caps.trailing_percent = false;
        caps.tifs = vec![Tif::Day];
        let refused = |s: OrderSpec| code(caps.check(&s)) == Some(RefusalCode::Unsupported);
        assert!(refused(spec(Qty::Notional(100.0), OrderKind::Market)));
        assert!(refused(spec(Qty::Shares(0.5), OrderKind::Market)));
        assert!(refused(spec(
            Qty::Shares(1.0),
            OrderKind::Trailing {
                trail: Trail::Percent(1.0)
            }
        )));
        let mut gtc = spec(Qty::Shares(1.0), OrderKind::Market);
        gtc.tif = Tif::Gtc;
        assert!(refused(gtc));
        let mut eth_market = spec(Qty::Shares(1.0), OrderKind::Market);
        eth_market.session = Session::Extended;
        assert!(refused(eth_market));
        let mut eth_limit = spec(Qty::Shares(1.0), OrderKind::Limit { limit: 10.0 });
        eth_limit.session = Session::Extended;
        assert!(!refused(eth_limit));
    }
}
