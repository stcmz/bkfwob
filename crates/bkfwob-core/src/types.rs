//! Broker-neutral value types: instruments, orders, fills, positions, accounts, quotes.
//!
//! Every adapter translates to and from these, so they describe what an order *means*, not how any
//! one broker spells it. Where brokers disagree on a default — most visibly whether an order may
//! execute outside regular trading hours — the field is explicit and required rather than
//! defaulted, so the meaning never depends on which broker is on the other end.

use std::fmt;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::status::OrderStatus;

/// What kind of security an instrument is. Equities only for now; the enum exists so adding
/// options later is a new variant rather than a reinterpretation of every existing instrument.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetKind {
    #[default]
    Stock,
}

/// A tradable security.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Instrument {
    pub symbol: String,
    #[serde(default)]
    pub kind: AssetKind,
    /// Routing destination; `SMART` lets the broker route.
    #[serde(default = "default_exchange")]
    pub exchange: String,
    /// Listing exchange, which IB needs to disambiguate symbols listed in more than one place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_exchange: Option<String>,
    #[serde(default = "default_currency")]
    pub currency: String,
}

fn default_exchange() -> String {
    "SMART".into()
}

fn default_currency() -> String {
    "USD".into()
}

impl Instrument {
    /// A US stock routed by the broker.
    pub fn stock(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            kind: AssetKind::Stock,
            exchange: default_exchange(),
            primary_exchange: None,
            currency: default_currency(),
        }
    }
}

impl fmt::Display for Instrument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.symbol)
    }
}

/// Direction of an order or fill. Buy and sell, not long and short: a sell can close a long or
/// open a short, and which one it is depends on the position it meets — the guardrails decide
/// whether the second is allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    /// +1 for a buy, -1 for a sell.
    pub fn sign(self) -> f64 {
        match self {
            Side::Buy => 1.0,
            Side::Sell => -1.0,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        })
    }
}

/// How much to trade.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "unit", content = "value", rename_all = "lowercase")]
pub enum Qty {
    /// This many shares.
    Shares(f64),
    /// This much cash value, sized by the broker at execution. Only for adapters that declare
    /// [`crate::broker::Capabilities::notional`].
    Notional(f64),
}

impl Qty {
    pub fn value(self) -> f64 {
        match self {
            Qty::Shares(v) | Qty::Notional(v) => v,
        }
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Qty::Shares(q) => write!(f, "{q}"),
            Qty::Notional(v) => write!(f, "usd:{v}"),
        }
    }
}

/// How a trailing stop's distance is measured.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "by", content = "value", rename_all = "lowercase")]
pub enum Trail {
    /// Absolute quote units.
    Amount(f64),
    /// Percent of the best price (2.0 = 2%).
    Percent(f64),
}

/// How an order is triggered and priced. The same set qtfwob simulates, so a backtested strategy
/// has a live equivalent for every order it can write.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum OrderKind {
    Market,
    Limit { limit: f64 },
    Stop { stop: f64 },
    StopLimit { stop: f64, limit: f64 },
    Trailing { trail: Trail },
}

impl OrderKind {
    /// The limit price, for the kinds that carry one.
    pub fn limit_price(self) -> Option<f64> {
        match self {
            OrderKind::Limit { limit } | OrderKind::StopLimit { limit, .. } => Some(limit),
            _ => None,
        }
    }

    /// Short name for display and error messages.
    pub fn label(self) -> &'static str {
        match self {
            OrderKind::Market => "mkt",
            OrderKind::Limit { .. } => "lmt",
            OrderKind::Stop { .. } => "stp",
            OrderKind::StopLimit { .. } => "stp-lmt",
            OrderKind::Trailing { .. } => "trail",
        }
    }
}

impl fmt::Display for OrderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            OrderKind::Market => f.write_str("mkt"),
            OrderKind::Limit { limit } => write!(f, "lmt:{limit}"),
            OrderKind::Stop { stop } => write!(f, "stp:{stop}"),
            OrderKind::StopLimit { stop, limit } => write!(f, "stp-lmt:{stop},{limit}"),
            OrderKind::Trailing {
                trail: Trail::Amount(a),
            } => write!(f, "trail:{a}"),
            OrderKind::Trailing {
                trail: Trail::Percent(p),
            } => write!(f, "trail:{p}%"),
        }
    }
}

/// Time in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tif {
    /// Expires at the end of the trading day.
    #[default]
    Day,
    /// Good till cancelled.
    Gtc,
    /// Immediate or cancel: fill what is possible now, cancel the rest.
    Ioc,
}

impl fmt::Display for Tif {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Tif::Day => "day",
            Tif::Gtc => "gtc",
            Tif::Ioc => "ioc",
        })
    }
}

/// Whether an order may execute outside regular trading hours.
///
/// Regular hours is the default here even though mdfwob and qtfwob default their *data* to
/// extended hours: an order outside RTH trades in a thin book, and that has to be asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Session {
    #[default]
    Rth,
    Extended,
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Session::Rth => "rth",
            Session::Extended => "eth",
        })
    }
}

/// Our identity for an order, assigned before it is sent.
///
/// It is what makes a submit whose outcome is unknown recoverable: the broker stores it with the
/// order (IB's `orderRef`, Alpaca's `client_order_id`), so after a timeout we can ask "does an
/// order with this id exist?" instead of guessing — and never re-send blindly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClientOrderId(pub String);

impl ClientOrderId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ClientOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What to trade, before it has an identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderSpec {
    pub instrument: Instrument,
    pub side: Side,
    pub qty: Qty,
    pub kind: OrderKind,
    pub tif: Tif,
    pub session: Session,
    /// A caller's label, e.g. `stop` for a protective stop. Informational for the broker; it is
    /// how a strategy finds "its stop" again across restarts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
}

impl OrderSpec {
    pub fn new(instrument: Instrument, side: Side, qty: Qty, kind: OrderKind) -> Self {
        Self {
            instrument,
            side,
            qty,
            kind,
            tif: Tif::Day,
            session: Session::Rth,
            tag: None,
        }
    }

    /// One-line summary, e.g. `buy 10 AAPL lmt:185.5 gtc rth`.
    pub fn summary(&self) -> String {
        let mut s = format!(
            "{} {} {} {} {} {}",
            self.side, self.qty, self.instrument, self.kind, self.tif, self.session
        );
        if let Some(tag) = &self.tag {
            s.push_str(&format!(" tag:{tag}"));
        }
        s
    }
}

/// An order with its identity: what is journaled and sent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderRequest {
    pub client_id: ClientOrderId,
    pub spec: OrderSpec,
}

/// A change to a working order. Unset fields are left as they are.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct OrderChange {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qty: Option<Qty>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<OrderKind>,
}

impl OrderChange {
    pub fn is_empty(&self) -> bool {
        self.qty.is_none() && self.kind.is_none()
    }

    /// The spec this change produces. A change may move prices and quantity but not the kind of
    /// order: turning a limit into a market order is a different order, placed separately.
    pub fn apply(&self, spec: &OrderSpec) -> Result<OrderSpec, String> {
        let mut out = spec.clone();
        if let Some(qty) = self.qty {
            if std::mem::discriminant(&qty) != std::mem::discriminant(&spec.qty) {
                return Err("a change cannot switch between shares and notional".into());
            }
            out.qty = qty;
        }
        if let Some(kind) = self.kind {
            if std::mem::discriminant(&kind) != std::mem::discriminant(&spec.kind) {
                return Err(format!(
                    "a change cannot turn a {} order into a {} order; cancel and place a new one",
                    spec.kind.label(),
                    kind.label()
                ));
            }
            out.kind = kind;
        }
        Ok(out)
    }
}

/// An order as the broker (or our journal) currently sees it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub client_id: ClientOrderId,
    /// The broker's own id, once it has assigned one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_id: Option<String>,
    pub spec: OrderSpec,
    pub status: OrderStatus,
    pub filled_qty: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avg_fill_price: Option<f64>,
    /// Why the order was rejected or cancelled, when the broker says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub updated_at: Timestamp,
}

impl Order {
    pub fn new(request: &OrderRequest, status: OrderStatus, at: Timestamp) -> Self {
        Self {
            client_id: request.client_id.clone(),
            broker_id: None,
            spec: request.spec.clone(),
            status,
            filled_qty: 0.0,
            avg_fill_price: None,
            reason: None,
            updated_at: at,
        }
    }

    /// Shares still to fill, for share-sized orders.
    pub fn remaining_shares(&self) -> Option<f64> {
        match self.spec.qty {
            Qty::Shares(q) => Some((q - self.filled_qty).max(0.0)),
            Qty::Notional(_) => None,
        }
    }
}

/// One execution. `exec_id` is the broker's id for it and the key for de-duplication: brokers
/// re-deliver executions after a reconnect, and counting one twice doubles a position on paper.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub exec_id: String,
    pub client_id: ClientOrderId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_order_id: Option<String>,
    pub instrument: Instrument,
    pub side: Side,
    pub qty: f64,
    pub price: f64,
    /// Arrives separately from the execution at some brokers, hence optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commission: Option<f64>,
    pub time: Timestamp,
}

/// A signed holding. Positive long, negative short.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Position {
    pub instrument: Instrument,
    pub qty: f64,
    pub avg_cost: f64,
}

/// Paper or live. Determined by the broker from the account itself, never by a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Paper,
    Live,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Mode::Paper => "paper",
            Mode::Live => "live",
        })
    }
}

/// Account balances.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub mode: Mode,
    pub currency: String,
    pub cash: f64,
    pub buying_power: f64,
    pub equity: f64,
}

/// A quote snapshot. Any side may be missing outside market hours or for illiquid names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quote {
    pub instrument: Instrument,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bid: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ask: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<Timestamp>,
}

impl Quote {
    /// The price an order on `side` would most plausibly trade at: the ask for a buy, the bid for
    /// a sell, falling back to the last trade.
    pub fn reference(&self, side: Side) -> Option<f64> {
        let touch = match side {
            Side::Buy => self.ask,
            Side::Sell => self.bid,
        };
        touch
            .filter(|p| *p > 0.0)
            .or(self.last.filter(|p| *p > 0.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(kind: OrderKind) -> OrderSpec {
        OrderSpec::new(
            Instrument::stock("AAPL"),
            Side::Buy,
            Qty::Shares(10.0),
            kind,
        )
    }

    #[test]
    fn a_change_moves_prices_but_never_changes_the_kind_of_order() {
        let base = spec(OrderKind::Limit { limit: 100.0 });
        let moved = OrderChange {
            kind: Some(OrderKind::Limit { limit: 101.0 }),
            qty: Some(Qty::Shares(5.0)),
        }
        .apply(&base)
        .unwrap();
        assert_eq!(moved.kind, OrderKind::Limit { limit: 101.0 });
        assert_eq!(moved.qty, Qty::Shares(5.0));

        let to_market = OrderChange {
            kind: Some(OrderKind::Market),
            qty: None,
        };
        assert!(to_market.apply(&base).is_err());
        let to_notional = OrderChange {
            qty: Some(Qty::Notional(500.0)),
            kind: None,
        };
        assert!(to_notional.apply(&base).is_err());
    }

    #[test]
    fn the_reference_price_is_the_touch_on_the_trading_side() {
        let quote = Quote {
            instrument: Instrument::stock("AAPL"),
            bid: Some(99.0),
            ask: Some(101.0),
            last: Some(100.0),
            time: None,
        };
        assert_eq!(quote.reference(Side::Buy), Some(101.0));
        assert_eq!(quote.reference(Side::Sell), Some(99.0));
        let closed = Quote {
            bid: None,
            ask: Some(0.0),
            ..quote
        };
        assert_eq!(
            closed.reference(Side::Buy),
            Some(100.0),
            "falls back to last"
        );
    }

    #[test]
    fn order_specs_round_trip_through_json() {
        let mut s = spec(OrderKind::Trailing {
            trail: Trail::Percent(2.0),
        });
        s.tag = Some("stop".into());
        let text = serde_json::to_string(&s).unwrap();
        assert_eq!(serde_json::from_str::<OrderSpec>(&text).unwrap(), s);
        assert_eq!(s.summary(), "buy 10 AAPL trail:2% day rth tag:stop");
    }
}
