//! The guardrails: what an order must pass before it is sent.
//!
//! Pure functions over a snapshot of the account, so each rule is testable on its own. The
//! [`crate::desk::Desk`] gathers the snapshot from the broker and the journal and calls
//! [`assess`] on every place and modify.
//!
//! Limits are deliberately conservative by default: the first live order should be small because
//! nobody raised the limits, not because someone remembered to type a small number.

use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::refusal::{Refusal, RefusalCode};
use crate::types::{Mode, Order, OrderKind, OrderSpec, Qty, Quote, Side, Trail};

/// Name of the environment variable that must be `1` for live trading.
pub const LIVE_ENV: &str = "BKFWOB_LIVE";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    /// Largest single order, in account currency.
    pub max_order_notional: f64,
    /// Total notional sent per exchange date, at each order's largest size.
    pub max_daily_notional: f64,
    /// Largest position value (long or, if allowed, short) an order may build toward, counting
    /// working orders on the same side.
    pub max_position_value: f64,
    /// Working orders in the account, ours or not.
    pub max_open_orders: usize,
    /// Places plus modifies in any 60 seconds.
    pub orders_per_minute: usize,
    /// How far through the market a limit price may be, in percent. 0 disables.
    pub collar_pct: f64,
    /// Market orders need a quote at most this old.
    pub quote_max_age_secs: i64,
    /// Whether a sell may exceed the position (open a short).
    pub allow_short: bool,
    /// If non-empty, only these symbols may be traded.
    pub symbols: Vec<String>,
    /// These symbols may never be traded.
    pub deny: Vec<String>,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_order_notional: 5_000.0,
            max_daily_notional: 25_000.0,
            max_position_value: 10_000.0,
            max_open_orders: 20,
            orders_per_minute: 10,
            collar_pct: 5.0,
            quote_max_age_secs: 60,
            allow_short: false,
            symbols: Vec::new(),
            deny: Vec::new(),
        }
    }
}

/// Live trading needs two independent yeses: the config says `live` *and* the environment says
/// `BKFWOB_LIVE=1`. Either alone is refused. The config is what someone wrote down; the
/// environment is what someone is doing right now — a copied config or a stray shell variable
/// cannot trade real money on its own.
pub fn live_gate(configured: Mode, env: Option<&str>) -> Result<(), String> {
    let env_live = env == Some("1");
    match (configured, env_live) {
        (Mode::Paper, false) => Ok(()),
        (Mode::Live, true) => Ok(()),
        (Mode::Live, false) => Err(format!(
            "the config says live, but {LIVE_ENV}=1 is not set in the environment"
        )),
        (Mode::Paper, true) => Err(format!(
            "{LIVE_ENV}=1 is set but the config says paper; refusing the ambiguity"
        )),
    }
}

/// The account as it stands, for one order's assessment.
#[derive(Debug, Clone)]
pub struct Snapshot<'a> {
    pub quote: Option<&'a Quote>,
    /// Current position in the order's symbol.
    pub position: f64,
    /// Every working order in the account (ours, others', and ours of unknown fate), except the
    /// one being modified.
    pub open_orders: &'a [Order],
    /// Notional already charged today, excluding the order being modified.
    pub charged_today: f64,
    /// For a modify: what the order was already charged; the charge never shrinks.
    pub charge_floor: f64,
    pub recent_submissions: usize,
    pub now: Timestamp,
}

/// What passing the guardrails established about an order.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Assessment {
    /// The price used to value it.
    pub reference_price: f64,
    pub est_notional: f64,
    pub est_shares: f64,
}

fn refuse<T>(code: RefusalCode, detail: String) -> Result<T, Refusal> {
    Err(Refusal::new(code, detail))
}

/// Shares still working in `side` for `symbol`.
fn working_shares(orders: &[Order], symbol: &str, side: Side, price: f64) -> f64 {
    orders
        .iter()
        .filter(|o| o.status.is_live())
        .filter(|o| o.spec.instrument.symbol == symbol && o.spec.side == side)
        .map(|o| match o.spec.qty {
            Qty::Shares(q) => (q - o.filled_qty).max(0.0),
            Qty::Notional(v) => (v / price - o.filled_qty).max(0.0),
        })
        .sum()
}

pub fn assess(limits: &Limits, spec: &OrderSpec, s: &Snapshot) -> Result<Assessment, Refusal> {
    let symbol = spec.instrument.symbol.as_str();
    if limits.deny.iter().any(|d| d == symbol)
        || (!limits.symbols.is_empty() && !limits.symbols.iter().any(|a| a == symbol))
    {
        return refuse(
            RefusalCode::SymbolNotAllowed,
            format!("{symbol} is not on the allowed list"),
        );
    }

    let quote_ref = s.quote.and_then(|q| q.reference(spec.side));
    let need_quote = |why: &str| {
        quote_ref.ok_or_else(|| {
            Refusal::new(
                RefusalCode::QuoteUnavailable,
                format!("no quote for {symbol}, needed {why}"),
            )
        })
    };
    let reference = match spec.kind {
        OrderKind::Market => {
            let price = need_quote("to value a market order")?;
            let age = s
                .quote
                .and_then(|q| q.time)
                .map(|t| s.now.as_second() - t.as_second());
            match age {
                Some(age) if age <= limits.quote_max_age_secs => {}
                Some(age) => {
                    return refuse(
                        RefusalCode::QuoteStale,
                        format!(
                            "the {symbol} quote is {age}s old (limit {}s); a market order \
                             now would trade blind — the market may be closed",
                            limits.quote_max_age_secs
                        ),
                    );
                }
                None => {
                    return refuse(
                        RefusalCode::QuoteStale,
                        format!("the {symbol} quote has no time; cannot tell if it is current"),
                    );
                }
            }
            price
        }
        OrderKind::Limit { limit } | OrderKind::StopLimit { limit, .. } => limit,
        OrderKind::Stop { stop } => stop,
        OrderKind::Trailing { .. } => need_quote("to value a trailing stop")?,
    };

    if limits.collar_pct > 0.0
        && let Some(limit) = spec.kind.limit_price()
    {
        let market = need_quote("for the limit-price collar")?;
        let band = limits.collar_pct / 100.0;
        let through = match spec.side {
            Side::Buy => limit > market * (1.0 + band),
            Side::Sell => limit < market * (1.0 - band),
        };
        if through {
            return refuse(
                RefusalCode::Collar,
                format!(
                    "{} limit {limit} is more than {}% through the market ({market})",
                    spec.side, limits.collar_pct
                ),
            );
        }
    }
    if let OrderKind::Trailing {
        trail: Trail::Amount(a),
    } = spec.kind
        && a >= reference
    {
        return refuse(
            RefusalCode::Invalid,
            format!("trailing amount {a} is not below the price {reference}"),
        );
    }

    let (est_shares, est_notional) = match spec.qty {
        Qty::Shares(q) => (q, q * reference),
        Qty::Notional(v) => (v / reference, v),
    };

    if est_notional > limits.max_order_notional {
        return refuse(
            RefusalCode::OrderNotional,
            format!(
                "order is worth about {est_notional:.2}, over the {:.2} per-order limit",
                limits.max_order_notional
            ),
        );
    }
    let charge = est_notional.max(s.charge_floor);
    if s.charged_today + charge > limits.max_daily_notional {
        return refuse(
            RefusalCode::DailyNotional,
            format!(
                "{:.2} already sent today; {charge:.2} more exceeds the {:.2} daily limit",
                s.charged_today, limits.max_daily_notional
            ),
        );
    }
    if s.recent_submissions >= limits.orders_per_minute {
        return refuse(
            RefusalCode::RateLimit,
            format!(
                "{} orders in the last minute (limit {})",
                s.recent_submissions, limits.orders_per_minute
            ),
        );
    }
    let open = s.open_orders.iter().filter(|o| o.status.is_live()).count();
    if open >= limits.max_open_orders {
        return refuse(
            RefusalCode::OpenOrders,
            format!(
                "{open} orders already working (limit {})",
                limits.max_open_orders
            ),
        );
    }

    // Exposure: where the position ends up if this order and every other working order on the
    // same side fill. Counting the working ones is what stops a resting stop-loss and a fresh
    // exit from *both* executing — the live version of the orphaned-stop short.
    let same_side = working_shares(s.open_orders, symbol, spec.side, reference);
    match spec.side {
        Side::Sell => {
            let committed = same_side + est_shares;
            let available = s.position.max(0.0);
            if !limits.allow_short && committed > available + 1e-9 {
                return refuse(
                    RefusalCode::Oversell,
                    format!(
                        "selling {est_shares} with {same_side} already working against a \
                         position of {}; that would open a short. Cancel or shrink the working \
                         sells first",
                        s.position
                    ),
                );
            }
            let short = (committed - available).max(0.0);
            if short * reference > limits.max_position_value {
                return refuse(
                    RefusalCode::PositionValue,
                    format!(
                        "a short of {short} shares (~{:.2}) exceeds the {:.2} position limit",
                        short * reference,
                        limits.max_position_value
                    ),
                );
            }
        }
        Side::Buy => {
            let end = s.position + same_side + est_shares;
            let value = end.max(0.0) * reference;
            if value > limits.max_position_value && end > s.position.max(0.0) {
                return refuse(
                    RefusalCode::PositionValue,
                    format!(
                        "{symbol} would reach ~{value:.2} with working buys, over the {:.2} \
                         position limit",
                        limits.max_position_value
                    ),
                );
            }
        }
    }

    Ok(Assessment {
        reference_price: reference,
        est_notional,
        est_shares,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::OrderStatus;
    use crate::types::{ClientOrderId, Instrument, OrderRequest};

    fn now() -> Timestamp {
        "2026-10-05T14:00:00Z".parse().unwrap()
    }

    fn quote(bid: f64, ask: f64, age_secs: i64) -> Quote {
        Quote {
            instrument: Instrument::stock("AAPL"),
            bid: Some(bid),
            ask: Some(ask),
            last: Some((bid + ask) / 2.0),
            time: Some(Timestamp::from_second(now().as_second() - age_secs).unwrap()),
        }
    }

    fn spec(side: Side, qty: f64, kind: OrderKind) -> OrderSpec {
        OrderSpec::new(Instrument::stock("AAPL"), side, Qty::Shares(qty), kind)
    }

    fn working(side: Side, qty: f64, kind: OrderKind) -> Order {
        let request = OrderRequest {
            client_id: ClientOrderId::new("w"),
            spec: spec(side, qty, kind),
        };
        Order::new(&request, OrderStatus::Submitted, now())
    }

    struct Case {
        limits: Limits,
        quote: Option<Quote>,
        position: f64,
        open: Vec<Order>,
        charged: f64,
        recent: usize,
    }

    impl Case {
        fn new() -> Self {
            Self {
                limits: Limits::default(),
                quote: Some(quote(99.99, 100.01, 1)),
                position: 0.0,
                open: Vec::new(),
                charged: 0.0,
                recent: 0,
            }
        }
        fn run(&self, spec: &OrderSpec) -> Result<Assessment, RefusalCode> {
            let snapshot = Snapshot {
                quote: self.quote.as_ref(),
                position: self.position,
                open_orders: &self.open,
                charged_today: self.charged,
                charge_floor: 0.0,
                recent_submissions: self.recent,
                now: now(),
            };
            assess(&self.limits, spec, &snapshot).map_err(|r| r.code)
        }
    }

    #[test]
    fn a_small_order_passes_and_is_valued_at_the_touch() {
        let a = Case::new()
            .run(&spec(Side::Buy, 10.0, OrderKind::Market))
            .unwrap();
        assert_eq!(a.reference_price, 100.01);
        assert!((a.est_notional - 1000.1).abs() < 1e-9);
    }

    #[test]
    fn size_limits() {
        let case = Case::new();
        assert_eq!(
            case.run(&spec(Side::Buy, 60.0, OrderKind::Market)),
            Err(RefusalCode::OrderNotional)
        );
        let mut tired = Case::new();
        tired.charged = 24_500.0;
        assert_eq!(
            tired.run(&spec(Side::Buy, 10.0, OrderKind::Market)),
            Err(RefusalCode::DailyNotional)
        );
        let mut big = Case::new();
        big.position = 95.0;
        assert_eq!(
            big.run(&spec(Side::Buy, 10.0, OrderKind::Market)),
            Err(RefusalCode::PositionValue)
        );
    }

    #[test]
    fn working_buys_count_toward_the_position_limit() {
        let mut case = Case::new();
        case.open = vec![working(Side::Buy, 49.0, OrderKind::Limit { limit: 100.0 })];
        case.position = 49.0;
        assert_eq!(
            case.run(&spec(Side::Buy, 10.0, OrderKind::Limit { limit: 100.0 })),
            Err(RefusalCode::PositionValue)
        );
    }

    #[test]
    fn rate_and_open_order_limits() {
        let mut busy = Case::new();
        busy.recent = 10;
        assert_eq!(
            busy.run(&spec(Side::Buy, 1.0, OrderKind::Market)),
            Err(RefusalCode::RateLimit)
        );
        let mut crowded = Case::new();
        crowded.open = (0..20)
            .map(|_| working(Side::Buy, 0.0001, OrderKind::Limit { limit: 1.0 }))
            .collect();
        assert_eq!(
            crowded.run(&spec(Side::Buy, 1.0, OrderKind::Market)),
            Err(RefusalCode::OpenOrders)
        );
    }

    #[test]
    fn symbol_lists() {
        let mut case = Case::new();
        case.limits.symbols = vec!["MSFT".into()];
        assert_eq!(
            case.run(&spec(Side::Buy, 1.0, OrderKind::Market)),
            Err(RefusalCode::SymbolNotAllowed)
        );
        let mut denied = Case::new();
        denied.limits.deny = vec!["AAPL".into()];
        assert_eq!(
            denied.run(&spec(Side::Buy, 1.0, OrderKind::Market)),
            Err(RefusalCode::SymbolNotAllowed)
        );
    }

    #[test]
    fn market_orders_need_a_fresh_quote_but_limits_do_not() {
        let mut stale = Case::new();
        stale.quote = Some(quote(99.99, 100.01, 3_600));
        assert_eq!(
            stale.run(&spec(Side::Buy, 1.0, OrderKind::Market)),
            Err(RefusalCode::QuoteStale)
        );
        assert!(
            stale
                .run(&spec(Side::Buy, 1.0, OrderKind::Limit { limit: 100.0 }))
                .is_ok(),
            "a GTC limit placed over the weekend is fine"
        );
        let mut none = Case::new();
        none.quote = None;
        assert_eq!(
            none.run(&spec(Side::Buy, 1.0, OrderKind::Market)),
            Err(RefusalCode::QuoteUnavailable)
        );
        none.limits.collar_pct = 0.0;
        assert!(
            none.run(&spec(Side::Buy, 1.0, OrderKind::Stop { stop: 100.0 }))
                .is_ok()
        );
    }

    #[test]
    fn the_collar_catches_limits_through_the_market_only() {
        let case = Case::new();
        // A buy limit far *above* the market is the fat finger; far below is just patient.
        assert_eq!(
            case.run(&spec(Side::Buy, 1.0, OrderKind::Limit { limit: 110.0 })),
            Err(RefusalCode::Collar)
        );
        assert!(
            case.run(&spec(Side::Buy, 1.0, OrderKind::Limit { limit: 50.0 }))
                .is_ok()
        );
        let mut holder = Case::new();
        holder.position = 10.0;
        assert_eq!(
            holder.run(&spec(Side::Sell, 1.0, OrderKind::Limit { limit: 90.0 })),
            Err(RefusalCode::Collar)
        );
        assert!(
            holder
                .run(&spec(Side::Sell, 1.0, OrderKind::Limit { limit: 150.0 }))
                .is_ok()
        );
    }

    #[test]
    fn a_sell_cannot_open_a_short_by_default() {
        let mut case = Case::new();
        case.position = 10.0;
        assert!(case.run(&spec(Side::Sell, 10.0, OrderKind::Market)).is_ok());
        assert_eq!(
            case.run(&spec(Side::Sell, 11.0, OrderKind::Market)),
            Err(RefusalCode::Oversell)
        );
        case.limits.allow_short = true;
        assert!(case.run(&spec(Side::Sell, 11.0, OrderKind::Market)).is_ok());
    }

    /// The orphaned-stop bug from qtfwob's cis strategy, live: a protective stop is resting for
    /// the whole position, and an exit is sent for the whole position too. If both fill, the
    /// account ends up short. Refused until the stop is cancelled.
    #[test]
    fn an_exit_cannot_stack_on_a_resting_stop() {
        let mut case = Case::new();
        case.position = 10.0;
        case.open = vec![working(Side::Sell, 10.0, OrderKind::Stop { stop: 90.0 })];
        assert_eq!(
            case.run(&spec(Side::Sell, 10.0, OrderKind::Market)),
            Err(RefusalCode::Oversell)
        );
        case.open[0].filled_qty = 10.0;
        case.open[0].status = OrderStatus::Filled;
        case.position = 0.0;
        assert_eq!(
            case.run(&spec(Side::Sell, 1.0, OrderKind::Market)),
            Err(RefusalCode::Oversell)
        );
    }

    #[test]
    fn the_live_gate_needs_both_keys() {
        assert!(live_gate(Mode::Paper, None).is_ok());
        assert!(live_gate(Mode::Live, Some("1")).is_ok());
        assert!(live_gate(Mode::Live, None).is_err());
        assert!(live_gate(Mode::Live, Some("true")).is_err());
        assert!(live_gate(Mode::Paper, Some("1")).is_err());
    }
}
