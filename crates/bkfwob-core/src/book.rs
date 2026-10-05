//! Our local view of our own orders, rebuilt from the journal.
//!
//! The broker is the source of truth; this is the mirror that lets us notice when we disagree with
//! it, and remember what we sent when the process restarts. Two rules govern updates:
//!
//! - **The broker wins** a disagreement about where a working order is — an "impossible"
//!   transition is applied and journaled as an anomaly.
//! - **Except that finished is finished.** A non-terminal status arriving for an order we already
//!   saw fill or cancel is a late, out-of-order message, not the order coming back to life. It is
//!   ignored (and noted). Likewise filled quantity only grows: a stale snapshot cannot un-fill.

use std::collections::{BTreeMap, HashSet};

use jiff::{SignedDuration, Timestamp};

use crate::clock::exchange_date;
use crate::journal::{Entry, Record};
use crate::status::OrderStatus;
use crate::types::{ClientOrderId, Fill, Order, Position};

/// One of our orders, with what we know about how it was sent.
#[derive(Debug, Clone, PartialEq)]
pub struct Tracked {
    pub order: Order,
    /// When the intent was journaled.
    pub sent_at: Timestamp,
    /// The largest notional this order has been estimated at, across modifies: what it is charged
    /// against the daily budget.
    pub est_notional: f64,
}

#[derive(Debug, Clone, Default)]
pub struct OrderBook {
    orders: BTreeMap<ClientOrderId, Tracked>,
    fills: Vec<Fill>,
    exec_ids: HashSet<String>,
    /// Times of every send and modify, for the rate limit.
    submissions: Vec<Timestamp>,
    /// The last reconciled positions and the executions they include.
    baseline: Option<(Vec<Position>, HashSet<String>)>,
}

impl OrderBook {
    pub fn from_records(records: &[Record]) -> (Self, Vec<String>) {
        let mut book = Self::default();
        let mut anomalies = Vec::new();
        for record in records {
            anomalies.extend(book.apply(record));
        }
        (book, anomalies)
    }

    /// Folds one journal record in. Returns anomalies found while doing so.
    pub fn apply(&mut self, record: &Record) -> Option<String> {
        match &record.entry {
            Entry::Intent {
                request,
                est_notional,
            } => {
                self.submissions.push(record.time);
                self.orders.insert(
                    request.client_id.clone(),
                    Tracked {
                        order: Order::new(request, OrderStatus::PendingSubmit, record.time),
                        sent_at: record.time,
                        est_notional: *est_notional,
                    },
                );
                None
            }
            Entry::ModifyIntent {
                client_id,
                est_notional,
                ..
            } => {
                self.submissions.push(record.time);
                if let Some(t) = self.orders.get_mut(client_id) {
                    t.est_notional = t.est_notional.max(*est_notional);
                }
                None
            }
            Entry::Order { order } => self.update(order.clone(), record.time),
            Entry::Unknown { client_id, .. } => {
                let tracked = self.orders.get_mut(client_id)?;
                if tracked.order.status == OrderStatus::PendingSubmit {
                    tracked.order.status = OrderStatus::Unknown;
                }
                None
            }
            Entry::Fill { fill } => {
                self.add_fill(fill.clone());
                None
            }
            Entry::Positions {
                positions,
                exec_ids,
            } => {
                self.baseline = Some((positions.clone(), exec_ids.iter().cloned().collect()));
                None
            }
            _ => None,
        }
    }

    /// Applies a broker snapshot of an order. Returns an anomaly description, if any.
    pub fn update(&mut self, incoming: Order, at: Timestamp) -> Option<String> {
        let Some(tracked) = self.orders.get_mut(&incoming.client_id) else {
            // An order we never sent (or whose intent predates the journal): track it as reported.
            self.orders.insert(
                incoming.client_id.clone(),
                Tracked {
                    order: incoming,
                    sent_at: at,
                    est_notional: 0.0,
                },
            );
            return None;
        };
        let current = &mut tracked.order;
        let from = current.status;
        let to = incoming.status;
        let mut anomaly = None;
        if !from.can_become(to) {
            if from.is_terminal() {
                return Some(format!(
                    "{}: ignored a late `{to}` for an order already `{from}`",
                    current.client_id
                ));
            }
            anomaly = Some(format!(
                "{}: broker moved the order `{from}` -> `{to}`, which the lifecycle does not \
                 allow; taking the broker's word",
                current.client_id
            ));
        }
        let filled = current.filled_qty.max(incoming.filled_qty);
        let avg = if incoming.filled_qty >= current.filled_qty {
            incoming.avg_fill_price.or(current.avg_fill_price)
        } else {
            current.avg_fill_price
        };
        let broker_id = incoming.broker_id.clone().or(current.broker_id.take());
        *current = Order {
            filled_qty: filled,
            avg_fill_price: avg,
            broker_id,
            reason: incoming.reason.clone().or(current.reason.take()),
            ..incoming
        };
        anomaly
    }

    /// Adds a fill unless its execution id was seen before. Returns whether it was new.
    pub fn add_fill(&mut self, fill: Fill) -> bool {
        if !self.exec_ids.insert(fill.exec_id.clone()) {
            return false;
        }
        self.fills.push(fill);
        true
    }

    pub fn get(&self, id: &ClientOrderId) -> Option<&Tracked> {
        self.orders.get(id)
    }

    pub fn orders(&self) -> impl Iterator<Item = &Tracked> {
        self.orders.values()
    }

    /// Orders that could still fill, including ones whose fate is unknown.
    pub fn live(&self) -> impl Iterator<Item = &Tracked> {
        self.orders.values().filter(|t| t.order.status.is_live())
    }

    /// Sent orders that never heard back: `Unknown`, or `PendingSubmit` left by a crash.
    pub fn unresolved(&self) -> impl Iterator<Item = &Tracked> {
        self.orders.values().filter(|t| {
            matches!(
                t.order.status,
                OrderStatus::Unknown | OrderStatus::PendingSubmit
            )
        })
    }

    pub fn fills(&self) -> &[Fill] {
        &self.fills
    }

    pub fn knows_exec(&self, exec_id: &str) -> bool {
        self.exec_ids.contains(exec_id)
    }

    pub fn baseline(&self) -> Option<&(Vec<Position>, HashSet<String>)> {
        self.baseline.as_ref()
    }

    /// Sends and modifies in the `window` before `now`.
    pub fn submissions_within(&self, now: Timestamp, window: SignedDuration) -> usize {
        let since = now.saturating_sub(window).unwrap_or(now);
        self.submissions.iter().filter(|t| **t > since).count()
    }

    /// Notional charged to `now`'s exchange date: every order sent that day, except those the
    /// broker refused or never received, at the largest size it was ever asked to be.
    pub fn notional_today(&self, now: Timestamp) -> f64 {
        let today = exchange_date(now);
        self.orders
            .values()
            .filter(|t| exchange_date(t.sent_at) == today)
            .filter(|t| {
                !matches!(
                    t.order.status,
                    OrderStatus::Rejected | OrderStatus::NotPlaced
                )
            })
            .map(|t| t.est_notional)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Instrument, OrderKind, OrderRequest, OrderSpec, Qty, Side};

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(1_791_208_800 + secs).unwrap() // 2026-10-05 10:00 New York
    }

    fn request(id: &str) -> OrderRequest {
        OrderRequest {
            client_id: ClientOrderId::new(id),
            spec: OrderSpec::new(
                Instrument::stock("AAPL"),
                Side::Buy,
                Qty::Shares(100.0),
                OrderKind::Market,
            ),
        }
    }

    fn record(secs: i64, entry: Entry) -> Record {
        Record {
            time: t(secs),
            pid: 1,
            entry,
        }
    }

    fn intent(secs: i64, id: &str, notional: f64) -> Record {
        record(
            secs,
            Entry::Intent {
                request: request(id),
                est_notional: notional,
            },
        )
    }

    fn snapshot(id: &str, status: OrderStatus, filled: f64) -> Order {
        let mut o = Order::new(&request(id), status, t(0));
        o.filled_qty = filled;
        o
    }

    #[test]
    fn an_intent_without_an_answer_is_unresolved_after_a_restart() {
        let (book, _) = OrderBook::from_records(&[intent(0, "a", 1.0)]);
        let unresolved: Vec<_> = book
            .unresolved()
            .map(|t| t.order.client_id.0.clone())
            .collect();
        assert_eq!(unresolved, ["a"]);
        assert!(
            book.get(&ClientOrderId::new("a"))
                .unwrap()
                .order
                .status
                .is_live()
        );
    }

    #[test]
    fn a_late_status_cannot_resurrect_a_finished_order() {
        let mut book = OrderBook::default();
        book.apply(&intent(0, "a", 1.0));
        assert!(
            book.update(snapshot("a", OrderStatus::Filled, 100.0), t(1))
                .is_none()
        );
        // The `Submitted` from before the fill arrives after it.
        let anomaly = book.update(snapshot("a", OrderStatus::Submitted, 0.0), t(2));
        assert!(anomaly.unwrap().contains("late"));
        let order = &book.get(&ClientOrderId::new("a")).unwrap().order;
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.filled_qty, 100.0);
    }

    #[test]
    fn filled_quantity_only_grows() {
        let mut book = OrderBook::default();
        book.apply(&intent(0, "a", 1.0));
        book.update(snapshot("a", OrderStatus::PartiallyFilled, 60.0), t(1));
        book.update(snapshot("a", OrderStatus::PartiallyFilled, 30.0), t(2));
        assert_eq!(
            book.get(&ClientOrderId::new("a")).unwrap().order.filled_qty,
            60.0
        );
    }

    #[test]
    fn an_impossible_live_transition_is_taken_but_noted() {
        let mut book = OrderBook::default();
        book.apply(&intent(0, "a", 1.0));
        book.update(snapshot("a", OrderStatus::PartiallyFilled, 10.0), t(1));
        let anomaly = book.update(snapshot("a", OrderStatus::Submitted, 10.0), t(2));
        assert!(anomaly.is_some());
        assert_eq!(
            book.get(&ClientOrderId::new("a")).unwrap().order.status,
            OrderStatus::Submitted
        );
    }

    #[test]
    fn a_redelivered_execution_counts_once() {
        let mut book = OrderBook::default();
        let fill = Fill {
            exec_id: "e1".into(),
            client_id: ClientOrderId::new("a"),
            broker_order_id: None,
            instrument: Instrument::stock("AAPL"),
            side: Side::Buy,
            qty: 10.0,
            price: 100.0,
            commission: None,
            time: t(0),
        };
        assert!(book.add_fill(fill.clone()));
        assert!(!book.add_fill(fill));
        assert_eq!(book.fills().len(), 1);
    }

    #[test]
    fn the_daily_budget_counts_today_and_skips_what_never_traded() {
        let mut book = OrderBook::default();
        book.apply(&intent(-86_400, "yesterday", 1_000.0));
        book.apply(&intent(0, "a", 500.0));
        book.apply(&intent(1, "rejected", 700.0));
        book.update(snapshot("rejected", OrderStatus::Rejected, 0.0), t(2));
        book.apply(&record(
            3,
            Entry::ModifyIntent {
                client_id: ClientOrderId::new("a"),
                spec: request("a").spec,
                est_notional: 800.0,
            },
        ));
        assert_eq!(book.notional_today(t(10)), 800.0);
        assert_eq!(
            book.submissions_within(t(10), SignedDuration::from_secs(60)),
            3
        );
        assert_eq!(
            book.submissions_within(t(70), SignedDuration::from_secs(60)),
            0
        );
    }
}
