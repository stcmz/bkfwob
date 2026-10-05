//! The desk against the scripted fake broker: every failure a real broker produces rarely, on
//! demand. Each test names the situation it reproduces.

use std::sync::Arc;

use bkfwob_core::broker::{Broker, BrokerError, BrokerEvent};
use bkfwob_core::clock::ManualClock;
use bkfwob_core::desk::{Desk, broker_error_of, refusal_of};
use bkfwob_core::fake::{FakeBroker, Script};
use bkfwob_core::guard::Limits;
use bkfwob_core::refusal::RefusalCode;
use bkfwob_core::state::StateDir;
use bkfwob_core::status::OrderStatus;
use bkfwob_core::types::{
    ClientOrderId, Instrument, Mode, OrderChange, OrderKind, OrderSpec, Qty, Side,
};
use tempfile::TempDir;

struct Rig {
    dir: TempDir,
    clock: Arc<ManualClock>,
    desk: Desk<FakeBroker>,
}

fn rig() -> Rig {
    let dir = tempfile::tempdir().unwrap();
    // A Monday morning in New York.
    let clock = ManualClock::new("2026-10-05T14:00:00Z".parse().unwrap());
    let mut broker = FakeBroker::new(clock.clone(), 100_000.0);
    broker.set_quote("AAPL", 99.99, 100.01);
    let state = StateDir::open(dir.path(), "fake-paper").unwrap();
    let desk = Desk::open(broker, state, Limits::default(), clock.clone()).unwrap();
    Rig { dir, clock, desk }
}

impl Rig {
    /// Another process on the same state directory.
    fn second_desk(&self) -> Desk<FakeBroker> {
        let broker = FakeBroker::new(self.clock.clone(), 100_000.0);
        let state = StateDir::open(self.dir.path(), "fake-paper").unwrap();
        Desk::open(broker, state, Limits::default(), self.clock.clone()).unwrap()
    }
    fn fake(&mut self) -> &mut FakeBroker {
        self.desk.broker_mut()
    }
}

fn order(side: Side, qty: f64, kind: OrderKind) -> OrderSpec {
    OrderSpec::new(Instrument::stock("AAPL"), side, Qty::Shares(qty), kind)
}

fn limit_buy(qty: f64) -> OrderSpec {
    order(Side::Buy, qty, OrderKind::Limit { limit: 99.0 })
}

fn code(error: anyhow::Error) -> RefusalCode {
    refusal_of(&error)
        .unwrap_or_else(|| panic!("expected a refusal, got {error:#}"))
        .code
}

fn status(desk: &Desk<FakeBroker>, id: &ClientOrderId) -> OrderStatus {
    desk.book().get(id).unwrap().order.status
}

#[test]
fn a_placed_order_is_journaled_then_tracked() {
    let mut r = rig();
    let placed = r.desk.place(&limit_buy(10.0)).unwrap();
    assert_eq!(placed.status, OrderStatus::Submitted);
    assert_eq!(placed.client_id.as_str(), "bk-20261005-0001");
    assert_eq!(status(&r.desk, &placed.client_id), OrderStatus::Submitted);
    assert_eq!(r.desk.book().notional_today(r.clock.now_ts()), 990.0);
}

/// The reply to a place is lost, but the broker did take the order. The write-ahead intent is
/// what makes this recoverable: the order is `Unknown`, never re-sent, and reconciliation finds
/// it by client id.
#[test]
fn a_lost_reply_is_resolved_by_lookup_not_by_resending() {
    let mut r = rig();
    r.fake().script(Script::LoseReplyAfterAccept);
    let error = r.desk.place(&limit_buy(10.0)).unwrap_err();
    assert!(matches!(
        broker_error_of(&error),
        Some(BrokerError::Ambiguous(_))
    ));
    let id = ClientOrderId::new("bk-20261005-0001");
    assert_eq!(status(&r.desk, &id), OrderStatus::Unknown);
    assert_eq!(r.fake().order_count(), 1, "sent exactly once");

    let report = r.desk.reconcile().unwrap();
    assert_eq!(report.adopted, std::slice::from_ref(&id));
    assert_eq!(status(&r.desk, &id), OrderStatus::Submitted);
    assert_eq!(r.fake().order_count(), 1, "reconciliation never re-sends");
}

/// The request never reached the broker. Inside the grace window it stays unknown (the broker may
/// simply be slow to show it); after, it is declared not placed and stops counting.
#[test]
fn a_lost_request_becomes_not_placed_after_the_grace_window() {
    let mut r = rig();
    r.fake().script(Script::LoseRequest);
    r.desk.place(&limit_buy(10.0)).unwrap_err();
    let id = ClientOrderId::new("bk-20261005-0001");

    let early = r.desk.reconcile().unwrap();
    assert_eq!(early.still_unknown, std::slice::from_ref(&id));
    assert_eq!(status(&r.desk, &id), OrderStatus::Unknown);

    r.clock.advance_secs(61);
    let late = r.desk.reconcile().unwrap();
    assert_eq!(late.not_placed, std::slice::from_ref(&id));
    assert_eq!(status(&r.desk, &id), OrderStatus::NotPlaced);
    assert_eq!(r.desk.book().notional_today(r.clock.now_ts()), 0.0);
}

/// A crash between journaling the intent and hearing back: the restarted process must know an
/// order may exist.
#[test]
fn an_intent_survives_a_restart_as_unresolved() {
    let mut r = rig();
    r.fake().script(Script::LoseReplyAfterAccept);
    r.desk.place(&limit_buy(10.0)).unwrap_err();
    let restarted = r.second_desk();
    let unresolved: Vec<_> = restarted
        .book()
        .unresolved()
        .map(|t| t.order.client_id.clone())
        .collect();
    assert_eq!(unresolved, [ClientOrderId::new("bk-20261005-0001")]);
}

#[test]
fn a_rejection_is_final_and_costs_no_budget() {
    let mut r = rig();
    r.fake()
        .script(Script::Reject("insufficient margin".into()));
    let error = r.desk.place(&limit_buy(10.0)).unwrap_err();
    assert!(matches!(
        broker_error_of(&error),
        Some(BrokerError::Rejected(_))
    ));
    let id = ClientOrderId::new("bk-20261005-0001");
    assert_eq!(status(&r.desk, &id), OrderStatus::Rejected);
    assert_eq!(r.desk.book().notional_today(r.clock.now_ts()), 0.0);
    assert!(r.desk.book().unresolved().next().is_none());
}

#[test]
fn a_disconnect_before_sending_places_nothing() {
    let mut r = rig();
    r.fake().script(Script::Disconnect);
    let error = r.desk.place(&limit_buy(10.0)).unwrap_err();
    assert!(matches!(
        broker_error_of(&error),
        Some(BrokerError::Disconnected(_))
    ));
    assert_eq!(r.fake().order_count(), 0);
}

/// Thirty of fifty shares fill, the rest is cancelled, and the last execution report arrives after the
/// cancel confirmation. The order stays cancelled; the late fill still counts, once.
#[test]
fn partial_fill_then_cancel_with_a_late_and_duplicated_execution() {
    let mut r = rig();
    let id = r.desk.place(&limit_buy(50.0)).unwrap().client_id;
    let fill = r.fake().fill(&id, 30.0, 99.0);
    r.desk.cancel(&id).unwrap();
    r.desk.drain().unwrap();
    assert_eq!(status(&r.desk, &id), OrderStatus::Cancelled);

    r.fake()
        .push_event(BrokerEvent::Fill { fill: fill.clone() });
    r.fake().push_event(BrokerEvent::Fill { fill });
    let mut stale = r.desk.book().get(&id).unwrap().order.clone();
    stale.status = OrderStatus::PartiallyFilled;
    r.fake().push_event(BrokerEvent::Order { order: stale });
    r.desk.drain().unwrap();

    let tracked = &r.desk.book().get(&id).unwrap().order;
    assert_eq!(
        tracked.status,
        OrderStatus::Cancelled,
        "finished is finished"
    );
    assert_eq!(tracked.filled_qty, 30.0);
    assert_eq!(
        r.desk.book().fills().len(),
        1,
        "one execution, counted once"
    );
}

/// The line drops, the order fills while we are away, the line comes back. The reconnect event
/// triggers reconciliation, which picks up the fill and the status, and the positions agree.
#[test]
fn fills_that_happen_while_disconnected_are_caught_on_reconnect() {
    let mut r = rig();
    r.desk.reconcile().unwrap(); // baseline: flat
    let id = r.desk.place(&limit_buy(10.0)).unwrap().client_id;
    r.desk.drain().unwrap();
    r.fake().script(Script::Disconnect);
    assert!(r.desk.positions().is_err());

    r.fake().fill(&id, 10.0, 99.0);
    // Drop the events the broker emitted while we were not listening; a real broker does not
    // replay them either.
    while r
        .fake()
        .next_event(std::time::Duration::ZERO)
        .unwrap()
        .is_some()
    {}
    r.fake().reconnect();
    let events = r.desk.drain().unwrap();
    assert!(events.contains(&BrokerEvent::Reconnected));

    assert_eq!(status(&r.desk, &id), OrderStatus::Filled);
    assert_eq!(r.desk.book().fills().len(), 1);
    assert!(r.desk.halted().is_none(), "the fill explains the position");
}

/// Shares appear that no fill explains — someone traded this account by hand, or our records are
/// wrong. Trading halts for every process; cancels still work.
#[test]
fn an_unexplained_position_halts_everything() {
    let mut r = rig();
    let resting = r.desk.place(&limit_buy(10.0)).unwrap().client_id;
    r.desk.reconcile().unwrap();
    r.fake().set_position("MSFT", 25.0, 400.0);

    let report = r.desk.reconcile().unwrap();
    assert!(report.halted);
    assert_eq!(report.mismatches[0].symbol, "MSFT");

    assert_eq!(
        code(r.desk.place(&limit_buy(1.0)).unwrap_err()),
        RefusalCode::Halted
    );
    let mut other = r.second_desk();
    assert_eq!(
        code(other.place(&limit_buy(1.0)).unwrap_err()),
        RefusalCode::Halted,
        "the halt is on disk, not in one process's memory"
    );
    r.desk.cancel(&resting).unwrap();

    assert!(r.desk.resume().unwrap());
    r.desk.reconcile().unwrap(); // the new baseline accepts the account as it is
    assert!(r.desk.place(&limit_buy(1.0)).is_ok());
}

#[test]
fn halting_cancels_everything_working() {
    let mut r = rig();
    let id = r.desk.place(&limit_buy(10.0)).unwrap().client_id;
    r.desk.halt("manual").unwrap();
    assert_eq!(r.fake().order(&id).unwrap().status, OrderStatus::Cancelled);
    assert_eq!(r.desk.halted().unwrap().reason, "manual");
}

/// The live version of qtfwob's orphaned-stop short: a stop rests for the whole position, and an
/// exit for the whole position is sent too. If both executed, the account would be short.
#[test]
fn an_exit_on_top_of_a_resting_stop_is_refused() {
    let mut r = rig();
    r.fake().set_position("AAPL", 10.0, 90.0);
    let stop = r
        .desk
        .place(&order(Side::Sell, 10.0, OrderKind::Stop { stop: 95.0 }))
        .unwrap()
        .client_id;
    assert_eq!(
        code(
            r.desk
                .place(&order(Side::Sell, 10.0, OrderKind::Market))
                .unwrap_err()
        ),
        RefusalCode::Oversell
    );
    r.desk.cancel(&stop).unwrap();
    assert!(
        r.desk
            .place(&order(Side::Sell, 10.0, OrderKind::Market))
            .is_ok()
    );
}

/// An order of unknown fate might be working, so it counts. Otherwise a lost reply would let the
/// same exit be sent twice.
#[test]
fn an_order_of_unknown_fate_counts_against_the_position() {
    let mut r = rig();
    r.fake().set_position("AAPL", 10.0, 90.0);
    r.fake().script(Script::LoseRequest);
    r.desk
        .place(&order(Side::Sell, 10.0, OrderKind::Limit { limit: 100.0 }))
        .unwrap_err();
    assert_eq!(
        code(
            r.desk
                .place(&order(Side::Sell, 10.0, OrderKind::Limit { limit: 100.0 }))
                .unwrap_err()
        ),
        RefusalCode::Oversell
    );
}

#[test]
fn refusals_are_journaled_and_send_nothing() {
    let mut r = rig();
    let error = r.desk.place(&limit_buy(1_000.0)).unwrap_err();
    assert_eq!(code(error), RefusalCode::OrderNotional);
    assert_eq!(r.fake().order_count(), 0);
    let journal = std::fs::read_dir(r.desk.state().journal_dir())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "jsonl"))
        .unwrap();
    let text = std::fs::read_to_string(journal).unwrap();
    assert!(text.contains(r#""kind":"refused""#), "{text}");
    assert!(text.contains("order-notional"), "{text}");
}

#[test]
fn modify_moves_a_working_order_and_refuses_finished_ones() {
    let mut r = rig();
    let id = r.desk.place(&limit_buy(10.0)).unwrap().client_id;
    let moved = r
        .desk
        .modify(
            &id,
            &OrderChange {
                kind: Some(OrderKind::Limit { limit: 98.5 }),
                qty: Some(Qty::Shares(20.0)),
            },
        )
        .unwrap();
    assert_eq!(moved.spec.kind, OrderKind::Limit { limit: 98.5 });
    assert_eq!(r.fake().order(&id).unwrap().spec.qty, Qty::Shares(20.0));

    let too_big = OrderChange {
        qty: Some(Qty::Shares(1_000.0)),
        kind: None,
    };
    assert_eq!(
        code(r.desk.modify(&id, &too_big).unwrap_err()),
        RefusalCode::OrderNotional
    );
    let to_market = OrderChange {
        kind: Some(OrderKind::Market),
        qty: None,
    };
    assert_eq!(
        code(r.desk.modify(&id, &to_market).unwrap_err()),
        RefusalCode::Invalid
    );

    r.desk.cancel(&id).unwrap();
    let again = OrderChange {
        kind: Some(OrderKind::Limit { limit: 98.0 }),
        qty: None,
    };
    assert_eq!(
        code(r.desk.modify(&id, &again).unwrap_err()),
        RefusalCode::NotWorking
    );
    assert_eq!(
        code(r.desk.cancel(&id).unwrap_err()),
        RefusalCode::NotWorking
    );
}

#[test]
fn the_rate_limit_counts_across_processes() {
    let mut r = rig();
    for _ in 0..5 {
        r.desk.place(&limit_buy(1.0)).unwrap();
    }
    let mut other = r.second_desk();
    other.broker_mut().set_quote("AAPL", 99.99, 100.01);
    for _ in 0..5 {
        other.place(&limit_buy(1.0)).unwrap();
    }
    assert_eq!(
        code(r.desk.place(&limit_buy(1.0)).unwrap_err()),
        RefusalCode::RateLimit
    );
    r.clock.advance_secs(61);
    assert!(r.desk.place(&limit_buy(1.0)).is_ok());
}

#[test]
fn a_paper_desk_will_not_run_against_a_live_account_or_vice_versa() {
    let mut r = rig();
    assert!(r.desk.verify_mode(Mode::Paper).is_ok());
    assert!(r.desk.verify_mode(Mode::Live).is_err());
    r.fake().set_mode(Mode::Live, "U1234567");
    assert!(r.desk.verify_mode(Mode::Paper).is_err());
}

#[test]
fn preview_says_what_would_happen_and_does_nothing() {
    let mut r = rig();
    let ok = r.desk.preview(&limit_buy(10.0)).unwrap();
    assert!(ok.refusal.is_none());
    assert_eq!(ok.assessment.unwrap().est_notional, 990.0);
    let no = r.desk.preview(&limit_buy(1_000.0)).unwrap();
    assert_eq!(no.refusal.unwrap().code, RefusalCode::OrderNotional);
    assert_eq!(r.fake().order_count(), 0);
    assert!(r.desk.book().orders().next().is_none());
}

trait NowTs {
    fn now_ts(&self) -> jiff::Timestamp;
}

impl NowTs for ManualClock {
    fn now_ts(&self) -> jiff::Timestamp {
        bkfwob_core::clock::Clock::now(self)
    }
}

/// A broker that, at the moment it receives an order, checks the journal already holds the
/// intent for it. "Write-ahead" is a claim about ordering, and only this observes the order.
struct Spy {
    inner: FakeBroker,
    journal: bkfwob_core::journal::Journal,
    saw_intent_first: Vec<bool>,
}

impl Broker for Spy {
    fn name(&self) -> &str {
        "spy"
    }
    fn capabilities(&self) -> bkfwob_core::broker::Capabilities {
        self.inner.capabilities()
    }
    fn account(&mut self) -> Result<bkfwob_core::types::Account, BrokerError> {
        self.inner.account()
    }
    fn positions(&mut self) -> Result<Vec<bkfwob_core::types::Position>, BrokerError> {
        self.inner.positions()
    }
    fn open_orders(&mut self) -> Result<Vec<bkfwob_core::types::Order>, BrokerError> {
        self.inner.open_orders()
    }
    fn find_order(
        &mut self,
        id: &ClientOrderId,
    ) -> Result<Option<bkfwob_core::types::Order>, BrokerError> {
        self.inner.find_order(id)
    }
    fn fills(
        &mut self,
        since: jiff::Timestamp,
    ) -> Result<Vec<bkfwob_core::types::Fill>, BrokerError> {
        self.inner.fills(since)
    }
    fn quote(&mut self, instrument: &Instrument) -> Result<bkfwob_core::types::Quote, BrokerError> {
        self.inner.quote(instrument)
    }
    fn place(
        &mut self,
        request: &bkfwob_core::types::OrderRequest,
    ) -> Result<bkfwob_core::types::Order, BrokerError> {
        let records = self.journal.read_all().unwrap();
        self.saw_intent_first.push(records.iter().any(|r| {
            matches!(&r.entry, bkfwob_core::journal::Entry::Intent { request: i, .. }
                if i.client_id == request.client_id)
        }));
        self.inner.place(request)
    }
    fn modify(
        &mut self,
        id: &ClientOrderId,
        spec: &OrderSpec,
    ) -> Result<bkfwob_core::types::Order, BrokerError> {
        self.inner.modify(id, spec)
    }
    fn cancel(&mut self, id: &ClientOrderId) -> Result<(), BrokerError> {
        self.inner.cancel(id)
    }
    fn cancel_all(&mut self) -> Result<(), BrokerError> {
        self.inner.cancel_all()
    }
    fn next_event(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<Option<BrokerEvent>, BrokerError> {
        self.inner.next_event(timeout)
    }
}

#[test]
fn the_intent_is_on_disk_before_the_broker_sees_the_order() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new("2026-10-05T14:00:00Z".parse().unwrap());
    let state = StateDir::open(dir.path(), "spy-paper").unwrap();
    let spy = Spy {
        inner: FakeBroker::new(clock.clone(), 100_000.0),
        journal: bkfwob_core::journal::Journal::open(state.journal_dir()).unwrap(),
        saw_intent_first: Vec::new(),
    };
    let mut desk = Desk::open(spy, state, Limits::default(), clock).unwrap();
    desk.place(&limit_buy(1.0)).unwrap();
    desk.place(&limit_buy(2.0)).unwrap();
    assert_eq!(desk.broker().saw_intent_first, [true, true]);
}
