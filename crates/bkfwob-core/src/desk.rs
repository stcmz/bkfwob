//! The desk: the only way orders reach a broker.
//!
//! It wraps a raw [`Broker`] with the three things that make sending orders safe, in this order:
//!
//! 1. **Guardrails** — the kill switch, the adapter's capabilities, and [`crate::guard::assess`].
//!    A refusal is journaled and returned; nothing is sent.
//! 2. **Write-ahead journal** — the intent is synced to disk under a fresh client id *before* the
//!    broker sees it.
//! 3. **Classified outcomes** — an answer updates the book; a rejection is final; a lost answer
//!    leaves the order `Unknown` for [`Desk::reconcile`] to resolve by id. Nothing is ever re-sent
//!    automatically.
//!
//! Cancels are allowed while halted (they only reduce risk); places and modifies are not.
//!
//! Several processes may share one state directory (a CLI next to an MCP server), so the book is
//! re-read from the journal before every write rather than trusted from memory.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use jiff::{SignedDuration, Timestamp};
use serde::Serialize;

use crate::book::OrderBook;
use crate::broker::{Broker, BrokerError, BrokerEvent, Capabilities};
use crate::clock::SharedClock;
use crate::guard::{Assessment, Limits, Snapshot, assess};
use crate::journal::{Entry, Journal};
use crate::refusal::{Refusal, RefusalCode};
use crate::state::{Halt, StateDir};
use crate::status::OrderStatus;
use crate::types::{
    Account, ClientOrderId, Fill, Mode, Order, OrderChange, OrderRequest, OrderSpec, Position,
    Quote,
};

/// How long an order of unknown fate may be missing at the broker before it is declared never
/// placed. Brokers can take a moment to make a just-received order visible to lookups.
pub const DEFAULT_GRACE: SignedDuration = SignedDuration::from_secs(60);

pub struct Desk<B: Broker> {
    broker: B,
    caps: Capabilities,
    state: StateDir,
    journal: Journal,
    book: OrderBook,
    limits: Limits,
    clock: SharedClock,
    grace: SignedDuration,
}

/// A guardrail verdict on an order that has not been sent.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Preview {
    pub spec: OrderSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assessment: Option<Assessment>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<Refusal>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Mismatch {
    pub symbol: String,
    pub expected: f64,
    pub actual: f64,
}

/// What a reconciliation found and did.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Reconciliation {
    /// Orders of unknown fate the broker turned out to have.
    pub adopted: Vec<ClientOrderId>,
    /// Orders of unknown fate the broker has no record of, past the grace window.
    pub not_placed: Vec<ClientOrderId>,
    /// Orders of unknown fate still inside the grace window.
    pub still_unknown: Vec<ClientOrderId>,
    /// Our orders whose status moved while we were not listening.
    pub changed: Vec<(ClientOrderId, OrderStatus, OrderStatus)>,
    pub new_fills: usize,
    /// Working orders in the account that we did not place.
    pub external: Vec<Order>,
    /// Positions that do not equal the last baseline plus the fills since.
    pub mismatches: Vec<Mismatch>,
    pub anomalies: Vec<String>,
    /// Whether this reconciliation halted trading.
    pub halted: bool,
}

fn refused(refusal: Refusal) -> anyhow::Error {
    anyhow::Error::new(refusal)
}

impl<B: Broker> Desk<B> {
    pub fn open(broker: B, state: StateDir, limits: Limits, clock: SharedClock) -> Result<Self> {
        let journal = Journal::open(state.journal_dir())?;
        let caps = broker.capabilities();
        let mut desk = Self {
            broker,
            caps,
            state,
            journal,
            book: OrderBook::default(),
            limits,
            clock,
            grace: DEFAULT_GRACE,
        };
        desk.refresh()?;
        Ok(desk)
    }

    pub fn with_grace(mut self, grace: SignedDuration) -> Self {
        self.grace = grace;
        self
    }

    /// Re-reads the journal, picking up what other processes wrote.
    pub fn refresh(&mut self) -> Result<()> {
        let records = self.journal.read_all()?;
        self.book = OrderBook::from_records(&records).0;
        Ok(())
    }

    pub fn broker(&self) -> &B {
        &self.broker
    }
    /// The raw broker, for test setup. Orders sent through this bypass every guardrail.
    pub fn broker_mut(&mut self) -> &mut B {
        &mut self.broker
    }
    pub fn book(&self) -> &OrderBook {
        &self.book
    }
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
    pub fn state(&self) -> &StateDir {
        &self.state
    }
    pub fn capabilities(&self) -> &Capabilities {
        &self.caps
    }
    pub fn halted(&self) -> Option<Halt> {
        self.state.halted()
    }

    fn now(&self) -> Timestamp {
        self.clock.now()
    }

    fn record(&mut self, entry: Entry, sync: bool) -> Result<()> {
        let now = self.now();
        let record = self.journal.append(now, entry, sync)?;
        if let Some(detail) = self.book.apply(&record) {
            self.journal.append(now, Entry::Anomaly { detail }, false)?;
        }
        Ok(())
    }

    /// Confirms the broker's account is the mode the caller expects. A paper config pointed at a
    /// live gateway (or the reverse) stops here, before anything else is asked of it.
    pub fn verify_mode(&mut self, expected: Mode) -> Result<Account> {
        let account = self.broker.account()?;
        if account.mode != expected {
            bail!(
                "expected a {expected} account but {} reports {} is {}; refusing to continue",
                self.broker.name(),
                account.id,
                account.mode
            );
        }
        Ok(account)
    }

    // ----- reads: straight through ---------------------------------------------------------

    pub fn account(&mut self) -> Result<Account> {
        Ok(self.broker.account()?)
    }
    pub fn positions(&mut self) -> Result<Vec<Position>> {
        Ok(self.broker.positions()?)
    }
    pub fn open_orders(&mut self) -> Result<Vec<Order>> {
        Ok(self.broker.open_orders()?)
    }
    pub fn fills(&mut self, since: Timestamp) -> Result<Vec<Fill>> {
        Ok(self.broker.fills(since)?)
    }
    pub fn quote(&mut self, instrument: &crate::types::Instrument) -> Result<Quote> {
        Ok(self.broker.quote(instrument)?)
    }
    pub fn find_order(&mut self, id: &ClientOrderId) -> Result<Option<Order>> {
        Ok(self.broker.find_order(id)?)
    }

    // ----- guardrails ------------------------------------------------------------------------

    fn check_halt(&self) -> Result<(), Refusal> {
        match self.state.halted() {
            Some(halt) => Err(Refusal::new(
                RefusalCode::Halted,
                format!(
                    "trading is halted ({}); `bkfwob resume` lifts it",
                    halt.reason
                ),
            )),
            None => Ok(()),
        }
    }

    /// Working orders as the guardrails must count them: everything the broker says is working,
    /// plus our orders of unknown fate, which might be working too.
    fn working_orders(&mut self, except: Option<&ClientOrderId>) -> Result<Vec<Order>> {
        let mut orders = self.broker.open_orders()?;
        let seen: HashSet<ClientOrderId> = orders.iter().map(|o| o.client_id.clone()).collect();
        orders.extend(
            self.book
                .unresolved()
                .filter(|t| !seen.contains(&t.order.client_id))
                .map(|t| t.order.clone()),
        );
        if let Some(id) = except {
            orders.retain(|o| &o.client_id != id);
        }
        Ok(orders)
    }

    fn assess(
        &mut self,
        spec: &OrderSpec,
        except: Option<&ClientOrderId>,
        charge_floor: f64,
    ) -> Result<Result<Assessment, Refusal>> {
        if let Err(refusal) = self.check_halt().and_then(|()| self.caps.check(spec)) {
            return Ok(Err(refusal));
        }
        let quote = match self.broker.quote(&spec.instrument) {
            Ok(q) => Some(q),
            Err(BrokerError::Disconnected(why)) => bail!(BrokerError::Disconnected(why)),
            Err(_) => None,
        };
        let position = self
            .broker
            .positions()?
            .iter()
            .find(|p| p.instrument.symbol == spec.instrument.symbol)
            .map_or(0.0, |p| p.qty);
        let open = self.working_orders(except)?;
        let now = self.now();
        let mut charged = self.book.notional_today(now);
        if let Some(t) = except.and_then(|id| self.book.get(id)) {
            charged -= t.est_notional;
        }
        let snapshot = Snapshot {
            quote: quote.as_ref(),
            position,
            open_orders: &open,
            charged_today: charged.max(0.0),
            charge_floor,
            recent_submissions: self
                .book
                .submissions_within(now, SignedDuration::from_secs(60)),
            now,
        };
        Ok(assess(&self.limits, spec, &snapshot))
    }

    /// The guardrail verdict on `spec`, without sending or journaling anything.
    pub fn preview(&mut self, spec: &OrderSpec) -> Result<Preview> {
        self.refresh()?;
        let verdict = self.assess(spec, None, 0.0)?;
        Ok(Preview {
            spec: spec.clone(),
            assessment: verdict.as_ref().ok().copied(),
            refusal: verdict.err(),
        })
    }

    // ----- writes ----------------------------------------------------------------------------

    /// Sends a new order through the guardrails. Errors carry a [`Refusal`] (nothing sent) or a
    /// [`BrokerError`] (see its variants for what each means for the order).
    pub fn place(&mut self, spec: &OrderSpec) -> Result<Order> {
        self.refresh()?;
        let assessment = match self.assess(spec, None, 0.0)? {
            Ok(a) => a,
            Err(refusal) => {
                self.record(
                    Entry::Refused {
                        spec: spec.clone(),
                        refusal: refusal.clone(),
                    },
                    false,
                )?;
                return Err(refused(refusal));
            }
        };
        let request = OrderRequest {
            client_id: self.state.next_client_id(self.now())?,
            spec: spec.clone(),
        };
        // Write-ahead: on disk before the broker can act on it.
        self.record(
            Entry::Intent {
                request: request.clone(),
                est_notional: assessment.est_notional,
            },
            true,
        )?;
        match self.broker.place(&request) {
            Ok(order) => {
                self.record(
                    Entry::Order {
                        order: order.clone(),
                    },
                    false,
                )?;
                Ok(order)
            }
            Err(BrokerError::Rejected(reason)) => {
                self.record_final(&request, OrderStatus::Rejected, &reason)?;
                Err(anyhow!(BrokerError::Rejected(reason)))
            }
            Err(BrokerError::Disconnected(reason)) => {
                self.record_final(&request, OrderStatus::NotPlaced, &reason)?;
                Err(anyhow!(BrokerError::Disconnected(reason)))
            }
            // Ambiguous, and anything unclassified: unsafe to assume it was not sent.
            Err(error) => {
                self.record(
                    Entry::Unknown {
                        client_id: request.client_id.clone(),
                        error: error.to_string(),
                    },
                    true,
                )?;
                Err(anyhow!(error)).with_context(|| {
                    format!(
                        "order {} may or may not have been placed; it will not be re-sent. \
                         `bkfwob reconcile` resolves it",
                        request.client_id
                    )
                })
            }
        }
    }

    fn record_final(
        &mut self,
        request: &OrderRequest,
        status: OrderStatus,
        reason: &str,
    ) -> Result<()> {
        let mut order = Order::new(request, status, self.now());
        order.reason = Some(reason.to_string());
        self.record(Entry::Order { order }, false)
    }

    /// The working order `id` as best we know it: the broker's view, else our book's.
    fn working(&mut self, id: &ClientOrderId) -> Result<Order> {
        let order = match self.broker.find_order(id)? {
            Some(order) => order,
            None => match self.book.get(id) {
                Some(t) => t.order.clone(),
                None => {
                    return Err(refused(Refusal::new(
                        RefusalCode::UnknownOrder,
                        format!("no order {id} at the broker or in the journal"),
                    )));
                }
            },
        };
        if order.status.is_terminal() {
            return Err(refused(Refusal::new(
                RefusalCode::NotWorking,
                format!("order {id} is already {}", order.status),
            )));
        }
        if matches!(
            order.status,
            OrderStatus::Unknown | OrderStatus::PendingSubmit
        ) {
            return Err(refused(Refusal::new(
                RefusalCode::NotWorking,
                format!("order {id} is of unknown fate; reconcile before changing it"),
            )));
        }
        Ok(order)
    }

    pub fn modify(&mut self, id: &ClientOrderId, change: &OrderChange) -> Result<Order> {
        self.refresh()?;
        if change.is_empty() {
            bail!("nothing to change: give a new quantity and/or price");
        }
        let current = self.working(id)?;
        let spec = change
            .apply(&current.spec)
            .map_err(|e| refused(Refusal::new(RefusalCode::Invalid, e)))?;
        let floor = self.book.get(id).map_or(0.0, |t| t.est_notional);
        let assessment = match self.assess(&spec, Some(id), floor)? {
            Ok(a) => a,
            Err(refusal) => {
                self.record(
                    Entry::Refused {
                        spec,
                        refusal: refusal.clone(),
                    },
                    false,
                )?;
                return Err(refused(refusal));
            }
        };
        self.record(
            Entry::ModifyIntent {
                client_id: id.clone(),
                spec: spec.clone(),
                est_notional: assessment.est_notional,
            },
            true,
        )?;
        match self.broker.modify(id, &spec) {
            Ok(order) => {
                self.record(
                    Entry::Order {
                        order: order.clone(),
                    },
                    false,
                )?;
                Ok(order)
            }
            Err(error) => {
                let detail = match &error {
                    BrokerError::Rejected(_) | BrokerError::Disconnected(_) => {
                        format!("modify of {id} failed; the order is unchanged: {error}")
                    }
                    _ => format!("modify of {id} has an unknown outcome; reconcile: {error}"),
                };
                self.record(Entry::Anomaly { detail }, false)?;
                Err(anyhow!(error))
            }
        }
    }

    pub fn cancel(&mut self, id: &ClientOrderId) -> Result<()> {
        self.refresh()?;
        if let Some(order) = self.broker.find_order(id)?
            && order.status.is_terminal()
        {
            return Err(refused(Refusal::new(
                RefusalCode::NotWorking,
                format!("order {id} is already {}", order.status),
            )));
        }
        self.record(
            Entry::CancelIntent {
                client_id: id.clone(),
            },
            true,
        )?;
        self.broker.cancel(id)?;
        if let Ok(Some(order)) = self.broker.find_order(id) {
            self.record(Entry::Order { order }, false)?;
        }
        Ok(())
    }

    /// Cancels every working order in the account, not only ours.
    pub fn cancel_all(&mut self) -> Result<()> {
        self.refresh()?;
        self.record(Entry::CancelAll, true)?;
        self.broker.cancel_all()?;
        for order in self.broker.open_orders().unwrap_or_default() {
            self.record(Entry::Order { order }, false)?;
        }
        Ok(())
    }

    /// Engages the kill switch for every process, then cancels everything working. The halt is
    /// in place even if the cancel fails.
    pub fn halt(&mut self, reason: &str) -> Result<()> {
        self.state.halt(reason, self.now())?;
        self.record(
            Entry::Halt {
                reason: reason.to_string(),
            },
            true,
        )?;
        self.cancel_all()
            .context("halted, but cancelling working orders failed; cancel them by hand")
    }

    pub fn resume(&mut self) -> Result<bool> {
        let lifted = self.state.resume()?;
        if lifted {
            self.record(Entry::Resume, true)?;
        }
        Ok(lifted)
    }

    // ----- events and reconciliation ---------------------------------------------------------

    /// Takes one event from the broker, journals it, and folds it into the book. A reconnect
    /// triggers a reconciliation: anything may have happened while the line was down.
    pub fn pump(&mut self, timeout: Duration) -> Result<Option<BrokerEvent>> {
        let Some(event) = self.broker.next_event(timeout)? else {
            return Ok(None);
        };
        match &event {
            BrokerEvent::Order { order } => self.record(
                Entry::Order {
                    order: order.clone(),
                },
                false,
            )?,
            BrokerEvent::Fill { fill } => {
                if !self.book.knows_exec(&fill.exec_id) {
                    self.record(Entry::Fill { fill: fill.clone() }, false)?;
                }
            }
            BrokerEvent::Account { .. } => {}
            BrokerEvent::Disconnected { reason } => self.record(
                Entry::Note {
                    message: format!("disconnected: {reason}"),
                },
                false,
            )?,
            BrokerEvent::Reconnected => {
                self.record(
                    Entry::Note {
                        message: "reconnected; reconciling".into(),
                    },
                    false,
                )?;
                self.reconcile()?;
            }
            BrokerEvent::Notice { message } => self.record(
                Entry::Note {
                    message: message.clone(),
                },
                false,
            )?,
        }
        Ok(Some(event))
    }

    /// Drains every queued event.
    pub fn drain(&mut self) -> Result<Vec<BrokerEvent>> {
        let mut events = Vec::new();
        while let Some(event) = self.pump(Duration::ZERO)? {
            events.push(event);
        }
        Ok(events)
    }

    /// Brings the book in line with the broker, which is the source of truth:
    ///
    /// - orders of unknown fate are looked up by client id — adopted if found, declared
    ///   not-placed if still missing after the grace window;
    /// - our other working orders are refreshed;
    /// - fills are pulled and de-duplicated;
    /// - positions are checked against the last baseline plus the fills since. A position that
    ///   moved without a fill to explain it means something else is trading this account, or our
    ///   records are wrong. Either way trading halts until a person looks.
    pub fn reconcile(&mut self) -> Result<Reconciliation> {
        self.refresh()?;
        let mut report = Reconciliation::default();
        let now = self.now();

        let unresolved: Vec<(ClientOrderId, Timestamp)> = self
            .book
            .unresolved()
            .map(|t| (t.order.client_id.clone(), t.sent_at))
            .collect();
        for (id, sent_at) in unresolved {
            match self.broker.find_order(&id)? {
                Some(order) => {
                    report.adopted.push(id);
                    self.record(Entry::Order { order }, false)?;
                }
                None if now.duration_since(sent_at) >= self.grace => {
                    let tracked = self.book.get(&id).expect("unresolved order is tracked");
                    let mut order = tracked.order.clone();
                    order.status = OrderStatus::NotPlaced;
                    order.reason = Some("the broker has no record of it".into());
                    order.updated_at = now;
                    report.not_placed.push(id);
                    self.record(Entry::Order { order }, false)?;
                }
                None => report.still_unknown.push(id),
            }
        }

        let live: Vec<(ClientOrderId, OrderStatus)> = self
            .book
            .live()
            .filter(|t| {
                !matches!(
                    t.order.status,
                    OrderStatus::Unknown | OrderStatus::PendingSubmit
                )
            })
            .map(|t| (t.order.client_id.clone(), t.order.status))
            .collect();
        for (id, was) in live {
            match self.broker.find_order(&id)? {
                Some(order) => {
                    if order.status != was {
                        report.changed.push((id, was, order.status));
                    }
                    self.record(Entry::Order { order }, false)?;
                }
                None => {
                    let detail = format!("the broker no longer knows working order {id}");
                    self.record(
                        Entry::Anomaly {
                            detail: detail.clone(),
                        },
                        false,
                    )?;
                    report.anomalies.push(detail);
                }
            }
        }

        for order in self.broker.open_orders()? {
            if self.book.get(&order.client_id).is_none() {
                report.external.push(order);
            }
        }

        self.check_positions(now, &mut report)?;
        Ok(report)
    }

    fn check_positions(&mut self, now: Timestamp, report: &mut Reconciliation) -> Result<()> {
        // Positions, then fills, then positions again: if the account moved in between, the two
        // reads do not describe the same moment and comparing them would invent a mismatch.
        let since = now
            .saturating_sub(SignedDuration::from_hours(24 * 7))
            .unwrap_or(now);
        let mut settled = None;
        for _ in 0..3 {
            let before = self.broker.positions()?;
            let fills = self.broker.fills(since)?;
            let after = self.broker.positions()?;
            if before == after {
                settled = Some((after, fills));
                break;
            }
        }
        let Some((positions, fills)) = settled else {
            bail!("positions kept changing during reconciliation; try again");
        };

        let baseline = self.book.baseline().cloned();
        for fill in &fills {
            if !self.book.knows_exec(&fill.exec_id) {
                report.new_fills += 1;
                self.record(Entry::Fill { fill: fill.clone() }, false)?;
            }
        }

        if let Some((base, counted)) = baseline {
            let mut expected: BTreeMap<String, f64> = base
                .iter()
                .map(|p| (p.instrument.symbol.clone(), p.qty))
                .collect();
            for fill in fills.iter().filter(|f| !counted.contains(&f.exec_id)) {
                *expected.entry(fill.instrument.symbol.clone()).or_default() +=
                    fill.side.sign() * fill.qty;
            }
            let actual: BTreeMap<String, f64> = positions
                .iter()
                .map(|p| (p.instrument.symbol.clone(), p.qty))
                .collect();
            let symbols: HashSet<&String> = expected.keys().chain(actual.keys()).collect();
            let mut symbols: Vec<&String> = symbols.into_iter().collect();
            symbols.sort();
            for symbol in symbols {
                let e = expected.get(symbol).copied().unwrap_or(0.0);
                let a = actual.get(symbol).copied().unwrap_or(0.0);
                if (e - a).abs() > 1e-6 {
                    report.mismatches.push(Mismatch {
                        symbol: symbol.clone(),
                        expected: e,
                        actual: a,
                    });
                }
            }
        }

        if !report.mismatches.is_empty() {
            let detail = report
                .mismatches
                .iter()
                .map(|m| format!("{} expected {} actual {}", m.symbol, m.expected, m.actual))
                .collect::<Vec<_>>()
                .join("; ");
            let reason = format!("position mismatch at reconciliation: {detail}");
            self.state.halt(&reason, now)?;
            self.record(Entry::Halt { reason }, true)?;
            report.halted = true;
        }
        self.record(
            Entry::Positions {
                positions,
                exec_ids: fills.into_iter().map(|f| f.exec_id).collect(),
            },
            false,
        )?;
        Ok(())
    }
}

/// The refusal inside an error from the desk, if that is what it was.
pub fn refusal_of(error: &anyhow::Error) -> Option<&Refusal> {
    error.chain().find_map(|e| e.downcast_ref::<Refusal>())
}

/// The broker error inside an error from the desk, if that is what it was.
pub fn broker_error_of(error: &anyhow::Error) -> Option<&BrokerError> {
    error.chain().find_map(|e| e.downcast_ref::<BrokerError>())
}
