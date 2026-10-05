//! Opening a desk: config, live gate, adapter, state scope, and the account-mode check, in that
//! order — every gate is passed before the first order could be.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bkfwob_core::broker::{Broker, BrokerError, BrokerEvent, Capabilities};
use bkfwob_core::clock;
use bkfwob_core::desk::Desk;
use bkfwob_core::fake::FakeBroker;
use bkfwob_core::guard::{LIVE_ENV, live_gate};
use bkfwob_core::state::StateDir;
use bkfwob_core::types::{
    Account, ClientOrderId, Fill, Instrument, Mode, Order, OrderRequest, OrderSpec, Position, Quote,
};
use jiff::Timestamp;

use crate::config::{BrokerKind, Config};

/// Every adapter this build has, behind one type, so the desk stays monomorphic.
pub enum AnyBroker {
    Fake(Box<FakeBroker>),
}

macro_rules! each {
    ($self:ident, $b:ident => $e:expr) => {
        match $self {
            AnyBroker::Fake($b) => $e,
        }
    };
}

impl Broker for AnyBroker {
    fn name(&self) -> &str {
        each!(self, b => b.name())
    }
    fn capabilities(&self) -> Capabilities {
        each!(self, b => b.capabilities())
    }
    fn account(&mut self) -> Result<Account, BrokerError> {
        each!(self, b => b.account())
    }
    fn positions(&mut self) -> Result<Vec<Position>, BrokerError> {
        each!(self, b => b.positions())
    }
    fn open_orders(&mut self) -> Result<Vec<Order>, BrokerError> {
        each!(self, b => b.open_orders())
    }
    fn find_order(&mut self, id: &ClientOrderId) -> Result<Option<Order>, BrokerError> {
        each!(self, b => b.find_order(id))
    }
    fn fills(&mut self, since: Timestamp) -> Result<Vec<Fill>, BrokerError> {
        each!(self, b => b.fills(since))
    }
    fn quote(&mut self, instrument: &Instrument) -> Result<Quote, BrokerError> {
        each!(self, b => b.quote(instrument))
    }
    fn place(&mut self, request: &OrderRequest) -> Result<Order, BrokerError> {
        each!(self, b => b.place(request))
    }
    fn modify(&mut self, id: &ClientOrderId, spec: &OrderSpec) -> Result<Order, BrokerError> {
        each!(self, b => b.modify(id, spec))
    }
    fn cancel(&mut self, id: &ClientOrderId) -> Result<(), BrokerError> {
        each!(self, b => b.cancel(id))
    }
    fn cancel_all(&mut self) -> Result<(), BrokerError> {
        each!(self, b => b.cancel_all())
    }
    fn next_event(&mut self, timeout: Duration) -> Result<Option<BrokerEvent>, BrokerError> {
        each!(self, b => b.next_event(timeout))
    }
}

/// What the command line selected, before anything is connected.
#[derive(Debug, Clone)]
pub struct Target {
    pub config: Config,
    pub kind: BrokerKind,
    pub mode: Mode,
    pub state_root: PathBuf,
}

impl Target {
    /// `kind` overrides the config's broker. `mode`, if given, must *agree* with the config: a
    /// `live` token confirms a live config, it cannot turn a paper config live (that takes the
    /// config file and the environment), and `paper` guards a script against a live config.
    pub fn resolve(
        config: Config,
        kind: Option<BrokerKind>,
        mode: Option<Mode>,
        state_flag: Option<&Path>,
    ) -> Result<Self> {
        let configured = config.broker.mode;
        if let Some(mode) = mode
            && mode != configured
        {
            bail!(
                "the `{mode}` token does not match the config, which says {configured}. The mode \
                 token only confirms the config; switching to live takes `mode = \"live\"` in \
                 the config and {LIVE_ENV}=1"
            );
        }
        let state_root = config.state_root(state_flag);
        Ok(Self {
            kind: kind.unwrap_or(config.broker.kind),
            mode: configured,
            state_root,
            config,
        })
    }

    pub fn scope(&self) -> String {
        format!("{}-{}", self.kind, self.mode)
    }

    pub fn state(&self) -> Result<StateDir> {
        StateDir::open(&self.state_root, &self.scope())
    }

    fn fake_path(&self) -> PathBuf {
        self.state_root.join(self.scope()).join("fake.json")
    }

    /// Connects and returns a desk whose account has been checked against the mode.
    pub fn open(&self) -> Result<Session> {
        live_gate(self.mode, std::env::var(LIVE_ENV).ok().as_deref())
            .map_err(anyhow::Error::msg)?;
        let state = self.state()?;
        let clock = clock::system();
        let broker = match self.kind {
            BrokerKind::Fake => {
                let mut fake = FakeBroker::load_or_new(
                    &self.fake_path(),
                    clock.clone(),
                    self.config.fake.cash,
                )?;
                if self.mode == Mode::Live {
                    fake.set_mode(Mode::Live, "FAKE-LIVE");
                }
                AnyBroker::Fake(Box::new(fake))
            }
            BrokerKind::Ibkr => bail!("the ibkr adapter is not built yet; use the `fake` broker"),
            BrokerKind::Alpaca => bail!("the alpaca adapter is not built yet"),
        };
        let mut desk = Desk::open(broker, state, self.config.limits.clone(), clock)?;
        let account = desk
            .verify_mode(self.mode)
            .with_context(|| format!("connecting to {}", self.kind))?;
        Ok(Session {
            desk,
            account,
            fake_path: (self.kind == BrokerKind::Fake).then(|| self.fake_path()),
        })
    }
}

pub struct Session {
    pub desk: Desk<AnyBroker>,
    pub account: Account,
    fake_path: Option<PathBuf>,
}

impl Session {
    /// Journals whatever the broker queued during the command, and persists the fake.
    pub fn close(mut self) -> Result<()> {
        self.desk.drain()?;
        self.save()
    }

    pub fn save(&self) -> Result<()> {
        if let (Some(path), AnyBroker::Fake(fake)) = (&self.fake_path, self.desk.broker()) {
            fake.save(path)?;
        }
        Ok(())
    }
}
