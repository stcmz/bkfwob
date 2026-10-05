//! The `bkfwob` command line.
//!
//! Tokens first, as in mdfwob and qtfwob: an optional `CONFIG.toml` first, then case-sensitive
//! tokens in any order. Named flags are kept for connection overrides and for `--yes`, which is a
//! safety switch rather than a parameter.

use std::io::{BufRead, IsTerminal, Write};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bkfwob_core::broker::{Broker, BrokerEvent};
use bkfwob_core::clock::EXCHANGE_TZ;
use bkfwob_core::desk::{Preview, Reconciliation};
use bkfwob_core::journal::{Entry, Journal};
use bkfwob_core::tokens::{OrderTokens, is_symbol};
use bkfwob_core::types::{ClientOrderId, Mode, Order, OrderChange, Side};
use clap::{Args, Parser, Subcommand};
use jiff::civil::{Date, DateTime};
use jiff::tz::TimeZone;
use jiff::{SignedDuration, Timestamp};

use crate::config::{BrokerKind, Config, Source};
use crate::output::{self, Format, Rows, money, num};
use crate::session::{Session, Target};

pub const CLI_AFTER_HELP: &str = "\
Common tokens (any command, any order, case-sensitive):
  CONFIG.toml   first argument only; else BKFWOB_CONFIG, else ~/.bkfwob/config.toml
  broker        ibkr | fake            overrides [broker] kind
  mode          paper | live           must match the config: it confirms, never switches
  format        table | csv | md | jsonl

Symbols are upper case (AAPL, BRK.B); every keyword is lower case.

Examples:
  bkfwob status
  bkfwob account fake
  bkfwob buy AAPL 10 lmt:185.50 gtc
  bkfwob buy AAPL usd:2000 preview
  bkfwob orders all jsonl
  bkfwob halt fat finger on NVDA

Exit codes: 0 ok, 1 error, 2 usage, 3 refused by a guardrail (nothing sent),
4 outcome unknown (the order may exist; run `bkfwob reconcile`).";

pub const ORDER_AFTER_HELP: &str = "\
Order tokens (any order):
  SYMBOL        AAPL, BRK.B
  QTY           100 (shares) | 0.5 (fractional, where supported) | usd:5000 (cash value)
  KIND          mkt (default) | lmt:P | stp:P | stp-lmt:STOP,LIMIT | trail:AMOUNT | trail:PCT%
  TIF           day (default) | gtc | ioc
  SESSION       rth (default) | eth
  tag:NAME      a label kept with the order
  preview       run the guardrails and show the verdict; send nothing

Orders default to regular hours even though mdfwob and qtfwob default their data to extended
hours: outside RTH the book is thin, and that has to be asked for with `eth`. Live orders ask
for confirmation unless --yes.

Examples:
  bkfwob buy AAPL 10
  bkfwob buy AAPL 10 lmt:185.50 gtc
  bkfwob sell MSFT 5 stp:400 gtc tag:stop
  bkfwob sell NVDA 3 trail:2% gtc
  bkfwob buy SPY usd:1000 lmt:600 eth
  bkfwob buy AAPL 10 preview fake";

pub const ORDERS_AFTER_HELP: &str = "\
Tokens:
  open | done | all   working orders at the broker (default); our finished orders; both
  SYMBOL...           only these symbols
  START..END          by last update; dates are New York, either side optional

Examples:
  bkfwob orders
  bkfwob orders all AAPL
  bkfwob orders done 2026-10-01.. csv";

pub const FILLS_AFTER_HELP: &str = "\
Tokens:
  SYMBOL...           only these symbols
  START..END          default: today (New York)

Examples:
  bkfwob fills
  bkfwob fills AAPL 2026-10-01..2026-10-03 jsonl";

pub const MODIFY_AFTER_HELP: &str = "\
Tokens: ORDER_ID, then a new QTY and/or a new price of the same KIND.
A modify cannot change the kind of order; cancel and place a new one for that.

Examples:
  bkfwob modify bk-20261005-0007 lmt:184.90
  bkfwob modify bk-20261005-0007 20 stp:395";

pub const CANCEL_AFTER_HELP: &str = "\
Tokens: ORDER_ID... | all [SYMBOL...]
  all          every working order in the account, including ones bkfwob did not place
  all AAPL     every working order in AAPL
Cancels are allowed while halted.

Examples:
  bkfwob cancel bk-20261005-0007
  bkfwob cancel all
  bkfwob cancel all NVDA";

pub const FLATTEN_AFTER_HELP: &str = "\
Tokens: all | SYMBOL...
Cancels every working order in each symbol, then closes the position at market. Allowed while
halted and exempt from the size limits: it can only move the account toward flat. Asks for
confirmation unless --yes.

Examples:
  bkfwob flatten AAPL
  bkfwob flatten all --yes";

pub const WATCH_AFTER_HELP: &str = "\
Tokens: orders | fills | account (any combination; default all), and a format.
Streams broker events, journaling each, until interrupted. A reconnect triggers reconciliation.

Examples:
  bkfwob watch
  bkfwob watch fills jsonl";

pub const MCP_AFTER_HELP: &str = "\
Register with an MCP client, e.g. Claude Code:

  { \"mcpServers\": { \"bkfwob\": { \"command\": \"bkfwob\", \"args\": [\"mcp\"] } } }

The broker and mode are fixed when the server starts (config, tokens, environment); no tool can
select a live account. A live account also needs `[mcp] allow_live = true`. Orders go through
bkfwob_preview_order, which returns a single-use token that bkfwob_place_order requires.
Agents can halt trading but not resume it. JSON-RPC on stdin/stdout; messages on stderr.

Examples:
  bkfwob mcp
  bkfwob mcp --read-only
  bkfwob mcp fake";

pub const HALT_AFTER_HELP: &str = "\
Everything after the command is the reason. Writes the HALT file, which stops every bkfwob
process from placing or modifying orders, then cancels every working order. If the broker is
unreachable the HALT file is still written. `bkfwob resume` lifts it.

Examples:
  bkfwob halt
  bkfwob halt fat finger on NVDA";

#[derive(Debug, Parser)]
#[command(
    name = "bkfwob",
    bin_name = "bkfwob",
    version,
    about = "Broker integration layer for the fwob toolchain: account, orders and guardrails",
    after_help = CLI_AFTER_HELP
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
    /// State directory: journals, the client-id sequence, the HALT file. Default BKFWOB_STATE or
    /// ~/.bkfwob.
    #[arg(long, global = true)]
    pub state: Option<PathBuf>,
    /// Answer yes to confirmations (live orders, flatten, resuming a live account).
    #[arg(long, global = true)]
    pub yes: bool,
    /// IB Gateway/TWS host. Overrides [ibkr] host.
    #[arg(long, global = true)]
    pub host: Option<String>,
    /// IB Gateway/TWS port. Overrides [ibkr] port (default 4002 paper, 4001 live).
    #[arg(long, global = true)]
    pub port: Option<u16>,
    /// IB API client id. Overrides [ibkr] client_id (default 17).
    #[arg(long, global = true)]
    pub client_id: Option<i32>,
    /// Account id, for logins with several. Overrides [ibkr] account.
    #[arg(long, global = true)]
    pub account: Option<String>,
}

#[derive(Debug, Args)]
pub struct Items {
    #[arg(value_name = "ITEM", num_args = 0..)]
    pub items: Vec<String>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Connection, mode, halt state, and today's usage against the limits.
    Status(Items),
    /// Cash, buying power and equity.
    Account(Items),
    /// Holdings.
    Positions(Items),
    /// Working and finished orders.
    #[command(after_help = ORDERS_AFTER_HELP)]
    Orders(Items),
    /// Executions.
    #[command(after_help = FILLS_AFTER_HELP)]
    Fills(Items),
    /// Quote snapshots.
    Quote(Items),
    /// Place a buy order through the guardrails.
    #[command(after_help = ORDER_AFTER_HELP)]
    Buy(Items),
    /// Place a sell order through the guardrails.
    #[command(after_help = ORDER_AFTER_HELP)]
    Sell(Items),
    /// Change a working order's quantity or price.
    #[command(after_help = MODIFY_AFTER_HELP)]
    Modify(Items),
    /// Cancel working orders.
    #[command(after_help = CANCEL_AFTER_HELP)]
    Cancel(Items),
    /// Cancel working orders and close positions at market.
    #[command(after_help = FLATTEN_AFTER_HELP)]
    Flatten(Items),
    /// Resolve orders of unknown fate, refresh order states, and check positions against fills.
    Reconcile(Items),
    /// Stream broker events.
    #[command(after_help = WATCH_AFTER_HELP)]
    Watch(Items),
    /// Kill switch: stop all order entry and cancel everything working.
    #[command(after_help = HALT_AFTER_HELP)]
    Halt(Items),
    /// Lift the kill switch.
    Resume(Items),
    /// Serve the account and the guardrailed order path to an LLM agent over MCP (stdio).
    #[cfg(feature = "mcp")]
    #[command(after_help = MCP_AFTER_HELP)]
    Mcp(McpArgs),
}

#[cfg(feature = "mcp")]
#[derive(Debug, Args)]
pub struct McpArgs {
    /// Expose only the read tools: no preview, place, modify, cancel or halt.
    #[arg(long)]
    pub read_only: bool,
    /// Optional CONFIG.toml, then a broker and mode token.
    #[arg(value_name = "ITEM", num_args = 0..)]
    pub items: Vec<String>,
}

/// Tokens every command understands.
struct Common {
    config: Option<PathBuf>,
    kind: Option<BrokerKind>,
    mode: Option<Mode>,
    format: Format,
    rest: Vec<String>,
}

fn set<T>(slot: &mut Option<T>, value: T, what: &str) -> Result<()> {
    if slot.is_some() {
        bail!("more than one {what} token given");
    }
    *slot = Some(value);
    Ok(())
}

fn common(items: &[String]) -> Result<Common> {
    let (config, items) = match items.split_first() {
        Some((first, rest)) if first.to_ascii_lowercase().ends_with(".toml") => {
            (Some(PathBuf::from(first)), rest)
        }
        _ => (None, items),
    };
    let mut kind = None;
    let mut mode = None;
    let mut format = None;
    let mut rest = Vec::new();
    for item in items {
        if item.to_ascii_lowercase().ends_with(".toml") {
            bail!("the config file must be the first argument");
        } else if let Some(k) = BrokerKind::parse(item) {
            set(&mut kind, k, "broker")?;
        } else if let Some(f) = Format::parse(item) {
            set(&mut format, f, "format")?;
        } else if item == "paper" {
            set(&mut mode, Mode::Paper, "mode")?;
        } else if item == "live" {
            set(&mut mode, Mode::Live, "mode")?;
        } else {
            rest.push(item.clone());
        }
    }
    Ok(Common {
        config,
        kind,
        mode,
        format: format.unwrap_or_default(),
        rest,
    })
}

fn exchange_tz() -> TimeZone {
    TimeZone::get(EXCHANGE_TZ).unwrap_or(TimeZone::UTC)
}

/// One side of `START..END`. A bare date is a New York day: its start, or for the end side the
/// start of the next day, so `..2026-10-03` includes all of the 3rd.
fn parse_bound(text: &str, end: bool) -> Result<Timestamp> {
    if let Ok(t) = text.parse::<Timestamp>() {
        return Ok(t);
    }
    let tz = exchange_tz();
    // A time part first: jiff's date parser would accept `2026-10-05T09:30` and drop the time.
    if text.contains(['T', ' '])
        && let Ok(dt) = text.parse::<DateTime>()
    {
        return Ok(dt.to_zoned(tz)?.timestamp());
    }
    if let Ok(date) = text.parse::<Date>() {
        let date = if end { date.tomorrow()? } else { date };
        return Ok(date.to_zoned(tz)?.timestamp());
    }
    bail!("`{text}` is not a date (2026-10-05), a local time (2026-10-05T09:30), or an instant")
}

type Range = (Option<Timestamp>, Option<Timestamp>);

fn parse_range(token: &str) -> Option<Result<Range>> {
    let (start, end) = token.split_once("..")?;
    let side = |text: &str, end: bool| {
        if text.is_empty() {
            Ok(None)
        } else {
            parse_bound(text, end).map(Some)
        }
    };
    Some(side(start, false).and_then(|s| side(end, true).map(|e| (s, e))))
}

fn in_range(t: Timestamp, range: &Range) -> bool {
    range.0.is_none_or(|s| t >= s) && range.1.is_none_or(|e| t < e)
}

pub(crate) fn today_start() -> Timestamp {
    let tz = exchange_tz();
    Timestamp::now()
        .to_zoned(tz.clone())
        .date()
        .to_zoned(tz)
        .map(|z| z.timestamp())
        .unwrap_or_else(|_| Timestamp::now())
}

/// Symbols and an optional range; anything else is an error naming the token.
fn symbols_and_range(rest: &[String], what: &str) -> Result<(Vec<String>, Range)> {
    let mut symbols = Vec::new();
    let mut range = None;
    for token in rest {
        if let Some(r) = parse_range(token) {
            set(&mut range, r?, "time range")?;
        } else if is_symbol(token) {
            symbols.push(token.clone());
        } else {
            bail!("`{token}` is not a token {what} understands; see `bkfwob {what} --help`");
        }
    }
    Ok((symbols, range.unwrap_or((None, None))))
}

fn wanted(symbols: &[String], symbol: &str) -> bool {
    symbols.is_empty() || symbols.iter().any(|s| s == symbol)
}

fn confirm(question: &str, yes: bool) -> Result<()> {
    if yes {
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        bail!("{question} needs confirmation; pass --yes to confirm non-interactively");
    }
    eprint!("{question} [y/N] ");
    std::io::stderr().flush()?;
    let mut answer = String::new();
    std::io::stdin().lock().read_line(&mut answer)?;
    if matches!(answer.trim(), "y" | "Y" | "yes") {
        Ok(())
    } else {
        bail!("cancelled");
    }
}

impl Cli {
    fn target(&self, common: &Common) -> Result<(Target, Source)> {
        let (mut config, source) = Config::load(common.config.as_deref())?;
        if let Some(host) = &self.host {
            config.ibkr.host = host.clone();
        }
        if let Some(port) = self.port {
            config.ibkr.port = Some(port);
        }
        if let Some(id) = self.client_id {
            config.ibkr.client_id = id;
        }
        if let Some(account) = &self.account {
            config.ibkr.account = Some(account.clone());
        }
        let target = Target::resolve(config, common.kind, common.mode, self.state.as_deref())?;
        Ok((target, source))
    }

    pub fn run(self) -> Result<()> {
        let items = match &self.command {
            Command::Status(i)
            | Command::Account(i)
            | Command::Positions(i)
            | Command::Orders(i)
            | Command::Fills(i)
            | Command::Quote(i)
            | Command::Buy(i)
            | Command::Sell(i)
            | Command::Modify(i)
            | Command::Cancel(i)
            | Command::Flatten(i)
            | Command::Reconcile(i)
            | Command::Watch(i)
            | Command::Halt(i)
            | Command::Resume(i) => &i.items,
            #[cfg(feature = "mcp")]
            Command::Mcp(m) => &m.items,
        };
        let common = common(items)?;
        let (target, source) = self.target(&common)?;
        let format = common.format;
        let rest = &common.rest;
        // Before stdout is locked: for the MCP server, stdout is the transport.
        #[cfg(feature = "mcp")]
        if let Command::Mcp(m) = &self.command {
            no_tokens(rest, "mcp")?;
            return crate::mcp::serve(target, m.read_only);
        }
        let out = &mut std::io::stdout().lock();
        match &self.command {
            Command::Status(_) => status(&target, &source, rest, format, out),
            Command::Halt(_) => halt(&target, rest, format, out),
            Command::Resume(_) => resume(&target, rest, self.yes, out),
            command => {
                let mut session = target.open()?;
                let result = match command {
                    Command::Account(_) => no_tokens(rest, "account")
                        .and_then(|()| output::account(&session.account).print(format, out)),
                    Command::Positions(_) => positions(&mut session, rest, format, out),
                    Command::Orders(_) => orders(&mut session, rest, format, out),
                    Command::Fills(_) => fills(&mut session, rest, format, out),
                    Command::Quote(_) => quote(&mut session, rest, format, out),
                    Command::Buy(_) => place(
                        &mut session,
                        Side::Buy,
                        rest,
                        format,
                        self.yes,
                        &target,
                        out,
                    ),
                    Command::Sell(_) => place(
                        &mut session,
                        Side::Sell,
                        rest,
                        format,
                        self.yes,
                        &target,
                        out,
                    ),
                    Command::Modify(_) => modify(&mut session, rest, format, out),
                    Command::Cancel(_) => cancel(&mut session, rest, format, out),
                    Command::Flatten(_) => flatten(&mut session, rest, format, self.yes, out),
                    Command::Reconcile(_) => no_tokens(rest, "reconcile")
                        .and_then(|()| reconcile(&mut session, format, out)),
                    Command::Watch(_) => watch(&mut session, rest, format, out),
                    _ => unreachable!("handled above"),
                };
                let closed = session.close();
                result?;
                closed
            }
        }
    }
}

fn no_tokens(rest: &[String], what: &str) -> Result<()> {
    match rest.first() {
        Some(token) => bail!("`{token}` is not a token {what} understands"),
        None => Ok(()),
    }
}

fn status(
    target: &Target,
    source: &Source,
    rest: &[String],
    format: Format,
    out: &mut impl Write,
) -> Result<()> {
    no_tokens(rest, "status")?;
    let state = target.state()?;
    let mut pairs = vec![
        (
            "config",
            match source {
                Source::File(p) => p.display().to_string(),
                Source::Defaults => "(defaults)".into(),
            },
        ),
        ("broker", target.kind.to_string()),
        ("mode", target.mode.to_string()),
        ("state", state.scope_dir().display().to_string()),
        (
            "halted",
            match state.halted() {
                Some(h) => format!("yes: {} (since {})", h.reason, output::time(h.at)),
                None => "no".into(),
            },
        ),
    ];
    let limits = &target.config.limits;
    match target.open() {
        Ok(mut session) => {
            let now = Timestamp::now();
            let book = session.desk.book();
            let unresolved = book.unresolved().count();
            let today = book.notional_today(now);
            let recent = book.submissions_within(now, SignedDuration::from_secs(60));
            pairs.push(("account", session.account.id.clone()));
            pairs.push(("equity", money(session.account.equity)));
            pairs.push((
                "working orders",
                session.desk.open_orders()?.len().to_string(),
            ));
            pairs.push(("unknown-fate orders", unresolved.to_string()));
            pairs.push((
                "sent today",
                format!("{} of {}", money(today), money(limits.max_daily_notional)),
            ));
            pairs.push((
                "last minute",
                format!("{recent} of {} orders", limits.orders_per_minute),
            ));
            session.close()?;
        }
        Err(error) => pairs.push(("connection", format!("failed: {error:#}"))),
    }
    pairs.push(("max order", money(limits.max_order_notional)));
    pairs.push(("max position", money(limits.max_position_value)));
    let json: serde_json::Map<String, serde_json::Value> = pairs
        .iter()
        .map(|(k, v)| (k.replace(' ', "_"), serde_json::Value::String(v.clone())))
        .collect();
    output::record(pairs, json).print(format, out)
}

fn positions(
    session: &mut Session,
    rest: &[String],
    format: Format,
    out: &mut impl Write,
) -> Result<()> {
    let (symbols, range) = symbols_and_range(rest, "positions")?;
    if range != (None, None) {
        bail!("positions are a snapshot; a time range does not apply");
    }
    let list: Vec<_> = session
        .desk
        .positions()?
        .into_iter()
        .filter(|p| wanted(&symbols, &p.instrument.symbol))
        .collect();
    output::positions(&list).print(format, out)
}

fn orders(
    session: &mut Session,
    rest: &[String],
    format: Format,
    out: &mut impl Write,
) -> Result<()> {
    let mut which = None;
    let mut others = Vec::new();
    for token in rest {
        match token.as_str() {
            "open" | "done" | "all" => set(&mut which, token.clone(), "open/done/all")?,
            _ => others.push(token.clone()),
        }
    }
    let (symbols, range) = symbols_and_range(&others, "orders")?;
    let which = which.unwrap_or_else(|| "open".into());
    let mut list: Vec<Order> = if which == "open" {
        session.desk.open_orders()?
    } else {
        session.desk.reconcile_orders()?;
        let mut ours: Vec<Order> = session
            .desk
            .book()
            .orders()
            .map(|t| t.order.clone())
            .filter(|o| which == "all" || o.status.is_terminal())
            .collect();
        if which == "all" {
            for o in session.desk.open_orders()? {
                if !ours.iter().any(|m| m.client_id == o.client_id) {
                    ours.push(o);
                }
            }
        }
        ours
    };
    list.retain(|o| wanted(&symbols, &o.spec.instrument.symbol) && in_range(o.updated_at, &range));
    list.sort_by_key(|o| o.updated_at);
    output::orders(&list).print(format, out)
}

fn fills(
    session: &mut Session,
    rest: &[String],
    format: Format,
    out: &mut impl Write,
) -> Result<()> {
    let (symbols, range) = symbols_and_range(rest, "fills")?;
    let since = range.0.unwrap_or_else(today_start);
    let list: Vec<_> = session
        .desk
        .fills(since)?
        .into_iter()
        .filter(|f| {
            wanted(&symbols, &f.instrument.symbol) && in_range(f.time, &(Some(since), range.1))
        })
        .collect();
    output::fills(&list).print(format, out)
}

fn quote(
    session: &mut Session,
    rest: &[String],
    format: Format,
    out: &mut impl Write,
) -> Result<()> {
    let (symbols, range) = symbols_and_range(rest, "quote")?;
    if symbols.is_empty() || range != (None, None) {
        bail!("quote takes one or more SYMBOLs");
    }
    let mut list = Vec::new();
    for symbol in symbols {
        list.push(
            session
                .desk
                .quote(&bkfwob_core::types::Instrument::stock(symbol))?,
        );
    }
    output::quotes(&list).print(format, out)
}

fn preview_rows(p: &Preview) -> Rows {
    let mut pairs = vec![("order", p.spec.summary())];
    match (&p.assessment, &p.refusal) {
        (_, Some(r)) => {
            pairs.push(("verdict", format!("refused ({})", r.code)));
            pairs.push(("detail", r.detail.clone()));
        }
        (Some(a), None) => {
            pairs.push(("verdict", "ok".into()));
            pairs.push(("reference price", num(a.reference_price)));
            pairs.push(("est. shares", num((a.est_shares * 1e4).round() / 1e4)));
            pairs.push(("est. notional", money(a.est_notional)));
        }
        (None, None) => {}
    }
    output::record(pairs, p)
}

#[allow(clippy::too_many_arguments)]
fn place(
    session: &mut Session,
    side: Side,
    rest: &[String],
    format: Format,
    yes: bool,
    target: &Target,
    out: &mut impl Write,
) -> Result<()> {
    let (tokens, leftover) = OrderTokens::parse(rest).map_err(anyhow::Error::msg)?;
    let mut preview = false;
    for token in leftover {
        if token == "preview" {
            preview = true;
        } else {
            bail!("`{token}` is not an order token; see `bkfwob {side} --help`");
        }
    }
    let spec = tokens.into_spec(side).map_err(anyhow::Error::msg)?;
    if preview {
        let verdict = session.desk.preview(&spec)?;
        return preview_rows(&verdict).print(format, out);
    }
    if target.mode == Mode::Live {
        confirm(
            &format!(
                "Send LIVE order to {} account {}: {}?",
                target.kind,
                session.account.id,
                spec.summary()
            ),
            yes,
        )?;
    }
    let order = session.desk.place(&spec)?;
    output::orders([&order]).print(format, out)
}

fn modify(
    session: &mut Session,
    rest: &[String],
    format: Format,
    out: &mut impl Write,
) -> Result<()> {
    let (tokens, leftover) = OrderTokens::parse(rest).map_err(anyhow::Error::msg)?;
    if tokens.symbol.is_some()
        || tokens.tif.is_some()
        || tokens.session.is_some()
        || tokens.tag.is_some()
    {
        bail!("modify changes only quantity and price; cancel and re-place for anything else");
    }
    let [id] = leftover.as_slice() else {
        bail!("modify takes exactly one ORDER_ID");
    };
    let change = OrderChange {
        qty: tokens.qty,
        kind: tokens.kind,
    };
    let order = session
        .desk
        .modify(&ClientOrderId::new(id.clone()), &change)?;
    output::orders([&order]).print(format, out)
}

fn cancel(
    session: &mut Session,
    rest: &[String],
    format: Format,
    out: &mut impl Write,
) -> Result<()> {
    let all = rest.iter().any(|t| t == "all");
    let others: Vec<&String> = rest.iter().filter(|t| *t != "all").collect();
    if all {
        let symbols: Vec<String> = others.iter().map(|s| s.to_string()).collect();
        if let Some(bad) = symbols.iter().find(|s| !is_symbol(s)) {
            bail!("`cancel all` takes SYMBOLs after it, not `{bad}`");
        }
        if symbols.is_empty() {
            session.desk.cancel_all()?;
        } else {
            for order in session.desk.open_orders()? {
                if wanted(&symbols, &order.spec.instrument.symbol) {
                    session.desk.cancel(&order.client_id)?;
                }
            }
        }
    } else {
        if others.is_empty() {
            bail!("cancel takes ORDER_IDs, or `all`");
        }
        for id in others {
            session.desk.cancel(&ClientOrderId::new(id.clone()))?;
        }
    }
    session.desk.drain()?;
    let still: Vec<Order> = session.desk.open_orders()?;
    writeln!(out, "{} order(s) still working", still.len())?;
    if !still.is_empty() {
        output::orders(&still).print(format, out)?;
    }
    Ok(())
}

fn flatten(
    session: &mut Session,
    rest: &[String],
    format: Format,
    yes: bool,
    out: &mut impl Write,
) -> Result<()> {
    let all = rest.iter().any(|t| t == "all");
    let mut symbols: Vec<String> = rest.iter().filter(|t| *t != "all").cloned().collect();
    if let Some(bad) = symbols.iter().find(|s| !is_symbol(s)) {
        bail!("flatten takes `all` or SYMBOLs, not `{bad}`");
    }
    if all {
        if !symbols.is_empty() {
            bail!("give `all` or SYMBOLs, not both");
        }
        symbols = session
            .desk
            .positions()?
            .into_iter()
            .map(|p| p.instrument.symbol)
            .collect();
    } else if symbols.is_empty() {
        bail!("flatten takes `all` or SYMBOLs");
    }
    if symbols.is_empty() {
        writeln!(out, "no positions")?;
        return Ok(());
    }
    confirm(
        &format!(
            "Close {} at market in {} account {}?",
            symbols.join(", "),
            session.account.mode,
            session.account.id
        ),
        yes,
    )?;
    let mut sent = Vec::new();
    for symbol in &symbols {
        if let Some(order) = session.desk.flatten(symbol)? {
            sent.push(order);
        }
    }
    output::orders(&sent).print(format, out)
}

fn reconcile_rows(r: &Reconciliation) -> Rows {
    let ids = |v: &[ClientOrderId]| {
        if v.is_empty() {
            "-".to_string()
        } else {
            v.iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(" ")
        }
    };
    let pairs = vec![
        ("adopted", ids(&r.adopted)),
        ("not placed", ids(&r.not_placed)),
        ("still unknown", ids(&r.still_unknown)),
        (
            "status changes",
            if r.changed.is_empty() {
                "-".into()
            } else {
                r.changed
                    .iter()
                    .map(|(id, a, b)| format!("{id} {a}->{b}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            },
        ),
        ("new fills", r.new_fills.to_string()),
        (
            "external orders",
            if r.external.is_empty() {
                "-".into()
            } else {
                r.external
                    .iter()
                    .map(|o| format!("{} {}", o.client_id, o.spec.summary()))
                    .collect::<Vec<_>>()
                    .join("; ")
            },
        ),
        (
            "mismatches",
            if r.mismatches.is_empty() {
                "-".into()
            } else {
                r.mismatches
                    .iter()
                    .map(|m| {
                        format!(
                            "{} expected {} actual {}",
                            m.symbol,
                            num(m.expected),
                            num(m.actual)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; ")
            },
        ),
        (
            "halted",
            if r.halted {
                "YES: positions moved without fills to explain them".into()
            } else {
                "no".into()
            },
        ),
    ];
    output::record(pairs, r)
}

fn reconcile(session: &mut Session, format: Format, out: &mut impl Write) -> Result<()> {
    session.desk.drain()?;
    let report = session.desk.reconcile()?;
    reconcile_rows(&report).print(format, out)
}

fn watch(
    session: &mut Session,
    rest: &[String],
    format: Format,
    out: &mut impl Write,
) -> Result<()> {
    let mut kinds = Vec::new();
    for token in rest {
        match token.as_str() {
            "orders" | "fills" | "account" => kinds.push(token.clone()),
            _ => bail!("`{token}` is not a watch token; use orders, fills or account"),
        }
    }
    let show = |kind: &str| kinds.is_empty() || kinds.iter().any(|k| k == kind);
    let offline = session.desk.broker().name() == "fake";
    loop {
        let Some(event) = session.desk.pump(Duration::from_secs(1))? else {
            if offline {
                return Ok(());
            }
            continue;
        };
        let (kind, line) = match &event {
            BrokerEvent::Order { order } => (
                "orders",
                format!(
                    "order {} {} {} filled {}",
                    order.client_id,
                    order.spec.summary(),
                    order.status,
                    num(order.filled_qty)
                ),
            ),
            BrokerEvent::Fill { fill } => (
                "fills",
                format!(
                    "fill  {} {} {} {} @ {}",
                    fill.client_id,
                    fill.side,
                    num(fill.qty),
                    fill.instrument,
                    num(fill.price)
                ),
            ),
            BrokerEvent::Account { account } => (
                "account",
                format!("account {} equity {}", account.id, money(account.equity)),
            ),
            BrokerEvent::Disconnected { reason } => ("", format!("disconnected: {reason}")),
            BrokerEvent::Reconnected => ("", "reconnected; reconciled".into()),
            BrokerEvent::Notice { message } => ("", format!("notice: {message}")),
        };
        if kind.is_empty() || show(kind) {
            if format == Format::Jsonl {
                writeln!(out, "{}", serde_json::to_string(&event)?)?;
            } else {
                writeln!(out, "{} {line}", output::time(Timestamp::now()))?;
            }
            out.flush()?;
        }
        session.save()?;
    }
}

fn halt(target: &Target, rest: &[String], format: Format, out: &mut impl Write) -> Result<()> {
    let reason = if rest.is_empty() {
        "manual halt".to_string()
    } else {
        rest.join(" ")
    };
    let cancelled = match target.open() {
        Ok(mut session) => {
            let result = session.desk.halt(&reason);
            session.close()?;
            result.map(|()| "yes".to_string())
        }
        Err(error) => {
            // No broker: still halt every process, and say plainly what did not happen.
            let state = target.state()?;
            state.halt(&reason, Timestamp::now())?;
            Journal::open(state.journal_dir())?.append(
                Timestamp::now(),
                Entry::Halt {
                    reason: reason.clone(),
                },
                true,
            )?;
            Ok(format!(
                "NO, could not reach the broker ({error:#}); cancel working orders by hand"
            ))
        }
    };
    let cancelled = match cancelled {
        Ok(c) => c,
        Err(e) => format!("NO: {e:#}"),
    };
    output::record(
        vec![
            ("halted", "yes".into()),
            ("reason", reason.clone()),
            ("working orders cancelled", cancelled.clone()),
        ],
        serde_json::json!({"halted": true, "reason": reason, "cancelled": cancelled}),
    )
    .print(format, out)
}

fn resume(target: &Target, rest: &[String], yes: bool, out: &mut impl Write) -> Result<()> {
    no_tokens(rest, "resume")?;
    let state = target.state()?;
    let Some(halt) = state.halted() else {
        writeln!(out, "not halted")?;
        return Ok(());
    };
    if target.mode == Mode::Live {
        confirm(
            &format!(
                "Resume trading on a LIVE account (halted: {})?",
                halt.reason
            ),
            yes,
        )?;
    }
    state.resume().context("lifting the halt")?;
    Journal::open(state.journal_dir())?.append(Timestamp::now(), Entry::Resume, true)?;
    writeln!(out, "resumed (was halted: {})", halt.reason)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn the_cli_is_well_formed() {
        Cli::command().debug_assert();
    }

    /// Every example in the help text must parse, so the help cannot drift from the grammar.
    #[test]
    fn every_help_example_parses() {
        let mut checked = 0;
        for text in [
            CLI_AFTER_HELP,
            ORDER_AFTER_HELP,
            ORDERS_AFTER_HELP,
            FILLS_AFTER_HELP,
            MODIFY_AFTER_HELP,
            CANCEL_AFTER_HELP,
            FLATTEN_AFTER_HELP,
            WATCH_AFTER_HELP,
            HALT_AFTER_HELP,
            #[cfg(feature = "mcp")]
            MCP_AFTER_HELP,
        ] {
            for line in text.lines().map(str::trim) {
                if !line.starts_with("bkfwob ") {
                    continue;
                }
                let cli = Cli::try_parse_from(line.split_whitespace())
                    .unwrap_or_else(|e| panic!("`{line}` does not parse: {e}"));
                let items = match &cli.command {
                    Command::Buy(i) | Command::Sell(i) => {
                        let common = common(&i.items).unwrap();
                        let (tokens, leftover) = OrderTokens::parse(&common.rest)
                            .unwrap_or_else(|e| panic!("`{line}`: {e}"));
                        assert!(
                            leftover.iter().all(|t| t == "preview"),
                            "`{line}`: unclaimed {leftover:?}"
                        );
                        tokens.into_spec(Side::Buy).unwrap();
                        continue;
                    }
                    _ => match &cli.command {
                        Command::Status(i)
                        | Command::Account(i)
                        | Command::Orders(i)
                        | Command::Fills(i)
                        | Command::Modify(i)
                        | Command::Cancel(i)
                        | Command::Flatten(i)
                        | Command::Watch(i)
                        | Command::Halt(i) => i.items.clone(),
                        _ => Vec::new(),
                    },
                };
                common(&items).unwrap_or_else(|e| panic!("`{line}`: {e}"));
                checked += 1;
            }
        }
        assert!(checked >= 15, "only {checked} examples found");
    }

    #[test]
    fn ranges() {
        let (s, e) = parse_range("2026-10-01..2026-10-03").unwrap().unwrap();
        assert_eq!(s.unwrap().to_string(), "2026-10-01T04:00:00Z");
        assert_eq!(
            e.unwrap().to_string(),
            "2026-10-04T04:00:00Z",
            "a bare end date includes the whole day"
        );
        let (s, e) = parse_range("..2026-10-03T09:30").unwrap().unwrap();
        assert!(s.is_none());
        assert_eq!(e.unwrap().to_string(), "2026-10-03T13:30:00Z");
        assert!(parse_range("x..y").unwrap().is_err());
        assert!(parse_range("AAPL").is_none());
    }

    #[test]
    fn common_tokens() {
        let items: Vec<String> = ["my.toml", "AAPL", "fake", "csv", "paper", "10"]
            .map(String::from)
            .into();
        let c = common(&items).unwrap();
        assert_eq!(c.config, Some(PathBuf::from("my.toml")));
        assert_eq!(c.kind, Some(BrokerKind::Fake));
        assert_eq!(c.mode, Some(Mode::Paper));
        assert_eq!(c.format, Format::Csv);
        assert_eq!(c.rest, ["AAPL", "10"]);
        let late: Vec<String> = ["AAPL", "my.toml"].map(String::from).into();
        assert!(common(&late).is_err());
        let twice: Vec<String> = ["csv", "md"].map(String::from).into();
        assert!(common(&twice).is_err());
    }
}
