//! `bkfwob mcp`: the account and the guardrailed order path, served to an LLM agent over the
//! [Model Context Protocol](https://modelcontextprotocol.io) on stdio.
//!
//! Same desk as the CLI, so the same guardrails, journal and kill switch. What differs is who is
//! asking, and that shapes four rules:
//!
//! - **The mode is fixed at startup.** Broker and paper/live come from the config and the
//!   environment when the server starts; no tool argument can reach a live account. A live
//!   account additionally needs `[mcp] allow_live = true`.
//! - **Preview, then place.** `bkfwob_place_order` takes no order, only a token from
//!   `bkfwob_preview_order`: single use, valid 60 seconds, bound on the server side to exactly the
//!   order that was previewed. The agent places what it looked at, not what it re-typed. The
//!   guardrails run again at placement.
//! - **An agent can stop trading but not restart it.** `bkfwob_halt` is a tool; resume is not,
//!   and neither is flatten.
//! - **`--read-only` removes the write tools** from the listing altogether, rather than having them
//!   refuse.
//!
//! Parameters are named and typed (as in mdfwob's server), not the CLI's token grammar: a schema
//! is something a model reads, a grammar something it guesses at.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bkfwob_core::desk::refusal_of;
use bkfwob_core::tokens::is_symbol;
use bkfwob_core::types::{
    ClientOrderId, Instrument, Mode, OrderChange, OrderKind, OrderSpec, Qty, Session as Hours,
    Side, Tif, Trail,
};
use jiff::Timestamp;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Implementation, ServerCapabilities, ServerConfig};
use rmcp::transport::stdio;
use rmcp::{ServerHandler, ServiceExt, schemars, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::session::{Session, Target};

/// Write tools, removed under `--read-only`.
const WRITE_TOOLS: &[&str] = &[
    "bkfwob_preview_order",
    "bkfwob_place_order",
    "bkfwob_modify_order",
    "bkfwob_cancel_order",
    "bkfwob_cancel_all",
    "bkfwob_halt",
];

const TOKEN_TTL: Duration = Duration::from_secs(60);

const INSTRUCTIONS: &str = "\
A brokerage account behind guardrails. Start with `bkfwob_status`: it says which broker and mode \
(paper or live) this server is fixed to, whether trading is halted, and how much of today's \
limits are used. `bkfwob_limits` lists the limits themselves.

To trade: call `bkfwob_preview_order` with the order. It runs every guardrail and, if the order \
passes, returns a token. Call `bkfwob_place_order` with that token within 60 seconds to send \
exactly the previewed order. A refusal comes back as `refused` with a stable `code` (e.g. \
`order-notional`, `oversell`, `quote-stale`, `halted`) and a `detail`; do not try to work around \
a refusal by splitting or resizing orders to slip under a limit — report it.

An error saying an order's outcome is unknown means it may or may not exist at the broker; it is \
never re-sent automatically. Check `bkfwob_orders` rather than placing it again.

`bkfwob_halt` stops all order entry for every process and cancels working orders. Only a person \
can resume.";

// ----- parameters -----------------------------------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct Symbols {
    /// Only these symbols, e.g. `["AAPL"]`. Omit for all.
    #[serde(default)]
    pub symbols: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Which {
    /// Working orders at the broker.
    Open,
    /// Our finished orders (filled, cancelled, rejected...).
    Done,
    /// Both.
    All,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct OrdersParams {
    /// `open` (default), `done` or `all`.
    pub which: Option<Which>,
    #[serde(default)]
    pub symbols: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct FillsParams {
    /// RFC3339 instant to list fills from. Default: the start of today in New York.
    pub since: Option<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct QuoteParams {
    /// Symbols to quote, e.g. `["AAPL", "SPY"]`.
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SideParam {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
    Trailing,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TifParam {
    Day,
    Gtc,
    Ioc,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub(crate) struct OrderParams {
    pub side: SideParam,
    /// Upper case, e.g. `AAPL`, `BRK.B`.
    pub symbol: String,
    /// Number of shares. Give exactly one of `shares` and `notional`.
    pub shares: Option<f64>,
    /// Cash value to trade instead of a share count, where the broker supports it.
    pub notional: Option<f64>,
    /// Default `market`.
    pub order_type: Option<OrderType>,
    /// Required for `limit` and `stop_limit`.
    pub limit_price: Option<f64>,
    /// Required for `stop` and `stop_limit`.
    pub stop_price: Option<f64>,
    /// For `trailing`: distance in price units. Give this or `trail_percent`.
    pub trail_amount: Option<f64>,
    /// For `trailing`: distance in percent (2.0 = 2%).
    pub trail_percent: Option<f64>,
    /// Time in force; default `day`.
    pub tif: Option<TifParam>,
    /// Allow execution outside regular trading hours. Default false. Not for market orders.
    pub extended_hours: Option<bool>,
    /// A label kept with the order, e.g. `stop`.
    pub tag: Option<String>,
}

fn need(v: Option<f64>, what: &str) -> Result<f64, String> {
    v.ok_or_else(|| format!("{what} is required for this order type"))
}

impl OrderParams {
    pub(crate) fn to_spec(&self) -> Result<OrderSpec, String> {
        if !is_symbol(&self.symbol) {
            return Err(format!(
                "`{}` is not a symbol; symbols are upper case like AAPL",
                self.symbol
            ));
        }
        let qty = match (self.shares, self.notional) {
            (Some(s), None) => Qty::Shares(s),
            (None, Some(n)) => Qty::Notional(n),
            _ => return Err("give exactly one of `shares` and `notional`".into()),
        };
        let kind = match self.order_type.unwrap_or(OrderType::Market) {
            OrderType::Market => OrderKind::Market,
            OrderType::Limit => OrderKind::Limit {
                limit: need(self.limit_price, "limit_price")?,
            },
            OrderType::Stop => OrderKind::Stop {
                stop: need(self.stop_price, "stop_price")?,
            },
            OrderType::StopLimit => OrderKind::StopLimit {
                stop: need(self.stop_price, "stop_price")?,
                limit: need(self.limit_price, "limit_price")?,
            },
            OrderType::Trailing => OrderKind::Trailing {
                trail: match (self.trail_amount, self.trail_percent) {
                    (Some(a), None) => Trail::Amount(a),
                    (None, Some(p)) => Trail::Percent(p),
                    _ => {
                        return Err("give exactly one of `trail_amount` and `trail_percent`".into());
                    }
                },
            },
        };
        Ok(OrderSpec {
            instrument: Instrument::stock(self.symbol.clone()),
            side: match self.side {
                SideParam::Buy => Side::Buy,
                SideParam::Sell => Side::Sell,
            },
            qty,
            kind,
            tif: match self.tif.unwrap_or(TifParam::Day) {
                TifParam::Day => Tif::Day,
                TifParam::Gtc => Tif::Gtc,
                TifParam::Ioc => Tif::Ioc,
            },
            session: if self.extended_hours.unwrap_or(false) {
                Hours::Extended
            } else {
                Hours::Rth
            },
            tag: self.tag.clone(),
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct TokenParams {
    /// The token from `bkfwob_preview_order`.
    pub token: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct ModifyParams {
    /// The order's client id, e.g. `bk-20261005-0007`.
    pub order_id: String,
    /// New total share count.
    pub shares: Option<f64>,
    /// New limit price (limit and stop-limit orders).
    pub limit_price: Option<f64>,
    /// New stop price (stop and stop-limit orders).
    pub stop_price: Option<f64>,
    /// New trailing amount (trailing orders by amount).
    pub trail_amount: Option<f64>,
    /// New trailing percent (trailing orders by percent).
    pub trail_percent: Option<f64>,
}

impl ModifyParams {
    /// The change, expressed in terms of the order's current kind.
    pub(crate) fn change(&self, current: OrderKind) -> Result<OrderChange, String> {
        let wrong = |what: &str| {
            Err(format!(
                "`{what}` does not apply to a {} order",
                current.label()
            ))
        };
        let kind = match current {
            OrderKind::Market => {
                if self.limit_price.is_some() || self.stop_price.is_some() {
                    return wrong("limit_price/stop_price");
                }
                None
            }
            OrderKind::Limit { limit } => {
                if self.stop_price.is_some() {
                    return wrong("stop_price");
                }
                self.limit_price
                    .map(|l| OrderKind::Limit { limit: l })
                    .or(Some(OrderKind::Limit { limit }))
            }
            OrderKind::Stop { stop } => {
                if self.limit_price.is_some() {
                    return wrong("limit_price");
                }
                Some(OrderKind::Stop {
                    stop: self.stop_price.unwrap_or(stop),
                })
            }
            OrderKind::StopLimit { stop, limit } => Some(OrderKind::StopLimit {
                stop: self.stop_price.unwrap_or(stop),
                limit: self.limit_price.unwrap_or(limit),
            }),
            OrderKind::Trailing { trail } => Some(OrderKind::Trailing {
                trail: match (trail, self.trail_amount, self.trail_percent) {
                    (Trail::Amount(_), Some(a), None) => Trail::Amount(a),
                    (Trail::Percent(_), None, Some(p)) => Trail::Percent(p),
                    (t, None, None) => t,
                    _ => return wrong("that trailing field"),
                },
            }),
        };
        let kind = kind.filter(|k| *k != current);
        let change = OrderChange {
            qty: self.shares.map(Qty::Shares),
            kind,
        };
        if change.is_empty() {
            return Err("nothing to change".into());
        }
        Ok(change)
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct OrderIdParams {
    /// The order's client id, e.g. `bk-20261005-0007`.
    pub order_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct HaltParams {
    /// Why, in a sentence; shown to the person who has to resume.
    pub reason: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct Empty {}

// ----- server ---------------------------------------------------------------------------------

/// Previewed orders awaiting placement.
#[derive(Default)]
pub(crate) struct Tokens {
    pending: HashMap<String, (OrderSpec, Instant)>,
    issued: u64,
}

impl Tokens {
    pub(crate) fn issue(&mut self, spec: OrderSpec, now: Instant) -> String {
        use std::hash::{Hash, Hasher};
        self.pending
            .retain(|_, (_, at)| now.duration_since(*at) < TOKEN_TTL);
        self.issued += 1;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.issued.hash(&mut h);
        std::process::id().hash(&mut h);
        Timestamp::now().as_nanosecond().hash(&mut h);
        spec.summary().hash(&mut h);
        let token = format!("pv-{:016x}", h.finish());
        self.pending.insert(token.clone(), (spec, now));
        token
    }

    /// The previewed order, once: the token is spent whether or not it was still valid.
    pub(crate) fn redeem(&mut self, token: &str, now: Instant) -> Result<OrderSpec, String> {
        match self.pending.remove(token) {
            Some((spec, at)) if now.duration_since(at) < TOKEN_TTL => Ok(spec),
            Some(_) => Err("that preview token has expired; preview the order again".into()),
            None => Err(
                "unknown or already used preview token; preview the order to get a new one".into(),
            ),
        }
    }
}

struct Inner {
    session: Session,
    tokens: Tokens,
}

#[derive(Clone)]
pub struct McpServer {
    inner: Arc<Mutex<Inner>>,
    target: Arc<Target>,
    read_only: bool,
    router: ToolRouter<Self>,
}

/// Runs blocking broker work off the async executor. Failures come back as `Err(String)` (a tool
/// result the model reads), refusals as a structured `refused` result it can act on.
async fn blocking<F>(f: F) -> Result<CallToolResult, String>
where
    F: FnOnce() -> Result<Value> + Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(Ok(value)) => Ok(CallToolResult::structured(value)),
        Ok(Err(error)) => match refusal_of(&error) {
            Some(r) => Ok(CallToolResult::structured_error(
                json!({ "refused": { "code": r.code, "detail": r.detail } }),
            )),
            None => Err(format!("{error:#}")),
        },
        Err(join) => Err(format!("broker task failed: {join}")),
    }
}

fn symbol_filter(symbols: &[String], symbol: &str) -> bool {
    symbols.is_empty() || symbols.iter().any(|s| s == symbol)
}

impl McpServer {
    fn with<F>(&self, f: F) -> impl FnOnce() -> Result<Value> + Send + 'static
    where
        F: FnOnce(&mut Inner, &Target) -> Result<Value> + Send + 'static,
    {
        let inner = self.inner.clone();
        let target = self.target.clone();
        move || {
            let mut guard = inner
                .lock()
                .map_err(|_| anyhow::anyhow!("server state poisoned"))?;
            // Fold in whatever the broker said since the last call, so every answer starts from
            // a journaled, current book.
            guard.session.desk.drain()?;
            let result = f(&mut guard, &target);
            guard.session.save()?;
            result
        }
    }
}

#[tool_router]
impl McpServer {
    #[tool(
        description = "Which broker and mode (paper/live) this server is fixed to, the account, \
                       whether trading is halted, and today's usage against the limits. Call \
                       this first."
    )]
    async fn bkfwob_status(
        &self,
        Parameters(_): Parameters<Empty>,
    ) -> Result<CallToolResult, String> {
        let read_only = self.read_only;
        blocking(self.with(move |inner, target| {
            let desk = &mut inner.session.desk;
            let now = Timestamp::now();
            let account = desk.account()?;
            let working = desk.open_orders()?.len();
            let book = desk.book();
            let limits = desk.limits().clone();
            Ok(json!({
                "broker": target.kind.to_string(),
                "mode": target.mode,
                "account": account.id,
                "read_only": read_only,
                "halted": desk.halted().map(|h| json!({"reason": h.reason, "since": h.at})),
                "working_orders": working,
                "unknown_fate_orders": book.unresolved().count(),
                "sent_today": book.notional_today(now),
                "max_daily_notional": limits.max_daily_notional,
                "orders_last_minute": book.submissions_within(now, jiff::SignedDuration::from_secs(60)),
                "orders_per_minute": limits.orders_per_minute,
            }))
        }))
        .await
    }

    #[tool(description = "The guardrail limits every order is checked against.")]
    async fn bkfwob_limits(
        &self,
        Parameters(_): Parameters<Empty>,
    ) -> Result<CallToolResult, String> {
        blocking(self.with(|inner, _| Ok(serde_json::to_value(inner.session.desk.limits())?))).await
    }

    #[tool(description = "Cash, buying power and equity.")]
    async fn bkfwob_account(
        &self,
        Parameters(_): Parameters<Empty>,
    ) -> Result<CallToolResult, String> {
        blocking(self.with(|inner, _| Ok(serde_json::to_value(inner.session.desk.account()?)?)))
            .await
    }

    #[tool(description = "Current positions: symbol, signed quantity, average cost.")]
    async fn bkfwob_positions(
        &self,
        Parameters(p): Parameters<Symbols>,
    ) -> Result<CallToolResult, String> {
        blocking(self.with(move |inner, _| {
            let list: Vec<_> = inner
                .session
                .desk
                .positions()?
                .into_iter()
                .filter(|x| symbol_filter(&p.symbols, &x.instrument.symbol))
                .collect();
            Ok(json!({ "positions": list }))
        }))
        .await
    }

    #[tool(
        description = "Orders: `open` (working at the broker, default), `done` (our finished \
                       orders), or `all`. Each has a client `client_id` used by modify and cancel."
    )]
    async fn bkfwob_orders(
        &self,
        Parameters(p): Parameters<OrdersParams>,
    ) -> Result<CallToolResult, String> {
        blocking(self.with(move |inner, _| {
            let desk = &mut inner.session.desk;
            let which = p.which.unwrap_or(Which::Open);
            let mut list = match which {
                Which::Open => desk.open_orders()?,
                Which::Done | Which::All => {
                    desk.reconcile_orders()?;
                    let mut ours: Vec<_> = desk
                        .book()
                        .orders()
                        .map(|t| t.order.clone())
                        .filter(|o| matches!(which, Which::All) || o.status.is_terminal())
                        .collect();
                    if matches!(which, Which::All) {
                        for o in desk.open_orders()? {
                            if !ours.iter().any(|m| m.client_id == o.client_id) {
                                ours.push(o);
                            }
                        }
                    }
                    ours
                }
            };
            list.retain(|o| symbol_filter(&p.symbols, &o.spec.instrument.symbol));
            list.sort_by_key(|o| o.updated_at);
            Ok(json!({ "orders": list }))
        }))
        .await
    }

    #[tool(description = "Executions since a time (default: start of today, New York).")]
    async fn bkfwob_fills(
        &self,
        Parameters(p): Parameters<FillsParams>,
    ) -> Result<CallToolResult, String> {
        blocking(self.with(move |inner, _| {
            let since = match &p.since {
                Some(s) => s
                    .parse::<Timestamp>()
                    .with_context(|| format!("`since` must be an RFC3339 instant, got `{s}`"))?,
                None => crate::cli::today_start(),
            };
            let list: Vec<_> = inner
                .session
                .desk
                .fills(since)?
                .into_iter()
                .filter(|f| symbol_filter(&p.symbols, &f.instrument.symbol))
                .collect();
            Ok(json!({ "fills": list }))
        }))
        .await
    }

    #[tool(
        description = "Quote snapshots: bid, ask, last and the time of the last trade. An old \
                       time means the market is closed or the symbol is not trading."
    )]
    async fn bkfwob_quote(
        &self,
        Parameters(p): Parameters<QuoteParams>,
    ) -> Result<CallToolResult, String> {
        blocking(self.with(move |inner, _| {
            if p.symbols.is_empty() {
                bail!("give at least one symbol");
            }
            let mut quotes = Vec::new();
            for symbol in &p.symbols {
                if !is_symbol(symbol) {
                    bail!("`{symbol}` is not a symbol; symbols are upper case like AAPL");
                }
                quotes.push(
                    inner
                        .session
                        .desk
                        .quote(&Instrument::stock(symbol.clone()))?,
                );
            }
            Ok(json!({ "quotes": quotes }))
        }))
        .await
    }

    #[tool(
        description = "Check an order against every guardrail without sending it. If it passes, \
                       returns a `token`; pass it to bkfwob_place_order within 60 seconds to send \
                       exactly this order."
    )]
    async fn bkfwob_preview_order(
        &self,
        Parameters(p): Parameters<OrderParams>,
    ) -> Result<CallToolResult, String> {
        blocking(self.with(move |inner, _| {
            let spec = p.to_spec().map_err(anyhow::Error::msg)?;
            let preview = inner.session.desk.preview(&spec)?;
            let mut out = json!({
                "order": spec.summary(),
                "spec": spec,
                "assessment": preview.assessment,
            });
            match preview.refusal {
                Some(r) => {
                    out["refused"] = json!({ "code": r.code, "detail": r.detail });
                }
                None => {
                    let token = inner.tokens.issue(preview.spec, Instant::now());
                    out["token"] = json!(token);
                    out["expires_in_secs"] = json!(TOKEN_TTL.as_secs());
                }
            }
            Ok(out)
        }))
        .await
    }

    #[tool(
        description = "Send the order previewed under `token`. Single use. The guardrails run \
                       again; the account may have changed since the preview."
    )]
    async fn bkfwob_place_order(
        &self,
        Parameters(p): Parameters<TokenParams>,
    ) -> Result<CallToolResult, String> {
        blocking(self.with(move |inner, _| {
            let spec = inner
                .tokens
                .redeem(&p.token, Instant::now())
                .map_err(anyhow::Error::msg)?;
            let order = inner.session.desk.place(&spec)?;
            Ok(json!({ "order": order }))
        }))
        .await
    }

    #[tool(
        description = "Change a working order's share count and/or price. The order type cannot \
                       change; cancel and place a new order for that. Guardrails apply."
    )]
    async fn bkfwob_modify_order(
        &self,
        Parameters(p): Parameters<ModifyParams>,
    ) -> Result<CallToolResult, String> {
        blocking(self.with(move |inner, _| {
            let id = ClientOrderId::new(p.order_id.clone());
            let desk = &mut inner.session.desk;
            let current = match desk.find_order(&id)? {
                Some(o) => o,
                None => bail!("no order {id} at the broker"),
            };
            let change = p.change(current.spec.kind).map_err(anyhow::Error::msg)?;
            let order = desk.modify(&id, &change)?;
            Ok(json!({ "order": order }))
        }))
        .await
    }

    #[tool(description = "Cancel one working order by its client id. Allowed while halted.")]
    async fn bkfwob_cancel_order(
        &self,
        Parameters(p): Parameters<OrderIdParams>,
    ) -> Result<CallToolResult, String> {
        blocking(self.with(move |inner, _| {
            let id = ClientOrderId::new(p.order_id);
            inner.session.desk.cancel(&id)?;
            let order = inner.session.desk.find_order(&id)?;
            Ok(json!({ "cancelled": id, "order": order }))
        }))
        .await
    }

    #[tool(
        description = "Cancel every working order in the account, including ones not placed \
                       through bkfwob. Allowed while halted."
    )]
    async fn bkfwob_cancel_all(
        &self,
        Parameters(_): Parameters<Empty>,
    ) -> Result<CallToolResult, String> {
        blocking(self.with(|inner, _| {
            inner.session.desk.cancel_all()?;
            Ok(json!({ "working_orders": inner.session.desk.open_orders()? }))
        }))
        .await
    }

    #[tool(
        description = "Kill switch: stop all order entry, for every bkfwob process, and cancel \
                       every working order. Only a person can resume. Use it when something is \
                       wrong."
    )]
    async fn bkfwob_halt(
        &self,
        Parameters(p): Parameters<HaltParams>,
    ) -> Result<CallToolResult, String> {
        blocking(self.with(move |inner, _| {
            let reason = format!("agent: {}", p.reason);
            inner.session.desk.halt(&reason)?;
            Ok(json!({ "halted": true, "reason": reason }))
        }))
        .await
    }
}

#[tool_handler(router = self.router)]
impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("bkfwob", env!("CARGO_PKG_VERSION"))
                    .with_title("bkfwob brokerage")
                    .with_description(
                        "A brokerage account behind guardrails: positions, orders, fills, and a \
                         preview-then-place order path.",
                    ),
            )
            .with_instructions(INSTRUCTIONS)
    }
}

/// Starts the server for `target`, after the checks the CLI would make plus the MCP-specific one.
pub fn serve(target: Target, read_only: bool) -> Result<()> {
    if target.mode == Mode::Live && !target.config.mcp.allow_live {
        bail!(
            "refusing to serve a live account over MCP: set `[mcp] allow_live = true` in the \
             config if an agent should be able to reach real money"
        );
    }
    let session = target.open()?;
    eprintln!(
        "bkfwob mcp: {} {} account {}{}; serving on stdio",
        target.kind,
        target.mode,
        session.account.id,
        if read_only { " (read-only)" } else { "" }
    );
    let mut router = McpServer::tool_router();
    if read_only {
        for name in WRITE_TOOLS {
            router.remove_route(name);
        }
    }
    let server = McpServer {
        inner: Arc::new(Mutex::new(Inner {
            session,
            tokens: Tokens::default(),
        })),
        target: Arc::new(target),
        read_only,
        router,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to start the async runtime")?;
    runtime.block_on(async move {
        let service = server
            .serve(stdio())
            .await
            .context("failed to start the MCP server")?;
        service.waiting().await.context("MCP server failed")?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order() -> OrderParams {
        OrderParams {
            side: SideParam::Buy,
            symbol: "AAPL".into(),
            shares: Some(10.0),
            notional: None,
            order_type: Some(OrderType::Limit),
            limit_price: Some(185.5),
            stop_price: None,
            trail_amount: None,
            trail_percent: None,
            tif: Some(TifParam::Gtc),
            extended_hours: None,
            tag: None,
        }
    }

    #[test]
    fn order_params_become_specs() {
        assert_eq!(
            order().to_spec().unwrap().summary(),
            "buy 10 AAPL lmt:185.5 gtc rth"
        );
        let mut no_price = order();
        no_price.limit_price = None;
        assert!(no_price.to_spec().is_err());
        let mut both = order();
        both.notional = Some(100.0);
        assert!(both.to_spec().is_err());
        let mut lower = order();
        lower.symbol = "aapl".into();
        assert!(lower.to_spec().is_err());
    }

    #[test]
    fn a_token_is_single_use_and_expires() {
        let mut tokens = Tokens::default();
        let spec = order().to_spec().unwrap();
        let t0 = Instant::now();
        let token = tokens.issue(spec.clone(), t0);
        assert_eq!(tokens.redeem(&token, t0).unwrap(), spec);
        assert!(tokens.redeem(&token, t0).is_err(), "single use");

        let stale = tokens.issue(spec.clone(), t0);
        assert!(tokens.redeem(&stale, t0 + TOKEN_TTL).is_err(), "expired");
        assert!(tokens.redeem("pv-made-up", t0).is_err(), "unknown");
        let a = tokens.issue(spec.clone(), t0);
        let b = tokens.issue(spec, t0);
        assert_ne!(a, b, "two previews of the same order get different tokens");
    }

    #[test]
    fn modify_params_keep_the_order_kind() {
        let p = ModifyParams {
            order_id: "x".into(),
            shares: None,
            limit_price: Some(184.9),
            stop_price: None,
            trail_amount: None,
            trail_percent: None,
        };
        let change = p.change(OrderKind::Limit { limit: 185.5 }).unwrap();
        assert_eq!(change.kind, Some(OrderKind::Limit { limit: 184.9 }));
        assert!(p.change(OrderKind::Stop { stop: 100.0 }).is_err());
        let nothing = ModifyParams {
            limit_price: None,
            ..p
        };
        assert!(nothing.change(OrderKind::Limit { limit: 1.0 }).is_err());
    }

    #[test]
    fn read_only_removes_every_write_tool() {
        let mut router = McpServer::tool_router();
        for name in WRITE_TOOLS {
            assert!(router.has_route(name), "{name} is not a tool");
            router.remove_route(name);
        }
        let left: Vec<String> = router
            .list_all()
            .into_iter()
            .map(|t| t.name.to_string())
            .collect();
        for name in WRITE_TOOLS {
            assert!(!left.iter().any(|n| n == name));
        }
        assert!(left.iter().any(|n| n == "bkfwob_status"));
        assert_eq!(left.len(), 7, "{left:?}");
    }
}
