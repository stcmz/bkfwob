//! Order tokens: the positional grammar shared by the CLI and anything else that takes orders as
//! text.
//!
//! The same convention as mdfwob and qtfwob — case-sensitive tokens in any order, `kind:params`
//! for anything with parameters. Symbols are upper case and every keyword is lower case, so the two
//! can never be confused: `GTC` is a ticker, `gtc` is a time in force.
//!
//! ```text
//! SYMBOL     AAPL, BRK.B                 upper case
//! QTY        100 | 0.5 | usd:5000        shares, or cash value (notional)
//! KIND       mkt | lmt:P | stp:P | stp-lmt:STOP,LIMIT | trail:AMOUNT | trail:PCT%
//! TIF        day | gtc | ioc
//! SESSION    rth | eth
//! TAG        tag:NAME
//! ```
//!
//! Each classifier returns `None` for a token that is not its kind, and `Some(Err)` for one that
//! is its kind but malformed — so `lmt:abc` reports a bad limit price rather than "unknown token".

use crate::types::{Instrument, OrderKind, OrderSpec, Qty, Session, Side, Tif, Trail};

pub type TokenResult<T> = Option<Result<T, String>>;

/// `AAPL`, `BRK.B`, `BF-B`: starts with an upper-case letter, then upper case, digits, `.` or `-`.
pub fn is_symbol(token: &str) -> bool {
    let mut chars = token.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_uppercase())
        && token.len() <= 12
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '.' || c == '-')
}

fn positive(text: &str, what: &str) -> Result<f64, String> {
    match text.parse::<f64>() {
        Ok(v) if v.is_finite() && v > 0.0 => Ok(v),
        _ => Err(format!("{what} must be a positive number, got `{text}`")),
    }
}

/// `100`, `0.5` → shares; `usd:5000` → notional.
pub fn parse_qty(token: &str) -> TokenResult<Qty> {
    if let Some(value) = token.strip_prefix("usd:") {
        return Some(positive(value, "a notional value").map(Qty::Notional));
    }
    if token.starts_with(|c: char| c.is_ascii_digit() || c == '.') {
        return Some(positive(token, "a quantity").map(Qty::Shares));
    }
    None
}

/// `mkt`, `lmt:P`, `stp:P`, `stp-lmt:S,L`, `trail:A`, `trail:P%`.
pub fn parse_kind(token: &str) -> TokenResult<OrderKind> {
    if token == "mkt" {
        return Some(Ok(OrderKind::Market));
    }
    let (name, params) = token.split_once(':')?;
    let kind = match name {
        "lmt" => positive(params, "a limit price").map(|limit| OrderKind::Limit { limit }),
        "stp" => positive(params, "a stop price").map(|stop| OrderKind::Stop { stop }),
        "stp-lmt" => match params.split_once(',') {
            Some((stop, limit)) => positive(stop, "a stop price").and_then(|stop| {
                positive(limit, "a limit price").map(|limit| OrderKind::StopLimit { stop, limit })
            }),
            None => Err(format!("stp-lmt takes STOP,LIMIT, got `{params}`")),
        },
        "trail" => match params.strip_suffix('%') {
            Some(pct) => positive(pct, "a trailing percent").and_then(|p| {
                if p < 100.0 {
                    Ok(OrderKind::Trailing {
                        trail: Trail::Percent(p),
                    })
                } else {
                    Err(format!("a trailing percent must be below 100, got {p}"))
                }
            }),
            None => positive(params, "a trailing amount").map(|a| OrderKind::Trailing {
                trail: Trail::Amount(a),
            }),
        },
        _ => return None,
    };
    Some(kind)
}

pub fn parse_tif(token: &str) -> TokenResult<Tif> {
    Some(Ok(match token {
        "day" => Tif::Day,
        "gtc" => Tif::Gtc,
        "ioc" => Tif::Ioc,
        _ => return None,
    }))
}

pub fn parse_session(token: &str) -> TokenResult<Session> {
    Some(Ok(match token {
        "rth" => Session::Rth,
        "eth" => Session::Extended,
        _ => return None,
    }))
}

/// `tag:NAME`, NAME up to 32 of `[A-Za-z0-9_-]`.
pub fn parse_tag(token: &str) -> TokenResult<String> {
    let name = token.strip_prefix("tag:")?;
    let ok = !name.is_empty()
        && name.len() <= 32
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    Some(if ok {
        Ok(name.to_string())
    } else {
        Err(format!(
            "a tag is 1-32 letters, digits, `_` or `-`, got `{name}`"
        ))
    })
}

/// The order-describing tokens of a command line, collected one slot at a time.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct OrderTokens {
    pub symbol: Option<String>,
    pub qty: Option<Qty>,
    pub kind: Option<OrderKind>,
    pub tif: Option<Tif>,
    pub session: Option<Session>,
    pub tag: Option<String>,
}

fn fill<T>(slot: &mut Option<T>, value: T, what: &str) -> Result<bool, String> {
    if slot.is_some() {
        return Err(format!("more than one {what} given"));
    }
    *slot = Some(value);
    Ok(true)
}

impl OrderTokens {
    /// Claims `token` if it describes part of an order. `Ok(false)` leaves it for the caller —
    /// the CLI has its own tokens (broker, mode, format) in the same position list.
    pub fn accept(&mut self, token: &str) -> Result<bool, String> {
        if let Some(kind) = parse_kind(token) {
            return fill(&mut self.kind, kind?, "order kind");
        }
        if let Some(qty) = parse_qty(token) {
            return fill(&mut self.qty, qty?, "quantity");
        }
        if let Some(tif) = parse_tif(token) {
            return fill(&mut self.tif, tif?, "time in force");
        }
        if let Some(session) = parse_session(token) {
            return fill(&mut self.session, session?, "session");
        }
        if let Some(tag) = parse_tag(token) {
            return fill(&mut self.tag, tag?, "tag");
        }
        if is_symbol(token) {
            return fill(&mut self.symbol, token.to_string(), "symbol");
        }
        Ok(false)
    }

    pub fn parse(tokens: &[impl AsRef<str>]) -> Result<(Self, Vec<String>), String> {
        let mut out = Self::default();
        let mut rest = Vec::new();
        for token in tokens {
            if !out.accept(token.as_ref())? {
                rest.push(token.as_ref().to_string());
            }
        }
        Ok((out, rest))
    }

    /// A complete order: symbol and quantity are required, the rest default to `mkt day rth`.
    pub fn into_spec(self, side: Side) -> Result<OrderSpec, String> {
        let symbol = self
            .symbol
            .ok_or("no SYMBOL given (symbols are upper case)")?;
        let qty = self
            .qty
            .ok_or("no quantity given, e.g. `100` or `usd:5000`")?;
        Ok(OrderSpec {
            instrument: Instrument::stock(symbol),
            side,
            qty,
            kind: self.kind.unwrap_or(OrderKind::Market),
            tif: self.tif.unwrap_or_default(),
            session: self.session.unwrap_or_default(),
            tag: self.tag,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbols_are_upper_case_and_keywords_are_not() {
        for ok in ["AAPL", "BRK.B", "BF-B", "X", "GTC"] {
            assert!(is_symbol(ok), "{ok}");
        }
        for not in ["aapl", "gtc", "1AB", "", "A B", "AAPL!"] {
            assert!(!is_symbol(not), "{not}");
        }
    }

    #[test]
    fn quantities() {
        assert_eq!(parse_qty("100"), Some(Ok(Qty::Shares(100.0))));
        assert_eq!(parse_qty("0.5"), Some(Ok(Qty::Shares(0.5))));
        assert_eq!(parse_qty("usd:5000"), Some(Ok(Qty::Notional(5000.0))));
        assert!(matches!(parse_qty("0"), Some(Err(_))));
        assert!(matches!(parse_qty("usd:-5"), Some(Err(_))));
        assert!(matches!(parse_qty("1e999"), Some(Err(_))), "infinite");
        assert_eq!(parse_qty("AAPL"), None);
    }

    #[test]
    fn order_kinds() {
        assert_eq!(parse_kind("mkt"), Some(Ok(OrderKind::Market)));
        assert_eq!(
            parse_kind("lmt:185.5"),
            Some(Ok(OrderKind::Limit { limit: 185.5 }))
        );
        assert_eq!(
            parse_kind("stp:180"),
            Some(Ok(OrderKind::Stop { stop: 180.0 }))
        );
        assert_eq!(
            parse_kind("stp-lmt:180,179.5"),
            Some(Ok(OrderKind::StopLimit {
                stop: 180.0,
                limit: 179.5
            }))
        );
        assert_eq!(
            parse_kind("trail:2.5"),
            Some(Ok(OrderKind::Trailing {
                trail: Trail::Amount(2.5)
            }))
        );
        assert_eq!(
            parse_kind("trail:1%"),
            Some(Ok(OrderKind::Trailing {
                trail: Trail::Percent(1.0)
            }))
        );
        for bad in [
            "lmt:abc",
            "lmt:",
            "stp:-1",
            "stp-lmt:180",
            "trail:100%",
            "trail:x%",
        ] {
            assert!(matches!(parse_kind(bad), Some(Err(_))), "{bad}");
        }
        assert_eq!(parse_kind("usd:5"), None);
        assert_eq!(parse_kind("day"), None);
    }

    #[test]
    fn tif_session_and_tag() {
        assert_eq!(parse_tif("gtc"), Some(Ok(Tif::Gtc)));
        assert_eq!(parse_tif("GTC"), None, "case-sensitive: that is a ticker");
        assert_eq!(parse_session("eth"), Some(Ok(Session::Extended)));
        assert_eq!(parse_tag("tag:stop-1"), Some(Ok("stop-1".into())));
        assert!(matches!(parse_tag("tag:"), Some(Err(_))));
        assert!(matches!(parse_tag("tag:a b"), Some(Err(_))));
    }

    #[test]
    fn a_full_order_line_in_any_order() {
        let (tokens, rest) =
            OrderTokens::parse(&["gtc", "lmt:185.5", "AAPL", "10", "ibkr", "tag:entry"]).unwrap();
        assert_eq!(rest, ["ibkr"], "unclaimed tokens are left for the caller");
        let spec = tokens.into_spec(Side::Buy).unwrap();
        assert_eq!(spec.summary(), "buy 10 AAPL lmt:185.5 gtc rth tag:entry");
    }

    #[test]
    fn defaults_are_market_day_regular_hours() {
        let (tokens, _) = OrderTokens::parse(&["MSFT", "usd:1000"]).unwrap();
        let spec = tokens.into_spec(Side::Sell).unwrap();
        assert_eq!(spec.kind, OrderKind::Market);
        assert_eq!(spec.tif, Tif::Day);
        assert_eq!(spec.session, Session::Rth);
    }

    #[test]
    fn duplicates_and_missing_parts_are_errors() {
        assert!(OrderTokens::parse(&["AAPL", "MSFT"]).is_err());
        assert!(OrderTokens::parse(&["10", "20"]).is_err());
        assert!(OrderTokens::parse(&["mkt", "lmt:1"]).is_err());
        let (no_qty, _) = OrderTokens::parse(&["AAPL"]).unwrap();
        assert!(no_qty.into_spec(Side::Buy).is_err());
        let (no_symbol, _) = OrderTokens::parse(&["10"]).unwrap();
        assert!(no_symbol.into_spec(Side::Buy).is_err());
    }
}
