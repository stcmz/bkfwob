//! Why an order was not sent: a stable code a program can match on, and a detail a person can read.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Stable across releases: the MCP server returns these to agents, and an agent's handling of
/// `daily-notional` must not break because the wording of the message changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RefusalCode {
    /// The kill switch is on.
    Halted,
    /// The broker cannot take this kind of order.
    Unsupported,
    /// Malformed: non-positive quantity, bad price.
    Invalid,
    NotOnTick,
    OrderNotional,
    DailyNotional,
    PositionValue,
    OpenOrders,
    RateLimit,
    SymbolNotAllowed,
    /// A limit price far through the market: the fat-finger check.
    Collar,
    /// A sell larger than the position not already committed to other sells.
    Oversell,
    QuoteUnavailable,
    QuoteStale,
    /// The order is finished and cannot be changed.
    NotWorking,
    UnknownOrder,
}

impl fmt::Display for RefusalCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The serde name, so the CLI and MCP spell it the same way.
        let text = serde_json::to_value(self).ok();
        f.write_str(text.as_ref().and_then(|v| v.as_str()).unwrap_or("refused"))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("refused ({code}): {detail}")]
pub struct Refusal {
    pub code: RefusalCode,
    pub detail: String,
}

impl Refusal {
    pub fn new(code: RefusalCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_display_as_their_wire_names() {
        assert_eq!(RefusalCode::DailyNotional.to_string(), "daily-notional");
        assert_eq!(
            Refusal::new(RefusalCode::Oversell, "x").to_string(),
            "refused (oversell): x"
        );
    }
}
