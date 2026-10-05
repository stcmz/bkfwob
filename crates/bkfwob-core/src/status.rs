//! The order lifecycle.
//!
//! The transition table is used to *detect* anomalies, not to overrule the broker. When the broker
//! reports a status our table says is impossible, the broker is still right about where the order
//! is — the anomaly is journaled so a person can find out why our model of the lifecycle was wrong.

use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OrderStatus {
    /// Journaled and about to be sent; no answer from the broker yet.
    PendingSubmit,
    /// Sent, but the answer was lost (timeout, disconnect). The order may or may not exist at the
    /// broker. Only reconciliation — looking the client id up — resolves it.
    Unknown,
    /// Accepted and working.
    Submitted,
    PartiallyFilled,
    /// A cancel was sent and not yet confirmed. Fills can still arrive.
    PendingCancel,
    Filled,
    Cancelled,
    Rejected,
    Expired,
    /// Reconciliation looked for an `Unknown` order and the broker has no record of it.
    NotPlaced,
}

impl OrderStatus {
    pub const ALL: [OrderStatus; 10] = [
        OrderStatus::PendingSubmit,
        OrderStatus::Unknown,
        OrderStatus::Submitted,
        OrderStatus::PartiallyFilled,
        OrderStatus::PendingCancel,
        OrderStatus::Filled,
        OrderStatus::Cancelled,
        OrderStatus::Rejected,
        OrderStatus::Expired,
        OrderStatus::NotPlaced,
    ];

    /// No further status change is expected. (Late fill *events* can still arrive for a terminal
    /// order; they update quantities, not the status.)
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Cancelled | Self::Rejected | Self::Expired | Self::NotPlaced
        )
    }

    /// Working at the broker, or possibly so — anything that could still fill.
    pub fn is_live(self) -> bool {
        !self.is_terminal()
    }

    /// Whether `self → next` is a transition the lifecycle allows. Staying put is always allowed:
    /// brokers repeat statuses freely.
    pub fn can_become(self, next: OrderStatus) -> bool {
        use OrderStatus::*;
        if self == next {
            return true;
        }
        match self {
            PendingSubmit => matches!(
                next,
                Unknown | Submitted | PartiallyFilled | Filled | Cancelled | Rejected
            ),
            // Anything the broker might have done with it, or nothing at all.
            Unknown => next != PendingSubmit,
            Submitted => matches!(
                next,
                PartiallyFilled | PendingCancel | Filled | Cancelled | Rejected | Expired
            ),
            PartiallyFilled => matches!(next, PendingCancel | Filled | Cancelled | Expired),
            // A fill can win the race against a cancel, and a cancel can be refused (back to
            // working).
            PendingCancel => matches!(next, Submitted | PartiallyFilled | Filled | Cancelled),
            Filled | Cancelled | Rejected | Expired | NotPlaced => false,
        }
    }
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OrderStatus::PendingSubmit => "pending-submit",
            OrderStatus::Unknown => "unknown",
            OrderStatus::Submitted => "submitted",
            OrderStatus::PartiallyFilled => "partially-filled",
            OrderStatus::PendingCancel => "pending-cancel",
            OrderStatus::Filled => "filled",
            OrderStatus::Cancelled => "cancelled",
            OrderStatus::Rejected => "rejected",
            OrderStatus::Expired => "expired",
            OrderStatus::NotPlaced => "not-placed",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::OrderStatus::{self, *};

    /// The full table, spelled out: every pair not listed here must be refused.
    const LEGAL: &[(OrderStatus, OrderStatus)] = &[
        (PendingSubmit, Unknown),
        (PendingSubmit, Submitted),
        (PendingSubmit, PartiallyFilled),
        (PendingSubmit, Filled),
        (PendingSubmit, Cancelled),
        (PendingSubmit, Rejected),
        (Unknown, Submitted),
        (Unknown, PartiallyFilled),
        (Unknown, PendingCancel),
        (Unknown, Filled),
        (Unknown, Cancelled),
        (Unknown, Rejected),
        (Unknown, Expired),
        (Unknown, NotPlaced),
        (Submitted, PartiallyFilled),
        (Submitted, PendingCancel),
        (Submitted, Filled),
        (Submitted, Cancelled),
        (Submitted, Rejected),
        (Submitted, Expired),
        (PartiallyFilled, PendingCancel),
        (PartiallyFilled, Filled),
        (PartiallyFilled, Cancelled),
        (PartiallyFilled, Expired),
        (PendingCancel, Submitted),
        (PendingCancel, PartiallyFilled),
        (PendingCancel, Filled),
        (PendingCancel, Cancelled),
    ];

    #[test]
    fn every_transition_matches_the_table() {
        for from in OrderStatus::ALL {
            for to in OrderStatus::ALL {
                let expected = from == to || LEGAL.contains(&(from, to));
                assert_eq!(from.can_become(to), expected, "{from} -> {to}");
            }
        }
    }

    #[test]
    fn terminal_states_go_nowhere() {
        for from in OrderStatus::ALL.into_iter().filter(|s| s.is_terminal()) {
            for to in OrderStatus::ALL.into_iter().filter(|to| *to != from) {
                assert!(!from.can_become(to), "{from} -> {to}");
            }
        }
    }

    #[test]
    fn an_order_of_unknown_fate_still_counts_as_live() {
        // The whole point of `Unknown`: until resolved, it might fill, so guardrails must count
        // it against limits exactly like a working order.
        assert!(Unknown.is_live());
        assert!(PendingCancel.is_live());
        assert!(!NotPlaced.is_live());
    }
}
