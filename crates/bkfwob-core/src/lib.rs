//! Broker-neutral core of bkfwob: order types, the [`broker::Broker`] trait adapters implement,
//! and the machinery that makes talking to a real broker safe — guardrails, a write-ahead
//! journal, and reconciliation.
//!
//! No network code lives here; adapters are in the `bkfwob` crate.

pub mod book;
pub mod broker;
pub mod clock;
pub mod desk;
pub mod fake;
pub mod guard;
pub mod journal;
pub mod refusal;
pub mod state;
pub mod status;
pub mod tokens;
pub mod types;
