//! The `bkfwob` command line and broker adapters, over [`bkfwob_core`].

pub mod cli;
pub mod config;
#[cfg(feature = "ibkr")]
pub mod ibkr;
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod output;
pub mod session;
