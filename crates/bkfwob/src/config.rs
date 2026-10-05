//! `config.toml`: which broker, which mode, how to reach it, and the limits.
//!
//! `deny_unknown_fields` throughout, as in qtfwob: a misspelled limit that silently fell back to
//! its default would trade under a limit nobody chose. Credentials never live here; adapters read
//! them from the environment.

use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use bkfwob_core::guard::Limits;
use bkfwob_core::state::StateDir;
use bkfwob_core::types::Mode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BrokerKind {
    #[default]
    Ibkr,
    Alpaca,
    Fake,
}

impl BrokerKind {
    pub fn parse(token: &str) -> Option<Self> {
        Some(match token {
            "ibkr" => Self::Ibkr,
            "alpaca" => Self::Alpaca,
            "fake" => Self::Fake,
            _ => return None,
        })
    }
}

impl fmt::Display for BrokerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ibkr => "ibkr",
            Self::Alpaca => "alpaca",
            Self::Fake => "fake",
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub broker: BrokerConfig,
    pub ibkr: IbkrConfig,
    pub fake: FakeConfig,
    pub limits: Limits,
    pub mcp: McpConfig,
    pub state: StateConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BrokerConfig {
    pub kind: BrokerKind,
    /// `paper` (default) or `live`. Live also needs `BKFWOB_LIVE=1` in the environment.
    pub mode: Mode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IbkrConfig {
    pub host: String,
    /// Defaults by mode to IB Gateway's ports: 4002 paper, 4001 live. TWS uses 7497 / 7496.
    pub port: Option<u16>,
    /// 17 rather than mdfwob's 0. Client 0 is IB's special "sees manual TWS orders" id, and an
    /// order can only be modified or cancelled from the client id that placed it, so this must
    /// stay stable across runs and differ from any downloader's.
    pub client_id: i32,
    /// Required when the login has more than one account.
    pub account: Option<String>,
    /// Seconds to wait for a reply before an order's outcome is declared unknown.
    pub timeout_secs: u64,
}

impl Default for IbkrConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: None,
            client_id: 17,
            account: None,
            timeout_secs: 10,
        }
    }
}

impl IbkrConfig {
    pub fn port_for(&self, mode: Mode) -> u16 {
        self.port.unwrap_or(match mode {
            Mode::Paper => 4002,
            Mode::Live => 4001,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FakeConfig {
    /// Starting cash for a new fake account.
    pub cash: f64,
}

impl Default for FakeConfig {
    fn default() -> Self {
        Self { cash: 100_000.0 }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct McpConfig {
    /// Whether `bkfwob mcp` may run against a live account at all. Off by default: an agent
    /// trading real money is a decision to write down, not a flag to pass.
    pub allow_live: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StateConfig {
    /// Journals, id sequence, HALT. Defaults to `BKFWOB_STATE` or `~/.bkfwob`.
    pub dir: Option<PathBuf>,
}

/// Where a config came from, for `status`.
#[derive(Debug, Clone)]
pub enum Source {
    File(PathBuf),
    Defaults,
}

impl Config {
    pub fn parse(text: &str) -> Result<Self> {
        let config: Self = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        let l = &self.limits;
        for (name, v) in [
            ("max_order_notional", l.max_order_notional),
            ("max_daily_notional", l.max_daily_notional),
            ("max_position_value", l.max_position_value),
        ] {
            if !(v.is_finite() && v > 0.0) {
                bail!("[limits] {name} must be positive");
            }
        }
        if !(0.0..100.0).contains(&l.collar_pct) {
            bail!("[limits] collar_pct must be in [0, 100)");
        }
        Ok(())
    }

    /// An explicit path, else `BKFWOB_CONFIG`, else `<state root>/config.toml` if it exists,
    /// else the defaults (IBKR paper).
    pub fn load(explicit: Option<&Path>) -> Result<(Self, Source)> {
        let path = explicit
            .map(Path::to_path_buf)
            .or_else(|| std::env::var_os("BKFWOB_CONFIG").map(PathBuf::from))
            .or_else(|| {
                let default = StateDir::default_root().join("config.toml");
                default.exists().then_some(default)
            });
        let Some(path) = path else {
            return Ok((Self::default(), Source::Defaults));
        };
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let config =
            Self::parse(&text).with_context(|| format!("parsing config {}", path.display()))?;
        Ok((config, Source::File(path)))
    }

    pub fn state_root(&self, flag: Option<&Path>) -> PathBuf {
        flag.map(Path::to_path_buf)
            .or_else(|| self.state.dir.clone())
            .unwrap_or_else(StateDir::default_root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_ibkr_paper_with_gateway_ports() {
        let config = Config::parse("").unwrap();
        assert_eq!(config.broker.kind, BrokerKind::Ibkr);
        assert_eq!(config.broker.mode, Mode::Paper);
        assert_eq!(config.ibkr.port_for(Mode::Paper), 4002);
        assert_eq!(config.ibkr.port_for(Mode::Live), 4001);
        assert_eq!(config.ibkr.client_id, 17);
    }

    #[test]
    fn a_misspelled_limit_is_an_error_not_a_default() {
        let error = Config::parse("[limits]\nmax_order_notinal = 1.0\n")
            .unwrap_err()
            .to_string();
        assert!(error.contains("max_order_notinal"), "{error}");
    }

    #[test]
    fn nonsense_limits_are_refused() {
        assert!(Config::parse("[limits]\nmax_order_notional = 0.0\n").is_err());
        assert!(Config::parse("[limits]\ncollar_pct = 150.0\n").is_err());
    }

    #[test]
    fn the_shipped_example_parses() {
        let config = Config::parse(include_str!("../../../config.example.toml"))
            .expect("the shipped example must parse");
        assert_eq!(config.broker.mode, Mode::Paper);
    }
}
