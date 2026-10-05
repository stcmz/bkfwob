//! The binary end to end against the fake broker: exit codes, the live gate, confirmations, and
//! the kill switch, as a user or a script would meet them.

use std::path::Path;
use std::process::{Command, Output, Stdio};

use tempfile::TempDir;

struct Env {
    dir: TempDir,
}

impl Env {
    fn new(config: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("config.toml"), config).unwrap();
        Self { dir }
    }

    fn paper() -> Self {
        Self::new("[broker]\nkind = \"fake\"\n")
    }

    fn run_with(&self, args: &[&str], live_env: Option<&str>) -> Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_bkfwob"));
        cmd.args(args)
            .env("BKFWOB_STATE", self.dir.path().join("state"))
            .env("BKFWOB_CONFIG", self.dir.path().join("config.toml"))
            .env_remove("BKFWOB_LIVE")
            .stdin(Stdio::null());
        if let Some(v) = live_env {
            cmd.env("BKFWOB_LIVE", v);
        }
        cmd.output().unwrap()
    }

    fn run(&self, args: &[&str]) -> Output {
        self.run_with(args, None)
    }

    fn state(&self) -> &Path {
        self.dir.path()
    }
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

fn code(o: &Output) -> i32 {
    o.status.code().unwrap()
}

#[test]
fn an_order_round_trip_on_the_fake_broker() {
    let env = Env::paper();
    let placed = env.run(&["buy", "AAPL", "5"]);
    assert_eq!(code(&placed), 0, "{}", stderr(&placed));
    assert!(stdout(&placed).contains("filled"), "{}", stdout(&placed));

    let positions = env.run(&["positions", "csv"]);
    assert!(
        stdout(&positions).contains("AAPL,5,"),
        "{}",
        stdout(&positions)
    );

    let resting = env.run(&["sell", "AAPL", "5", "lmt:999", "gtc", "jsonl"]);
    assert_eq!(code(&resting), 0, "{}", stderr(&resting));
    let order: serde_json::Value = serde_json::from_str(stdout(&resting).trim()).unwrap();
    assert_eq!(order["status"], "submitted");
    let id = order["client_id"].as_str().unwrap().to_string();

    let open = env.run(&["orders"]);
    assert!(stdout(&open).contains(&id));
    assert_eq!(code(&env.run(&["cancel", &id])), 0);
    assert!(!stdout(&env.run(&["orders"])).contains(&id));
}

#[test]
fn a_guardrail_refusal_exits_3_and_sends_nothing() {
    let env = Env::paper();
    let refused = env.run(&["buy", "AAPL", "1000"]);
    assert_eq!(code(&refused), 3);
    assert!(
        stderr(&refused).contains("order-notional"),
        "{}",
        stderr(&refused)
    );
    assert!(!stdout(&env.run(&["orders", "all"])).contains("bk-"));
}

#[test]
fn preview_sends_nothing() {
    let env = Env::paper();
    let preview = env.run(&["buy", "AAPL", "usd:1000", "preview"]);
    assert_eq!(code(&preview), 0, "{}", stderr(&preview));
    assert!(stdout(&preview).contains("verdict"), "{}", stdout(&preview));
    assert!(!stdout(&env.run(&["orders", "all"])).contains("bk-"));
}

/// Live needs the config *and* the environment; and even then, a live order is confirmed.
#[test]
fn live_takes_two_keys_and_a_confirmation() {
    let env = Env::new("[broker]\nkind = \"fake\"\nmode = \"live\"\n");
    let no_env = env.run(&["account"]);
    assert_eq!(code(&no_env), 1);
    assert!(
        stderr(&no_env).contains("BKFWOB_LIVE=1"),
        "{}",
        stderr(&no_env)
    );

    let unconfirmed = env.run_with(&["buy", "AAPL", "1"], Some("1"));
    assert_eq!(code(&unconfirmed), 1);
    assert!(
        stderr(&unconfirmed).contains("--yes"),
        "{}",
        stderr(&unconfirmed)
    );

    let confirmed = env.run_with(&["buy", "AAPL", "1", "--yes"], Some("1"));
    assert_eq!(code(&confirmed), 0, "{}", stderr(&confirmed));

    let paper = Env::paper();
    let env_only = paper.run_with(&["account"], Some("1"));
    assert_eq!(code(&env_only), 1, "the env var alone is refused too");
}

#[test]
fn the_mode_token_confirms_and_never_switches() {
    let env = Env::paper();
    assert_eq!(code(&env.run(&["account", "paper"])), 0);
    let switch = env.run(&["account", "live"]);
    assert_eq!(code(&switch), 1);
    assert!(
        stderr(&switch).contains("does not match"),
        "{}",
        stderr(&switch)
    );
}

#[test]
fn halt_blocks_orders_but_not_flatten_and_resume_lifts_it() {
    let env = Env::paper();
    assert_eq!(code(&env.run(&["buy", "MSFT", "3"])), 0);
    let halted = env.run(&["halt", "testing"]);
    assert_eq!(code(&halted), 0, "{}", stderr(&halted));
    assert!(env.state().join("state").join("HALT").exists());

    let blocked = env.run(&["buy", "MSFT", "1"]);
    assert_eq!(code(&blocked), 3);
    assert!(stderr(&blocked).contains("halted"));

    assert_eq!(code(&env.run(&["flatten", "MSFT"])), 1, "needs --yes");
    let flat = env.run(&["flatten", "MSFT", "--yes"]);
    assert_eq!(code(&flat), 0, "{}", stderr(&flat));
    assert!(!stdout(&env.run(&["positions"])).contains("MSFT"));

    assert_eq!(code(&env.run(&["resume"])), 0);
    assert_eq!(code(&env.run(&["buy", "MSFT", "1"])), 0);
}

#[test]
fn halt_works_even_when_the_broker_is_unreachable() {
    let env = Env::new("[broker]\nkind = \"ibkr\"\n[ibkr]\nport = 1\ntimeout_secs = 1\n");
    let halted = env.run(&["halt"]);
    assert_eq!(code(&halted), 0, "{}", stderr(&halted));
    assert!(stdout(&halted).contains("NO"), "{}", stdout(&halted));
    assert!(env.state().join("state").join("HALT").exists());
}

#[test]
fn unknown_tokens_are_named() {
    let env = Env::paper();
    let bad = env.run(&["buy", "AAPL", "10", "limit:5"]);
    assert_eq!(code(&bad), 1);
    assert!(stderr(&bad).contains("limit:5"), "{}", stderr(&bad));
}
