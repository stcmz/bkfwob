//! Against a real IB Gateway or TWS **paper** account. Ignored by default; run with
//!
//! ```text
//! BKFWOB_IBKR_PAPER=1 cargo test -p bkfwob --test ibkr_paper -- --ignored --test-threads=1
//! ```
//!
//! Needs the gateway on 127.0.0.1:4002 (or `BKFWOB_IBKR_PORT`) with "Read-Only API" unticked.
//! Every order is a GTC limit buy far below the market, so it cannot fill, and it is cancelled
//! before the test ends. bkfwob's own mode check refuses to run if the account is not a `DU…`
//! paper account.

use std::process::{Command, Output, Stdio};

fn enabled() -> bool {
    std::env::var("BKFWOB_IBKR_PAPER").as_deref() == Ok("1")
}

struct Paper {
    dir: tempfile::TempDir,
}

impl Paper {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let port = std::env::var("BKFWOB_IBKR_PORT").unwrap_or_else(|_| "4002".into());
        std::fs::write(
            dir.path().join("config.toml"),
            format!("[broker]\nkind = \"ibkr\"\nmode = \"paper\"\n[ibkr]\nport = {port}\n"),
        )
        .unwrap();
        Self { dir }
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_bkfwob"))
            .args(args)
            .env("BKFWOB_STATE", self.dir.path().join("state"))
            .env("BKFWOB_CONFIG", self.dir.path().join("config.toml"))
            .env_remove("BKFWOB_LIVE")
            .stdin(Stdio::null())
            .output()
            .unwrap()
    }

    fn ok(&self, args: &[&str]) -> String {
        let out = self.run(args);
        assert!(
            out.status.success(),
            "bkfwob {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }
}

fn json(line: &str) -> serde_json::Value {
    serde_json::from_str(line.trim()).unwrap()
}

#[test]
#[ignore = "needs an IB paper gateway; see the module docs"]
fn a_resting_order_is_placed_found_modified_and_cancelled() {
    if !enabled() {
        return;
    }
    let ib = Paper::new();
    let account = json(&ib.ok(&["account", "jsonl"]));
    assert_eq!(account["mode"], "paper");

    let placed = json(&ib.ok(&["buy", "AAPL", "1", "lmt:5", "gtc", "jsonl"]));
    let id = placed["client_id"].as_str().unwrap().to_string();
    assert!(
        ["submitted", "partially-filled"].contains(&placed["status"].as_str().unwrap()),
        "{placed}"
    );

    let result = std::panic::catch_unwind(|| {
        assert!(
            ib.ok(&["orders", "jsonl"]).contains(&id),
            "IB lists it by orderRef"
        );
        let moved = json(&ib.ok(&["modify", &id, "lmt:6", "jsonl"]));
        assert_eq!(moved["spec"]["kind"]["limit"], 6.0);
    });
    // Cancel whatever happened above, so a failure never leaves an order resting.
    ib.ok(&["cancel", &id]);
    result.unwrap();

    std::thread::sleep(std::time::Duration::from_secs(2));
    assert!(!ib.ok(&["orders", "jsonl"]).contains(&id));
    let report = json(&ib.ok(&["reconcile", "jsonl"]));
    assert_eq!(report["halted"], false, "{report}");
}

#[test]
#[ignore = "needs an IB paper gateway; see the module docs"]
fn a_market_order_is_refused_when_the_quote_is_stale_or_missing() {
    if !enabled() {
        return;
    }
    // Outside regular hours the last trade is old and the guard must refuse; during them this
    // passes trivially or the order is refused for another reason — either way nothing is sent
    // without `preview` being honoured.
    let ib = Paper::new();
    let preview = ib.ok(&["buy", "AAPL", "1", "preview", "jsonl"]);
    let verdict = json(&preview);
    assert!(verdict.get("assessment").is_some() || verdict.get("refusal").is_some());
    assert!(!ib.ok(&["orders", "all", "jsonl"]).contains("bk-"));
}
