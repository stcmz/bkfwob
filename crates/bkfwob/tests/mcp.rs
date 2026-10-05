//! `bkfwob mcp` end to end: a real server process on stdio, a minimal JSON-RPC client, and the fake
//! broker behind it.

#![cfg(feature = "mcp")]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde_json::{Value, json};
use tempfile::TempDir;

struct Server {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
    _dir: TempDir,
}

impl Server {
    fn start(config: &str, args: &[&str], live: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("config.toml"), config).unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_bkfwob"));
        cmd.arg("mcp")
            .args(args)
            .env("BKFWOB_STATE", dir.path().join("state"))
            .env("BKFWOB_CONFIG", dir.path().join("config.toml"))
            .env_remove("BKFWOB_LIVE")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if live {
            cmd.env("BKFWOB_LIVE", "1");
        }
        let mut child = cmd.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut server = Self {
            child,
            stdin,
            stdout,
            next_id: 1,
            _dir: dir,
        };
        server.request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "bkfwob-test", "version": "0" }
            }),
        );
        server.send(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));
        server
    }

    fn fake(args: &[&str]) -> Self {
        Self::start("[broker]\nkind = \"fake\"\n", args, false)
    }

    fn send(&mut self, message: Value) {
        writeln!(self.stdin, "{message}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        loop {
            let mut line = String::new();
            assert!(
                self.stdout.read_line(&mut line).unwrap() > 0,
                "server closed stdout"
            );
            let message: Value = serde_json::from_str(&line).unwrap();
            if message["id"] == json!(id) {
                assert!(message.get("error").is_none(), "{message}");
                return message["result"].clone();
            }
        }
    }

    fn tools(&mut self) -> Vec<String> {
        self.request("tools/list", json!({}))["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect()
    }

    /// Returns (structured content, is_error).
    fn call(&mut self, tool: &str, arguments: Value) -> (Value, bool) {
        let result = self.request(
            "tools/call",
            json!({ "name": tool, "arguments": arguments }),
        );
        let is_error = result["isError"].as_bool().unwrap_or(false);
        let content = match result.get("structuredContent") {
            Some(v) if !v.is_null() => v.clone(),
            _ => json!({ "text": result["content"][0]["text"] }),
        };
        (content, is_error)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn buy(shares: f64, limit: f64) -> Value {
    json!({
        "side": "buy", "symbol": "AAPL", "shares": shares,
        "order_type": "limit", "limit_price": limit, "tif": "gtc"
    })
}

#[test]
fn preview_then_place_with_a_single_use_token() {
    let mut s = Server::fake(&[]);
    let (status, _) = s.call("bkfwob_status", json!({}));
    assert_eq!(status["mode"], "paper");
    assert_eq!(status["read_only"], false);

    let (preview, err) = s.call("bkfwob_preview_order", buy(2.0, 10.0));
    assert!(!err, "{preview}");
    let token = preview["token"].as_str().unwrap().to_string();

    let (placed, err) = s.call("bkfwob_place_order", json!({ "token": token }));
    assert!(!err, "{placed}");
    assert_eq!(placed["order"]["status"], "submitted");
    let id = placed["order"]["client_id"].as_str().unwrap().to_string();

    let (again, err) = s.call("bkfwob_place_order", json!({ "token": token }));
    assert!(err, "a token places once: {again}");

    let (orders, _) = s.call("bkfwob_orders", json!({}));
    assert_eq!(orders["orders"].as_array().unwrap().len(), 1, "{orders}");

    let (moved, err) = s.call(
        "bkfwob_modify_order",
        json!({ "order_id": id, "limit_price": 11.0 }),
    );
    assert!(!err, "{moved}");
    assert_eq!(moved["order"]["spec"]["kind"]["limit"], 11.0);

    let (_, err) = s.call("bkfwob_cancel_order", json!({ "order_id": id }));
    assert!(!err);
    let (orders, _) = s.call("bkfwob_orders", json!({}));
    assert!(orders["orders"].as_array().unwrap().is_empty());
}

#[test]
fn a_refusal_is_structured_and_issues_no_token() {
    let mut s = Server::fake(&[]);
    let (preview, err) = s.call("bkfwob_preview_order", buy(1000.0, 99.0));
    assert!(!err, "a refused preview is an answer, not an error");
    assert_eq!(preview["refused"]["code"], "order-notional", "{preview}");
    assert!(preview.get("token").is_none());

    let (bad, err) = s.call(
        "bkfwob_preview_order",
        json!({ "side": "buy", "symbol": "aapl", "shares": 1 }),
    );
    assert!(err, "{bad}");
}

#[test]
fn an_agent_can_halt_but_not_resume() {
    let mut s = Server::fake(&[]);
    let tools = s.tools();
    assert!(tools.contains(&"bkfwob_halt".to_string()));
    assert!(
        !tools
            .iter()
            .any(|t| t.contains("resume") || t.contains("flatten"))
    );

    let (_, err) = s.call("bkfwob_halt", json!({ "reason": "test" }));
    assert!(!err);
    let (preview, _) = s.call("bkfwob_preview_order", buy(1.0, 10.0));
    assert_eq!(preview["refused"]["code"], "halted", "{preview}");
    let (status, _) = s.call("bkfwob_status", json!({}));
    assert_eq!(status["halted"]["reason"], "agent: test");
}

#[test]
fn read_only_lists_no_write_tools() {
    let mut s = Server::fake(&["--read-only"]);
    let tools = s.tools();
    assert!(tools.contains(&"bkfwob_positions".to_string()));
    for write in [
        "preview_order",
        "place_order",
        "modify_order",
        "cancel",
        "halt",
    ] {
        assert!(!tools.iter().any(|t| t.contains(write)), "{tools:?}");
    }
    let (status, _) = s.call("bkfwob_status", json!({}));
    assert_eq!(status["read_only"], true);
}

#[test]
fn a_live_account_needs_allow_live() {
    let out = Command::new(env!("CARGO_BIN_EXE_bkfwob"))
        .args(["mcp"])
        .env("BKFWOB_STATE", tempfile::tempdir().unwrap().path())
        .env("BKFWOB_LIVE", "1")
        .env("BKFWOB_CONFIG", {
            let dir = tempfile::tempdir().unwrap().keep();
            let path = dir.join("config.toml");
            std::fs::write(&path, "[broker]\nkind = \"fake\"\nmode = \"live\"\n").unwrap();
            path
        })
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("allow_live"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let mut s = Server::start(
        "[broker]\nkind = \"fake\"\nmode = \"live\"\n[mcp]\nallow_live = true\n",
        &[],
        true,
    );
    let (status, _) = s.call("bkfwob_status", json!({}));
    assert_eq!(status["mode"], "live");
}
