//! The append-only record of everything bkfwob asked for and everything the broker said.
//!
//! One JSON object per line, one file per exchange date. Two jobs:
//!
//! - **Audit.** Every request, guardrail verdict, broker answer and event, in order, readable with
//!   any text tool.
//! - **Write-ahead.** An order's intent is written and fsynced *before* it is sent. If the process
//!   dies between sending and hearing back, the restarted process finds an intent with no outcome,
//!   knows an order may exist, and reconciles it by client id instead of forgetting it.
//!
//! A torn final line (a crash mid-write) is skipped; a malformed line anywhere else is an error,
//! because silently dropping the middle of an audit log is how its gaps go unnoticed.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use fs2::FileExt;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::clock::exchange_date;
use crate::refusal::Refusal;
use crate::types::{ClientOrderId, Fill, Order, OrderRequest, OrderSpec, Position};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Entry {
    /// About to send this order. Written before the send, and synced.
    Intent {
        request: OrderRequest,
        /// What the guardrails estimated it is worth; what the daily budget is charged.
        est_notional: f64,
    },
    /// About to change a working order to `spec`. Synced, like an intent.
    ModifyIntent {
        client_id: ClientOrderId,
        spec: OrderSpec,
        est_notional: f64,
    },
    CancelIntent {
        client_id: ClientOrderId,
    },
    CancelAll,
    /// An order as the broker reported it: an acknowledgement, an event, or a reconciled lookup.
    Order {
        order: Order,
    },
    /// The send went out and the answer was lost.
    Unknown {
        client_id: ClientOrderId,
        error: String,
    },
    Fill {
        fill: Fill,
    },
    /// The guardrails stopped an order before it was sent.
    Refused {
        spec: OrderSpec,
        refusal: Refusal,
    },
    /// Positions as the broker reported them at reconciliation, with every execution id counted
    /// in them: the baseline the next reconciliation checks the account against.
    Positions {
        positions: Vec<Position>,
        exec_ids: Vec<String>,
    },
    Halt {
        reason: String,
    },
    Resume,
    /// Something that should not happen, did. Not an error: the broker's word stands, and this is
    /// the note that our model of it was wrong.
    Anomaly {
        detail: String,
    },
    Note {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub time: Timestamp,
    pub pid: u32,
    #[serde(flatten)]
    pub entry: Entry,
}

#[derive(Debug, Clone)]
pub struct Journal {
    dir: PathBuf,
}

impl Journal {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)
            .with_context(|| format!("creating journal directory {}", dir.display()))?;
        Ok(Self { dir })
    }

    fn file_for(&self, at: Timestamp) -> PathBuf {
        self.dir
            .join(format!("{}.jsonl", exchange_date(at).strftime("%Y-%m-%d")))
    }

    /// Appends one entry. With `sync`, returns only once it is on disk.
    ///
    /// The file is locked for the write so two processes appending at once cannot interleave
    /// halves of their lines.
    pub fn append(&self, at: Timestamp, entry: Entry, sync: bool) -> Result<Record> {
        let record = Record {
            time: at,
            pid: std::process::id(),
            entry,
        };
        let mut line = serde_json::to_string(&record)?;
        line.push('\n');
        let path = self.file_for(at);
        // The lock is a separate file: Windows will not lock a handle opened append-only.
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.dir.join(".lock"))
            .context("opening the journal lock")?;
        lock.lock_exclusive().context("locking the journal")?;
        let written = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .and_then(|mut file| {
                file.write_all(line.as_bytes())?;
                if sync { file.sync_all() } else { Ok(()) }
            });
        FileExt::unlock(&lock)?;
        written.with_context(|| format!("writing journal {}", path.display()))?;
        Ok(record)
    }

    /// Every record, oldest first.
    pub fn read_all(&self) -> Result<Vec<Record>> {
        let mut files: Vec<PathBuf> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
            .collect();
        files.sort();
        let mut out = Vec::new();
        for path in files {
            read_file(&path, &mut out)?;
        }
        Ok(out)
    }
}

fn read_file(path: &PathBuf, out: &mut Vec<Record>) -> Result<()> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let lines: Vec<String> = BufReader::new(file).lines().collect::<Result<_, _>>()?;
    let last = lines.len().saturating_sub(1);
    for (n, line) in lines.iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Record>(line) {
            Ok(record) => out.push(record),
            // A torn final line is what a crash mid-append leaves; nothing after it was lost.
            Err(_) if n == last => {}
            Err(e) => bail!("{}:{}: unreadable journal line: {e}", path.display(), n + 1),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Instrument, OrderKind, Qty, Side};

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn intent(id: &str) -> Entry {
        Entry::Intent {
            request: OrderRequest {
                client_id: ClientOrderId::new(id),
                spec: OrderSpec::new(
                    Instrument::stock("AAPL"),
                    Side::Buy,
                    Qty::Shares(1.0),
                    OrderKind::Market,
                ),
            },
            est_notional: 100.0,
        }
    }

    #[test]
    fn records_come_back_in_order_across_days() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::open(dir.path()).unwrap();
        journal
            .append(at("2026-10-05T14:00:00Z"), intent("a"), true)
            .unwrap();
        journal
            .append(at("2026-10-06T14:00:00Z"), intent("b"), false)
            .unwrap();
        journal
            .append(
                at("2026-10-06T14:00:01Z"),
                Entry::Note {
                    message: "hi".into(),
                },
                false,
            )
            .unwrap();
        let records = journal.read_all().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].entry, intent("a"));
        assert_eq!(records[1].entry, intent("b"));
        let files = fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                let path = e.as_ref().unwrap().path();
                path.extension().is_some_and(|x| x == "jsonl")
            })
            .count();
        assert_eq!(files, 2, "one file per day");
    }

    #[test]
    fn a_torn_last_line_is_skipped_but_a_corrupt_middle_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::open(dir.path()).unwrap();
        let t = at("2026-10-05T14:00:00Z");
        journal.append(t, intent("a"), true).unwrap();
        let path = journal.file_for(t);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(br#"{"time":"2026-10-05T14:00:01Z","pid":1,"ki"#)
            .unwrap();
        assert_eq!(journal.read_all().unwrap().len(), 1);

        file.write_all(b"\n").unwrap();
        journal.append(t, intent("b"), true).unwrap();
        assert!(journal.read_all().is_err());
    }

    #[test]
    fn lines_are_flat_json_with_a_kind() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::open(dir.path()).unwrap();
        let t = at("2026-10-05T14:00:00Z");
        journal.append(t, Entry::CancelAll, false).unwrap();
        let text = fs::read_to_string(journal.file_for(t)).unwrap();
        assert!(text.contains(r#""kind":"cancel-all""#), "{text}");
    }
}
