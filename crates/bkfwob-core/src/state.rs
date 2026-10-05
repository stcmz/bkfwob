//! The on-disk state shared by every bkfwob process: journals, the client-id sequence, and the
//! kill switch.
//!
//! ```text
//! <root>/HALT                    kill switch: present = no process may send or change orders
//! <root>/<scope>/journal/*.jsonl one append-only file per exchange date
//! <root>/<scope>/seq             last client-id sequence number
//! ```
//!
//! The kill switch is a file, not a flag in memory, so it stops every process at once (the CLI
//! and a long-running MCP server alike) and survives restarts. It lives at the root, above the
//! per-broker scopes, so halting stops everything. A scope (`ibkr-paper`, `fake-paper`) keeps one
//! broker's journal from being reconciled against another.

use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use fs2::FileExt;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::clock::exchange_date;
use crate::types::ClientOrderId;

#[derive(Debug, Clone)]
pub struct StateDir {
    root: PathBuf,
    scope: PathBuf,
}

/// Why and when trading was halted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Halt {
    pub reason: String,
    pub at: Timestamp,
}

impl StateDir {
    /// `~/.bkfwob`, or `BKFWOB_STATE` when set.
    pub fn default_root() -> PathBuf {
        if let Some(dir) = std::env::var_os("BKFWOB_STATE") {
            return PathBuf::from(dir);
        }
        let home = std::env::var_os("USERPROFILE")
            .or_else(|| std::env::var_os("HOME"))
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        home.join(".bkfwob")
    }

    pub fn open(root: impl Into<PathBuf>, scope: &str) -> Result<Self> {
        let root = root.into();
        let scope = root.join(scope);
        fs::create_dir_all(scope.join("journal"))
            .with_context(|| format!("creating state directory {}", scope.display()))?;
        Ok(Self { root, scope })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn scope_dir(&self) -> &Path {
        &self.scope
    }
    pub fn journal_dir(&self) -> PathBuf {
        self.scope.join("journal")
    }
    fn halt_path(&self) -> PathBuf {
        self.root.join("HALT")
    }

    /// The halt in force, if any. Read from disk on every call: another process may have set it.
    pub fn halted(&self) -> Option<Halt> {
        let text = fs::read_to_string(self.halt_path()).ok()?;
        // An unreadable HALT file still halts: its presence is the signal, the content a note.
        Some(serde_json::from_str(&text).unwrap_or(Halt {
            reason: text.trim().to_string(),
            at: Timestamp::UNIX_EPOCH,
        }))
    }

    pub fn halt(&self, reason: &str, at: Timestamp) -> Result<()> {
        let halt = Halt {
            reason: reason.to_string(),
            at,
        };
        fs::write(self.halt_path(), serde_json::to_string_pretty(&halt)?)
            .context("writing HALT file")
    }

    /// Lifts the halt; `false` if none was in force.
    pub fn resume(&self) -> Result<bool> {
        match fs::remove_file(self.halt_path()) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e).context("removing HALT file"),
        }
    }

    /// A fresh client order id, `bk-YYYYMMDD-NNNN`.
    ///
    /// The sequence is global and only ever grows, held in a file under an exclusive lock, so two
    /// processes allocating at the same moment still get different ids and a restart never reuses
    /// one. Reuse would be dangerous, not just untidy: a broker looking up a reused id finds the
    /// wrong order.
    pub fn next_client_id(&self, now: Timestamp) -> Result<ClientOrderId> {
        let path = self.scope.join("seq");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        file.lock_exclusive().context("locking the id sequence")?;
        let mut text = String::new();
        file.read_to_string(&mut text)?;
        let last: u64 = if text.trim().is_empty() {
            0
        } else {
            text.trim()
                .parse()
                .with_context(|| format!("corrupt id sequence in {}", path.display()))?
        };
        let next = last + 1;
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;
        write!(file, "{next}")?;
        file.sync_all()?;
        FileExt::unlock(&file)?;
        let date = exchange_date(now).strftime("%Y%m%d");
        Ok(ClientOrderId::new(format!("bk-{date}-{next:04}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> Timestamp {
        "2026-10-05T15:00:00Z".parse().unwrap()
    }

    #[test]
    fn client_ids_never_repeat_across_reopens_or_threads() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateDir::open(dir.path(), "fake-paper").unwrap();
        assert_eq!(
            state.next_client_id(now()).unwrap().as_str(),
            "bk-20261005-0001"
        );
        assert_eq!(
            state.next_client_id(now()).unwrap().as_str(),
            "bk-20261005-0002"
        );

        let reopened = StateDir::open(dir.path(), "fake-paper").unwrap();
        assert_eq!(
            reopened.next_client_id(now()).unwrap().as_str(),
            "bk-20261005-0003"
        );

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let state = reopened.clone();
                std::thread::spawn(move || {
                    (0..10)
                        .map(|_| state.next_client_id(now()).unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut ids: Vec<_> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 80, "concurrent allocation produced a duplicate");
    }

    #[test]
    fn the_halt_is_visible_from_every_scope_and_survives_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let ibkr = StateDir::open(dir.path(), "ibkr-paper").unwrap();
        let fake = StateDir::open(dir.path(), "fake-paper").unwrap();
        assert!(fake.halted().is_none());
        ibkr.halt("position mismatch", now()).unwrap();
        let seen = StateDir::open(dir.path(), "fake-paper")
            .unwrap()
            .halted()
            .unwrap();
        assert_eq!(seen.reason, "position mismatch");
        assert!(fake.resume().unwrap());
        assert!(ibkr.halted().is_none());
        assert!(!fake.resume().unwrap());
    }

    #[test]
    fn a_garbled_halt_file_still_halts() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateDir::open(dir.path(), "x").unwrap();
        fs::write(dir.path().join("HALT"), "stop everything").unwrap();
        assert_eq!(state.halted().unwrap().reason, "stop everything");
    }
}
