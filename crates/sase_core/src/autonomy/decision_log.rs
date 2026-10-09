//! Host decision log store: `<sase_home>/autonomy/decisions.jsonl`.
//!
//! One append per evaluated gate, bounded rotation with a single `.1`
//! segment, a sibling lock, and torn-last-line tolerance on reads —
//! the same contract as the Python `append_jsonl_record` helper.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use thiserror::Error;

use super::wires::{AutonomyLogEntryWire, AutonomyLogQueryWire};

/// Directory under the sase home holding the decision log.
pub const AUTONOMY_LOG_DIR: &str = "autonomy";
/// Live decision log file name.
pub const AUTONOMY_DECISIONS_FILE: &str = "decisions.jsonl";
/// Rotated (previous segment) file name.
pub const AUTONOMY_DECISIONS_ROTATED: &str = "decisions.jsonl.1";
/// Size cap mirroring the bounded Python log contract (~2 MiB).
pub const AUTONOMY_LOG_MAX_BYTES: u64 = 2 * 1024 * 1024;

/// Decision log store failures.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AutonomyLogError {
    /// Filesystem failure; carries the path and the OS message.
    #[error("autonomy decision log io error at {path}: {message}")]
    Io { path: String, message: String },
    /// The entry could not be serialized to one JSON line.
    #[error("autonomy decision log serialize error: {message}")]
    Serialize { message: String },
}

fn io_error(path: &Path, error: std::io::Error) -> AutonomyLogError {
    AutonomyLogError::Io {
        path: path.display().to_string(),
        message: error.to_string(),
    }
}

fn log_paths(home: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let dir = home.join(AUTONOMY_LOG_DIR);
    let live = dir.join(AUTONOMY_DECISIONS_FILE);
    let rotated = dir.join(AUTONOMY_DECISIONS_ROTATED);
    let lock = dir.join(format!(".{AUTONOMY_DECISIONS_FILE}.lock"));
    (live, rotated, lock)
}

/// Append one decision entry, rotating the log past `max_bytes` by
/// moving the live segment to `decisions.jsonl.1` first. The entry is
/// fully serialized before the lock is taken, so one append is one
/// `O_APPEND` write.
pub fn append_autonomy_decision_with_cap(
    home: &Path,
    entry: &AutonomyLogEntryWire,
    max_bytes: u64,
) -> Result<(), AutonomyLogError> {
    let mut encoded = serde_json::to_string(entry).map_err(|error| {
        AutonomyLogError::Serialize {
            message: error.to_string(),
        }
    })?;
    encoded.push('\n');
    let bytes = encoded.into_bytes();
    let (live, rotated, lock) = log_paths(home);
    if let Some(parent) = live.parent() {
        fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
    }
    let lock_file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock)
        .map_err(|error| io_error(&lock, error))?;
    lock_file
        .lock_exclusive()
        .map_err(|error| io_error(&lock, error))?;
    let outcome = (|| -> Result<(), AutonomyLogError> {
        let current = fs::metadata(&live).map(|meta| meta.len()).unwrap_or(0);
        if current + bytes.len() as u64 > max_bytes {
            if fs::metadata(&rotated).is_ok() {
                fs::remove_file(&rotated)
                    .map_err(|error| io_error(&rotated, error))?;
            }
            if fs::metadata(&live).is_ok() {
                fs::rename(&live, &rotated)
                    .map_err(|error| io_error(&live, error))?;
            }
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&live)
            .map_err(|error| io_error(&live, error))?;
        file.write_all(&bytes)
            .map_err(|error| io_error(&live, error))?;
        Ok(())
    })();
    let _ = FileExt::unlock(&lock_file);
    outcome
}

/// Append one decision entry with the default size cap.
pub fn append_autonomy_decision(
    home: &Path,
    entry: &AutonomyLogEntryWire,
) -> Result<(), AutonomyLogError> {
    append_autonomy_decision_with_cap(home, entry, AUTONOMY_LOG_MAX_BYTES)
}

fn parse_segment(path: &Path) -> Vec<AutonomyLogEntryWire> {
    let text = fs::read_to_string(path).unwrap_or_default();
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            // Tolerate a torn last line (or any corrupt line): a
            // concurrent crash mid-write must not lose the segment.
            serde_json::from_str(line).ok()
        })
        .collect()
}

fn matches(query: &AutonomyLogQueryWire, entry: &AutonomyLogEntryWire) -> bool {
    if let Some(since) = query.since.as_deref().filter(|s| !s.is_empty()) {
        if entry.at.as_str() < since {
            return false;
        }
    }
    if let Some(agent) = query.agent.as_deref().filter(|s| !s.is_empty()) {
        if entry.agent != agent {
            return false;
        }
    }
    if let Some(kind) = query.gate_kind.as_deref().filter(|s| !s.is_empty()) {
        if entry.gate_kind != kind {
            return false;
        }
    }
    if let Some(outcome) = query.outcome.as_deref().filter(|s| !s.is_empty()) {
        if entry.decision.outcome != outcome {
            return false;
        }
    }
    true
}

/// Read decision entries newest first across both segments. Missing
/// files read as empty; no lock is taken.
pub fn read_autonomy_decisions(
    home: &Path,
    query: &AutonomyLogQueryWire,
) -> Result<Vec<AutonomyLogEntryWire>, AutonomyLogError> {
    let dir = home.join(AUTONOMY_LOG_DIR);
    let live = dir.join(AUTONOMY_DECISIONS_FILE);
    let rotated = dir.join(AUTONOMY_DECISIONS_ROTATED);
    let mut entries = parse_segment(&rotated);
    entries.extend(parse_segment(&live));
    entries.reverse();
    let mut kept: Vec<AutonomyLogEntryWire> =
        entries.into_iter().filter(|e| matches(query, e)).collect();
    if let Some(limit) = query.limit {
        kept.truncate(limit);
    }
    Ok(kept)
}
