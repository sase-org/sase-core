//! Versioned ToolRun ledger: records, store, catalog, and fingerprints.

mod canonical;
mod catalog;
mod fingerprint;
mod handoff_wire;
pub mod receipt;
mod store;
mod triage;
mod wire;

pub use catalog::{
    normalize_receipt_policy, normalize_tool_definition, parse_receipt_ttl,
    receipt_ttl_seconds,
};
pub use fingerprint::{canonicalize_tool_fingerprint, unknown_evidence};
pub use handoff_wire::*;
pub use receipt::{
    build_receipt_proof, diff_proof_against_fingerprint, is_safe_relative_path,
    proof_from_json, proof_to_json, receipt_id_for_run, sha256_hex,
    ReceiptProofDirtyWire, ReceiptProofInputMatchWire, ReceiptProofInputWire,
    ReceiptProofRepoWire, ReceiptProofToolchainWire, ReceiptProofWire,
    RECEIPT_MAX_CHANGED_PATHS, RECEIPT_MAX_TTL_SECONDS, RECEIPT_POLICY_VERSION,
};
pub use store::{
    append_event, begin, claim, finish, list_runs, observe, receipt_lookup,
    receipt_settle, reconcile, request_stop, retention_apply,
    retention_preview, show_run, store_stats, summarize, tool_run_failures,
    tool_run_receipts_report, triage_record, triage_settle, triage_show,
    triage_stage,
};
pub use triage::*;
pub use triage::{
    compare_triage_signatures, extract_triage_items,
    TOOL_RUN_TRIAGE_DISPLAY_MAX_CHARS, TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT,
};
pub use wire::*;

use std::fmt;
use std::io;
use std::path::PathBuf;

#[derive(Debug)]
pub enum ToolRunError {
    SchemaVersion { expected: u32, actual: u32 },
    NewerSchema { expected: u32, actual: u32 },
    Invalid { message: String },
    InvalidTransition { from: String, to: String },
    ConflictingEvent { event_id: String, reason: String },
    NotFound { run_id: String },
    DuplicateRun { run_id: String },
    Busy { message: String },
    ReadOnly { message: String },
    Io { message: String },
    Store { message: String },
}

impl ToolRunError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid {
            message: message.into(),
        }
    }

    pub fn store(message: impl Into<String>) -> Self {
        Self::Store {
            message: message.into(),
        }
    }
}

impl fmt::Display for ToolRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaVersion { expected, actual } => {
                write!(
                    formatter,
                    "tool run requires schema_version {expected}, got {actual}"
                )
            }
            Self::NewerSchema { expected, actual } => {
                write!(
                    formatter,
                    "tool run store schema version {actual} is newer than supported version {expected}"
                )
            }
            Self::Invalid { message } => write!(formatter, "{message}"),
            Self::InvalidTransition { from, to } => {
                write!(
                    formatter,
                    "invalid tool run transition from {from} to {to}"
                )
            }
            Self::ConflictingEvent { event_id, reason } => {
                write!(
                    formatter,
                    "conflicting tool run event {event_id}: {reason}"
                )
            }
            Self::NotFound { run_id } => {
                write!(formatter, "tool run {run_id} was not found")
            }
            Self::DuplicateRun { run_id } => {
                write!(formatter, "tool run {run_id} already exists")
            }
            Self::Busy { message } => {
                write!(formatter, "tool run store is busy: {message}")
            }
            Self::ReadOnly { message } => write!(formatter, "{message}"),
            Self::Io { message } | Self::Store { message } => {
                write!(formatter, "{message}")
            }
        }
    }
}

impl std::error::Error for ToolRunError {}

impl From<io::Error> for ToolRunError {
    fn from(error: io::Error) -> Self {
        Self::Io {
            message: error.to_string(),
        }
    }
}

impl From<rusqlite::Error> for ToolRunError {
    fn from(error: rusqlite::Error) -> Self {
        if matches!(
            error.sqlite_error_code(),
            Some(
                rusqlite::ErrorCode::DatabaseBusy
                    | rusqlite::ErrorCode::DatabaseLocked
            )
        ) {
            Self::Busy {
                message: error.to_string(),
            }
        } else {
            Self::Store {
                message: error.to_string(),
            }
        }
    }
}

pub(crate) fn lock_paths(store_path: &std::path::Path) -> (PathBuf, PathBuf) {
    let lock_path = store_path.with_extension("sqlite.lock");
    let holder_path = store_path.with_extension("sqlite.lock.holder");
    (lock_path, holder_path)
}
