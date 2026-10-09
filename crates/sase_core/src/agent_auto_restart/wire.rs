//! Wire records for the update-skew agent auto-restart domain.
//!
//! These types are the stable boundary between the Rust failure
//! classifier / ledger state machine and Python's
//! `sase.core.agent_auto_restart_facade`. Every object carries
//! `schema_version: 1`.

use serde::{Deserialize, Serialize};

pub const AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION: u32 = 1;

/// One link of the exception cause/context chain, mirroring
/// `src/sase/axe/runner_failure_facts.py::_chain_link` exactly.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFailureChainLinkWire {
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub qualname: String,
    #[serde(default)]
    pub module: String,
    #[serde(default)]
    pub message: String,
}

/// Structured `ImportError` fields, mirroring `_extract_import_error`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFailureImportErrorWire {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub missing_symbol: Option<String>,
}

/// Structured module-target `AttributeError` fields, mirroring
/// `_extract_attribute_error`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFailureAttributeErrorWire {
    #[serde(default)]
    pub module: Option<String>,
    #[serde(default)]
    pub attribute: Option<String>,
}

/// One bounded traceback frame, innermost last, mirroring
/// `_extract_frames`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFailureFrameWire {
    #[serde(default)]
    pub file: String,
    #[serde(default)]
    pub function: String,
    #[serde(default)]
    pub line: Option<i64>,
}

/// Structured failure facts, mirroring phase `failure-facts`
/// (`capture_failure_facts`) exactly.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFailureFactsWire {
    pub schema_version: u32,
    #[serde(default)]
    pub captured_at: Option<String>,
    #[serde(default)]
    pub lifecycle_phase: Option<String>,
    #[serde(default)]
    pub exception_chain: Vec<AgentFailureChainLinkWire>,
    #[serde(default)]
    pub import_error: Option<AgentFailureImportErrorWire>,
    #[serde(default)]
    pub attribute_error: Option<AgentFailureAttributeErrorWire>,
    #[serde(default)]
    pub frames: Vec<AgentFailureFrameWire>,
    #[serde(default)]
    pub last_frame_file: Option<String>,
    #[serde(default)]
    pub skew_suspect: bool,
    #[serde(default)]
    pub error_text: Option<String>,
}

/// One managed code root the classifier trusts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoRestartManagedRootWire {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub root: String,
}

/// Caller-supplied classification context: everything the classifier
/// needs beyond structured facts and witnesses.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoRestartContextWire {
    pub schema_version: u32,
    #[serde(default)]
    pub managed_roots: Vec<AutoRestartManagedRootWire>,
    #[serde(default)]
    pub workspace_dir: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub kill_source: Option<String>,
    #[serde(default)]
    pub lifecycle_phase: Option<String>,
    #[serde(default)]
    pub has_pending_question: bool,
    #[serde(default)]
    pub has_pending_handoff: bool,
    #[serde(default)]
    pub is_remote: bool,
    #[serde(default)]
    pub error_text: String,
    #[serde(default)]
    pub traceback_text: String,
    #[serde(default)]
    pub log_tail: String,
}

/// File-level proof (W3): the symbol or module exists at the boot
/// revision and not at HEAD, or the reverse.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoRestartFileProofWire {
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub module: Option<String>,
    #[serde(default)]
    pub boot_has: Option<bool>,
    #[serde(default)]
    pub head_has: Option<bool>,
    #[serde(default)]
    pub culprit_commit: Option<String>,
    #[serde(default)]
    pub culprit_subject: Option<String>,
}

/// Fresh-interpreter probe result (W4).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoRestartProbeWire {
    #[serde(default)]
    pub ok: bool,
    #[serde(default)]
    pub failures: Vec<String>,
}

/// The refresh log line parsed from the runner log tail.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoRestartRefreshLogLineWire {
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub to: String,
}

/// Witness bundle W1-W4 plus the refresh log line.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoRestartWitnessesWire {
    pub schema_version: u32,
    #[serde(default)]
    pub boot_identity: Option<String>,
    #[serde(default)]
    pub current_identity: Option<String>,
    #[serde(default)]
    pub journal_updates: Vec<String>,
    #[serde(default)]
    pub file_proof: Option<AutoRestartFileProofWire>,
    #[serde(default)]
    pub probe: Option<AutoRestartProbeWire>,
    #[serde(default)]
    pub refresh_log_line: Option<AutoRestartRefreshLogLineWire>,
}

/// The classifier verdict.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryVerdictWire {
    pub schema_version: u32,
    #[serde(default)]
    pub tier: String,
    #[serde(default)]
    pub family: String,
    #[serde(default)]
    pub signature: String,
    #[serde(default)]
    pub origin_module: Option<String>,
    #[serde(default)]
    pub missing_symbol: Option<String>,
    #[serde(default)]
    pub phase_class: String,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub reason_text: String,
    #[serde(default)]
    pub witnesses_fired: Vec<String>,
    #[serde(default)]
    pub episode_id: Option<String>,
}

/// One ledger history entry.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoRestartLedgerHistoryWire {
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub at: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
}

/// Durable per-lineage restart ledger record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoRestartLedgerRecordWire {
    pub schema_version: u32,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub lineage_root: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub claimed_at: Option<String>,
    #[serde(default)]
    pub failed_artifacts_dir: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub episode_id: Option<String>,
    #[serde(default)]
    pub planned_name: Option<String>,
    #[serde(default)]
    pub launched_artifacts_dir: Option<String>,
    #[serde(default)]
    pub evidence_dir: Option<String>,
    #[serde(default)]
    pub decline_reason: Option<String>,
    #[serde(default)]
    pub deferrals: u32,
    #[serde(default)]
    pub history: Vec<AutoRestartLedgerHistoryWire>,
}

/// Derived update episode identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoRestartEpisodeWire {
    pub schema_version: u32,
    pub id: String,
    pub slug: String,
    #[serde(default)]
    pub culprit_short: Option<String>,
    #[serde(default)]
    pub from_rev: Option<String>,
    #[serde(default)]
    pub to_rev: Option<String>,
    pub label: String,
}

/// The `done.json` recovery object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRecoveryWire {
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub reason_text: Option<String>,
    #[serde(default)]
    pub requested_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub episode_id: Option<String>,
    #[serde(default)]
    pub ledger_key: Option<String>,
}
