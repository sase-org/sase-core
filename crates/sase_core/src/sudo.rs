//! Sudo request manifest, risk, and execution-ledger contracts.
//!
//! This module is deliberately transport- and process-free. It owns the
//! stable JSON shapes, validation, canonical manifest bytes, digest binding,
//! deterministic risk badges, and ledger consistency checks consumed by the
//! unprivileged gateway runner and Python bindings.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const SUDO_MANIFEST_WIRE_SCHEMA_VERSION: u32 = 1;
pub const SUDO_LEDGER_WIRE_SCHEMA_VERSION: u32 = 1;
pub const SUDO_RISK_WIRE_SCHEMA_VERSION: u32 = 1;
pub const SUDO_EXEC_STARTED_WIRE_SCHEMA_VERSION: u32 = 1;
pub const SUDO_ATTEMPT_WIRE_SCHEMA_VERSION: u32 = 1;
pub const SUDO_SETTLEMENT_AUTHORIZATION_WIRE_SCHEMA_VERSION: u32 = 1;
pub const SUDO_EXEC_STARTED_KIND: &str = "sudo_exec_started";

pub const SUDO_MANIFEST_MAX_BYTES: usize = 64 * 1024;
pub const SUDO_LEDGER_MAX_BYTES: usize = 256 * 1024;
pub const SUDO_EXEC_STARTED_MAX_BYTES: usize = 16 * 1024;
pub const SUDO_ATTEMPT_MAX_BYTES: usize = 64 * 1024;
pub const SUDO_SETTLEMENT_AUTHORIZATION_MAX_BYTES: usize = 128 * 1024;
pub const SUDO_MAX_COMMANDS: usize = 128;
pub const SUDO_MAX_ARGV: usize = 64;
pub const SUDO_MAX_ENV: usize = 64;
pub const SUDO_MAX_ID_BYTES: usize = 128;
pub const SUDO_MAX_LABEL_BYTES: usize = 256;
pub const SUDO_MAX_PATH_BYTES: usize = 1024;
pub const SUDO_MAX_ENV_VALUE_BYTES: usize = 4096;
pub const SUDO_MAX_WHY_BYTES: usize = 2048;
pub const SUDO_MAX_OUTPUT_TAIL_BYTES: usize = 64 * 1024;
pub const SUDO_MAX_DIAGNOSTIC_BYTES: usize = 2048;
pub const SUDO_MAX_TIMEOUT_SECONDS: f64 = 24.0 * 60.0 * 60.0;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SudoErrorCodeWire {
    Json,
    UnsupportedSchema,
    Validation,
    DigestMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SudoErrorWire {
    pub schema_version: u32,
    pub code: SudoErrorCodeWire,
    pub message: String,
    pub target: Option<String>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{message}")]
pub struct SudoWireError {
    pub code: SudoErrorCodeWire,
    pub message: String,
    pub target: Option<String>,
}

impl SudoWireError {
    pub fn wire(&self) -> SudoErrorWire {
        SudoErrorWire {
            schema_version: SUDO_MANIFEST_WIRE_SCHEMA_VERSION,
            code: self.code.clone(),
            message: self.message.clone(),
            target: self.target.clone(),
        }
    }

    fn json(message: impl Into<String>) -> Self {
        Self {
            code: SudoErrorCodeWire::Json,
            message: message.into(),
            target: None,
        }
    }

    fn unsupported_schema(kind: &str, actual: u32) -> Self {
        Self {
            code: SudoErrorCodeWire::UnsupportedSchema,
            message: format!("{kind} schema_version {actual} is not supported"),
            target: Some("schema_version".to_string()),
        }
    }

    fn validation(
        target: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: SudoErrorCodeWire::Validation,
            message: message.into(),
            target: Some(target.into()),
        }
    }

    pub fn digest_mismatch(expected: &str, actual: &str) -> Self {
        Self {
            code: SudoErrorCodeWire::DigestMismatch,
            message: format!(
                "sudo manifest SHA-256 mismatch: expected {expected}, got {actual}"
            ),
            target: Some("expected_sha256".to_string()),
        }
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SudoOutputPolicyWire {
    #[default]
    None,
    Tail,
    Full,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SudoManifestWire {
    pub schema_version: u32,
    pub request_id: String,
    pub host: String,
    #[serde(default)]
    pub host_is_remote: bool,
    pub run_as: String,
    pub cwd: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default = "default_stop_on_failure")]
    pub stop_on_failure: bool,
    #[serde(default)]
    pub output_to_agent: SudoOutputPolicyWire,
    pub commands: Vec<SudoCommandWire>,
    #[serde(default)]
    pub resume_from: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SudoCommandWire {
    pub id: String,
    pub argv: Vec<String>,
    pub why: String,
    #[serde(default)]
    pub timeout_seconds: Option<f64>,
    #[serde(default)]
    pub shell: bool,
}

fn default_stop_on_failure() -> bool {
    true
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum SudoRiskBadgeKindWire {
    #[serde(rename = "shell")]
    Shell,
    #[serde(rename = "network")]
    Network,
    #[serde(rename = "package-manager")]
    PackageManager,
    #[serde(rename = "system-path-write")]
    SystemPathWrite,
    #[serde(rename = "service-restart")]
    ServiceRestart,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SudoRiskAssessmentWire {
    pub schema_version: u32,
    pub command_id: String,
    pub badges: Vec<SudoRiskBadgeKindWire>,
    pub lockout_prone: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SudoLedgerOutcomeWire {
    Completed,
    AuthFailed,
    Cancelled,
    TtyUnavailable,
    RunnerError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SudoLedgerEntryStatusWire {
    Ran,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SudoLedgerWire {
    pub schema_version: u32,
    pub request_id: String,
    pub manifest_sha256: String,
    pub outcome: SudoLedgerOutcomeWire,
    pub entries: Vec<SudoLedgerEntryWire>,
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SudoLedgerEntryWire {
    pub id: String,
    pub status: SudoLedgerEntryStatusWire,
    pub exit_code: Option<i32>,
    pub duration_seconds: f64,
    pub output_tail: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SudoExecStartedWire {
    pub schema_version: u32,
    pub kind: String,
    pub manifest_sha256: String,
    pub executor_pid: u32,
    pub executor_identity: String,
    pub ledger_path: String,
    pub log_path: String,
    pub started_at: f64,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SudoAttemptTargetKindWire {
    Local,
    Remote,
    #[default]
    Unknown,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SudoAttemptStartupStateWire {
    Legacy,
    Reserved,
    Authenticating,
    Starting,
    Started,
    Settling,
    Settled,
    Terminal,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SudoExecutorLivenessWire {
    Live,
    Dead,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SudoRemoteHandoffWire {
    pub directory: String,
    pub handshake: String,
    pub ledger: String,
    pub log: String,
    pub manifest: String,
    pub stop: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SudoAttemptWire {
    pub schema_version: u32,
    pub gate_id: String,
    #[serde(default)]
    pub selected_command_ids: Vec<String>,
    pub manifest_sha256: String,
    pub handoff_dir: String,
    #[serde(default)]
    pub handshake: Option<SudoExecStartedWire>,
    #[serde(default)]
    pub finalize_proc_id: Option<String>,
    #[serde(default)]
    pub target_kind: SudoAttemptTargetKindWire,
    #[serde(default)]
    pub target_host: Option<String>,
    #[serde(default)]
    pub startup_state: SudoAttemptStartupStateWire,
    #[serde(default)]
    pub operation_payload_digest: Option<String>,
    #[serde(default)]
    pub authorization_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_handoff: Option<SudoRemoteHandoffWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SudoExecutorFactsWire {
    #[serde(default)]
    pub finalize_proc_live: Option<bool>,
    #[serde(default)]
    pub executor_pid_live: Option<bool>,
    #[serde(default)]
    pub executor_identity_matches: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SudoExecutorLivenessDecisionWire {
    pub schema_version: u32,
    pub classification: SudoExecutorLivenessWire,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SudoSettlementAuthorizationWire {
    pub schema_version: u32,
    pub authorized: bool,
    pub status: String,
    pub authorization_id: String,
    pub gate_id: String,
    pub selected_command_ids: Vec<String>,
    pub manifest_sha256: String,
    pub ledger_outcome: SudoLedgerOutcomeWire,
}

pub fn sudo_manifest_from_json_value(
    value: &JsonValue,
) -> Result<SudoManifestWire, SudoWireError> {
    ensure_json_size("sudo manifest", value, SUDO_MANIFEST_MAX_BYTES)?;
    let manifest: SudoManifestWire = serde_json::from_value(value.clone())
        .map_err(|error| {
            SudoWireError::json(format!(
                "sudo manifest JSON does not match wire contract: {error}"
            ))
        })?;
    validate_sudo_manifest(&manifest)
}

pub fn sudo_manifest_from_json_slice(
    bytes: &[u8],
) -> Result<SudoManifestWire, SudoWireError> {
    if bytes.len() > SUDO_MANIFEST_MAX_BYTES {
        return Err(SudoWireError::validation(
            "manifest",
            format!("sudo manifest exceeds {SUDO_MANIFEST_MAX_BYTES} bytes"),
        ));
    }
    let value: JsonValue = serde_json::from_slice(bytes).map_err(|error| {
        SudoWireError::json(format!("sudo manifest is not valid JSON: {error}"))
    })?;
    sudo_manifest_from_json_value(&value)
}

pub fn validate_sudo_manifest(
    manifest: &SudoManifestWire,
) -> Result<SudoManifestWire, SudoWireError> {
    validate_schema(
        "sudo manifest",
        manifest.schema_version,
        SUDO_MANIFEST_WIRE_SCHEMA_VERSION,
    )?;
    validate_non_empty_bounded(
        "request_id",
        &manifest.request_id,
        SUDO_MAX_ID_BYTES,
    )?;
    reject_path_like("request_id", &manifest.request_id)?;
    validate_non_empty_bounded("host", &manifest.host, SUDO_MAX_LABEL_BYTES)?;
    validate_non_empty_bounded(
        "run_as",
        &manifest.run_as,
        SUDO_MAX_LABEL_BYTES,
    )?;
    reject_path_like("run_as", &manifest.run_as)?;
    validate_absolute_path("cwd", &manifest.cwd)?;
    validate_env(&manifest.env)?;
    if manifest.commands.is_empty() {
        return Err(SudoWireError::validation(
            "commands",
            "sudo manifest requires at least one command",
        ));
    }
    if manifest.commands.len() > SUDO_MAX_COMMANDS {
        return Err(SudoWireError::validation(
            "commands",
            format!(
                "sudo manifest commands exceeds {SUDO_MAX_COMMANDS} entries"
            ),
        ));
    }
    let mut ids = BTreeSet::new();
    for (index, command) in manifest.commands.iter().enumerate() {
        validate_command(command, index)?;
        if !ids.insert(command.id.clone()) {
            return Err(SudoWireError::validation(
                format!("commands[{index}].id"),
                format!("duplicate sudo command id {:?}", command.id),
            ));
        }
    }
    if let Some(resume_from) = &manifest.resume_from {
        validate_non_empty_bounded(
            "resume_from",
            resume_from,
            SUDO_MAX_ID_BYTES,
        )?;
        if !ids.contains(resume_from) {
            return Err(SudoWireError::validation(
                "resume_from",
                format!(
                    "resume_from command id {resume_from:?} does not exist"
                ),
            ));
        }
    }
    Ok(manifest.clone())
}

pub fn sudo_manifest_canonical_json_bytes(
    manifest: &SudoManifestWire,
) -> Result<Vec<u8>, SudoWireError> {
    let manifest = validate_sudo_manifest(manifest)?;
    let value = serde_json::to_value(&manifest).map_err(|error| {
        SudoWireError::json(format!(
            "unable to serialize sudo manifest for hashing: {error}"
        ))
    })?;
    serde_json::to_vec(&canonical_json_value(&value)).map_err(|error| {
        SudoWireError::json(format!(
            "unable to encode canonical sudo manifest JSON: {error}"
        ))
    })
}

pub fn sudo_manifest_sha256(
    manifest: &SudoManifestWire,
) -> Result<String, SudoWireError> {
    let bytes = sudo_manifest_canonical_json_bytes(manifest)?;
    Ok(hex::encode(Sha256::digest(&bytes)))
}

pub fn sudo_manifest_json_sha256(
    value: &JsonValue,
) -> Result<String, SudoWireError> {
    let manifest = sudo_manifest_from_json_value(value)?;
    sudo_manifest_sha256(&manifest)
}

pub fn derive_sudo_risk_badges(
    manifest: &SudoManifestWire,
) -> Result<Vec<SudoRiskAssessmentWire>, SudoWireError> {
    let manifest = validate_sudo_manifest(manifest)?;
    Ok(manifest
        .commands
        .iter()
        .map(|command| {
            let mut badges = Vec::new();
            let mut seen = BTreeSet::new();
            for badge in [
                SudoRiskBadgeKindWire::Shell,
                SudoRiskBadgeKindWire::Network,
                SudoRiskBadgeKindWire::PackageManager,
                SudoRiskBadgeKindWire::SystemPathWrite,
                SudoRiskBadgeKindWire::ServiceRestart,
            ] {
                if command_has_badge(command, badge) && seen.insert(badge) {
                    badges.push(badge);
                }
            }
            SudoRiskAssessmentWire {
                schema_version: SUDO_RISK_WIRE_SCHEMA_VERSION,
                command_id: command.id.clone(),
                badges,
                lockout_prone: manifest.host_is_remote
                    && service_restart_lockout_prone(command),
            }
        })
        .collect())
}

pub fn sudo_ledger_from_json_value(
    value: &JsonValue,
) -> Result<SudoLedgerWire, SudoWireError> {
    ensure_json_size("sudo ledger", value, SUDO_LEDGER_MAX_BYTES)?;
    let ledger: SudoLedgerWire = serde_json::from_value(value.clone())
        .map_err(|error| {
            SudoWireError::json(format!(
                "sudo ledger JSON does not match wire contract: {error}"
            ))
        })?;
    validate_sudo_ledger(&ledger, None)
}

pub fn sudo_validate_ledger_json_value(
    ledger: &JsonValue,
    manifest: Option<&JsonValue>,
) -> Result<SudoLedgerWire, SudoWireError> {
    let parsed_manifest =
        manifest.map(sudo_manifest_from_json_value).transpose()?;
    let ledger: SudoLedgerWire = serde_json::from_value(ledger.clone())
        .map_err(|error| {
            SudoWireError::json(format!(
                "sudo ledger JSON does not match wire contract: {error}"
            ))
        })?;
    validate_sudo_ledger(&ledger, parsed_manifest.as_ref())
}

pub fn validate_sudo_ledger(
    ledger: &SudoLedgerWire,
    manifest: Option<&SudoManifestWire>,
) -> Result<SudoLedgerWire, SudoWireError> {
    validate_schema(
        "sudo ledger",
        ledger.schema_version,
        SUDO_LEDGER_WIRE_SCHEMA_VERSION,
    )?;
    validate_non_empty_bounded(
        "request_id",
        &ledger.request_id,
        SUDO_MAX_ID_BYTES,
    )?;
    validate_sha256("manifest_sha256", &ledger.manifest_sha256)?;
    if let Some(diagnostic) = &ledger.diagnostic {
        if ledger.outcome != SudoLedgerOutcomeWire::RunnerError {
            return Err(SudoWireError::validation(
                "diagnostic",
                "sudo ledger diagnostic is only allowed for runner_error outcomes",
            ));
        }
        validate_bounded("diagnostic", diagnostic, SUDO_MAX_DIAGNOSTIC_BYTES)?;
    }
    if ledger.entries.len() > SUDO_MAX_COMMANDS {
        return Err(SudoWireError::validation(
            "entries",
            format!("sudo ledger entries exceeds {SUDO_MAX_COMMANDS} entries"),
        ));
    }
    let mut seen = BTreeSet::new();
    for (index, entry) in ledger.entries.iter().enumerate() {
        validate_ledger_entry(entry, index)?;
        if !seen.insert(entry.id.clone()) {
            return Err(SudoWireError::validation(
                format!("entries[{index}].id"),
                format!("duplicate sudo ledger entry id {:?}", entry.id),
            ));
        }
    }
    if let Some(manifest) = manifest {
        let manifest = validate_sudo_manifest(manifest)?;
        if ledger.request_id != manifest.request_id {
            return Err(SudoWireError::validation(
                "request_id",
                "sudo ledger request_id does not match manifest",
            ));
        }
        let expected = sudo_manifest_sha256(&manifest)?;
        if ledger.manifest_sha256 != expected {
            return Err(SudoWireError::validation(
                "manifest_sha256",
                "sudo ledger manifest_sha256 does not match manifest",
            ));
        }
        if ledger.entries.len() != manifest.commands.len() {
            return Err(SudoWireError::validation(
                "entries",
                "sudo ledger must contain exactly one entry per manifest command",
            ));
        }
        for (index, (entry, command)) in ledger
            .entries
            .iter()
            .zip(manifest.commands.iter())
            .enumerate()
        {
            if entry.id != command.id {
                return Err(SudoWireError::validation(
                    format!("entries[{index}].id"),
                    "sudo ledger command order does not match manifest",
                ));
            }
        }
    }
    Ok(ledger.clone())
}

pub fn sudo_exec_started_from_json_value(
    value: &JsonValue,
) -> Result<SudoExecStartedWire, SudoWireError> {
    ensure_json_size(
        "sudo exec started handshake",
        value,
        SUDO_EXEC_STARTED_MAX_BYTES,
    )?;
    let handshake: SudoExecStartedWire = serde_json::from_value(value.clone())
        .map_err(|error| {
            SudoWireError::json(format!(
                "sudo exec started handshake JSON does not match wire contract: {error}"
            ))
        })?;
    validate_sudo_exec_started(&handshake, None)
}

pub fn sudo_validate_exec_started_json_value(
    handshake: &JsonValue,
    manifest: Option<&JsonValue>,
) -> Result<SudoExecStartedWire, SudoWireError> {
    ensure_json_size(
        "sudo exec started handshake",
        handshake,
        SUDO_EXEC_STARTED_MAX_BYTES,
    )?;
    let parsed_manifest =
        manifest.map(sudo_manifest_from_json_value).transpose()?;
    let handshake: SudoExecStartedWire =
        serde_json::from_value(handshake.clone()).map_err(|error| {
            SudoWireError::json(format!(
                "sudo exec started handshake JSON does not match wire contract: {error}"
            ))
        })?;
    validate_sudo_exec_started(&handshake, parsed_manifest.as_ref())
}

pub fn validate_sudo_exec_started(
    handshake: &SudoExecStartedWire,
    manifest: Option<&SudoManifestWire>,
) -> Result<SudoExecStartedWire, SudoWireError> {
    validate_schema(
        "sudo exec started handshake",
        handshake.schema_version,
        SUDO_EXEC_STARTED_WIRE_SCHEMA_VERSION,
    )?;
    if handshake.kind != SUDO_EXEC_STARTED_KIND {
        return Err(SudoWireError::validation(
            "kind",
            format!(
                "sudo exec started kind must be {SUDO_EXEC_STARTED_KIND:?}"
            ),
        ));
    }
    validate_sha256("manifest_sha256", &handshake.manifest_sha256)?;
    if let Some(manifest) = manifest {
        let manifest = validate_sudo_manifest(manifest)?;
        let expected = sudo_manifest_sha256(&manifest)?;
        if handshake.manifest_sha256 != expected {
            return Err(SudoWireError::digest_mismatch(
                &expected,
                &handshake.manifest_sha256,
            ));
        }
    }
    if handshake.executor_pid == 0 || handshake.executor_pid > i32::MAX as u32 {
        return Err(SudoWireError::validation(
            "executor_pid",
            "sudo exec started executor_pid must be a positive platform PID",
        ));
    }
    validate_process_identity(&handshake.executor_identity)?;
    validate_absolute_path("ledger_path", &handshake.ledger_path)?;
    validate_absolute_path("log_path", &handshake.log_path)?;
    if handshake.ledger_path == handshake.log_path {
        return Err(SudoWireError::validation(
            "log_path",
            "sudo exec started ledger_path and log_path must differ",
        ));
    }
    validate_positive_timestamp("started_at", handshake.started_at)?;
    Ok(handshake.clone())
}

pub fn sudo_attempt_from_json_value(
    value: &JsonValue,
) -> Result<SudoAttemptWire, SudoWireError> {
    ensure_json_size("sudo attempt", value, SUDO_ATTEMPT_MAX_BYTES)?;
    let mut attempt: SudoAttemptWire = serde_json::from_value(value.clone())
        .map_err(|error| {
            SudoWireError::json(format!(
                "sudo attempt JSON does not match wire contract: {error}"
            ))
        })?;
    let object = value.as_object().ok_or_else(|| {
        SudoWireError::validation("attempt", "sudo attempt must be an object")
    })?;
    if !object.contains_key("target_kind")
        || !object.contains_key("startup_state")
    {
        attempt.startup_state = SudoAttemptStartupStateWire::Unknown;
        if !object.contains_key("target_kind") {
            attempt.target_kind = SudoAttemptTargetKindWire::Unknown;
        }
    }
    validate_sudo_attempt(&attempt)
}

pub fn validate_sudo_attempt(
    attempt: &SudoAttemptWire,
) -> Result<SudoAttemptWire, SudoWireError> {
    validate_schema(
        "sudo attempt",
        attempt.schema_version,
        SUDO_ATTEMPT_WIRE_SCHEMA_VERSION,
    )?;
    validate_non_empty_bounded("gate_id", &attempt.gate_id, SUDO_MAX_ID_BYTES)?;
    validate_sha256("manifest_sha256", &attempt.manifest_sha256)?;
    validate_absolute_path("handoff_dir", &attempt.handoff_dir)?;
    if attempt.selected_command_ids.is_empty() {
        return Err(SudoWireError::validation(
            "selected_command_ids",
            "sudo attempt requires at least one selected command id",
        ));
    }
    if attempt.selected_command_ids.len() > SUDO_MAX_COMMANDS {
        return Err(SudoWireError::validation(
            "selected_command_ids",
            format!(
                "sudo attempt selected_command_ids exceeds {SUDO_MAX_COMMANDS} entries"
            ),
        ));
    }
    let mut ids = BTreeSet::new();
    for (index, id) in attempt.selected_command_ids.iter().enumerate() {
        validate_non_empty_bounded(
            format!("selected_command_ids[{index}]"),
            id,
            SUDO_MAX_ID_BYTES,
        )?;
        reject_path_like(format!("selected_command_ids[{index}]"), id)?;
        if !ids.insert(id.clone()) {
            return Err(SudoWireError::validation(
                format!("selected_command_ids[{index}]"),
                format!("duplicate sudo selected command id {id:?}"),
            ));
        }
    }
    if let Some(handshake) = &attempt.handshake {
        validate_sudo_exec_started(handshake, None)?;
        if handshake.manifest_sha256 != attempt.manifest_sha256 {
            return Err(SudoWireError::validation(
                "handshake.manifest_sha256",
                "sudo attempt handshake digest does not match attempt",
            ));
        }
    }
    if let Some(proc_id) = &attempt.finalize_proc_id {
        validate_non_empty_bounded(
            "finalize_proc_id",
            proc_id,
            SUDO_MAX_LABEL_BYTES,
        )?;
    }
    match attempt.target_kind {
        SudoAttemptTargetKindWire::Remote => {
            let host = attempt.target_host.as_deref().ok_or_else(|| {
                SudoWireError::validation(
                    "target_host",
                    "remote sudo attempts require target_host",
                )
            })?;
            validate_non_empty_bounded(
                "target_host",
                host,
                SUDO_MAX_LABEL_BYTES,
            )?;
        }
        SudoAttemptTargetKindWire::Local
        | SudoAttemptTargetKindWire::Unknown => {
            if let Some(host) = &attempt.target_host {
                validate_bounded("target_host", host, SUDO_MAX_LABEL_BYTES)?;
            }
        }
    }
    if let Some(digest) = &attempt.operation_payload_digest {
        validate_sha256("operation_payload_digest", digest)?;
    }
    if let Some(authorization_id) = &attempt.authorization_id {
        validate_sha256("authorization_id", authorization_id)?;
    }
    if let Some(remote_handoff) = &attempt.remote_handoff {
        validate_sudo_remote_handoff(attempt, remote_handoff)?;
    }
    Ok(attempt.clone())
}

fn validate_sudo_remote_handoff(
    attempt: &SudoAttemptWire,
    remote_handoff: &SudoRemoteHandoffWire,
) -> Result<(), SudoWireError> {
    if !matches!(attempt.target_kind, SudoAttemptTargetKindWire::Remote) {
        return Err(SudoWireError::validation(
            "remote_handoff",
            "sudo remote handoff metadata requires target_kind remote",
        ));
    }
    let host = attempt.target_host.as_deref().ok_or_else(|| {
        SudoWireError::validation(
            "target_host",
            "sudo remote handoff metadata requires target_host",
        )
    })?;
    validate_non_empty_bounded("target_host", host, SUDO_MAX_LABEL_BYTES)?;
    validate_safe_absolute_path(
        "remote_handoff.directory",
        &remote_handoff.directory,
    )?;
    let files = [
        ("remote_handoff.handshake", &remote_handoff.handshake),
        ("remote_handoff.ledger", &remote_handoff.ledger),
        ("remote_handoff.log", &remote_handoff.log),
        ("remote_handoff.manifest", &remote_handoff.manifest),
        ("remote_handoff.stop", &remote_handoff.stop),
    ];
    let mut unique = BTreeSet::new();
    unique.insert(remote_handoff.directory.clone());
    for (target, path) in files {
        validate_safe_absolute_path(target, path)?;
        if !path_is_within(&remote_handoff.directory, path) {
            return Err(SudoWireError::validation(
                target,
                "sudo remote handoff path must be inside the remote directory",
            ));
        }
        if !unique.insert((*path).clone()) {
            return Err(SudoWireError::validation(
                target,
                "sudo remote handoff paths must be unique",
            ));
        }
    }
    Ok(())
}

pub fn classify_sudo_executor_liveness(
    attempt: &SudoAttemptWire,
    facts: &SudoExecutorFactsWire,
) -> Result<SudoExecutorLivenessDecisionWire, SudoWireError> {
    let attempt = validate_sudo_attempt(attempt)?;
    if facts.finalize_proc_live == Some(true) {
        return Ok(sudo_liveness_decision(
            SudoExecutorLivenessWire::Live,
            "finalize proc is live",
        ));
    }
    if matches!(attempt.startup_state, SudoAttemptStartupStateWire::Settled) {
        return Ok(sudo_liveness_decision(
            SudoExecutorLivenessWire::Dead,
            "attempt is already settled",
        ));
    }
    if matches!(attempt.startup_state, SudoAttemptStartupStateWire::Terminal) {
        return Ok(sudo_liveness_decision(
            SudoExecutorLivenessWire::Dead,
            "attempt ended before a durable executor owned it",
        ));
    }
    if matches!(attempt.target_kind, SudoAttemptTargetKindWire::Remote) {
        return Ok(sudo_liveness_decision(
            SudoExecutorLivenessWire::Unknown,
            "remote executor liveness is durable but not locally provable",
        ));
    }
    if matches!(attempt.target_kind, SudoAttemptTargetKindWire::Unknown) {
        return Ok(sudo_liveness_decision(
            SudoExecutorLivenessWire::Unknown,
            "attempt target is unknown",
        ));
    }
    if matches!(
        attempt.startup_state,
        SudoAttemptStartupStateWire::Reserved
            | SudoAttemptStartupStateWire::Authenticating
            | SudoAttemptStartupStateWire::Starting
            | SudoAttemptStartupStateWire::Unknown
    ) {
        return Ok(sudo_liveness_decision(
            SudoExecutorLivenessWire::Unknown,
            "attempt startup has not reached a terminal proof",
        ));
    }
    if attempt.handshake.is_none() {
        if matches!(attempt.startup_state, SudoAttemptStartupStateWire::Legacy)
        {
            return Ok(sudo_liveness_decision(
                SudoExecutorLivenessWire::Dead,
                "legacy local attempt has no executor witness",
            ));
        }
        return Ok(sudo_liveness_decision(
            SudoExecutorLivenessWire::Unknown,
            "started attempt is missing executor witness",
        ));
    }
    match (facts.executor_pid_live, facts.executor_identity_matches) {
        (Some(true), Some(true)) => Ok(sudo_liveness_decision(
            SudoExecutorLivenessWire::Live,
            "executor process identity still matches",
        )),
        (Some(false), _) | (_, Some(false)) => Ok(sudo_liveness_decision(
            SudoExecutorLivenessWire::Dead,
            "executor process identity is not live",
        )),
        _ => Ok(sudo_liveness_decision(
            SudoExecutorLivenessWire::Unknown,
            "executor process facts are incomplete",
        )),
    }
}

pub fn authorize_sudo_settlement_json_value(
    value: &JsonValue,
) -> Result<SudoSettlementAuthorizationWire, SudoWireError> {
    ensure_json_size(
        "sudo settlement authorization",
        value,
        SUDO_SETTLEMENT_AUTHORIZATION_MAX_BYTES,
    )?;
    let object = value.as_object().ok_or_else(|| {
        SudoWireError::validation(
            "authorization",
            "sudo settlement authorization request must be an object",
        )
    })?;
    let attempt_value = object.get("attempt").ok_or_else(|| {
        SudoWireError::validation("attempt", "sudo attempt is required")
    })?;
    let attempt = sudo_attempt_from_json_value(attempt_value)?;
    let ledger_value = object.get("ledger").ok_or_else(|| {
        SudoWireError::validation("ledger", "terminal sudo ledger is required")
    })?;
    let ledger = sudo_ledger_from_json_value(ledger_value)?;
    if ledger.manifest_sha256 != attempt.manifest_sha256 {
        return Err(SudoWireError::validation(
            "ledger.manifest_sha256",
            "sudo ledger digest does not match attempt",
        ));
    }
    if ledger.request_id != attempt.gate_id {
        return Err(SudoWireError::validation(
            "ledger.request_id",
            "sudo ledger request id does not match attempt",
        ));
    }
    if !matches!(
        ledger.outcome,
        SudoLedgerOutcomeWire::Completed | SudoLedgerOutcomeWire::RunnerError
    ) {
        return Err(SudoWireError::validation(
            "ledger.outcome",
            "sudo settlement requires a terminal execution ledger",
        ));
    }
    let ledger_ids: Vec<String> = ledger
        .entries
        .iter()
        .map(|entry| entry.id.clone())
        .collect();
    if ledger_ids != attempt.selected_command_ids {
        return Err(SudoWireError::validation(
            "ledger.entries",
            "sudo ledger command ids do not match the persisted attempt",
        ));
    }
    let handshake = object.get("handshake");
    match (&attempt.handshake, handshake) {
        (Some(recorded), Some(value)) => {
            let supplied = sudo_exec_started_from_json_value(value)?;
            if supplied != *recorded {
                return Err(SudoWireError::validation(
                    "handshake",
                    "sudo settlement handshake does not match attempt",
                ));
            }
        }
        (Some(_), None) => {
            return Err(SudoWireError::validation(
                "handshake",
                "sudo settlement is missing the recorded handshake",
            ));
        }
        (None, Some(_)) => {
            return Err(SudoWireError::validation(
                "handshake",
                "sudo settlement supplied a handshake for an attempt without one",
            ));
        }
        (None, None) => {}
    }
    if let Some(expected) = &attempt.operation_payload_digest {
        let actual = object
            .get("operation_payload_digest")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                SudoWireError::validation(
                    "operation_payload_digest",
                    "sudo settlement operation payload digest is required",
                )
            })?;
        if actual != expected {
            return Err(SudoWireError::validation(
                "operation_payload_digest",
                "sudo settlement operation payload digest does not match attempt",
            ));
        }
    }
    let mut authorization_identity = JsonMap::new();
    let mut identity_attempt = attempt.clone();
    identity_attempt.authorization_id = None;
    authorization_identity.insert(
        "attempt".to_string(),
        serde_json::to_value(&identity_attempt).map_err(|error| {
            SudoWireError::json(format!(
                "unable to serialize sudo attempt for authorization: {error}"
            ))
        })?,
    );
    authorization_identity.insert("ledger".to_string(), ledger_value.clone());
    if let Some(handshake) = handshake {
        authorization_identity
            .insert("handshake".to_string(), handshake.clone());
    }
    if let Some(operation_payload_digest) = object
        .get("operation_payload_digest")
        .and_then(JsonValue::as_str)
    {
        authorization_identity.insert(
            "operation_payload_digest".to_string(),
            JsonValue::String(operation_payload_digest.to_string()),
        );
    }
    let authorization_id =
        sudo_json_sha256(&JsonValue::Object(authorization_identity))?;
    let status = if attempt.authorization_id.as_deref()
        == Some(authorization_id.as_str())
    {
        "replayed"
    } else {
        "authorized"
    };
    Ok(SudoSettlementAuthorizationWire {
        schema_version: SUDO_SETTLEMENT_AUTHORIZATION_WIRE_SCHEMA_VERSION,
        authorized: true,
        status: status.to_string(),
        authorization_id,
        gate_id: attempt.gate_id,
        selected_command_ids: attempt.selected_command_ids,
        manifest_sha256: attempt.manifest_sha256,
        ledger_outcome: ledger.outcome,
    })
}

pub fn truncate_sudo_output_tail(value: &str, max_bytes: usize) -> String {
    truncate_utf8_tail(value, max_bytes.min(SUDO_MAX_OUTPUT_TAIL_BYTES))
}

fn sudo_liveness_decision(
    classification: SudoExecutorLivenessWire,
    reason: impl Into<String>,
) -> SudoExecutorLivenessDecisionWire {
    SudoExecutorLivenessDecisionWire {
        schema_version: SUDO_ATTEMPT_WIRE_SCHEMA_VERSION,
        classification,
        reason: reason.into(),
    }
}

fn sudo_json_sha256(value: &JsonValue) -> Result<String, SudoWireError> {
    let bytes =
        serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
            SudoWireError::json(format!(
                "unable to encode canonical sudo authorization JSON: {error}"
            ))
        })?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn validate_schema(
    kind: &str,
    actual: u32,
    expected: u32,
) -> Result<(), SudoWireError> {
    if actual == expected {
        Ok(())
    } else {
        Err(SudoWireError::unsupported_schema(kind, actual))
    }
}

fn ensure_json_size(
    kind: &str,
    value: &JsonValue,
    max_bytes: usize,
) -> Result<(), SudoWireError> {
    let size = serde_json::to_vec(value)
        .map_err(|error| {
            SudoWireError::json(format!("unable to encode {kind}: {error}"))
        })?
        .len();
    if size > max_bytes {
        return Err(SudoWireError::validation(
            kind,
            format!("{kind} exceeds {max_bytes} bytes"),
        ));
    }
    Ok(())
}

fn validate_command(
    command: &SudoCommandWire,
    index: usize,
) -> Result<(), SudoWireError> {
    validate_non_empty_bounded(
        format!("commands[{index}].id"),
        &command.id,
        SUDO_MAX_ID_BYTES,
    )?;
    reject_path_like(format!("commands[{index}].id"), &command.id)?;
    if command.argv.is_empty() {
        return Err(SudoWireError::validation(
            format!("commands[{index}].argv"),
            "sudo command argv must not be empty",
        ));
    }
    if command.argv.len() > SUDO_MAX_ARGV {
        return Err(SudoWireError::validation(
            format!("commands[{index}].argv"),
            format!("sudo command argv exceeds {SUDO_MAX_ARGV} entries"),
        ));
    }
    for (argv_index, arg) in command.argv.iter().enumerate() {
        validate_non_empty_bounded(
            format!("commands[{index}].argv[{argv_index}]"),
            arg,
            SUDO_MAX_PATH_BYTES,
        )?;
    }
    validate_non_empty_bounded(
        format!("commands[{index}].why"),
        &command.why,
        SUDO_MAX_WHY_BYTES,
    )?;
    if let Some(timeout) = command.timeout_seconds {
        validate_positive_bounded_seconds(
            format!("commands[{index}].timeout_seconds"),
            timeout,
            SUDO_MAX_TIMEOUT_SECONDS,
        )?;
    }
    let basename = executable_basename(&command.argv[0]);
    if nested_elevation_program(&basename) {
        return Err(SudoWireError::validation(
            format!("commands[{index}].argv[0]"),
            "sudo command must not start with a nested elevation program",
        ));
    }
    if first_version_interactive_exclusion(&basename) {
        return Err(SudoWireError::validation(
            format!("commands[{index}].argv[0]"),
            "sudo command starts with an interactive program excluded from the first sudo manifest version",
        ));
    }
    Ok(())
}

fn validate_ledger_entry(
    entry: &SudoLedgerEntryWire,
    index: usize,
) -> Result<(), SudoWireError> {
    validate_non_empty_bounded(
        format!("entries[{index}].id"),
        &entry.id,
        SUDO_MAX_ID_BYTES,
    )?;
    validate_non_negative_seconds(
        format!("entries[{index}].duration_seconds"),
        entry.duration_seconds,
    )?;
    validate_bounded(
        format!("entries[{index}].output_tail"),
        &entry.output_tail,
        SUDO_MAX_OUTPUT_TAIL_BYTES,
    )?;
    match (entry.status, entry.exit_code) {
        (SudoLedgerEntryStatusWire::Ran, Some(0)) => {}
        (SudoLedgerEntryStatusWire::Ran, _) => {
            return Err(SudoWireError::validation(
                format!("entries[{index}].exit_code"),
                "ran sudo ledger entries require exit_code 0",
            ));
        }
        (SudoLedgerEntryStatusWire::Failed, Some(0)) => {
            return Err(SudoWireError::validation(
                format!("entries[{index}].exit_code"),
                "failed sudo ledger entries must not carry exit_code 0",
            ));
        }
        (SudoLedgerEntryStatusWire::Failed, _) => {}
        (SudoLedgerEntryStatusWire::Skipped, Some(_)) => {
            return Err(SudoWireError::validation(
                format!("entries[{index}].exit_code"),
                "skipped sudo ledger entries must not carry exit_code",
            ));
        }
        (SudoLedgerEntryStatusWire::Skipped, None) => {}
    }
    Ok(())
}

fn validate_env(env: &BTreeMap<String, String>) -> Result<(), SudoWireError> {
    if env.len() > SUDO_MAX_ENV {
        return Err(SudoWireError::validation(
            "env",
            format!("sudo manifest env exceeds {SUDO_MAX_ENV} entries"),
        ));
    }
    for (name, value) in env {
        validate_env_name(name)?;
        validate_bounded(
            format!("env.{name}"),
            value,
            SUDO_MAX_ENV_VALUE_BYTES,
        )?;
    }
    Ok(())
}

fn validate_env_name(name: &str) -> Result<(), SudoWireError> {
    validate_non_empty_bounded("env", name, SUDO_MAX_LABEL_BYTES)?;
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(SudoWireError::validation(
            "env",
            "sudo environment names must not be empty",
        ));
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Err(SudoWireError::validation(
            format!("env.{name}"),
            "sudo environment name is malformed",
        ));
    }
    if name == "PATH" || name.starts_with("LD_") || name.starts_with("SUDO_") {
        return Err(SudoWireError::validation(
            format!("env.{name}"),
            "sudo environment name is not allowed",
        ));
    }
    Ok(())
}

fn validate_absolute_path(
    target: impl Into<String>,
    value: &str,
) -> Result<(), SudoWireError> {
    let target = target.into();
    validate_non_empty_bounded(&target, value, SUDO_MAX_PATH_BYTES)?;
    if !Path::new(value).is_absolute() {
        return Err(SudoWireError::validation(
            target,
            "sudo path must be absolute",
        ));
    }
    Ok(())
}

fn validate_safe_absolute_path(
    target: impl Into<String>,
    value: &str,
) -> Result<(), SudoWireError> {
    let target = target.into();
    validate_absolute_path(&target, value)?;
    if value.contains('\0') || value.ends_with('/') {
        return Err(SudoWireError::validation(
            target,
            "sudo path must be a safe absolute path",
        ));
    }
    for component in Path::new(value).components() {
        match component {
            Component::RootDir | Component::Normal(_) => {}
            _ => {
                return Err(SudoWireError::validation(
                    target,
                    "sudo path must not contain relative or special components",
                ));
            }
        }
    }
    Ok(())
}

fn path_is_within(directory: &str, child: &str) -> bool {
    let prefix = format!("{directory}/");
    child.starts_with(&prefix) && child.len() > prefix.len()
}

fn validate_non_empty_bounded(
    target: impl Into<String>,
    value: &str,
    max_bytes: usize,
) -> Result<(), SudoWireError> {
    let target = target.into();
    if value.is_empty() || value.trim().is_empty() {
        return Err(SudoWireError::validation(
            target,
            "sudo wire string must not be empty",
        ));
    }
    validate_bounded(target, value, max_bytes)
}

fn validate_bounded(
    target: impl Into<String>,
    value: &str,
    max_bytes: usize,
) -> Result<(), SudoWireError> {
    if value.len() > max_bytes {
        return Err(SudoWireError::validation(
            target,
            format!("sudo wire string exceeds {max_bytes} bytes"),
        ));
    }
    Ok(())
}

fn validate_positive_bounded_seconds(
    target: impl Into<String>,
    value: f64,
    max: f64,
) -> Result<(), SudoWireError> {
    let target = target.into();
    if !value.is_finite() || value <= 0.0 {
        return Err(SudoWireError::validation(
            target,
            "sudo duration must be finite and positive",
        ));
    }
    if value > max {
        return Err(SudoWireError::validation(
            target,
            format!("sudo duration exceeds {max} seconds"),
        ));
    }
    Ok(())
}

fn validate_non_negative_seconds(
    target: impl Into<String>,
    value: f64,
) -> Result<(), SudoWireError> {
    if !value.is_finite() || value < 0.0 {
        return Err(SudoWireError::validation(
            target,
            "sudo duration must be finite and non-negative",
        ));
    }
    Ok(())
}

fn validate_positive_timestamp(
    target: impl Into<String>,
    value: f64,
) -> Result<(), SudoWireError> {
    if !value.is_finite() || value <= 0.0 {
        return Err(SudoWireError::validation(
            target,
            "sudo timestamp must be finite and positive",
        ));
    }
    Ok(())
}

fn validate_sha256(
    target: impl Into<String>,
    value: &str,
) -> Result<(), SudoWireError> {
    let target = target.into();
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(SudoWireError::validation(
            target,
            "value must be a lowercase SHA-256 hex digest",
        ))
    }
}

fn validate_process_identity(value: &str) -> Result<(), SudoWireError> {
    validate_non_empty_bounded(
        "executor_identity",
        value,
        SUDO_MAX_LABEL_BYTES,
    )?;
    let Some((boot_id, ticks)) = value.split_once(':') else {
        return Err(SudoWireError::validation(
            "executor_identity",
            "sudo exec started executor_identity must be <boot_id>:<start_ticks>",
        ));
    };
    if boot_id.trim().is_empty() {
        return Err(SudoWireError::validation(
            "executor_identity",
            "sudo exec started executor_identity boot id must not be empty",
        ));
    }
    if ticks.is_empty()
        || !ticks.bytes().all(|byte| byte.is_ascii_digit())
        || ticks.parse::<u64>().is_err()
    {
        return Err(SudoWireError::validation(
            "executor_identity",
            "sudo exec started executor_identity start ticks must be numeric",
        ));
    }
    Ok(())
}

fn reject_path_like(
    target: impl Into<String>,
    value: &str,
) -> Result<(), SudoWireError> {
    if value.contains('/') || value.contains('\\') {
        return Err(SudoWireError::validation(
            target,
            "sudo wire label must not look like a path",
        ));
    }
    Ok(())
}

fn nested_elevation_program(basename: &str) -> bool {
    matches!(basename, "sudo" | "sudoedit" | "doas" | "su" | "pkexec")
}

fn first_version_interactive_exclusion(basename: &str) -> bool {
    matches!(basename, "passwd" | "visudo")
}

fn executable_basename(value: &str) -> String {
    value
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(value)
        .to_ascii_lowercase()
}

fn command_has_badge(
    command: &SudoCommandWire,
    badge: SudoRiskBadgeKindWire,
) -> bool {
    match badge {
        SudoRiskBadgeKindWire::Shell => command.shell,
        SudoRiskBadgeKindWire::Network => {
            package_manager_command(command)
                || network_command(&executable_basename(&command.argv[0]))
        }
        SudoRiskBadgeKindWire::PackageManager => {
            package_manager_command(command)
        }
        SudoRiskBadgeKindWire::SystemPathWrite => {
            system_path_write_command(command)
        }
        SudoRiskBadgeKindWire::ServiceRestart => {
            service_restart_command(command)
        }
    }
}

fn network_command(basename: &str) -> bool {
    matches!(
        basename,
        "curl"
            | "wget"
            | "scp"
            | "sftp"
            | "ssh"
            | "rsync"
            | "git"
            | "gh"
            | "nc"
            | "ncat"
            | "netcat"
            | "telnet"
            | "ftp"
            | "dig"
            | "host"
            | "nslookup"
    )
}

fn package_manager_command(command: &SudoCommandWire) -> bool {
    let basename = executable_basename(&command.argv[0]);
    matches!(
        basename.as_str(),
        "apt"
            | "apt-get"
            | "aptitude"
            | "dnf"
            | "yum"
            | "pacman"
            | "zypper"
            | "apk"
            | "brew"
            | "port"
            | "nix-env"
            | "nix"
            | "npm"
            | "yarn"
            | "pnpm"
            | "pip"
            | "pip3"
            | "python"
            | "python3"
            | "cargo"
    ) && (basename != "python" && basename != "python3"
        || command
            .argv
            .windows(2)
            .any(|pair| pair[0] == "-m" && pair[1].starts_with("pip")))
}

fn system_path_write_command(command: &SudoCommandWire) -> bool {
    let basename = executable_basename(&command.argv[0]);
    let write_or_shell = command.shell
        || matches!(
            basename.as_str(),
            "cp" | "mv"
                | "rm"
                | "rmdir"
                | "mkdir"
                | "touch"
                | "tee"
                | "install"
                | "ln"
                | "chmod"
                | "chown"
                | "chgrp"
                | "truncate"
                | "dd"
                | "sed"
                | "perl"
        );
    write_or_shell && command.argv.iter().any(|arg| targets_system_path(arg))
}

fn targets_system_path(arg: &str) -> bool {
    let normalized = arg.trim_matches(|ch| ch == '"' || ch == '\'');
    let prefixes = [
        "/etc", "/usr", "/var", "/opt", "/boot", "/sys", "/proc", "/root",
    ];
    prefixes.iter().any(|prefix| {
        normalized == *prefix
            || normalized
                .strip_prefix(*prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn service_restart_command(command: &SudoCommandWire) -> bool {
    service_restart_services(command).is_some()
}

fn service_restart_lockout_prone(command: &SudoCommandWire) -> bool {
    service_restart_services(command).is_some_and(|services| {
        services
            .iter()
            .any(|service| ssh_or_tailscale_service(service))
    })
}

fn service_restart_services(command: &SudoCommandWire) -> Option<Vec<String>> {
    let basename = executable_basename(command.argv.first()?);
    if basename == "systemctl" {
        let mut iter = command.argv.iter().skip(1).filter(|arg| {
            !arg.starts_with('-')
                && !matches!(
                    arg.as_str(),
                    "--user" | "--global" | "--system" | "--no-pager"
                )
        });
        let action = iter.next()?.to_ascii_lowercase();
        if restart_like_action(&action) {
            let services = iter
                .filter(|arg| !arg.starts_with('-'))
                .map(|arg| arg.to_ascii_lowercase())
                .collect::<Vec<_>>();
            return Some(services);
        }
    }
    if basename == "service" && command.argv.len() >= 3 {
        let service = command.argv[1].to_ascii_lowercase();
        let action = command.argv[2].to_ascii_lowercase();
        if restart_like_action(&action) {
            return Some(vec![service]);
        }
    }
    None
}

fn restart_like_action(action: &str) -> bool {
    matches!(
        action,
        "restart" | "reload" | "try-restart" | "reload-or-restart"
    )
}

fn ssh_or_tailscale_service(service: &str) -> bool {
    let service = service
        .trim_end_matches(".service")
        .trim_end_matches('@')
        .to_ascii_lowercase();
    service == "ssh"
        || service == "sshd"
        || service.starts_with("ssh@")
        || service == "tailscale"
        || service == "tailscaled"
        || service.starts_with("tailscaled@")
}

fn canonical_json_value(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(items) => {
            JsonValue::Array(items.iter().map(canonical_json_value).collect())
        }
        JsonValue::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            let mut sorted = JsonMap::new();
            for key in keys {
                sorted.insert(key.clone(), canonical_json_value(&map[key]));
            }
            JsonValue::Object(sorted)
        }
        other => other.clone(),
    }
}

fn truncate_utf8_tail(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut start = value.len() - max_bytes;
    while !value.is_char_boundary(start) {
        start += 1;
    }
    value[start..].to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn manifest_value() -> JsonValue {
        json!({
            "schema_version": 1,
            "request_id": "req-1",
            "host": "athena",
            "host_is_remote": false,
            "run_as": "root",
            "cwd": "/tmp",
            "env": {
                "LC_ALL": "C",
                "TERM": "xterm-256color"
            },
            "stop_on_failure": true,
            "output_to_agent": "tail",
            "commands": [
                {
                    "id": "one",
                    "argv": ["apt-get", "update"],
                    "why": "Refresh package metadata",
                    "timeout_seconds": 30.0,
                    "shell": false
                },
                {
                    "id": "two",
                    "argv": ["systemctl", "restart", "sshd.service"],
                    "why": "Restart ssh",
                    "shell": false
                }
            ],
            "resume_from": null
        })
    }

    fn manifest() -> SudoManifestWire {
        sudo_manifest_from_json_value(&manifest_value()).unwrap()
    }

    #[test]
    fn canonical_manifest_sorts_object_keys_but_preserves_command_order() {
        let left = manifest_value();
        let right = json!({
            "commands": [
                {
                    "shell": false,
                    "timeout_seconds": 30.0,
                    "why": "Refresh package metadata",
                    "argv": ["apt-get", "update"],
                    "id": "one"
                },
                {
                    "shell": false,
                    "why": "Restart ssh",
                    "argv": ["systemctl", "restart", "sshd.service"],
                    "id": "two"
                }
            ],
            "output_to_agent": "tail",
            "stop_on_failure": true,
            "env": {
                "TERM": "xterm-256color",
                "LC_ALL": "C"
            },
            "cwd": "/tmp",
            "run_as": "root",
            "host_is_remote": false,
            "host": "athena",
            "request_id": "req-1",
            "resume_from": null,
            "schema_version": 1
        });
        let left = sudo_manifest_from_json_value(&left).unwrap();
        let right = sudo_manifest_from_json_value(&right).unwrap();
        assert_eq!(
            sudo_manifest_canonical_json_bytes(&left).unwrap(),
            sudo_manifest_canonical_json_bytes(&right).unwrap()
        );
        assert_eq!(
            sudo_manifest_sha256(&left).unwrap(),
            sudo_manifest_sha256(&right).unwrap()
        );
    }

    #[test]
    fn manifest_digest_changes_for_execution_relevant_fields() {
        let base = manifest();
        let base_digest = sudo_manifest_sha256(&base).unwrap();

        let mut changed = base.clone();
        changed.cwd = "/var/tmp".to_string();
        assert_ne!(base_digest, sudo_manifest_sha256(&changed).unwrap());

        let mut changed = base.clone();
        changed.commands.swap(0, 1);
        assert_ne!(base_digest, sudo_manifest_sha256(&changed).unwrap());

        let mut changed = base.clone();
        changed.commands[0].argv =
            vec!["apt-get".to_string(), "upgrade".to_string()];
        assert_ne!(base_digest, sudo_manifest_sha256(&changed).unwrap());
    }

    #[test]
    fn validation_rejects_forbidden_env_and_nested_elevation() {
        let mut value = manifest_value();
        value["env"] = json!({"PATH": "/tmp/bin"});
        let error = sudo_manifest_from_json_value(&value).unwrap_err();
        assert_eq!(error.code, SudoErrorCodeWire::Validation);
        assert!(error.message.contains("not allowed"));

        let mut value = manifest_value();
        value["commands"][0]["argv"] = json!(["sudo", "id"]);
        let error = sudo_manifest_from_json_value(&value).unwrap_err();
        assert!(error.message.contains("nested elevation"));
    }

    #[test]
    fn validation_rejects_relative_cwd_duplicate_id_and_missing_resume() {
        let mut value = manifest_value();
        value["cwd"] = json!("relative");
        assert!(sudo_manifest_from_json_value(&value)
            .unwrap_err()
            .message
            .contains("absolute"));

        let mut value = manifest_value();
        value["commands"][1]["id"] = json!("one");
        assert!(sudo_manifest_from_json_value(&value)
            .unwrap_err()
            .message
            .contains("duplicate"));

        let mut value = manifest_value();
        value["resume_from"] = json!("missing");
        assert!(sudo_manifest_from_json_value(&value)
            .unwrap_err()
            .message
            .contains("does not exist"));
    }

    #[test]
    fn risk_badges_are_ordered_deduped_and_remote_lockout_only() {
        let mut manifest = manifest();
        manifest.host_is_remote = true;
        manifest.commands = vec![
            SudoCommandWire {
                id: "pkg".to_string(),
                argv: vec![
                    "apt-get".to_string(),
                    "install".to_string(),
                    "curl".to_string(),
                ],
                why: "Install package".to_string(),
                timeout_seconds: None,
                shell: true,
            },
            SudoCommandWire {
                id: "restart".to_string(),
                argv: vec![
                    "systemctl".to_string(),
                    "reload-or-restart".to_string(),
                    "tailscaled.service".to_string(),
                ],
                why: "Restart transport".to_string(),
                timeout_seconds: None,
                shell: false,
            },
            SudoCommandWire {
                id: "near-miss".to_string(),
                argv: vec!["echo".to_string(), "/etc/passwd".to_string()],
                why: "No write action".to_string(),
                timeout_seconds: None,
                shell: false,
            },
        ];
        let risks = derive_sudo_risk_badges(&manifest).unwrap();
        assert_eq!(
            risks[0].badges,
            vec![
                SudoRiskBadgeKindWire::Shell,
                SudoRiskBadgeKindWire::Network,
                SudoRiskBadgeKindWire::PackageManager,
            ]
        );
        assert_eq!(
            risks[1].badges,
            vec![SudoRiskBadgeKindWire::ServiceRestart]
        );
        assert!(risks[1].lockout_prone);
        assert!(risks[2].badges.is_empty());

        manifest.host_is_remote = false;
        let local = derive_sudo_risk_badges(&manifest).unwrap();
        assert!(!local[1].lockout_prone);
    }

    #[test]
    fn risk_detects_system_path_writes_and_service_near_misses() {
        let mut manifest = manifest();
        manifest.commands = vec![
            SudoCommandWire {
                id: "write".to_string(),
                argv: vec![
                    "install".to_string(),
                    "tool".to_string(),
                    "/usr/local/bin/tool".to_string(),
                ],
                why: "Install binary".to_string(),
                timeout_seconds: None,
                shell: false,
            },
            SudoCommandWire {
                id: "status".to_string(),
                argv: vec![
                    "systemctl".to_string(),
                    "status".to_string(),
                    "sshd".to_string(),
                ],
                why: "Inspect service".to_string(),
                timeout_seconds: None,
                shell: false,
            },
        ];
        let risks = derive_sudo_risk_badges(&manifest).unwrap();
        assert_eq!(
            risks[0].badges,
            vec![SudoRiskBadgeKindWire::SystemPathWrite]
        );
        assert!(risks[1].badges.is_empty());
        assert!(!risks[1].lockout_prone);
    }

    #[test]
    fn ledger_validates_against_manifest_order_and_hash() {
        let manifest = manifest();
        let digest = sudo_manifest_sha256(&manifest).unwrap();
        let ledger = SudoLedgerWire {
            schema_version: 1,
            request_id: manifest.request_id.clone(),
            manifest_sha256: digest,
            outcome: SudoLedgerOutcomeWire::Completed,
            entries: vec![
                SudoLedgerEntryWire {
                    id: "one".to_string(),
                    status: SudoLedgerEntryStatusWire::Ran,
                    exit_code: Some(0),
                    duration_seconds: 0.1,
                    output_tail: "ok".to_string(),
                },
                SudoLedgerEntryWire {
                    id: "two".to_string(),
                    status: SudoLedgerEntryStatusWire::Skipped,
                    exit_code: None,
                    duration_seconds: 0.0,
                    output_tail: String::new(),
                },
            ],
            diagnostic: None,
        };
        assert_eq!(
            validate_sudo_ledger(&ledger, Some(&manifest)).unwrap(),
            ledger
        );

        let value = serde_json::to_value(&ledger).unwrap();
        let round_trip = sudo_validate_ledger_json_value(
            &value,
            Some(&serde_json::to_value(&manifest).unwrap()),
        )
        .unwrap();
        assert_eq!(round_trip, ledger);
    }

    #[test]
    fn ledger_rejects_status_exit_code_mismatches_and_wrong_order() {
        let manifest = manifest();
        let digest = sudo_manifest_sha256(&manifest).unwrap();
        let mut ledger = SudoLedgerWire {
            schema_version: 1,
            request_id: manifest.request_id.clone(),
            manifest_sha256: digest,
            outcome: SudoLedgerOutcomeWire::Completed,
            entries: vec![
                SudoLedgerEntryWire {
                    id: "one".to_string(),
                    status: SudoLedgerEntryStatusWire::Ran,
                    exit_code: Some(1),
                    duration_seconds: 0.1,
                    output_tail: String::new(),
                },
                SudoLedgerEntryWire {
                    id: "two".to_string(),
                    status: SudoLedgerEntryStatusWire::Skipped,
                    exit_code: None,
                    duration_seconds: 0.0,
                    output_tail: String::new(),
                },
            ],
            diagnostic: None,
        };
        assert!(validate_sudo_ledger(&ledger, Some(&manifest))
            .unwrap_err()
            .message
            .contains("exit_code 0"));

        ledger.entries[0].status = SudoLedgerEntryStatusWire::Ran;
        ledger.entries[0].exit_code = Some(0);
        ledger.entries.swap(0, 1);
        assert!(validate_sudo_ledger(&ledger, Some(&manifest))
            .unwrap_err()
            .message
            .contains("order"));
    }

    #[test]
    fn ledger_diagnostic_is_runner_error_only() {
        let mut ledger = SudoLedgerWire {
            schema_version: 1,
            request_id: "req-1".to_string(),
            manifest_sha256: "a".repeat(64),
            outcome: SudoLedgerOutcomeWire::Completed,
            entries: Vec::new(),
            diagnostic: Some("cleanup failed".to_string()),
        };
        assert!(validate_sudo_ledger(&ledger, None)
            .unwrap_err()
            .message
            .contains("runner_error"));
        ledger.outcome = SudoLedgerOutcomeWire::RunnerError;
        validate_sudo_ledger(&ledger, None).unwrap();
    }

    fn handshake_value(manifest: &SudoManifestWire) -> JsonValue {
        json!({
            "schema_version": 1,
            "kind": "sudo_exec_started",
            "manifest_sha256": sudo_manifest_sha256(manifest).unwrap(),
            "executor_pid": 1234,
            "executor_identity": "boot-a:5678",
            "ledger_path": "/tmp/sase-sudo/ledger.json",
            "log_path": "/tmp/sase-sudo/output.log",
            "started_at": 1_800_000_000.25
        })
    }

    #[test]
    fn exec_started_validates_against_manifest_digest() {
        let manifest = manifest();
        let value = handshake_value(&manifest);
        let handshake = sudo_validate_exec_started_json_value(
            &value,
            Some(&serde_json::to_value(&manifest).unwrap()),
        )
        .unwrap();
        assert_eq!(handshake.kind, SUDO_EXEC_STARTED_KIND);
        assert_eq!(handshake.executor_pid, 1234);
        assert_eq!(handshake.ledger_path, "/tmp/sase-sudo/ledger.json");
    }

    #[test]
    fn exec_started_rejects_bad_kind_digest_pid_paths_identity_and_time() {
        let manifest = manifest();
        let manifest_json = serde_json::to_value(&manifest).unwrap();

        let mut value = handshake_value(&manifest);
        value["kind"] = json!("other");
        assert!(sudo_validate_exec_started_json_value(
            &value,
            Some(&manifest_json)
        )
        .unwrap_err()
        .message
        .contains("kind"));

        let mut value = handshake_value(&manifest);
        value["manifest_sha256"] = json!("b".repeat(64));
        assert_eq!(
            sudo_validate_exec_started_json_value(&value, Some(&manifest_json))
                .unwrap_err()
                .code,
            SudoErrorCodeWire::DigestMismatch
        );

        let mut value = handshake_value(&manifest);
        value["executor_pid"] = json!(0);
        assert!(sudo_exec_started_from_json_value(&value)
            .unwrap_err()
            .message
            .contains("executor_pid"));

        let mut value = handshake_value(&manifest);
        value["ledger_path"] = json!("relative/ledger.json");
        assert!(sudo_exec_started_from_json_value(&value)
            .unwrap_err()
            .message
            .contains("absolute"));

        let mut value = handshake_value(&manifest);
        value["executor_identity"] = json!("boot-a:not-number");
        assert!(sudo_exec_started_from_json_value(&value)
            .unwrap_err()
            .message
            .contains("numeric"));

        let mut value = handshake_value(&manifest);
        value["started_at"] = json!(0.0);
        assert!(sudo_exec_started_from_json_value(&value)
            .unwrap_err()
            .message
            .contains("positive"));
    }

    fn attempt_value(
        manifest: &SudoManifestWire,
        startup_state: &str,
        target_kind: &str,
    ) -> JsonValue {
        json!({
            "schema_version": 1,
            "gate_id": manifest.request_id,
            "selected_command_ids": ["one", "two"],
            "manifest_sha256": sudo_manifest_sha256(manifest).unwrap(),
            "handoff_dir": "/tmp/sase-sudo/req-1",
            "handshake": handshake_value(manifest),
            "finalize_proc_id": "proc-1",
            "target_kind": target_kind,
            "target_host": if target_kind == "remote" { json!("apollo") } else { JsonValue::Null },
            "startup_state": startup_state,
            "operation_payload_digest": "c".repeat(64),
            "authorization_id": null
        })
    }

    fn remote_handoff_value() -> JsonValue {
        json!({
            "directory": "/tmp/sase-sudo/req-1",
            "handshake": "/tmp/sase-sudo/req-1/handshake.json",
            "ledger": "/tmp/sase-sudo/req-1/ledger.json",
            "log": "/tmp/sase-sudo/req-1/output.log",
            "manifest": "/tmp/sase-sudo/req-1/manifest.json",
            "stop": "/tmp/sase-sudo/req-1/stop"
        })
    }

    fn ledger_value(manifest: &SudoManifestWire) -> JsonValue {
        json!({
            "schema_version": 1,
            "request_id": manifest.request_id,
            "manifest_sha256": sudo_manifest_sha256(manifest).unwrap(),
            "outcome": "completed",
            "entries": [
                {
                    "id": "one",
                    "status": "ran",
                    "exit_code": 0,
                    "duration_seconds": 0.1,
                    "output_tail": ""
                },
                {
                    "id": "two",
                    "status": "ran",
                    "exit_code": 0,
                    "duration_seconds": 0.1,
                    "output_tail": ""
                }
            ],
            "diagnostic": null
        })
    }

    #[test]
    fn attempt_liveness_treats_legacy_missing_fields_as_unknown() {
        let manifest = manifest();
        let mut value = attempt_value(&manifest, "started", "local");
        let object = value.as_object_mut().unwrap();
        object.remove("target_kind");
        object.remove("target_host");
        object.remove("startup_state");
        object.remove("handshake");
        let attempt = sudo_attempt_from_json_value(&value).unwrap();
        assert_eq!(attempt.target_kind, SudoAttemptTargetKindWire::Unknown);
        assert_eq!(attempt.startup_state, SudoAttemptStartupStateWire::Unknown);

        let decision = classify_sudo_executor_liveness(
            &attempt,
            &SudoExecutorFactsWire {
                finalize_proc_live: Some(false),
                executor_pid_live: Some(false),
                executor_identity_matches: Some(false),
            },
        )
        .unwrap();
        assert_eq!(decision.classification, SudoExecutorLivenessWire::Unknown);
    }

    #[test]
    fn attempt_liveness_keeps_remote_and_starting_attempts_unknown() {
        let manifest = manifest();
        let remote = sudo_attempt_from_json_value(&attempt_value(
            &manifest, "started", "remote",
        ))
        .unwrap();
        let facts = SudoExecutorFactsWire {
            finalize_proc_live: Some(false),
            executor_pid_live: Some(false),
            executor_identity_matches: Some(false),
        };
        let decision =
            classify_sudo_executor_liveness(&remote, &facts).unwrap();
        assert_eq!(decision.classification, SudoExecutorLivenessWire::Unknown);

        let starting = sudo_attempt_from_json_value(&attempt_value(
            &manifest, "starting", "local",
        ))
        .unwrap();
        let decision =
            classify_sudo_executor_liveness(&starting, &facts).unwrap();
        assert_eq!(decision.classification, SudoExecutorLivenessWire::Unknown);
    }

    #[test]
    fn attempt_liveness_classifies_local_started_from_host_facts() {
        let manifest = manifest();
        let attempt = sudo_attempt_from_json_value(&attempt_value(
            &manifest, "started", "local",
        ))
        .unwrap();
        let live = classify_sudo_executor_liveness(
            &attempt,
            &SudoExecutorFactsWire {
                finalize_proc_live: Some(false),
                executor_pid_live: Some(true),
                executor_identity_matches: Some(true),
            },
        )
        .unwrap();
        assert_eq!(live.classification, SudoExecutorLivenessWire::Live);

        let dead = classify_sudo_executor_liveness(
            &attempt,
            &SudoExecutorFactsWire {
                finalize_proc_live: Some(false),
                executor_pid_live: Some(false),
                executor_identity_matches: None,
            },
        )
        .unwrap();
        assert_eq!(dead.classification, SudoExecutorLivenessWire::Dead);
    }

    #[test]
    fn attempt_accepts_legacy_records_without_remote_handoff() {
        let manifest = manifest();
        let value = attempt_value(&manifest, "started", "local");
        let attempt = sudo_attempt_from_json_value(&value).unwrap();
        assert!(attempt.remote_handoff.is_none());
        let serialized = serde_json::to_value(&attempt).unwrap();
        assert!(serialized.get("remote_handoff").is_none());
    }

    #[test]
    fn attempt_accepts_valid_remote_handoff_metadata() {
        let manifest = manifest();
        let mut value = attempt_value(&manifest, "started", "remote");
        value["remote_handoff"] = remote_handoff_value();
        let attempt = sudo_attempt_from_json_value(&value).unwrap();
        let handoff = attempt.remote_handoff.expect("remote handoff");
        assert_eq!(handoff.directory, "/tmp/sase-sudo/req-1");
        assert_eq!(handoff.ledger, "/tmp/sase-sudo/req-1/ledger.json");
        assert_eq!(attempt.target_host.as_deref(), Some("apollo"));
    }

    #[test]
    fn attempt_rejects_malformed_remote_handoff_metadata() {
        let manifest = manifest();

        let mut local = attempt_value(&manifest, "started", "local");
        local["remote_handoff"] = remote_handoff_value();
        assert!(sudo_attempt_from_json_value(&local)
            .unwrap_err()
            .message
            .contains("target_kind remote"));

        let mut missing_host = attempt_value(&manifest, "started", "remote");
        missing_host["target_host"] = JsonValue::Null;
        missing_host["remote_handoff"] = remote_handoff_value();
        assert!(sudo_attempt_from_json_value(&missing_host)
            .unwrap_err()
            .message
            .contains("target_host"));

        let mut relative = attempt_value(&manifest, "started", "remote");
        let mut handoff = remote_handoff_value();
        handoff["log"] = json!("output.log");
        relative["remote_handoff"] = handoff;
        assert!(sudo_attempt_from_json_value(&relative)
            .unwrap_err()
            .message
            .contains("absolute"));

        let mut escaped = attempt_value(&manifest, "started", "remote");
        let mut handoff = remote_handoff_value();
        handoff["stop"] = json!("/tmp/sase-sudo/req-1/../stop");
        escaped["remote_handoff"] = handoff;
        assert!(sudo_attempt_from_json_value(&escaped)
            .unwrap_err()
            .message
            .contains("relative"));

        let mut outside = attempt_value(&manifest, "started", "remote");
        let mut handoff = remote_handoff_value();
        handoff["manifest"] = json!("/tmp/other/manifest.json");
        outside["remote_handoff"] = handoff;
        assert!(sudo_attempt_from_json_value(&outside)
            .unwrap_err()
            .message
            .contains("inside the remote directory"));

        let mut duplicate = attempt_value(&manifest, "started", "remote");
        let mut handoff = remote_handoff_value();
        handoff["stop"] = json!("/tmp/sase-sudo/req-1/ledger.json");
        duplicate["remote_handoff"] = handoff;
        assert!(sudo_attempt_from_json_value(&duplicate)
            .unwrap_err()
            .message
            .contains("unique"));
    }

    #[test]
    fn settlement_authorization_matches_attempt_and_replays_exact_digest() {
        let manifest = manifest();
        let attempt = attempt_value(&manifest, "started", "local");
        let request = json!({
            "attempt": attempt,
            "handshake": handshake_value(&manifest),
            "ledger": ledger_value(&manifest),
            "operation_payload_digest": "c".repeat(64)
        });
        let authorized =
            authorize_sudo_settlement_json_value(&request).unwrap();
        assert!(authorized.authorized);
        assert_eq!(authorized.status, "authorized");

        let mut replay = request;
        replay["attempt"]["authorization_id"] =
            json!(authorized.authorization_id.clone());
        let replayed = authorize_sudo_settlement_json_value(&replay).unwrap();
        assert_eq!(replayed.status, "replayed");
        assert_eq!(replayed.authorization_id, authorized.authorization_id);
    }

    #[test]
    fn settlement_authorization_rejects_mismatched_digest_and_commands() {
        let manifest = manifest();
        let mut request = json!({
            "attempt": attempt_value(&manifest, "started", "local"),
            "handshake": handshake_value(&manifest),
            "ledger": ledger_value(&manifest),
            "operation_payload_digest": "d".repeat(64)
        });
        assert!(authorize_sudo_settlement_json_value(&request)
            .unwrap_err()
            .message
            .contains("operation payload digest"));

        request["operation_payload_digest"] = json!("c".repeat(64));
        request["ledger"]["entries"][0]["id"] = json!("other");
        assert!(authorize_sudo_settlement_json_value(&request)
            .unwrap_err()
            .message
            .contains("command ids"));
    }

    #[test]
    fn output_tail_does_not_split_utf8() {
        let value = "abcéxyz";
        assert_eq!(truncate_sudo_output_tail(value, 5), "éxyz");
    }
}
