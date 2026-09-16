//! Durable, TTL-bounded agent hold records.
//!
//! This module deliberately keeps scheduling policy as data. Callers provide
//! armer liveness facts, and the pure predicate below never touches disk.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::NamedTempFile;

use crate::agent_identity::{
    agent_name_in_hood, historical_family_scope, parse_agent_family_name,
};
use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, LockMode,
    StoreLockError,
};

pub const AGENT_HOLD_WIRE_SCHEMA_VERSION: u32 = 1;
pub const AGENT_HOLD_STATE_FILENAME: &str = "agent_holds.json";
pub const AGENT_HOLD_LOCK_FILENAME: &str = "agent_holds.lock";

const LOCK_TIMEOUT_ENV: &str = "SASE_AGENT_HOLD_LOCK_TIMEOUT";
const LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(2);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentHoldArmerKindWire {
    Agent,
    Proc,
    Cli,
}

impl AgentHoldArmerKindWire {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Proc => "proc",
            Self::Cli => "cli",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHoldArmerWire {
    pub kind: AgentHoldArmerKindWire,
    pub key: String,
    pub display: String,
    pub project: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub family: Option<String>,
    #[serde(default)]
    pub clan: Option<String>,
    #[serde(default)]
    pub proc_id: Option<String>,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub done_marker_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentHoldScopeWire {
    Project { project: String },
    Host,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHoldSelectorsWire {
    #[serde(default)]
    pub artifact_dirs: Vec<String>,
    #[serde(default)]
    pub names: Vec<String>,
    #[serde(default)]
    pub families: Vec<String>,
    #[serde(default)]
    pub hoods: Vec<String>,
    #[serde(default)]
    pub clans: Vec<String>,
    #[serde(default)]
    pub workflows: Vec<String>,
    #[serde(default)]
    pub tribes: Vec<String>,
    #[serde(default)]
    pub future: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHoldRecordWire {
    pub schema_version: u32,
    pub armer: AgentHoldArmerWire,
    pub scope: AgentHoldScopeWire,
    pub selectors: AgentHoldSelectorsWire,
    pub created_at: f64,
    pub expires_at: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHoldLivenessFactsWire {
    #[serde(default)]
    pub armers: BTreeMap<String, AgentHoldArmerLivenessFactWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentHoldArmerLivenessFactWire {
    Agent {
        pid_alive: bool,
        done_marker_present: bool,
    },
    Proc {
        terminal: bool,
    },
    Cli {
        pid_alive: bool,
        done_marker_present: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHoldSnapshotWire {
    pub schema_version: u32,
    pub holds: Vec<AgentHoldRecordWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHoldCandidateWire {
    pub project: String,
    pub created_at: f64,
    #[serde(default)]
    pub artifact_dirs: Vec<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub family: Option<String>,
    #[serde(default)]
    pub clan: Option<String>,
    #[serde(default)]
    pub workflow: Option<String>,
    #[serde(default)]
    pub tribe: Option<String>,
    #[serde(default)]
    pub armer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHoldSelectorMatchWire {
    pub kind: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHoldBlockArmerWire {
    pub kind: AgentHoldArmerKindWire,
    pub key: String,
    pub display: String,
    pub project: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHoldBlockWire {
    pub schema_version: u32,
    pub armer: AgentHoldBlockArmerWire,
    pub expires_at: f64,
    pub matches: Vec<AgentHoldSelectorMatchWire>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentHoldStateWire {
    schema_version: u32,
    holds: BTreeMap<String, AgentHoldRecordWire>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAgentHoldStateWire {
    schema_version: u32,
    holds: BTreeMap<String, Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum AgentHoldError {
    #[error("{0}")]
    Validation(String),
    #[error(
        "agent hold lock timed out after {waited_ms}ms waiting for {mode} lock: {}; holder: {holder}",
        path.display()
    )]
    LockTimeout {
        mode: &'static str,
        path: PathBuf,
        waited_ms: u128,
        holder: String,
    },
    #[error("agent hold store I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("agent hold store serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn agent_hold_state_path(sase_home: &Path) -> PathBuf {
    sase_home.join(AGENT_HOLD_STATE_FILENAME)
}

pub fn agent_hold_lock_path(sase_home: &Path) -> PathBuf {
    sase_home.join(AGENT_HOLD_LOCK_FILENAME)
}

pub fn list_agent_holds(
    sase_home: &Path,
    liveness: &AgentHoldLivenessFactsWire,
    now: f64,
) -> Result<AgentHoldSnapshotWire, AgentHoldError> {
    validate_timestamp("now", now)?;
    with_hold_lock(sase_home, "list_agent_holds", || {
        let records = read_records_locked(sase_home, liveness, now)?;
        Ok(snapshot_from_records(records))
    })
}

pub fn arm_agent_hold_relative(
    sase_home: &Path,
    armer: AgentHoldArmerWire,
    scope: AgentHoldScopeWire,
    selectors: AgentHoldSelectorsWire,
    duration_seconds: f64,
    liveness: &AgentHoldLivenessFactsWire,
    now: f64,
) -> Result<AgentHoldRecordWire, AgentHoldError> {
    validate_timestamp("now", now)?;
    if !duration_seconds.is_finite() || duration_seconds <= 0.0 {
        return Err(AgentHoldError::Validation(
            "duration_seconds must be finite and positive".to_string(),
        ));
    }
    let expires_at = now + duration_seconds;
    if !expires_at.is_finite() {
        return Err(AgentHoldError::Validation(
            "computed expires_at must be finite".to_string(),
        ));
    }
    arm_agent_hold_until(
        sase_home, armer, scope, selectors, expires_at, liveness, now,
    )
}

pub fn arm_agent_hold_until(
    sase_home: &Path,
    armer: AgentHoldArmerWire,
    scope: AgentHoldScopeWire,
    selectors: AgentHoldSelectorsWire,
    expires_at: f64,
    liveness: &AgentHoldLivenessFactsWire,
    now: f64,
) -> Result<AgentHoldRecordWire, AgentHoldError> {
    validate_timestamp("now", now)?;
    validate_timestamp("expires_at", expires_at)?;
    if expires_at <= now {
        return Err(AgentHoldError::Validation(
            "expires_at must be after created_at".to_string(),
        ));
    }
    let mut record = AgentHoldRecordWire {
        schema_version: AGENT_HOLD_WIRE_SCHEMA_VERSION,
        armer,
        scope,
        selectors,
        created_at: now,
        expires_at,
    };
    validate_and_normalize_record(&mut record)?;
    let key = record.armer.key.clone();
    with_hold_lock(sase_home, "arm_agent_hold", || {
        let mut records = read_records_locked(sase_home, liveness, now)?;
        records.insert(key, record.clone());
        write_or_remove_state(&agent_hold_state_path(sase_home), &records)?;
        Ok(record)
    })
}

pub fn release_agent_hold(
    sase_home: &Path,
    armer_key: &str,
    liveness: &AgentHoldLivenessFactsWire,
    now: f64,
) -> Result<bool, AgentHoldError> {
    validate_timestamp("now", now)?;
    let armer_key = validate_plain_string("armer_key", armer_key)?;
    with_hold_lock(sase_home, "release_agent_hold", || {
        let path = agent_hold_state_path(sase_home);
        let mut records = read_records_locked(sase_home, liveness, now)?;
        let removed = records.remove(&armer_key).is_some();
        write_or_remove_state(&path, &records)?;
        Ok(removed)
    })
}

pub fn hold_blocks_candidate(
    record: &AgentHoldRecordWire,
    candidate: &AgentHoldCandidateWire,
) -> Result<Option<AgentHoldBlockWire>, AgentHoldError> {
    let mut record = record.clone();
    validate_and_normalize_record(&mut record)?;
    let mut candidate = candidate.clone();
    validate_and_normalize_candidate(&mut candidate)?;
    if candidate.created_at >= record.expires_at {
        return Ok(None);
    }
    if !scope_matches(&record.scope, &candidate) {
        return Ok(None);
    }
    if armer_kin_excluded(&record.armer, &candidate) {
        return Ok(None);
    }
    let matches = selector_matches(&record, &candidate);
    if matches.is_empty() {
        return Ok(None);
    }
    Ok(Some(AgentHoldBlockWire {
        schema_version: AGENT_HOLD_WIRE_SCHEMA_VERSION,
        armer: AgentHoldBlockArmerWire {
            kind: record.armer.kind,
            key: record.armer.key,
            display: record.armer.display,
            project: record.armer.project,
        },
        expires_at: record.expires_at,
        matches,
    }))
}

fn snapshot_from_records(
    records: BTreeMap<String, AgentHoldRecordWire>,
) -> AgentHoldSnapshotWire {
    AgentHoldSnapshotWire {
        schema_version: AGENT_HOLD_WIRE_SCHEMA_VERSION,
        holds: records.into_values().collect(),
    }
}

fn read_records_locked(
    sase_home: &Path,
    liveness: &AgentHoldLivenessFactsWire,
    now: f64,
) -> Result<BTreeMap<String, AgentHoldRecordWire>, AgentHoldError> {
    let path = agent_hold_state_path(sase_home);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(BTreeMap::new())
        }
        Err(_) => {
            remove_state_best_effort(&path);
            return Ok(BTreeMap::new());
        }
    };
    let raw: RawAgentHoldStateWire = match serde_json::from_slice(&bytes) {
        Ok(raw) => raw,
        Err(_) => {
            remove_state_best_effort(&path);
            return Ok(BTreeMap::new());
        }
    };
    if raw.schema_version != AGENT_HOLD_WIRE_SCHEMA_VERSION {
        remove_state_best_effort(&path);
        return Ok(BTreeMap::new());
    }

    let mut changed = false;
    let mut records = BTreeMap::new();
    for (key, value) in raw.holds {
        let mut record =
            match serde_json::from_value::<AgentHoldRecordWire>(value) {
                Ok(record) => record,
                Err(_) => {
                    changed = true;
                    continue;
                }
            };
        if validate_and_normalize_record(&mut record).is_err()
            || record.armer.key != key
            || now >= record.expires_at
            || !armer_is_alive(&record, liveness)
        {
            changed = true;
            continue;
        }
        records.insert(key, record);
    }
    if changed {
        write_or_remove_state(&path, &records)?;
    }
    Ok(records)
}

fn write_or_remove_state(
    path: &Path,
    records: &BTreeMap<String, AgentHoldRecordWire>,
) -> Result<(), AgentHoldError> {
    if records.is_empty() {
        remove_invalid_state(path)?;
    } else {
        write_state_atomic(path, records)?;
    }
    Ok(())
}

fn write_state_atomic(
    path: &Path,
    records: &BTreeMap<String, AgentHoldRecordWire>,
) -> Result<(), AgentHoldError> {
    let parent = path.parent().ok_or_else(|| {
        AgentHoldError::Validation(
            "agent hold state path has no parent directory".to_string(),
        )
    })?;
    fs::create_dir_all(parent)?;
    let state = AgentHoldStateWire {
        schema_version: AGENT_HOLD_WIRE_SCHEMA_VERSION,
        holds: records.clone(),
    };
    let mut temporary = NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, &state)?;
    temporary.write_all(b"\n")?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn remove_invalid_state(path: &Path) -> Result<(), AgentHoldError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn remove_state_best_effort(path: &Path) {
    let _ = fs::remove_file(path);
}

fn with_hold_lock<T>(
    sase_home: &Path,
    operation_name: &str,
    operation: impl FnOnce() -> Result<T, AgentHoldError>,
) -> Result<T, AgentHoldError> {
    fs::create_dir_all(sase_home)?;
    let lock_path = agent_hold_lock_path(sase_home);
    let lock = acquire_store_lock(
        &lock_path,
        &holder_path_for(&lock_path),
        LockMode::Exclusive,
        timeout_from_env(LOCK_TIMEOUT_ENV, LOCK_TIMEOUT_DEFAULT),
        operation_name,
    )
    .map_err(lock_error_to_agent_hold)?;
    let result = operation();
    let unlock = lock.release();
    match (result, unlock) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}

fn lock_error_to_agent_hold(error: StoreLockError) -> AgentHoldError {
    match error {
        StoreLockError::Timeout {
            mode,
            lock_path,
            waited_ms,
            holder,
        } => AgentHoldError::LockTimeout {
            mode,
            path: lock_path,
            waited_ms,
            holder: holder
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
        },
        StoreLockError::Open { source, .. }
        | StoreLockError::Acquire { source, .. } => source.into(),
    }
}

fn armer_is_alive(
    record: &AgentHoldRecordWire,
    liveness: &AgentHoldLivenessFactsWire,
) -> bool {
    let Some(fact) = liveness.armers.get(&record.armer.key) else {
        return true;
    };
    match (record.armer.kind, fact) {
        (
            AgentHoldArmerKindWire::Agent,
            AgentHoldArmerLivenessFactWire::Agent {
                pid_alive,
                done_marker_present,
            },
        )
        | (
            AgentHoldArmerKindWire::Cli,
            AgentHoldArmerLivenessFactWire::Cli {
                pid_alive,
                done_marker_present,
            },
        ) => *pid_alive && !*done_marker_present,
        (
            AgentHoldArmerKindWire::Proc,
            AgentHoldArmerLivenessFactWire::Proc { terminal },
        ) => !*terminal,
        _ => true,
    }
}

fn validate_and_normalize_record(
    record: &mut AgentHoldRecordWire,
) -> Result<(), AgentHoldError> {
    if record.schema_version != AGENT_HOLD_WIRE_SCHEMA_VERSION {
        return Err(AgentHoldError::Validation(format!(
            "unsupported agent hold schema_version {}",
            record.schema_version
        )));
    }
    validate_and_normalize_armer(&mut record.armer)?;
    validate_and_normalize_scope(&mut record.scope)?;
    validate_and_normalize_selectors(&mut record.selectors)?;
    validate_timestamp("created_at", record.created_at)?;
    validate_timestamp("expires_at", record.expires_at)?;
    if record.expires_at <= record.created_at {
        return Err(AgentHoldError::Validation(
            "expires_at must be after created_at".to_string(),
        ));
    }
    Ok(())
}

fn validate_and_normalize_armer(
    armer: &mut AgentHoldArmerWire,
) -> Result<(), AgentHoldError> {
    armer.key = validate_plain_string("armer.key", &armer.key)?;
    armer.display = validate_plain_string("armer.display", &armer.display)?;
    armer.project = validate_plain_string("armer.project", &armer.project)?;
    armer.agent_name = normalize_optional_agent_name(
        "armer.agent_name",
        armer.agent_name.take(),
    )?;
    armer.family =
        normalize_optional_family("armer.family", armer.family.take())?;
    armer.clan = normalize_optional_plain("armer.clan", armer.clan.take())?;
    armer.proc_id =
        normalize_optional_plain("armer.proc_id", armer.proc_id.take())?;
    armer.done_marker_path = normalize_optional_plain(
        "armer.done_marker_path",
        armer.done_marker_path.take(),
    )?;
    match armer.kind {
        AgentHoldArmerKindWire::Agent => {
            if armer.agent_name.is_none() {
                return Err(AgentHoldError::Validation(
                    "agent armer requires agent_name".to_string(),
                ));
            }
            if armer.pid.is_none() && armer.done_marker_path.is_none() {
                return Err(AgentHoldError::Validation(
                    "agent armer requires pid or done_marker_path".to_string(),
                ));
            }
        }
        AgentHoldArmerKindWire::Proc => {
            if armer.proc_id.is_none() {
                return Err(AgentHoldError::Validation(
                    "proc armer requires proc_id".to_string(),
                ));
            }
        }
        AgentHoldArmerKindWire::Cli => {
            if armer.pid.is_none() && armer.done_marker_path.is_none() {
                return Err(AgentHoldError::Validation(
                    "cli armer requires pid or done_marker_path".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_and_normalize_scope(
    scope: &mut AgentHoldScopeWire,
) -> Result<(), AgentHoldError> {
    if let AgentHoldScopeWire::Project { project } = scope {
        *project = validate_plain_string("scope.project", project)?;
    }
    Ok(())
}

fn validate_and_normalize_selectors(
    selectors: &mut AgentHoldSelectorsWire,
) -> Result<(), AgentHoldError> {
    selectors.artifact_dirs = normalize_plain_vec(
        "selectors.artifact_dirs",
        &selectors.artifact_dirs,
    )?;
    selectors.names =
        normalize_agent_name_vec("selectors.names", &selectors.names)?;
    selectors.families =
        normalize_family_vec("selectors.families", &selectors.families)?;
    selectors.hoods = normalize_hood_vec("selectors.hoods", &selectors.hoods)?;
    selectors.clans = normalize_plain_vec("selectors.clans", &selectors.clans)?;
    selectors.workflows =
        normalize_plain_vec("selectors.workflows", &selectors.workflows)?;
    selectors.tribes =
        normalize_plain_vec("selectors.tribes", &selectors.tribes)?;
    if selectors.artifact_dirs.is_empty()
        && selectors.names.is_empty()
        && selectors.families.is_empty()
        && selectors.hoods.is_empty()
        && selectors.clans.is_empty()
        && selectors.workflows.is_empty()
        && selectors.tribes.is_empty()
        && !selectors.future
    {
        return Err(AgentHoldError::Validation(
            "at least one hold selector is required".to_string(),
        ));
    }
    Ok(())
}

fn validate_and_normalize_candidate(
    candidate: &mut AgentHoldCandidateWire,
) -> Result<(), AgentHoldError> {
    candidate.project =
        validate_plain_string("candidate.project", &candidate.project)?;
    validate_timestamp("candidate.created_at", candidate.created_at)?;
    candidate.artifact_dirs = normalize_plain_vec(
        "candidate.artifact_dirs",
        &candidate.artifact_dirs,
    )?;
    candidate.agent_name = normalize_optional_agent_name(
        "candidate.agent_name",
        candidate.agent_name.take(),
    )?;
    candidate.family =
        normalize_optional_family("candidate.family", candidate.family.take())?;
    candidate.clan =
        normalize_optional_plain("candidate.clan", candidate.clan.take())?;
    candidate.workflow = normalize_optional_plain(
        "candidate.workflow",
        candidate.workflow.take(),
    )?;
    candidate.tribe =
        normalize_optional_plain("candidate.tribe", candidate.tribe.take())?;
    candidate.armer_key = normalize_optional_plain(
        "candidate.armer_key",
        candidate.armer_key.take(),
    )?;
    Ok(())
}

fn validate_timestamp(label: &str, value: f64) -> Result<(), AgentHoldError> {
    if !value.is_finite() || value <= 0.0 {
        return Err(AgentHoldError::Validation(format!(
            "{label} must be finite and positive"
        )));
    }
    Ok(())
}

fn validate_plain_string(
    label: &str,
    value: &str,
) -> Result<String, AgentHoldError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AgentHoldError::Validation(format!(
            "{label} must be non-empty"
        )));
    }
    if trimmed != value {
        return Err(AgentHoldError::Validation(format!(
            "{label} must not contain leading or trailing whitespace"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(AgentHoldError::Validation(format!(
            "{label} must not contain control characters"
        )));
    }
    Ok(value.to_string())
}

fn normalize_optional_plain(
    label: &str,
    value: Option<String>,
) -> Result<Option<String>, AgentHoldError> {
    value
        .map(|value| validate_plain_string(label, &value))
        .transpose()
}

fn normalize_optional_agent_name(
    label: &str,
    value: Option<String>,
) -> Result<Option<String>, AgentHoldError> {
    value
        .map(|value| validate_agent_name_like(label, &value).map(|_| value))
        .transpose()
}

fn normalize_optional_family(
    label: &str,
    value: Option<String>,
) -> Result<Option<String>, AgentHoldError> {
    value
        .map(|value| normalize_family(label, &value))
        .transpose()
}

fn normalize_plain_vec(
    label: &str,
    values: &[String],
) -> Result<Vec<String>, AgentHoldError> {
    let mut set = BTreeSet::new();
    for value in values {
        set.insert(validate_plain_string(label, value)?);
    }
    Ok(set.into_iter().collect())
}

fn normalize_agent_name_vec(
    label: &str,
    values: &[String],
) -> Result<Vec<String>, AgentHoldError> {
    let mut set = BTreeSet::new();
    for value in values {
        validate_agent_name_like(label, value)?;
        set.insert(value.to_string());
    }
    Ok(set.into_iter().collect())
}

fn normalize_family_vec(
    label: &str,
    values: &[String],
) -> Result<Vec<String>, AgentHoldError> {
    let mut set = BTreeSet::new();
    for value in values {
        set.insert(normalize_family(label, value)?);
    }
    Ok(set.into_iter().collect())
}

fn normalize_hood_vec(
    label: &str,
    values: &[String],
) -> Result<Vec<String>, AgentHoldError> {
    let mut set = BTreeSet::new();
    for value in values {
        let hood = validate_plain_string(label, value)?;
        if hood.contains("--") {
            return Err(AgentHoldError::Validation(format!(
                "{label} must name a hood without a -- role suffix"
            )));
        }
        parse_agent_family_name(&hood).map_err(|error| {
            AgentHoldError::Validation(format!(
                "{label} is not a valid hood name: {error}"
            ))
        })?;
        set.insert(hood);
    }
    Ok(set.into_iter().collect())
}

fn validate_agent_name_like(
    label: &str,
    value: &str,
) -> Result<(), AgentHoldError> {
    let value = validate_plain_string(label, value)?;
    parse_agent_family_name(&value).map_err(|error| {
        AgentHoldError::Validation(format!(
            "{label} is not a valid agent name: {error}"
        ))
    })?;
    Ok(())
}

fn normalize_family(
    label: &str,
    value: &str,
) -> Result<String, AgentHoldError> {
    let value = validate_plain_string(label, value)?;
    let parsed = parse_agent_family_name(&value).map_err(|error| {
        AgentHoldError::Validation(format!(
            "{label} is not a valid family name: {error}"
        ))
    })?;
    Ok(historical_family_scope(&parsed.family_name))
}

fn scope_matches(
    scope: &AgentHoldScopeWire,
    candidate: &AgentHoldCandidateWire,
) -> bool {
    match scope {
        AgentHoldScopeWire::Project { project } => {
            candidate.project == *project
        }
        AgentHoldScopeWire::Host => true,
    }
}

fn armer_kin_excluded(
    armer: &AgentHoldArmerWire,
    candidate: &AgentHoldCandidateWire,
) -> bool {
    if candidate
        .armer_key
        .as_ref()
        .is_some_and(|key| key == &armer.key)
    {
        return true;
    }
    if let (Some(candidate_name), Some(armer_name)) =
        (&candidate.agent_name, &armer.agent_name)
    {
        if candidate_name == armer_name {
            return true;
        }
    }
    let armer_family = armer_family(armer);
    let candidate_family = candidate_family(candidate);
    if let (Some(candidate_family), Some(armer_family)) =
        (candidate_family.as_deref(), armer_family.as_deref())
    {
        if same_or_dotted_descendant(candidate_family, armer_family) {
            return true;
        }
    }
    if let (Some(candidate_clan), Some(armer_clan)) =
        (&candidate.clan, &armer.clan)
    {
        if candidate_clan == armer_clan {
            return true;
        }
    }
    false
}

fn armer_family(armer: &AgentHoldArmerWire) -> Option<String> {
    armer.family.clone().or_else(|| {
        armer
            .agent_name
            .as_deref()
            .and_then(|name| normalize_family("armer.agent_name", name).ok())
    })
}

fn candidate_family(candidate: &AgentHoldCandidateWire) -> Option<String> {
    candidate.family.clone().or_else(|| {
        candidate.agent_name.as_deref().and_then(|name| {
            normalize_family("candidate.agent_name", name).ok()
        })
    })
}

fn same_or_dotted_descendant(candidate: &str, armer: &str) -> bool {
    candidate == armer
        || candidate
            .strip_prefix(armer)
            .is_some_and(|suffix| suffix.starts_with('.'))
}

fn selector_matches(
    record: &AgentHoldRecordWire,
    candidate: &AgentHoldCandidateWire,
) -> Vec<AgentHoldSelectorMatchWire> {
    let selectors = &record.selectors;
    let mut matches = Vec::new();
    push_intersection_matches(
        &mut matches,
        "artifact_dir",
        &selectors.artifact_dirs,
        &candidate.artifact_dirs,
    );
    if let Some(agent_name) = &candidate.agent_name {
        push_exact_matches(&mut matches, "name", &selectors.names, agent_name);
    }
    if let Some(family) = candidate_family(candidate).as_deref() {
        push_exact_matches(&mut matches, "family", &selectors.families, family);
    }
    if let Some(agent_or_family) =
        candidate.agent_name.as_ref().or(candidate.family.as_ref())
    {
        for hood in &selectors.hoods {
            if agent_name_in_hood(agent_or_family, hood).unwrap_or(false) {
                matches.push(AgentHoldSelectorMatchWire {
                    kind: "hood".to_string(),
                    value: hood.clone(),
                });
            }
        }
    }
    if let Some(clan) = &candidate.clan {
        push_exact_matches(&mut matches, "clan", &selectors.clans, clan);
    }
    if let Some(workflow) = &candidate.workflow {
        push_exact_matches(
            &mut matches,
            "workflow",
            &selectors.workflows,
            workflow,
        );
    }
    if let Some(tribe) = &candidate.tribe {
        push_exact_matches(&mut matches, "tribe", &selectors.tribes, tribe);
    }
    if selectors.future && candidate.created_at > record.created_at {
        matches.push(AgentHoldSelectorMatchWire {
            kind: "future".to_string(),
            value: "true".to_string(),
        });
    }
    matches
}

fn push_intersection_matches(
    matches: &mut Vec<AgentHoldSelectorMatchWire>,
    kind: &str,
    selectors: &[String],
    candidates: &[String],
) {
    let candidate_set: BTreeSet<_> = candidates.iter().collect();
    for selector in selectors {
        if candidate_set.contains(selector) {
            matches.push(AgentHoldSelectorMatchWire {
                kind: kind.to_string(),
                value: selector.clone(),
            });
        }
    }
}

fn push_exact_matches(
    matches: &mut Vec<AgentHoldSelectorMatchWire>,
    kind: &str,
    selectors: &[String],
    candidate: &str,
) {
    for selector in selectors {
        if selector == candidate {
            matches.push(AgentHoldSelectorMatchWire {
                kind: kind.to_string(),
                value: selector.clone(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs2::FileExt;
    use serde_json::json;
    use std::fs::OpenOptions;
    use tempfile::tempdir;

    const NOW: f64 = 1_800_000_000.0;

    fn armer(key: &str) -> AgentHoldArmerWire {
        AgentHoldArmerWire {
            kind: AgentHoldArmerKindWire::Agent,
            key: key.to_string(),
            display: format!("{key} display"),
            project: "sase".to_string(),
            agent_name: Some(format!("{key}.worker")),
            family: Some(format!("{key}.worker")),
            clan: Some("builders".to_string()),
            proc_id: None,
            pid: Some(1234),
            done_marker_path: None,
        }
    }

    fn proc_armer(key: &str) -> AgentHoldArmerWire {
        AgentHoldArmerWire {
            kind: AgentHoldArmerKindWire::Proc,
            key: key.to_string(),
            display: "proc display".to_string(),
            project: "sase".to_string(),
            agent_name: None,
            family: None,
            clan: None,
            proc_id: Some(format!("proc-{key}")),
            pid: None,
            done_marker_path: None,
        }
    }

    fn selectors() -> AgentHoldSelectorsWire {
        AgentHoldSelectorsWire {
            artifact_dirs: vec!["artifacts/old".to_string()],
            names: vec!["target.agent--code".to_string()],
            families: vec!["target.agent".to_string()],
            hoods: vec!["target".to_string()],
            clans: vec!["blocked-clan".to_string()],
            workflows: vec!["wf".to_string()],
            tribes: vec!["tribe-a".to_string()],
            future: true,
        }
    }

    fn liveness_alive(key: &str) -> AgentHoldLivenessFactsWire {
        AgentHoldLivenessFactsWire {
            armers: BTreeMap::from([(
                key.to_string(),
                AgentHoldArmerLivenessFactWire::Agent {
                    pid_alive: true,
                    done_marker_present: false,
                },
            )]),
        }
    }

    fn candidate() -> AgentHoldCandidateWire {
        AgentHoldCandidateWire {
            project: "sase".to_string(),
            created_at: NOW + 5.0,
            artifact_dirs: vec!["artifacts/old".to_string()],
            agent_name: Some("target.agent--code".to_string()),
            family: None,
            clan: Some("blocked-clan".to_string()),
            workflow: Some("wf".to_string()),
            tribe: Some("tribe-a".to_string()),
            armer_key: None,
        }
    }

    #[test]
    fn arm_list_rearm_and_release_round_trip() {
        let temp = tempdir().unwrap();
        let live = liveness_alive("a");
        let first = arm_agent_hold_relative(
            temp.path(),
            armer("a"),
            AgentHoldScopeWire::Project {
                project: "sase".to_string(),
            },
            selectors(),
            30.0,
            &live,
            NOW,
        )
        .unwrap();
        assert_eq!(first.expires_at, NOW + 30.0);

        let replacement = arm_agent_hold_until(
            temp.path(),
            armer("a"),
            AgentHoldScopeWire::Host,
            AgentHoldSelectorsWire {
                future: true,
                ..AgentHoldSelectorsWire::default()
            },
            NOW + 60.0,
            &live,
            NOW + 1.0,
        )
        .unwrap();
        assert_eq!(replacement.expires_at, NOW + 60.0);
        let snapshot = list_agent_holds(temp.path(), &live, NOW + 2.0).unwrap();
        assert_eq!(snapshot.holds.len(), 1);
        assert!(matches!(snapshot.holds[0].scope, AgentHoldScopeWire::Host));

        assert!(release_agent_hold(temp.path(), "a", &live, NOW + 3.0).unwrap());
        assert!(
            !release_agent_hold(temp.path(), "a", &live, NOW + 4.0).unwrap()
        );
        assert!(list_agent_holds(temp.path(), &live, NOW + 5.0)
            .unwrap()
            .holds
            .is_empty());
    }

    #[test]
    fn list_prunes_expired_malformed_stale_schema_and_dead_armers() {
        let temp = tempdir().unwrap();
        let path = agent_hold_state_path(temp.path());
        let mut stale = armer("stale");
        stale.key = "stale".to_string();
        let valid = AgentHoldRecordWire {
            schema_version: AGENT_HOLD_WIRE_SCHEMA_VERSION,
            armer: stale,
            scope: AgentHoldScopeWire::Host,
            selectors: AgentHoldSelectorsWire {
                future: true,
                ..AgentHoldSelectorsWire::default()
            },
            created_at: NOW - 10.0,
            expires_at: NOW + 10.0,
        };
        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "schema_version": AGENT_HOLD_WIRE_SCHEMA_VERSION,
                "holds": {
                    "expired": {
                        "schema_version": AGENT_HOLD_WIRE_SCHEMA_VERSION,
                        "armer": armer("expired"),
                        "scope": {"kind": "host"},
                        "selectors": {"future": true},
                        "created_at": NOW - 20.0,
                        "expires_at": NOW
                    },
                    "malformed": {"schema_version": AGENT_HOLD_WIRE_SCHEMA_VERSION},
                    "stale": valid
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let dead = AgentHoldLivenessFactsWire {
            armers: BTreeMap::from([(
                "stale".to_string(),
                AgentHoldArmerLivenessFactWire::Agent {
                    pid_alive: false,
                    done_marker_present: false,
                },
            )]),
        };
        let snapshot = list_agent_holds(temp.path(), &dead, NOW).unwrap();
        assert!(snapshot.holds.is_empty());
        assert!(!path.exists());

        fs::write(
            &path,
            br#"{"schema_version":999,"holds":{"x":{"bad":true}}}"#,
        )
        .unwrap();
        assert!(list_agent_holds(temp.path(), &dead, NOW)
            .unwrap()
            .holds
            .is_empty());
        assert!(!path.exists());

        fs::write(&path, b"not json").unwrap();
        assert!(list_agent_holds(temp.path(), &dead, NOW)
            .unwrap()
            .holds
            .is_empty());
        assert!(!path.exists());
    }

    #[test]
    fn proc_terminal_fact_prunes_record() {
        let temp = tempdir().unwrap();
        let liveness = AgentHoldLivenessFactsWire::default();
        arm_agent_hold_relative(
            temp.path(),
            proc_armer("p"),
            AgentHoldScopeWire::Host,
            AgentHoldSelectorsWire {
                future: true,
                ..AgentHoldSelectorsWire::default()
            },
            30.0,
            &liveness,
            NOW,
        )
        .unwrap();
        let terminal = AgentHoldLivenessFactsWire {
            armers: BTreeMap::from([(
                "p".to_string(),
                AgentHoldArmerLivenessFactWire::Proc { terminal: true },
            )]),
        };
        assert!(list_agent_holds(temp.path(), &terminal, NOW + 1.0)
            .unwrap()
            .holds
            .is_empty());
    }

    #[test]
    fn unreadable_store_path_is_treated_as_empty() {
        let temp = tempdir().unwrap();
        let path = agent_hold_state_path(temp.path());
        fs::create_dir(&path).unwrap();

        let snapshot = list_agent_holds(
            temp.path(),
            &AgentHoldLivenessFactsWire::default(),
            NOW,
        )
        .unwrap();

        assert!(snapshot.holds.is_empty());
        assert!(path.is_dir());
    }

    #[test]
    fn predicate_matches_each_selector_and_scope() {
        let record = AgentHoldRecordWire {
            schema_version: AGENT_HOLD_WIRE_SCHEMA_VERSION,
            armer: armer("holder"),
            scope: AgentHoldScopeWire::Project {
                project: "sase".to_string(),
            },
            selectors: selectors(),
            created_at: NOW,
            expires_at: NOW + 60.0,
        };
        let block = hold_blocks_candidate(&record, &candidate())
            .unwrap()
            .unwrap();
        assert_eq!(
            block
                .matches
                .iter()
                .map(|m| m.kind.as_str())
                .collect::<Vec<_>>(),
            [
                "artifact_dir",
                "name",
                "family",
                "hood",
                "clan",
                "workflow",
                "tribe",
                "future"
            ]
        );

        let mut other_project = candidate();
        other_project.project = "other".to_string();
        assert!(hold_blocks_candidate(&record, &other_project)
            .unwrap()
            .is_none());

        let mut host_record = record.clone();
        host_record.scope = AgentHoldScopeWire::Host;
        assert!(hold_blocks_candidate(&host_record, &other_project)
            .unwrap()
            .is_some());
    }

    #[test]
    fn future_selector_only_matches_after_arm_time() {
        let record = AgentHoldRecordWire {
            schema_version: AGENT_HOLD_WIRE_SCHEMA_VERSION,
            armer: armer("holder"),
            scope: AgentHoldScopeWire::Host,
            selectors: AgentHoldSelectorsWire {
                future: true,
                ..AgentHoldSelectorsWire::default()
            },
            created_at: NOW,
            expires_at: NOW + 60.0,
        };
        let mut before = candidate();
        before.created_at = NOW - 1.0;
        before.artifact_dirs.clear();
        before.agent_name = Some("other.agent".to_string());
        before.clan = None;
        before.workflow = None;
        before.tribe = None;
        assert!(hold_blocks_candidate(&record, &before).unwrap().is_none());

        before.created_at = NOW;
        assert!(hold_blocks_candidate(&record, &before).unwrap().is_none());

        before.created_at = NOW + 0.001;
        assert!(hold_blocks_candidate(&record, &before).unwrap().is_some());
    }

    #[test]
    fn armer_kin_exclusion_covers_self_family_clan_and_descendant() {
        let record = AgentHoldRecordWire {
            schema_version: AGENT_HOLD_WIRE_SCHEMA_VERSION,
            armer: armer("holder"),
            scope: AgentHoldScopeWire::Host,
            selectors: AgentHoldSelectorsWire {
                future: true,
                ..AgentHoldSelectorsWire::default()
            },
            created_at: NOW,
            expires_at: NOW + 60.0,
        };
        let cases = [
            AgentHoldCandidateWire {
                armer_key: Some("holder".to_string()),
                ..candidate()
            },
            AgentHoldCandidateWire {
                agent_name: Some("holder.worker".to_string()),
                ..candidate()
            },
            AgentHoldCandidateWire {
                family: Some("holder.worker.child".to_string()),
                ..candidate()
            },
            AgentHoldCandidateWire {
                clan: Some("builders".to_string()),
                ..candidate()
            },
        ];
        for case in cases {
            assert!(hold_blocks_candidate(&record, &case).unwrap().is_none());
        }

        let mut historical_record = record;
        historical_record.armer.agent_name =
            Some("fi--code.f0--plan".to_string());
        historical_record.armer.family = None;
        historical_record.armer.clan = None;
        let historical_cases = ["fi.f0--code", "fi.f0.child--plan"];
        for name in historical_cases {
            let mut historical_candidate = candidate();
            historical_candidate.agent_name = Some(name.to_string());
            historical_candidate.family = None;
            historical_candidate.clan = None;
            assert!(hold_blocks_candidate(
                &historical_record,
                &historical_candidate
            )
            .unwrap()
            .is_none());
        }
    }

    #[test]
    fn hood_matching_honors_component_boundaries_and_shell_suffixes() {
        let mut record = AgentHoldRecordWire {
            schema_version: AGENT_HOLD_WIRE_SCHEMA_VERSION,
            armer: armer("holder"),
            scope: AgentHoldScopeWire::Host,
            selectors: AgentHoldSelectorsWire {
                hoods: vec!["foo".to_string()],
                ..AgentHoldSelectorsWire::default()
            },
            created_at: NOW,
            expires_at: NOW + 60.0,
        };
        let mut hit = candidate();
        hit.agent_name = Some("foo.bar--code".to_string());
        hit.artifact_dirs.clear();
        hit.clan = None;
        hit.workflow = None;
        hit.tribe = None;
        assert!(hold_blocks_candidate(&record, &hit).unwrap().is_some());

        let mut miss = hit.clone();
        miss.agent_name = Some("foobar.baz".to_string());
        assert!(hold_blocks_candidate(&record, &miss).unwrap().is_none());

        record.selectors.hoods = vec!["fi".to_string()];
        hit.agent_name = Some("fi--code.f0--plan".to_string());
        assert!(hold_blocks_candidate(&record, &hit).unwrap().is_some());
    }

    #[test]
    fn validation_rejects_bad_inputs_and_sorts_sets() {
        let temp = tempdir().unwrap();
        let mut bad = armer("a");
        bad.project = String::new();
        let error = arm_agent_hold_relative(
            temp.path(),
            bad,
            AgentHoldScopeWire::Host,
            selectors(),
            10.0,
            &AgentHoldLivenessFactsWire::default(),
            NOW,
        )
        .unwrap_err();
        assert!(matches!(error, AgentHoldError::Validation(_)));

        let record = arm_agent_hold_relative(
            temp.path(),
            armer("a"),
            AgentHoldScopeWire::Host,
            AgentHoldSelectorsWire {
                tribes: vec!["z".to_string(), "a".to_string(), "z".to_string()],
                ..AgentHoldSelectorsWire::default()
            },
            10.0,
            &AgentHoldLivenessFactsWire::default(),
            NOW,
        )
        .unwrap();
        assert_eq!(record.selectors.tribes, ["a", "z"]);
    }

    #[test]
    fn lock_wait_is_bounded() {
        let temp = tempdir().unwrap();
        let lock_path = agent_hold_lock_path(temp.path());
        fs::create_dir_all(temp.path()).unwrap();
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        lock.lock_exclusive().unwrap();
        std::env::set_var(LOCK_TIMEOUT_ENV, "0.01");
        let result = list_agent_holds(
            temp.path(),
            &AgentHoldLivenessFactsWire::default(),
            NOW,
        );
        std::env::remove_var(LOCK_TIMEOUT_ENV);
        lock.unlock().unwrap();
        assert!(matches!(result, Err(AgentHoldError::LockTimeout { .. })));
    }
}
