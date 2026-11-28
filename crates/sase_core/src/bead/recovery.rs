//! Core-owned activation and recovery-claim records for zombie epic recovery.
//!
//! Records live in a `recovery/` namespace inside the beads sidecar and stay
//! independent of issue lifecycle status and assignee. Writes are
//! create-if-absent under the bead-store mutation lock. Host adapters publish
//! and confirm a winner; this module never starts a process.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::jsonl::write_file_atomic;
use super::mutation::{with_bead_mutation_lock, MutationLockWait};
use super::wire::BeadError;

pub const RECOVERY_WIRE_SCHEMA_VERSION: u32 = 1;

const RECOVERY_DIR: &str = "recovery";
const ACTIVATION_FILE: &str = "activation.json";
const CLAIMS_DIR: &str = "claims";

fn default_schema_version() -> u32 {
    RECOVERY_WIRE_SCHEMA_VERSION
}

/// Versioned project activation: the operational rollout cutoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryActivationWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub project_id: String,
    pub armed_at: String,
    pub generation: u64,
}

/// Durable recovery claim for one `(project, epic, close-token)` key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryClaimWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub project_id: String,
    pub epic_id: String,
    pub close_token: String,
    pub owner_machine: String,
    pub proc_id: String,
    pub request_fingerprint: String,
    pub activation_generation: u64,
    pub created_at: String,
}

/// Read-only activation inspection. Malformed records disable recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryActivationInspectWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub project_id: String,
    /// `absent`, `armed`, or `malformed`.
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<RecoveryActivationWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    pub recovery_enabled: bool,
}

/// Create-if-absent arming request. Existing cutoffs are never reset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryArmRequestWire {
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub armed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
}

/// Result of create-if-absent activation arming.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryArmResultWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// `created`, `existing`, `malformed`, or `refused`.
    pub state: String,
    pub created: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<RecoveryActivationWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    pub recovery_enabled: bool,
    #[serde(default)]
    pub lock_wait_ms: u64,
}

impl MutationLockWait for RecoveryArmResultWire {
    fn set_lock_wait_ms(&mut self, lock_wait_ms: u64) {
        self.lock_wait_ms = lock_wait_ms;
    }
}

/// Lookup key for one recovery claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryClaimLookupWire {
    pub project_id: String,
    pub epic_id: String,
    pub close_token: String,
}

/// Read-only claim inspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryClaimInspectWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub claim_key: String,
    pub project_id: String,
    pub epic_id: String,
    pub close_token: String,
    /// `absent`, `present`, `malformed`, or `refused`.
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim: Option<RecoveryClaimWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

/// Create-if-absent claim request. Existing claims are never replaced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryClaimCreateRequestWire {
    pub project_id: String,
    pub epic_id: String,
    pub close_token: String,
    pub owner_machine: String,
    pub proc_id: String,
    pub request_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

/// Result of create-if-absent claim publication into the local store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryClaimCreateResultWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub claim_key: String,
    /// `created`, `existing`, `malformed`, or `refused`.
    pub state: String,
    pub created: bool,
    /// True when the stored claim matches this request's owner/proc identity.
    pub local_winner: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim: Option<RecoveryClaimWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    #[serde(default)]
    pub lock_wait_ms: u64,
}

impl MutationLockWait for RecoveryClaimCreateResultWire {
    fn set_lock_wait_ms(&mut self, lock_wait_ms: u64) {
        self.lock_wait_ms = lock_wait_ms;
    }
}

/// Combined read-only view of one project's recovery namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryStoreInspectWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub activation: RecoveryActivationInspectWire,
    #[serde(default)]
    pub claims: Vec<RecoveryClaimInspectWire>,
}

/// Deterministic claim key for `(project identity, epic ID, close token)`.
pub fn recovery_claim_key(
    project_id: &str,
    epic_id: &str,
    close_token: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(project_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(epic_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(close_token.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn inspect_recovery_activation(
    beads_dir: &Path,
    project_id: &str,
) -> Result<RecoveryActivationInspectWire, BeadError> {
    require_project_id(project_id)?;
    require_beads_dir(beads_dir)?;
    Ok(read_activation_inspect(beads_dir, project_id))
}

pub fn arm_recovery_activation(
    beads_dir: &Path,
    request: &RecoveryArmRequestWire,
) -> Result<RecoveryArmResultWire, BeadError> {
    require_beads_dir(beads_dir)?;
    let prepared = match prepare_activation(request) {
        Ok(activation) => activation,
        Err(diagnostic) => {
            return Ok(RecoveryArmResultWire {
                schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                state: "refused".to_string(),
                created: false,
                activation: None,
                diagnostic: Some(diagnostic),
                recovery_enabled: false,
                lock_wait_ms: 0,
            });
        }
    };
    with_bead_mutation_lock(beads_dir, "recovery_arm_activation", || {
        Ok(arm_activation_locked(beads_dir, prepared))
    })
}

pub fn inspect_recovery_claim(
    beads_dir: &Path,
    lookup: &RecoveryClaimLookupWire,
) -> Result<RecoveryClaimInspectWire, BeadError> {
    require_beads_dir(beads_dir)?;
    if let Err(diagnostic) = validate_lookup(lookup) {
        return Ok(refused_claim_inspect(lookup, diagnostic));
    }
    Ok(read_claim_inspect(beads_dir, lookup))
}

pub fn create_recovery_claim(
    beads_dir: &Path,
    request: &RecoveryClaimCreateRequestWire,
) -> Result<RecoveryClaimCreateResultWire, BeadError> {
    require_beads_dir(beads_dir)?;
    if let Err(diagnostic) = validate_create_request(request) {
        return Ok(refused_claim_create(request, diagnostic));
    }
    with_bead_mutation_lock(beads_dir, "recovery_create_claim", || {
        Ok(create_claim_locked(beads_dir, request))
    })
}

pub fn inspect_recovery_store(
    beads_dir: &Path,
    project_id: &str,
) -> Result<RecoveryStoreInspectWire, BeadError> {
    require_project_id(project_id)?;
    require_beads_dir(beads_dir)?;
    let activation = read_activation_inspect(beads_dir, project_id);
    let mut claims = read_claim_inspects(beads_dir, project_id)?;
    claims.sort_by(|left, right| left.claim_key.cmp(&right.claim_key));
    Ok(RecoveryStoreInspectWire {
        schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
        activation,
        claims,
    })
}

fn require_beads_dir(beads_dir: &Path) -> Result<(), BeadError> {
    if beads_dir.is_dir() {
        Ok(())
    } else {
        Err(BeadError::io(format!(
            "No beads directory found at {}",
            beads_dir.display()
        )))
    }
}

fn require_project_id(project_id: &str) -> Result<(), BeadError> {
    if project_id.trim().is_empty() {
        Err(BeadError::validation(
            "recovery project_id must be a non-empty string",
        ))
    } else {
        Ok(())
    }
}

fn recovery_dir(beads_dir: &Path) -> PathBuf {
    beads_dir.join(RECOVERY_DIR)
}

fn activation_path(beads_dir: &Path) -> PathBuf {
    recovery_dir(beads_dir).join(ACTIVATION_FILE)
}

fn claims_dir(beads_dir: &Path) -> PathBuf {
    recovery_dir(beads_dir).join(CLAIMS_DIR)
}

fn claim_path(beads_dir: &Path, claim_key: &str) -> PathBuf {
    claims_dir(beads_dir).join(format!("{claim_key}.json"))
}

fn now_utc() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn is_rfc3339(value: &str) -> bool {
    DateTime::parse_from_rfc3339(value).is_ok()
}

fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>, BeadError> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    if !bytes.ends_with(b"\n") {
        bytes.push(b'\n');
    }
    Ok(bytes)
}

fn required_text(value: &str, field: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("recovery {field} must be a non-empty string"))
    } else {
        Ok(())
    }
}

fn prepare_activation(
    request: &RecoveryArmRequestWire,
) -> Result<RecoveryActivationWire, String> {
    required_text(&request.project_id, "project_id")?;
    let armed_at = request.armed_at.clone().unwrap_or_else(now_utc);
    if !is_rfc3339(&armed_at) {
        return Err("recovery armed_at must be RFC3339".to_string());
    }
    let generation = request.generation.unwrap_or(1);
    if generation < 1 {
        return Err("recovery activation generation must be >= 1".to_string());
    }
    Ok(RecoveryActivationWire {
        schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
        project_id: request.project_id.clone(),
        armed_at,
        generation,
    })
}

fn validate_lookup(lookup: &RecoveryClaimLookupWire) -> Result<(), String> {
    required_text(&lookup.project_id, "project_id")?;
    required_text(&lookup.epic_id, "epic_id")?;
    required_text(&lookup.close_token, "close_token")?;
    Ok(())
}

fn validate_create_request(
    request: &RecoveryClaimCreateRequestWire,
) -> Result<(), String> {
    required_text(&request.project_id, "project_id")?;
    required_text(&request.epic_id, "epic_id")?;
    required_text(&request.close_token, "close_token")?;
    required_text(&request.owner_machine, "owner_machine")?;
    required_text(&request.proc_id, "proc_id")?;
    required_text(&request.request_fingerprint, "request_fingerprint")?;
    if let Some(created_at) = request.created_at.as_deref() {
        if !is_rfc3339(created_at) {
            return Err("recovery created_at must be RFC3339".to_string());
        }
    }
    if let Some(generation) = request.activation_generation {
        if generation < 1 {
            return Err(
                "recovery activation generation must be >= 1".to_string()
            );
        }
    }
    Ok(())
}

fn parse_activation_bytes(
    bytes: &[u8],
    expected_project: &str,
) -> Result<RecoveryActivationWire, String> {
    let activation: RecoveryActivationWire = serde_json::from_slice(bytes)
        .map_err(|error| format!("unreadable recovery activation: {error}"))?;
    validate_activation(&activation, expected_project)?;
    Ok(activation)
}

fn validate_activation(
    activation: &RecoveryActivationWire,
    expected_project: &str,
) -> Result<(), String> {
    if activation.schema_version != RECOVERY_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported recovery activation schema_version {}",
            activation.schema_version
        ));
    }
    required_text(&activation.project_id, "project_id")?;
    if activation.project_id != expected_project {
        return Err(format!(
            "recovery activation project_id {} does not match {expected_project}",
            activation.project_id
        ));
    }
    if !is_rfc3339(&activation.armed_at) {
        return Err("recovery armed_at must be RFC3339".to_string());
    }
    if activation.generation < 1 {
        return Err("recovery activation generation must be >= 1".to_string());
    }
    Ok(())
}

fn parse_claim_bytes(
    bytes: &[u8],
    expected_project: &str,
) -> Result<RecoveryClaimWire, String> {
    let claim: RecoveryClaimWire = serde_json::from_slice(bytes)
        .map_err(|error| format!("unreadable recovery claim: {error}"))?;
    validate_claim(&claim, expected_project)?;
    Ok(claim)
}

fn validate_claim(
    claim: &RecoveryClaimWire,
    expected_project: &str,
) -> Result<(), String> {
    if claim.schema_version != RECOVERY_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported recovery claim schema_version {}",
            claim.schema_version
        ));
    }
    required_text(&claim.project_id, "project_id")?;
    required_text(&claim.epic_id, "epic_id")?;
    required_text(&claim.close_token, "close_token")?;
    required_text(&claim.owner_machine, "owner_machine")?;
    required_text(&claim.proc_id, "proc_id")?;
    required_text(&claim.request_fingerprint, "request_fingerprint")?;
    if claim.project_id != expected_project {
        return Err(format!(
            "recovery claim project_id {} does not match {expected_project}",
            claim.project_id
        ));
    }
    if !is_rfc3339(&claim.created_at) {
        return Err("recovery created_at must be RFC3339".to_string());
    }
    if claim.activation_generation < 1 {
        return Err("recovery activation generation must be >= 1".to_string());
    }
    Ok(())
}

fn read_activation_inspect(
    beads_dir: &Path,
    project_id: &str,
) -> RecoveryActivationInspectWire {
    match fs::read(activation_path(beads_dir)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            RecoveryActivationInspectWire {
                schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                project_id: project_id.to_string(),
                state: "absent".to_string(),
                activation: None,
                diagnostic: None,
                recovery_enabled: false,
            }
        }
        Err(error) => RecoveryActivationInspectWire {
            schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
            project_id: project_id.to_string(),
            state: "malformed".to_string(),
            activation: None,
            diagnostic: Some(format!(
                "unreadable recovery activation: {error}"
            )),
            recovery_enabled: false,
        },
        Ok(bytes) => match parse_activation_bytes(&bytes, project_id) {
            Ok(activation) => RecoveryActivationInspectWire {
                schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                project_id: project_id.to_string(),
                state: "armed".to_string(),
                activation: Some(activation),
                diagnostic: None,
                recovery_enabled: true,
            },
            Err(diagnostic) => RecoveryActivationInspectWire {
                schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                project_id: project_id.to_string(),
                state: "malformed".to_string(),
                activation: None,
                diagnostic: Some(diagnostic),
                recovery_enabled: false,
            },
        },
    }
}

fn arm_activation_locked(
    beads_dir: &Path,
    prepared: RecoveryActivationWire,
) -> RecoveryArmResultWire {
    let inspect = read_activation_inspect(beads_dir, &prepared.project_id);
    match inspect.state.as_str() {
        "armed" => RecoveryArmResultWire {
            schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
            state: "existing".to_string(),
            created: false,
            activation: inspect.activation,
            diagnostic: None,
            recovery_enabled: true,
            lock_wait_ms: 0,
        },
        "malformed" => RecoveryArmResultWire {
            schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
            state: "malformed".to_string(),
            created: false,
            activation: None,
            diagnostic: inspect.diagnostic,
            recovery_enabled: false,
            lock_wait_ms: 0,
        },
        _ => match encode_json(&prepared).and_then(|bytes| {
            write_file_atomic(&activation_path(beads_dir), &bytes)
        }) {
            Ok(()) => RecoveryArmResultWire {
                schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                state: "created".to_string(),
                created: true,
                activation: Some(prepared),
                diagnostic: None,
                recovery_enabled: true,
                lock_wait_ms: 0,
            },
            Err(error) => RecoveryArmResultWire {
                schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                state: "refused".to_string(),
                created: false,
                activation: None,
                diagnostic: Some(error.message),
                recovery_enabled: false,
                lock_wait_ms: 0,
            },
        },
    }
}

fn read_claim_inspect(
    beads_dir: &Path,
    lookup: &RecoveryClaimLookupWire,
) -> RecoveryClaimInspectWire {
    let claim_key = recovery_claim_key(
        &lookup.project_id,
        &lookup.epic_id,
        &lookup.close_token,
    );
    inspect_claim_file(beads_dir, lookup, &claim_key)
}

fn inspect_claim_file(
    beads_dir: &Path,
    lookup: &RecoveryClaimLookupWire,
    claim_key: &str,
) -> RecoveryClaimInspectWire {
    match fs::read(claim_path(beads_dir, claim_key)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            RecoveryClaimInspectWire {
                schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                claim_key: claim_key.to_string(),
                project_id: lookup.project_id.clone(),
                epic_id: lookup.epic_id.clone(),
                close_token: lookup.close_token.clone(),
                state: "absent".to_string(),
                claim: None,
                diagnostic: None,
            }
        }
        Err(error) => RecoveryClaimInspectWire {
            schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
            claim_key: claim_key.to_string(),
            project_id: lookup.project_id.clone(),
            epic_id: lookup.epic_id.clone(),
            close_token: lookup.close_token.clone(),
            state: "malformed".to_string(),
            claim: None,
            diagnostic: Some(format!("unreadable recovery claim: {error}")),
        },
        Ok(bytes) => match parse_claim_bytes(&bytes, &lookup.project_id) {
            Ok(claim) => {
                let expected_key = recovery_claim_key(
                    &claim.project_id,
                    &claim.epic_id,
                    &claim.close_token,
                );
                if expected_key != claim_key {
                    RecoveryClaimInspectWire {
                        schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                        claim_key: claim_key.to_string(),
                        project_id: lookup.project_id.clone(),
                        epic_id: lookup.epic_id.clone(),
                        close_token: lookup.close_token.clone(),
                        state: "malformed".to_string(),
                        claim: None,
                        diagnostic: Some(
                            "recovery claim key does not match stored identity"
                                .to_string(),
                        ),
                    }
                } else {
                    RecoveryClaimInspectWire {
                        schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                        claim_key: claim_key.to_string(),
                        project_id: lookup.project_id.clone(),
                        epic_id: lookup.epic_id.clone(),
                        close_token: lookup.close_token.clone(),
                        state: "present".to_string(),
                        claim: Some(claim),
                        diagnostic: None,
                    }
                }
            }
            Err(diagnostic) => RecoveryClaimInspectWire {
                schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                claim_key: claim_key.to_string(),
                project_id: lookup.project_id.clone(),
                epic_id: lookup.epic_id.clone(),
                close_token: lookup.close_token.clone(),
                state: "malformed".to_string(),
                claim: None,
                diagnostic: Some(diagnostic),
            },
        },
    }
}

fn create_claim_locked(
    beads_dir: &Path,
    request: &RecoveryClaimCreateRequestWire,
) -> RecoveryClaimCreateResultWire {
    let inspect = read_activation_inspect(beads_dir, &request.project_id);
    if inspect.state == "malformed" {
        return refused_claim_create(
            request,
            inspect
                .diagnostic
                .unwrap_or_else(|| "malformed recovery activation".to_string()),
        );
    }
    let Some(activation) = inspect.activation else {
        return refused_claim_create(
            request,
            "recovery activation is absent; arm the project before claiming"
                .to_string(),
        );
    };
    if let Some(generation) = request.activation_generation {
        if generation != activation.generation {
            return refused_claim_create(
                request,
                format!(
                    "recovery claim generation {generation} does not match activation generation {}",
                    activation.generation
                ),
            );
        }
    }
    let lookup = RecoveryClaimLookupWire {
        project_id: request.project_id.clone(),
        epic_id: request.epic_id.clone(),
        close_token: request.close_token.clone(),
    };
    let existing = read_claim_inspect(beads_dir, &lookup);
    match existing.state.as_str() {
        "present" => {
            let claim = existing.claim.clone();
            let local_winner = claim
                .as_ref()
                .is_some_and(|stored| claim_matches_request(stored, request));
            RecoveryClaimCreateResultWire {
                schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                claim_key: existing.claim_key,
                state: "existing".to_string(),
                created: false,
                local_winner,
                claim,
                diagnostic: None,
                lock_wait_ms: 0,
            }
        }
        "malformed" => RecoveryClaimCreateResultWire {
            schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
            claim_key: existing.claim_key,
            state: "malformed".to_string(),
            created: false,
            local_winner: false,
            claim: None,
            diagnostic: existing.diagnostic,
            lock_wait_ms: 0,
        },
        _ => {
            let claim = RecoveryClaimWire {
                schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                project_id: request.project_id.clone(),
                epic_id: request.epic_id.clone(),
                close_token: request.close_token.clone(),
                owner_machine: request.owner_machine.clone(),
                proc_id: request.proc_id.clone(),
                request_fingerprint: request.request_fingerprint.clone(),
                activation_generation: activation.generation,
                created_at: request.created_at.clone().unwrap_or_else(now_utc),
            };
            let claim_key = recovery_claim_key(
                &claim.project_id,
                &claim.epic_id,
                &claim.close_token,
            );
            match encode_json(&claim).and_then(|bytes| {
                write_file_atomic(&claim_path(beads_dir, &claim_key), &bytes)
            }) {
                Ok(()) => RecoveryClaimCreateResultWire {
                    schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                    claim_key,
                    state: "created".to_string(),
                    created: true,
                    local_winner: true,
                    claim: Some(claim),
                    diagnostic: None,
                    lock_wait_ms: 0,
                },
                Err(error) => RecoveryClaimCreateResultWire {
                    schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                    claim_key,
                    state: "refused".to_string(),
                    created: false,
                    local_winner: false,
                    claim: None,
                    diagnostic: Some(error.message),
                    lock_wait_ms: 0,
                },
            }
        }
    }
}

fn claim_matches_request(
    claim: &RecoveryClaimWire,
    request: &RecoveryClaimCreateRequestWire,
) -> bool {
    claim.project_id == request.project_id
        && claim.epic_id == request.epic_id
        && claim.close_token == request.close_token
        && claim.owner_machine == request.owner_machine
        && claim.proc_id == request.proc_id
        && claim.request_fingerprint == request.request_fingerprint
}

fn refused_claim_inspect(
    lookup: &RecoveryClaimLookupWire,
    diagnostic: String,
) -> RecoveryClaimInspectWire {
    let claim_key = if lookup.project_id.trim().is_empty()
        || lookup.epic_id.trim().is_empty()
        || lookup.close_token.trim().is_empty()
    {
        String::new()
    } else {
        recovery_claim_key(
            &lookup.project_id,
            &lookup.epic_id,
            &lookup.close_token,
        )
    };
    RecoveryClaimInspectWire {
        schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
        claim_key,
        project_id: lookup.project_id.clone(),
        epic_id: lookup.epic_id.clone(),
        close_token: lookup.close_token.clone(),
        state: "refused".to_string(),
        claim: None,
        diagnostic: Some(diagnostic),
    }
}

fn refused_claim_create(
    request: &RecoveryClaimCreateRequestWire,
    diagnostic: String,
) -> RecoveryClaimCreateResultWire {
    let claim_key = if request.project_id.trim().is_empty()
        || request.epic_id.trim().is_empty()
        || request.close_token.trim().is_empty()
    {
        String::new()
    } else {
        recovery_claim_key(
            &request.project_id,
            &request.epic_id,
            &request.close_token,
        )
    };
    RecoveryClaimCreateResultWire {
        schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
        claim_key,
        state: "refused".to_string(),
        created: false,
        local_winner: false,
        claim: None,
        diagnostic: Some(diagnostic),
        lock_wait_ms: 0,
    }
}

fn read_claim_inspects(
    beads_dir: &Path,
    project_id: &str,
) -> Result<Vec<RecoveryClaimInspectWire>, BeadError> {
    let dir = claims_dir(beads_dir);
    let entries = match fs::read_dir(&dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(error) => return Err(error.into()),
        Ok(entries) => entries,
    };
    let mut claims = Vec::new();
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(claim_key) = name.strip_suffix(".json") else {
            continue;
        };
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) => {
                claims.push(RecoveryClaimInspectWire {
                    schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                    claim_key: claim_key.to_string(),
                    project_id: project_id.to_string(),
                    epic_id: String::new(),
                    close_token: String::new(),
                    state: "malformed".to_string(),
                    claim: None,
                    diagnostic: Some(format!(
                        "unreadable recovery claim: {error}"
                    )),
                });
                continue;
            }
        };
        match parse_claim_bytes(&bytes, project_id) {
            Ok(claim) => {
                let expected_key = recovery_claim_key(
                    &claim.project_id,
                    &claim.epic_id,
                    &claim.close_token,
                );
                if expected_key != claim_key {
                    claims.push(RecoveryClaimInspectWire {
                        schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                        claim_key: claim_key.to_string(),
                        project_id: project_id.to_string(),
                        epic_id: claim.epic_id,
                        close_token: claim.close_token,
                        state: "malformed".to_string(),
                        claim: None,
                        diagnostic: Some(
                            "recovery claim key does not match stored identity"
                                .to_string(),
                        ),
                    });
                    continue;
                }
                claims.push(RecoveryClaimInspectWire {
                    schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                    claim_key: claim_key.to_string(),
                    project_id: claim.project_id.clone(),
                    epic_id: claim.epic_id.clone(),
                    close_token: claim.close_token.clone(),
                    state: "present".to_string(),
                    claim: Some(claim),
                    diagnostic: None,
                });
            }
            Err(diagnostic) => claims.push(RecoveryClaimInspectWire {
                schema_version: RECOVERY_WIRE_SCHEMA_VERSION,
                claim_key: claim_key.to_string(),
                project_id: project_id.to_string(),
                epic_id: String::new(),
                close_token: String::new(),
                state: "malformed".to_string(),
                claim: None,
                diagnostic: Some(diagnostic),
            }),
        }
    }
    Ok(claims)
}

#[cfg(test)]
mod tests;
