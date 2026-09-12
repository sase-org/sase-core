//! Machine-wide temporary LLM provider priority state and policy.
//!
//! Priority is independent from actual provider disables. It is persisted in
//! its own strict v1 record, while authoritative snapshots and priority writes
//! share the existing provider-disable lock so callers observe both stores at
//! one captured clock.

use crate::provider_disable::{
    provider_disable_lock_path, read_records_locked_with_repair,
    ProviderDisableError, ProviderDisableMode, ProviderDisableReadRepair,
    ProviderDisableWire, PROVIDER_DISABLE_WIRE_SCHEMA_VERSION,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use thiserror::Error;

pub const PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION: u32 = 1;
pub const PROVIDER_ROUTING_CONTEXT_WIRE_SCHEMA_VERSION: u32 = 1;
pub const PROVIDER_AVAILABILITY_WIRE_SCHEMA_VERSION: u32 = 1;
pub const PROVIDER_PRIORITY_STATE_FILENAME: &str = "llm_provider_priority.json";
const LOCK_TIMEOUT: Duration = Duration::from_millis(250);
const LOCK_RETRY_DELAY: Duration = Duration::from_millis(5);

/// Stable priority record returned to frontends and stored on disk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderPriorityWire {
    pub version: u32,
    pub provider: String,
    pub created_at: f64,
    pub expires_at: Option<f64>,
    pub source: String,
}

/// Frontend-supplied provider facts used to decide set eligibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderPriorityTargetFactsWire {
    pub provider: String,
    pub registered: bool,
    pub user_facing: bool,
    pub cli_available: bool,
}

/// Result status for priority set/change/clear attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderPriorityWriteStatus {
    Changed,
    Unchanged,
    IneligibleTarget,
    Conflict,
}

/// Typed outcome for priority state writes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderPriorityWriteOutcomeWire {
    pub version: u32,
    pub status: ProviderPriorityWriteStatus,
    pub record: Option<ProviderPriorityWire>,
    pub current: Option<ProviderPriorityWire>,
    pub reason: Option<String>,
}

/// Lock-free decode result for cached display data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderPriorityDecodeWire {
    pub version: u32,
    pub priority: Option<ProviderPriorityWire>,
    pub diagnostics: Vec<String>,
}

/// Immutable state captured once for a routing/display decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRoutingContextWire {
    pub version: u32,
    pub captured_at: f64,
    pub disables: Vec<ProviderDisableWire>,
    pub priority: Option<ProviderPriorityWire>,
    pub diagnostics: Vec<String>,
}

/// Frontend-supplied provider facts for pure availability classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAvailabilityFactsWire {
    pub provider: String,
    pub registered: bool,
    pub user_facing: bool,
    pub cli_available: bool,
}

/// Existing tri-state provider availability used by routing selectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderEffectiveAvailability {
    Preferred,
    Sparing,
    Unavailable,
}

/// Reasons that produced a provider's effective availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderAvailabilityProvenance {
    OrdinaryAvailable,
    Unregistered,
    NotUserFacing,
    CliMissing,
    ActualHardDisable,
    ActualSoftDisable,
    Priority,
    PriorityBackup,
}

/// Pure availability classification for one supplied provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAvailabilityWire {
    pub version: u32,
    pub provider: String,
    pub availability: ProviderEffectiveAvailability,
    pub provenance: Vec<ProviderAvailabilityProvenance>,
    pub actual_disable: Option<ProviderDisableWire>,
    pub priority: Option<ProviderPriorityWire>,
    pub eligible_for_priority: bool,
}

#[derive(Debug, Error)]
pub enum ProviderPriorityError {
    #[error("{0}")]
    Validation(String),
    #[error("timed out waiting for the provider-routing state lock")]
    LockTimeout,
    #[error("provider-priority state I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("provider-priority state serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<ProviderDisableError> for ProviderPriorityError {
    fn from(error: ProviderDisableError) -> Self {
        match error {
            ProviderDisableError::Validation(message) => {
                Self::Validation(message)
            }
            ProviderDisableError::LockTimeout => Self::LockTimeout,
            ProviderDisableError::Io(error) => Self::Io(error),
            ProviderDisableError::Json(error) => Self::Json(error),
        }
    }
}

pub fn provider_priority_state_path(sase_home: &Path) -> PathBuf {
    sase_home.join(PROVIDER_PRIORITY_STATE_FILENAME)
}

/// Read the active priority, pruning malformed or expired state.
pub fn get_provider_priority(
    sase_home: &Path,
    now: f64,
) -> Result<Option<ProviderPriorityWire>, ProviderPriorityError> {
    validate_now(now)?;
    with_provider_state_lock(sase_home, || {
        Ok(read_priority_record_locked(sase_home, now)?.priority)
    })
}

/// Read and decode priority state without taking the shared routing lock.
///
/// This is intended for cached display paths. It never prunes or rewrites the
/// priority store.
pub fn peek_provider_priority(
    sase_home: &Path,
    now: f64,
) -> Result<ProviderPriorityDecodeWire, ProviderPriorityError> {
    validate_now(now)?;
    let path = provider_priority_state_path(sase_home);
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return decode_provider_priority_bytes(None, now)
        }
        Err(error) => return Err(error.into()),
    };
    decode_provider_priority_bytes(Some(&bytes), now)
}

/// Decode supplied priority bytes without filesystem or locking side effects.
pub fn decode_provider_priority_bytes(
    data: Option<&[u8]>,
    now: f64,
) -> Result<ProviderPriorityDecodeWire, ProviderPriorityError> {
    validate_now(now)?;
    let Some(bytes) = data else {
        return Ok(priority_decode(None, vec![]));
    };
    let record = match serde_json::from_slice::<ProviderPriorityWire>(bytes) {
        Ok(record) => record,
        Err(error) => {
            return Ok(priority_decode(
                None,
                vec![format!(
                    "provider priority state is not a valid v1 record: {error}"
                )],
            ))
        }
    };
    if let Err(message) = validate_priority_record(&record) {
        return Ok(priority_decode(
            None,
            vec![format!("provider priority state is invalid: {message}")],
        ));
    }
    if priority_is_expired(&record, now) {
        return Ok(priority_decode(None, vec![]));
    }
    Ok(priority_decode(Some(record), vec![]))
}

/// Set or replace priority for a relative duration (`None` = until cleared).
pub fn set_provider_priority_relative(
    sase_home: &Path,
    provider: &str,
    duration_seconds: Option<f64>,
    source: &str,
    facts: &ProviderPriorityTargetFactsWire,
    expected_current: Option<&ProviderPriorityWire>,
    now: f64,
) -> Result<ProviderPriorityWriteOutcomeWire, ProviderPriorityError> {
    let provider = validate_provider(provider)?;
    let source = validate_source(source)?;
    validate_now(now)?;
    validate_target_facts(&provider, facts)?;
    validate_expected_current(expected_current)?;
    let expires_at = validate_relative_expires_at(duration_seconds, now)?;
    write_priority_record(
        sase_home,
        &provider,
        &source,
        facts,
        expected_current,
        now,
        expires_at,
    )
}

/// Set or replace priority until an exact future Unix timestamp.
pub fn set_provider_priority_until(
    sase_home: &Path,
    provider: &str,
    expires_at: f64,
    source: &str,
    facts: &ProviderPriorityTargetFactsWire,
    expected_current: Option<&ProviderPriorityWire>,
    now: f64,
) -> Result<ProviderPriorityWriteOutcomeWire, ProviderPriorityError> {
    let provider = validate_provider(provider)?;
    let source = validate_source(source)?;
    validate_now(now)?;
    validate_target_facts(&provider, facts)?;
    validate_expected_current(expected_current)?;
    let expires_at = validate_until_expires_at(expires_at, now)?;
    write_priority_record(
        sase_home,
        &provider,
        &source,
        facts,
        expected_current,
        now,
        Some(expires_at),
    )
}

/// Clear priority if the live record matches the expected current record.
pub fn clear_provider_priority(
    sase_home: &Path,
    expected_current: Option<&ProviderPriorityWire>,
    now: f64,
) -> Result<ProviderPriorityWriteOutcomeWire, ProviderPriorityError> {
    validate_now(now)?;
    validate_expected_current(expected_current)?;
    with_provider_state_lock(sase_home, || {
        let current = read_priority_record_locked(sase_home, now)?.priority;
        if current.is_none() {
            return Ok(priority_write_outcome(
                ProviderPriorityWriteStatus::Unchanged,
                None,
                None,
                None,
            ));
        }
        if !expected_matches(current.as_ref(), expected_current) {
            return Ok(priority_write_outcome(
                ProviderPriorityWriteStatus::Conflict,
                None,
                current,
                Some("live priority did not match expected state".to_string()),
            ));
        }
        remove_state(&provider_priority_state_path(sase_home))?;
        Ok(priority_write_outcome(
            ProviderPriorityWriteStatus::Changed,
            None,
            None,
            None,
        ))
    })
}

/// Capture actual disables and priority under one routing-state lock.
pub fn get_provider_routing_context(
    sase_home: &Path,
    now: f64,
) -> Result<ProviderRoutingContextWire, ProviderPriorityError> {
    validate_now(now)?;
    with_provider_state_lock(sase_home, || {
        let disables = read_records_locked_with_repair(
            sase_home,
            Some(now),
            ProviderDisableReadRepair::Repair,
        )?;
        let priority = read_priority_record_locked(sase_home, now)?;
        Ok(ProviderRoutingContextWire {
            version: PROVIDER_ROUTING_CONTEXT_WIRE_SCHEMA_VERSION,
            captured_at: now,
            disables: disables.into_values().collect(),
            priority: priority.priority,
            diagnostics: priority.diagnostics,
        })
    })
}

/// Build a routing context from already-decoded parts without filesystem I/O.
pub fn provider_routing_context_from_parts(
    disables: Vec<ProviderDisableWire>,
    priority: Option<ProviderPriorityWire>,
    captured_at: f64,
) -> Result<ProviderRoutingContextWire, ProviderPriorityError> {
    validate_now(captured_at)?;
    let mut seen = BTreeSet::new();
    let mut active_disables = BTreeMap::new();
    for record in disables {
        validate_provider_disable_record(&record)?;
        if !seen.insert(record.provider.clone()) {
            return Err(ProviderPriorityError::Validation(format!(
                "duplicate provider disable for {:?}",
                record.provider
            )));
        }
        if record
            .expires_at
            .is_some_and(|expires_at| captured_at >= expires_at)
        {
            continue;
        }
        active_disables.insert(record.provider.clone(), record);
    }
    let priority = match priority {
        Some(record) => {
            validate_priority_record(&record)
                .map_err(ProviderPriorityError::Validation)?;
            if priority_is_expired(&record, captured_at) {
                None
            } else {
                Some(record)
            }
        }
        None => None,
    };
    Ok(ProviderRoutingContextWire {
        version: PROVIDER_ROUTING_CONTEXT_WIRE_SCHEMA_VERSION,
        captured_at,
        disables: active_disables.into_values().collect(),
        priority,
        diagnostics: vec![],
    })
}

/// Classify one provider with no filesystem, lock, or cursor side effects.
pub fn classify_provider_availability(
    context: &ProviderRoutingContextWire,
    facts: &ProviderAvailabilityFactsWire,
) -> Result<ProviderAvailabilityWire, ProviderPriorityError> {
    let context = normalized_routing_context(context)?;
    validate_availability_facts(facts)?;
    let actual_disable = context
        .disables
        .iter()
        .find(|record| record.provider == facts.provider)
        .cloned();
    let priority = context.priority.clone();
    let is_priority = priority
        .as_ref()
        .is_some_and(|record| record.provider == facts.provider);
    let mut provenance = vec![];

    if !facts.registered {
        provenance.push(ProviderAvailabilityProvenance::Unregistered);
        if is_priority {
            provenance.push(ProviderAvailabilityProvenance::Priority);
        }
        return Ok(provider_availability(
            facts,
            ProviderEffectiveAvailability::Unavailable,
            provenance,
            actual_disable,
            priority,
        ));
    }
    if !facts.user_facing {
        provenance.push(ProviderAvailabilityProvenance::NotUserFacing);
        if is_priority {
            provenance.push(ProviderAvailabilityProvenance::Priority);
        }
        return Ok(provider_availability(
            facts,
            ProviderEffectiveAvailability::Unavailable,
            provenance,
            actual_disable,
            priority,
        ));
    }
    if !facts.cli_available {
        provenance.push(ProviderAvailabilityProvenance::CliMissing);
        if is_priority {
            provenance.push(ProviderAvailabilityProvenance::Priority);
        }
        return Ok(provider_availability(
            facts,
            ProviderEffectiveAvailability::Unavailable,
            provenance,
            actual_disable,
            priority,
        ));
    }

    match actual_disable.as_ref().map(|record| record.mode) {
        Some(ProviderDisableMode::Hard) => {
            provenance.push(ProviderAvailabilityProvenance::ActualHardDisable);
            if is_priority {
                provenance.push(ProviderAvailabilityProvenance::Priority);
            }
            Ok(provider_availability(
                facts,
                ProviderEffectiveAvailability::Unavailable,
                provenance,
                actual_disable,
                priority,
            ))
        }
        Some(ProviderDisableMode::Soft) => {
            provenance.push(ProviderAvailabilityProvenance::ActualSoftDisable);
            if is_priority {
                provenance.push(ProviderAvailabilityProvenance::Priority);
            } else if priority.is_some() {
                provenance.push(ProviderAvailabilityProvenance::PriorityBackup);
            }
            Ok(provider_availability(
                facts,
                ProviderEffectiveAvailability::Sparing,
                provenance,
                actual_disable,
                priority,
            ))
        }
        None if is_priority => Ok(provider_availability(
            facts,
            ProviderEffectiveAvailability::Preferred,
            vec![ProviderAvailabilityProvenance::Priority],
            actual_disable,
            priority,
        )),
        None if priority.is_some() => Ok(provider_availability(
            facts,
            ProviderEffectiveAvailability::Sparing,
            vec![ProviderAvailabilityProvenance::PriorityBackup],
            actual_disable,
            priority,
        )),
        None => Ok(provider_availability(
            facts,
            ProviderEffectiveAvailability::Preferred,
            vec![ProviderAvailabilityProvenance::OrdinaryAvailable],
            actual_disable,
            priority,
        )),
    }
}

/// Classify many providers from the same immutable context.
pub fn classify_provider_availability_many(
    context: &ProviderRoutingContextWire,
    facts: &[ProviderAvailabilityFactsWire],
) -> Result<Vec<ProviderAvailabilityWire>, ProviderPriorityError> {
    facts
        .iter()
        .map(|item| classify_provider_availability(context, item))
        .collect()
}

/// Return the primary-pool admission mask from classified members.
///
/// `records` are the primary pool members only. Last-resort tail members
/// must be omitted so a healthy tail cannot spare or invalidate a primary
/// member. Usable members without an actual soft disable outrank actually
/// soft-disabled members. Priority then applies only within that preferred
/// subset: a preferred member spares ordinary backups, and an all-backup
/// subset rotates normally. Actually soft-disabled members are admitted
/// only when the pool has no usable non-soft member.
pub fn pool_eligibility_mask(
    records: &[ProviderAvailabilityWire],
) -> Result<Vec<bool>, ProviderPriorityError> {
    validate_pool_records(records)?;
    let any_usable_non_soft = records.iter().any(member_is_usable_non_soft);
    if any_usable_non_soft {
        let any_preferred_non_soft = records.iter().any(|record| {
            member_is_usable_non_soft(record)
                && record.availability
                    == ProviderEffectiveAvailability::Preferred
        });
        return Ok(records
            .iter()
            .map(|record| {
                if !member_is_usable_non_soft(record) {
                    false
                } else if any_preferred_non_soft {
                    record.availability
                        == ProviderEffectiveAvailability::Preferred
                } else {
                    true
                }
            })
            .collect());
    }
    Ok(records.iter().map(member_is_usable).collect())
}

/// Return whether the reserved primary member is still eligible.
///
/// Fresh priority masking must not itself invalidate an already-reserved
/// ordinary backup. An unavailable reserved member is rejected. An actually
/// soft-disabled reservation is rejected only when a usable non-soft
/// primary member exists. `reserved_index` is into `records`, which must
/// be the primary pool members only.
pub fn pool_reservation_eligible(
    records: &[ProviderAvailabilityWire],
    reserved_index: usize,
) -> Result<bool, ProviderPriorityError> {
    validate_pool_records(records)?;
    if reserved_index >= records.len() {
        return Err(ProviderPriorityError::Validation(format!(
            "reserved member index {reserved_index} is out of range \
             for {} pool members",
            records.len()
        )));
    }
    let reserved = &records[reserved_index];
    if !member_is_usable(reserved) {
        return Ok(false);
    }
    if actually_soft_disabled(reserved)
        && records.iter().any(member_is_usable_non_soft)
    {
        return Ok(false);
    }
    Ok(true)
}

fn write_priority_record(
    sase_home: &Path,
    provider: &str,
    source: &str,
    facts: &ProviderPriorityTargetFactsWire,
    expected_current: Option<&ProviderPriorityWire>,
    now: f64,
    expires_at: Option<f64>,
) -> Result<ProviderPriorityWriteOutcomeWire, ProviderPriorityError> {
    let candidate = ProviderPriorityWire {
        version: PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION,
        provider: provider.to_string(),
        created_at: now,
        expires_at,
        source: source.to_string(),
    };
    with_provider_state_lock(sase_home, || {
        let current = read_priority_record_locked(sase_home, now)?.priority;
        if !expected_matches(current.as_ref(), expected_current) {
            return Ok(priority_write_outcome(
                ProviderPriorityWriteStatus::Conflict,
                None,
                current,
                Some("live priority did not match expected state".to_string()),
            ));
        }
        let disables = read_records_locked_with_repair(
            sase_home,
            Some(now),
            ProviderDisableReadRepair::ReadOnly,
        )?;
        if let Some(reason) =
            priority_ineligibility_reason(facts, disables.get(provider))
        {
            return Ok(priority_write_outcome(
                ProviderPriorityWriteStatus::IneligibleTarget,
                None,
                current,
                Some(reason),
            ));
        }
        if current.as_ref() == Some(&candidate) {
            return Ok(priority_write_outcome(
                ProviderPriorityWriteStatus::Unchanged,
                Some(candidate),
                current,
                None,
            ));
        }
        write_record_atomic(
            &provider_priority_state_path(sase_home),
            &candidate,
        )?;
        Ok(priority_write_outcome(
            ProviderPriorityWriteStatus::Changed,
            Some(candidate.clone()),
            Some(candidate),
            None,
        ))
    })
}

fn priority_ineligibility_reason(
    facts: &ProviderPriorityTargetFactsWire,
    actual_disable: Option<&ProviderDisableWire>,
) -> Option<String> {
    if !facts.registered {
        return Some("provider is not registered".to_string());
    }
    if !facts.user_facing {
        return Some("provider is not user-facing".to_string());
    }
    if !facts.cli_available {
        return Some("provider CLI is unavailable".to_string());
    }
    match actual_disable.map(|record| record.mode) {
        Some(ProviderDisableMode::Hard) => {
            Some("provider is hard-disabled".to_string())
        }
        Some(ProviderDisableMode::Soft) => {
            Some("provider is soft-disabled".to_string())
        }
        None => None,
    }
}

fn read_priority_record_locked(
    sase_home: &Path,
    now: f64,
) -> Result<ProviderPriorityDecodeWire, ProviderPriorityError> {
    let path = provider_priority_state_path(sase_home);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(priority_decode(None, vec![]))
        }
        Err(error) => return Err(error.into()),
    };
    let decode = decode_provider_priority_bytes(Some(&bytes), now)?;
    if decode.priority.is_none() {
        remove_state(&path)?;
    }
    Ok(decode)
}

fn normalized_routing_context(
    context: &ProviderRoutingContextWire,
) -> Result<ProviderRoutingContextWire, ProviderPriorityError> {
    if context.version != PROVIDER_ROUTING_CONTEXT_WIRE_SCHEMA_VERSION {
        return Err(ProviderPriorityError::Validation(format!(
            "routing context version must be {}, got {}",
            PROVIDER_ROUTING_CONTEXT_WIRE_SCHEMA_VERSION, context.version
        )));
    }
    provider_routing_context_from_parts(
        context.disables.clone(),
        context.priority.clone(),
        context.captured_at,
    )
}

fn validate_expected_current(
    expected_current: Option<&ProviderPriorityWire>,
) -> Result<(), ProviderPriorityError> {
    if let Some(record) = expected_current {
        validate_priority_record(record)
            .map_err(ProviderPriorityError::Validation)?;
    }
    Ok(())
}

fn expected_matches(
    current: Option<&ProviderPriorityWire>,
    expected: Option<&ProviderPriorityWire>,
) -> bool {
    match (current, expected) {
        (None, None) => true,
        (Some(current), Some(expected)) => current == expected,
        _ => false,
    }
}

fn priority_decode(
    priority: Option<ProviderPriorityWire>,
    diagnostics: Vec<String>,
) -> ProviderPriorityDecodeWire {
    ProviderPriorityDecodeWire {
        version: PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION,
        priority,
        diagnostics,
    }
}

fn priority_write_outcome(
    status: ProviderPriorityWriteStatus,
    record: Option<ProviderPriorityWire>,
    current: Option<ProviderPriorityWire>,
    reason: Option<String>,
) -> ProviderPriorityWriteOutcomeWire {
    ProviderPriorityWriteOutcomeWire {
        version: PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION,
        status,
        record,
        current,
        reason,
    }
}

fn validate_pool_records(
    records: &[ProviderAvailabilityWire],
) -> Result<(), ProviderPriorityError> {
    if records.is_empty() {
        return Err(ProviderPriorityError::Validation(
            "pool eligibility records must be non-empty".to_string(),
        ));
    }
    for record in records {
        validate_availability_record(record)?;
    }
    Ok(())
}

fn validate_availability_record(
    record: &ProviderAvailabilityWire,
) -> Result<(), ProviderPriorityError> {
    if record.version != PROVIDER_AVAILABILITY_WIRE_SCHEMA_VERSION {
        return Err(ProviderPriorityError::Validation(format!(
            "provider availability version must be {}, got {}",
            PROVIDER_AVAILABILITY_WIRE_SCHEMA_VERSION, record.version
        )));
    }
    validate_provider(&record.provider)?;
    if record.provenance.is_empty() {
        return Err(ProviderPriorityError::Validation(
            "provider availability provenance must be non-empty".to_string(),
        ));
    }
    if let Some(disable) = record.actual_disable.as_ref() {
        validate_provider_disable_record(disable)?;
        if disable.provider != record.provider {
            return Err(ProviderPriorityError::Validation(format!(
                "actual disable provider {:?} does not match {:?}",
                disable.provider, record.provider
            )));
        }
    }
    if let Some(priority) = record.priority.as_ref() {
        validate_priority_record(priority)
            .map_err(ProviderPriorityError::Validation)?;
    }
    Ok(())
}

fn actually_soft_disabled(record: &ProviderAvailabilityWire) -> bool {
    record
        .actual_disable
        .as_ref()
        .is_some_and(|disable| disable.mode == ProviderDisableMode::Soft)
}

fn member_is_usable(record: &ProviderAvailabilityWire) -> bool {
    record.availability != ProviderEffectiveAvailability::Unavailable
}

fn member_is_usable_non_soft(record: &ProviderAvailabilityWire) -> bool {
    member_is_usable(record) && !actually_soft_disabled(record)
}

fn provider_availability(
    facts: &ProviderAvailabilityFactsWire,
    availability: ProviderEffectiveAvailability,
    provenance: Vec<ProviderAvailabilityProvenance>,
    actual_disable: Option<ProviderDisableWire>,
    priority: Option<ProviderPriorityWire>,
) -> ProviderAvailabilityWire {
    let eligible_for_priority = facts.registered
        && facts.user_facing
        && facts.cli_available
        && actual_disable.is_none();
    ProviderAvailabilityWire {
        version: PROVIDER_AVAILABILITY_WIRE_SCHEMA_VERSION,
        provider: facts.provider.clone(),
        availability,
        provenance,
        actual_disable,
        priority,
        eligible_for_priority,
    }
}

fn validate_target_facts(
    provider: &str,
    facts: &ProviderPriorityTargetFactsWire,
) -> Result<(), ProviderPriorityError> {
    validate_provider(&facts.provider)?;
    if facts.provider != provider {
        return Err(ProviderPriorityError::Validation(format!(
            "target facts provider {:?} does not match requested provider {:?}",
            facts.provider, provider
        )));
    }
    Ok(())
}

fn validate_availability_facts(
    facts: &ProviderAvailabilityFactsWire,
) -> Result<(), ProviderPriorityError> {
    validate_provider(&facts.provider)?;
    Ok(())
}

fn validate_provider_disable_record(
    record: &ProviderDisableWire,
) -> Result<(), ProviderPriorityError> {
    if record.version != PROVIDER_DISABLE_WIRE_SCHEMA_VERSION {
        return Err(ProviderPriorityError::Validation(format!(
            "provider disable record version must be {}, got {}",
            PROVIDER_DISABLE_WIRE_SCHEMA_VERSION, record.version
        )));
    }
    validate_provider(&record.provider)?;
    validate_source(&record.source)?;
    if !record.created_at.is_finite() || record.created_at <= 0.0 {
        return Err(ProviderPriorityError::Validation(
            "provider disable created_at must be finite and positive"
                .to_string(),
        ));
    }
    if record.expires_at.is_some_and(|expires_at| {
        !expires_at.is_finite() || expires_at <= record.created_at
    }) {
        return Err(ProviderPriorityError::Validation(
            "provider disable expires_at must be finite and after created_at"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_provider(provider: &str) -> Result<String, ProviderPriorityError> {
    let cleaned = provider.trim();
    if cleaned.is_empty() {
        return Err(ProviderPriorityError::Validation(
            "provider must be non-empty".to_string(),
        ));
    }
    if provider != cleaned {
        return Err(ProviderPriorityError::Validation(
            "provider must not contain leading or trailing whitespace"
                .to_string(),
        ));
    }
    if cleaned.chars().any(char::is_control) {
        return Err(ProviderPriorityError::Validation(
            "provider must not contain control characters".to_string(),
        ));
    }
    Ok(cleaned.to_string())
}

fn validate_source(source: &str) -> Result<String, ProviderPriorityError> {
    let cleaned = source.trim();
    if cleaned.is_empty() {
        return Err(ProviderPriorityError::Validation(
            "source must be non-empty".to_string(),
        ));
    }
    if cleaned.chars().any(char::is_control) {
        return Err(ProviderPriorityError::Validation(
            "source must not contain control characters".to_string(),
        ));
    }
    Ok(cleaned.to_string())
}

fn validate_now(now: f64) -> Result<(), ProviderPriorityError> {
    if !now.is_finite() || now <= 0.0 {
        return Err(ProviderPriorityError::Validation(
            "current timestamp must be finite and positive".to_string(),
        ));
    }
    Ok(())
}

fn validate_relative_expires_at(
    duration_seconds: Option<f64>,
    now: f64,
) -> Result<Option<f64>, ProviderPriorityError> {
    if let Some(duration) = duration_seconds {
        if !duration.is_finite() || duration <= 0.0 {
            return Err(ProviderPriorityError::Validation(
                "duration_seconds must be finite and positive or None"
                    .to_string(),
            ));
        }
    }
    let expires_at = duration_seconds.map(|duration| now + duration);
    if expires_at.is_some_and(|expiry| !expiry.is_finite()) {
        return Err(ProviderPriorityError::Validation(
            "computed expires_at must be finite".to_string(),
        ));
    }
    Ok(expires_at)
}

fn validate_until_expires_at(
    expires_at: f64,
    now: f64,
) -> Result<f64, ProviderPriorityError> {
    if !expires_at.is_finite() {
        return Err(ProviderPriorityError::Validation(
            "expires_at must be finite".to_string(),
        ));
    }
    if expires_at <= now {
        return Err(ProviderPriorityError::Validation(
            "expires_at must be in the future".to_string(),
        ));
    }
    Ok(expires_at)
}

fn validate_priority_record(
    record: &ProviderPriorityWire,
) -> Result<(), String> {
    if record.version != PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "version must be {}, got {}",
            PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION, record.version
        ));
    }
    validate_provider(&record.provider).map_err(|error| error.to_string())?;
    validate_source(&record.source).map_err(|error| error.to_string())?;
    if !record.created_at.is_finite() || record.created_at <= 0.0 {
        return Err("created_at must be finite and positive".to_string());
    }
    if record.expires_at.is_some_and(|expires_at| {
        !expires_at.is_finite() || expires_at <= record.created_at
    }) {
        return Err(
            "expires_at must be finite and after created_at".to_string()
        );
    }
    Ok(())
}

fn priority_is_expired(record: &ProviderPriorityWire, now: f64) -> bool {
    record
        .expires_at
        .is_some_and(|expires_at| now >= expires_at)
}

fn write_record_atomic(
    path: &Path,
    record: &ProviderPriorityWire,
) -> Result<(), ProviderPriorityError> {
    let parent = path.parent().ok_or_else(|| {
        ProviderPriorityError::Validation(
            "provider-priority path has no parent directory".to_string(),
        )
    })?;
    fs::create_dir_all(parent)?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, record)?;
    temporary.write_all(b"\n")?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn remove_state(path: &Path) -> Result<(), ProviderPriorityError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn with_provider_state_lock<T>(
    sase_home: &Path,
    operation: impl FnOnce() -> Result<T, ProviderPriorityError>,
) -> Result<T, ProviderPriorityError> {
    fs::create_dir_all(sase_home)?;
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(provider_disable_lock_path(sase_home))?;
    let started = Instant::now();
    loop {
        match FileExt::try_lock_exclusive(&lock) {
            Ok(()) => break,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if started.elapsed() >= LOCK_TIMEOUT {
                    return Err(ProviderPriorityError::LockTimeout);
                }
                thread::sleep(LOCK_RETRY_DELAY);
            }
            Err(error) => return Err(error.into()),
        }
    }
    let result = operation();
    FileExt::unlock(&lock)?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider_disable::{
        provider_disable_state_path, set_provider_disable_relative,
    };
    use serde_json::json;
    use std::fs::File;
    use std::sync::{Arc, Barrier};
    use tempfile::tempdir;

    const NOW: f64 = 1_800_000_000.0;

    fn eligible(provider: &str) -> ProviderPriorityTargetFactsWire {
        ProviderPriorityTargetFactsWire {
            provider: provider.to_string(),
            registered: true,
            user_facing: true,
            cli_available: true,
        }
    }

    fn availability_facts(provider: &str) -> ProviderAvailabilityFactsWire {
        ProviderAvailabilityFactsWire {
            provider: provider.to_string(),
            registered: true,
            user_facing: true,
            cli_available: true,
        }
    }

    #[test]
    fn relative_exact_and_indefinite_windows_round_trip() {
        let temp = tempdir().unwrap();
        let facts = eligible("codex");
        let relative = set_provider_priority_relative(
            temp.path(),
            "codex",
            Some(900.0),
            "ace",
            &facts,
            None,
            NOW,
        )
        .unwrap();
        assert_eq!(relative.status, ProviderPriorityWriteStatus::Changed);
        assert_eq!(
            relative.record.as_ref().unwrap().expires_at,
            Some(NOW + 900.0)
        );
        assert_eq!(
            get_provider_priority(temp.path(), NOW).unwrap(),
            relative.record
        );

        let exact = set_provider_priority_until(
            temp.path(),
            "codex",
            NOW + 60.0,
            "ace",
            &facts,
            relative.current.as_ref(),
            NOW,
        )
        .unwrap();
        assert_eq!(exact.status, ProviderPriorityWriteStatus::Changed);
        assert_eq!(exact.record.as_ref().unwrap().expires_at, Some(NOW + 60.0));
        assert_eq!(
            get_provider_priority(temp.path(), NOW + 59.999).unwrap(),
            exact.record
        );
        assert_eq!(
            get_provider_priority(temp.path(), NOW + 60.0).unwrap(),
            None
        );
        assert!(!provider_priority_state_path(temp.path()).exists());

        let indefinite = set_provider_priority_relative(
            temp.path(),
            "codex",
            None,
            "ace",
            &facts,
            None,
            NOW,
        )
        .unwrap();
        assert_eq!(indefinite.record.as_ref().unwrap().expires_at, None);
        assert_eq!(
            get_provider_priority(temp.path(), NOW + 1_000_000.0).unwrap(),
            indefinite.record
        );
    }

    #[test]
    fn replacement_and_clear_require_complete_expected_record() {
        let temp = tempdir().unwrap();
        let codex_facts = eligible("codex");
        let claude_facts = eligible("claude");
        let first = set_provider_priority_relative(
            temp.path(),
            "codex",
            Some(900.0),
            "ace",
            &codex_facts,
            None,
            NOW,
        )
        .unwrap();
        let conflict = set_provider_priority_until(
            temp.path(),
            "claude",
            NOW + 60.0,
            "ace",
            &claude_facts,
            None,
            NOW,
        )
        .unwrap();
        assert_eq!(conflict.status, ProviderPriorityWriteStatus::Conflict);
        assert_eq!(conflict.current, first.current);

        let replacement = set_provider_priority_until(
            temp.path(),
            "claude",
            NOW + 60.0,
            "ace",
            &claude_facts,
            first.current.as_ref(),
            NOW,
        )
        .unwrap();
        assert_eq!(replacement.status, ProviderPriorityWriteStatus::Changed);
        assert_eq!(replacement.record.as_ref().unwrap().provider, "claude");

        let stale_clear =
            clear_provider_priority(temp.path(), first.current.as_ref(), NOW)
                .unwrap();
        assert_eq!(stale_clear.status, ProviderPriorityWriteStatus::Conflict);
        assert_eq!(stale_clear.current, replacement.current);

        let clear = clear_provider_priority(
            temp.path(),
            replacement.current.as_ref(),
            NOW,
        )
        .unwrap();
        assert_eq!(clear.status, ProviderPriorityWriteStatus::Changed);
        assert_eq!(
            clear_provider_priority(
                temp.path(),
                replacement.current.as_ref(),
                NOW
            )
            .unwrap()
            .status,
            ProviderPriorityWriteStatus::Unchanged
        );
    }

    #[test]
    fn ineligible_target_preserves_existing_priority_and_disable_file() {
        let temp = tempdir().unwrap();
        let codex_facts = eligible("codex");
        let existing = set_provider_priority_relative(
            temp.path(),
            "codex",
            None,
            "ace",
            &codex_facts,
            None,
            NOW,
        )
        .unwrap();
        let disabled = set_provider_disable_relative(
            temp.path(),
            "claude",
            Some(300.0),
            "usage_limit",
            NOW,
            ProviderDisableMode::Soft,
        )
        .unwrap();
        let disable_path = provider_disable_state_path(temp.path());
        let disable_bytes = fs::read(&disable_path).unwrap();
        let result = set_provider_priority_relative(
            temp.path(),
            "claude",
            Some(60.0),
            "ace",
            &eligible("claude"),
            existing.current.as_ref(),
            NOW,
        )
        .unwrap();
        assert_eq!(
            result.status,
            ProviderPriorityWriteStatus::IneligibleTarget
        );
        assert_eq!(result.current, existing.current);
        assert_eq!(fs::read(&disable_path).unwrap(), disable_bytes);
        assert_eq!(disabled.mode, ProviderDisableMode::Soft);

        for facts in [
            ProviderPriorityTargetFactsWire {
                provider: "grok".to_string(),
                registered: false,
                user_facing: true,
                cli_available: true,
            },
            ProviderPriorityTargetFactsWire {
                provider: "grok".to_string(),
                registered: true,
                user_facing: false,
                cli_available: true,
            },
            ProviderPriorityTargetFactsWire {
                provider: "grok".to_string(),
                registered: true,
                user_facing: true,
                cli_available: false,
            },
        ] {
            let result = set_provider_priority_relative(
                temp.path(),
                "grok",
                Some(60.0),
                "ace",
                &facts,
                existing.current.as_ref(),
                NOW,
            )
            .unwrap();
            assert_eq!(
                result.status,
                ProviderPriorityWriteStatus::IneligibleTarget
            );
            assert_eq!(result.current, existing.current);
        }
    }

    #[test]
    fn invalid_values_are_rejected_without_writing() {
        let temp = tempdir().unwrap();
        let facts = eligible("codex");
        for provider in ["", " codex", "codex\n"] {
            assert!(set_provider_priority_relative(
                temp.path(),
                provider,
                Some(1.0),
                "ace",
                &facts,
                None,
                NOW,
            )
            .is_err());
        }
        for duration in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(set_provider_priority_relative(
                temp.path(),
                "codex",
                Some(duration),
                "ace",
                &facts,
                None,
                NOW,
            )
            .is_err());
        }
        for expiry in [NOW, NOW - 1.0, f64::NAN, f64::INFINITY] {
            assert!(set_provider_priority_until(
                temp.path(),
                "codex",
                expiry,
                "ace",
                &facts,
                None,
                NOW,
            )
            .is_err());
        }
        for now in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(get_provider_priority(temp.path(), now).is_err());
        }
        assert!(!provider_priority_state_path(temp.path()).exists());
    }

    #[test]
    fn malformed_priority_degrades_with_diagnostic_and_valid_disables_survive()
    {
        let temp = tempdir().unwrap();
        let disable = set_provider_disable_relative(
            temp.path(),
            "claude",
            None,
            "usage_limit",
            NOW,
            ProviderDisableMode::Hard,
        )
        .unwrap();
        fs::write(provider_priority_state_path(temp.path()), "not json")
            .unwrap();

        let peek = peek_provider_priority(temp.path(), NOW).unwrap();
        assert!(peek.priority.is_none());
        assert_eq!(peek.diagnostics.len(), 1);
        assert!(provider_priority_state_path(temp.path()).exists());

        let context = get_provider_routing_context(temp.path(), NOW).unwrap();
        assert_eq!(context.disables, vec![disable]);
        assert!(context.priority.is_none());
        assert_eq!(context.diagnostics.len(), 1);
        assert!(!provider_priority_state_path(temp.path()).exists());
    }

    #[test]
    fn read_only_context_builder_filters_expired_state() {
        let priority = ProviderPriorityWire {
            version: PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION,
            provider: "codex".to_string(),
            created_at: NOW - 20.0,
            expires_at: Some(NOW - 1.0),
            source: "ace".to_string(),
        };
        let disable = ProviderDisableWire {
            version: PROVIDER_DISABLE_WIRE_SCHEMA_VERSION,
            provider: "claude".to_string(),
            created_at: NOW - 20.0,
            expires_at: Some(NOW - 1.0),
            source: "ace".to_string(),
            mode: ProviderDisableMode::Soft,
        };
        let context = provider_routing_context_from_parts(
            vec![disable],
            Some(priority),
            NOW,
        )
        .unwrap();
        assert!(context.priority.is_none());
        assert!(context.disables.is_empty());
    }

    #[test]
    fn classifier_normalizes_expired_supplied_context() {
        let context = ProviderRoutingContextWire {
            version: PROVIDER_ROUTING_CONTEXT_WIRE_SCHEMA_VERSION,
            captured_at: NOW,
            disables: vec![],
            priority: Some(ProviderPriorityWire {
                version: PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION,
                provider: "codex".to_string(),
                created_at: NOW - 20.0,
                expires_at: Some(NOW - 1.0),
                source: "ace".to_string(),
            }),
            diagnostics: vec![],
        };

        let codex = classify_provider_availability(
            &context,
            &availability_facts("codex"),
        )
        .unwrap();

        assert_eq!(
            codex.availability,
            ProviderEffectiveAvailability::Preferred
        );
        assert_eq!(
            codex.provenance,
            vec![ProviderAvailabilityProvenance::OrdinaryAvailable]
        );
    }

    #[test]
    fn precedence_table_classifies_priority_overlay() {
        let priority = ProviderPriorityWire {
            version: PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION,
            provider: "codex".to_string(),
            created_at: NOW,
            expires_at: Some(NOW + 60.0),
            source: "ace".to_string(),
        };
        let soft = ProviderDisableWire {
            version: PROVIDER_DISABLE_WIRE_SCHEMA_VERSION,
            provider: "grok".to_string(),
            created_at: NOW,
            expires_at: None,
            source: "usage_limit".to_string(),
            mode: ProviderDisableMode::Soft,
        };
        let hard_priority = ProviderDisableWire {
            version: PROVIDER_DISABLE_WIRE_SCHEMA_VERSION,
            provider: "codex-hard".to_string(),
            created_at: NOW,
            expires_at: None,
            source: "ace".to_string(),
            mode: ProviderDisableMode::Hard,
        };
        let context = provider_routing_context_from_parts(
            vec![soft.clone(), hard_priority.clone()],
            Some(priority.clone()),
            NOW,
        )
        .unwrap();

        let codex = classify_provider_availability(
            &context,
            &availability_facts("codex"),
        )
        .unwrap();
        assert_eq!(
            codex.availability,
            ProviderEffectiveAvailability::Preferred
        );
        assert_eq!(
            codex.provenance,
            vec![ProviderAvailabilityProvenance::Priority]
        );

        let claude = classify_provider_availability(
            &context,
            &availability_facts("claude"),
        )
        .unwrap();
        assert_eq!(claude.availability, ProviderEffectiveAvailability::Sparing);
        assert_eq!(
            claude.provenance,
            vec![ProviderAvailabilityProvenance::PriorityBackup]
        );

        let grok = classify_provider_availability(
            &context,
            &availability_facts("grok"),
        )
        .unwrap();
        assert_eq!(grok.availability, ProviderEffectiveAvailability::Sparing);
        assert_eq!(
            grok.provenance,
            vec![
                ProviderAvailabilityProvenance::ActualSoftDisable,
                ProviderAvailabilityProvenance::PriorityBackup
            ]
        );
        assert_eq!(grok.actual_disable, Some(soft));

        let mut missing_cli = availability_facts("codex");
        missing_cli.cli_available = false;
        let missing =
            classify_provider_availability(&context, &missing_cli).unwrap();
        assert_eq!(
            missing.availability,
            ProviderEffectiveAvailability::Unavailable
        );
        assert_eq!(
            missing.provenance,
            vec![
                ProviderAvailabilityProvenance::CliMissing,
                ProviderAvailabilityProvenance::Priority
            ]
        );

        let no_priority =
            provider_routing_context_from_parts(vec![], None, NOW).unwrap();
        let ordinary = classify_provider_availability(
            &no_priority,
            &availability_facts("claude"),
        )
        .unwrap();
        assert_eq!(
            ordinary.availability,
            ProviderEffectiveAvailability::Preferred
        );
        assert_eq!(
            ordinary.provenance,
            vec![ProviderAvailabilityProvenance::OrdinaryAvailable]
        );
    }

    #[test]
    fn priority_provider_soft_or_hard_disable_keeps_both_causes() {
        let priority = ProviderPriorityWire {
            version: PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION,
            provider: "codex".to_string(),
            created_at: NOW,
            expires_at: None,
            source: "ace".to_string(),
        };
        for (mode, availability, cause) in [
            (
                ProviderDisableMode::Soft,
                ProviderEffectiveAvailability::Sparing,
                ProviderAvailabilityProvenance::ActualSoftDisable,
            ),
            (
                ProviderDisableMode::Hard,
                ProviderEffectiveAvailability::Unavailable,
                ProviderAvailabilityProvenance::ActualHardDisable,
            ),
        ] {
            let disable = ProviderDisableWire {
                version: PROVIDER_DISABLE_WIRE_SCHEMA_VERSION,
                provider: "codex".to_string(),
                created_at: NOW,
                expires_at: None,
                source: "usage_limit".to_string(),
                mode,
            };
            let context = provider_routing_context_from_parts(
                vec![disable],
                Some(priority.clone()),
                NOW,
            )
            .unwrap();
            let result = classify_provider_availability(
                &context,
                &availability_facts("codex"),
            )
            .unwrap();
            assert_eq!(result.availability, availability);
            assert_eq!(
                result.provenance,
                vec![cause, ProviderAvailabilityProvenance::Priority]
            );
        }
    }

    #[test]
    fn lock_wait_is_bounded() {
        let temp = tempdir().unwrap();
        fs::create_dir_all(temp.path()).unwrap();
        let holder =
            File::create(provider_disable_lock_path(temp.path())).unwrap();
        FileExt::lock_exclusive(&holder).unwrap();
        let started = Instant::now();
        let result = get_provider_priority(temp.path(), NOW);
        assert!(matches!(result, Err(ProviderPriorityError::LockTimeout)));
        assert!(started.elapsed() < Duration::from_secs(2));
        FileExt::unlock(&holder).unwrap();
    }

    #[test]
    fn concurrent_priority_changes_and_auto_disables_are_serialized() {
        let temp = tempdir().unwrap();
        let home = Arc::new(temp.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(9));
        let mut handles = vec![];
        for index in 0..4 {
            let home = Arc::clone(&home);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                set_provider_priority_relative(
                    &home,
                    "codex",
                    Some(60.0 + f64::from(index)),
                    "ace",
                    &eligible("codex"),
                    None,
                    NOW + f64::from(index),
                )
                .unwrap()
            }));
        }
        for index in 0..4 {
            let home = Arc::clone(&home);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                set_provider_disable_relative(
                    &home,
                    &format!("provider-{index}"),
                    Some(300.0),
                    "usage_limit",
                    NOW,
                    ProviderDisableMode::Hard,
                )
                .unwrap();
                priority_write_outcome(
                    ProviderPriorityWriteStatus::Unchanged,
                    None,
                    None,
                    None,
                )
            }));
        }
        barrier.wait();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| {
                    outcome.status == ProviderPriorityWriteStatus::Changed
                })
                .count(),
            1
        );
        let context = get_provider_routing_context(temp.path(), NOW).unwrap();
        assert!(context.priority.is_some());
        assert_eq!(context.disables.len(), 4);
    }

    #[test]
    fn io_failures_are_reported() {
        let temp = tempdir().unwrap();
        let bad_home = temp.path().join("file-home");
        fs::write(&bad_home, b"not a directory").unwrap();
        let error = get_provider_priority(&bad_home, NOW).unwrap_err();
        assert!(matches!(error, ProviderPriorityError::Io(_)));

        let decode = decode_provider_priority_bytes(
            Some(
                serde_json::to_string(&json!({
                    "version": 99,
                    "provider": "codex",
                    "created_at": NOW,
                    "expires_at": null,
                    "source": "ace",
                }))
                .unwrap()
                .as_bytes(),
            ),
            NOW,
        )
        .unwrap();
        assert!(decode.priority.is_none());
        assert_eq!(decode.diagnostics.len(), 1);
    }

    fn classified(
        context: &ProviderRoutingContextWire,
        provider: &str,
    ) -> ProviderAvailabilityWire {
        classify_provider_availability(context, &availability_facts(provider))
            .unwrap()
    }

    fn classified_cli(
        context: &ProviderRoutingContextWire,
        provider: &str,
        cli_available: bool,
    ) -> ProviderAvailabilityWire {
        let mut facts = availability_facts(provider);
        facts.cli_available = cli_available;
        classify_provider_availability(context, &facts).unwrap()
    }

    fn soft_disable(provider: &str) -> ProviderDisableWire {
        ProviderDisableWire {
            version: PROVIDER_DISABLE_WIRE_SCHEMA_VERSION,
            provider: provider.to_string(),
            created_at: NOW,
            expires_at: None,
            source: "ace".to_string(),
            mode: ProviderDisableMode::Soft,
        }
    }

    fn priority_record(provider: &str) -> ProviderPriorityWire {
        ProviderPriorityWire {
            version: PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION,
            provider: provider.to_string(),
            created_at: NOW,
            expires_at: None,
            source: "ace".to_string(),
        }
    }

    #[test]
    fn pool_mask_prefers_non_soft_when_competing_with_actual_soft() {
        let context = provider_routing_context_from_parts(
            vec![soft_disable("claude")],
            None,
            NOW,
        )
        .unwrap();
        let records = vec![
            classified(&context, "claude"),
            classified(&context, "codex"),
        ];

        assert_eq!(pool_eligibility_mask(&records).unwrap(), vec![false, true]);
        assert!(!pool_reservation_eligible(&records, 0).unwrap());
        assert!(pool_reservation_eligible(&records, 1).unwrap());
    }

    #[test]
    fn pool_mask_prefers_non_soft_when_priority_is_only_in_the_tail() {
        let context = provider_routing_context_from_parts(
            vec![soft_disable("claude")],
            Some(priority_record("grok")),
            NOW,
        )
        .unwrap();
        let records = vec![
            classified(&context, "claude"),
            classified(&context, "codex"),
        ];

        assert_eq!(
            records[0].availability,
            ProviderEffectiveAvailability::Sparing
        );
        assert_eq!(
            records[1].availability,
            ProviderEffectiveAvailability::Sparing
        );
        assert_eq!(pool_eligibility_mask(&records).unwrap(), vec![false, true]);
        assert!(!pool_reservation_eligible(&records, 0).unwrap());
        assert!(pool_reservation_eligible(&records, 1).unwrap());
    }

    #[test]
    fn pool_mask_prefers_non_soft_when_priority_provider_is_unavailable() {
        let context = provider_routing_context_from_parts(
            vec![soft_disable("claude")],
            Some(priority_record("grok")),
            NOW,
        )
        .unwrap();
        let records = [
            classified(&context, "claude"),
            classified(&context, "codex"),
        ];
        assert_eq!(
            classified_cli(&context, "grok", false).availability,
            ProviderEffectiveAvailability::Unavailable
        );

        assert_eq!(pool_eligibility_mask(&records).unwrap(), vec![false, true]);
        assert!(!pool_reservation_eligible(&records, 0).unwrap());
    }

    #[test]
    fn pool_mask_keeps_priority_winner_in_the_primary_pool() {
        let context = provider_routing_context_from_parts(
            vec![],
            Some(priority_record("codex")),
            NOW,
        )
        .unwrap();
        let records = vec![
            classified(&context, "claude"),
            classified(&context, "codex"),
        ];

        assert_eq!(pool_eligibility_mask(&records).unwrap(), vec![false, true]);
        assert!(pool_reservation_eligible(&records, 0).unwrap());
        assert!(pool_reservation_eligible(&records, 1).unwrap());
    }

    #[test]
    fn pool_mask_rotates_when_every_usable_member_is_actual_soft() {
        let context = provider_routing_context_from_parts(
            vec![soft_disable("claude"), soft_disable("codex")],
            None,
            NOW,
        )
        .unwrap();
        let records = vec![
            classified(&context, "claude"),
            classified(&context, "codex"),
        ];

        assert_eq!(pool_eligibility_mask(&records).unwrap(), vec![true, true]);
        assert!(pool_reservation_eligible(&records, 0).unwrap());
        assert!(pool_reservation_eligible(&records, 1).unwrap());
    }

    #[test]
    fn pool_mask_uses_soft_member_when_it_is_the_only_usable_primary() {
        let context = provider_routing_context_from_parts(
            vec![soft_disable("claude")],
            Some(priority_record("grok")),
            NOW,
        )
        .unwrap();
        let records = vec![
            classified(&context, "claude"),
            classified_cli(&context, "codex", false),
        ];

        assert_eq!(pool_eligibility_mask(&records).unwrap(), vec![true, false]);
        assert!(pool_reservation_eligible(&records, 0).unwrap());
        assert!(!pool_reservation_eligible(&records, 1).unwrap());
    }

    #[test]
    fn pool_mask_all_unavailable_admits_nobody() {
        let context =
            provider_routing_context_from_parts(vec![], None, NOW).unwrap();
        let records = vec![
            classified_cli(&context, "claude", false),
            classified_cli(&context, "codex", false),
        ];

        assert_eq!(
            pool_eligibility_mask(&records).unwrap(),
            vec![false, false]
        );
        assert!(!pool_reservation_eligible(&records, 0).unwrap());
        assert!(!pool_reservation_eligible(&records, 1).unwrap());
    }

    #[test]
    fn actually_soft_priority_target_loses_to_non_soft_backup() {
        let context = provider_routing_context_from_parts(
            vec![soft_disable("codex")],
            Some(priority_record("codex")),
            NOW,
        )
        .unwrap();
        let records = vec![
            classified(&context, "claude"),
            classified(&context, "codex"),
        ];

        assert_eq!(
            records[1].availability,
            ProviderEffectiveAvailability::Sparing
        );
        assert_eq!(pool_eligibility_mask(&records).unwrap(), vec![true, false]);
        assert!(pool_reservation_eligible(&records, 0).unwrap());
        assert!(!pool_reservation_eligible(&records, 1).unwrap());
    }

    #[test]
    fn pool_policy_rejects_empty_records_and_out_of_range_index() {
        let error = pool_eligibility_mask(&[]).unwrap_err();
        assert!(matches!(error, ProviderPriorityError::Validation(_)));

        let context =
            provider_routing_context_from_parts(vec![], None, NOW).unwrap();
        let records = vec![classified(&context, "claude")];
        let error = pool_reservation_eligible(&records, 1).unwrap_err();
        assert!(matches!(error, ProviderPriorityError::Validation(_)));

        let mut bad = classified(&context, "claude");
        bad.version = 99;
        let error = pool_eligibility_mask(&[bad]).unwrap_err();
        assert!(matches!(error, ProviderPriorityError::Validation(_)));
    }

    #[test]
    fn pool_policy_preserves_classified_wire_records() {
        let context = provider_routing_context_from_parts(
            vec![soft_disable("claude")],
            Some(priority_record("grok")),
            NOW,
        )
        .unwrap();
        let before = classified(&context, "claude");
        let _mask =
            pool_eligibility_mask(std::slice::from_ref(&before)).unwrap();
        let after = classified(&context, "claude");
        assert_eq!(before, after);
        assert_eq!(
            before.provenance,
            vec![
                ProviderAvailabilityProvenance::ActualSoftDisable,
                ProviderAvailabilityProvenance::PriorityBackup
            ]
        );
    }
}
