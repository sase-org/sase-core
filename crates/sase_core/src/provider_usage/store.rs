//! Machine-local subscription-capacity store.
//!
//! The store keeps normalized provider observations only. It owns account
//! generation fencing, complete/partial inventory merges, and refresh
//! reservation primitives; provider subprocess execution remains outside the
//! core crate.

use super::{
    project_window, sanitize_diagnostic, summarize_filtered_windows,
    usage_window_applies, validate_ident, validate_now, validate_usage_cadence,
    validate_usage_observation, validate_usage_thresholds, window_attention,
    ProviderUsageObservationWire, UsageAttentionKind, UsageAttentionWire,
    UsageCollectionHealth, UsageCollectionOutcome, UsageCompleteness,
    UsageKnownConstraintWire, UsagePublicProviderWire, UsagePublicSnapshotWire,
    UsagePublicWindowWire, UsageReasonCode, UsageSource,
    UsageWindowObservationWire, MAX_CONTEXT_LEN, MAX_DIAGNOSTIC_LEN,
    MAX_KEY_LEN, MAX_OBSERVATIONS, MAX_WINDOWS,
    PROVIDER_USAGE_PUBLIC_SCHEMA_VERSION,
};
use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, LockMode,
    StoreLockError,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

pub const PROVIDER_USAGE_STORE_SCHEMA_VERSION: u32 = 1;
pub const PROVIDER_USAGE_STATE_FILENAME: &str = "llm_provider_usage.json";
const PROVIDER_USAGE_LOCK_FILENAME: &str = "llm_provider_usage.lock";
const LOCK_TIMEOUT_ENV: &str = "SASE_PROVIDER_USAGE_LOCK_TIMEOUT_SECONDS";
const LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_millis(250);
const MAX_STORE_BYTES: usize = 512 * 1024;
const MAX_RESERVATIONS: usize = 128;
const MAX_RESERVATION_TTL_SECONDS: f64 = 3_600.0;
const STALE_TEMP_MAX_AGE: Duration = Duration::from_secs(30 * 60);
const RESERVATION_KEY_SEPARATOR: char = '\u{1f}';

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageStoreDiagnosticWire {
    pub provider: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageStoreReadWire {
    pub version: u32,
    pub snapshot: UsagePublicSnapshotWire,
    pub diagnostics: Vec<ProviderUsageStoreDiagnosticWire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderUsageStoreWriteStatus {
    Recorded,
    Unchanged,
    StaleWriter,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageStoreWriteOutcomeWire {
    pub version: u32,
    pub status: ProviderUsageStoreWriteStatus,
    pub accepted: bool,
    pub provider: String,
    pub account_generation: u64,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageAccountContextWire {
    pub version: u32,
    pub provider: String,
    pub context_id: String,
    pub account_generation: u64,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageRefreshReservationRequestWire {
    pub provider: String,
    pub context_id: String,
    pub account_generation: u64,
    pub operation_id: String,
    pub ttl_seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageRefreshReservationWire {
    pub version: u32,
    pub provider: String,
    pub context_id: String,
    pub account_generation: u64,
    pub operation_id: String,
    pub lease_id: String,
    pub reserved_at: f64,
    pub expires_at: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderUsageRefreshReservationStatus {
    Reserved,
    Joined,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageRefreshReservationOutcomeWire {
    pub version: u32,
    pub status: ProviderUsageRefreshReservationStatus,
    pub reservation: ProviderUsageRefreshReservationWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderUsageStoredWindowWire {
    window: UsageWindowObservationWire,
    ordering_token: f64,
    received_at: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderUsageStoredProviderWire {
    version: u32,
    provider: String,
    context_id: String,
    account_generation: u64,
    last_attempt: ProviderUsageObservationWire,
    last_attempt_ordering_token: f64,
    last_attempt_received_at: f64,
    last_full_observation_at: Option<f64>,
    last_full_ordering_token: Option<f64>,
    windows: BTreeMap<String, ProviderUsageStoredWindowWire>,
    tombstones: BTreeMap<String, f64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderUsageStoreStateWire {
    version: u32,
    providers: BTreeMap<String, Value>,
    reservations: BTreeMap<String, Value>,
}

#[derive(Debug, Clone)]
struct DecodedUsageState {
    providers: BTreeMap<String, ProviderUsageStoredProviderWire>,
    reservations: BTreeMap<String, ProviderUsageRefreshReservationWire>,
    diagnostics: Vec<ProviderUsageStoreDiagnosticWire>,
}

#[derive(Debug, Error)]
pub enum ProviderUsageStoreError {
    #[error("{0}")]
    Validation(String),
    #[error("timed out waiting for the provider-usage store lock: {0}")]
    LockTimeout(String),
    #[error("provider-usage store I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("provider-usage store serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<super::ProviderUsageError> for ProviderUsageStoreError {
    fn from(error: super::ProviderUsageError) -> Self {
        Self::Validation(error.to_string())
    }
}

pub fn provider_usage_state_path(sase_home: &Path) -> PathBuf {
    sase_home.join(PROVIDER_USAGE_STATE_FILENAME)
}

pub fn load_provider_usage_store(
    sase_home: &Path,
    now: f64,
    cadence_seconds: f64,
    warn_percent: f64,
    critical_percent: f64,
) -> Result<ProviderUsageStoreReadWire, ProviderUsageStoreError> {
    validate_read_inputs(now, cadence_seconds, warn_percent, critical_percent)?;
    with_usage_lock(
        sase_home,
        LockMode::Shared,
        "load_provider_usage_store",
        || {
            let decoded = read_state_unlocked(sase_home, now)?;
            Ok(read_wire_from_state(
                decoded,
                now,
                cadence_seconds,
                warn_percent,
                critical_percent,
            ))
        },
    )
}

pub fn record_provider_usage_observation(
    sase_home: &Path,
    observation: ProviderUsageObservationWire,
    now: f64,
) -> Result<ProviderUsageStoreWriteOutcomeWire, ProviderUsageStoreError> {
    validate_now(now)?;
    let observation = validate_usage_observation(observation, now)?;
    with_usage_lock(
        sase_home,
        LockMode::Exclusive,
        "record_provider_usage_observation",
        || {
            let mut state = read_state_unlocked(sase_home, now)?;
            let provider = observation.provider.clone();
            let incoming_generation = observation.account_generation;
            let outcome = match state.providers.get_mut(&provider) {
                Some(record) => {
                    if observation_is_stale_for_record(&observation, record) {
                        usage_write_outcome(
                            ProviderUsageStoreWriteStatus::StaleWriter,
                            false,
                            provider,
                            record.account_generation,
                            Some(
                                "observation belongs to an older account context"
                                    .to_string(),
                            ),
                        )
                    } else {
                        let changed =
                            merge_observation_into_record(record, observation);
                        usage_write_outcome(
                            if changed {
                                ProviderUsageStoreWriteStatus::Recorded
                            } else {
                                ProviderUsageStoreWriteStatus::Unchanged
                            },
                            changed,
                            provider,
                            incoming_generation,
                            None,
                        )
                    }
                }
                None => {
                    let record = record_from_observation(observation);
                    state.providers.insert(provider.clone(), record);
                    usage_write_outcome(
                        ProviderUsageStoreWriteStatus::Recorded,
                        true,
                        provider,
                        incoming_generation,
                        None,
                    )
                }
            };
            if outcome.accepted {
                write_state_unlocked(sase_home, &state)?;
            }
            Ok(outcome)
        },
    )
}

pub fn prepare_provider_usage_account_context(
    sase_home: &Path,
    provider: &str,
    context_id: &str,
    now: f64,
) -> Result<ProviderUsageAccountContextWire, ProviderUsageStoreError> {
    validate_now(now)?;
    let provider =
        validate_ident("provider", provider, super::MAX_PROVIDER_LEN)?;
    let context_id = validate_ident("context_id", context_id, MAX_CONTEXT_LEN)?;
    with_usage_lock(
        sase_home,
        LockMode::Exclusive,
        "prepare_provider_usage_account_context",
        || {
            let mut state = read_state_unlocked(sase_home, now)?;
            let mut changed = false;
            let generation = match state.providers.get(&provider) {
                Some(record) if record.context_id == context_id => {
                    record.account_generation
                }
                Some(record) => record.account_generation.saturating_add(1),
                None => 1,
            };
            let needs_record = state
                .providers
                .get(&provider)
                .map_or(true, |record| record.context_id != context_id);
            if needs_record {
                state.providers.insert(
                    provider.clone(),
                    account_context_changed_record(
                        &provider,
                        &context_id,
                        generation,
                        now,
                    ),
                );
                state
                    .reservations
                    .retain(|_, reservation| reservation.provider != provider);
                changed = true;
            }
            if changed {
                write_state_unlocked(sase_home, &state)?;
            }
            Ok(ProviderUsageAccountContextWire {
                version: PROVIDER_USAGE_STORE_SCHEMA_VERSION,
                provider,
                context_id,
                account_generation: generation,
                changed,
            })
        },
    )
}

pub fn reserve_provider_usage_refresh(
    sase_home: &Path,
    request: ProviderUsageRefreshReservationRequestWire,
    now: f64,
) -> Result<ProviderUsageRefreshReservationOutcomeWire, ProviderUsageStoreError>
{
    validate_now(now)?;
    let request = validate_reservation_request(request)?;
    with_usage_lock(
        sase_home,
        LockMode::Exclusive,
        "reserve_provider_usage_refresh",
        || {
            let mut state = read_state_unlocked(sase_home, now)?;
            prune_expired_reservations(&mut state.reservations, now);
            let key = reservation_key(
                &request.provider,
                &request.context_id,
                request.account_generation,
            );
            if let Some(existing) = state.reservations.get(&key) {
                return Ok(ProviderUsageRefreshReservationOutcomeWire {
                    version: PROVIDER_USAGE_STORE_SCHEMA_VERSION,
                    status: ProviderUsageRefreshReservationStatus::Joined,
                    reservation: existing.clone(),
                });
            }
            if state.reservations.len() >= MAX_RESERVATIONS {
                return Err(ProviderUsageStoreError::Validation(format!(
                    "provider-usage store has more than {MAX_RESERVATIONS} refresh reservations"
                )));
            }
            let reservation = ProviderUsageRefreshReservationWire {
                version: PROVIDER_USAGE_STORE_SCHEMA_VERSION,
                provider: request.provider,
                context_id: request.context_id,
                account_generation: request.account_generation,
                operation_id: request.operation_id,
                lease_id: generate_lease_id(),
                reserved_at: now,
                expires_at: now + request.ttl_seconds,
            };
            state.reservations.insert(key, reservation.clone());
            write_state_unlocked(sase_home, &state)?;
            Ok(ProviderUsageRefreshReservationOutcomeWire {
                version: PROVIDER_USAGE_STORE_SCHEMA_VERSION,
                status: ProviderUsageRefreshReservationStatus::Reserved,
                reservation,
            })
        },
    )
}

pub fn release_provider_usage_refresh(
    sase_home: &Path,
    provider: &str,
    context_id: &str,
    account_generation: u64,
    lease_id: &str,
    now: f64,
) -> Result<bool, ProviderUsageStoreError> {
    validate_now(now)?;
    let provider =
        validate_ident("provider", provider, super::MAX_PROVIDER_LEN)?;
    let context_id = validate_ident("context_id", context_id, MAX_CONTEXT_LEN)?;
    let lease_id = validate_ident("lease_id", lease_id, MAX_KEY_LEN)?;
    with_usage_lock(
        sase_home,
        LockMode::Exclusive,
        "release_provider_usage_refresh",
        || {
            let mut state = read_state_unlocked(sase_home, now)?;
            prune_expired_reservations(&mut state.reservations, now);
            let key =
                reservation_key(&provider, &context_id, account_generation);
            let removed = state
                .reservations
                .get(&key)
                .is_some_and(|reservation| reservation.lease_id == lease_id);
            if removed {
                state.reservations.remove(&key);
                write_state_unlocked(sase_home, &state)?;
            }
            Ok(removed)
        },
    )
}

fn validate_read_inputs(
    now: f64,
    cadence_seconds: f64,
    warn_percent: f64,
    critical_percent: f64,
) -> Result<(), ProviderUsageStoreError> {
    validate_now(now)?;
    validate_usage_cadence(cadence_seconds)?;
    validate_usage_thresholds(warn_percent, critical_percent)?;
    Ok(())
}

fn read_wire_from_state(
    decoded: DecodedUsageState,
    now: f64,
    cadence_seconds: f64,
    warn_percent: f64,
    critical_percent: f64,
) -> ProviderUsageStoreReadWire {
    let providers = decoded
        .providers
        .into_values()
        .map(|record| {
            public_provider_from_record(
                record,
                now,
                cadence_seconds,
                warn_percent,
                critical_percent,
            )
        })
        .collect::<Vec<_>>();
    let collection_health =
        store_snapshot_health(&providers, &decoded.diagnostics);
    let attention = store_snapshot_attention(&providers);
    ProviderUsageStoreReadWire {
        version: PROVIDER_USAGE_STORE_SCHEMA_VERSION,
        snapshot: UsagePublicSnapshotWire {
            schema_version: PROVIDER_USAGE_PUBLIC_SCHEMA_VERSION,
            generated_at: now,
            collection_health,
            providers,
            attention,
        },
        diagnostics: decoded.diagnostics,
    }
}

fn public_provider_from_record(
    record: ProviderUsageStoredProviderWire,
    now: f64,
    cadence_seconds: f64,
    warn_percent: f64,
    critical_percent: f64,
) -> UsagePublicProviderWire {
    let mut windows = record
        .windows
        .into_values()
        .map(|stored| project_window(&stored.window, now, cadence_seconds))
        .collect::<Vec<_>>();
    windows.sort_by(|left, right| left.key.cmp(&right.key));
    let summary = if record.last_attempt.outcome == UsageCollectionOutcome::Ok {
        summarize_filtered_windows(&windows, None)
    } else {
        None
    };
    let known_constraints =
        store_constraint_list(&windows, warn_percent, critical_percent);
    let attention = store_provider_attention(
        &record.provider,
        record.last_attempt.outcome,
        &windows,
        &summary,
        warn_percent,
        critical_percent,
    );
    UsagePublicProviderWire {
        provider: record.provider,
        context_ref: record.context_id,
        account_generation: record.account_generation,
        collection_status: record.last_attempt.outcome,
        collection_reason: record.last_attempt.reason_code,
        completeness: if record.last_attempt.outcome
            == UsageCollectionOutcome::Ok
        {
            record.last_attempt.completeness
        } else {
            UsageCompleteness::Partial
        },
        last_attempt_at: record.last_attempt_received_at,
        last_full_observation_at: record.last_full_observation_at,
        diagnostic: record.last_attempt.diagnostic,
        account_mode: record.last_attempt.account_mode,
        plan: record.last_attempt.plan,
        windows,
        summary,
        known_constraints,
        attention,
    }
}

fn store_constraint_list(
    windows: &[UsagePublicWindowWire],
    warn_percent: f64,
    critical_percent: f64,
) -> Vec<UsageKnownConstraintWire> {
    let mut constraints = Vec::new();
    for window in windows {
        if usage_window_applies(&window.applicability, None)
            == super::UsageApplicabilityMatch::DoesNotApply
        {
            continue;
        }
        let attention =
            window_attention(window, warn_percent, critical_percent);
        if !matches!(
            attention,
            UsageAttentionKind::Rejected
                | UsageAttentionKind::VeryLow
                | UsageAttentionKind::Low
        ) {
            continue;
        }
        constraints.push(UsageKnownConstraintWire {
            window_key: window.key.clone(),
            vendor_state: window.vendor_state,
            used_percent: window.used_percent,
            remaining_percent: window.remaining_percent,
            freshness: window.freshness,
            reset_passed: window.reset_passed,
            observed_at: window.observed_at,
            attention,
        });
    }
    constraints.sort_by(|left, right| {
        right
            .attention
            .rank()
            .cmp(&left.attention.rank())
            .then_with(|| left.window_key.cmp(&right.window_key))
    });
    constraints
}

fn store_provider_attention(
    provider: &str,
    outcome: UsageCollectionOutcome,
    windows: &[UsagePublicWindowWire],
    summary: &Option<super::UsageScopedSummaryWire>,
    warn_percent: f64,
    critical_percent: f64,
) -> UsageAttentionWire {
    let mut best = UsageAttentionWire {
        kind: if outcome.is_problem() {
            UsageAttentionKind::CollectionProblem
        } else {
            UsageAttentionKind::None
        },
        provider: provider.to_string(),
        window_key: None,
    };
    for window in windows {
        let kind = window_attention(window, warn_percent, critical_percent);
        if kind.rank() > best.kind.rank()
            || (kind.rank() == best.kind.rank()
                && kind.rank() > 0
                && best
                    .window_key
                    .as_deref()
                    .map_or(true, |current| window.key.as_str() < current))
        {
            best.kind = kind;
            best.window_key = Some(window.key.clone());
        }
    }
    if best.kind == UsageAttentionKind::None
        && summary.is_none()
        && outcome == UsageCollectionOutcome::Ok
        && !windows.is_empty()
    {
        best.kind = UsageAttentionKind::CollectionProblem;
    }
    best
}

fn store_snapshot_health(
    providers: &[UsagePublicProviderWire],
    diagnostics: &[ProviderUsageStoreDiagnosticWire],
) -> UsageCollectionHealth {
    if providers.is_empty() && diagnostics.is_empty() {
        return UsageCollectionHealth::Empty;
    }
    let any_problem = !diagnostics.is_empty()
        || providers
            .iter()
            .any(|provider| provider.collection_status.is_problem());
    let any_ok = providers
        .iter()
        .any(|provider| !provider.collection_status.is_problem());
    match (any_problem, any_ok) {
        (true, true) => UsageCollectionHealth::Partial,
        (true, false) => UsageCollectionHealth::Error,
        (false, true) => UsageCollectionHealth::Ok,
        (false, false) => UsageCollectionHealth::Empty,
    }
}

fn store_snapshot_attention(
    providers: &[UsagePublicProviderWire],
) -> Option<UsageAttentionWire> {
    let mut best: Option<&UsageAttentionWire> = None;
    for provider in providers {
        let candidate = &provider.attention;
        if candidate.kind == UsageAttentionKind::None {
            continue;
        }
        let take = match best {
            None => true,
            Some(current) => {
                candidate.kind.rank() > current.kind.rank()
                    || (candidate.kind.rank() == current.kind.rank()
                        && candidate.provider < current.provider)
            }
        };
        if take {
            best = Some(candidate);
        }
    }
    best.cloned()
}

fn observation_is_stale_for_record(
    observation: &ProviderUsageObservationWire,
    record: &ProviderUsageStoredProviderWire,
) -> bool {
    observation.account_generation < record.account_generation
        || (observation.account_generation == record.account_generation
            && observation.context_id != record.context_id)
}

fn record_from_observation(
    observation: ProviderUsageObservationWire,
) -> ProviderUsageStoredProviderWire {
    let mut record = ProviderUsageStoredProviderWire {
        version: PROVIDER_USAGE_STORE_SCHEMA_VERSION,
        provider: observation.provider.clone(),
        context_id: observation.context_id.clone(),
        account_generation: observation.account_generation,
        last_attempt: observation.clone(),
        last_attempt_ordering_token: observation.ordering_token,
        last_attempt_received_at: observation.received_at,
        last_full_observation_at: None,
        last_full_ordering_token: None,
        windows: BTreeMap::new(),
        tombstones: BTreeMap::new(),
    };
    merge_observation_into_record(&mut record, observation);
    record
}

fn merge_observation_into_record(
    record: &mut ProviderUsageStoredProviderWire,
    observation: ProviderUsageObservationWire,
) -> bool {
    let before = record.clone();
    let advanced_generation =
        observation.account_generation > record.account_generation;
    if advanced_generation {
        record.context_id = observation.context_id.clone();
        record.account_generation = observation.account_generation;
        record.windows.clear();
        record.tombstones.clear();
        record.last_full_observation_at = None;
        record.last_full_ordering_token = None;
    }
    if advanced_generation || attempt_wins(&observation, record) {
        record.last_attempt = observation.clone();
        record.last_attempt_ordering_token = observation.ordering_token;
        record.last_attempt_received_at = observation.received_at;
    }
    match observation.outcome {
        UsageCollectionOutcome::Ok => {
            merge_ok_observation(record, &observation);
        }
        UsageCollectionOutcome::NotApplicable
        | UsageCollectionOutcome::Unauthenticated
        | UsageCollectionOutcome::Unsupported => {
            if attempt_wins_or_equal(&observation, record) {
                record.windows.clear();
                record.tombstones.clear();
                record.last_full_observation_at = None;
                record.last_full_ordering_token = None;
            }
        }
        UsageCollectionOutcome::Error => {}
    }
    before != *record
}

fn merge_ok_observation(
    record: &mut ProviderUsageStoredProviderWire,
    observation: &ProviderUsageObservationWire,
) {
    if observation.authoritative_empty
        && observation.completeness == UsageCompleteness::Complete
    {
        if complete_inventory_wins(observation, record) {
            record.windows.clear();
            record.tombstones.clear();
            record.last_full_observation_at = Some(observation.ordering_token);
            record.last_full_ordering_token = Some(observation.ordering_token);
        }
        return;
    }

    let complete_inventory = observation.completeness
        == UsageCompleteness::Complete
        && complete_inventory_wins(observation, record);
    let observed_keys = observation
        .windows
        .iter()
        .map(|window| window.key.clone())
        .collect::<BTreeSet<_>>();
    for window in &observation.windows {
        merge_window(record, observation, window.clone());
    }
    if complete_inventory {
        reconcile_complete_inventory(record, observation, &observed_keys);
        record.last_full_observation_at = Some(observation.ordering_token);
        record.last_full_ordering_token = Some(observation.ordering_token);
    }
}

fn merge_window(
    record: &mut ProviderUsageStoredProviderWire,
    observation: &ProviderUsageObservationWire,
    window: UsageWindowObservationWire,
) {
    if record
        .tombstones
        .get(&window.key)
        .is_some_and(|token| observation.ordering_token <= *token)
    {
        return;
    }
    let incoming = ProviderUsageStoredWindowWire {
        window,
        ordering_token: observation.ordering_token,
        received_at: observation.received_at,
    };
    match record.windows.get(&incoming.window.key) {
        Some(existing) if !stored_window_wins(&incoming, existing) => {}
        _ => {
            record.tombstones.remove(&incoming.window.key);
            record.windows.insert(incoming.window.key.clone(), incoming);
        }
    }
}

fn reconcile_complete_inventory(
    record: &mut ProviderUsageStoredProviderWire,
    observation: &ProviderUsageObservationWire,
    observed_keys: &BTreeSet<String>,
) {
    let to_remove = record
        .windows
        .iter()
        .filter_map(|(key, stored)| {
            if observed_keys.contains(key) {
                return None;
            }
            if observation.ordering_token > stored.ordering_token {
                Some(key.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    for key in to_remove {
        record.windows.remove(&key);
        record.tombstones.insert(key, observation.ordering_token);
    }
}

fn attempt_wins(
    observation: &ProviderUsageObservationWire,
    record: &ProviderUsageStoredProviderWire,
) -> bool {
    observation.ordering_token > record.last_attempt_ordering_token
        || (observation.ordering_token == record.last_attempt_ordering_token
            && observation.received_at > record.last_attempt_received_at)
}

fn attempt_wins_or_equal(
    observation: &ProviderUsageObservationWire,
    record: &ProviderUsageStoredProviderWire,
) -> bool {
    observation.ordering_token > record.last_attempt_ordering_token
        || (observation.ordering_token == record.last_attempt_ordering_token
            && observation.received_at >= record.last_attempt_received_at)
}

fn complete_inventory_wins(
    observation: &ProviderUsageObservationWire,
    record: &ProviderUsageStoredProviderWire,
) -> bool {
    record
        .last_full_ordering_token
        .map_or(true, |token| observation.ordering_token > token)
}

fn stored_window_wins(
    incoming: &ProviderUsageStoredWindowWire,
    existing: &ProviderUsageStoredWindowWire,
) -> bool {
    incoming.ordering_token > existing.ordering_token
        || (incoming.ordering_token == existing.ordering_token
            && incoming.received_at > existing.received_at)
}

fn account_context_changed_record(
    provider: &str,
    context_id: &str,
    account_generation: u64,
    now: f64,
) -> ProviderUsageStoredProviderWire {
    let diagnostic = sanitize_diagnostic(
        "account context changed; awaiting subscription usage observation",
    );
    let observation = ProviderUsageObservationWire {
        schema_version: super::PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION,
        provider: provider.to_string(),
        context_id: context_id.to_string(),
        account_generation,
        ordering_token: now,
        received_at: now,
        source: UsageSource::Probe,
        outcome: UsageCollectionOutcome::Error,
        reason_code: Some(UsageReasonCode::AccountContextChanged),
        diagnostic,
        completeness: UsageCompleteness::Partial,
        authoritative_empty: false,
        account_mode: None,
        plan: None,
        windows: vec![],
    };
    ProviderUsageStoredProviderWire {
        version: PROVIDER_USAGE_STORE_SCHEMA_VERSION,
        provider: provider.to_string(),
        context_id: context_id.to_string(),
        account_generation,
        last_attempt: observation,
        last_attempt_ordering_token: now,
        last_attempt_received_at: now,
        last_full_observation_at: None,
        last_full_ordering_token: None,
        windows: BTreeMap::new(),
        tombstones: BTreeMap::new(),
    }
}

fn usage_write_outcome(
    status: ProviderUsageStoreWriteStatus,
    accepted: bool,
    provider: String,
    account_generation: u64,
    reason: Option<String>,
) -> ProviderUsageStoreWriteOutcomeWire {
    ProviderUsageStoreWriteOutcomeWire {
        version: PROVIDER_USAGE_STORE_SCHEMA_VERSION,
        status,
        accepted,
        provider,
        account_generation,
        reason,
    }
}

fn read_state_unlocked(
    sase_home: &Path,
    now: f64,
) -> Result<DecodedUsageState, ProviderUsageStoreError> {
    let path = provider_usage_state_path(sase_home);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(empty_decoded_state())
        }
        Err(error) => return Err(error.into()),
    };
    if bytes.len() > MAX_STORE_BYTES {
        return Err(ProviderUsageStoreError::Validation(format!(
            "provider-usage store exceeds {MAX_STORE_BYTES} bytes"
        )));
    }
    let raw: ProviderUsageStoreStateWire = serde_json::from_slice(&bytes)?;
    if raw.version != PROVIDER_USAGE_STORE_SCHEMA_VERSION {
        return Err(ProviderUsageStoreError::Validation(format!(
            "provider-usage store version must be {}, got {}",
            PROVIDER_USAGE_STORE_SCHEMA_VERSION, raw.version
        )));
    }
    let mut providers = BTreeMap::new();
    let mut reservations = BTreeMap::new();
    let mut diagnostics = Vec::new();
    if raw.providers.len() > MAX_OBSERVATIONS {
        diagnostics.push(store_diagnostic(
            None,
            format!(
                "provider-usage store has more than {MAX_OBSERVATIONS} provider records"
            ),
        ));
    } else {
        for (provider, value) in raw.providers {
            match decode_provider_record(&provider, value, now) {
                Ok(record) => {
                    providers.insert(provider, record);
                }
                Err(message) => {
                    diagnostics.push(store_diagnostic(Some(provider), message));
                }
            }
        }
    }
    for (key, value) in raw.reservations {
        match decode_reservation(&key, value, now) {
            Ok(Some(reservation)) => {
                reservations.insert(key, reservation);
            }
            Ok(None) => {}
            Err(message) => diagnostics.push(store_diagnostic(None, message)),
        }
    }
    Ok(DecodedUsageState {
        providers,
        reservations,
        diagnostics,
    })
}

fn write_state_unlocked(
    sase_home: &Path,
    state: &DecodedUsageState,
) -> Result<(), ProviderUsageStoreError> {
    let path = provider_usage_state_path(sase_home);
    let parent = path.parent().ok_or_else(|| {
        ProviderUsageStoreError::Validation(
            "provider-usage path has no parent directory".to_string(),
        )
    })?;
    fs::create_dir_all(parent)?;
    restrict_dir(parent)?;
    reap_stale_temp_siblings(&path, SystemTime::now());
    let wire = ProviderUsageStoreStateWire {
        version: PROVIDER_USAGE_STORE_SCHEMA_VERSION,
        providers: state
            .providers
            .iter()
            .map(|(provider, record)| {
                serde_json::to_value(record)
                    .map(|value| (provider.clone(), value))
                    .map_err(ProviderUsageStoreError::Json)
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?,
        reservations: state
            .reservations
            .iter()
            .map(|(key, reservation)| {
                serde_json::to_value(reservation)
                    .map(|value| (key.clone(), value))
                    .map_err(ProviderUsageStoreError::Json)
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?,
    };
    let mut bytes = serde_json::to_vec_pretty(&wire)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_STORE_BYTES {
        return Err(ProviderUsageStoreError::Validation(format!(
            "serialized provider-usage store exceeds {MAX_STORE_BYTES} bytes"
        )));
    }
    let tmp_path = temp_path_for(&path);
    let write_result = (|| -> Result<(), ProviderUsageStoreError> {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let handle = options.open(&tmp_path)?;
        let mut writer = BufWriter::new(handle);
        writer.write_all(&bytes)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        fs::rename(&tmp_path, &path)?;
        restrict_file(&path)?;
        if let Ok(directory) = File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_result
}

fn empty_decoded_state() -> DecodedUsageState {
    DecodedUsageState {
        providers: BTreeMap::new(),
        reservations: BTreeMap::new(),
        diagnostics: Vec::new(),
    }
}

fn decode_provider_record(
    key: &str,
    value: Value,
    now: f64,
) -> Result<ProviderUsageStoredProviderWire, String> {
    let mut record: ProviderUsageStoredProviderWire =
        serde_json::from_value(value).map_err(|error| {
            format!("provider usage record is not valid v1 JSON: {error}")
        })?;
    validate_stored_provider_record(key, &mut record, now)?;
    Ok(record)
}

fn validate_stored_provider_record(
    key: &str,
    record: &mut ProviderUsageStoredProviderWire,
    now: f64,
) -> Result<(), String> {
    if record.version != PROVIDER_USAGE_STORE_SCHEMA_VERSION {
        return Err(format!(
            "provider usage record version must be {}, got {}",
            PROVIDER_USAGE_STORE_SCHEMA_VERSION, record.version
        ));
    }
    if record.provider != key {
        return Err(
            "provider usage record key does not match provider".to_string()
        );
    }
    validate_usage_observation(record.last_attempt.clone(), now)
        .map_err(|error| error.to_string())?;
    if record.last_attempt.provider != record.provider
        || record.last_attempt.context_id != record.context_id
        || record.last_attempt.account_generation != record.account_generation
    {
        return Err(
            "provider usage last_attempt does not match record context"
                .to_string(),
        );
    }
    if !record.last_attempt_ordering_token.is_finite()
        || record.last_attempt_ordering_token <= 0.0
        || !record.last_attempt_received_at.is_finite()
        || record.last_attempt_received_at <= 0.0
    {
        return Err(
            "provider usage last attempt ordering must be finite and positive"
                .to_string(),
        );
    }
    if record.windows.len() > MAX_WINDOWS {
        return Err(format!(
            "provider usage record has more than {MAX_WINDOWS} windows"
        ));
    }
    for (window_key, stored) in &record.windows {
        if stored.window.key != *window_key {
            return Err(
                "provider usage window key does not match map key".to_string()
            );
        }
        super::validate_window(&record.provider, stored.window.clone(), now)
            .map_err(|error| error.to_string())?;
        if !stored.ordering_token.is_finite()
            || stored.ordering_token <= 0.0
            || !stored.received_at.is_finite()
            || stored.received_at <= 0.0
        {
            return Err(
                "provider usage stored window ordering must be finite and positive"
                    .to_string(),
            );
        }
    }
    for (window_key, token) in &record.tombstones {
        validate_ident(
            &format!("provider {} tombstone key", record.provider),
            window_key,
            MAX_KEY_LEN,
        )
        .map_err(|error| error.to_string())?;
        if !token.is_finite() || *token <= 0.0 {
            return Err(
                "provider usage tombstone token must be finite and positive"
                    .to_string(),
            );
        }
    }
    record.tombstones.retain(|key, token| {
        !record.windows.contains_key(key)
            && *token
                >= record
                    .last_full_ordering_token
                    .unwrap_or(record.last_attempt_ordering_token)
    });
    Ok(())
}

fn decode_reservation(
    key: &str,
    value: Value,
    now: f64,
) -> Result<Option<ProviderUsageRefreshReservationWire>, String> {
    let reservation: ProviderUsageRefreshReservationWire =
        serde_json::from_value(value).map_err(|error| {
            format!(
                "provider usage refresh reservation is not valid JSON: {error}"
            )
        })?;
    validate_reservation(&reservation)?;
    if reservation_key(
        &reservation.provider,
        &reservation.context_id,
        reservation.account_generation,
    ) != key
    {
        return Err(
            "provider usage refresh reservation key does not match record"
                .to_string(),
        );
    }
    if now >= reservation.expires_at {
        Ok(None)
    } else {
        Ok(Some(reservation))
    }
}

fn validate_reservation_request(
    request: ProviderUsageRefreshReservationRequestWire,
) -> Result<ProviderUsageRefreshReservationRequestWire, ProviderUsageStoreError>
{
    let provider =
        validate_ident("provider", &request.provider, super::MAX_PROVIDER_LEN)?;
    let context_id =
        validate_ident("context_id", &request.context_id, MAX_CONTEXT_LEN)?;
    let operation_id =
        validate_ident("operation_id", &request.operation_id, MAX_KEY_LEN)?;
    if !request.ttl_seconds.is_finite()
        || request.ttl_seconds <= 0.0
        || request.ttl_seconds > MAX_RESERVATION_TTL_SECONDS
    {
        return Err(ProviderUsageStoreError::Validation(format!(
            "ttl_seconds must be finite and in (0, {MAX_RESERVATION_TTL_SECONDS}]"
        )));
    }
    Ok(ProviderUsageRefreshReservationRequestWire {
        provider,
        context_id,
        account_generation: request.account_generation,
        operation_id,
        ttl_seconds: request.ttl_seconds,
    })
}

fn validate_reservation(
    reservation: &ProviderUsageRefreshReservationWire,
) -> Result<(), String> {
    if reservation.version != PROVIDER_USAGE_STORE_SCHEMA_VERSION {
        return Err(format!(
            "reservation version must be {}, got {}",
            PROVIDER_USAGE_STORE_SCHEMA_VERSION, reservation.version
        ));
    }
    validate_ident(
        "reservation provider",
        &reservation.provider,
        super::MAX_PROVIDER_LEN,
    )
    .map_err(|error| error.to_string())?;
    validate_ident(
        "reservation context_id",
        &reservation.context_id,
        MAX_CONTEXT_LEN,
    )
    .map_err(|error| error.to_string())?;
    validate_ident(
        "reservation operation_id",
        &reservation.operation_id,
        MAX_KEY_LEN,
    )
    .map_err(|error| error.to_string())?;
    validate_ident("reservation lease_id", &reservation.lease_id, MAX_KEY_LEN)
        .map_err(|error| error.to_string())?;
    if !reservation.reserved_at.is_finite()
        || reservation.reserved_at <= 0.0
        || !reservation.expires_at.is_finite()
        || reservation.expires_at <= reservation.reserved_at
    {
        return Err(
            "reservation timestamps must be finite and ordered".to_string()
        );
    }
    Ok(())
}

fn prune_expired_reservations(
    reservations: &mut BTreeMap<String, ProviderUsageRefreshReservationWire>,
    now: f64,
) {
    reservations.retain(|_, reservation| now < reservation.expires_at);
}

fn reservation_key(
    provider: &str,
    context_id: &str,
    account_generation: u64,
) -> String {
    format!(
        "{provider}{RESERVATION_KEY_SEPARATOR}{account_generation}{RESERVATION_KEY_SEPARATOR}{context_id}"
    )
}

fn store_diagnostic(
    provider: Option<String>,
    message: String,
) -> ProviderUsageStoreDiagnosticWire {
    let message = sanitize_diagnostic(&message)
        .unwrap_or_else(|| "provider usage store is corrupt".to_string());
    let message = if message.len() > MAX_DIAGNOSTIC_LEN {
        message[..MAX_DIAGNOSTIC_LEN].to_string()
    } else {
        message
    };
    ProviderUsageStoreDiagnosticWire { provider, message }
}

fn with_usage_lock<T>(
    sase_home: &Path,
    mode: LockMode,
    operation_name: &str,
    operation: impl FnOnce() -> Result<T, ProviderUsageStoreError>,
) -> Result<T, ProviderUsageStoreError> {
    fs::create_dir_all(sase_home)?;
    restrict_dir(sase_home)?;
    let lock_path = sase_home.join(PROVIDER_USAGE_LOCK_FILENAME);
    let lock = acquire_store_lock(
        &lock_path,
        &holder_path_for(&lock_path),
        mode,
        timeout_from_env(LOCK_TIMEOUT_ENV, LOCK_TIMEOUT_DEFAULT),
        operation_name,
    )
    .map_err(lock_error_to_usage)?;
    let result = operation();
    let unlock = lock.release();
    match (result, unlock) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}

fn lock_error_to_usage(error: StoreLockError) -> ProviderUsageStoreError {
    match error {
        StoreLockError::Timeout {
            mode,
            lock_path,
            waited_ms,
            holder,
        } => ProviderUsageStoreError::LockTimeout(format!(
            "mode={mode} path={} waited_ms={waited_ms} holder={}",
            lock_path.display(),
            holder
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        )),
        StoreLockError::Open { source, .. }
        | StoreLockError::Acquire { source, .. } => source.into(),
    }
}

fn restrict_dir(path: &Path) -> Result<(), ProviderUsageStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn restrict_file(path: &Path) -> Result<(), ProviderUsageStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn temp_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(PROVIDER_USAGE_STATE_FILENAME);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.with_file_name(format!(".{filename}.{}.{}.tmp", process::id(), nanos))
}

fn reap_stale_temp_siblings(path: &Path, now: SystemTime) {
    let Some(parent) = path.parent() else {
        return;
    };
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(PROVIDER_USAGE_STATE_FILENAME);
    let prefix = format!(".{filename}.");
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(&prefix)
            || !name.ends_with(".tmp")
            || name.len() <= prefix.len() + ".tmp".len()
        {
            continue;
        }
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        let Ok(age) = now.duration_since(modified) else {
            continue;
        };
        if age > STALE_TEMP_MAX_AGE {
            let _ = fs::remove_file(entry.path());
        }
    }
}

fn generate_lease_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("usage-refresh-{}-{nanos}", process::id())
}
