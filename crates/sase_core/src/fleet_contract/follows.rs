use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::MAX_LABEL_BYTES;
use super::locators::logical_key_matches;
use super::locators::logical_key_unchecked;
use super::locators::LogicalAgentLocatorWire;
use super::locators::OriginLocatorWire;
use super::operations::ScopedOperationKeyWire;
use super::reads::count_scope;
use super::reads::FleetLogicalAgentCountsWire;
use super::resolution::validate_resolved_agent_summary;
use super::resolution::ResolvedAgentSummaryWire;
use super::status::ObservationFreshnessWire;
use super::validation::length_key;
use super::validation::validate_identifier;
use super::validation::validate_label;
use super::validation::validate_schema;
use super::validation::validate_timestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Source that created a viewer-local follow record.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FollowCreatedByWire {
    Explicit,
    Dispatch,
}

/// Lifecycle state for a durable follow.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FollowStateWire {
    Pending,
    Active,
}

/// Durable viewer-local follow intent.
///
/// The record identity is `(logical_key, created_by)`, where
/// `logical_key` includes origin installation ID, project ID, agent session ID, and
/// agent ID. Dispatch follows may be prewritten as `pending` before remote
/// admission and activated when the authoritative receipt arrives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowRecordWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub logical_key: String,
    pub created_by: FollowCreatedByWire,
    pub state: FollowStateWire,
    pub created_at_unix: f64,
    pub updated_at_unix: f64,
    pub activated_at_unix: Option<f64>,
    pub operation_key: Option<ScopedOperationKeyWire>,
}

/// Explicit local unfollow tombstone.
///
/// Tombstones are keyed by logical identity, not by `created_by`, so they can
/// suppress automatic dispatch/reconciliation follows without preventing a
/// later explicit follow action from deliberately clearing them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowTombstoneWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub logical_key: String,
    pub unfollowed_at_unix: f64,
}

/// Promote a singleton follow to the formed agent session identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowAgentSessionPromotionWire {
    pub schema_version: u32,
    pub from: LogicalAgentLocatorWire,
    pub to: LogicalAgentLocatorWire,
}

/// Activate a pending dispatch follow after receipt binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowActivationWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub operation_key: Option<ScopedOperationKeyWire>,
    pub activated_at_unix: f64,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FollowDiagnosticSeverityWire {
    Info,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowDiagnosticWire {
    pub schema_version: u32,
    pub severity: FollowDiagnosticSeverityWire,
    pub code: String,
    pub message: String,
    pub logical_key: Option<String>,
}

/// Normalize durable follow state after local mutations or remote
/// reconciliation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowReconciliationRequestWire {
    pub schema_version: u32,
    pub records: Vec<FollowRecordWire>,
    pub tombstones: Vec<FollowTombstoneWire>,
    #[serde(default)]
    pub promotions: Vec<FollowAgentSessionPromotionWire>,
    #[serde(default)]
    pub activations: Vec<FollowActivationWire>,
    pub now_unix: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowReconciliationWire {
    pub schema_version: u32,
    pub records: Vec<FollowRecordWire>,
    pub tombstones: Vec<FollowTombstoneWire>,
    pub changed: bool,
    pub diagnostics: Vec<FollowDiagnosticWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetHostCountInputWire {
    pub schema_version: u32,
    pub origin: OriginLocatorWire,
    pub summaries: Vec<ResolvedAgentSummaryWire>,
    pub observed_at_unix: Option<f64>,
    pub freshness: ObservationFreshnessWire,
    #[serde(default)]
    pub authoritative_counts: Option<FleetLogicalAgentCountsWire>,
    #[serde(default)]
    pub partial: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FocusFleetCountsRequestWire {
    pub schema_version: u32,
    pub local_summaries: Vec<ResolvedAgentSummaryWire>,
    pub followed_remote_hosts: Vec<FleetHostCountInputWire>,
    pub fleet_hosts: Vec<FleetHostCountInputWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetHostCountWire {
    pub schema_version: u32,
    pub origin: OriginLocatorWire,
    pub counts: FleetLogicalAgentCountsWire,
    pub partial: bool,
    pub observed_at_unix: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetScopeCountsWire {
    pub schema_version: u32,
    pub counts: FleetLogicalAgentCountsWire,
    pub partial: bool,
    pub observed_at_unix_max: Option<f64>,
    pub unknown_origins: Vec<String>,
    pub host_counts: Vec<FleetHostCountWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FocusFleetCountsWire {
    pub schema_version: u32,
    pub focus: FleetScopeCountsWire,
    pub fleet: FleetScopeCountsWire,
}

pub fn follow_record_key(
    record: &FollowRecordWire,
) -> Result<String, FleetContractError> {
    record.validate()?;
    Ok(follow_record_key_unchecked(
        record.logical_key.as_str(),
        record.created_by,
    ))
}

pub fn reconcile_follow_records(
    request: &FollowReconciliationRequestWire,
) -> Result<FollowReconciliationWire, FleetContractError> {
    validate_schema("follow reconciliation request", request.schema_version)?;
    validate_timestamp("now_unix", request.now_unix)?;
    let input_records = request.records.clone();
    let input_tombstones = request.tombstones.clone();
    let mut diagnostics = Vec::new();
    let mut tombstones = normalize_follow_tombstones(&request.tombstones)?;
    let mut records = normalize_follow_records(&request.records)?;

    for promotion in &request.promotions {
        apply_follow_promotion(
            &mut records,
            &tombstones,
            promotion,
            request.now_unix,
            &mut diagnostics,
        )?;
    }
    for activation in &request.activations {
        apply_follow_activation(
            &mut records,
            &tombstones,
            activation,
            &mut diagnostics,
        )?;
    }

    tombstones = normalize_follow_tombstones(
        &tombstones.into_values().collect::<Vec<_>>(),
    )?;
    let mut filtered_records = BTreeMap::new();
    for record in records.into_values() {
        if follow_tombstone_blocks_record(&tombstones, &record) {
            diagnostics.push(follow_diagnostic(
                FollowDiagnosticSeverityWire::Info,
                "follow_tombstone_blocked",
                "explicit unfollow tombstone suppressed automatic follow",
                Some(record.logical_key.clone()),
            )?);
            continue;
        }
        filtered_records.insert(
            follow_record_key_unchecked(
                record.logical_key.as_str(),
                record.created_by,
            ),
            record,
        );
    }

    let records = filtered_records.into_values().collect::<Vec<_>>();
    let tombstones = tombstones.into_values().collect::<Vec<_>>();
    let changed = records != input_records || tombstones != input_tombstones;
    Ok(FollowReconciliationWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        records,
        tombstones,
        changed,
        diagnostics,
    })
}

pub fn count_focus_and_fleet(
    request: &FocusFleetCountsRequestWire,
) -> Result<FocusFleetCountsWire, FleetContractError> {
    validate_schema("focus/fleet counts request", request.schema_version)?;
    let focus = count_scope(
        &request.local_summaries,
        &request.followed_remote_hosts,
        "followed_remote_hosts",
        false,
    )?;
    let fleet = count_scope(&[], &request.fleet_hosts, "fleet_hosts", true)?;
    Ok(FocusFleetCountsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        focus,
        fleet,
    })
}

impl FollowRecordWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("follow record", self.schema_version)?;
        self.logical_locator.validate()?;
        if !logical_key_matches(&self.logical_key, &self.logical_locator) {
            return Err(FleetContractError::Validation(
                "follow record logical_key does not match logical locator"
                    .to_string(),
            ));
        }
        validate_timestamp(
            "follow record created_at_unix",
            self.created_at_unix,
        )?;
        validate_timestamp(
            "follow record updated_at_unix",
            self.updated_at_unix,
        )?;
        if self.updated_at_unix < self.created_at_unix {
            return Err(FleetContractError::Validation(
                "follow record updated_at_unix predates created_at_unix"
                    .to_string(),
            ));
        }
        match (self.state, self.activated_at_unix) {
            (FollowStateWire::Active, Some(value)) => {
                validate_timestamp("follow record activated_at_unix", value)?;
                if value < self.created_at_unix {
                    return Err(FleetContractError::Validation(
                        "follow record activated_at_unix predates created_at_unix"
                            .to_string(),
                    ));
                }
            }
            (FollowStateWire::Active, None) => {
                return Err(FleetContractError::Validation(
                    "active follow record requires activated_at_unix"
                        .to_string(),
                ));
            }
            (FollowStateWire::Pending, Some(_)) => {
                return Err(FleetContractError::Validation(
                    "pending follow record must not include activated_at_unix"
                        .to_string(),
                ));
            }
            (FollowStateWire::Pending, None) => {}
        }
        if let Some(key) = &self.operation_key {
            key.validate()?;
        }
        if self.created_by == FollowCreatedByWire::Dispatch
            && self.operation_key.is_none()
        {
            return Err(FleetContractError::Validation(
                "dispatch follow record requires operation_key".to_string(),
            ));
        }
        Ok(())
    }
}

impl FollowTombstoneWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("follow tombstone", self.schema_version)?;
        self.logical_locator.validate()?;
        if !logical_key_matches(&self.logical_key, &self.logical_locator) {
            return Err(FleetContractError::Validation(
                "follow tombstone logical_key does not match logical locator"
                    .to_string(),
            ));
        }
        validate_timestamp(
            "follow tombstone unfollowed_at_unix",
            self.unfollowed_at_unix,
        )
    }
}

impl FollowAgentSessionPromotionWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("follow agent session promotion", self.schema_version)?;
        self.from.validate()?;
        self.to.validate()?;
        if self.from.agent_session_id.is_some() {
            return Err(FleetContractError::Validation(
                "follow agent session promotion source must be a singleton locator"
                    .to_string(),
            ));
        }
        if self.to.agent_session_id.is_none() {
            return Err(FleetContractError::Validation(
                "follow agent session promotion target must include agent_session_id"
                    .to_string(),
            ));
        }
        if self.from.project != self.to.project
            || self.from.agent_id != self.to.agent_id
        {
            return Err(FleetContractError::Validation(
                "follow agent session promotion must keep origin, project, and agent_id"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

impl FollowActivationWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("follow activation", self.schema_version)?;
        self.logical_locator.validate()?;
        if let Some(key) = &self.operation_key {
            key.validate()?;
        }
        validate_timestamp(
            "follow activation activated_at_unix",
            self.activated_at_unix,
        )
    }
}

impl FleetHostCountInputWire {
    pub(crate) fn validate(
        &self,
        label: &str,
    ) -> Result<(), FleetContractError> {
        validate_schema(label, self.schema_version)?;
        self.origin.validate()?;
        if let Some(observed) = self.observed_at_unix {
            validate_timestamp("host observed_at_unix", observed)?;
        }
        for summary in &self.summaries {
            let summary = validate_resolved_agent_summary(summary)?;
            if summary.logical_locator.project.origin != self.origin {
                return Err(FleetContractError::Validation(format!(
                    "{label} summary belongs to a different origin"
                )));
            }
        }
        Ok(())
    }
}

fn normalize_follow_records(
    records: &[FollowRecordWire],
) -> Result<BTreeMap<String, FollowRecordWire>, FleetContractError> {
    let mut normalized = BTreeMap::new();
    for record in records {
        record.validate()?;
        let key = follow_record_key_unchecked(
            record.logical_key.as_str(),
            record.created_by,
        );
        match normalized.get(&key) {
            Some(existing)
                if follow_record_prefer_existing(existing, record) => {}
            _ => {
                normalized.insert(key, record.clone());
            }
        }
    }
    Ok(normalized)
}

fn normalize_follow_tombstones(
    tombstones: &[FollowTombstoneWire],
) -> Result<BTreeMap<String, FollowTombstoneWire>, FleetContractError> {
    let mut normalized: BTreeMap<String, FollowTombstoneWire> = BTreeMap::new();
    for tombstone in tombstones {
        tombstone.validate()?;
        match normalized.get(&tombstone.logical_key) {
            Some(existing)
                if existing.unfollowed_at_unix
                    >= tombstone.unfollowed_at_unix => {}
            _ => {
                normalized
                    .insert(tombstone.logical_key.clone(), tombstone.clone());
            }
        }
    }
    Ok(normalized)
}

fn follow_record_prefer_existing(
    existing: &FollowRecordWire,
    candidate: &FollowRecordWire,
) -> bool {
    if existing.updated_at_unix != candidate.updated_at_unix {
        return existing.updated_at_unix > candidate.updated_at_unix;
    }
    if existing.state != candidate.state {
        return existing.state == FollowStateWire::Active;
    }
    true
}

fn apply_follow_promotion(
    records: &mut BTreeMap<String, FollowRecordWire>,
    tombstones: &BTreeMap<String, FollowTombstoneWire>,
    promotion: &FollowAgentSessionPromotionWire,
    now_unix: f64,
    diagnostics: &mut Vec<FollowDiagnosticWire>,
) -> Result<(), FleetContractError> {
    promotion.validate()?;
    validate_timestamp("follow promotion now_unix", now_unix)?;
    let from_key = logical_key_unchecked(&promotion.from);
    let to_logical_key = logical_key_unchecked(&promotion.to);
    let candidates =
        [FollowCreatedByWire::Explicit, FollowCreatedByWire::Dispatch];
    for created_by in candidates {
        let record_key = follow_record_key_unchecked(&from_key, created_by);
        let Some(mut record) = records.remove(&record_key) else {
            continue;
        };
        if follow_tombstone_blocks_record(tombstones, &record) {
            diagnostics.push(follow_diagnostic(
                FollowDiagnosticSeverityWire::Info,
                "follow_promotion_source_tombstoned",
                "explicit unfollow tombstone suppressed source follow promotion",
                Some(record.logical_key.clone()),
            )?);
            continue;
        }
        record.logical_locator = promotion.to.clone();
        record.logical_key = to_logical_key.clone();
        record.updated_at_unix = record.updated_at_unix.max(now_unix);
        record.validate()?;
        if follow_tombstone_blocks_record(tombstones, &record) {
            diagnostics.push(follow_diagnostic(
                FollowDiagnosticSeverityWire::Info,
                "follow_promotion_tombstoned",
                "explicit unfollow tombstone suppressed agent session promotion",
                Some(to_logical_key.clone()),
            )?);
            continue;
        }
        let to_record_key =
            follow_record_key_unchecked(&to_logical_key, created_by);
        match records.get(&to_record_key) {
            Some(existing)
                if follow_record_prefer_existing(existing, &record) => {}
            _ => {
                records.insert(to_record_key, record);
            }
        }
    }
    Ok(())
}

fn apply_follow_activation(
    records: &mut BTreeMap<String, FollowRecordWire>,
    tombstones: &BTreeMap<String, FollowTombstoneWire>,
    activation: &FollowActivationWire,
    diagnostics: &mut Vec<FollowDiagnosticWire>,
) -> Result<(), FleetContractError> {
    activation.validate()?;
    let logical_key = logical_key_unchecked(&activation.logical_locator);
    let record_key = follow_record_key_unchecked(
        &logical_key,
        FollowCreatedByWire::Dispatch,
    );
    let Some(record) = records.get_mut(&record_key) else {
        return Ok(());
    };
    if let Some(expected) = &activation.operation_key {
        if record.operation_key.as_ref() != Some(expected) {
            diagnostics.push(follow_diagnostic(
                FollowDiagnosticSeverityWire::Warning,
                "follow_activation_operation_mismatch",
                "dispatch follow activation operation_key did not match record",
                Some(logical_key),
            )?);
            return Ok(());
        }
    }
    record.state = FollowStateWire::Active;
    record.activated_at_unix = Some(activation.activated_at_unix);
    record.updated_at_unix =
        record.updated_at_unix.max(activation.activated_at_unix);
    record.validate()?;
    if follow_tombstone_blocks_record(tombstones, record) {
        diagnostics.push(follow_diagnostic(
            FollowDiagnosticSeverityWire::Info,
            "follow_activation_tombstoned",
            "explicit unfollow tombstone suppressed dispatch follow activation",
            Some(record.logical_key.clone()),
        )?);
    }
    Ok(())
}

fn follow_tombstone_blocks_record(
    tombstones: &BTreeMap<String, FollowTombstoneWire>,
    record: &FollowRecordWire,
) -> bool {
    let Some(tombstone) = tombstones.get(&record.logical_key) else {
        return false;
    };
    record.created_by != FollowCreatedByWire::Explicit
        || tombstone.unfollowed_at_unix >= record.updated_at_unix
}

fn follow_diagnostic(
    severity: FollowDiagnosticSeverityWire,
    code: &str,
    message: &str,
    logical_key: Option<String>,
) -> Result<FollowDiagnosticWire, FleetContractError> {
    validate_identifier("follow diagnostic code", code)?;
    validate_label("follow diagnostic message", message, MAX_LABEL_BYTES)?;
    Ok(FollowDiagnosticWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        severity,
        code: code.to_string(),
        message: message.to_string(),
        logical_key,
    })
}

fn follow_record_key_unchecked(
    logical_key: &str,
    created_by: FollowCreatedByWire,
) -> String {
    length_key([
        ("logical", logical_key),
        (
            "created_by",
            match created_by {
                FollowCreatedByWire::Explicit => "explicit",
                FollowCreatedByWire::Dispatch => "dispatch",
            },
        ),
    ])
}
