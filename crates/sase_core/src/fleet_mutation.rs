//! Journaled fleet lifecycle mutation contract.
//!
//! Stop, retry, and fork-on-target are exact-instance mutations. Viewers never
//! act locally on a remote row; the owning host revalidates the observed
//! locator and row revision before executing. Operation keys provide scoped
//! idempotency inside an acceptance window.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::fleet_contract::{
    duration_ms, logical_key_unchecked, operation_payload_fingerprint,
    reject_path_like, reject_secretish, timestamp_ms, validate_installation_id,
    validate_label, validate_non_negative_seconds, validate_schema,
    validate_timestamp, AgentInstanceLocatorWire, CapabilitySetWire,
    FleetContractError, FleetLifecycleWire, FleetRowKindWire,
    LogicalAgentLocatorWire, OperationDecisionKindWire,
    OperationDecisionReasonWire, OperationReceiptStateWire, OwnerLivenessWire,
    PayloadFingerprintRequestWire, PayloadFingerprintWire,
    ResolvedAgentSummaryWire, ResourceRevisionWire, ScopedOperationKeyWire,
    FLEET_CONTRACT_SCHEMA_VERSION, MAX_INTENT_BYTES, MAX_LABEL_BYTES,
    MAX_LAUNCH_PROMPT_BYTES,
};

/// Capability advertised for owner-side stop of a live agent-shell instance.
pub const FLEET_MUTATION_CAPABILITY_STOP: &str = "lifecycle.stop";
/// Capability advertised for owner-side retry of an agent-shell row.
pub const FLEET_MUTATION_CAPABILITY_RETRY: &str = "lifecycle.retry";
/// Capability advertised for owner-side fork of an agent-shell row.
pub const FLEET_MUTATION_CAPABILITY_FORK: &str = "lifecycle.fork";

/// Closed mutation kinds the fleet mutation journal accepts.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetMutationKindWire {
    Stop,
    Retry,
    Fork,
}

/// Exact-target mutation intent. Paths and secrets are rejected at validate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetMutationIntentWire {
    pub schema_version: u32,
    pub kind: FleetMutationKindWire,
    pub target: AgentInstanceLocatorWire,
    pub row_revision: ResourceRevisionWire,
    pub reason: Option<String>,
    pub fork_prompt: Option<String>,
    pub kill_source_first: Option<bool>,
    pub follow: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetMutationRequestWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub target_installation_id: String,
    pub intent: FleetMutationIntentWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub acceptance_window_seconds: f64,
}

/// Settled mutation outcome recorded on the receipt.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetMutationOutcomeWire {
    Applied,
    AlreadySettled,
    PreconditionFailed,
    CapabilityMissing,
    AlreadyTerminal,
    UnknownRow,
    InstanceMismatch,
    StaleRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetMutationReceiptWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub target_installation_id: String,
    pub target: AgentInstanceLocatorWire,
    pub resource_revision: ResourceRevisionWire,
    pub accepted_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub state: OperationReceiptStateWire,
    pub outcome: Option<FleetMutationOutcomeWire>,
    pub logical_locator: Option<LogicalAgentLocatorWire>,
    pub instance_locator: Option<AgentInstanceLocatorWire>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableFleetMutationRecordWire {
    pub schema_version: u32,
    pub receipt: FleetMutationReceiptWire,
    pub tombstoned_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetMutationDecisionRequestWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub target: AgentInstanceLocatorWire,
    pub resource_revision: ResourceRevisionWire,
    pub target_installation_id: String,
    pub now_unix: f64,
    pub acceptance_window_seconds: f64,
    pub existing_record: Option<DurableFleetMutationRecordWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetMutationDecisionWire {
    pub schema_version: u32,
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: Option<FleetMutationReceiptWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetMutationResponseWire {
    pub schema_version: u32,
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: FleetMutationReceiptWire,
}

/// Typed refusal from target-side revalidation of an observed row.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetMutationPreconditionReasonWire {
    Ok,
    UnknownRow,
    InstanceMismatch,
    StaleRevision,
    CapabilityMissing,
    AlreadyTerminal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetMutationPreconditionDecisionWire {
    pub schema_version: u32,
    pub allowed: bool,
    pub reason: FleetMutationPreconditionReasonWire,
    pub required_capability: String,
}

/// One bulk-action target attributed to an origin installation when possible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetBulkTargetWire {
    pub schema_version: u32,
    pub origin_installation_id: Option<String>,
    pub alias: Option<String>,
    pub target: AgentInstanceLocatorWire,
    pub row_revision: ResourceRevisionWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetBulkOriginGroupWire {
    pub schema_version: u32,
    pub origin_installation_id: String,
    pub alias: Option<String>,
    pub targets: Vec<FleetBulkTargetWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetBulkPartitionWire {
    pub schema_version: u32,
    pub groups: Vec<FleetBulkOriginGroupWire>,
    pub unattributed: Vec<FleetBulkTargetWire>,
}

impl FleetMutationKindWire {
    pub fn required_capability(self) -> &'static str {
        match self {
            Self::Stop => FLEET_MUTATION_CAPABILITY_STOP,
            Self::Retry => FLEET_MUTATION_CAPABILITY_RETRY,
            Self::Fork => FLEET_MUTATION_CAPABILITY_FORK,
        }
    }
}

impl FleetMutationIntentWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet mutation intent", self.schema_version)?;
        self.target.validate()?;
        self.row_revision
            .validate_for_logical(&self.target.logical)?;
        if let Some(reason) = &self.reason {
            validate_label("fleet mutation reason", reason, MAX_INTENT_BYTES)?;
            reject_path_like("fleet mutation reason", reason)?;
            reject_secretish("fleet mutation reason", reason)?;
        }
        match self.kind {
            FleetMutationKindWire::Fork => {
                let Some(prompt) = &self.fork_prompt else {
                    return Err(FleetContractError::Validation(
                        "fleet mutation fork requires a prompt".to_string(),
                    ));
                };
                validate_fork_prompt(prompt)?;
                if self.kill_source_first.is_some() {
                    return Err(FleetContractError::Validation(
                        "fleet mutation fork must not set kill_source_first"
                            .to_string(),
                    ));
                }
            }
            FleetMutationKindWire::Stop | FleetMutationKindWire::Retry => {
                if self.fork_prompt.is_some() {
                    return Err(FleetContractError::Validation(
                        "fleet mutation stop/retry must not include a fork prompt"
                            .to_string(),
                    ));
                }
                if self.kind == FleetMutationKindWire::Stop
                    && self.kill_source_first.is_some()
                {
                    return Err(FleetContractError::Validation(
                        "fleet mutation stop must not set kill_source_first"
                            .to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl FleetMutationReceiptWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet mutation receipt", self.schema_version)?;
        self.key.validate()?;
        self.payload_fingerprint.validate()?;
        validate_installation_id(&self.target_installation_id)?;
        self.target.validate()?;
        self.resource_revision
            .validate_for_logical(&self.target.logical)?;
        if self.expires_at_unix_ms < self.accepted_at_unix_ms {
            return Err(FleetContractError::Validation(
                "fleet mutation receipt expires before it was accepted"
                    .to_string(),
            ));
        }
        if let Some(locator) = &self.logical_locator {
            locator.validate()?;
        }
        if let Some(locator) = &self.instance_locator {
            locator.validate()?;
        }
        if let Some(message) = &self.message {
            validate_safe_message(message)?;
        }
        Ok(())
    }
}

impl DurableFleetMutationRecordWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("durable fleet mutation record", self.schema_version)?;
        self.receipt.validate()?;
        if self
            .tombstoned_at_unix_ms
            .is_some_and(|value| value < self.receipt.accepted_at_unix_ms)
        {
            return Err(FleetContractError::Validation(
                "fleet mutation tombstone predates acceptance".to_string(),
            ));
        }
        Ok(())
    }
}

impl FleetBulkTargetWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet bulk target", self.schema_version)?;
        self.target.validate()?;
        self.row_revision
            .validate_for_logical(&self.target.logical)?;
        if let Some(origin) = &self.origin_installation_id {
            validate_installation_id(origin)?;
        }
        if let Some(alias) = &self.alias {
            validate_label("fleet bulk target alias", alias, MAX_LABEL_BYTES)?;
            reject_path_like("fleet bulk target alias", alias)?;
            reject_secretish("fleet bulk target alias", alias)?;
        }
        Ok(())
    }
}

pub fn validate_fleet_mutation_intent(
    intent: &FleetMutationIntentWire,
) -> Result<FleetMutationIntentWire, FleetContractError> {
    intent.validate()?;
    Ok(intent.clone())
}

pub fn fleet_mutation_payload_fingerprint(
    intent: &FleetMutationIntentWire,
) -> Result<PayloadFingerprintWire, FleetContractError> {
    intent.validate()?;
    operation_payload_fingerprint(&PayloadFingerprintRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        payload: serde_json::to_value(intent).map_err(|source| {
            FleetContractError::Json {
                path: PathBuf::from("<fleet_mutation_intent>"),
                source,
            }
        })?,
    })
}

pub fn validate_fleet_mutation_request(
    request: &FleetMutationRequestWire,
) -> Result<FleetMutationRequestWire, FleetContractError> {
    validate_schema("fleet mutation request", request.schema_version)?;
    request.key.validate()?;
    validate_installation_id(&request.target_installation_id)?;
    request.intent.validate()?;
    request.payload_fingerprint.validate()?;
    let expected = fleet_mutation_payload_fingerprint(&request.intent)?;
    if request.payload_fingerprint != expected {
        return Err(FleetContractError::Validation(
            "fleet mutation payload_fingerprint does not match intent"
                .to_string(),
        ));
    }
    validate_non_negative_seconds(
        "acceptance_window_seconds",
        request.acceptance_window_seconds,
    )?;
    Ok(request.clone())
}

pub fn decide_fleet_mutation_replay(
    request: &FleetMutationDecisionRequestWire,
) -> Result<FleetMutationDecisionWire, FleetContractError> {
    validate_schema("fleet mutation decision request", request.schema_version)?;
    request.key.validate()?;
    request.payload_fingerprint.validate()?;
    request.target.validate()?;
    request
        .resource_revision
        .validate_for_logical(&request.target.logical)?;
    validate_installation_id(&request.target_installation_id)?;
    validate_timestamp("now_unix", request.now_unix)?;
    validate_non_negative_seconds(
        "acceptance_window_seconds",
        request.acceptance_window_seconds,
    )?;
    let now_ms = timestamp_ms("now_unix", request.now_unix)?;
    let expires_at = now_ms.saturating_add(duration_ms(
        "acceptance_window_seconds",
        request.acceptance_window_seconds,
    )?);
    if let Some(record) = &request.existing_record {
        record.validate()?;
        if record.receipt.key != request.key {
            return Err(FleetContractError::Validation(
                "existing fleet mutation record key does not match request key"
                    .to_string(),
            ));
        }
        if record.tombstoned_at_unix_ms.is_some()
            || now_ms > record.receipt.expires_at_unix_ms
        {
            return Ok(fleet_mutation_decision(
                OperationDecisionKindWire::Expired,
                OperationDecisionReasonWire::ExpiredOrTombstonedKey,
                None,
            ));
        }
        if record.receipt.payload_fingerprint != request.payload_fingerprint {
            return Ok(fleet_mutation_decision(
                OperationDecisionKindWire::Conflict,
                OperationDecisionReasonWire::SameScopedKeyDifferentPayload,
                Some(record.receipt.clone()),
            ));
        }
        if record.receipt.target != request.target
            || record.receipt.resource_revision != request.resource_revision
            || record.receipt.target_installation_id
                != request.target_installation_id
        {
            return Ok(fleet_mutation_decision(
                OperationDecisionKindWire::PreconditionMismatch,
                OperationDecisionReasonWire::TargetOrRevisionMismatch,
                Some(record.receipt.clone()),
            ));
        }
        return Ok(fleet_mutation_decision(
            OperationDecisionKindWire::ReturnOriginalReceipt,
            OperationDecisionReasonWire::SameScopedKeyAndPayload,
            Some(record.receipt.clone()),
        ));
    }
    let receipt = FleetMutationReceiptWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        key: request.key.clone(),
        payload_fingerprint: request.payload_fingerprint.clone(),
        target_installation_id: request.target_installation_id.clone(),
        target: request.target.clone(),
        resource_revision: request.resource_revision.clone(),
        accepted_at_unix_ms: now_ms,
        expires_at_unix_ms: expires_at,
        state: OperationReceiptStateWire::Accepted,
        outcome: None,
        logical_locator: None,
        instance_locator: None,
        message: None,
    };
    Ok(fleet_mutation_decision(
        OperationDecisionKindWire::AcceptNew,
        OperationDecisionReasonWire::UnseenInWindow,
        Some(receipt),
    ))
}

pub fn evaluate_mutation_precondition(
    intent: &FleetMutationIntentWire,
    observed: Option<&ResolvedAgentSummaryWire>,
) -> Result<FleetMutationPreconditionDecisionWire, FleetContractError> {
    intent.validate()?;
    let required_capability = intent.kind.required_capability().to_string();
    let Some(summary) = observed else {
        return Ok(precondition(
            FleetMutationPreconditionReasonWire::UnknownRow,
            required_capability,
        ));
    };
    summary.logical_locator.validate()?;
    let expected_logical = logical_key_unchecked(&intent.target.logical);
    if summary.logical_key != expected_logical {
        return Ok(precondition(
            FleetMutationPreconditionReasonWire::UnknownRow,
            required_capability,
        ));
    }
    match &summary.exact_locator {
        Some(exact) if exact == &intent.target => {}
        Some(_) | None => {
            return Ok(precondition(
                FleetMutationPreconditionReasonWire::InstanceMismatch,
                required_capability,
            ));
        }
    }
    if summary.row_revision != intent.row_revision {
        return Ok(precondition(
            FleetMutationPreconditionReasonWire::StaleRevision,
            required_capability,
        ));
    }
    if !capability_present(&summary.capabilities, &required_capability) {
        return Ok(precondition(
            FleetMutationPreconditionReasonWire::CapabilityMissing,
            required_capability,
        ));
    }
    if mutation_already_terminal(intent.kind, summary) {
        return Ok(precondition(
            FleetMutationPreconditionReasonWire::AlreadyTerminal,
            required_capability,
        ));
    }
    Ok(precondition(
        FleetMutationPreconditionReasonWire::Ok,
        required_capability,
    ))
}

pub fn partition_bulk_targets(
    targets: &[FleetBulkTargetWire],
) -> Result<FleetBulkPartitionWire, FleetContractError> {
    let mut groups: BTreeMap<String, FleetBulkOriginGroupWire> =
        BTreeMap::new();
    let mut unattributed = Vec::new();
    for target in targets {
        target.validate()?;
        match target.origin_installation_id.as_deref() {
            Some(origin) if !origin.is_empty() => {
                let group =
                    groups.entry(origin.to_string()).or_insert_with(|| {
                        FleetBulkOriginGroupWire {
                            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                            origin_installation_id: origin.to_string(),
                            alias: target.alias.clone(),
                            targets: Vec::new(),
                        }
                    });
                if group.alias.is_none() {
                    group.alias = target.alias.clone();
                }
                group.targets.push(target.clone());
            }
            _ => unattributed.push(target.clone()),
        }
    }
    let mut groups: Vec<FleetBulkOriginGroupWire> =
        groups.into_values().collect();
    for group in &mut groups {
        group.targets.sort_by(|left, right| {
            target_sort_key(left).cmp(&target_sort_key(right))
        });
    }
    unattributed.sort_by(|left, right| {
        target_sort_key(left).cmp(&target_sort_key(right))
    });
    Ok(FleetBulkPartitionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        groups,
        unattributed,
    })
}

fn validate_fork_prompt(prompt: &str) -> Result<(), FleetContractError> {
    if prompt.trim().is_empty() {
        return Err(FleetContractError::Validation(
            "fleet mutation fork prompt must be non-empty".to_string(),
        ));
    }
    if prompt.len() > MAX_LAUNCH_PROMPT_BYTES {
        return Err(FleetContractError::Validation(format!(
            "fleet mutation fork prompt exceeds {MAX_LAUNCH_PROMPT_BYTES} bytes"
        )));
    }
    if prompt.chars().any(|ch| ch == '\0') {
        return Err(FleetContractError::Validation(
            "fleet mutation fork prompt must not contain NUL bytes".to_string(),
        ));
    }
    reject_secretish("fleet mutation fork prompt", prompt)?;
    Ok(())
}

fn validate_safe_message(value: &str) -> Result<(), FleetContractError> {
    validate_label("fleet mutation message", value, MAX_LABEL_BYTES)?;
    reject_path_like("fleet mutation message", value)?;
    reject_secretish("fleet mutation message", value)
}

fn fleet_mutation_decision(
    decision: OperationDecisionKindWire,
    reason: OperationDecisionReasonWire,
    receipt: Option<FleetMutationReceiptWire>,
) -> FleetMutationDecisionWire {
    FleetMutationDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        decision,
        reason,
        receipt,
    }
}

fn precondition(
    reason: FleetMutationPreconditionReasonWire,
    required_capability: String,
) -> FleetMutationPreconditionDecisionWire {
    FleetMutationPreconditionDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        allowed: reason == FleetMutationPreconditionReasonWire::Ok,
        reason,
        required_capability,
    }
}

fn capability_present(capabilities: &CapabilitySetWire, wanted: &str) -> bool {
    capabilities.resource.iter().any(|value| value == wanted)
}

fn mutation_already_terminal(
    kind: FleetMutationKindWire,
    summary: &ResolvedAgentSummaryWire,
) -> bool {
    match kind {
        FleetMutationKindWire::Stop => {
            !summary.current_instance
                || summary.liveness != OwnerLivenessWire::Alive
                || matches!(
                    summary.lifecycle,
                    FleetLifecycleWire::Terminal | FleetLifecycleWire::Failed
                )
        }
        FleetMutationKindWire::Retry | FleetMutationKindWire::Fork => {
            summary.row_kind != FleetRowKindWire::AgentShell
                && matches!(
                    summary.lifecycle,
                    FleetLifecycleWire::Terminal | FleetLifecycleWire::Failed
                )
        }
    }
}

fn target_sort_key(target: &FleetBulkTargetWire) -> (String, String, String) {
    (
        logical_key_unchecked(&target.target.logical),
        target.target.run_id.clone(),
        target.target.attempt_id.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet_contract::FleetFamilyRoleWire;
    use crate::fleet_contract::{
        ConnectionHealthWire, ContentMetadataWire, FleetStatusBucketWire,
        HumanDisplayLabelsWire, ObservationFreshnessWire, OriginLocatorWire,
        ProjectLocatorWire,
    };

    fn id(hex: char) -> String {
        format!(
            "{}{}",
            crate::fleet_contract::FLEET_INSTALLATION_ID_PREFIX,
            hex.to_string().repeat(64)
        )
    }

    fn origin(hex: char) -> OriginLocatorWire {
        OriginLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            installation_id: id(hex),
        }
    }

    fn logical(hex: char, agent: &str) -> LogicalAgentLocatorWire {
        LogicalAgentLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            project: ProjectLocatorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin: origin(hex),
                project_id: "project-1".to_string(),
            },
            agent_id: agent.to_string(),
            family_id: Some("family-1".to_string()),
        }
    }

    fn exact(hex: char, agent: &str, run: &str) -> AgentInstanceLocatorWire {
        AgentInstanceLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical: logical(hex, agent),
            shell_id: "shell-1".to_string(),
            run_id: run.to_string(),
            attempt_id: "attempt-1".to_string(),
        }
    }

    fn revision(
        locator: &LogicalAgentLocatorWire,
        revision: u64,
    ) -> ResourceRevisionWire {
        ResourceRevisionWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical_key: logical_key_unchecked(locator),
            revision,
        }
    }

    fn stop_intent() -> FleetMutationIntentWire {
        let target = exact('a', "athena.worker", "run-1");
        FleetMutationIntentWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            kind: FleetMutationKindWire::Stop,
            row_revision: revision(&target.logical, 3),
            target,
            reason: Some("user stop".to_string()),
            fork_prompt: None,
            kill_source_first: None,
            follow: false,
        }
    }

    fn fork_intent() -> FleetMutationIntentWire {
        let target = exact('a', "athena.worker", "run-1");
        FleetMutationIntentWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            kind: FleetMutationKindWire::Fork,
            row_revision: revision(&target.logical, 3),
            target,
            reason: None,
            fork_prompt: Some("continue from here".to_string()),
            kill_source_first: None,
            follow: true,
        }
    }

    fn retry_intent() -> FleetMutationIntentWire {
        let target = exact('a', "athena.worker", "run-1");
        FleetMutationIntentWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            kind: FleetMutationKindWire::Retry,
            row_revision: revision(&target.logical, 3),
            target,
            reason: None,
            fork_prompt: None,
            kill_source_first: Some(true),
            follow: true,
        }
    }

    fn summary_for(
        intent: &FleetMutationIntentWire,
    ) -> ResolvedAgentSummaryWire {
        ResolvedAgentSummaryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical_locator: intent.target.logical.clone(),
            exact_locator: Some(intent.target.clone()),
            logical_key: logical_key_unchecked(&intent.target.logical),
            exact_key: Some("exact-1".to_string()),
            row_kind: FleetRowKindWire::AgentShell,
            family_role: FleetFamilyRoleWire::Root,
            parent_timestamp: None,
            labels: HumanDisplayLabelsWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                project_label: "project-1".to_string(),
                agent_label: Some("athena.worker".to_string()),
                family_label: Some("family-1".to_string()),
                owner_label: None,
                alias: Some("apollo".to_string()),
            },
            project_name: "project-1".to_string(),
            model: Some("gpt-5".to_string()),
            provider: Some("codex".to_string()),
            status: "running".to_string(),
            status_bucket: FleetStatusBucketWire::Running,
            intent: None,
            observed_at_unix: 1000.0,
            row_revision: intent.row_revision.clone(),
            lifecycle: FleetLifecycleWire::Running,
            liveness: OwnerLivenessWire::Alive,
            connection_health: ConnectionHealthWire::Online,
            freshness: ObservationFreshnessWire::Fresh,
            capabilities: CapabilitySetWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                resource: vec![
                    FLEET_MUTATION_CAPABILITY_STOP.to_string(),
                    FLEET_MUTATION_CAPABILITY_RETRY.to_string(),
                    FLEET_MUTATION_CAPABILITY_FORK.to_string(),
                ],
                host: Vec::new(),
                protocol: vec!["fleet.v1".to_string()],
            },
            content: ContentMetadataWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                handle_count: 0,
                total_byte_len: None,
                kinds: Vec::new(),
                supports_range: false,
                supports_growth: false,
            },
            queue_weight: None,
            queue_weight_explicit: false,
            queue_weight_invalid: false,
            queue_weight_error: None,
            current_instance: true,
            dismissable: false,
            needs_attention: false,
            occupied_runner_slot: true,
            container_projected_concrete_agent: false,
        }
    }

    fn operation_key(id: &str) -> ScopedOperationKeyWire {
        ScopedOperationKeyWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            controller_id: "controller-1".to_string(),
            operation_id: id.to_string(),
        }
    }

    #[test]
    fn mutation_fingerprint_is_stable_for_identical_intents() {
        let intent = stop_intent();
        let first = fleet_mutation_payload_fingerprint(&intent).unwrap();
        let second = fleet_mutation_payload_fingerprint(&intent).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.sha256.len(), 64);
    }

    #[test]
    fn mutation_fingerprint_changes_when_intent_changes() {
        let mut other = stop_intent();
        other.reason = Some("different".to_string());
        assert_ne!(
            fleet_mutation_payload_fingerprint(&stop_intent()).unwrap(),
            fleet_mutation_payload_fingerprint(&other).unwrap()
        );
    }

    #[test]
    fn fork_without_prompt_is_rejected() {
        let mut intent = fork_intent();
        intent.fork_prompt = None;
        let error = intent.validate().unwrap_err().to_string();
        assert!(error.contains("fork requires a prompt"), "{error}");
    }

    #[test]
    fn stop_and_retry_reject_fork_prompt() {
        let mut stop = stop_intent();
        stop.fork_prompt = Some("nope".to_string());
        assert!(stop.validate().is_err());
        let mut retry = retry_intent();
        retry.fork_prompt = Some("nope".to_string());
        assert!(retry.validate().is_err());
    }

    #[test]
    fn path_like_reason_is_rejected() {
        let mut intent = stop_intent();
        intent.reason = Some("/tmp/secret".to_string());
        let error = intent.validate().unwrap_err().to_string();
        assert!(error.contains("must not look like a path"), "{error}");
    }

    #[test]
    fn request_fingerprint_mismatch_is_rejected() {
        let intent = stop_intent();
        let fingerprint = fleet_mutation_payload_fingerprint(&intent).unwrap();
        let mut bad = fingerprint.clone();
        bad.sha256 = "b".repeat(64);
        let error =
            validate_fleet_mutation_request(&FleetMutationRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                key: operation_key("op-1"),
                target_installation_id: id('a'),
                intent,
                payload_fingerprint: bad,
                acceptance_window_seconds: 30.0,
            })
            .unwrap_err()
            .to_string();
        assert!(error.contains("payload_fingerprint"), "{error}");
    }

    fn decision_request(
        fingerprint: PayloadFingerprintWire,
        existing: Option<DurableFleetMutationRecordWire>,
        now: f64,
    ) -> FleetMutationDecisionRequestWire {
        let intent = stop_intent();
        FleetMutationDecisionRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            key: operation_key("op-1"),
            payload_fingerprint: fingerprint,
            target: intent.target,
            resource_revision: intent.row_revision,
            target_installation_id: id('a'),
            now_unix: now,
            acceptance_window_seconds: 30.0,
            existing_record: existing,
        }
    }

    #[test]
    fn replay_accepts_unseen_key() {
        let intent = stop_intent();
        let fingerprint = fleet_mutation_payload_fingerprint(&intent).unwrap();
        let decision = decide_fleet_mutation_replay(&decision_request(
            fingerprint,
            None,
            10.0,
        ))
        .unwrap();
        assert_eq!(decision.decision, OperationDecisionKindWire::AcceptNew);
        assert_eq!(
            decision.reason,
            OperationDecisionReasonWire::UnseenInWindow
        );
        assert_eq!(
            decision.receipt.unwrap().state,
            OperationReceiptStateWire::Accepted
        );
    }

    #[test]
    fn replay_returns_original_receipt_for_same_payload() {
        let intent = stop_intent();
        let fingerprint = fleet_mutation_payload_fingerprint(&intent).unwrap();
        let accepted = decide_fleet_mutation_replay(&decision_request(
            fingerprint.clone(),
            None,
            10.0,
        ))
        .unwrap()
        .receipt
        .unwrap();
        let decision = decide_fleet_mutation_replay(&decision_request(
            fingerprint,
            Some(DurableFleetMutationRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: accepted.clone(),
                tombstoned_at_unix_ms: None,
            }),
            11.0,
        ))
        .unwrap();
        assert_eq!(
            decision.decision,
            OperationDecisionKindWire::ReturnOriginalReceipt
        );
        assert_eq!(decision.receipt.unwrap(), accepted);
    }

    #[test]
    fn replay_conflicts_on_changed_payload() {
        let intent = stop_intent();
        let fingerprint = fleet_mutation_payload_fingerprint(&intent).unwrap();
        let accepted = decide_fleet_mutation_replay(&decision_request(
            fingerprint.clone(),
            None,
            10.0,
        ))
        .unwrap()
        .receipt
        .unwrap();
        let mut other = fingerprint;
        other.sha256 = "c".repeat(64);
        let decision = decide_fleet_mutation_replay(&decision_request(
            other,
            Some(DurableFleetMutationRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: accepted,
                tombstoned_at_unix_ms: None,
            }),
            11.0,
        ))
        .unwrap();
        assert_eq!(decision.decision, OperationDecisionKindWire::Conflict);
        assert_eq!(
            decision.reason,
            OperationDecisionReasonWire::SameScopedKeyDifferentPayload
        );
    }

    #[test]
    fn replay_rejects_expired_or_tombstoned_key() {
        let intent = stop_intent();
        let fingerprint = fleet_mutation_payload_fingerprint(&intent).unwrap();
        let mut accepted = decide_fleet_mutation_replay(&decision_request(
            fingerprint.clone(),
            None,
            10.0,
        ))
        .unwrap()
        .receipt
        .unwrap();
        accepted.expires_at_unix_ms = 10_000;
        let expired = decide_fleet_mutation_replay(&decision_request(
            fingerprint.clone(),
            Some(DurableFleetMutationRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: accepted.clone(),
                tombstoned_at_unix_ms: None,
            }),
            20.0,
        ))
        .unwrap();
        assert_eq!(expired.decision, OperationDecisionKindWire::Expired);
        let tombstoned = decide_fleet_mutation_replay(&decision_request(
            fingerprint,
            Some(DurableFleetMutationRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: accepted,
                tombstoned_at_unix_ms: Some(10_001),
            }),
            11.0,
        ))
        .unwrap();
        assert_eq!(tombstoned.decision, OperationDecisionKindWire::Expired);
    }

    #[test]
    fn precondition_allows_matching_live_row() {
        let intent = stop_intent();
        let summary = summary_for(&intent);
        let decision =
            evaluate_mutation_precondition(&intent, Some(&summary)).unwrap();
        assert!(decision.allowed);
        assert_eq!(decision.reason, FleetMutationPreconditionReasonWire::Ok);
        assert_eq!(
            decision.required_capability,
            FLEET_MUTATION_CAPABILITY_STOP
        );
    }

    #[test]
    fn precondition_unknown_row_when_missing_or_logical_mismatch() {
        let intent = stop_intent();
        let missing = evaluate_mutation_precondition(&intent, None).unwrap();
        assert_eq!(
            missing.reason,
            FleetMutationPreconditionReasonWire::UnknownRow
        );
        let mut summary = summary_for(&intent);
        summary.logical_key = "other".to_string();
        let mismatched =
            evaluate_mutation_precondition(&intent, Some(&summary)).unwrap();
        assert_eq!(
            mismatched.reason,
            FleetMutationPreconditionReasonWire::UnknownRow
        );
    }

    #[test]
    fn precondition_instance_mismatch_for_superseded_run() {
        let intent = stop_intent();
        let mut summary = summary_for(&intent);
        summary.exact_locator = Some(exact('a', "athena.worker", "run-2"));
        let decision =
            evaluate_mutation_precondition(&intent, Some(&summary)).unwrap();
        assert_eq!(
            decision.reason,
            FleetMutationPreconditionReasonWire::InstanceMismatch
        );
    }

    #[test]
    fn precondition_stale_revision() {
        let intent = stop_intent();
        let mut summary = summary_for(&intent);
        summary.row_revision.revision = 99;
        let decision =
            evaluate_mutation_precondition(&intent, Some(&summary)).unwrap();
        assert_eq!(
            decision.reason,
            FleetMutationPreconditionReasonWire::StaleRevision
        );
    }

    #[test]
    fn precondition_capability_missing() {
        let intent = stop_intent();
        let mut summary = summary_for(&intent);
        summary.capabilities.resource.clear();
        let decision =
            evaluate_mutation_precondition(&intent, Some(&summary)).unwrap();
        assert_eq!(
            decision.reason,
            FleetMutationPreconditionReasonWire::CapabilityMissing
        );
    }

    #[test]
    fn precondition_already_terminal_for_stop() {
        let intent = stop_intent();
        let mut summary = summary_for(&intent);
        summary.lifecycle = FleetLifecycleWire::Terminal;
        summary.liveness = OwnerLivenessWire::Dead;
        summary.current_instance = false;
        let decision =
            evaluate_mutation_precondition(&intent, Some(&summary)).unwrap();
        assert_eq!(
            decision.reason,
            FleetMutationPreconditionReasonWire::AlreadyTerminal
        );
    }

    #[test]
    fn bulk_partition_groups_by_origin_deterministically() {
        let left = exact('a', "alpha", "run-1");
        let right = exact('b', "beta", "run-1");
        let extra = exact('a', "gamma", "run-1");
        let orphan = exact('c', "delta", "run-1");
        let partition = partition_bulk_targets(&[
            FleetBulkTargetWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin_installation_id: Some(id('b')),
                alias: Some("bravo".to_string()),
                row_revision: revision(&right.logical, 1),
                target: right,
            },
            FleetBulkTargetWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin_installation_id: Some(id('a')),
                alias: Some("apollo".to_string()),
                row_revision: revision(&extra.logical, 1),
                target: extra,
            },
            FleetBulkTargetWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin_installation_id: Some(id('a')),
                alias: Some("apollo".to_string()),
                row_revision: revision(&left.logical, 1),
                target: left,
            },
            FleetBulkTargetWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin_installation_id: None,
                alias: None,
                row_revision: revision(&orphan.logical, 1),
                target: orphan,
            },
        ])
        .unwrap();
        assert_eq!(partition.groups.len(), 2);
        assert_eq!(partition.groups[0].origin_installation_id, id('a'));
        assert_eq!(partition.groups[0].targets.len(), 2);
        assert_eq!(
            partition.groups[0].targets[0].target.logical.agent_id,
            "alpha"
        );
        assert_eq!(
            partition.groups[0].targets[1].target.logical.agent_id,
            "gamma"
        );
        assert_eq!(partition.groups[1].origin_installation_id, id('b'));
        assert_eq!(partition.unattributed.len(), 1);
        assert_eq!(partition.unattributed[0].target.logical.agent_id, "delta");
    }
}
