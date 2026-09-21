use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::FLEET_READ_MAX_BATCH_IDS;
use super::error::MAX_LABEL_BYTES;
use super::error::MAX_LAUNCH_PROMPT_BYTES;
use super::locators::AgentInstanceLocatorWire;
use super::locators::LogicalAgentLocatorWire;
use super::operations::operation_payload_fingerprint;
use super::operations::OperationDecisionKindWire;
use super::operations::OperationDecisionReasonWire;
use super::operations::OperationReceiptStateWire;
use super::operations::PayloadFingerprintRequestWire;
use super::operations::PayloadFingerprintWire;
use super::operations::ScopedOperationKeyWire;
use super::validation::duration_ms;
use super::validation::reject_path_like;
use super::validation::reject_secretish;
use super::validation::timestamp_ms;
use super::validation::validate_identifier;
use super::validation::validate_installation_id;
use super::validation::validate_label;
use super::validation::validate_non_negative_seconds;
use super::validation::validate_reference_id;
use super::validation::validate_schema;
use super::validation::validate_sha256_digest;
use super::validation::validate_timestamp;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Portable source project context for remote launch.
///
/// This is deliberately identity/evidence only. It must not carry a checkout
/// path from the source machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchProjectContextWire {
    pub schema_version: u32,
    pub provider_ref: Option<String>,
    pub project_id: String,
    pub revision: Option<String>,
    pub patch_ref: Option<String>,
}

/// Portable reference consumed by a target-side launch.
///
/// V1 accepts only opaque references; local paths are rejected during
/// validation so the target must resolve everything from provider state.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetLaunchReferenceKindWire {
    Artifact,
    Patch,
    Url,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchReferenceWire {
    pub schema_version: u32,
    pub kind: FleetLaunchReferenceKindWire,
    pub reference: String,
    pub sha256: Option<String>,
}

/// Side-effect-free remote launch intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchIntentWire {
    pub schema_version: u32,
    pub prompt: String,
    pub request_id: Option<String>,
    pub display_name: Option<String>,
    pub name: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub runtime: Option<String>,
    pub project: FleetLaunchProjectContextWire,
    pub dry_run: Option<bool>,
    pub follow: bool,
    #[serde(default)]
    pub references: Vec<FleetLaunchReferenceWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchRequestWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub target_installation_id: String,
    pub intent: FleetLaunchIntentWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub acceptance_window_seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchReceiptWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub target_installation_id: String,
    pub accepted_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub state: OperationReceiptStateWire,
    pub logical_locator: Option<LogicalAgentLocatorWire>,
    pub instance_locator: Option<AgentInstanceLocatorWire>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableFleetLaunchRecordWire {
    pub schema_version: u32,
    pub receipt: FleetLaunchReceiptWire,
    pub tombstoned_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchDecisionRequestWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub target_installation_id: String,
    pub now_unix: f64,
    pub acceptance_window_seconds: f64,
    pub existing_record: Option<DurableFleetLaunchRecordWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchDecisionWire {
    pub schema_version: u32,
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: Option<FleetLaunchReceiptWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLaunchResponseWire {
    pub schema_version: u32,
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: FleetLaunchReceiptWire,
}

pub fn validate_fleet_launch_intent(
    intent: &FleetLaunchIntentWire,
) -> Result<FleetLaunchIntentWire, FleetContractError> {
    intent.validate()?;
    Ok(intent.clone())
}

pub fn fleet_launch_payload_fingerprint(
    intent: &FleetLaunchIntentWire,
) -> Result<PayloadFingerprintWire, FleetContractError> {
    intent.validate()?;
    operation_payload_fingerprint(&PayloadFingerprintRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        payload: serde_json::to_value(intent).map_err(|source| {
            FleetContractError::Json {
                path: PathBuf::from("<fleet_launch_intent>"),
                source,
            }
        })?,
    })
}

pub fn validate_fleet_launch_request(
    request: &FleetLaunchRequestWire,
) -> Result<FleetLaunchRequestWire, FleetContractError> {
    validate_schema("fleet launch request", request.schema_version)?;
    request.key.validate()?;
    validate_installation_id(&request.target_installation_id)?;
    request.intent.validate()?;
    request.payload_fingerprint.validate()?;
    let expected = fleet_launch_payload_fingerprint(&request.intent)?;
    if request.payload_fingerprint != expected {
        return Err(FleetContractError::Validation(
            "fleet launch payload_fingerprint does not match intent"
                .to_string(),
        ));
    }
    validate_non_negative_seconds(
        "acceptance_window_seconds",
        request.acceptance_window_seconds,
    )?;
    Ok(request.clone())
}

pub fn decide_fleet_launch_replay(
    request: &FleetLaunchDecisionRequestWire,
) -> Result<FleetLaunchDecisionWire, FleetContractError> {
    validate_schema("fleet launch decision request", request.schema_version)?;
    request.key.validate()?;
    request.payload_fingerprint.validate()?;
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
                "existing fleet launch record key does not match request key"
                    .to_string(),
            ));
        }
        if record.tombstoned_at_unix_ms.is_some()
            || now_ms > record.receipt.expires_at_unix_ms
        {
            return Ok(fleet_launch_decision(
                OperationDecisionKindWire::Expired,
                OperationDecisionReasonWire::ExpiredOrTombstonedKey,
                None,
            ));
        }
        if record.receipt.payload_fingerprint != request.payload_fingerprint {
            return Ok(fleet_launch_decision(
                OperationDecisionKindWire::Conflict,
                OperationDecisionReasonWire::SameScopedKeyDifferentPayload,
                Some(record.receipt.clone()),
            ));
        }
        if record.receipt.target_installation_id
            != request.target_installation_id
        {
            return Ok(fleet_launch_decision(
                OperationDecisionKindWire::PreconditionMismatch,
                OperationDecisionReasonWire::TargetOrRevisionMismatch,
                Some(record.receipt.clone()),
            ));
        }
        return Ok(fleet_launch_decision(
            OperationDecisionKindWire::ReturnOriginalReceipt,
            OperationDecisionReasonWire::SameScopedKeyAndPayload,
            Some(record.receipt.clone()),
        ));
    }
    let receipt = FleetLaunchReceiptWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        key: request.key.clone(),
        payload_fingerprint: request.payload_fingerprint.clone(),
        target_installation_id: request.target_installation_id.clone(),
        accepted_at_unix_ms: now_ms,
        expires_at_unix_ms: expires_at,
        state: OperationReceiptStateWire::Accepted,
        logical_locator: None,
        instance_locator: None,
        message: None,
    };
    Ok(fleet_launch_decision(
        OperationDecisionKindWire::AcceptNew,
        OperationDecisionReasonWire::UnseenInWindow,
        Some(receipt),
    ))
}

impl FleetLaunchProjectContextWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet launch project context", self.schema_version)?;
        validate_identifier("fleet launch project_id", &self.project_id)?;
        reject_path_like("fleet launch project_id", &self.project_id)?;
        if let Some(provider_ref) = &self.provider_ref {
            validate_reference_id("fleet launch provider_ref", provider_ref)?;
        }
        if let Some(revision) = &self.revision {
            validate_reference_id("fleet launch revision", revision)?;
        }
        if let Some(patch_ref) = &self.patch_ref {
            validate_reference_id("fleet launch patch_ref", patch_ref)?;
        }
        if self.revision.is_none() && self.patch_ref.is_none() {
            return Err(FleetContractError::Validation(
                "fleet launch project context requires revision or patch_ref evidence"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

impl FleetLaunchReferenceWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet launch reference", self.schema_version)?;
        validate_reference_id("fleet launch reference", &self.reference)?;
        reject_path_like("fleet launch reference", &self.reference)?;
        if let Some(digest) = &self.sha256 {
            validate_sha256_digest("fleet launch reference sha256", digest)?;
        }
        Ok(())
    }
}

impl FleetLaunchIntentWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet launch intent", self.schema_version)?;
        if self.prompt.trim().is_empty() {
            return Err(FleetContractError::Validation(
                "fleet launch prompt must be non-empty".to_string(),
            ));
        }
        if self.prompt.len() > MAX_LAUNCH_PROMPT_BYTES {
            return Err(FleetContractError::Validation(format!(
                "fleet launch prompt exceeds {MAX_LAUNCH_PROMPT_BYTES} bytes"
            )));
        }
        if self.prompt.chars().any(|ch| ch == '\0') {
            return Err(FleetContractError::Validation(
                "fleet launch prompt must not contain NUL bytes".to_string(),
            ));
        }
        self.project.validate()?;
        if let Some(request_id) = &self.request_id {
            validate_reference_id("fleet launch request_id", request_id)?;
        }
        if let Some(display_name) = &self.display_name {
            validate_label(
                "fleet launch display_name",
                display_name,
                MAX_LABEL_BYTES,
            )?;
        }
        for (field, value) in [
            ("fleet launch name", self.name.as_ref()),
            ("fleet launch model", self.model.as_ref()),
            ("fleet launch provider", self.provider.as_ref()),
            ("fleet launch runtime", self.runtime.as_ref()),
        ] {
            if let Some(value) = value {
                validate_reference_id(field, value)?;
            }
        }
        if self.references.len() > FLEET_READ_MAX_BATCH_IDS {
            return Err(FleetContractError::Validation(format!(
                "fleet launch references exceed {FLEET_READ_MAX_BATCH_IDS} entries"
            )));
        }
        for reference in &self.references {
            reference.validate()?;
        }
        Ok(())
    }
}

impl FleetLaunchReceiptWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet launch receipt", self.schema_version)?;
        self.key.validate()?;
        self.payload_fingerprint.validate()?;
        validate_installation_id(&self.target_installation_id)?;
        if self.expires_at_unix_ms < self.accepted_at_unix_ms {
            return Err(FleetContractError::Validation(
                "fleet launch receipt expires before it was accepted"
                    .to_string(),
            ));
        }
        if let Some(logical) = &self.logical_locator {
            logical.validate()?;
            if logical.project.origin.installation_id
                != self.target_installation_id
            {
                return Err(FleetContractError::Validation(
                    "fleet launch logical locator targets a different installation"
                        .to_string(),
                ));
            }
        }
        if let Some(instance) = &self.instance_locator {
            instance.validate()?;
            if instance.logical.project.origin.installation_id
                != self.target_installation_id
            {
                return Err(FleetContractError::Validation(
                    "fleet launch instance locator targets a different installation"
                        .to_string(),
                ));
            }
            if let Some(logical) = &self.logical_locator {
                if instance.logical != *logical {
                    return Err(FleetContractError::Validation(
                        "fleet launch instance locator does not match logical locator"
                            .to_string(),
                    ));
                }
            }
        }
        if let Some(message) = &self.message {
            validate_label(
                "fleet launch receipt message",
                message,
                MAX_LABEL_BYTES,
            )?;
            reject_path_like("fleet launch receipt message", message)?;
            reject_secretish("fleet launch receipt message", message)?;
        }
        Ok(())
    }
}

impl DurableFleetLaunchRecordWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("durable fleet launch record", self.schema_version)?;
        self.receipt.validate()?;
        if self
            .tombstoned_at_unix_ms
            .is_some_and(|value| value < self.receipt.accepted_at_unix_ms)
        {
            return Err(FleetContractError::Validation(
                "fleet launch tombstone predates acceptance".to_string(),
            ));
        }
        Ok(())
    }
}

fn fleet_launch_decision(
    decision: OperationDecisionKindWire,
    reason: OperationDecisionReasonWire,
    receipt: Option<FleetLaunchReceiptWire>,
) -> FleetLaunchDecisionWire {
    FleetLaunchDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        decision,
        reason,
        receipt,
    }
}
