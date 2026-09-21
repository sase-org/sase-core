use super::content::ResourceRevisionWire;
use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::PAYLOAD_FINGERPRINT_DOMAIN;
use super::identity::canonical_json_into;
use super::locators::AgentInstanceLocatorWire;
use super::validation::duration_ms;
use super::validation::timestamp_ms;
use super::validation::validate_reference_id;
use super::validation::validate_schema;
use super::validation::validate_sha256_digest;
use super::validation::validate_timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Idempotency key scoped by authenticated controller identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedOperationKeyWire {
    pub schema_version: u32,
    pub controller_id: String,
    pub operation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadFingerprintWire {
    pub schema_version: u32,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadFingerprintRequestWire {
    pub schema_version: u32,
    pub payload: Value,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OperationReceiptStateWire {
    Accepted,
    Pending,
    Settled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationReceiptWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub target: AgentInstanceLocatorWire,
    pub resource_revision: ResourceRevisionWire,
    pub accepted_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub state: OperationReceiptStateWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableOperationRecordWire {
    pub schema_version: u32,
    pub receipt: OperationReceiptWire,
    pub tombstoned_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationDecisionRequestWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub target: AgentInstanceLocatorWire,
    pub resource_revision: ResourceRevisionWire,
    pub now_unix: f64,
    pub acceptance_window_seconds: f64,
    pub existing_record: Option<DurableOperationRecordWire>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OperationDecisionKindWire {
    AcceptNew,
    ReturnOriginalReceipt,
    Conflict,
    Expired,
    PreconditionMismatch,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OperationDecisionReasonWire {
    UnseenInWindow,
    SameScopedKeyAndPayload,
    SameScopedKeyDifferentPayload,
    ExpiredOrTombstonedKey,
    TargetOrRevisionMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationDecisionWire {
    pub schema_version: u32,
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: Option<OperationReceiptWire>,
}

pub fn operation_payload_fingerprint(
    request: &PayloadFingerprintRequestWire,
) -> Result<PayloadFingerprintWire, FleetContractError> {
    validate_schema("payload fingerprint request", request.schema_version)?;
    let mut hasher = Sha256::new();
    hasher.update(PAYLOAD_FINGERPRINT_DOMAIN);
    canonical_json_into(&request.payload, &mut hasher)?;
    Ok(PayloadFingerprintWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        sha256: hex::encode(hasher.finalize()),
    })
}

pub fn decide_operation_replay(
    request: &OperationDecisionRequestWire,
) -> Result<OperationDecisionWire, FleetContractError> {
    validate_schema("operation decision request", request.schema_version)?;
    request.key.validate()?;
    request.payload_fingerprint.validate()?;
    request.target.validate()?;
    request
        .resource_revision
        .validate_for_logical(&request.target.logical)?;
    validate_timestamp("now_unix", request.now_unix)?;
    if !request.acceptance_window_seconds.is_finite()
        || request.acceptance_window_seconds < 0.0
    {
        return Err(FleetContractError::Validation(
            "acceptance_window_seconds must be finite and non-negative"
                .to_string(),
        ));
    }
    let now_ms = timestamp_ms("now_unix", request.now_unix)?;
    let window_ms = duration_ms(
        "acceptance_window_seconds",
        request.acceptance_window_seconds,
    )?;
    let expires_at = now_ms.saturating_add(window_ms);
    if let Some(record) = &request.existing_record {
        record.validate()?;
        if record.receipt.key != request.key {
            return Err(FleetContractError::Validation(
                "existing operation record key does not match request key"
                    .to_string(),
            ));
        }
        if record.tombstoned_at_unix_ms.is_some()
            || now_ms > record.receipt.expires_at_unix_ms
        {
            return Ok(operation_decision(
                OperationDecisionKindWire::Expired,
                OperationDecisionReasonWire::ExpiredOrTombstonedKey,
                None,
            ));
        }
        if record.receipt.payload_fingerprint != request.payload_fingerprint {
            return Ok(operation_decision(
                OperationDecisionKindWire::Conflict,
                OperationDecisionReasonWire::SameScopedKeyDifferentPayload,
                Some(record.receipt.clone()),
            ));
        }
        if record.receipt.target != request.target
            || record.receipt.resource_revision != request.resource_revision
        {
            return Ok(operation_decision(
                OperationDecisionKindWire::PreconditionMismatch,
                OperationDecisionReasonWire::TargetOrRevisionMismatch,
                Some(record.receipt.clone()),
            ));
        }
        return Ok(operation_decision(
            OperationDecisionKindWire::ReturnOriginalReceipt,
            OperationDecisionReasonWire::SameScopedKeyAndPayload,
            Some(record.receipt.clone()),
        ));
    }
    let receipt = OperationReceiptWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        key: request.key.clone(),
        payload_fingerprint: request.payload_fingerprint.clone(),
        target: request.target.clone(),
        resource_revision: request.resource_revision.clone(),
        accepted_at_unix_ms: now_ms,
        expires_at_unix_ms: expires_at,
        state: OperationReceiptStateWire::Accepted,
    };
    Ok(operation_decision(
        OperationDecisionKindWire::AcceptNew,
        OperationDecisionReasonWire::UnseenInWindow,
        Some(receipt),
    ))
}

impl ScopedOperationKeyWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("scoped operation key", self.schema_version)?;
        validate_reference_id("controller_id", &self.controller_id)?;
        validate_reference_id("operation_id", &self.operation_id)
    }
}

impl PayloadFingerprintWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("payload fingerprint", self.schema_version)?;
        validate_sha256_digest("payload fingerprint", &self.sha256)
    }
}

impl OperationReceiptWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("operation receipt", self.schema_version)?;
        self.key.validate()?;
        self.payload_fingerprint.validate()?;
        self.target.validate()?;
        self.resource_revision
            .validate_for_logical(&self.target.logical)?;
        if self.expires_at_unix_ms < self.accepted_at_unix_ms {
            return Err(FleetContractError::Validation(
                "operation receipt expires before it was accepted".to_string(),
            ));
        }
        Ok(())
    }
}

impl DurableOperationRecordWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("durable operation record", self.schema_version)?;
        self.receipt.validate()?;
        if self
            .tombstoned_at_unix_ms
            .is_some_and(|value| value < self.receipt.accepted_at_unix_ms)
        {
            return Err(FleetContractError::Validation(
                "operation tombstone predates acceptance".to_string(),
            ));
        }
        Ok(())
    }
}

fn operation_decision(
    decision: OperationDecisionKindWire,
    reason: OperationDecisionReasonWire,
    receipt: Option<OperationReceiptWire>,
) -> OperationDecisionWire {
    OperationDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        decision,
        reason,
        receipt,
    }
}
