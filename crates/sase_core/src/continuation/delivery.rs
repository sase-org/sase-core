//! Pure delivery-state transitions for monitor continuations.
//!
//! Python owns persistence, launch admission journals, process spawn, and
//! provider invocation. This module only validates identity reuse and the
//! legal disposition graph:
//!
//! ```text
//! pending -> reserved -> dispatching -> acknowledged -> settled
//!                |            |
//!                +-- recovery/inspection of the same delivery key --+
//! ```
//!
//! Host completion may skip `dispatching` (`reserved -> acknowledged`)
//! because there is no successor process. Ordinary `continue` deliveries
//! must not.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::schema::{
    validate_continuation_delivery_record, validate_non_empty_text,
    validate_optional_reference, validate_schema,
    ContinuationDeliveryAttemptWire, ContinuationDeliveryDispositionWire,
    ContinuationDeliveryKeyWire, ContinuationDeliveryRecordWire,
    ContinuationError, CONTINUATION_WIRE_SCHEMA_VERSION, MAX_REF_BYTES,
    MAX_TEXT_BYTES,
};

const KIND_MANUAL_REVISION: &str = "manual_revision";
const OUTCOME_ADMIT: &str = "admit";
const OUTCOME_ALREADY_DELIVERED: &str = "already_delivered";
const OUTCOME_EXISTING_RECEIVER: &str = "existing_receiver";
const OUTCOME_NEEDS_ATTENTION: &str = "needs_attention";
const AMBIGUOUS_RECEIVER_REASON: &str = "receiver ownership is ambiguous";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationDeliveryNewRequestWire {
    pub key: ContinuationDeliveryKeyWire,
    pub selected_action: String,
    #[serde(default)]
    pub reserved_identity: Option<String>,
    pub recorded_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationDeliveryTransitionRequestWire {
    pub schema_version: u32,
    pub record: ContinuationDeliveryRecordWire,
    pub target: ContinuationDeliveryDispositionWire,
    #[serde(default)]
    pub reserved_identity: Option<String>,
    #[serde(default)]
    pub acknowledged_by: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    pub recorded_at: String,
    #[serde(default)]
    pub workspace_identity: Option<String>,
    #[serde(default)]
    pub workspace_degraded: bool,
    #[serde(default)]
    pub retryable_pre_dispatch_failure: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationResumeAdoptionRequestBodyWire {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub monitor_id: String,
    #[serde(default)]
    pub result_id: String,
    #[serde(default)]
    pub requested_branch: Option<String>,
    #[serde(default)]
    pub revision_fingerprint: Option<String>,
    #[serde(default)]
    pub existing_manual_branch: Option<String>,
    #[serde(default)]
    pub next_manual_branch: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationReceiverProofWire {
    #[serde(default)]
    pub branch: String,
    #[serde(default)]
    pub identity: Option<String>,
    #[serde(default)]
    pub discoverable: bool,
    #[serde(default)]
    pub process_alive: bool,
    #[serde(default)]
    pub spawn_recorded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationResumeAdoptionRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub records: Vec<ContinuationDeliveryRecordWire>,
    pub request: ContinuationResumeAdoptionRequestBodyWire,
    #[serde(default)]
    pub receiver_proofs: Vec<ContinuationReceiverProofWire>,
    #[serde(default)]
    pub recorded_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationResumeAdoptionDecisionWire {
    pub schema_version: u32,
    pub outcome: String,
    pub admit: bool,
    pub selected_branch: String,
    #[serde(default)]
    pub preserve_keys: Vec<ContinuationDeliveryKeyWire>,
    #[serde(default)]
    pub fence_keys: Vec<ContinuationDeliveryKeyWire>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub retryable_pre_dispatch_failure: bool,
}

impl ContinuationDeliveryDispositionWire {
    fn is_discoverable(self) -> bool {
        matches!(self, Self::Dispatching | Self::Acknowledged | Self::Settled)
    }

    fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Settled
                | Self::Cancelled
                | Self::Nonlaunchable
                | Self::NeedsAttention
        )
    }
}

pub fn new_continuation_delivery_record(
    request: ContinuationDeliveryNewRequestWire,
) -> Result<ContinuationDeliveryRecordWire, ContinuationError> {
    validate_non_empty_text(
        &request.recorded_at,
        "recorded_at",
        MAX_REF_BYTES,
    )?;
    validate_continuation_delivery_record(ContinuationDeliveryRecordWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        key: request.key,
        selected_action: request.selected_action,
        reserved_identity: request.reserved_identity,
        attempt_history: vec![ContinuationDeliveryAttemptWire {
            attempt_id: "attempt-1".to_string(),
            status: ContinuationDeliveryDispositionWire::Pending,
            recorded_at: request.recorded_at,
            detail: None,
        }],
        acknowledged_by: None,
        disposition: ContinuationDeliveryDispositionWire::Pending,
        disposition_reason: None,
        workspace_identity: None,
        workspace_degraded: false,
    })
}

pub fn transition_continuation_delivery(
    request: ContinuationDeliveryTransitionRequestWire,
) -> Result<ContinuationDeliveryRecordWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ContinuationDeliveryTransitionRequestWire",
    )?;
    validate_non_empty_text(
        &request.recorded_at,
        "recorded_at",
        MAX_REF_BYTES,
    )?;
    if let Some(reason) = &request.reason {
        validate_text_reason(reason)?;
    }
    validate_optional_reference(
        &request.reserved_identity,
        "reserved_identity",
    )?;
    validate_optional_reference(&request.acknowledged_by, "acknowledged_by")?;
    validate_optional_reference(
        &request.workspace_identity,
        "workspace_identity",
    )?;

    let mut record =
        validate_continuation_delivery_record(request.record.clone())?;
    if let Some(existing) =
        discover_existing(&record, request.target, &request)?
    {
        return Ok(existing);
    }
    apply_identity(&mut record, &request)?;
    apply_workspace(&mut record, &request)?;
    reject_degraded_host_completion(&record, request.target)?;
    validate_transition(record.disposition, request.target, &record)?;
    apply_acknowledgment(&mut record, request.target, &request)?;

    record
        .attempt_history
        .push(ContinuationDeliveryAttemptWire {
            attempt_id: format!("attempt-{}", record.attempt_history.len() + 1),
            status: request.target,
            recorded_at: request.recorded_at,
            detail: request.reason.clone(),
        });
    record.disposition = request.target;
    if request.reason.is_some() {
        record.disposition_reason = request.reason;
    }
    validate_continuation_delivery_record(record)
}

pub fn decide_resume_adoption(
    request: ContinuationResumeAdoptionRequestWire,
) -> Result<ContinuationResumeAdoptionDecisionWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ContinuationResumeAdoptionRequestWire",
    )?;
    if !request.recorded_at.is_empty() {
        validate_non_empty_text(
            &request.recorded_at,
            "recorded_at",
            MAX_REF_BYTES,
        )?;
    }
    let mut records = Vec::with_capacity(request.records.len());
    for record in request.records {
        records.push(validate_continuation_delivery_record(record)?);
    }
    let body = &request.request;
    if !body.monitor_id.is_empty() {
        validate_non_empty_text(
            &body.monitor_id,
            "request.monitor_id",
            MAX_REF_BYTES,
        )?;
    }
    if !body.result_id.is_empty() {
        validate_non_empty_text(
            &body.result_id,
            "request.result_id",
            MAX_REF_BYTES,
        )?;
    }
    let records: Vec<ContinuationDeliveryRecordWire> = records
        .into_iter()
        .filter(|record| {
            (body.monitor_id.is_empty()
                || record.key.monitor_id == body.monitor_id)
                && (body.result_id.is_empty()
                    || record.key.result_id == body.result_id)
        })
        .collect();
    let proofs: HashMap<&str, &ContinuationReceiverProofWire> = request
        .receiver_proofs
        .iter()
        .filter(|proof| !proof.branch.is_empty())
        .map(|proof| (proof.branch.as_str(), proof))
        .collect();
    let requested_branch = body
        .requested_branch
        .as_deref()
        .or(body.next_manual_branch.as_deref())
        .or_else(|| records.first().map(|record| record.key.branch.as_str()))
        .unwrap_or("failed")
        .to_string();
    Ok(decide_resume_adoption_inner(
        &records,
        body,
        &proofs,
        requested_branch,
    ))
}

fn decide_resume_adoption_inner(
    records: &[ContinuationDeliveryRecordWire],
    body: &ContinuationResumeAdoptionRequestBodyWire,
    proofs: &HashMap<&str, &ContinuationReceiverProofWire>,
    requested_branch: String,
) -> ContinuationResumeAdoptionDecisionWire {
    let manual = body.kind == KIND_MANUAL_REVISION;
    let delivered: Vec<&ContinuationDeliveryRecordWire> = records
        .iter()
        .filter(|record| is_delivered(record.disposition))
        .collect();
    if !delivered.is_empty() {
        return ContinuationResumeAdoptionDecisionWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            outcome: OUTCOME_ALREADY_DELIVERED.to_string(),
            admit: false,
            selected_branch: delivered[0].key.branch.clone(),
            preserve_keys: delivered
                .iter()
                .map(|record| record.key.clone())
                .collect(),
            fence_keys: Vec::new(),
            reason: None,
            retryable_pre_dispatch_failure: false,
        };
    }

    let active: Vec<&ContinuationDeliveryRecordWire> = records
        .iter()
        .filter(|record| is_active(record.disposition))
        .collect();
    if let Some(existing) = body.existing_manual_branch.as_deref() {
        if !existing.is_empty() {
            if let Some(record) =
                records.iter().find(|record| record.key.branch == existing)
            {
                if is_active(record.disposition) {
                    return no_admit(
                        OUTCOME_EXISTING_RECEIVER,
                        existing.to_string(),
                        Vec::new(),
                    );
                }
            }
            return no_admit(
                OUTCOME_EXISTING_RECEIVER,
                existing.to_string(),
                Vec::new(),
            );
        }
    }

    if !active.is_empty() {
        let mut live = Vec::new();
        let mut stale = Vec::new();
        let mut unproven = Vec::new();
        for record in &active {
            let proof = proofs.get(record.key.branch.as_str()).copied();
            if proof_live(proof) {
                live.push(*record);
            } else if proof_stale(proof) {
                stale.push(*record);
            } else {
                unproven.push(*record);
            }
        }
        if !live.is_empty() {
            if manual {
                return ContinuationResumeAdoptionDecisionWire {
                    schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
                    outcome: OUTCOME_NEEDS_ATTENTION.to_string(),
                    admit: false,
                    selected_branch: live[0].key.branch.clone(),
                    preserve_keys: Vec::new(),
                    fence_keys: Vec::new(),
                    reason: Some(AMBIGUOUS_RECEIVER_REASON.to_string()),
                    retryable_pre_dispatch_failure: false,
                };
            }
            return no_admit(
                OUTCOME_EXISTING_RECEIVER,
                live[0].key.branch.clone(),
                Vec::new(),
            );
        }
        if !stale.is_empty() {
            return ContinuationResumeAdoptionDecisionWire {
                schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
                outcome: OUTCOME_NEEDS_ATTENTION.to_string(),
                admit: false,
                selected_branch: stale[0].key.branch.clone(),
                preserve_keys: Vec::new(),
                fence_keys: stale
                    .iter()
                    .map(|record| record.key.clone())
                    .collect(),
                reason: Some(AMBIGUOUS_RECEIVER_REASON.to_string()),
                retryable_pre_dispatch_failure: false,
            };
        }
        if !manual {
            return no_admit(
                OUTCOME_EXISTING_RECEIVER,
                unproven
                    .first()
                    .map(|record| record.key.branch.clone())
                    .unwrap_or(requested_branch),
                Vec::new(),
            );
        }
        let selected = body
            .next_manual_branch
            .clone()
            .filter(|branch| !branch.is_empty())
            .unwrap_or(requested_branch);
        return ContinuationResumeAdoptionDecisionWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            outcome: OUTCOME_ADMIT.to_string(),
            admit: true,
            selected_branch: selected,
            preserve_keys: Vec::new(),
            fence_keys: unproven
                .iter()
                .map(|record| record.key.clone())
                .collect(),
            reason: None,
            retryable_pre_dispatch_failure: false,
        };
    }

    let selected = if manual {
        body.next_manual_branch
            .clone()
            .filter(|branch| !branch.is_empty())
            .unwrap_or(requested_branch)
    } else {
        requested_branch
    };
    ContinuationResumeAdoptionDecisionWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        outcome: OUTCOME_ADMIT.to_string(),
        admit: true,
        selected_branch: selected,
        preserve_keys: Vec::new(),
        fence_keys: Vec::new(),
        reason: None,
        retryable_pre_dispatch_failure: false,
    }
}

fn no_admit(
    outcome: &str,
    selected_branch: String,
    fence_keys: Vec<ContinuationDeliveryKeyWire>,
) -> ContinuationResumeAdoptionDecisionWire {
    ContinuationResumeAdoptionDecisionWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        outcome: outcome.to_string(),
        admit: false,
        selected_branch,
        preserve_keys: Vec::new(),
        fence_keys,
        reason: None,
        retryable_pre_dispatch_failure: false,
    }
}

fn is_delivered(disposition: ContinuationDeliveryDispositionWire) -> bool {
    matches!(
        disposition,
        ContinuationDeliveryDispositionWire::Acknowledged
            | ContinuationDeliveryDispositionWire::Settled
    )
}

fn is_active(disposition: ContinuationDeliveryDispositionWire) -> bool {
    matches!(
        disposition,
        ContinuationDeliveryDispositionWire::Pending
            | ContinuationDeliveryDispositionWire::Reserved
            | ContinuationDeliveryDispositionWire::Dispatching
    )
}

fn proof_live(proof: Option<&ContinuationReceiverProofWire>) -> bool {
    proof.is_some_and(|proof| proof.process_alive || proof.discoverable)
}

fn proof_stale(proof: Option<&ContinuationReceiverProofWire>) -> bool {
    proof.is_some_and(|proof| {
        proof.spawn_recorded && !proof.process_alive && !proof.discoverable
    })
}

fn discover_existing(
    record: &ContinuationDeliveryRecordWire,
    target: ContinuationDeliveryDispositionWire,
    request: &ContinuationDeliveryTransitionRequestWire,
) -> Result<Option<ContinuationDeliveryRecordWire>, ContinuationError> {
    if record.disposition == target {
        ensure_same_identity(record, request)?;
        return Ok(Some(record.clone()));
    }
    if request.retryable_pre_dispatch_failure {
        return Ok(None);
    }
    if matches!(
        target,
        ContinuationDeliveryDispositionWire::Reserved
            | ContinuationDeliveryDispositionWire::Dispatching
    ) && record.disposition.is_discoverable()
    {
        ensure_same_identity(record, request)?;
        return Ok(Some(record.clone()));
    }
    Ok(None)
}

fn ensure_same_identity(
    record: &ContinuationDeliveryRecordWire,
    request: &ContinuationDeliveryTransitionRequestWire,
) -> Result<(), ContinuationError> {
    if let (Some(existing), Some(requested)) = (
        record.reserved_identity.as_deref(),
        request.reserved_identity.as_deref(),
    ) {
        if existing != requested {
            return Err(ContinuationError::conflict(format!(
                "delivery key already reserved as {existing}; cannot allocate {requested}"
            )));
        }
    }
    if let (Some(existing), Some(requested)) = (
        record.acknowledged_by.as_deref(),
        request.acknowledged_by.as_deref(),
    ) {
        if existing != requested {
            return Err(ContinuationError::conflict(format!(
                "delivery already acknowledged by {existing}; {requested} cannot adopt it"
            )));
        }
    }
    Ok(())
}

fn apply_identity(
    record: &mut ContinuationDeliveryRecordWire,
    request: &ContinuationDeliveryTransitionRequestWire,
) -> Result<(), ContinuationError> {
    if let Some(requested) = request.reserved_identity.as_ref() {
        match record.reserved_identity.as_deref() {
            None => record.reserved_identity = Some(requested.clone()),
            Some(existing) if existing == requested => {}
            Some(existing) => {
                return Err(ContinuationError::conflict(format!(
                    "delivery key already reserved as {existing}; cannot allocate {requested}"
                )));
            }
        }
    }
    Ok(())
}

fn apply_workspace(
    record: &mut ContinuationDeliveryRecordWire,
    request: &ContinuationDeliveryTransitionRequestWire,
) -> Result<(), ContinuationError> {
    if let Some(workspace) = request.workspace_identity.as_ref() {
        record.workspace_identity = Some(workspace.clone());
    }
    if request.workspace_degraded {
        record.workspace_degraded = true;
    }
    Ok(())
}

fn reject_degraded_host_completion(
    record: &ContinuationDeliveryRecordWire,
    target: ContinuationDeliveryDispositionWire,
) -> Result<(), ContinuationError> {
    if record.selected_action == "complete"
        && record.workspace_degraded
        && matches!(
            target,
            ContinuationDeliveryDispositionWire::Reserved
                | ContinuationDeliveryDispositionWire::Dispatching
                | ContinuationDeliveryDispositionWire::Acknowledged
                | ContinuationDeliveryDispositionWire::Settled
        )
    {
        return Err(ContinuationError::validation(
            "host completion requires the original verified workspace and cannot use degraded workspace fallback",
        ));
    }
    Ok(())
}

fn apply_acknowledgment(
    record: &mut ContinuationDeliveryRecordWire,
    target: ContinuationDeliveryDispositionWire,
    request: &ContinuationDeliveryTransitionRequestWire,
) -> Result<(), ContinuationError> {
    if target != ContinuationDeliveryDispositionWire::Acknowledged
        && request.acknowledged_by.is_none()
    {
        return Ok(());
    }
    if target != ContinuationDeliveryDispositionWire::Acknowledged {
        if let Some(actor) = request.acknowledged_by.as_ref() {
            record.acknowledged_by = Some(actor.clone());
        }
        return Ok(());
    }
    let actor = request
        .acknowledged_by
        .as_ref()
        .or(record.reserved_identity.as_ref())
        .ok_or_else(|| {
            ContinuationError::validation(
                "acknowledged delivery requires acknowledged_by or reserved_identity",
            )
        })?;
    let reserved = record.reserved_identity.as_deref().ok_or_else(|| {
        ContinuationError::validation(
            "acknowledged delivery requires a reserved_identity",
        )
    })?;
    if actor != reserved {
        return Err(ContinuationError::conflict(format!(
            "receiver {actor} cannot adopt delivery reserved for {reserved}"
        )));
    }
    record.acknowledged_by = Some(actor.clone());
    Ok(())
}

fn validate_transition(
    current: ContinuationDeliveryDispositionWire,
    target: ContinuationDeliveryDispositionWire,
    record: &ContinuationDeliveryRecordWire,
) -> Result<(), ContinuationError> {
    if current.is_terminal() && current != target {
        return Err(ContinuationError::conflict(format!(
            "cannot transition terminal delivery from {} to {}",
            disposition_name(current),
            disposition_name(target)
        )));
    }
    let allowed = matches!(
        (current, target),
        (
            ContinuationDeliveryDispositionWire::Pending,
            ContinuationDeliveryDispositionWire::Reserved
                | ContinuationDeliveryDispositionWire::Cancelled
                | ContinuationDeliveryDispositionWire::Nonlaunchable
                | ContinuationDeliveryDispositionWire::NeedsAttention
        ) | (
            ContinuationDeliveryDispositionWire::Reserved,
            ContinuationDeliveryDispositionWire::Dispatching
                | ContinuationDeliveryDispositionWire::Cancelled
                | ContinuationDeliveryDispositionWire::Nonlaunchable
                | ContinuationDeliveryDispositionWire::NeedsAttention
        ) | (
            ContinuationDeliveryDispositionWire::Reserved,
            ContinuationDeliveryDispositionWire::Acknowledged
        ) | (
            ContinuationDeliveryDispositionWire::Dispatching,
            ContinuationDeliveryDispositionWire::Acknowledged
                | ContinuationDeliveryDispositionWire::Reserved
                | ContinuationDeliveryDispositionWire::Nonlaunchable
                | ContinuationDeliveryDispositionWire::NeedsAttention
        ) | (
            ContinuationDeliveryDispositionWire::Acknowledged,
            ContinuationDeliveryDispositionWire::Settled
                | ContinuationDeliveryDispositionWire::NeedsAttention
        )
    );
    if !allowed {
        return Err(ContinuationError::conflict(format!(
            "illegal delivery transition from {} to {}",
            disposition_name(current),
            disposition_name(target)
        )));
    }
    if target == ContinuationDeliveryDispositionWire::Reserved
        && record.reserved_identity.is_none()
    {
        return Err(ContinuationError::validation(
            "reserved delivery requires reserved_identity",
        ));
    }
    if target == ContinuationDeliveryDispositionWire::Acknowledged
        && current == ContinuationDeliveryDispositionWire::Reserved
        && record.selected_action != "complete"
    {
        return Err(ContinuationError::validation(
            "ordinary continue deliveries must pass through dispatching before acknowledgment",
        ));
    }
    Ok(())
}

fn disposition_name(
    value: ContinuationDeliveryDispositionWire,
) -> &'static str {
    match value {
        ContinuationDeliveryDispositionWire::Pending => "pending",
        ContinuationDeliveryDispositionWire::Reserved => "reserved",
        ContinuationDeliveryDispositionWire::Dispatching => "dispatching",
        ContinuationDeliveryDispositionWire::Acknowledged => "acknowledged",
        ContinuationDeliveryDispositionWire::Settled => "settled",
        ContinuationDeliveryDispositionWire::Cancelled => "cancelled",
        ContinuationDeliveryDispositionWire::Nonlaunchable => "nonlaunchable",
        ContinuationDeliveryDispositionWire::NeedsAttention => {
            "needs_attention"
        }
    }
}

fn validate_text_reason(reason: &str) -> Result<(), ContinuationError> {
    super::schema::validate_text(reason, "reason", MAX_TEXT_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::continuation::schema::ContinuationDeliveryKeyWire;

    fn key() -> ContinuationDeliveryKeyWire {
        ContinuationDeliveryKeyWire {
            monitor_id: "monitor-1".to_string(),
            result_id: "result-1".to_string(),
            branch: "failed".to_string(),
        }
    }

    fn pending(action: &str) -> ContinuationDeliveryRecordWire {
        new_continuation_delivery_record(ContinuationDeliveryNewRequestWire {
            key: key(),
            selected_action: action.to_string(),
            reserved_identity: None,
            recorded_at: "2026-09-12T00:00:00Z".to_string(),
        })
        .unwrap()
    }

    fn request(
        record: ContinuationDeliveryRecordWire,
        target: ContinuationDeliveryDispositionWire,
    ) -> ContinuationDeliveryTransitionRequestWire {
        ContinuationDeliveryTransitionRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            record,
            target,
            reserved_identity: None,
            acknowledged_by: None,
            reason: None,
            recorded_at: "2026-09-12T00:00:01Z".to_string(),
            workspace_identity: None,
            workspace_degraded: false,
            retryable_pre_dispatch_failure: false,
        }
    }

    fn reserve(
        record: ContinuationDeliveryRecordWire,
    ) -> ContinuationDeliveryRecordWire {
        let mut req =
            request(record, ContinuationDeliveryDispositionWire::Reserved);
        req.reserved_identity = Some("acme--1".to_string());
        transition_continuation_delivery(req).unwrap()
    }

    fn dispatch(
        record: ContinuationDeliveryRecordWire,
    ) -> ContinuationDeliveryRecordWire {
        transition_continuation_delivery(request(
            record,
            ContinuationDeliveryDispositionWire::Dispatching,
        ))
        .unwrap()
    }

    #[test]
    fn ordinary_continue_walks_the_approved_graph() {
        let reserved = reserve(pending("continue"));
        assert_eq!(
            reserved.disposition,
            ContinuationDeliveryDispositionWire::Reserved
        );
        assert_eq!(reserved.reserved_identity.as_deref(), Some("acme--1"));

        let dispatching = dispatch(reserved);
        let mut ack = request(
            dispatching,
            ContinuationDeliveryDispositionWire::Acknowledged,
        );
        ack.acknowledged_by = Some("acme--1".to_string());
        let acknowledged = transition_continuation_delivery(ack).unwrap();
        assert_eq!(
            acknowledged.disposition,
            ContinuationDeliveryDispositionWire::Acknowledged
        );

        let settled = transition_continuation_delivery(request(
            acknowledged,
            ContinuationDeliveryDispositionWire::Settled,
        ))
        .unwrap();
        assert_eq!(
            settled.disposition,
            ContinuationDeliveryDispositionWire::Settled
        );
        assert_eq!(settled.attempt_history.len(), 5);
    }

    #[test]
    fn concurrent_reserve_discovers_the_same_identity() {
        let reserved = reserve(pending("continue"));
        let mut again = request(
            reserved.clone(),
            ContinuationDeliveryDispositionWire::Reserved,
        );
        again.reserved_identity = Some("acme--1".to_string());
        let discovered = transition_continuation_delivery(again).unwrap();
        assert_eq!(discovered, reserved);

        let dispatching = dispatch(reserved);
        let mut steal = request(
            dispatching.clone(),
            ContinuationDeliveryDispositionWire::Reserved,
        );
        steal.reserved_identity = Some("acme--2".to_string());
        let err = transition_continuation_delivery(steal).unwrap_err();
        assert_eq!(err.kind, "conflict");
        assert!(err.message.contains("acme--1"));

        let mut same = request(
            dispatching.clone(),
            ContinuationDeliveryDispositionWire::Dispatching,
        );
        same.reserved_identity = Some("acme--1".to_string());
        let discovered_dispatch =
            transition_continuation_delivery(same).unwrap();
        assert_eq!(discovered_dispatch.disposition, dispatching.disposition);
        assert_eq!(
            discovered_dispatch.reserved_identity,
            dispatching.reserved_identity
        );
    }

    #[test]
    fn ordinary_continue_cannot_skip_dispatching() {
        let reserved = reserve(pending("continue"));
        let mut ack = request(
            reserved,
            ContinuationDeliveryDispositionWire::Acknowledged,
        );
        ack.acknowledged_by = Some("acme--1".to_string());
        let err = transition_continuation_delivery(ack).unwrap_err();
        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("dispatching"));
    }

    #[test]
    fn host_completion_may_acknowledge_from_reserved() {
        let mut pending = pending("complete");
        pending.reserved_identity = Some("host-completion".to_string());
        let mut reserved =
            request(pending, ContinuationDeliveryDispositionWire::Reserved);
        reserved.reserved_identity = Some("host-completion".to_string());
        let reserved = transition_continuation_delivery(reserved).unwrap();
        let mut ack = request(
            reserved,
            ContinuationDeliveryDispositionWire::Acknowledged,
        );
        ack.acknowledged_by = Some("host-completion".to_string());
        let acknowledged = transition_continuation_delivery(ack).unwrap();
        assert_eq!(
            acknowledged.disposition,
            ContinuationDeliveryDispositionWire::Acknowledged
        );
        assert_eq!(
            acknowledged.acknowledged_by.as_deref(),
            Some("host-completion")
        );
    }

    #[test]
    fn host_completion_rejects_degraded_workspace() {
        let mut reserved_req = request(
            pending("complete"),
            ContinuationDeliveryDispositionWire::Reserved,
        );
        reserved_req.reserved_identity = Some("host-completion".to_string());
        let reserved = transition_continuation_delivery(reserved_req).unwrap();
        let mut ack = request(
            reserved,
            ContinuationDeliveryDispositionWire::Acknowledged,
        );
        ack.acknowledged_by = Some("host-completion".to_string());
        ack.workspace_degraded = true;
        let err = transition_continuation_delivery(ack).unwrap_err();
        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("original verified workspace"));
    }

    #[test]
    fn wrong_receiver_cannot_adopt_reserved_identity() {
        let dispatching = dispatch(reserve(pending("continue")));
        let mut ack = request(
            dispatching,
            ContinuationDeliveryDispositionWire::Acknowledged,
        );
        ack.acknowledged_by = Some("intruder--1".to_string());
        let err = transition_continuation_delivery(ack).unwrap_err();
        assert_eq!(err.kind, "conflict");
        assert!(err.message.contains("intruder--1"));
    }

    #[test]
    fn retryable_pre_dispatch_failure_returns_to_reserved() {
        let dispatching = dispatch(reserve(pending("continue")));
        let mut retry =
            request(dispatching, ContinuationDeliveryDispositionWire::Reserved);
        retry.reason = Some("spawn_failed".to_string());
        retry.retryable_pre_dispatch_failure = true;
        let reserved = transition_continuation_delivery(retry).unwrap();
        assert_eq!(
            reserved.disposition,
            ContinuationDeliveryDispositionWire::Reserved
        );
        assert_eq!(reserved.reserved_identity.as_deref(), Some("acme--1"));
    }

    #[test]
    fn stop_before_reservation_cancels() {
        let cancelled = transition_continuation_delivery(request(
            pending("continue"),
            ContinuationDeliveryDispositionWire::Cancelled,
        ))
        .unwrap();
        assert_eq!(
            cancelled.disposition,
            ContinuationDeliveryDispositionWire::Cancelled
        );
        let err = transition_continuation_delivery(request(
            cancelled,
            ContinuationDeliveryDispositionWire::Reserved,
        ))
        .unwrap_err();
        assert_eq!(err.kind, "conflict");
    }

    #[test]
    fn reserved_requires_identity() {
        let err = transition_continuation_delivery(request(
            pending("continue"),
            ContinuationDeliveryDispositionWire::Reserved,
        ))
        .unwrap_err();
        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("reserved_identity"));
    }

    fn adoption(
        records: Vec<ContinuationDeliveryRecordWire>,
        kind: &str,
        next_manual: Option<&str>,
        existing_manual: Option<&str>,
        proofs: Vec<ContinuationReceiverProofWire>,
    ) -> ContinuationResumeAdoptionDecisionWire {
        decide_resume_adoption(ContinuationResumeAdoptionRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            records,
            request: ContinuationResumeAdoptionRequestBodyWire {
                kind: kind.to_string(),
                monitor_id: "monitor-1".to_string(),
                result_id: "result-1".to_string(),
                requested_branch: Some("failed".to_string()),
                revision_fingerprint: None,
                existing_manual_branch: existing_manual.map(str::to_string),
                next_manual_branch: next_manual.map(str::to_string),
            },
            receiver_proofs: proofs,
            recorded_at: "2026-09-13T00:00:00Z".to_string(),
        })
        .unwrap()
    }

    fn proof(
        branch: &str,
        spawn_recorded: bool,
        process_alive: bool,
    ) -> ContinuationReceiverProofWire {
        ContinuationReceiverProofWire {
            branch: branch.to_string(),
            identity: Some("acme--1".to_string()),
            discoverable: process_alive,
            process_alive,
            spawn_recorded,
        }
    }

    #[test]
    fn resume_adoption_preserves_acknowledged_records() {
        let dispatching = dispatch(reserve(pending("continue")));
        let mut ack = request(
            dispatching,
            ContinuationDeliveryDispositionWire::Acknowledged,
        );
        ack.acknowledged_by = Some("acme--1".to_string());
        let acknowledged = transition_continuation_delivery(ack).unwrap();
        let decision = adoption(
            vec![acknowledged.clone()],
            KIND_MANUAL_REVISION,
            Some("manual-recovery-1"),
            None,
            vec![],
        );
        assert_eq!(decision.outcome, OUTCOME_ALREADY_DELIVERED);
        assert!(!decision.admit);
        assert_eq!(decision.preserve_keys, vec![acknowledged.key]);
        assert!(decision.fence_keys.is_empty());
    }

    #[test]
    fn ordinary_resume_does_not_retry_unproven_dispatching() {
        let dispatching = dispatch(reserve(pending("continue")));
        let decision =
            adoption(vec![dispatching.clone()], "ordinary", None, None, vec![]);
        assert_eq!(decision.outcome, OUTCOME_EXISTING_RECEIVER);
        assert!(!decision.admit);
        assert!(decision.fence_keys.is_empty());
        assert_eq!(decision.selected_branch, dispatching.key.branch);
    }

    #[test]
    fn stale_receiver_without_live_process_needs_attention() {
        let mut dispatching = dispatch(reserve(pending("continue")));
        dispatching.workspace_identity = Some("/tmp/work".to_string());
        let decision = adoption(
            vec![dispatching.clone()],
            "ordinary",
            None,
            None,
            vec![proof("failed", true, false)],
        );
        assert_eq!(decision.outcome, OUTCOME_NEEDS_ATTENTION);
        assert!(!decision.admit);
        assert_eq!(decision.fence_keys, vec![dispatching.key]);
        assert_eq!(decision.reason.as_deref(), Some(AMBIGUOUS_RECEIVER_REASON));
    }

    #[test]
    fn manual_revision_fences_unproven_dispatching_and_admits() {
        let dispatching = dispatch(reserve(pending("continue")));
        let decision = adoption(
            vec![dispatching.clone()],
            KIND_MANUAL_REVISION,
            Some("manual-recovery-1"),
            None,
            vec![],
        );
        assert_eq!(decision.outcome, OUTCOME_ADMIT);
        assert!(decision.admit);
        assert_eq!(decision.selected_branch, "manual-recovery-1");
        assert_eq!(decision.fence_keys, vec![dispatching.key]);
    }

    #[test]
    fn empty_records_admit_requested_ordinary_branch() {
        let decision = adoption(vec![], "ordinary", None, None, vec![]);
        assert_eq!(decision.outcome, OUTCOME_ADMIT);
        assert!(decision.admit);
        assert_eq!(decision.selected_branch, "failed");
        assert!(decision.fence_keys.is_empty());
    }

    #[test]
    fn existing_manual_branch_does_not_admit_a_second_spawn() {
        let decision = adoption(
            vec![],
            KIND_MANUAL_REVISION,
            Some("manual-recovery-1"),
            Some("manual-recovery-1"),
            vec![],
        );
        assert_eq!(decision.outcome, OUTCOME_EXISTING_RECEIVER);
        assert!(!decision.admit);
        assert_eq!(decision.selected_branch, "manual-recovery-1");
    }
}
