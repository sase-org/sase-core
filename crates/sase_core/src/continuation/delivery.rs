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

use serde::{Deserialize, Serialize};

use super::schema::{
    validate_continuation_delivery_record, validate_non_empty_text,
    validate_optional_reference, validate_schema,
    ContinuationDeliveryAttemptWire, ContinuationDeliveryDispositionWire,
    ContinuationDeliveryKeyWire, ContinuationDeliveryRecordWire,
    ContinuationError, CONTINUATION_WIRE_SCHEMA_VERSION, MAX_REF_BYTES,
    MAX_TEXT_BYTES,
};

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
}
