use super::wire::{
    GateFollowupAttemptWire, GateFollowupDecisionRequestWire,
    GateFollowupDecisionWire, GateFollowupError,
    GateFollowupSuccessorEvidenceWire, ATTEMPT_STAGE_FAILED,
    DISPOSITION_AMBIGUOUS, DISPOSITION_FAILED, DISPOSITION_INTENTIONAL_NONE,
    DISPOSITION_INTERRUPTED, DISPOSITION_IN_PROGRESS, DISPOSITION_LAUNCHED,
    DISPOSITION_LAUNCHED_DEGRADED, DISPOSITION_NOT_LAUNCHABLE,
    DISPOSITION_SUPPRESSED, GATE_FOLLOWUP_WIRE_SCHEMA_VERSION, MODE_DIAGNOSE,
    MODE_RESUME, MODE_SETTLE, OUTCOME_FAILED, OUTCOME_LAUNCHED,
    OUTCOME_LAUNCHED_DEGRADED, OUTCOME_NOT_LAUNCHABLE, OUTCOME_SUPPRESSED,
    RECOVERY_ADOPT, RECOVERY_NOOP, RECOVERY_REPORT_AMBIGUOUS, RECOVERY_RESUME,
    RECOVERY_WAIT,
};
use sha2::{Digest, Sha256};

const TERMINAL_GATE_STATES: [&str; 6] = [
    "answered",
    "completed",
    "failed",
    "timeout",
    "stopped",
    "lost",
];

/// Return a stable attempt identity for one gate and request fingerprint.
pub fn gate_followup_attempt_id(gate_id: &str, fingerprint: &str) -> String {
    let mut material =
        Vec::with_capacity(gate_id.len() + fingerprint.len() + 1);
    material.extend_from_slice(gate_id.as_bytes());
    material.push(0);
    material.extend_from_slice(fingerprint.as_bytes());
    hex::encode(Sha256::digest(&material))
}

/// Classify one gate's follow-up disposition and recovery action.
///
/// This operation performs no filesystem, clock, or process I/O.
pub fn decide_gate_followup(
    request: &GateFollowupDecisionRequestWire,
) -> Result<GateFollowupDecisionWire, GateFollowupError> {
    validate_request(request)?;

    if let Some(decision) = decide_ambiguous(request) {
        return Ok(decision);
    }
    if let Some(decision) = decide_existing_successor(request) {
        return Ok(decision);
    }
    if let Some(decision) = decide_live_attempt(request) {
        return Ok(decision);
    }
    if let Some(decision) = decide_suppressed(request) {
        return Ok(decision);
    }
    if let Some(decision) = decide_confirmed_outcome(request) {
        return Ok(decision);
    }
    if !request.followup_requested {
        return Ok(decision(
            DISPOSITION_INTENTIONAL_NONE,
            RECOVERY_NOOP,
            None,
            None,
            false,
            false,
            false,
            "no follow-up was requested for this settled branch",
            Vec::new(),
        ));
    }
    if let Some(decision) = decide_recorded_failure(request) {
        return Ok(decision);
    }
    Ok(decide_missing_disposition(request))
}

fn validate_request(
    request: &GateFollowupDecisionRequestWire,
) -> Result<(), GateFollowupError> {
    if request.schema_version != GATE_FOLLOWUP_WIRE_SCHEMA_VERSION {
        return Err(GateFollowupError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!(
                "got {}, expected {GATE_FOLLOWUP_WIRE_SCHEMA_VERSION}",
                request.schema_version
            ),
        ));
    }
    if request.gate_id.trim().is_empty() {
        return Err(GateFollowupError::new(
            "blank_value",
            "$.gate_id",
            "gate_id must not be blank",
        ));
    }
    if request.gate_state.trim().is_empty() {
        return Err(GateFollowupError::new(
            "blank_value",
            "$.gate_state",
            "gate_state must not be blank",
        ));
    }
    match request.mode.as_str() {
        MODE_SETTLE | MODE_RESUME | MODE_DIAGNOSE => Ok(()),
        other => Err(GateFollowupError::new(
            "invalid_mode",
            "$.mode",
            format!("mode must be settle, resume, or diagnose; got {other:?}"),
        )),
    }
}

fn decide_ambiguous(
    request: &GateFollowupDecisionRequestWire,
) -> Option<GateFollowupDecisionWire> {
    let evidence = request.successor_evidence.as_ref()?;
    if !evidence.ambiguous {
        return None;
    }
    Some(decision(
        DISPOSITION_AMBIGUOUS,
        RECOVERY_REPORT_AMBIGUOUS,
        None,
        None,
        true,
        false,
        false,
        "successor attachment is ambiguous; refusing to start a second provider",
        diagnostics(request),
    ))
}

fn decide_existing_successor(
    request: &GateFollowupDecisionRequestWire,
) -> Option<GateFollowupDecisionWire> {
    let evidence = request.successor_evidence.as_ref()?;
    if evidence.ambiguous || !(evidence.running || evidence.completed) {
        return None;
    }
    let agent = first_present(&[
        evidence.attached_agent.as_deref(),
        request.followup_agent.as_deref(),
        evidence.launch_receipt.as_deref(),
    ]);
    let persist = persist_launched_outcome(request.followup_outcome.as_deref());
    let receipt_present = request
        .followup_agent
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
        || matches!(
            request.followup_outcome.as_deref(),
            Some(OUTCOME_LAUNCHED | OUTCOME_LAUNCHED_DEGRADED)
        );
    if receipt_present {
        Some(decision(
            persist.unwrap_or(DISPOSITION_LAUNCHED),
            RECOVERY_NOOP,
            persist.map(str::to_string),
            agent.map(str::to_string),
            false,
            false,
            false,
            "an existing successor already matches this gate handoff",
            diagnostics(request),
        ))
    } else {
        Some(decision(
            DISPOSITION_LAUNCHED,
            RECOVERY_ADOPT,
            Some(OUTCOME_LAUNCHED.to_string()),
            agent.map(str::to_string),
            false,
            false,
            false,
            "a successor already started; adopt it and repair the launch receipt",
            diagnostics(request),
        ))
    }
}

fn decide_live_attempt(
    request: &GateFollowupDecisionRequestWire,
) -> Option<GateFollowupDecisionWire> {
    let attempt = matching_attempt(request)?;
    if !attempt.live {
        return None;
    }
    Some(decision(
        DISPOSITION_IN_PROGRESS,
        RECOVERY_WAIT,
        None,
        None,
        false,
        false,
        false,
        "a live handoff attempt still owns this gate",
        diagnostics(request),
    ))
}

fn decide_suppressed(
    request: &GateFollowupDecisionRequestWire,
) -> Option<GateFollowupDecisionWire> {
    let suppressed_outcome =
        request.followup_outcome.as_deref() == Some(OUTCOME_SUPPRESSED);
    if !(request.creator_live || request.auto_suppressed || suppressed_outcome)
    {
        return None;
    }
    if !(request.followup_requested || suppressed_outcome) {
        return None;
    }
    Some(decision(
        DISPOSITION_SUPPRESSED,
        RECOVERY_NOOP,
        Some(OUTCOME_SUPPRESSED.to_string()),
        None,
        false,
        false,
        false,
        "follow-up is suppressed in-process and must not launch a detached coder",
        diagnostics(request),
    ))
}

fn decide_confirmed_outcome(
    request: &GateFollowupDecisionRequestWire,
) -> Option<GateFollowupDecisionWire> {
    match request.followup_outcome.as_deref() {
        Some(OUTCOME_LAUNCHED) => Some(launched_noop(
            DISPOSITION_LAUNCHED,
            OUTCOME_LAUNCHED,
            request,
        )),
        Some(OUTCOME_LAUNCHED_DEGRADED) => Some(launched_noop(
            DISPOSITION_LAUNCHED_DEGRADED,
            OUTCOME_LAUNCHED_DEGRADED,
            request,
        )),
        _ => None,
    }
}

fn launched_noop(
    disposition: &str,
    outcome: &str,
    request: &GateFollowupDecisionRequestWire,
) -> GateFollowupDecisionWire {
    decision(
        disposition,
        RECOVERY_NOOP,
        Some(outcome.to_string()),
        request.followup_agent.clone(),
        false,
        false,
        false,
        "follow-up launch receipt is already recorded",
        diagnostics(request),
    )
}

fn decide_recorded_failure(
    request: &GateFollowupDecisionRequestWire,
) -> Option<GateFollowupDecisionWire> {
    let outcome = request.followup_outcome.as_deref();
    let attempt_failed = matching_attempt(request)
        .and_then(|attempt| attempt.stage.as_deref())
        == Some(ATTEMPT_STAGE_FAILED);
    let failed = matches!(outcome, Some(OUTCOME_FAILED)) || attempt_failed;
    let not_launchable = outcome == Some(OUTCOME_NOT_LAUNCHABLE);
    if !(failed || not_launchable) {
        return None;
    }
    let disposition = if not_launchable && !failed {
        DISPOSITION_NOT_LAUNCHABLE
    } else {
        DISPOSITION_FAILED
    };
    let persist = if not_launchable && !failed {
        OUTCOME_NOT_LAUNCHABLE
    } else {
        outcome.unwrap_or(OUTCOME_FAILED)
    };
    Some(failure_or_resume(
        request,
        disposition,
        persist,
        "recorded follow-up failure remains recoverable",
    ))
}

fn decide_missing_disposition(
    request: &GateFollowupDecisionRequestWire,
) -> GateFollowupDecisionWire {
    let stale_attempt =
        matching_attempt(request).is_some_and(|attempt| !attempt.live);
    let reason = if stale_attempt {
        "a stale handoff attempt can be resumed after ruling out a launched successor"
    } else if is_terminal(request.gate_state.as_str()) {
        "requested follow-up has no recorded disposition after terminal settlement"
    } else {
        "follow-up was requested and has not been launched yet"
    };
    failure_or_resume(request, DISPOSITION_INTERRUPTED, OUTCOME_FAILED, reason)
}

fn failure_or_resume(
    request: &GateFollowupDecisionRequestWire,
    disposition: &str,
    persist: &str,
    reason: &str,
) -> GateFollowupDecisionWire {
    let launch_allowed = request.mode == MODE_RESUME
        || (request.mode == MODE_SETTLE && !request.already_settled);
    decision(
        disposition,
        RECOVERY_RESUME,
        Some(persist.to_string()),
        None,
        !launch_allowed,
        true,
        launch_allowed,
        reason,
        diagnostics(request),
    )
}

fn matching_attempt(
    request: &GateFollowupDecisionRequestWire,
) -> Option<&GateFollowupAttemptWire> {
    let attempt = request.attempt.as_ref()?;
    if fingerprints_conflict(
        request.request_fingerprint.as_deref(),
        attempt.fingerprint.as_deref(),
    ) {
        return None;
    }
    Some(attempt)
}

fn fingerprints_conflict(left: Option<&str>, right: Option<&str>) -> bool {
    match (nonzero(left), nonzero(right)) {
        (Some(left), Some(right)) => left != right,
        _ => false,
    }
}

fn persist_launched_outcome(outcome: Option<&str>) -> Option<&str> {
    match outcome {
        Some(OUTCOME_LAUNCHED_DEGRADED) => Some(OUTCOME_LAUNCHED_DEGRADED),
        Some(OUTCOME_LAUNCHED) | None => Some(OUTCOME_LAUNCHED),
        _ => Some(OUTCOME_LAUNCHED),
    }
}

fn is_terminal(gate_state: &str) -> bool {
    TERMINAL_GATE_STATES.contains(&gate_state)
}

fn nonzero(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn first_present<'a>(values: &[Option<&'a str>]) -> Option<&'a str> {
    values.iter().copied().find_map(nonzero)
}

fn diagnostics(request: &GateFollowupDecisionRequestWire) -> Vec<String> {
    let mut items = Vec::new();
    if let Some(error) = nonzero(request.followup_error.as_deref()) {
        items.push(format!("followup_error={error}"));
    }
    if let Some(reason) = nonzero(request.followup_degraded_reason.as_deref()) {
        items.push(format!("followup_degraded_reason={reason}"));
    }
    if let Some(attempt) = request.attempt.as_ref() {
        if let Some(stage) = nonzero(attempt.stage.as_deref()) {
            items.push(format!("attempt_stage={stage}"));
        }
        if let Some(error_type) = nonzero(attempt.error_type.as_deref()) {
            items.push(format!("attempt_error_type={error_type}"));
        }
        if let Some(message) = nonzero(attempt.error_message.as_deref()) {
            items.push(format!("attempt_error_message={message}"));
        }
    }
    if let Some(evidence) = request.successor_evidence.as_ref() {
        push_evidence_diagnostics(&mut items, evidence);
    }
    items
}

fn push_evidence_diagnostics(
    items: &mut Vec<String>,
    evidence: &GateFollowupSuccessorEvidenceWire,
) {
    if let Some(agent) = nonzero(evidence.attached_agent.as_deref()) {
        items.push(format!("attached_agent={agent}"));
    }
    if evidence.running {
        items.push("successor_running".to_string());
    }
    if evidence.completed {
        items.push("successor_completed".to_string());
    }
}

#[allow(clippy::too_many_arguments)]
fn decision(
    disposition: &str,
    recovery: &str,
    persist_outcome: Option<String>,
    adopt_agent: Option<String>,
    needs_attention: bool,
    resume_eligible: bool,
    launch_allowed: bool,
    reason: &str,
    diagnostics: Vec<String>,
) -> GateFollowupDecisionWire {
    GateFollowupDecisionWire {
        schema_version: GATE_FOLLOWUP_WIRE_SCHEMA_VERSION,
        disposition: disposition.to_string(),
        recovery: recovery.to_string(),
        persist_outcome,
        adopt_agent,
        needs_attention,
        resume_eligible,
        launch_allowed,
        reason: reason.to_string(),
        diagnostics,
    }
}
