use super::*;

fn request() -> GateFollowupDecisionRequestWire {
    GateFollowupDecisionRequestWire {
        schema_version: GATE_FOLLOWUP_WIRE_SCHEMA_VERSION,
        mode: MODE_DIAGNOSE.to_string(),
        gate_id: "c117f874-83de-4840-8405-58a8dc1efd66".to_string(),
        gate_kind: Some("plan".to_string()),
        gate_state: "answered".to_string(),
        already_settled: true,
        request_fingerprint: Some("sha256:plan-approve-commit".to_string()),
        followup_requested: true,
        creator_live: false,
        auto_suppressed: false,
        followup_outcome: None,
        followup_agent: None,
        followup_error: None,
        followup_degraded_reason: None,
        followup_prompt_path: None,
        attempt: None,
        successor_evidence: None,
    }
}

#[test]
fn attempt_id_is_stable_for_the_same_gate_and_fingerprint() {
    let first = gate_followup_attempt_id("gate-1", "fp-a");
    let second = gate_followup_attempt_id("gate-1", "fp-a");
    let other = gate_followup_attempt_id("gate-1", "fp-b");
    assert_eq!(first, second);
    assert_ne!(first, other);
    assert_eq!(first.len(), 64);
}

#[test]
fn incident_legacy_terminal_metadata_is_interrupted() {
    let verdict = decide_gate_followup(&request()).unwrap();
    assert_eq!(verdict.disposition, DISPOSITION_INTERRUPTED);
    assert_eq!(verdict.recovery, RECOVERY_RESUME);
    assert!(verdict.needs_attention);
    assert!(verdict.resume_eligible);
    assert!(!verdict.launch_allowed);
}

#[test]
fn explicit_resume_of_the_incident_may_launch() {
    let mut input = request();
    input.mode = MODE_RESUME.to_string();
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.disposition, DISPOSITION_INTERRUPTED);
    assert_eq!(verdict.recovery, RECOVERY_RESUME);
    assert!(verdict.launch_allowed);
    assert!(verdict.resume_eligible);
    assert!(!verdict.needs_attention);
}

#[test]
fn first_pass_settle_may_launch_after_publishing_terminal_state() {
    let mut input = request();
    input.mode = MODE_SETTLE.to_string();
    input.already_settled = false;
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.recovery, RECOVERY_RESUME);
    assert!(verdict.launch_allowed);
    assert!(!verdict.needs_attention);
}

#[test]
fn later_settle_of_a_terminal_gate_does_not_auto_launch() {
    let mut input = request();
    input.mode = MODE_SETTLE.to_string();
    input.already_settled = true;
    let verdict = decide_gate_followup(&input).unwrap();
    assert!(!verdict.launch_allowed);
    assert!(verdict.needs_attention);
    assert!(verdict.resume_eligible);
}

#[test]
fn intentional_none_is_not_a_failed_launch() {
    let mut input = request();
    input.followup_requested = false;
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.disposition, DISPOSITION_INTENTIONAL_NONE);
    assert_eq!(verdict.recovery, RECOVERY_NOOP);
    assert!(!verdict.needs_attention);
    assert!(!verdict.launch_allowed);
    assert!(!verdict.resume_eligible);
}

#[test]
fn suppressed_auto_followup_does_not_launch() {
    let mut input = request();
    input.auto_suppressed = true;
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.disposition, DISPOSITION_SUPPRESSED);
    assert_eq!(verdict.persist_outcome.as_deref(), Some(OUTCOME_SUPPRESSED));
    assert_eq!(verdict.recovery, RECOVERY_NOOP);
    assert!(!verdict.launch_allowed);
}

#[test]
fn recorded_suppressed_outcome_is_idempotent() {
    let mut input = request();
    input.followup_outcome = Some(OUTCOME_SUPPRESSED.to_string());
    input.mode = MODE_RESUME.to_string();
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.disposition, DISPOSITION_SUPPRESSED);
    assert_eq!(verdict.recovery, RECOVERY_NOOP);
    assert!(!verdict.launch_allowed);
}

#[test]
fn launched_receipt_is_a_noop() {
    let mut input = request();
    input.followup_outcome = Some(OUTCOME_LAUNCHED.to_string());
    input.followup_agent = Some("0j8.f0.f2--code".to_string());
    input.mode = MODE_RESUME.to_string();
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.disposition, DISPOSITION_LAUNCHED);
    assert_eq!(verdict.recovery, RECOVERY_NOOP);
    assert!(!verdict.launch_allowed);
    assert!(!verdict.needs_attention);
}

#[test]
fn existing_running_successor_without_receipt_is_adopted() {
    let mut input = request();
    input.successor_evidence = Some(GateFollowupSuccessorEvidenceWire {
        family_name: Some("0j8.f0.f2".to_string()),
        expected_suffix: Some("--code".to_string()),
        attached_agent: Some("0j8.f0.f2--code".to_string()),
        running: true,
        ..Default::default()
    });
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.disposition, DISPOSITION_LAUNCHED);
    assert_eq!(verdict.recovery, RECOVERY_ADOPT);
    assert_eq!(verdict.adopt_agent.as_deref(), Some("0j8.f0.f2--code"));
    assert!(!verdict.launch_allowed);
}

#[test]
fn completed_successor_still_counts() {
    let mut input = request();
    input.successor_evidence = Some(GateFollowupSuccessorEvidenceWire {
        attached_agent: Some("0j8.f0.f2--code".to_string()),
        completed: true,
        ..Default::default()
    });
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.recovery, RECOVERY_ADOPT);
    assert!(!verdict.launch_allowed);
}

#[test]
fn ambiguous_successor_never_launches() {
    let mut input = request();
    input.mode = MODE_RESUME.to_string();
    input.successor_evidence = Some(GateFollowupSuccessorEvidenceWire {
        ambiguous: true,
        running: true,
        attached_agent: Some("maybe--code".to_string()),
        ..Default::default()
    });
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.disposition, DISPOSITION_AMBIGUOUS);
    assert_eq!(verdict.recovery, RECOVERY_REPORT_AMBIGUOUS);
    assert!(!verdict.launch_allowed);
    assert!(verdict.needs_attention);
}

#[test]
fn live_attempt_is_never_stolen() {
    let mut input = request();
    input.mode = MODE_RESUME.to_string();
    input.attempt = Some(GateFollowupAttemptWire {
        attempt_id: "att-1".to_string(),
        fingerprint: Some("sha256:plan-approve-commit".to_string()),
        stage: Some(ATTEMPT_STAGE_LAUNCHING.to_string()),
        live: true,
        owner_pid: Some(4242),
        started_at: None,
        error_stage: None,
        error_type: None,
        error_message: None,
    });
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.disposition, DISPOSITION_IN_PROGRESS);
    assert_eq!(verdict.recovery, RECOVERY_WAIT);
    assert!(!verdict.launch_allowed);
}

#[test]
fn stale_attempt_can_resume_after_ruling_out_a_successor() {
    let mut input = request();
    input.mode = MODE_RESUME.to_string();
    input.attempt = Some(GateFollowupAttemptWire {
        attempt_id: "att-1".to_string(),
        fingerprint: Some("sha256:plan-approve-commit".to_string()),
        stage: Some(ATTEMPT_STAGE_PREPARING.to_string()),
        live: false,
        owner_pid: Some(4242),
        started_at: None,
        error_stage: Some("preparing".to_string()),
        error_type: Some("RuntimeError".to_string()),
        error_message: Some("boom".to_string()),
    });
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.disposition, DISPOSITION_INTERRUPTED);
    assert!(verdict.launch_allowed);
    assert!(verdict
        .diagnostics
        .iter()
        .any(|item| item.contains("RuntimeError")));
}

#[test]
fn recorded_failure_is_resume_eligible() {
    let mut input = request();
    input.followup_outcome = Some(OUTCOME_FAILED.to_string());
    input.followup_error = Some("TypeError: missing model".to_string());
    let diagnose = decide_gate_followup(&input).unwrap();
    assert_eq!(diagnose.disposition, DISPOSITION_FAILED);
    assert!(diagnose.needs_attention);
    assert!(!diagnose.launch_allowed);

    input.mode = MODE_RESUME.to_string();
    let resume = decide_gate_followup(&input).unwrap();
    assert!(resume.launch_allowed);
    assert_eq!(resume.recovery, RECOVERY_RESUME);
}

#[test]
fn not_launchable_stays_not_launchable() {
    let mut input = request();
    input.followup_outcome = Some(OUTCOME_NOT_LAUNCHABLE.to_string());
    input.followup_error =
        Some("could not resolve the gate shell's project".to_string());
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.disposition, DISPOSITION_NOT_LAUNCHABLE);
    assert_eq!(
        verdict.persist_outcome.as_deref(),
        Some(OUTCOME_NOT_LAUNCHABLE)
    );
}

#[test]
fn mismatched_attempt_fingerprint_is_ignored() {
    let mut input = request();
    input.mode = MODE_RESUME.to_string();
    input.attempt = Some(GateFollowupAttemptWire {
        attempt_id: "att-old".to_string(),
        fingerprint: Some("sha256:other".to_string()),
        stage: Some(ATTEMPT_STAGE_LAUNCHING.to_string()),
        live: true,
        owner_pid: Some(1),
        started_at: None,
        error_stage: None,
        error_type: None,
        error_message: None,
    });
    let verdict = decide_gate_followup(&input).unwrap();
    assert_eq!(verdict.disposition, DISPOSITION_INTERRUPTED);
    assert!(verdict.launch_allowed);
}

#[test]
fn schema_mismatch_and_invalid_mode_are_structural_errors() {
    let mut input = request();
    input.schema_version = 99;
    let error = decide_gate_followup(&input).unwrap_err();
    assert_eq!(error.code, "schema_version_mismatch");

    input.schema_version = GATE_FOLLOWUP_WIRE_SCHEMA_VERSION;
    input.mode = "replay".to_string();
    let error = decide_gate_followup(&input).unwrap_err();
    assert_eq!(error.code, "invalid_mode");
}
