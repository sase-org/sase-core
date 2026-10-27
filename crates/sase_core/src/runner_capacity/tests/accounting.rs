//! Per-record capacity accounting tests.
//!
//! The screenshot shape from the machine-capacity-transparency plan (three
//! workers, two verify monitors, one explicit-zero monitor) must report 5.0
//! occupied with one accounting entry per record, plus the structural
//! invariants every snapshot upholds.

use super::super::capacity_math::compensated_sum;
use super::super::*;
use super::support::*;

fn entry<'a>(
    snapshot: &'a RunnerCapacitySnapshotWire,
    name: &str,
) -> &'a RunnerCapacityAccountingWire {
    let artifact_dir = format!("/tmp/{name}");
    snapshot
        .accounting
        .iter()
        .find(|entry| entry.artifact_dir == artifact_dir)
        .expect("accounting entry should be present")
}

fn verify_monitor(name: &str, session: &str) -> RunnerCapacityRecordWire {
    let mut monitor = running(name, Some(1.0));
    monitor.agent_session = Some(session.to_string());
    monitor.agent_session_role = Some("monitor".to_string());
    monitor.agent_session_turn_kind = Some("monitor".to_string());
    monitor.agent_session_turn_id = Some(format!("{session}-turn"));
    monitor.pid = Some(99);
    monitor.run_started_at = None;
    monitor.queue_weight_inherited = true;
    monitor
}

fn screenshot_records() -> Vec<RunnerCapacityRecordWire> {
    let mut records = vec![
        running("worker-a", Some(1.0)),
        running("worker-b", Some(1.0)),
        running("worker-c", Some(1.0)),
        verify_monitor("verify-a", "verify-a"),
        verify_monitor("verify-b", "verify-b"),
    ];
    let mut cutover = running("cutover", Some(0.0));
    cutover.agent_session = Some("cutover".to_string());
    cutover.agent_session_role = Some("monitor".to_string());
    cutover.agent_session_turn_kind = Some("monitor".to_string());
    cutover.agent_session_turn_id = Some("cutover-turn".to_string());
    cutover.pid = Some(100);
    cutover.run_started_at = None;
    cutover.queue_weight_explicit = true;
    records.push(cutover);
    records
}

#[test]
fn screenshot_shape_reports_five_occupied_with_six_dispositions() {
    let result = snapshot(8.0, screenshot_records());
    assert_eq!(result.occupied_capacity, 5.0);
    assert_eq!(result.accounting.len(), 6);
    assert!(result.accounting_valid);

    for name in ["worker-a", "worker-b", "worker-c"] {
        let holding = entry(&result, name);
        assert_eq!(holding.state, "holding");
        assert_eq!(holding.charged_weight, 1.0);
        assert_eq!(holding.weight_source, "default");
        assert!(holding.reason_code.is_none());
    }
    for name in ["verify-a", "verify-b"] {
        let holding = entry(&result, name);
        assert_eq!(holding.state, "holding");
        assert_eq!(holding.charged_weight, 1.0);
        assert_eq!(holding.weight_source, "inherited");
        assert!(holding.reason_code.is_none());
    }
    let zero = entry(&result, "cutover");
    assert_eq!(zero.state, "holding");
    assert_eq!(zero.charged_weight, 0.0);
    assert_eq!(zero.weight_source, "explicit_zero");
    assert_eq!(zero.reason_code.as_deref(), Some("explicit_zero_weight"));
}

#[test]
fn accounting_invariants_hold_for_mixed_snapshot() {
    let mut serial_root = running("serial-root", Some(2.0));
    serial_root.agent_session = Some("fam".to_string());
    let mut serial_child = running("serial-child", Some(1.0));
    serial_child.agent_session = Some("fam".to_string());
    serial_child.parent_timestamp = Some("serial-root".to_string());
    let waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
    let mut pending_gate = rec("gate");
    pending_gate.agent_session_role = Some("gate".to_string());
    pending_gate.agent_session_turn_kind = Some("gate".to_string());
    pending_gate.agent_session_turn_id = Some("gate-1".to_string());
    pending_gate.agent_session_turn_state = Some("pending".to_string());
    let mut dead = running("dead", Some(1.0));
    dead.live = false;
    let mut done = running("done", Some(1.0));
    done.has_done_marker = true;
    let mut non_agent = rec("sidecar");
    non_agent.workflow_dir_name = "other".to_string();
    let mut no_pid = verify_monitor("no-pid", "no-pid");
    no_pid.pid = None;
    let mut question = running("question", Some(1.0));
    question.pending_question = true;
    let records = vec![
        serial_root,
        serial_child,
        waiter,
        pending_gate,
        dead,
        done,
        non_agent,
        no_pid,
        question,
        rec("fresh"),
    ];

    let result = snapshot(8.0, records.clone());
    assert!(result.accounting_valid);

    // Every request record appears exactly once.
    assert_eq!(result.accounting.len(), records.len());
    let mut dirs: Vec<&str> = result
        .accounting
        .iter()
        .map(|entry| entry.artifact_dir.as_str())
        .collect();
    dirs.sort_unstable();
    let mut expected: Vec<&str> = records
        .iter()
        .map(|record| record.artifact_dir.as_str())
        .collect();
    expected.sort_unstable();
    assert_eq!(dirs, expected);

    // The compensated charged sum equals occupied capacity.
    let charged = compensated_sum(
        result.accounting.iter().map(|entry| entry.charged_weight),
    );
    assert_eq!(charged, Some(result.occupied_capacity));

    // Claim owner keys of holding entries equal the claim set, with exactly
    // one holder per claim.
    let mut holder_keys: Vec<&str> = result
        .accounting
        .iter()
        .filter(|entry| entry.state == "holding")
        .map(|entry| {
            entry
                .claim_owner_key
                .as_deref()
                .expect("holder has a claim")
        })
        .collect();
    holder_keys.sort_unstable();
    let mut claim_keys: Vec<&str> = result
        .claims
        .iter()
        .map(|claim| claim.owner_key.as_str())
        .collect();
    claim_keys.sort_unstable();
    assert_eq!(holder_keys, claim_keys);

    // Waiting entries equal waiters.
    let mut waiting_dirs: Vec<&str> = result
        .accounting
        .iter()
        .filter(|entry| entry.state == "waiting")
        .map(|entry| entry.artifact_dir.as_str())
        .collect();
    waiting_dirs.sort_unstable();
    let mut waiter_dirs: Vec<&str> = result
        .waiters
        .iter()
        .map(|waiter| waiter.artifact_dir.as_str())
        .collect();
    waiter_dirs.sort_unstable();
    assert_eq!(waiting_dirs, waiter_dirs);

    // Spot-check the dispositions.
    assert_eq!(entry(&result, "serial-root").state, "holding");
    assert_eq!(entry(&result, "serial-root").charged_weight, 2.0);
    let rider = entry(&result, "serial-child");
    assert_eq!(rider.state, "riding_claim");
    assert_eq!(rider.charged_weight, 0.0);
    assert_eq!(
        rider.claim_holder_artifact_dir.as_deref(),
        Some("/tmp/serial-root")
    );
    assert!(rider.reason_code.is_none());
    let waiting_entry = entry(&result, "waiter");
    assert_eq!(waiting_entry.state, "waiting");
    assert_eq!(waiting_entry.reason_code.as_deref(), Some("queued"));
    assert_eq!(
        entry(&result, "gate").reason_code.as_deref(),
        Some("pending_gate")
    );
    assert_eq!(
        entry(&result, "dead").reason_code.as_deref(),
        Some("not_live")
    );
    assert_eq!(entry(&result, "done").reason_code.as_deref(), Some("done"));
    assert_eq!(
        entry(&result, "sidecar").reason_code.as_deref(),
        Some("not_agent_record")
    );
    let unknown = entry(&result, "no-pid");
    assert_eq!(unknown.state, "unknown");
    assert_eq!(unknown.reason_code.as_deref(), Some("no_pid"));
    assert_eq!(
        entry(&result, "question").reason_code.as_deref(),
        Some("pending_question")
    );
    assert_eq!(
        entry(&result, "fresh").reason_code.as_deref(),
        Some("not_started")
    );
}

#[test]
fn invalid_live_claim_weight_reports_unknown_and_invalid_snapshot() {
    let mut bad = running("bad", Some(1.0));
    bad.queue_weight_invalid = true;
    let good = running("good", Some(1.0));

    let result = snapshot(8.0, vec![bad, good]);
    assert!(!result.accounting_valid);
    assert!(result.claims.iter().all(|claim| {
        claim.artifact_dirs.iter().all(|dir| dir != "/tmp/bad")
    }));
    let unknown = entry(&result, "bad");
    assert_eq!(unknown.state, "unknown");
    assert_eq!(unknown.weight_source, "invalid");
    assert_eq!(unknown.effective_weight, None);
    assert_eq!(unknown.reason_code.as_deref(), Some("invalid_weight"));
    assert_eq!(entry(&result, "good").state, "holding");
}

#[test]
fn claim_holder_election_breaks_weight_ties_lexicographically() {
    let mut first = running("b-member", Some(1.0));
    first.agent_session = Some("fam".to_string());
    let mut second = running("a-member", Some(1.0));
    second.agent_session = Some("fam".to_string());

    let result = snapshot(8.0, vec![first, second]);
    assert_eq!(result.claims.len(), 1);
    assert_eq!(result.occupied_capacity, 1.0);
    let holder = entry(&result, "a-member");
    assert_eq!(holder.state, "holding");
    let rider = entry(&result, "b-member");
    assert_eq!(rider.state, "riding_claim");
    assert_eq!(
        rider.claim_holder_artifact_dir.as_deref(),
        Some("/tmp/a-member")
    );
}

#[test]
fn candidate_is_excluded_from_accounting() {
    let root = running("root", Some(1.0));
    let candidate = waiting("newcomer", "2026-09-10T00:00:01Z", Some(1.0));

    let result = snapshot_with_candidate(8.0, vec![root], candidate);
    assert_eq!(result.accounting.len(), 1);
    assert_eq!(result.accounting[0].artifact_dir, "/tmp/root");
    assert!(result.candidate_decision.is_some());
}
