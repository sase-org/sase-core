//! Capacity budget, weight, and overflow tests.

use super::super::capacity_math::capacity_fits;
use super::super::*;
use super::support::*;

#[test]
fn default_weights_match_serial_and_parallel_lane_counting() {
    let standalone = running("standalone", None);
    let mut serial_root = running("serial-root", Some(2.0));
    serial_root.agent_session = Some("fam".to_string());
    let mut serial_child = running("serial-child", Some(1.0));
    serial_child.agent_session = Some("fam".to_string());
    serial_child.parent_timestamp = Some("serial-root".to_string());
    let mut parallel = running("parallel", Some(0.25));
    parallel.agent_session = Some("fam".to_string());
    parallel.agent_session_parallel = true;
    let mut pending_question = running("question", Some(4.0));
    pending_question.pending_question = true;
    let mut pending_gate = running("gate", Some(4.0));
    pending_gate.agent_session_role = Some("gate".to_string());
    pending_gate.agent_session_turn_kind = Some("gate".to_string());
    pending_gate.agent_session_turn_id = Some("gate-1".to_string());
    pending_gate.agent_session_turn_state = Some("pending".to_string());
    let mut hidden_workflow = running("workflow", Some(4.0));
    hidden_workflow.workflow_dir_name = "workflow-build".to_string();

    let result = snapshot(
        8.0,
        vec![
            standalone,
            serial_root,
            serial_child,
            parallel,
            pending_question,
            pending_gate,
            hidden_workflow,
        ],
    );
    assert_eq!(result.occupied_lanes, 3);
    assert_eq!(result.occupied_capacity, 3.25);
    assert_eq!(
        result
            .claims
            .iter()
            .map(|claim| claim.claim_kind.as_str())
            .collect::<Vec<_>>(),
        ["serial_session", "parallel_member", "standalone"]
    );
}

#[test]
fn explicit_capacity_cannot_bypass_global_budget() {
    let occupied = running("occupied", Some(1.0));
    let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
    waiter.queue_capacity = Some(99);
    waiter.queue_capacity_explicit = true;

    let result = snapshot(1.0, vec![occupied, waiter]);
    assert!(result.first_eligible_artifact_dir.is_none());
    assert_eq!(result.waiters[0].blockers[0].code, "insufficient-capacity");
    assert_eq!(result.waiters[0].wait_capacity_shortfall, 0.0);
}

#[test]
fn capacity_budget_can_exceed_global_limit() {
    let flags = capacity_budget_flags();
    let occupied = running("occupied", Some(1.0));
    let mut waiting_agent =
        waiting("waiter", "2026-09-10T00:00:00Z", Some(1.0));
    waiting_agent.queue_capacity = Some(100);
    waiting_agent.queue_capacity_explicit = true;

    let result =
        snapshot_with_flags(1.0, vec![occupied, waiting_agent], &flags);
    assert_eq!(
        result.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/waiter")
    );
    assert_eq!(result.occupied_capacity, 1.0);
    assert_eq!(result.effective_limit, 1.0);
    let waiter = waiter(&result, "waiter");
    assert!(waiter.eligible);
    assert_eq!(waiter.admission_limit, 100.0);
    assert_eq!(waiter.capacity_shortfall, 0.0);
    assert_eq!(waiter.wait_capacity_shortfall, 0.0);
}

#[test]
fn capacity_budget_below_global_limit_is_strict() {
    let flags = capacity_budget_flags();
    let occupied = running("occupied", Some(1.0));
    let mut waiting_agent =
        waiting("waiter", "2026-09-10T00:00:00Z", Some(1.0));
    waiting_agent.queue_capacity = Some(1);
    waiting_agent.queue_capacity_explicit = true;

    let result =
        snapshot_with_flags(8.0, vec![occupied, waiting_agent], &flags);
    assert!(result.first_eligible_artifact_dir.is_none());
    let waiter = waiter(&result, "waiter");
    assert!(!waiter.eligible);
    assert_eq!(waiter.admission_limit, 1.0);
    assert_eq!(waiter.capacity_shortfall, 1.0);
    assert_eq!(waiter.wait_capacity_shortfall, 0.0);
    assert_eq!(waiter.blockers[0].code, "insufficient-capacity");
    assert_eq!(waiter.blockers[0].admission_limit, Some(1.0));
}

#[test]
fn four_light_claims_satisfy_capacity_one() {
    let mut records: Vec<RunnerCapacityRecordWire> = (0..4)
        .map(|index| running(&format!("light{index}"), Some(0.25)))
        .collect();
    let mut waiting_agent =
        waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
    waiting_agent.queue_capacity = Some(1);
    waiting_agent.queue_capacity_explicit = true;
    records.push(waiting_agent);

    let result = snapshot(8.0, records);
    assert_eq!(result.occupied_capacity, 1.0);
    assert_eq!(result.occupied_lanes, 4);
    assert_eq!(
        result.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/waiter")
    );
    assert!(waiter(&result, "waiter").eligible);
    assert_eq!(waiter(&result, "waiter").wait_capacity_shortfall, 0.0);
}

#[test]
fn one_heavy_claim_exceeds_capacity_one() {
    let occupied = running("heavy", Some(2.0));
    let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
    waiter.queue_capacity = Some(1);
    waiter.queue_capacity_explicit = true;

    let result = snapshot(8.0, vec![occupied, waiter]);
    assert!(result.first_eligible_artifact_dir.is_none());
    assert_eq!(result.waiters[0].blockers[0].code, "capacity-condition");
    assert_eq!(result.waiters[0].blockers[0].occupied_capacity, Some(2.0));
    assert_eq!(result.waiters[0].blockers[0].capacity_threshold, Some(1));
    assert_eq!(result.waiters[0].wait_capacity_shortfall, 1.0);
}

#[test]
fn capacity_zero_is_true_drain_including_tiny_weight() {
    let occupied = running("tiny", Some(f64::MIN_POSITIVE));
    let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
    waiter.queue_capacity = Some(0);
    waiter.queue_capacity_explicit = true;

    let blocked = snapshot(8.0, vec![occupied, waiter.clone()]);
    assert!(blocked.first_eligible_artifact_dir.is_none());
    assert_eq!(blocked.waiters[0].blockers[0].code, "capacity-condition");
    assert!(blocked.waiters[0].wait_capacity_shortfall > 0.0);

    occupied_drain_admits(waiter);
}

#[test]
fn capacity_budget_one_is_drain_barrier() {
    let flags = capacity_budget_flags();
    let occupied = running("occupied", Some(0.25));
    let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", None);
    waiter.queue_capacity = Some(1);
    waiter.queue_capacity_explicit = true;

    let blocked =
        snapshot_with_flags(8.0, vec![occupied, waiter.clone()], &flags);
    assert!(blocked.first_eligible_artifact_dir.is_none());
    assert_eq!(blocked.waiters[0].blockers[0].code, "insufficient-capacity");
    assert_eq!(blocked.waiters[0].admission_limit, 1.0);

    let drained = snapshot_with_flags(8.0, vec![waiter], &flags);
    assert_eq!(
        drained.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/waiter")
    );
    assert!(drained.waiters[0].eligible);
}

#[test]
fn zero_weight_capacity_budget_waiter_needs_free_capacity() {
    let flags = capacity_budget_flags();
    let occupied = running("occupied", Some(1.0));
    let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.0));
    waiter.queue_weight_explicit = true;
    waiter.queue_capacity = Some(1);
    waiter.queue_capacity_explicit = true;

    let blocked =
        snapshot_with_flags(8.0, vec![occupied, waiter.clone()], &flags);
    assert!(blocked.first_eligible_artifact_dir.is_none());
    assert_eq!(blocked.waiters[0].blockers[0].code, "insufficient-capacity");

    let drained = snapshot_with_flags(8.0, vec![waiter], &flags);
    assert_eq!(
        drained.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/waiter")
    );
    assert!(drained.waiters[0].eligible);
}

#[test]
fn legacy_capacity_zero_translates_under_capacity_budget() {
    let flags = capacity_budget_flags();
    let occupied = running("occupied", Some(0.1));
    let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
    waiter.queue_capacity = Some(0);
    waiter.queue_capacity_explicit = true;

    let blocked =
        snapshot_with_flags(8.0, vec![occupied, waiter.clone()], &flags);
    assert!(blocked.first_eligible_artifact_dir.is_none());
    assert_eq!(blocked.waiters[0].admission_limit, 0.25);
    assert_eq!(blocked.waiters[0].blockers[0].code, "insufficient-capacity");
    assert_eq!(blocked.waiters[0].wait_capacity_shortfall, 0.0);
    assert!(blocked
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "legacy-capacity-zero"));

    let drained = snapshot_with_flags(8.0, vec![waiter], &flags);
    assert_eq!(
        drained.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/waiter")
    );
    assert!(drained.waiters[0].eligible);
    assert_eq!(drained.waiters[0].admission_limit, 0.25);
}

#[test]
fn legacy_capacity_zero_with_zero_weight_is_a_true_drain_barrier() {
    let flags = capacity_budget_flags();
    let occupied = running("occupied", Some(f64::MIN_POSITIVE));
    let mut waiting_agent =
        waiting("waiter", "2026-09-10T00:00:00Z", Some(0.0));
    waiting_agent.queue_weight_explicit = true;
    waiting_agent.queue_capacity = Some(0);
    waiting_agent.queue_capacity_explicit = true;

    let blocked =
        snapshot_with_flags(8.0, vec![occupied, waiting_agent.clone()], &flags);
    let blocked_waiter = waiter(&blocked, "waiter");
    assert!(!blocked_waiter.eligible);
    assert_eq!(blocked_waiter.admission_limit, 0.0);
    assert_eq!(blocked_waiter.blockers[0].code, "insufficient-capacity");
    assert!(blocked_waiter
        .blockers
        .iter()
        .all(|blocker| blocker.code != "invalid-capacity-limit"));
    assert!(blocked
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "legacy-capacity-zero"));

    let drained = snapshot_with_flags(8.0, vec![waiting_agent], &flags);
    assert_eq!(
        drained.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/waiter")
    );
    assert!(drained.waiters[0].eligible);
    assert_eq!(drained.waiters[0].admission_limit, 0.0);
    assert!(drained.waiters[0].blockers.is_empty());
}

fn occupied_drain_admits(mut waiter: RunnerCapacityRecordWire) {
    waiter.queue_capacity = Some(0);
    waiter.queue_capacity_explicit = true;
    let drained = snapshot(8.0, vec![waiter]);
    assert_eq!(
        drained.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/waiter")
    );
    assert!(drained.waiters[0].eligible);
    assert_eq!(drained.waiters[0].wait_capacity_shortfall, 0.0);
}

#[test]
fn proposed_total_allows_repeated_decimal_boundary() {
    let mut records = Vec::new();
    for index in 0..99 {
        records.push(running(&format!("r{index}"), Some(0.1)));
    }
    records.push(waiting("next", "2026-09-10T00:00:00Z", Some(0.1)));

    let result = snapshot(10.0, records);
    assert!(capacity_fits(result.occupied_capacity, 10.0));
    assert_eq!(
        result.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/next")
    );
    assert!(waiter(&result, "next").eligible);
}

#[test]
fn heterogeneous_and_meaningful_boundaries_use_proposed_total() {
    let result = snapshot(
        1.0,
        vec![
            running("a", Some(0.1)),
            running("b", Some(0.2)),
            running("c", Some(0.3)),
            waiting("fits", "2026-09-10T00:00:00Z", Some(0.4)),
        ],
    );
    assert_eq!(
        result.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/fits")
    );

    let under = snapshot(
        1.0,
        vec![
            running("held", Some(0.6)),
            waiting("under", "2026-09-10T00:00:00Z", Some(0.399999999999)),
        ],
    );
    assert!(waiter(&under, "under").eligible);

    let over = snapshot(
        1.0,
        vec![
            running("held", Some(0.6)),
            waiting("over", "2026-09-10T00:00:00Z", Some(0.400000000001)),
        ],
    );
    assert_eq!(
        waiter(&over, "over").blockers[0].code,
        "insufficient-capacity"
    );
}

#[test]
fn tiny_positive_weights_are_valid_but_still_accounted() {
    let tiny = snapshot(
        f64::MIN_POSITIVE,
        vec![waiting(
            "tiny",
            "2026-09-10T00:00:00Z",
            Some(f64::MIN_POSITIVE),
        )],
    );
    assert!(waiter(&tiny, "tiny").eligible);

    let over = snapshot(
        f64::MIN_POSITIVE,
        vec![
            running("held", Some(f64::MIN_POSITIVE)),
            waiting("tiny", "2026-09-10T00:00:00Z", Some(f64::MIN_POSITIVE)),
        ],
    );
    assert_eq!(
        waiter(&over, "tiny").blockers[0].code,
        "insufficient-capacity"
    );
}

#[test]
fn invalid_live_claim_and_aggregate_overflow_fail_closed() {
    let mut bad = running("bad", None);
    bad.queue_weight_invalid = true;
    let huge_a = running("huge-a", Some(f64::MAX));
    let huge_b = running("huge-b", Some(f64::MAX));
    let waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));

    let result = snapshot(f64::MAX, vec![bad, huge_a, huge_b, waiter]);
    assert!(result.first_eligible_artifact_dir.is_none());
    assert!(result
        .diagnostics
        .iter()
        .any(|diag| diag.code == "invalid-live-claim-weight"));
    assert!(result
        .diagnostics
        .iter()
        .any(|diag| diag.code == "capacity-overflow"));
    assert_eq!(
        result.waiters[0].blockers[0].code,
        "capacity-snapshot-invalid"
    );
}

#[test]
fn oversized_and_invalid_request_weights_are_blocked() {
    let oversized = waiting("oversized", "2026-09-10T00:00:00Z", Some(2.0));
    let mut invalid = waiting("invalid", "2026-09-10T00:00:01Z", None);
    invalid.queue_weight_invalid = true;

    let result = snapshot(1.0, vec![oversized, invalid]);
    assert!(result.waiters.iter().all(|waiter| !waiter.eligible));
    assert_eq!(
        waiter(&result, "oversized").blockers[0].code,
        "weight-exceeds-limit"
    );
    assert_eq!(
        waiter(&result, "invalid").blockers[0].code,
        "invalid-request-weight"
    );
}

#[test]
fn proposed_waiter_overflow_fails_closed() {
    let result = snapshot(
        f64::MAX,
        vec![
            running("held", Some(f64::MAX)),
            waiting("waiter", "2026-09-10T00:00:00Z", Some(f64::MAX)),
        ],
    );
    assert_eq!(
        waiter(&result, "waiter").blockers[0].code,
        "capacity-overflow"
    );
}
