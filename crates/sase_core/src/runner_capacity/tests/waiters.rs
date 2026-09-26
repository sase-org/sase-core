//! Waiter ordering, deference, and eligibility tests.

use super::support::*;

#[test]
fn lighter_waiter_can_pass_non_fitting_heavy_waiter() {
    let occupied = running("occupied", Some(0.75));
    let heavy = waiting("heavy", "2026-09-10T00:00:00Z", Some(0.5));
    let light = waiting("light", "2026-09-10T00:00:01Z", Some(0.25));

    let result = snapshot(1.0, vec![occupied, heavy, light]);
    assert_eq!(
        result.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/light")
    );
    assert_eq!(
        result
            .waiters
            .iter()
            .map(|waiter| waiter.artifact_dir.as_str())
            .collect::<Vec<_>>(),
        ["/tmp/light", "/tmp/heavy"]
    );
    assert_eq!(
        waiter(&result, "heavy").blockers[0].code,
        "insufficient-capacity"
    );
    assert!(waiter(&result, "light").eligible);
}

#[test]
fn parked_waiters_sort_by_capacity_then_runner_shortfall_before_priority() {
    let held = running("held", Some(1.0));
    let mut heavy = waiting("heavy", "2026-09-10T00:00:00Z", Some(2.0));
    heavy.wait_priority = Some(1);
    let mut light = waiting("light", "2026-09-10T00:00:01Z", Some(0.5));
    light.wait_priority = Some(10);

    let result = snapshot(1.0, vec![held, heavy, light]);
    assert_eq!(
        result
            .waiters
            .iter()
            .map(|waiter| waiter.artifact_dir.as_str())
            .collect::<Vec<_>>(),
        ["/tmp/light", "/tmp/heavy"]
    );
    assert!(result.waiters.iter().all(|waiter| waiter.parked));
    assert_eq!(result.waiters[0].capacity_shortfall, 0.5);
    assert_eq!(result.waiters[1].capacity_shortfall, 2.0);
}

#[test]
fn deference_blocks_lower_priority_until_window_elapses() {
    let mut pending_better = rec("pending-better");
    pending_better.wait_priority = Some(5);
    let mut lower = waiting("lower", "2026-09-10T00:00:00Z", Some(0.25));
    lower.wait_priority = Some(12);
    lower.eligible_since = Some("2026-09-10T00:00:55Z".to_string());

    let blocked = snapshot(1.0, vec![pending_better.clone(), lower.clone()]);
    assert_eq!(blocked.waiters[0].blockers[0].code, "deference-window");

    lower.eligible_since = Some("2026-09-10T00:00:00Z".to_string());
    let satisfied = snapshot(1.0, vec![pending_better, lower]);
    assert!(satisfied.waiters[0].eligible);
}

#[test]
fn explicit_zero_weight_monitor_occupies_nothing_at_a_full_limit() {
    let busy = running("busy", Some(1.0));
    let mut zero_weight_monitor = running("monitor", Some(0.0));
    zero_weight_monitor.queue_weight_explicit = true;

    let result = snapshot(1.0, vec![busy, zero_weight_monitor]);
    assert!(result.diagnostics.is_empty());
    assert_eq!(result.occupied_lanes, 2);
    assert_eq!(result.occupied_capacity, 1.0);
    assert_eq!(
        result
            .claims
            .iter()
            .find(|claim| claim.artifact_dirs == ["/tmp/monitor"])
            .unwrap()
            .occupied_capacity,
        0.0
    );
}

#[test]
fn waiter_admits_when_only_other_records_are_zero_weight() {
    let mut zero_weight_monitor = running("monitor", Some(0.0));
    zero_weight_monitor.queue_weight_explicit = true;
    let waiting_agent = waiting("waiter", "2026-09-10T00:00:00Z", Some(1.0));

    let result = snapshot(1.0, vec![zero_weight_monitor, waiting_agent]);
    assert_eq!(result.occupied_capacity, 0.0);
    assert_eq!(
        result.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/waiter")
    );
    assert!(waiter(&result, "waiter").eligible);
}

#[test]
fn explicit_zero_weight_waiter_respects_queue_order() {
    let mut first = waiting("first", "2026-09-10T00:00:00Z", Some(0.0));
    first.queue_weight_explicit = true;
    let second = waiting("second", "2026-09-10T00:00:01Z", Some(1.0));

    let result = snapshot(2.0, vec![first, second]);
    assert_eq!(
        result.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/first")
    );
    assert!(waiter(&result, "first").eligible);
    assert_eq!(waiter(&result, "second").blockers[0].code, "queue-order");
}

#[test]
fn implicit_zero_negative_and_nan_record_weights_still_fail_closed() {
    let implicit_zero =
        waiting("implicit-zero", "2026-09-10T00:00:00Z", Some(0.0));

    let mut negative = waiting("negative", "2026-09-10T00:00:01Z", Some(-1.0));
    negative.queue_weight_explicit = true;

    let mut nan = waiting("nan", "2026-09-10T00:00:02Z", Some(f64::NAN));
    nan.queue_weight_explicit = true;

    for bad in [implicit_zero, negative, nan] {
        let name = bad.timestamp.clone();
        let result = snapshot(8.0, vec![bad]);
        assert_eq!(
            waiter(&result, &name).blockers[0].code,
            "invalid-request-weight",
            "{name}"
        );
        assert!(!waiter(&result, &name).eligible, "{name}");
    }
}

#[test]
fn zero_weight_claim_is_not_reusable_by_a_serial_successor() {
    let mut starter = running("starter", Some(2.0));
    starter.agent_session = Some("fam".to_string());
    starter.live = false;
    let mut monitor = running("monitor", Some(0.0));
    monitor.agent_session = Some("fam".to_string());
    monitor.agent_session_role = Some("monitor".to_string());
    monitor.agent_session_turn_kind = Some("monitor".to_string());
    monitor.agent_session_turn_id = Some("mon-1".to_string());
    monitor.parent_timestamp = Some("starter".to_string());
    monitor.pid = Some(99);
    monitor.run_started_at = None;
    monitor.queue_weight_explicit = true;
    let mut successor = waiting("successor", "2026-09-10T00:00:00Z", Some(1.0));
    successor.agent_session = Some("fam".to_string());
    successor.parent_timestamp = Some("monitor".to_string());
    successor.queue_weight_explicit = true;

    let result = snapshot(
        4.0,
        vec![starter.clone(), monitor.clone(), successor.clone()],
    );
    assert_eq!(result.claims.len(), 1);
    assert_eq!(result.claims[0].occupied_capacity, 0.0);
    assert_eq!(result.occupied_capacity, 0.0);
    assert_eq!(result.waiters.len(), 1);
    assert!(result.waiters[0].eligible);
    assert_eq!(result.waiters[0].requested_weight, 1.0);

    let decision =
        snapshot_with_candidate(4.0, vec![starter, monitor], successor)
            .candidate_decision
            .unwrap();
    assert_eq!(decision.decision, "acquire_capacity");
    assert_eq!(decision.effective_weight, 1.0);
}

#[test]
fn multiplier_capacity_resolves_for_waiters_and_preserves_legacy_behavior() {
    for (multiplier, effective_limit, expected) in
        [(1.5, 5.0, 7.5), (0.5, 5.0, 2.5), (1.15, 3.0, 3.45)]
    {
        let mut record =
            waiting("multiplier", "2026-09-10T00:00:00Z", Some(0.25));
        record.queue_capacity_multiplier = Some(multiplier);
        let result = snapshot_with_flags(
            effective_limit,
            vec![record],
            &capacity_budget_flags(),
        );
        let projected = waiter(&result, "multiplier");
        assert_eq!(projected.queue_capacity_multiplier, Some(multiplier));
        assert_eq!(projected.admission_limit, expected);
        assert!(projected.eligible);
    }

    let mut over_limit =
        waiting("over-limit", "2026-09-10T00:00:00Z", Some(8.0));
    over_limit.queue_capacity_multiplier = Some(1.5);
    let blocked =
        snapshot_with_flags(5.0, vec![over_limit], &capacity_budget_flags());
    assert_eq!(waiter(&blocked, "over-limit").admission_limit, 7.5);
    assert_eq!(
        waiter(&blocked, "over-limit").blockers[0].code,
        "weight-exceeds-limit"
    );

    let mut integer_wins =
        waiting("integer-wins", "2026-09-10T00:00:00Z", Some(0.25));
    integer_wins.queue_capacity = Some(4);
    integer_wins.queue_capacity_explicit = true;
    integer_wins.queue_capacity_multiplier = Some(1.5);
    let integer_result =
        snapshot_with_flags(5.0, vec![integer_wins], &capacity_budget_flags());
    let integer_waiter = waiter(&integer_result, "integer-wins");
    assert_eq!(integer_waiter.queue_capacity, Some(4));
    assert_eq!(integer_waiter.queue_capacity_multiplier, None);
    assert_eq!(integer_waiter.admission_limit, 4.0);

    let mut legacy = waiting("legacy", "2026-09-10T00:00:00Z", Some(0.25));
    legacy.queue_capacity_multiplier = Some(1.5);
    let legacy_result = snapshot(5.0, vec![legacy]);
    let legacy_waiter = waiter(&legacy_result, "legacy");
    assert_eq!(legacy_waiter.queue_capacity_multiplier, Some(1.5));
    assert_eq!(legacy_waiter.admission_limit, 5.0);
}
