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
    starter.agent_family = Some("fam".to_string());
    starter.live = false;
    let mut monitor = running("monitor", Some(0.0));
    monitor.agent_family = Some("fam".to_string());
    monitor.agent_family_role = Some("monitor".to_string());
    monitor.family_shell_kind = Some("monitor".to_string());
    monitor.family_shell_id = Some("mon-1".to_string());
    monitor.parent_timestamp = Some("starter".to_string());
    monitor.pid = Some(99);
    monitor.run_started_at = None;
    monitor.queue_weight_explicit = true;
    let mut successor = waiting("successor", "2026-09-10T00:00:00Z", Some(1.0));
    successor.agent_family = Some("fam".to_string());
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
