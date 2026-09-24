//! Claim building and lineage tests.

use super::support::*;

#[test]
fn shared_agent_session_claim_counts_once_for_capacity() {
    let mut serial_root = running("serial-root", Some(2.0));
    serial_root.agent_session = Some("fam".to_string());
    let mut serial_child = running("serial-child", Some(1.0));
    serial_child.agent_session = Some("fam".to_string());
    serial_child.parent_timestamp = Some("serial-root".to_string());
    let mut waiting_agent =
        waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
    waiting_agent.queue_capacity = Some(2);
    waiting_agent.queue_capacity_explicit = true;

    let result = snapshot(8.0, vec![serial_root, serial_child, waiting_agent]);
    assert_eq!(result.occupied_lanes, 1);
    assert_eq!(result.occupied_capacity, 2.0);
    assert_eq!(
        result.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/waiter")
    );
    assert!(waiter(&result, "waiter").eligible);
}

#[test]
fn serial_successor_waits_only_after_agent_session_releases_claim() {
    let mut active = running("root", Some(2.0));
    active.agent_session = Some("fam".to_string());
    let mut successor = waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
    successor.agent_session = Some("fam".to_string());
    successor.parent_timestamp = Some("root".to_string());

    let with_claim = snapshot(4.0, vec![active.clone(), successor.clone()]);
    assert!(with_claim.waiters.is_empty());

    active.live = false;
    let released = snapshot(4.0, vec![active, successor]);
    assert_eq!(released.waiters.len(), 1);
    assert!(released.waiters[0].eligible);
}

#[test]
fn serial_successor_of_parallel_member_shares_parallel_lineage_claim() {
    let mut parallel = running("parallel", Some(2.0));
    parallel.agent_session = Some("fam".to_string());
    parallel.agent_session_parallel = true;
    let mut successor = running("successor", Some(2.0));
    successor.agent_session = Some("fam".to_string());
    successor.parent_timestamp = Some("parallel".to_string());

    let result = snapshot(4.0, vec![parallel, successor]);
    assert_eq!(result.occupied_lanes, 1);
    assert_eq!(result.occupied_capacity, 2.0);
    assert_eq!(result.claims.len(), 1);
    assert_eq!(result.claims[0].claim_kind, "parallel_member");
    assert_eq!(
        result.claims[0].artifact_dirs,
        ["/tmp/parallel", "/tmp/successor"]
    );
}

#[test]
fn unrelated_same_display_agent_session_claim_does_not_hide_parallel_successor()
{
    let mut serial_branch = running("serial-branch", Some(2.0));
    serial_branch.agent_session = Some("fam".to_string());
    let mut parallel_parent = running("parallel-parent", Some(2.0));
    parallel_parent.agent_session = Some("fam".to_string());
    parallel_parent.agent_session_parallel = true;
    parallel_parent.live = false;
    let mut successor = waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
    successor.agent_session = Some("fam".to_string());
    successor.parent_timestamp = Some("parallel-parent".to_string());

    let result = snapshot(4.0, vec![serial_branch, parallel_parent, successor]);
    assert_eq!(
        result.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/successor")
    );
    assert_eq!(result.waiters.len(), 1);
}

#[test]
fn explicit_claim_owner_separates_same_agent_session_branches() {
    let mut left = running("left", Some(1.0));
    left.agent_session = Some("fam".to_string());
    left.runner_claim_owner_key = Some("branch-left".to_string());
    let mut right = running("right", Some(1.0));
    right.agent_session = Some("fam".to_string());
    right.runner_claim_owner_key = Some("branch-right".to_string());

    let result = snapshot(4.0, vec![left, right]);
    assert_eq!(result.occupied_lanes, 2);
    assert_eq!(result.occupied_capacity, 2.0);
    assert_eq!(
        result
            .claims
            .iter()
            .map(|claim| claim.owner_key.as_str())
            .collect::<Vec<_>>(),
        ["proj:branch-left", "proj:branch-right"]
    );
}

#[test]
fn nested_monitor_successor_reuses_starter_lineage() {
    let mut starter = running("starter", Some(2.0));
    starter.agent_session = Some("fam".to_string());
    let mut monitor = running("monitor", Some(2.0));
    monitor.agent_session = Some("fam".to_string());
    monitor.agent_session_role = Some("monitor".to_string());
    monitor.agent_session_shell_kind = Some("monitor".to_string());
    monitor.agent_session_shell_id = Some("mon-1".to_string());
    monitor.parent_timestamp = Some("starter".to_string());
    monitor.pid = Some(99);
    monitor.run_started_at = None;
    let mut successor = waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
    successor.agent_session = Some("fam".to_string());
    successor.parent_timestamp = Some("monitor".to_string());

    let live = snapshot(
        4.0,
        vec![starter.clone(), monitor.clone(), successor.clone()],
    );
    assert_eq!(live.occupied_lanes, 1);
    assert_eq!(live.occupied_capacity, 2.0);
    assert!(live.waiters.is_empty());

    let decision =
        snapshot_with_candidate(4.0, vec![starter, monitor], successor)
            .candidate_decision
            .unwrap();
    assert_eq!(decision.decision, "reuse_existing_claim");
    assert_eq!(decision.owner_key, "proj:fam");
    assert_eq!(decision.lineage_key, "fam");
    assert_eq!(decision.effective_weight, 2.0);
}

#[test]
fn nested_gate_successor_reuses_starter_lineage() {
    let mut starter = running("starter", Some(2.0));
    starter.agent_session = Some("fam".to_string());
    let mut gate = running("gate", Some(2.0));
    gate.agent_session = Some("fam".to_string());
    gate.agent_session_role = Some("gate".to_string());
    gate.agent_session_shell_kind = Some("gate".to_string());
    gate.agent_session_shell_id = Some("gate-1".to_string());
    gate.agent_session_shell_state = Some("approved".to_string());
    gate.parent_timestamp = Some("starter".to_string());
    let mut successor = waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
    successor.agent_session = Some("fam".to_string());
    successor.parent_timestamp = Some("gate".to_string());

    let decision = snapshot_with_candidate(4.0, vec![starter, gate], successor)
        .candidate_decision
        .unwrap();
    assert_eq!(decision.decision, "reuse_existing_claim");
    assert_eq!(decision.lineage_key, "fam");
    assert_eq!(decision.effective_weight, 2.0);
}
