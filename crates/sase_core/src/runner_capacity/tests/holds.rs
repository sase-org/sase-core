//! Hold-barrier blocking tests.

use super::support::*;
use crate::agent_hold::{AgentHoldScopeWire, AgentHoldSelectorsWire};

#[test]
fn hold_barrier_blocks_waiting_candidate_with_metadata() {
    let hold = hold(
        "agent:hold-a",
        AgentHoldSelectorsWire {
            names: vec!["target.agent--code".to_string()],
            ..AgentHoldSelectorsWire::default()
        },
    );
    let result =
        snapshot_with_holds(4.0, vec![identity_waiter("waiter")], vec![hold]);
    assert!(result.first_eligible_artifact_dir.is_none());
    let waiter = waiter(&result, "waiter");
    assert!(!waiter.eligible);
    assert!(waiter.parked);
    assert_eq!(waiter.blockers[0].code, "hold-barrier");
    assert_eq!(
        waiter.blockers[0].message,
        "held by agent:hold-a display (expires in 2m)"
    );
    assert_eq!(waiter.blockers[0].held_by.as_deref(), Some("agent:hold-a"));
    assert_eq!(
        waiter.blockers[0].hold_expires_at,
        Some(epoch("2026-09-10T00:03:00Z"))
    );
}

#[test]
fn hold_barrier_blocks_queued_candidate_decision() {
    let hold = hold(
        "agent:hold-a",
        AgentHoldSelectorsWire {
            families: vec!["target.agent".to_string()],
            ..AgentHoldSelectorsWire::default()
        },
    );
    let candidate = identity_waiter("candidate");
    let result = snapshot_with_candidate_and_holds(
        4.0,
        Vec::new(),
        candidate,
        vec![hold],
    );
    let decision = result.candidate_decision.as_ref().unwrap();
    assert_eq!(decision.decision, "blocked");
    assert_eq!(decision.blockers[0].code, "hold-barrier");
    assert_eq!(
        decision.blockers[0].held_by.as_deref(),
        Some("agent:hold-a")
    );
}

#[test]
fn running_candidate_is_not_reblocked_by_hold() {
    let hold = hold(
        "agent:hold-a",
        AgentHoldSelectorsWire {
            names: vec!["target.agent--code".to_string()],
            ..AgentHoldSelectorsWire::default()
        },
    );
    let mut running = identity_waiter("candidate");
    running.run_started_at = Some("2026-09-10T00:00:45Z".to_string());
    running.slot_requested_at = None;
    let result = snapshot_with_holds(4.0, vec![running], vec![hold]);
    assert!(result.waiters.is_empty());
    assert_eq!(result.occupied_lanes, 1);
}

#[test]
fn hold_barrier_honors_project_and_future_selectors() {
    let mut wrong_project = hold(
        "agent:hold-a",
        AgentHoldSelectorsWire {
            names: vec!["target.agent--code".to_string()],
            ..AgentHoldSelectorsWire::default()
        },
    );
    wrong_project.scope = AgentHoldScopeWire::Project {
        project: "other".to_string(),
    };
    let wrong_project_result = snapshot_with_holds(
        4.0,
        vec![identity_waiter("waiter")],
        vec![wrong_project],
    );
    assert!(waiter(&wrong_project_result, "waiter").eligible);

    let future = hold(
        "agent:hold-b",
        AgentHoldSelectorsWire {
            future: true,
            ..AgentHoldSelectorsWire::default()
        },
    );
    let mut older = identity_waiter("older");
    older.created_at = Some(epoch("2026-09-09T23:59:00Z"));
    let older_result =
        snapshot_with_holds(4.0, vec![older], vec![future.clone()]);
    assert!(waiter(&older_result, "older").eligible);

    let newer_result =
        snapshot_with_holds(4.0, vec![identity_waiter("newer")], vec![future]);
    assert_eq!(
        waiter(&newer_result, "newer").blockers[0].code,
        "hold-barrier"
    );
}

#[test]
fn multiple_holds_release_independently() {
    let by_name = hold(
        "agent:hold-a",
        AgentHoldSelectorsWire {
            names: vec!["target.agent--code".to_string()],
            ..AgentHoldSelectorsWire::default()
        },
    );
    let by_workflow = hold(
        "agent:hold-b",
        AgentHoldSelectorsWire {
            workflows: vec!["build".to_string()],
            ..AgentHoldSelectorsWire::default()
        },
    );
    let both = snapshot_with_holds(
        4.0,
        vec![identity_waiter("waiter")],
        vec![by_name.clone(), by_workflow.clone()],
    );
    assert_eq!(
        waiter(&both, "waiter")
            .blockers
            .iter()
            .filter(|blocker| blocker.code == "hold-barrier")
            .count(),
        2
    );

    let one_left = snapshot_with_holds(
        4.0,
        vec![identity_waiter("waiter")],
        vec![by_workflow],
    );
    assert_eq!(waiter(&one_left, "waiter").blockers.len(), 1);
    assert_eq!(
        waiter(&one_left, "waiter").blockers[0].held_by.as_deref(),
        Some("agent:hold-b")
    );

    let released =
        snapshot_with_holds(4.0, vec![identity_waiter("waiter")], vec![]);
    assert!(waiter(&released, "waiter").eligible);
}
