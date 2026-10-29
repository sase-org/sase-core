//! Staged-join reservation contract tests: reservation, contention,
//! activation, terminal-after-reserve, and identity-scoped release.

use super::{definition, store};
use crate::tool_run::catalog::normalize_tool_definition;
use crate::tool_run::handoff_wire::{
    ToolRunActivateStagedJoinOutcomeWire, ToolRunActivateStagedJoinRefusalWire,
    ToolRunActivateStagedJoinRequestWire, ToolRunJoinOutcomeWire,
    ToolRunJoinRefusalWire, ToolRunJoinRequestWire,
    ToolRunReleaseStagedJoinOutcomeWire, ToolRunReleaseStagedJoinRequestWire,
    ToolRunReserveStagedJoinOutcomeWire, ToolRunReserveStagedJoinRefusalWire,
    ToolRunReserveStagedJoinRequestWire, ToolRunStarterWire,
};
use crate::tool_run::store::{
    activate_staged_join, begin, claim, finish, join, release_staged_join,
    request_stop, reserve_staged_join,
};
use crate::tool_run::wire::{
    ToolRunBeginRequestWire, ToolRunFinishRequestWire, ToolRunStateWire,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use std::time::Duration;

const STAGED_A: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const STAGED_B: &str = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff";

fn starter(agent: &str, pid: i64) -> ToolRunStarterWire {
    ToolRunStarterWire {
        agent: agent.into(),
        pid,
        boot_id: Some("boot-9".into()),
        process_start_identity: Some(format!("boot-9:{pid}")),
    }
}

fn handoff_envelope(
    digest: &str,
) -> crate::tool_run::handoff_wire::ToolRunLaunchEnvelopeWire {
    crate::tool_run::handoff_wire::ToolRunLaunchEnvelopeWire {
        continuation_mode: None,
        argv: vec!["just".into(), "check".into()],
        cwd: None,
        tool_name: Some("check".into()),
        extra_args: Vec::new(),
        display_argv: vec!["just".into(), "check".into()],
        private_argv: None,
        definition: definition(),
        digest: Some(digest.to_string()),
        adhoc: false,
    }
}

fn detached_run(
    path: &std::path::Path,
    run_id: &str,
    starter: Option<ToolRunStarterWire>,
) {
    let normalized = normalize_tool_definition(definition()).unwrap();
    let digest = normalized.digest.clone();
    begin(
        path,
        ToolRunBeginRequestWire {
            starter,
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some(run_id.to_string()),
            created_event_id: None,
            running_event_id: None,
            tool_name: Some("check".into()),
            definition: normalized.definition,
            extra_args: Vec::new(),
            display_argv: vec!["just".into(), "check".into()],
            private_argv: None,
            project: Some("sase".into()),
            agent: Some("agent-1".into()),
            workspace: None,
            bead: None,
            owner_kind: Some("proc".into()),
            owner_id: Some("proc-1".into()),
            parent_run_id: None,
            wrapper_pid: Some(111),
            boot_id: Some("boot-1".into()),
            process_start_identity: Some("boot-1:111".into()),
            events_path: None,
            log_stdout_path: None,
            log_stderr_path: None,
            now_ts: Some(10),
            commit_running: false,
            launch_mode: Some(
                crate::tool_run::handoff_wire::ToolRunLaunchModeWire::Handoff,
            ),
            launch: Some(handoff_envelope(&digest)),
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn reserve_request(
    run_id: &str,
    staged_id: &str,
    monitor_id: &str,
    agent: &str,
) -> ToolRunReserveStagedJoinRequestWire {
    ToolRunReserveStagedJoinRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.to_string(),
        staged_id: staged_id.to_string(),
        monitor_id: monitor_id.to_string(),
        agent: agent.to_string(),
        requested_by: None,
        now_ts: Some(11),
    }
}

fn activate_request(
    run_id: &str,
    staged_id: &str,
    monitor_id: &str,
) -> ToolRunActivateStagedJoinRequestWire {
    ToolRunActivateStagedJoinRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.to_string(),
        staged_id: staged_id.to_string(),
        monitor_id: monitor_id.to_string(),
        requested_by: None,
        now_ts: Some(12),
    }
}

fn release_request(
    run_id: &str,
    staged_id: &str,
    monitor_id: &str,
) -> ToolRunReleaseStagedJoinRequestWire {
    ToolRunReleaseStagedJoinRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.to_string(),
        staged_id: staged_id.to_string(),
        monitor_id: monitor_id.to_string(),
        now_ts: Some(13),
    }
}

fn settle_run(path: &std::path::Path, run_id: &str) {
    claim(
        path,
        crate::tool_run::handoff_wire::ToolRunClaimRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            owner_kind: "proc".to_string(),
            owner_id: "proc-1".to_string(),
            wrapper_pid: Some(111),
            boot_id: Some("boot-1".to_string()),
            process_start_identity: Some("boot-1:111".to_string()),
            owner_log_path: None,
            running_event_id: None,
            now_ts: Some(12),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    finish(
        path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            event_id: None,
            state: ToolRunStateWire::Succeeded,
            exit_code: Some(0),
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: Some(1),
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(14),
            terminal_cause: Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited,
            ),
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

#[test]
fn reserve_activate_release_round_trip() {
    let (_temp, path) = store();
    detached_run(&path, "run-1", Some(starter("agent-1", 999)));

    let reserved = reserve_staged_join(
        &path,
        reserve_request("run-1", STAGED_A, "m-1", "agent-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        reserved.outcome,
        ToolRunReserveStagedJoinOutcomeWire::Reserved
    );
    assert!(!reserved.replayed);
    let stored = reserved.run.staged_join_reservation.expect("reservation");
    assert_eq!(stored.staged_id, STAGED_A);
    assert_eq!(stored.monitor_id, "m-1");

    // Matching replay succeeds without duplicating the reservation.
    let replayed = reserve_staged_join(
        &path,
        reserve_request("run-1", STAGED_A, "m-1", "agent-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        replayed.outcome,
        ToolRunReserveStagedJoinOutcomeWire::Reserved
    );
    assert!(replayed.replayed);

    let activated = activate_staged_join(
        &path,
        activate_request("run-1", STAGED_A, "m-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        activated.outcome,
        ToolRunActivateStagedJoinOutcomeWire::Activated
    );
    assert!(!activated.replayed);
    let join = activated.run.join.expect("ordinary join");
    assert_eq!(join.kind, "monitor");
    assert_eq!(join.id, "m-1");
    assert!(activated.run.staged_join_reservation.is_none());

    // Activating again replays the completed adoption.
    let again = activate_staged_join(
        &path,
        activate_request("run-1", STAGED_A, "m-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        again.outcome,
        ToolRunActivateStagedJoinOutcomeWire::Activated
    );
    assert!(again.replayed);

    // Releasing after adoption reports nothing reserved and keeps the join.
    let released = release_staged_join(
        &path,
        release_request("run-1", STAGED_A, "m-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        released.outcome,
        ToolRunReleaseStagedJoinOutcomeWire::NotReserved
    );
    assert!(released.run.join.is_some());
}

#[test]
fn reservation_blocks_other_joiners_and_stages() {
    let (_temp, path) = store();
    detached_run(&path, "run-1", Some(starter("agent-1", 999)));
    reserve_staged_join(
        &path,
        reserve_request("run-1", STAGED_A, "m-1", "agent-1"),
        Duration::from_secs(1),
    )
    .unwrap();

    // Another stage cannot take the reservation.
    let refused = reserve_staged_join(
        &path,
        reserve_request("run-1", STAGED_B, "m-2", "agent-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        refused.outcome,
        ToolRunReserveStagedJoinOutcomeWire::Refused
    );
    assert_eq!(
        refused.refusal,
        Some(ToolRunReserveStagedJoinRefusalWire::ReservationElsewhere)
    );

    // An ordinary join cannot take the reserved run either.
    let joined = join(
        &path,
        ToolRunJoinRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "run-1".to_string(),
            joiner_kind: "monitor".to_string(),
            joiner_id: "m-9".to_string(),
            agent: Some("agent-1".to_string()),
            requested_by: None,
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(joined.outcome, ToolRunJoinOutcomeWire::Refused);
    assert_eq!(joined.refusal, Some(ToolRunJoinRefusalWire::StagedReserved));

    // Foreign activation cannot steal the reservation.
    let stolen = activate_staged_join(
        &path,
        activate_request("run-1", STAGED_B, "m-2"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        stolen.outcome,
        ToolRunActivateStagedJoinOutcomeWire::Refused
    );
    assert_eq!(
        stolen.refusal,
        Some(ToolRunActivateStagedJoinRefusalWire::ReservationMismatch)
    );

    // Foreign release leaves the reservation alone.
    let kept = release_staged_join(
        &path,
        release_request("run-1", STAGED_B, "m-2"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        kept.outcome,
        ToolRunReleaseStagedJoinOutcomeWire::ReservedElsewhere
    );
    assert!(kept.run.staged_join_reservation.is_some());

    // Matching release clears it and repeats safely.
    let released = release_staged_join(
        &path,
        release_request("run-1", STAGED_A, "m-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        released.outcome,
        ToolRunReleaseStagedJoinOutcomeWire::Released
    );
    let again = release_staged_join(
        &path,
        release_request("run-1", STAGED_A, "m-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        again.outcome,
        ToolRunReleaseStagedJoinOutcomeWire::NotReserved
    );
}

#[test]
fn reserve_validates_starter_and_existing_state() {
    let (_temp, path) = store();
    detached_run(&path, "run-1", Some(starter("agent-1", 999)));

    // Foreign starter agent is refused.
    let refused = reserve_staged_join(
        &path,
        reserve_request("run-1", STAGED_A, "m-1", "agent-9"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        refused.refusal,
        Some(ToolRunReserveStagedJoinRefusalWire::AgentMismatch)
    );

    // Runs without a starter are not detached.
    detached_run(&path, "run-plain", None);
    let refused = reserve_staged_join(
        &path,
        reserve_request("run-plain", STAGED_A, "m-1", "agent-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        refused.refusal,
        Some(ToolRunReserveStagedJoinRefusalWire::NotDetached)
    );

    // A run terminal before reservation is still refused.
    settle_run(&path, "run-1");
    let refused = reserve_staged_join(
        &path,
        reserve_request("run-1", STAGED_A, "m-1", "agent-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        refused.refusal,
        Some(ToolRunReserveStagedJoinRefusalWire::Settled)
    );

    // A stopped run cannot be reserved.
    detached_run(&path, "run-2", Some(starter("agent-1", 999)));
    request_stop(
        &path,
        crate::tool_run::handoff_wire::ToolRunStopRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "run-2".to_string(),
            requested_by: None,
            reason: None,
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let refused = reserve_staged_join(
        &path,
        reserve_request("run-2", STAGED_A, "m-1", "agent-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        refused.refusal,
        Some(ToolRunReserveStagedJoinRefusalWire::StopRequested)
    );
}

#[test]
fn terminal_after_reserve_can_activate_and_release() {
    let (_temp, path) = store();
    detached_run(&path, "run-1", Some(starter("agent-1", 999)));
    reserve_staged_join(
        &path,
        reserve_request("run-1", STAGED_A, "m-1", "agent-1"),
        Duration::from_secs(1),
    )
    .unwrap();

    settle_run(&path, "run-1");

    // Activation consumes the settled run's result.
    let activated = activate_staged_join(
        &path,
        activate_request("run-1", STAGED_A, "m-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        activated.outcome,
        ToolRunActivateStagedJoinOutcomeWire::Activated
    );
    assert!(!activated.replayed);
    assert_eq!(activated.run.state, ToolRunStateWire::Succeeded);
    let join = activated.run.join.expect("join");
    assert_eq!(join.id, "m-1");

    // Release after settlement is safe and leaves the join alone.
    detached_run(&path, "run-2", Some(starter("agent-1", 999)));
    reserve_staged_join(
        &path,
        reserve_request("run-2", STAGED_A, "m-1", "agent-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    settle_run(&path, "run-2");
    let released = release_staged_join(
        &path,
        release_request("run-2", STAGED_A, "m-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        released.outcome,
        ToolRunReleaseStagedJoinOutcomeWire::Released
    );
    assert!(released.run.join.is_none());
}

#[test]
fn activate_without_reservation_refuses() {
    let (_temp, path) = store();
    detached_run(&path, "run-1", Some(starter("agent-1", 999)));
    let refused = activate_staged_join(
        &path,
        activate_request("run-1", STAGED_A, "m-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        refused.outcome,
        ToolRunActivateStagedJoinOutcomeWire::Refused
    );
    assert_eq!(
        refused.refusal,
        Some(ToolRunActivateStagedJoinRefusalWire::NoReservation)
    );
}

#[test]
fn ordinary_join_still_works_without_reservation() {
    let (_temp, path) = store();
    detached_run(&path, "run-1", Some(starter("agent-1", 999)));
    let joined = join(
        &path,
        ToolRunJoinRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "run-1".to_string(),
            joiner_kind: "monitor".to_string(),
            joiner_id: "m-1".to_string(),
            agent: Some("agent-1".to_string()),
            requested_by: None,
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(joined.outcome, ToolRunJoinOutcomeWire::Joined);

    // Reserving a joined run is refused.
    let refused = reserve_staged_join(
        &path,
        reserve_request("run-1", STAGED_A, "m-1", "agent-1"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        refused.refusal,
        Some(ToolRunReserveStagedJoinRefusalWire::JoinedElsewhere)
    );
}
