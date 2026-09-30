use super::{begin_named, definition, store};
use crate::tool_run::catalog::normalize_tool_definition;
use crate::tool_run::handoff_wire::{
    ToolRunClaimRequestWire, ToolRunJoinOutcomeWire, ToolRunJoinRefusalWire,
    ToolRunJoinRequestWire, ToolRunLaunchEnvelopeWire,
    ToolRunReleaseJoinOutcomeWire, ToolRunReleaseJoinRequestWire,
    ToolRunStarterWire, ToolRunStopRequestWire,
};
use crate::tool_run::store::{
    begin, claim, finish, join, release_join, request_stop, show_run,
};
use crate::tool_run::wire::{
    ToolArgsPolicyWire, ToolDefinitionWire, ToolFingerprintSpecWire,
    ToolRunBeginRequestWire, ToolRunFinishRequestWire, ToolRunShowRequestWire,
    ToolRunStateWire, ToolStagesWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use crate::tool_run::ToolRunError;
use std::time::Duration;

fn handoff_envelope(digest: &str) -> ToolRunLaunchEnvelopeWire {
    ToolRunLaunchEnvelopeWire {
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

fn handoff_begin(
    path: &std::path::Path,
    run_id: &str,
    now: i64,
) -> crate::tool_run::wire::ToolRunBeginResultWire {
    let normalized = normalize_tool_definition(definition()).unwrap();
    let digest = normalized.digest.clone();
    begin(
        path,
        ToolRunBeginRequestWire {
            starter: None,
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
            agent: None,
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
            now_ts: Some(now),
            commit_running: false,
            launch_mode: Some(
                crate::tool_run::handoff_wire::ToolRunLaunchModeWire::Handoff,
            ),
            launch: Some(handoff_envelope(&digest)),
            owner_log_path: Some("logs/proc-1.log".into()),
        },
        Duration::from_secs(1),
    )
    .unwrap()
}

fn claim_request(run_id: &str) -> ToolRunClaimRequestWire {
    ToolRunClaimRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.to_string(),
        owner_kind: "proc".into(),
        owner_id: "proc-1".into(),
        wrapper_pid: Some(4242),
        boot_id: Some("boot-1".into()),
        process_start_identity: Some("boot-1:4242".into()),
        owner_log_path: Some("logs/proc-1.log".into()),
        running_event_id: None,
        now_ts: Some(11),
    }
}

#[test]
fn handoff_begin_stores_launcher_with_null_wrapper() {
    let (_temp, path) = store();
    let started = handoff_begin(&path, "handoff-1", 10);
    assert_eq!(started.run.state, ToolRunStateWire::Created);
    assert_eq!(started.run.launch_mode.as_deref(), Some("handoff"));
    assert_eq!(
        started.run.logs.owner_log_path.as_deref(),
        Some("logs/proc-1.log")
    );
    assert!(started.run.wrapper_pid.is_none());
    assert!(started.run.boot_id.is_none());
    assert!(started.run.process_start_identity.is_none());
    let launcher = started.run.launcher.expect("launcher");
    assert_eq!(launcher.pid, Some(111));
    assert_eq!(launcher.boot_id.as_deref(), Some("boot-1"));
    assert_eq!(
        launcher.process_start_identity.as_deref(),
        Some("boot-1:111")
    );
}

#[test]
fn handoff_begin_rejects_invalid_combinations() {
    let (_temp, path) = store();
    let normalized = normalize_tool_definition(definition()).unwrap();
    let digest = normalized.digest.clone();
    let base = || ToolRunBeginRequestWire {
        starter: None,
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: None,
        created_event_id: None,
        running_event_id: None,
        tool_name: Some("check".into()),
        definition: normalized.definition.clone(),
        extra_args: Vec::new(),
        display_argv: vec!["just".into(), "check".into()],
        private_argv: None,
        project: Some("sase".into()),
        agent: None,
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
    };
    // launch without handoff mode
    let mut bad = base();
    bad.launch_mode = None;
    assert!(begin(&path, bad, Duration::from_secs(1)).is_err());
    // handoff without launch
    let mut bad = base();
    bad.launch = None;
    assert!(begin(&path, bad, Duration::from_secs(1)).is_err());
    // handoff with commit_running true
    let mut bad = base();
    bad.commit_running = true;
    assert!(begin(&path, bad, Duration::from_secs(1)).is_err());
    // handoff missing owner
    let mut bad = base();
    bad.owner_id = None;
    assert!(begin(&path, bad, Duration::from_secs(1)).is_err());
    // handoff with parent
    let parent = begin_named(&path, 9);
    let mut bad = base();
    bad.parent_run_id = Some(parent.run.run_id);
    assert!(begin(&path, bad, Duration::from_secs(1)).is_err());
    // envelope argv mismatch
    let mut bad = base();
    bad.launch.as_mut().unwrap().argv = vec!["other".into()];
    assert!(begin(&path, bad, Duration::from_secs(1)).is_err());
    // envelope display mismatch
    let mut bad = base();
    bad.launch.as_mut().unwrap().display_argv = vec!["other".into()];
    assert!(begin(&path, bad, Duration::from_secs(1)).is_err());
    // envelope adhoc mismatch
    let mut bad = base();
    bad.launch.as_mut().unwrap().adhoc = true;
    assert!(begin(&path, bad, Duration::from_secs(1)).is_err());
    // envelope digest mismatch
    let mut bad = base();
    bad.launch.as_mut().unwrap().digest = Some("wrong".into());
    assert!(begin(&path, bad, Duration::from_secs(1)).is_err());
}

#[test]
fn duplicate_run_id_returns_duplicate_run() {
    let (_temp, path) = store();
    handoff_begin(&path, "dup-1", 10);
    let normalized = normalize_tool_definition(definition()).unwrap();
    let digest = normalized.digest.clone();
    let err = begin(
        &path,
        ToolRunBeginRequestWire {
            starter: None,
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some("dup-1".into()),
            created_event_id: None,
            running_event_id: None,
            tool_name: Some("check".into()),
            definition: normalized.definition,
            extra_args: Vec::new(),
            display_argv: vec!["just".into(), "check".into()],
            private_argv: None,
            project: None,
            agent: None,
            workspace: None,
            bead: None,
            owner_kind: Some("proc".into()),
            owner_id: Some("proc-1".into()),
            parent_run_id: None,
            wrapper_pid: Some(111),
            boot_id: None,
            process_start_identity: None,
            events_path: None,
            log_stdout_path: None,
            log_stderr_path: None,
            now_ts: Some(11),
            commit_running: false,
            launch_mode: Some(
                crate::tool_run::handoff_wire::ToolRunLaunchModeWire::Handoff,
            ),
            launch: Some(handoff_envelope(&digest)),
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(err, ToolRunError::DuplicateRun { .. }));
    assert_eq!(err.to_string(), "tool run dup-1 already exists");
}

#[test]
fn claim_succeeds_and_replays() {
    let (_temp, path) = store();
    handoff_begin(&path, "claim-1", 10);
    let claimed =
        claim(&path, claim_request("claim-1"), Duration::from_secs(1)).unwrap();
    assert_eq!(
        claimed.outcome,
        crate::tool_run::handoff_wire::ToolRunClaimOutcomeWire::Claimed
    );
    assert!(!claimed.replayed);
    assert_eq!(claimed.launch.as_ref().unwrap().argv, vec!["just", "check"]);
    assert_eq!(claimed.run.wrapper_pid, Some(4242));
    assert_eq!(
        claimed.run.logs.owner_log_path.as_deref(),
        Some("logs/proc-1.log")
    );
    let replay =
        claim(&path, claim_request("claim-1"), Duration::from_secs(1)).unwrap();
    assert!(replay.replayed);
    assert_eq!(
        replay.outcome,
        crate::tool_run::handoff_wire::ToolRunClaimOutcomeWire::Claimed
    );
}

#[test]
fn claim_refusals_are_typed() {
    let (_temp, path) = store();
    // not_handoff: foreground created run
    let normalized = normalize_tool_definition(definition()).unwrap();
    begin(
        &path,
        ToolRunBeginRequestWire {
            starter: None,
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some("fore-1".into()),
            created_event_id: None,
            running_event_id: None,
            tool_name: Some("check".into()),
            definition: normalized.definition,
            extra_args: Vec::new(),
            display_argv: vec!["just".into(), "check".into()],
            private_argv: None,
            project: None,
            agent: None,
            workspace: None,
            bead: None,
            owner_kind: None,
            owner_id: None,
            parent_run_id: None,
            wrapper_pid: None,
            boot_id: None,
            process_start_identity: None,
            events_path: None,
            log_stdout_path: None,
            log_stderr_path: None,
            now_ts: Some(10),
            commit_running: false,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let refused = claim(
        &path,
        ToolRunClaimRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "fore-1".into(),
            owner_kind: "proc".into(),
            owner_id: "proc-1".into(),
            wrapper_pid: None,
            boot_id: None,
            process_start_identity: None,
            owner_log_path: None,
            running_event_id: None,
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        refused.refusal,
        Some(
            crate::tool_run::handoff_wire::ToolRunClaimRefusalWire::NotHandoff
        )
    );
    // owner_mismatch
    handoff_begin(&path, "claim-2", 10);
    let mut bad = claim_request("claim-2");
    bad.owner_id = "other".into();
    let refused = claim(&path, bad, Duration::from_secs(1)).unwrap();
    assert_eq!(
        refused.refusal,
        Some(crate::tool_run::handoff_wire::ToolRunClaimRefusalWire::OwnerMismatch)
    );
    // already_claimed by different claimant
    claim(&path, claim_request("claim-2"), Duration::from_secs(1)).unwrap();
    let mut other = claim_request("claim-2");
    other.wrapper_pid = Some(9999);
    let refused = claim(&path, other, Duration::from_secs(1)).unwrap();
    assert_eq!(
        refused.refusal,
        Some(crate::tool_run::handoff_wire::ToolRunClaimRefusalWire::AlreadyClaimed)
    );
    // not_created: settled run
    finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "claim-2".into(),
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
            now_ts: Some(12),
            terminal_cause: Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited,
            ),
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let refused =
        claim(&path, claim_request("claim-2"), Duration::from_secs(1)).unwrap();
    assert_eq!(
        refused.refusal,
        Some(
            crate::tool_run::handoff_wire::ToolRunClaimRefusalWire::NotCreated
        )
    );
}

#[test]
fn claim_with_pending_stop_settles_stopped() {
    let (_temp, path) = store();
    handoff_begin(&path, "stop-1", 10);
    let stopped = request_stop(
        &path,
        ToolRunStopRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "stop-1".into(),
            requested_by: Some("agent-1".into()),
            reason: Some("user stop".into()),
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        stopped.outcome,
        crate::tool_run::handoff_wire::ToolRunStopOutcomeWire::Recorded
    );
    let claimed =
        claim(&path, claim_request("stop-1"), Duration::from_secs(1)).unwrap();
    assert_eq!(
        claimed.outcome,
        crate::tool_run::handoff_wire::ToolRunClaimOutcomeWire::Stopped
    );
    assert!(claimed.launch.is_none());
    assert_eq!(claimed.run.state, ToolRunStateWire::Signaled);
    assert_eq!(
        claimed.run.terminal_cause.as_deref(),
        Some("stop_requested")
    );
    assert!(claimed
        .run
        .diagnostics
        .iter()
        .any(|item| item.contains("command was not run")));
    let again =
        claim(&path, claim_request("stop-1"), Duration::from_secs(1)).unwrap();
    assert_eq!(
        again.outcome,
        crate::tool_run::handoff_wire::ToolRunClaimOutcomeWire::Stopped
    );
}

#[test]
fn stop_first_wins_and_settled_writes_nothing() {
    let (_temp, path) = store();
    handoff_begin(&path, "stop-2", 10);
    let first = request_stop(
        &path,
        ToolRunStopRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "stop-2".into(),
            requested_by: Some("a".into()),
            reason: Some("first".into()),
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let second = request_stop(
        &path,
        ToolRunStopRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "stop-2".into(),
            requested_by: Some("b".into()),
            reason: Some("second".into()),
            now_ts: Some(12),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        second.outcome,
        crate::tool_run::handoff_wire::ToolRunStopOutcomeWire::AlreadyRequested
    );
    assert_eq!(
        second
            .stop_request
            .as_ref()
            .unwrap()
            .requested_by
            .as_deref(),
        Some("a")
    );
    assert_eq!(first.stop_request, second.stop_request);
    // stop during running
    claim(&path, claim_request("stop-2"), Duration::from_secs(1)).unwrap();
    // claim with pending stop settles stopped, so this path is already covered;
    // use a fresh running handoff for during-running stop
    handoff_begin(&path, "stop-3", 10);
    claim(&path, claim_request("stop-3"), Duration::from_secs(1)).unwrap();
    let during = request_stop(
        &path,
        ToolRunStopRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "stop-3".into(),
            requested_by: None,
            reason: None,
            now_ts: Some(13),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        during.outcome,
        crate::tool_run::handoff_wire::ToolRunStopOutcomeWire::Recorded
    );
    // stop on settled
    finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "stop-3".into(),
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
    let settled = request_stop(
        &path,
        ToolRunStopRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "stop-3".into(),
            requested_by: None,
            reason: None,
            now_ts: Some(15),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        settled.outcome,
        crate::tool_run::handoff_wire::ToolRunStopOutcomeWire::AlreadySettled
    );
}

#[test]
fn created_transitions_require_causes_and_no_codes() {
    let (_temp, path) = store();
    handoff_begin(&path, "trans-1", 10);
    // created -> failed without cause is refused
    let err = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "trans-1".into(),
            event_id: None,
            state: ToolRunStateWire::Failed,
            exit_code: None,
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: None,
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(11),
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(err, ToolRunError::InvalidTransition { .. }));
    // wrong cause
    let err = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "trans-1".into(),
            event_id: None,
            state: ToolRunStateWire::Failed,
            exit_code: None,
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: None,
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(11),
            terminal_cause: Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Timeout,
            ),
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(err, ToolRunError::Invalid { .. }));
    // with exit code refused
    let err = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "trans-1".into(),
            event_id: None,
            state: ToolRunStateWire::Failed,
            exit_code: Some(1),
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: None,
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(11),
            terminal_cause: Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::LaunchFailed,
            ),
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(err, ToolRunError::Invalid { .. }));
    // correct launch_failed
    let finished = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "trans-1".into(),
            event_id: None,
            state: ToolRunStateWire::Failed,
            exit_code: None,
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: None,
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(11),
            terminal_cause: Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::LaunchFailed,
            ),
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        finished.run.terminal_cause.as_deref(),
        Some("launch_failed")
    );
    // created -> signaled with stop_requested
    handoff_begin(&path, "trans-2", 10);
    let finished = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "trans-2".into(),
            event_id: None,
            state: ToolRunStateWire::Signaled,
            exit_code: None,
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: None,
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(11),
            terminal_cause: Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::StopRequested,
            ),
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        finished.run.terminal_cause.as_deref(),
        Some("stop_requested")
    );
    // append_event cannot take new edges
    handoff_begin(&path, "trans-3", 10);
    let err = crate::tool_run::store::append_event(
        &path,
        crate::tool_run::wire::ToolRunAppendRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            event: crate::tool_run::wire::ToolRunEventWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                event_id: "evt-1".into(),
                run_id: "trans-3".into(),
                attempt: 1,
                kind: crate::tool_run::wire::ToolRunEventKindWire::Failed,
                created_ts: 11,
                stage: None,
                sample: None,
                exit_code: None,
                signal: None,
                reason: None,
                diagnostics: Vec::new(),
            },
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(err, ToolRunError::InvalidTransition { .. }));
}

#[test]
fn finish_cause_table_and_diagnostics_persist() {
    let (_temp, path) = store();
    // exited requires succeeded/failed
    handoff_begin(&path, "cause-1", 10);
    claim(&path, claim_request("cause-1"), Duration::from_secs(1)).unwrap();
    let err = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "cause-1".into(),
            event_id: None,
            state: ToolRunStateWire::Signaled,
            exit_code: None,
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: None,
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(12),
            terminal_cause: Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited,
            ),
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(err, ToolRunError::Invalid { .. }));
    // diagnostics persist and replay does not duplicate
    let finished = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "cause-1".into(),
            event_id: Some("fin-1".into()),
            state: ToolRunStateWire::Succeeded,
            exit_code: Some(0),
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: Some(5),
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(12),
            terminal_cause: Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited,
            ),
            diagnostics: vec!["settled ok".into(), "settled ok".into()],
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(finished.run.diagnostics, vec!["settled ok".to_string()]);
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "cause-1".into(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        shown.run.as_ref().unwrap().diagnostics,
        vec!["settled ok".to_string()]
    );
    // replay same event id does not duplicate
    let replayed = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "cause-1".into(),
            event_id: Some("fin-1".into()),
            state: ToolRunStateWire::Succeeded,
            exit_code: Some(0),
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: Some(5),
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(12),
            terminal_cause: Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited,
            ),
            diagnostics: vec!["settled ok".into()],
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(replayed.run.diagnostics, vec!["settled ok".to_string()]);
}

#[test]
fn queries_never_expose_the_envelope() {
    let (_temp, path) = store();
    let secret_argv = vec![
        "secret".to_string(),
        "--token".to_string(),
        "abc123".to_string(),
    ];
    let display = vec!["secret".to_string(), "--token=<redacted>".to_string()];
    let normalized = normalize_tool_definition(ToolDefinitionWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        name: "secret".into(),
        argv: secret_argv.clone(),
        description: "s".into(),
        stages: ToolStagesWire::None,
        inputs: Vec::new(),
        env: Vec::new(),
        args: ToolArgsPolicyWire::Deny,
        fingerprint: ToolFingerprintSpecWire::default(),
        receipt: None,
        duration_class: None,
        diagnostics: Vec::new(),
    })
    .unwrap();
    let digest = normalized.digest.clone();
    begin(
        &path,
        ToolRunBeginRequestWire {
            starter: None,
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some("secret-1".into()),
            created_event_id: None,
            running_event_id: None,
            tool_name: Some("secret".into()),
            definition: normalized.definition.clone(),
            extra_args: Vec::new(),
            display_argv: display.clone(),
            private_argv: Some(secret_argv.clone()),
            project: None,
            agent: None,
            workspace: None,
            bead: None,
            owner_kind: Some("proc".into()),
            owner_id: Some("proc-1".into()),
            parent_run_id: None,
            wrapper_pid: Some(1),
            boot_id: None,
            process_start_identity: None,
            events_path: None,
            log_stdout_path: None,
            log_stderr_path: None,
            now_ts: Some(10),
            commit_running: false,
            launch_mode: Some(
                crate::tool_run::handoff_wire::ToolRunLaunchModeWire::Handoff,
            ),
            launch: Some(ToolRunLaunchEnvelopeWire {
                continuation_mode: None,
                argv: secret_argv.clone(),
                cwd: None,
                tool_name: Some("secret".into()),
                extra_args: Vec::new(),
                display_argv: display.clone(),
                private_argv: Some(secret_argv.clone()),
                definition: normalized.definition,
                digest: Some(digest),
                adhoc: false,
            }),
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "secret-1".into(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let run = shown.run.unwrap();
    assert_eq!(run.display_argv, display);
    let encoded = serde_json::to_value(&run).unwrap();
    let text = serde_json::to_string(&encoded).unwrap();
    assert!(!text.contains("abc123"));
    assert!(!text.contains("launch_envelope"));
    assert!(!text.contains("\"private_argv\":"));
    assert!(!text.contains("\"launch\":"));
    for event in shown.events {
        let payload = serde_json::to_value(&event).unwrap();
        assert!(!serde_json::to_string(&payload).unwrap().contains("abc123"));
    }
}

#[test]
fn golden_handoff_fixtures_pin_the_wire_shape() {
    let claim_request: ToolRunClaimRequestWire =
        serde_json::from_str(include_str!("../../fixtures/claim_request.json"))
            .unwrap();
    assert_eq!(claim_request.owner_kind, "proc");
    let stop_request: ToolRunStopRequestWire =
        serde_json::from_str(include_str!("../../fixtures/stop_request.json"))
            .unwrap();
    assert_eq!(stop_request.run_id, claim_request.run_id);
    let owner: crate::tool_run::handoff_wire::ToolRunOwnerFactWire =
        serde_json::from_str(include_str!("../../fixtures/owner_fact.json"))
            .unwrap();
    assert_eq!(owner.kind, "proc");
    let claim_result: crate::tool_run::handoff_wire::ToolRunClaimResultWire =
        serde_json::from_str(include_str!("../../fixtures/claim_result.json"))
            .unwrap();
    assert_eq!(
        claim_result.outcome,
        crate::tool_run::handoff_wire::ToolRunClaimOutcomeWire::Claimed
    );
    let stop_result: crate::tool_run::handoff_wire::ToolRunStopResultWire =
        serde_json::from_str(include_str!("../../fixtures/stop_result.json"))
            .unwrap();
    assert_eq!(
        stop_result.outcome,
        crate::tool_run::handoff_wire::ToolRunStopOutcomeWire::Recorded
    );
    let join_request: crate::tool_run::handoff_wire::ToolRunJoinRequestWire =
        serde_json::from_str(include_str!("../../fixtures/join_request.json"))
            .unwrap();
    assert_eq!(join_request.run_id, claim_request.run_id);
    let join_result: crate::tool_run::handoff_wire::ToolRunJoinResultWire =
        serde_json::from_str(include_str!("../../fixtures/join_result.json"))
            .unwrap();
    assert_eq!(join_result.outcome, ToolRunJoinOutcomeWire::Joined);
    let release_request: crate::tool_run::handoff_wire::ToolRunReleaseJoinRequestWire =
        serde_json::from_str(include_str!(
            "../../fixtures/release_join_request.json"
        ))
        .unwrap();
    assert_eq!(release_request.run_id, claim_request.run_id);
    let release_result: crate::tool_run::handoff_wire::ToolRunReleaseJoinResultWire =
        serde_json::from_str(include_str!(
            "../../fixtures/release_join_result.json"
        ))
        .unwrap();
    assert_eq!(
        release_result.outcome,
        ToolRunReleaseJoinOutcomeWire::Released
    );
    let begin_starter: ToolRunBeginRequestWire = serde_json::from_str(
        include_str!("../../fixtures/begin_starter_request.json"),
    )
    .unwrap();
    assert_eq!(begin_starter.starter.as_ref().unwrap().agent, "agent-1");
}

fn starter(agent: &str, pid: i64) -> ToolRunStarterWire {
    ToolRunStarterWire {
        agent: agent.to_string(),
        pid,
        boot_id: Some("boot-9".into()),
        process_start_identity: Some("boot-9:999".into()),
    }
}

fn handoff_begin_with_starter(
    path: &std::path::Path,
    run_id: &str,
    now: i64,
    starter: Option<ToolRunStarterWire>,
) -> crate::tool_run::wire::ToolRunBeginResultWire {
    let normalized = normalize_tool_definition(definition()).unwrap();
    let digest = normalized.digest.clone();
    begin(
        path,
        ToolRunBeginRequestWire {
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
            agent: None,
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
            now_ts: Some(now),
            commit_running: false,
            launch_mode: Some(
                crate::tool_run::handoff_wire::ToolRunLaunchModeWire::Handoff,
            ),
            launch: Some(handoff_envelope(&digest)),
            owner_log_path: None,
            starter,
        },
        Duration::from_secs(1),
    )
    .unwrap()
}

fn join_request(run_id: &str) -> ToolRunJoinRequestWire {
    ToolRunJoinRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.to_string(),
        joiner_kind: "monitor".into(),
        joiner_id: "mon-1".into(),
        agent: Some("agent-1".into()),
        requested_by: Some("agent-1".into()),
        now_ts: Some(12),
    }
}

fn release_request(run_id: &str) -> ToolRunReleaseJoinRequestWire {
    ToolRunReleaseJoinRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.to_string(),
        joiner_kind: "monitor".into(),
        joiner_id: "mon-1".into(),
        now_ts: Some(13),
    }
}

fn finish_succeeded(path: &std::path::Path, run_id: &str, now: i64) {
    finish(
        path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.into(),
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
            now_ts: Some(now),
            terminal_cause: Some(
                crate::tool_run::handoff_wire::ToolRunTerminalCauseWire::Exited,
            ),
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn shown_run(
    path: &std::path::Path,
    run_id: &str,
) -> crate::tool_run::wire::ToolRunWire {
    show_run(
        path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.into(),
        },
        Duration::from_secs(1),
    )
    .unwrap()
    .run
    .expect("shown run")
}

#[test]
fn handoff_begin_with_starter_persists_and_shows() {
    let (_temp, path) = store();
    let started = handoff_begin_with_starter(
        &path,
        "starter-1",
        10,
        Some(starter("agent-1", 999)),
    );
    assert_eq!(started.run.state, ToolRunStateWire::Created);
    let stored = started.run.starter.expect("starter");
    assert_eq!(stored.agent, "agent-1");
    assert_eq!(stored.pid, 999);
    assert_eq!(stored.boot_id.as_deref(), Some("boot-9"));
    assert_eq!(stored.process_start_identity.as_deref(), Some("boot-9:999"));
    let shown = shown_run(&path, "starter-1");
    let shown_starter = shown.starter.expect("shown starter");
    assert_eq!(shown_starter.agent, "agent-1");
    assert_eq!(shown_starter.pid, 999);
}

#[test]
fn starter_requires_handoff_and_valid_fields() {
    let (_temp, path) = store();
    let normalized = normalize_tool_definition(definition()).unwrap();
    // Starter without handoff launch_mode errors and writes no row.
    let err = begin(
        &path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some("starter-fore".into()),
            created_event_id: None,
            running_event_id: None,
            tool_name: Some("check".into()),
            definition: normalized.definition.clone(),
            extra_args: Vec::new(),
            display_argv: vec!["just".into(), "check".into()],
            private_argv: None,
            project: None,
            agent: None,
            workspace: None,
            bead: None,
            owner_kind: None,
            owner_id: None,
            parent_run_id: None,
            wrapper_pid: None,
            boot_id: None,
            process_start_identity: None,
            events_path: None,
            log_stdout_path: None,
            log_stderr_path: None,
            now_ts: Some(10),
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
            starter: Some(starter("agent-1", 999)),
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(err, ToolRunError::Invalid { .. }));
    assert!(err.to_string().contains("handoff"));
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "starter-fore".into(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(shown.run.is_none());
    // Empty and whitespace agents error.
    for agent in ["", "   "] {
        let err = begin(
            &path,
            ToolRunBeginRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: None,
                created_event_id: None,
                running_event_id: None,
                tool_name: Some("check".into()),
                definition: normalized.definition.clone(),
                extra_args: Vec::new(),
                display_argv: vec!["just".into(), "check".into()],
                private_argv: None,
                project: Some("sase".into()),
                agent: None,
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
                launch: Some(handoff_envelope(&normalized.digest)),
                owner_log_path: None,
                starter: Some(starter(agent, 999)),
            },
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(matches!(err, ToolRunError::Invalid { .. }));
    }
    // pid 0 and -1 error.
    for pid in [0, -1] {
        let err = begin(
            &path,
            ToolRunBeginRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: None,
                created_event_id: None,
                running_event_id: None,
                tool_name: Some("check".into()),
                definition: normalized.definition.clone(),
                extra_args: Vec::new(),
                display_argv: vec!["just".into(), "check".into()],
                private_argv: None,
                project: Some("sase".into()),
                agent: None,
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
                launch: Some(handoff_envelope(&normalized.digest)),
                owner_log_path: None,
                starter: Some(starter("agent-1", pid)),
            },
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(matches!(err, ToolRunError::Invalid { .. }));
    }
}

#[test]
fn continuation_mode_round_trips_through_claim() {
    let (_temp, path) = store();
    let normalized = normalize_tool_definition(definition()).unwrap();
    let digest = normalized.digest.clone();
    for mode in ["known", "always", "never"] {
        let run_id = format!("cont-{mode}");
        let mut envelope = handoff_envelope(&digest);
        envelope.continuation_mode = Some(mode.to_string());
        begin(
            &path,
            ToolRunBeginRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: Some(run_id.clone()),
                created_event_id: None,
                running_event_id: None,
                tool_name: Some("check".into()),
                definition: normalized.definition.clone(),
                extra_args: Vec::new(),
                display_argv: vec!["just".into(), "check".into()],
                private_argv: None,
                project: Some("sase".into()),
                agent: None,
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
                launch: Some(envelope),
                owner_log_path: None,
                starter: None,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let claimed =
            claim(&path, claim_request(&run_id), Duration::from_secs(1))
                .unwrap();
        assert_eq!(
            claimed
                .launch
                .as_ref()
                .unwrap()
                .continuation_mode
                .as_deref(),
            Some(mode)
        );
    }
    for mode in ["sometimes", ""] {
        let mut envelope = handoff_envelope(&digest);
        envelope.continuation_mode = Some(mode.to_string());
        let err = begin(
            &path,
            ToolRunBeginRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: None,
                created_event_id: None,
                running_event_id: None,
                tool_name: Some("check".into()),
                definition: normalized.definition.clone(),
                extra_args: Vec::new(),
                display_argv: vec!["just".into(), "check".into()],
                private_argv: None,
                project: Some("sase".into()),
                agent: None,
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
                launch: Some(envelope),
                owner_log_path: None,
                starter: None,
            },
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(matches!(err, ToolRunError::Invalid { .. }));
    }
    // An omitted field still claims with continuation_mode None.
    handoff_begin(&path, "cont-omitted", 10);
    let claimed =
        claim(&path, claim_request("cont-omitted"), Duration::from_secs(1))
            .unwrap();
    assert_eq!(claimed.launch.as_ref().unwrap().continuation_mode, None);
}

#[test]
fn join_first_write_replay_and_elsewhere() {
    let (_temp, path) = store();
    handoff_begin_with_starter(
        &path,
        "join-1",
        10,
        Some(starter("agent-1", 999)),
    );
    let first =
        join(&path, join_request("join-1"), Duration::from_secs(1)).unwrap();
    assert_eq!(first.outcome, ToolRunJoinOutcomeWire::Joined);
    assert!(!first.replayed);
    assert!(first.refusal.is_none());
    assert!(first.diagnostics.is_empty());
    let record = first.run.join.clone().expect("join record");
    assert_eq!(record.kind, "monitor");
    assert_eq!(record.id, "mon-1");
    assert_eq!(record.joined_ts, 12);
    assert_eq!(record.requested_by.as_deref(), Some("agent-1"));
    assert_eq!(first.run.state, ToolRunStateWire::Created);
    // show round-trips starter and join.
    let shown = shown_run(&path, "join-1");
    assert_eq!(shown.starter.as_ref().unwrap().agent, "agent-1");
    assert_eq!(shown.join.as_ref().unwrap().id, "mon-1");
    // Same kind and id replays without rewriting joined_ts.
    let mut again = join_request("join-1");
    again.now_ts = Some(99);
    let replayed = join(&path, again, Duration::from_secs(1)).unwrap();
    assert_eq!(replayed.outcome, ToolRunJoinOutcomeWire::Joined);
    assert!(replayed.replayed);
    assert_eq!(replayed.run.join.as_ref().unwrap().joined_ts, 12);
    // Different id, and same id with a different kind, both refuse.
    let mut other_id = join_request("join-1");
    other_id.joiner_id = "mon-2".into();
    let refused = join(&path, other_id, Duration::from_secs(1)).unwrap();
    assert_eq!(refused.outcome, ToolRunJoinOutcomeWire::Refused);
    assert_eq!(
        refused.refusal,
        Some(ToolRunJoinRefusalWire::JoinedElsewhere)
    );
    assert!(!refused.replayed);
    let mut other_kind = join_request("join-1");
    other_kind.joiner_kind = "other".into();
    let refused = join(&path, other_kind, Duration::from_secs(1)).unwrap();
    assert_eq!(
        refused.refusal,
        Some(ToolRunJoinRefusalWire::JoinedElsewhere)
    );
    assert_eq!(
        shown_run(&path, "join-1").join.as_ref().unwrap().id,
        "mon-1"
    );
}

#[test]
fn join_agent_rules() {
    let (_temp, path) = store();
    handoff_begin_with_starter(
        &path,
        "join-agent",
        10,
        Some(starter("agent-1", 999)),
    );
    // Omitted agent succeeds.
    let mut omitted = join_request("join-agent");
    omitted.agent = None;
    let ok = join(&path, omitted, Duration::from_secs(1)).unwrap();
    assert_eq!(ok.outcome, ToolRunJoinOutcomeWire::Joined);
    // A different agent is a mismatch and writes nothing, even when the
    // joiner kind and id match the existing join.
    let mut wrong = join_request("join-agent");
    wrong.agent = Some("agent-2".into());
    let refused = join(&path, wrong, Duration::from_secs(1)).unwrap();
    assert_eq!(refused.outcome, ToolRunJoinOutcomeWire::Refused);
    assert_eq!(refused.refusal, Some(ToolRunJoinRefusalWire::AgentMismatch));
    assert!(!refused.replayed);
    assert_eq!(
        shown_run(&path, "join-agent").join.as_ref().unwrap().id,
        "mon-1"
    );
    // An agent that is empty after trimming is Invalid, not a mismatch.
    let mut blank_agent = join_request("join-agent");
    blank_agent.agent = Some("   ".into());
    let err = join(&path, blank_agent, Duration::from_secs(1)).unwrap_err();
    assert!(matches!(err, ToolRunError::Invalid { .. }));
}

#[test]
fn join_refuses_without_starter_stop_or_settlement() {
    let (_temp, path) = store();
    // Foreground run without a starter.
    let created = begin_named(&path, 10);
    let refused = join(
        &path,
        join_request(&created.run.run_id),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(refused.refusal, Some(ToolRunJoinRefusalWire::NotDetached));
    // A settled foreground run is still not_detached, not settled.
    finish_succeeded(&path, &created.run.run_id, 11);
    let refused = join(
        &path,
        join_request(&created.run.run_id),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(refused.refusal, Some(ToolRunJoinRefusalWire::NotDetached));
    // Handoff run without a starter is not_detached too.
    handoff_begin(&path, "join-nostarter", 10);
    let refused = join(
        &path,
        join_request("join-nostarter"),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(refused.refusal, Some(ToolRunJoinRefusalWire::NotDetached));
    // Stop requested first: stop_requested and no join written.
    handoff_begin_with_starter(
        &path,
        "join-stop",
        10,
        Some(starter("agent-1", 999)),
    );
    request_stop(
        &path,
        ToolRunStopRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "join-stop".into(),
            requested_by: None,
            reason: None,
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let refused =
        join(&path, join_request("join-stop"), Duration::from_secs(1)).unwrap();
    assert_eq!(refused.refusal, Some(ToolRunJoinRefusalWire::StopRequested));
    assert!(shown_run(&path, "join-stop").join.is_none());
    // Finished first: settled wins even with a stop record present.
    handoff_begin_with_starter(
        &path,
        "join-settled",
        10,
        Some(starter("agent-1", 999)),
    );
    claim(&path, claim_request("join-settled"), Duration::from_secs(1))
        .unwrap();
    request_stop(
        &path,
        ToolRunStopRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "join-settled".into(),
            requested_by: None,
            reason: None,
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    finish_succeeded(&path, "join-settled", 12);
    let refused =
        join(&path, join_request("join-settled"), Duration::from_secs(1))
            .unwrap();
    assert_eq!(refused.refusal, Some(ToolRunJoinRefusalWire::Settled));
}

#[test]
fn join_rejects_bad_ids_before_a_store_write() {
    let (_temp, path) = store();
    handoff_begin_with_starter(
        &path,
        "join-bad",
        10,
        Some(starter("agent-1", 999)),
    );
    // Unknown run id is NotFound, the same error request_stop returns.
    let missing = join(
        &path,
        ToolRunJoinRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "missing".into(),
            joiner_kind: "monitor".into(),
            joiner_id: "mon-1".into(),
            agent: None,
            requested_by: None,
            now_ts: None,
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(missing, ToolRunError::NotFound { .. }));
    let stop_missing = request_stop(
        &path,
        ToolRunStopRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "missing".into(),
            requested_by: None,
            reason: None,
            now_ts: None,
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(
        (missing, stop_missing),
        (ToolRunError::NotFound { .. }, ToolRunError::NotFound { .. })
    ));
    // Blank run, kind, and id are Invalid and write nothing.
    for request in [
        ToolRunJoinRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "   ".into(),
            joiner_kind: "monitor".into(),
            joiner_id: "mon-1".into(),
            agent: None,
            requested_by: None,
            now_ts: None,
        },
        ToolRunJoinRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "join-bad".into(),
            joiner_kind: "  ".into(),
            joiner_id: "mon-1".into(),
            agent: None,
            requested_by: None,
            now_ts: None,
        },
        ToolRunJoinRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "join-bad".into(),
            joiner_kind: "monitor".into(),
            joiner_id: "  ".into(),
            agent: None,
            requested_by: None,
            now_ts: None,
        },
    ] {
        assert!(matches!(
            join(&path, request, Duration::from_secs(1)).unwrap_err(),
            ToolRunError::Invalid { .. }
        ));
    }
    assert!(shown_run(&path, "join-bad").join.is_none());
}

#[test]
fn release_join_matrix() {
    let (_temp, path) = store();
    handoff_begin_with_starter(
        &path,
        "rel-1",
        10,
        Some(starter("agent-1", 999)),
    );
    // Release with no join is not_joined and touches nothing.
    let empty =
        release_join(&path, release_request("rel-1"), Duration::from_secs(1))
            .unwrap();
    assert_eq!(empty.outcome, ToolRunReleaseJoinOutcomeWire::NotJoined);
    // Matching release clears the join and keeps state.
    join(&path, join_request("rel-1"), Duration::from_secs(1)).unwrap();
    let released =
        release_join(&path, release_request("rel-1"), Duration::from_secs(1))
            .unwrap();
    assert_eq!(released.outcome, ToolRunReleaseJoinOutcomeWire::Released);
    assert!(released.run.join.is_none());
    assert_eq!(released.run.state, ToolRunStateWire::Created);
    assert!(released.diagnostics.is_empty());
    // Repeat release is not_joined.
    let again =
        release_join(&path, release_request("rel-1"), Duration::from_secs(1))
            .unwrap();
    assert_eq!(again.outcome, ToolRunReleaseJoinOutcomeWire::NotJoined);
    // A different id leaves the join in place.
    join(&path, join_request("rel-1"), Duration::from_secs(1)).unwrap();
    let mut other = release_request("rel-1");
    other.joiner_id = "mon-2".into();
    let elsewhere = release_join(&path, other, Duration::from_secs(1)).unwrap();
    assert_eq!(
        elsewhere.outcome,
        ToolRunReleaseJoinOutcomeWire::JoinedElsewhere
    );
    assert_eq!(shown_run(&path, "rel-1").join.as_ref().unwrap().id, "mon-1");
    // Release after finish still returns released.
    claim(&path, claim_request("rel-1"), Duration::from_secs(1)).unwrap();
    finish_succeeded(&path, "rel-1", 14);
    let after_finish =
        release_join(&path, release_request("rel-1"), Duration::from_secs(1))
            .unwrap();
    assert_eq!(
        after_finish.outcome,
        ToolRunReleaseJoinOutcomeWire::Released
    );
    assert!(after_finish.run.join.is_none());
    // Unknown run is NotFound.
    let missing = release_join(
        &path,
        ToolRunReleaseJoinRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "missing".into(),
            joiner_kind: "monitor".into(),
            joiner_id: "mon-1".into(),
            now_ts: None,
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(missing, ToolRunError::NotFound { .. }));
}
