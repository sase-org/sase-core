use super::{definition, store};
use crate::tool_run::catalog::normalize_tool_definition;
use crate::tool_run::handoff_wire::{
    ToolRunLaunchEnvelopeWire, ToolRunOwnerFactWire, ToolRunOwnerStateWire,
};
use crate::tool_run::store::{begin, claim, reconcile, show_run};
use crate::tool_run::wire::{
    ToolLivenessObservationWire, ToolRunBeginRequestWire,
    ToolRunLivenessFactWire, ToolRunReconcileRequestWire,
    ToolRunShowRequestWire, ToolRunStateWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use std::time::Duration;

// NOTE: claim request wire lives in handoff_wire; import it directly.
use crate::tool_run::handoff_wire::ToolRunClaimRequestWire;

fn envelope(digest: &str) -> ToolRunLaunchEnvelopeWire {
    ToolRunLaunchEnvelopeWire {
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

fn begin_handoff(path: &std::path::Path, run_id: &str) {
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
            project: None,
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
            launch: Some(envelope(&digest)),
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn claim_it(path: &std::path::Path, run_id: &str) {
    claim(
        path,
        ToolRunClaimRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            owner_kind: "proc".into(),
            owner_id: "proc-1".into(),
            wrapper_pid: Some(4242),
            boot_id: Some("boot-1".into()),
            process_start_identity: Some("boot-1:4242".into()),
            owner_log_path: None,
            running_event_id: None,
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn owner(
    state: ToolRunOwnerStateWire,
    reason: Option<&str>,
    code: Option<i32>,
    stop: Option<bool>,
) -> ToolRunOwnerFactWire {
    ToolRunOwnerFactWire {
        kind: "proc".into(),
        id: "proc-1".into(),
        state,
        exit_code: code,
        termination_reason: reason.map(str::to_string),
        stop_requested: stop,
    }
}

fn fact(
    run_id: &str,
    observation: ToolLivenessObservationWire,
    owner: Option<ToolRunOwnerFactWire>,
) -> ToolRunLivenessFactWire {
    ToolRunLivenessFactWire {
        run_id: run_id.to_string(),
        wrapper_pid: Some(111),
        boot_id: Some("boot-1".into()),
        process_start_identity: Some("boot-1:111".into()),
        observation,
        reason: None,
        owner,
    }
}

fn worker_fact(
    run_id: &str,
    observation: ToolLivenessObservationWire,
    owner: Option<ToolRunOwnerFactWire>,
) -> ToolRunLivenessFactWire {
    ToolRunLivenessFactWire {
        run_id: run_id.to_string(),
        wrapper_pid: Some(4242),
        boot_id: Some("boot-1".into()),
        process_start_identity: Some("boot-1:4242".into()),
        observation,
        reason: None,
        owner,
    }
}

fn reconcile_one(
    path: &std::path::Path,
    facts: Vec<ToolRunLivenessFactWire>,
) -> crate::tool_run::wire::ToolRunReconcileResultWire {
    reconcile(
        path,
        ToolRunReconcileRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            facts,
            now_ts: Some(20),
        },
        Duration::from_secs(1),
    )
    .unwrap()
}

fn show_state(
    path: &std::path::Path,
    run_id: &str,
) -> crate::tool_run::wire::ToolRunWire {
    show_run(
        path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
        },
        Duration::from_secs(1),
    )
    .unwrap()
    .run
    .unwrap()
}

#[test]
fn created_owner_active_or_launcher_alive_is_noop() {
    let (_temp, path) = store();
    begin_handoff(&path, "c-1");
    let result = reconcile_one(
        &path,
        vec![fact(
            "c-1",
            ToolLivenessObservationWire::Dead,
            Some(owner(ToolRunOwnerStateWire::Active, None, None, None)),
        )],
    );
    assert!(result.settled.is_empty());
    assert_eq!(show_state(&path, "c-1").state, ToolRunStateWire::Created);
    let result = reconcile_one(
        &path,
        vec![fact(
            "c-1",
            ToolLivenessObservationWire::Alive,
            Some(owner(ToolRunOwnerStateWire::Unknown, None, None, None)),
        )],
    );
    assert!(result.settled.is_empty());
}

#[test]
fn created_owner_terminal_stop_settles_signaled() {
    let (_temp, path) = store();
    begin_handoff(&path, "c-2");
    let result = reconcile_one(
        &path,
        vec![fact(
            "c-2",
            ToolLivenessObservationWire::Dead,
            Some(owner(
                ToolRunOwnerStateWire::Terminal,
                Some("stop"),
                None,
                None,
            )),
        )],
    );
    assert_eq!(result.settled.len(), 1);
    let run = show_state(&path, "c-2");
    assert_eq!(run.state, ToolRunStateWire::Signaled);
    assert_eq!(run.terminal_cause.as_deref(), Some("stop_requested"));
    assert_eq!(run.settled_by.as_deref(), Some("reconcile"));
}

#[test]
fn created_owner_terminal_otherwise_settles_launch_failed() {
    let (_temp, path) = store();
    begin_handoff(&path, "c-3");
    let result = reconcile_one(
        &path,
        vec![fact(
            "c-3",
            ToolLivenessObservationWire::Dead,
            Some(owner(
                ToolRunOwnerStateWire::Terminal,
                Some("error"),
                Some(1),
                None,
            )),
        )],
    );
    assert_eq!(result.settled.len(), 1);
    let run = show_state(&path, "c-3");
    assert_eq!(run.state, ToolRunStateWire::Failed);
    assert_eq!(run.terminal_cause.as_deref(), Some("launch_failed"));
}

#[test]
fn created_owner_missing_launcher_dead_settles_launch_failed() {
    let (_temp, path) = store();
    begin_handoff(&path, "c-4");
    let result = reconcile_one(
        &path,
        vec![fact(
            "c-4",
            ToolLivenessObservationWire::Dead,
            Some(owner(ToolRunOwnerStateWire::Missing, None, None, None)),
        )],
    );
    assert_eq!(result.settled.len(), 1);
    assert_eq!(show_state(&path, "c-4").state, ToolRunStateWire::Failed);
}

#[test]
fn created_unknown_is_noop_with_diagnostic() {
    let (_temp, path) = store();
    begin_handoff(&path, "c-5");
    let result = reconcile_one(
        &path,
        vec![fact("c-5", ToolLivenessObservationWire::Dead, None)],
    );
    assert!(result.settled.is_empty());
    assert!(!result.diagnostics.is_empty());
    // mismatched owner counts as unknown
    let mut bad = owner(
        ToolRunOwnerStateWire::Terminal,
        Some("error"),
        Some(1),
        None,
    );
    bad.id = "other".into();
    let result = reconcile_one(
        &path,
        vec![fact("c-5", ToolLivenessObservationWire::Dead, Some(bad))],
    );
    assert!(result.settled.is_empty());
}

#[test]
fn running_worker_alive_or_owner_active_is_noop() {
    let (_temp, path) = store();
    begin_handoff(&path, "r-1");
    claim_it(&path, "r-1");
    let result = reconcile_one(
        &path,
        vec![worker_fact(
            "r-1",
            ToolLivenessObservationWire::Alive,
            Some(owner(
                ToolRunOwnerStateWire::Terminal,
                Some("success"),
                Some(0),
                None,
            )),
        )],
    );
    assert!(result.settled.is_empty());
    let result = reconcile_one(
        &path,
        vec![worker_fact(
            "r-1",
            ToolLivenessObservationWire::Dead,
            Some(owner(ToolRunOwnerStateWire::Active, None, None, None)),
        )],
    );
    assert!(result.settled.is_empty());
}

#[test]
fn running_recovers_owner_success_and_error() {
    let (_temp, path) = store();
    begin_handoff(&path, "r-2");
    claim_it(&path, "r-2");
    let result = reconcile_one(
        &path,
        vec![worker_fact(
            "r-2",
            ToolLivenessObservationWire::Dead,
            Some(owner(
                ToolRunOwnerStateWire::Terminal,
                Some("success"),
                Some(0),
                None,
            )),
        )],
    );
    assert_eq!(result.settled.len(), 1);
    let run = show_state(&path, "r-2");
    assert_eq!(run.state, ToolRunStateWire::Succeeded);
    assert_eq!(run.exit_code, Some(0));
    assert_eq!(run.terminal_cause.as_deref(), Some("exited"));
    assert_eq!(run.settled_by.as_deref(), Some("owner"));
    begin_handoff(&path, "r-3");
    claim_it(&path, "r-3");
    let result = reconcile_one(
        &path,
        vec![worker_fact(
            "r-3",
            ToolLivenessObservationWire::Dead,
            Some(owner(
                ToolRunOwnerStateWire::Terminal,
                Some("error"),
                Some(3),
                None,
            )),
        )],
    );
    let run = show_state(&path, "r-3");
    assert_eq!(run.state, ToolRunStateWire::Failed);
    assert_eq!(run.exit_code, Some(3));
    assert_eq!(result.settled[0].settled_by, "owner");
}

#[test]
fn running_timeout_and_stop_settle_signaled() {
    let (_temp, path) = store();
    begin_handoff(&path, "r-4");
    claim_it(&path, "r-4");
    let result: crate::tool_run::wire::ToolRunReconcileResultWire =
        reconcile_one(
            &path,
            vec![worker_fact(
                "r-4",
                ToolLivenessObservationWire::Dead,
                Some(owner(
                    ToolRunOwnerStateWire::Terminal,
                    Some("total-timeout"),
                    None,
                    None,
                )),
            )],
        );
    assert_eq!(result.settled.len(), 1);
    let run = show_state(&path, "r-4");
    assert_eq!(run.state, ToolRunStateWire::Signaled);
    assert_eq!(run.terminal_cause.as_deref(), Some("timeout"));
    assert!(run.exit_code.is_none());
    begin_handoff(&path, "r-5");
    claim_it(&path, "r-5");
    let result = reconcile_one(
        &path,
        vec![worker_fact(
            "r-5",
            ToolLivenessObservationWire::Dead,
            Some(owner(
                ToolRunOwnerStateWire::Terminal,
                Some("stop"),
                None,
                Some(true),
            )),
        )],
    );
    assert_eq!(result.settled.len(), 1);
    assert_eq!(
        show_state(&path, "r-5").terminal_cause.as_deref(),
        Some("stop_requested")
    );
}

#[test]
fn running_supervisor_loss_or_missing_settles_owner_lost() {
    let (_temp, path) = store();
    begin_handoff(&path, "r-6");
    claim_it(&path, "r-6");
    let result = reconcile_one(
        &path,
        vec![worker_fact(
            "r-6",
            ToolLivenessObservationWire::Dead,
            Some(owner(
                ToolRunOwnerStateWire::Terminal,
                Some("reboot"),
                None,
                None,
            )),
        )],
    );
    assert_eq!(
        show_state(&path, "r-6").terminal_cause.as_deref(),
        Some("owner_lost")
    );
    assert_eq!(result.settled[0].state, ToolRunStateWire::Lost);
    begin_handoff(&path, "r-7");
    claim_it(&path, "r-7");
    let result = reconcile_one(
        &path,
        vec![worker_fact(
            "r-7",
            ToolLivenessObservationWire::Dead,
            Some(owner(ToolRunOwnerStateWire::Missing, None, None, None)),
        )],
    );
    assert_eq!(result.settled.len(), 1);
    assert_eq!(show_state(&path, "r-7").state, ToolRunStateWire::Lost);
    // error without usable exit code
    begin_handoff(&path, "r-8");
    claim_it(&path, "r-8");
    let result = reconcile_one(
        &path,
        vec![worker_fact(
            "r-8",
            ToolLivenessObservationWire::Dead,
            Some(owner(
                ToolRunOwnerStateWire::Terminal,
                Some("error"),
                None,
                None,
            )),
        )],
    );
    assert_eq!(
        show_state(&path, "r-8").terminal_cause.as_deref(),
        Some("owner_lost")
    );
    let _ = result;
}

#[test]
fn running_unknown_owner_is_noop() {
    let (_temp, path) = store();
    begin_handoff(&path, "r-9");
    claim_it(&path, "r-9");
    let result = reconcile_one(
        &path,
        vec![worker_fact(
            "r-9",
            ToolLivenessObservationWire::Dead,
            Some(owner(ToolRunOwnerStateWire::Unknown, None, None, None)),
        )],
    );
    assert!(result.settled.is_empty());
    assert!(!result.diagnostics.is_empty());
}

#[test]
fn settled_reconcile_is_noop_without_new_event() {
    let (_temp, path) = store();
    begin_handoff(&path, "r-10");
    claim_it(&path, "r-10");
    let first = reconcile_one(
        &path,
        vec![worker_fact(
            "r-10",
            ToolLivenessObservationWire::Dead,
            Some(owner(
                ToolRunOwnerStateWire::Terminal,
                Some("success"),
                Some(0),
                None,
            )),
        )],
    );
    assert_eq!(first.settled.len(), 1);
    let before = show_state(&path, "r-10");
    let second = reconcile_one(
        &path,
        vec![worker_fact(
            "r-10",
            ToolLivenessObservationWire::Dead,
            Some(owner(
                ToolRunOwnerStateWire::Terminal,
                Some("success"),
                Some(0),
                None,
            )),
        )],
    );
    assert!(second.settled.is_empty());
    let after = show_state(&path, "r-10");
    assert_eq!(before.settled_ts, after.settled_ts);
}

#[test]
fn foreground_lost_records_wrapper_lost_and_reap_only_unowned() {
    let (_temp, path) = store();
    // unowned foreground still reaps
    let normalized = normalize_tool_definition(definition()).unwrap();
    begin(
        &path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some("f-1".into()),
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
            wrapper_pid: Some(1),
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
        },
        Duration::from_secs(1),
    )
    .unwrap();
    crate::tool_run::store::observe(
        &path,
        crate::tool_run::wire::ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "f-1".into(),
            child_pid: Some(99),
            child_pgid: Some(99),
            child_process_start_identity: Some("boot-1:99".into()),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let result = reconcile_one(
        &path,
        vec![ToolRunLivenessFactWire {
            run_id: "f-1".into(),
            wrapper_pid: Some(1),
            boot_id: None,
            process_start_identity: None,
            observation: ToolLivenessObservationWire::Dead,
            reason: None,
            owner: None,
        }],
    );
    assert_eq!(result.marked_lost, vec!["f-1".to_string()]);
    assert_eq!(result.reap_candidates.len(), 1);
    let run = show_state(&path, "f-1");
    assert_eq!(run.terminal_cause.as_deref(), Some("wrapper_lost"));
    assert_eq!(run.settled_by.as_deref(), Some("reconcile"));
    // owned foreground yields no reap
    let normalized = normalize_tool_definition(definition()).unwrap();
    begin(
        &path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some("f-2".into()),
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
            owner_id: Some("proc-9".into()),
            parent_run_id: None,
            wrapper_pid: Some(1),
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
        },
        Duration::from_secs(1),
    )
    .unwrap();
    crate::tool_run::store::observe(
        &path,
        crate::tool_run::wire::ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "f-2".into(),
            child_pid: Some(100),
            child_pgid: Some(100),
            child_process_start_identity: Some("boot-1:100".into()),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let result = reconcile_one(
        &path,
        vec![ToolRunLivenessFactWire {
            run_id: "f-2".into(),
            wrapper_pid: Some(1),
            boot_id: None,
            process_start_identity: None,
            observation: ToolLivenessObservationWire::Dead,
            reason: None,
            owner: None,
        }],
    );
    assert!(result.reap_candidates.is_empty());
    assert_eq!(show_state(&path, "f-2").state, ToolRunStateWire::Lost);
}

#[test]
fn handoff_never_reaps_and_settled_list_is_correct() {
    let (_temp, path) = store();
    begin_handoff(&path, "h-1");
    crate::tool_run::store::observe(
        &path,
        crate::tool_run::wire::ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "h-1".into(),
            child_pid: Some(50),
            child_pgid: Some(50),
            child_process_start_identity: Some("boot-1:50".into()),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let result = reconcile_one(
        &path,
        vec![fact(
            "h-1",
            ToolLivenessObservationWire::Dead,
            Some(owner(ToolRunOwnerStateWire::Missing, None, None, None)),
        )],
    );
    assert!(result.reap_candidates.is_empty());
    assert_eq!(result.settled.len(), 1);
    assert_eq!(result.settled[0].run_id, "h-1");
}
