//! Host-wide admission: FIFO slot decisions across projects, settlement
//! release, abandonment, force, demand-merge preservation, projections,
//! and the never-over-limit race.

use super::super::admission::queue_sql;
use super::{definition, store};
use crate::tool_run::admission_wire::{
    ToolRunAdmissionEnterRequestWire, ToolRunAdmissionForceReasonWire,
    ToolRunAdmissionForceRequestWire, ToolRunAdmissionOutcomeWire,
    ToolRunAdmissionPollRequestWire, ToolRunAdmissionResultWire,
    ToolRunAdmissionStateWire, ToolRunAdmissionWire, ToolRunAdmittedByWire,
};
use crate::tool_run::catalog::normalize_tool_definition;
use crate::tool_run::demand_wire::{
    ToolRunDemandContextWire, ToolRunDemandWire,
    ToolRunRecordDemandRequestWire, ToolRunResourceUsageWire,
};
use crate::tool_run::projection::{
    tool_run_briefs, tool_run_detail, tool_run_live_glance,
    tool_run_node_summaries, ToolRunBriefsRequestWire,
    ToolRunDetailRequestWire, ToolRunLiveGlanceRequestWire,
    ToolRunNodeSelectorWire, ToolRunNodeSummariesRequestWire,
};
use crate::tool_run::store::{
    admission_enter, admission_force, admission_poll, begin, finish, reconcile,
    record_demand, show_run,
};
use crate::tool_run::wire::{
    ToolLivenessObservationWire, ToolRunBeginRequestWire,
    ToolRunFinishRequestWire, ToolRunLivenessFactWire,
    ToolRunReconcileRequestWire, ToolRunShowRequestWire, ToolRunShowResultWire,
    ToolRunStateWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use crate::tool_run::ToolRunError;
use rusqlite::Connection;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

/// Injected request clock: 2023-11-14, far from the live clock, so
/// settlement stamps in seconds are honored exactly.
const T0_S: i64 = 1_700_000_000;
const T0_MS: i64 = T0_S * 1000;
const BUSY: Duration = Duration::from_secs(10);

fn begin_request(
    project: &str,
    commit_running: bool,
    now_s: i64,
) -> ToolRunBeginRequestWire {
    let normalized = normalize_tool_definition(definition()).unwrap();
    ToolRunBeginRequestWire {
        starter: None,
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: None,
        created_event_id: None,
        running_event_id: None,
        tool_name: Some("check".into()),
        definition: normalized.definition,
        extra_args: Vec::new(),
        display_argv: vec!["just".into(), "check".into()],
        private_argv: None,
        project: Some(project.into()),
        agent: Some(format!("agent-{project}")),
        workspace: None,
        bead: None,
        owner_kind: None,
        owner_id: None,
        parent_run_id: None,
        wrapper_pid: Some(4242),
        boot_id: Some("boot-1".into()),
        process_start_identity: Some("start-1".into()),
        events_path: None,
        log_stdout_path: None,
        log_stderr_path: None,
        now_ts: Some(now_s),
        commit_running,
        launch_mode: None,
        launch: None,
        owner_log_path: None,
    }
}

fn begin_run(path: &Path, project: &str) -> String {
    begin(path, begin_request(project, true, T0_S), BUSY)
        .unwrap()
        .run
        .run_id
}

fn enter_key(
    path: &Path,
    run_id: &str,
    key: &str,
    limit: u32,
    now_ms: i64,
) -> ToolRunAdmissionResultWire {
    admission_enter(
        path,
        ToolRunAdmissionEnterRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.into(),
            key: key.into(),
            limit,
            now_ms: Some(now_ms),
        },
        BUSY,
    )
    .unwrap()
}

fn enter(
    path: &Path,
    run_id: &str,
    limit: u32,
    now_ms: i64,
) -> ToolRunAdmissionResultWire {
    enter_key(path, run_id, "check", limit, now_ms)
}

fn poll_at(
    path: &Path,
    run_id: &str,
    now_ms: Option<i64>,
    busy: Duration,
) -> Result<ToolRunAdmissionResultWire, ToolRunError> {
    admission_poll(
        path,
        ToolRunAdmissionPollRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.into(),
            now_ms,
        },
        busy,
    )
}

fn poll(path: &Path, run_id: &str, now_ms: i64) -> ToolRunAdmissionResultWire {
    poll_at(path, run_id, Some(now_ms), BUSY).unwrap()
}

fn force(path: &Path, run_id: &str, now_ms: i64) -> ToolRunAdmissionResultWire {
    admission_force(
        path,
        ToolRunAdmissionForceRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.into(),
            reason: ToolRunAdmissionForceReasonWire::MaxWait,
            now_ms: Some(now_ms),
        },
        BUSY,
    )
    .unwrap()
}

fn finish_as(
    path: &Path,
    run_id: &str,
    state: ToolRunStateWire,
    now_s: Option<i64>,
) {
    let (exit_code, signal) = match state {
        ToolRunStateWire::Succeeded => (Some(0), None),
        ToolRunStateWire::Signaled => (None, Some(15)),
        _ => (None, None),
    };
    finish(
        path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.into(),
            event_id: None,
            state,
            exit_code,
            signal,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: Some(5),
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: now_s,
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        BUSY,
    )
    .unwrap();
}

fn succeed(path: &Path, run_id: &str) {
    finish_as(path, run_id, ToolRunStateWire::Succeeded, Some(T0_S + 600));
}

fn show(path: &Path, run_id: &str) -> ToolRunShowResultWire {
    show_run(
        path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.into(),
        },
        BUSY,
    )
    .unwrap()
}

fn stored_record(path: &Path, run_id: &str) -> Option<ToolRunAdmissionWire> {
    show(path, run_id)
        .run
        .and_then(|run| run.demand)
        .and_then(|demand| demand.admission)
}

fn waiter_ids(result: &ToolRunAdmissionResultWire) -> Vec<String> {
    result
        .waiters_ahead
        .iter()
        .map(|peer| peer.run_id.clone())
        .collect()
}

#[test]
fn enter_admits_under_the_limit_and_queues_the_rest() {
    let (_temp, path) = store();
    let first = begin_run(&path, "sase");
    let second = begin_run(&path, "sase");
    let third = begin_run(&path, "sase");

    let a = enter(&path, &first, 2, T0_MS);
    assert_eq!(a.outcome, ToolRunAdmissionOutcomeWire::Admitted);
    assert!(a.changed);
    let record = a.admission.clone().unwrap();
    assert_eq!(record.state, ToolRunAdmissionStateWire::Admitted);
    assert_eq!(record.admitted_by, Some(ToolRunAdmittedByWire::Slot));
    assert_eq!(record.wait_ms, Some(0));
    assert_eq!(record.admitted_ts_ms, Some(T0_MS));
    assert_eq!(a.holders.len(), 1);

    let b = enter(&path, &second, 2, T0_MS + 1);
    assert_eq!(b.outcome, ToolRunAdmissionOutcomeWire::Admitted);
    assert_eq!(b.holders.len(), 2);

    let c = enter(&path, &third, 2, T0_MS + 2);
    assert_eq!(c.outcome, ToolRunAdmissionOutcomeWire::Waiting);
    assert_eq!(c.key.as_deref(), Some("check"));
    assert_eq!(c.limit, Some(2));
    assert_eq!(c.ahead, 0);
    assert!(c.waiters_ahead.is_empty());
    let holder_ids: Vec<&str> =
        c.holders.iter().map(|peer| peer.run_id.as_str()).collect();
    assert_eq!(holder_ids, [first.as_str(), second.as_str()]);
    // Holders carry the wrapper identity the gate probes for liveness.
    assert_eq!(c.holders[0].wrapper_pid, Some(4242));
    assert_eq!(c.holders[0].boot_id.as_deref(), Some("boot-1"));
    assert_eq!(
        c.holders[0].process_start_identity.as_deref(),
        Some("start-1")
    );
    assert_eq!(c.holders[0].admitted_ts_ms, Some(T0_MS));
    let waiting = stored_record(&path, &third).unwrap();
    assert_eq!(waiting.state, ToolRunAdmissionStateWire::Waiting);
    assert_eq!(waiting.requested_ts_ms, T0_MS + 2);
    assert_eq!(waiting.ahead_at_request, 0);
    assert_eq!(waiting.admitted_ts_ms, None);
    assert_eq!(waiting.wait_ms, None);
    assert!(waiting.seq > 0);
}

#[test]
fn fifo_orders_by_request_time_then_seq() {
    let (_temp, path) = store();
    let holder = begin_run(&path, "sase");
    let late = begin_run(&path, "sase");
    let early = begin_run(&path, "sase");
    let tied = begin_run(&path, "sase");
    enter(&path, &holder, 1, T0_MS);
    // `late` enters first (lower seq) but asks later.
    let late_entry = enter(&path, &late, 1, T0_MS + 2_000);
    let early_entry = enter(&path, &early, 1, T0_MS + 1_000);
    // Same request time as `early`; its higher seq breaks the tie.
    let tied_entry = enter(&path, &tied, 1, T0_MS + 1_000);
    assert!(
        late_entry.admission.as_ref().unwrap().seq
            < early_entry.admission.as_ref().unwrap().seq
    );
    assert!(
        early_entry.admission.as_ref().unwrap().seq
            < tied_entry.admission.as_ref().unwrap().seq
    );
    assert_eq!(late_entry.admission.as_ref().unwrap().ahead_at_request, 0);
    assert_eq!(tied_entry.admission.as_ref().unwrap().ahead_at_request, 1);

    let now = T0_MS + 5_000;
    assert_eq!(poll(&path, &early, now).ahead, 0);
    let tied_poll = poll(&path, &tied, now);
    assert_eq!(tied_poll.ahead, 1);
    assert_eq!(waiter_ids(&tied_poll), std::slice::from_ref(&early));
    let late_poll = poll(&path, &late, now);
    assert_eq!(late_poll.ahead, 2);
    assert_eq!(waiter_ids(&late_poll), [early.clone(), tied.clone()]);

    succeed(&path, &holder);
    // Only the head of the queue is eligible, whoever polls first.
    let late_poll = poll(&path, &late, now);
    assert_eq!(late_poll.outcome, ToolRunAdmissionOutcomeWire::Waiting);
    assert!(!late_poll.changed);
    assert_eq!(
        poll(&path, &tied, now).outcome,
        ToolRunAdmissionOutcomeWire::Waiting
    );
    let granted = poll(&path, &early, now);
    assert_eq!(granted.outcome, ToolRunAdmissionOutcomeWire::Admitted);
    assert!(granted.changed);
    assert_eq!(granted.admission.unwrap().wait_ms, Some(4_000));

    succeed(&path, &early);
    assert_eq!(
        poll(&path, &late, now).outcome,
        ToolRunAdmissionOutcomeWire::Waiting
    );
    assert_eq!(
        poll(&path, &tied, now).outcome,
        ToolRunAdmissionOutcomeWire::Admitted
    );
    succeed(&path, &tied);
    assert_eq!(
        poll(&path, &late, now).outcome,
        ToolRunAdmissionOutcomeWire::Admitted
    );
}

#[test]
fn one_key_counts_across_projects_and_keys_are_independent() {
    let (_temp, path) = store();
    let sase = begin_run(&path, "sase");
    let tailnet = begin_run(&path, "tailnet");
    let lint = begin_run(&path, "tailnet");
    assert_eq!(
        enter(&path, &sase, 1, T0_MS).outcome,
        ToolRunAdmissionOutcomeWire::Admitted
    );
    let waiting = enter(&path, &tailnet, 1, T0_MS + 1);
    assert_eq!(waiting.outcome, ToolRunAdmissionOutcomeWire::Waiting);
    assert_eq!(waiting.holders.len(), 1);
    assert_eq!(waiting.holders[0].project.as_deref(), Some("sase"));
    assert_eq!(waiting.holders[0].agent.as_deref(), Some("agent-sase"));
    let other_key = enter_key(&path, &lint, "lint", 1, T0_MS + 2);
    assert_eq!(other_key.outcome, ToolRunAdmissionOutcomeWire::Admitted);
    assert_eq!(other_key.holders.len(), 1);
}

#[test]
fn enter_is_idempotent_per_run() {
    let (_temp, path) = store();
    let holder = begin_run(&path, "sase");
    let waiter = begin_run(&path, "sase");
    enter(&path, &holder, 1, T0_MS);
    let first = enter(&path, &waiter, 1, T0_MS + 10);
    assert!(first.changed);
    let again = enter(&path, &waiter, 1, T0_MS + 99);
    assert!(!again.changed);
    assert!(again.diagnostics.is_empty());
    assert_eq!(again.admission, first.admission);
    let other_limit = enter(&path, &waiter, 5, T0_MS + 120);
    assert!(!other_limit.changed);
    assert_eq!(other_limit.admission, first.admission);
    assert_eq!(other_limit.limit, Some(1));
    assert_eq!(other_limit.diagnostics.len(), 1);
    assert!(other_limit.diagnostics[0].contains("kept the first record"));
    // The holder's repeat enter keeps its admission too.
    let holder_again = enter(&path, &holder, 1, T0_MS + 500);
    assert_eq!(holder_again.outcome, ToolRunAdmissionOutcomeWire::Admitted);
    assert!(!holder_again.changed);
    assert_eq!(holder_again.admission.unwrap().admitted_ts_ms, Some(T0_MS));
}

#[test]
fn enter_refuses_created_runs_and_reports_settled_or_missing_runs() {
    let (_temp, path) = store();
    let created = begin(&path, begin_request("sase", false, T0_S), BUSY)
        .unwrap()
        .run
        .run_id;
    let error = admission_enter(
        &path,
        ToolRunAdmissionEnterRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: created.clone(),
            key: "check".into(),
            limit: 1,
            now_ms: Some(T0_MS),
        },
        BUSY,
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("admission requires a running run"));
    assert!(stored_record(&path, &created).is_none());

    let settled = begin_run(&path, "sase");
    succeed(&path, &settled);
    let refused = enter(&path, &settled, 1, T0_MS);
    assert_eq!(refused.outcome, ToolRunAdmissionOutcomeWire::Settled);
    assert!(!refused.changed);
    assert!(stored_record(&path, &settled).is_none());

    let missing = enter(&path, "no-such-run", 1, T0_MS);
    assert_eq!(missing.outcome, ToolRunAdmissionOutcomeWire::NotFound);

    // Poll needs an entered record, and validates its inputs.
    let running = begin_run(&path, "sase");
    let error = poll_at(&path, &running, Some(T0_MS), BUSY).unwrap_err();
    assert!(error.to_string().contains("no admission record"));
    let invalid = admission_enter(
        &path,
        ToolRunAdmissionEnterRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: running.clone(),
            key: "check".into(),
            limit: 0,
            now_ms: None,
        },
        BUSY,
    )
    .unwrap_err();
    assert!(matches!(invalid, ToolRunError::Invalid { .. }));
    let (_other, absent) = store();
    let no_store = poll_at(&absent, "run", None, BUSY).unwrap();
    assert_eq!(no_store.outcome, ToolRunAdmissionOutcomeWire::NotFound);
    assert!(!absent.exists());
}

#[test]
fn racing_pollers_never_exceed_the_limit() {
    const LIMIT: usize = 2;
    const RUNNERS: usize = 6;
    let (_temp, path) = store();
    // Create the store before the race so every thread opens a live file.
    let warm = begin_run(&path, "sase");
    succeed(&path, &warm);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(RUNNERS));
    let mut handles = Vec::new();
    for index in 0..RUNNERS {
        let path = path.clone();
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let project = if index % 2 == 0 { "sase" } else { "tailnet" };
            let run_id = begin_run(&path, project);
            barrier.wait();
            let mut result = admission_enter(
                &path,
                ToolRunAdmissionEnterRequestWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    run_id: run_id.clone(),
                    key: "check".into(),
                    limit: LIMIT as u32,
                    now_ms: None,
                },
                BUSY,
            )
            .unwrap();
            while result.outcome != ToolRunAdmissionOutcomeWire::Admitted {
                assert_eq!(
                    result.outcome,
                    ToolRunAdmissionOutcomeWire::Waiting
                );
                thread::sleep(Duration::from_millis(2));
                result = poll_at(&path, &run_id, None, BUSY).unwrap();
            }
            // The slot is granted in core before this counter moves and
            // released by settlement after it drops, so the counter can
            // only undercount the store's holders.
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            assert!(result.holders.len() <= LIMIT, "{result:?}");
            thread::sleep(Duration::from_millis(15));
            active.fetch_sub(1, Ordering::SeqCst);
            finish_as(&path, &run_id, ToolRunStateWire::Succeeded, None);
            run_id
        }));
    }
    let run_ids: Vec<String> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    let peak = peak.load(Ordering::SeqCst);
    assert!(peak <= LIMIT, "peak {peak} exceeded limit {LIMIT}");
    assert!(peak >= 1);
    for run_id in run_ids {
        let record = stored_record(&path, &run_id).unwrap();
        assert_eq!(record.state, ToolRunAdmissionStateWire::Admitted);
        assert_eq!(record.admitted_by, Some(ToolRunAdmittedByWire::Slot));
        assert!(record.wait_ms.is_some());
    }
}

#[test]
fn settlement_and_lost_reconcile_free_slots() {
    let (_temp, path) = store();
    let holder = begin_run(&path, "sase");
    let second = begin_run(&path, "sase");
    let third = begin_run(&path, "sase");
    enter(&path, &holder, 1, T0_MS);
    enter(&path, &second, 1, T0_MS + 1_000);
    enter(&path, &third, 1, T0_MS + 2_000);

    succeed(&path, &holder);
    let granted = poll(&path, &second, T0_MS + 3_000);
    assert_eq!(granted.outcome, ToolRunAdmissionOutcomeWire::Admitted);
    assert_eq!(granted.admission.unwrap().wait_ms, Some(2_000));

    // A crashed holder is freed once reconcile marks its wrapper lost.
    let lost = reconcile(
        &path,
        ToolRunReconcileRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            facts: vec![ToolRunLivenessFactWire {
                run_id: second.clone(),
                wrapper_pid: Some(4242),
                boot_id: Some("boot-1".into()),
                process_start_identity: Some("start-1".into()),
                observation: ToolLivenessObservationWire::Dead,
                reason: None,
                owner: None,
            }],
            now_ts: Some(T0_S + 4),
        },
        BUSY,
    )
    .unwrap();
    assert_eq!(lost.marked_lost, std::slice::from_ref(&second));
    // The lost holder keeps its admitted record as evidence.
    assert_eq!(
        stored_record(&path, &second).unwrap().state,
        ToolRunAdmissionStateWire::Admitted
    );
    let freed = poll(&path, &third, T0_MS + 5_000);
    assert_eq!(freed.outcome, ToolRunAdmissionOutcomeWire::Admitted);
    assert_eq!(freed.holders.len(), 1);
}

#[test]
fn settling_while_waiting_finalizes_abandoned() {
    let (_temp, path) = store();
    let holder = begin_run(&path, "sase");
    let stopped = begin_run(&path, "sase");
    let lost = begin_run(&path, "sase");
    let behind = begin_run(&path, "sase");
    enter(&path, &holder, 1, T0_MS);
    enter(&path, &stopped, 1, T0_MS);
    enter(&path, &lost, 1, T0_MS + 1_000);
    enter(&path, &behind, 1, T0_MS + 2_000);

    finish_as(&path, &stopped, ToolRunStateWire::Signaled, Some(T0_S + 40));
    let record = stored_record(&path, &stopped).unwrap();
    assert_eq!(record.state, ToolRunAdmissionStateWire::Abandoned);
    assert_eq!(record.wait_ms, Some(40_000));
    assert_eq!(record.admitted_ts_ms, None);
    assert_eq!(record.admitted_by, None);
    assert_eq!(
        poll(&path, &stopped, T0_MS).outcome,
        ToolRunAdmissionOutcomeWire::Settled
    );

    reconcile(
        &path,
        ToolRunReconcileRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            facts: vec![ToolRunLivenessFactWire {
                run_id: lost.clone(),
                wrapper_pid: Some(4242),
                boot_id: Some("boot-1".into()),
                process_start_identity: Some("start-1".into()),
                observation: ToolLivenessObservationWire::Dead,
                reason: None,
                owner: None,
            }],
            now_ts: Some(T0_S + 61),
        },
        BUSY,
    )
    .unwrap();
    let record = stored_record(&path, &lost).unwrap();
    assert_eq!(record.state, ToolRunAdmissionStateWire::Abandoned);
    assert_eq!(record.wait_ms, Some(60_000));

    // Dead waiters ahead never wedge the run behind them.
    let behind_poll = poll(&path, &behind, T0_MS + 62_000);
    assert_eq!(behind_poll.ahead, 0);
    assert!(behind_poll.waiters_ahead.is_empty());
    succeed(&path, &holder);
    assert_eq!(
        poll(&path, &behind, T0_MS + 63_000).outcome,
        ToolRunAdmissionOutcomeWire::Admitted
    );
}

#[test]
fn force_admits_over_the_limit_and_counts_as_a_holder() {
    let (_temp, path) = store();
    let holder = begin_run(&path, "sase");
    let forced = begin_run(&path, "sase");
    let later = begin_run(&path, "sase");
    enter(&path, &holder, 1, T0_MS);
    enter(&path, &forced, 1, T0_MS + 1_000);

    let result = force(&path, &forced, T0_MS + 3_601_000);
    assert_eq!(result.outcome, ToolRunAdmissionOutcomeWire::Admitted);
    assert!(result.changed);
    assert_eq!(result.holders.len(), 2);
    let record = result.admission.unwrap();
    assert_eq!(record.admitted_by, Some(ToolRunAdmittedByWire::MaxWait));
    assert_eq!(record.wait_ms, Some(3_600_000));
    // Forcing an admitted run changes nothing.
    let again = force(&path, &forced, T0_MS + 3_700_000);
    assert!(!again.changed);
    assert_eq!(
        again.admission.unwrap().admitted_ts_ms,
        Some(T0_MS + 3_601_000)
    );

    // Two holders over a limit of one: one waiter-equivalent ahead.
    let queued = enter(&path, &later, 1, T0_MS + 3_602_000);
    assert_eq!(queued.outcome, ToolRunAdmissionOutcomeWire::Waiting);
    assert_eq!(queued.ahead, 1);
    assert_eq!(queued.admission.unwrap().ahead_at_request, 1);
    succeed(&path, &holder);
    let still = poll(&path, &later, T0_MS + 3_603_000);
    assert_eq!(still.outcome, ToolRunAdmissionOutcomeWire::Waiting);
    assert_eq!(still.ahead, 0);
    succeed(&path, &forced);
    assert_eq!(
        poll(&path, &later, T0_MS + 3_604_000).outcome,
        ToolRunAdmissionOutcomeWire::Admitted
    );
}

fn demand_request(run_id: &str) -> ToolRunRecordDemandRequestWire {
    ToolRunRecordDemandRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.into(),
        context: Some(ToolRunDemandContextWire {
            provider: Some("Muse".into()),
            sync_ceiling_seconds: None,
            sync_soft_ceiling_seconds: None,
        }),
        usage: Some(ToolRunResourceUsageWire {
            cpu_user_ms: Some(10),
            cpu_system_ms: None,
            max_process_rss_kib: None,
            peak_tree_rss_kib: None,
            tree_rss_samples: 0,
            availability: Vec::new(),
        }),
        worker_grants: Vec::new(),
        diagnostics: Vec::new(),
    }
}

#[test]
fn demand_merges_preserve_the_admission_record() {
    let (_temp, path) = store();
    let entered_first = begin_run(&path, "sase");
    let recorded_first = begin_run(&path, "sase");
    let entry = enter(&path, &entered_first, 2, T0_MS);
    let recorded =
        record_demand(&path, demand_request(&entered_first), BUSY).unwrap();
    assert_eq!(recorded.demand.admission, entry.admission);
    assert!(recorded.demand.usage.is_some());
    let shown = show(&path, &entered_first).run.unwrap().demand.unwrap();
    assert_eq!(shown.admission, entry.admission);
    assert_eq!(shown.context.unwrap().provider.as_deref(), Some("Muse"));

    record_demand(&path, demand_request(&recorded_first), BUSY).unwrap();
    let entry = enter(&path, &recorded_first, 2, T0_MS + 5);
    let shown = show(&path, &recorded_first).run.unwrap().demand.unwrap();
    assert_eq!(shown.admission, entry.admission);
    assert_eq!(shown.usage.unwrap().cpu_user_ms, Some(10));
    assert_eq!(shown.context.unwrap().provider.as_deref(), Some("Muse"));
}

#[test]
fn absent_admission_keeps_demand_json_byte_identical() {
    let literal = r#"{"schema_version":1,"context":{"provider":"Muse"},"usage":{"cpu_user_ms":10,"tree_rss_samples":0,"availability":[]},"worker_grants":[],"diagnostics":[]}"#;
    let parsed: ToolRunDemandWire = serde_json::from_str(literal).unwrap();
    assert!(parsed.admission.is_none());
    assert_eq!(serde_json::to_string(&parsed).unwrap(), literal);

    let (_temp, path) = store();
    let run_id = begin_run(&path, "sase");
    record_demand(&path, demand_request(&run_id), BUSY).unwrap();
    let conn = Connection::open(&path).unwrap();
    let stored: String = conn
        .query_row(
            "SELECT demand_json FROM runs WHERE run_id = ?1",
            [&run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, literal);
}

#[test]
fn projections_carry_the_live_admission_view() {
    let (_temp, path) = store();
    let holder = begin_run(&path, "sase");
    let next = begin_run(&path, "sase");
    let last = begin_run(&path, "sase");
    let plain = begin_run(&path, "sase");
    enter(&path, &holder, 1, T0_MS);
    enter(&path, &next, 1, T0_MS + 1_000);
    enter(&path, &last, 1, T0_MS + 2_000);

    let shown = show(&path, &last);
    let view = shown.admission.unwrap();
    assert_eq!(view.state, ToolRunAdmissionStateWire::Waiting);
    assert_eq!(view.key, "check");
    assert_eq!(view.limit, 1);
    assert_eq!(view.ahead, 1);
    assert_eq!(view.holders, 1);
    assert_eq!(view.requested_ts_ms, T0_MS + 2_000);
    assert!(show(&path, &plain).admission.is_none());

    let glance = tool_run_live_glance(
        &path,
        ToolRunLiveGlanceRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            now_ts: Some(T0_S),
        },
        BUSY,
    )
    .unwrap();
    let glance_view = |run_id: &str| {
        glance
            .runs
            .iter()
            .find(|run| run.run_id == run_id)
            .unwrap()
            .admission
            .clone()
    };
    let held = glance_view(&holder).unwrap();
    assert_eq!(held.state, ToolRunAdmissionStateWire::Admitted);
    assert_eq!(held.ahead, 0);
    assert_eq!(held.admitted_by, Some(ToolRunAdmittedByWire::Slot));
    assert_eq!(held.admitted_ts_ms, Some(T0_MS));
    assert_eq!(glance_view(&next).unwrap().ahead, 0);
    assert_eq!(glance_view(&last).unwrap().ahead, 1);
    assert!(glance_view(&plain).is_none());

    let briefs = tool_run_briefs(
        &path,
        serde_json::from_value::<ToolRunBriefsRequestWire>(
            serde_json::json!({"schema_version": 1}),
        )
        .unwrap(),
        BUSY,
    )
    .unwrap();
    let brief = briefs.runs.iter().find(|run| run.run_id == last).unwrap();
    assert_eq!(brief.admission.as_ref().unwrap().ahead, 1);

    let detail_request = |run_id: &str| ToolRunDetailRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.into(),
        witness_window_days: 7,
        item_limit: 50,
        now_ts: Some(T0_S),
    };
    let detail = tool_run_detail(&path, detail_request(&last), BUSY).unwrap();
    let detail_view = detail.brief.unwrap().admission.unwrap();
    assert_eq!(detail_view.state, ToolRunAdmissionStateWire::Waiting);
    assert_eq!(detail_view.ahead, 1);

    let nodes = tool_run_node_summaries(
        &path,
        ToolRunNodeSummariesRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            nodes: vec![ToolRunNodeSelectorWire {
                key: "node".into(),
                agents: vec!["agent-sase".into()],
                owners: Vec::new(),
                since_ts: None,
            }],
            per_node_limit: 20,
        },
        BUSY,
    )
    .unwrap();
    let node = &nodes.nodes[0];
    let live = node.live.iter().find(|run| run.run_id == last).unwrap();
    assert_eq!(live.admission.as_ref().unwrap().ahead, 1);
    let run = node.runs.iter().find(|run| run.run_id == next).unwrap();
    assert_eq!(run.admission.as_ref().unwrap().ahead, 0);

    // Settled runs drop the live view; the stored record stays.
    succeed(&path, &holder);
    let settled = show(&path, &holder);
    assert!(settled.admission.is_none());
    assert_eq!(
        settled
            .run
            .unwrap()
            .demand
            .unwrap()
            .admission
            .unwrap()
            .state,
        ToolRunAdmissionStateWire::Admitted
    );
    let detail = tool_run_detail(&path, detail_request(&holder), BUSY).unwrap();
    assert!(detail.brief.unwrap().admission.is_none());
    // The live view moves as the queue does.
    let moved = show(&path, &last).admission.unwrap();
    assert_eq!(moved.holders, 0);
    assert_eq!(moved.ahead, 1);

    let json = serde_json::to_value(show(&path, &plain)).unwrap();
    assert!(json.get("admission").is_none());
}

#[test]
fn ineligible_poll_answers_without_the_write_lock() {
    let (_temp, path) = store();
    let holder = begin_run(&path, "sase");
    let waiter = begin_run(&path, "sase");
    enter(&path, &holder, 1, T0_MS);
    enter(&path, &waiter, 1, T0_MS + 1);
    let blocker = Connection::open(&path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let result =
        poll_at(&path, &waiter, Some(T0_MS + 2), Duration::from_millis(50))
            .unwrap();
    assert_eq!(result.outcome, ToolRunAdmissionOutcomeWire::Waiting);
    assert!(!result.changed);
    // The blocker does hold the write lock: a grant needs it and times out.
    succeed_blocked_check(&path, &blocker);
    blocker.execute_batch("ROLLBACK").unwrap();
    succeed(&path, &holder);
    assert_eq!(
        poll(&path, &waiter, T0_MS + 3).outcome,
        ToolRunAdmissionOutcomeWire::Admitted
    );
}

fn succeed_blocked_check(path: &Path, blocker: &Connection) {
    assert!(!blocker.is_autocommit());
    let fresh = admission_enter(
        path,
        ToolRunAdmissionEnterRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "any".into(),
            key: "check".into(),
            limit: 1,
            now_ms: Some(T0_MS),
        },
        Duration::from_millis(50),
    );
    assert!(matches!(fresh, Err(ToolRunError::Busy { .. })), "{fresh:?}");
}

#[test]
fn queue_query_reads_the_unsettled_set_through_the_state_index() {
    let (_temp, path) = store();
    let run_id = begin_run(&path, "sase");
    enter(&path, &run_id, 1, T0_MS);
    let conn = Connection::open(&path).unwrap();
    let mut stmt = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {}", queue_sql("launch_mode")))
        .unwrap();
    let details: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let plan = details.join("\n");
    assert!(plan.contains("idx_tool_runs_state"), "{plan}");
    assert!(!plan.contains("SCAN runs"), "{plan}");
}
