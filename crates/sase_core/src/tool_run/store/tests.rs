use super::super::wire::{
    ToolLivenessObservationWire, ToolRunAppendRequestWire,
    ToolRunBeginRequestWire, ToolRunBeginResultWire, ToolRunEventKindWire,
    ToolRunEventWire, ToolRunFinishRequestWire, ToolRunListRequestWire,
    ToolRunLivenessFactWire, ToolRunObserveRequestWire,
    ToolRunObserveResultWire, ToolRunReconcileRequestWire,
    ToolRunRetentionPolicyWire, ToolRunRetentionRequestWire,
    ToolRunRetentionResultWire, ToolRunShowRequestWire, ToolRunStateWire,
    ToolRunSummaryRequestWire, ToolStageWire,
    TOOL_RUN_LOST_REASON_RUNNER_EXITED, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::*;
use crate::tool_run::catalog::normalize_tool_definition;
use crate::tool_run::wire::{
    ToolArgsPolicyWire, ToolDefinitionWire, ToolFingerprintSpecWire,
    ToolStagesWire,
};
use rusqlite::Connection;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

mod compat;
mod handoff;
mod reconcile_owner;
mod triage;

fn definition() -> ToolDefinitionWire {
    ToolDefinitionWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        name: "check".into(),
        argv: vec!["just".into(), "check".into()],
        description: "check".into(),
        stages: ToolStagesWire::RunSilent,
        inputs: vec!["Justfile".into()],
        env: Vec::new(),
        args: ToolArgsPolicyWire::Deny,
        fingerprint: ToolFingerprintSpecWire::default(),
        diagnostics: Vec::new(),
    }
}

fn begin_named(path: &Path, now: i64) -> ToolRunBeginResultWire {
    let normalized = normalize_tool_definition(definition()).unwrap();
    begin(
        path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: None,
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
            owner_kind: None,
            owner_id: None,
            parent_run_id: None,
            wrapper_pid: Some(4242),
            boot_id: Some("boot-1".into()),
            process_start_identity: Some("start-1".into()),
            events_path: Some("logs/run/events.jsonl".into()),
            log_stdout_path: Some("logs/run/stdout.log".into()),
            log_stderr_path: Some("logs/run/stderr.log".into()),
            now_ts: Some(now),
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap()
}

fn store() -> (tempfile::TempDir, PathBuf) {
    let temp = tempdir().unwrap();
    let path = temp.path().join("tools").join("runs.sqlite");
    (temp, path)
}

#[test]
fn begin_commits_created_and_running_together() {
    let (_temp, path) = store();
    let started = begin_named(&path, 1_700_000_000);
    assert_eq!(started.run.state, ToolRunStateWire::Running);
    assert!(started.running_event_id.is_some());
    assert_eq!(started.run.created_ts, started.run.running_ts.unwrap());
    assert!(!started.run.logs.has_private_argv);
}

#[test]
fn every_terminal_state_round_trips() {
    let (_temp, path) = store();
    for (state, exit, signal) in [
        (ToolRunStateWire::Succeeded, Some(0), None),
        (ToolRunStateWire::Failed, Some(3), None),
        (ToolRunStateWire::Signaled, None, Some(15)),
        (ToolRunStateWire::Interrupted, None, Some(2)),
        (ToolRunStateWire::Lost, None, None),
    ] {
        let started = begin_named(&path, 10);
        let finished = finish(
            &path,
            ToolRunFinishRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: started.run.run_id,
                event_id: None,
                state,
                exit_code: exit,
                signal,
                interruption_reason: (state == ToolRunStateWire::Interrupted)
                    .then(|| "wrapper SIGINT".into()),
                lost_reason: (state == ToolRunStateWire::Lost)
                    .then(|| TOOL_RUN_LOST_REASON_RUNNER_EXITED.to_string()),
                child_pid: Some(99),
                child_pgid: Some(99),
                child_process_start_identity: None,
                duration_ms: (state != ToolRunStateWire::Lost).then_some(12),
                fingerprint_before: None,
                fingerprint_after: None,
                mutated_input: None,
                now_ts: Some(20),
                terminal_cause: None,
                diagnostics: Vec::new(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(finished.run.state, state);
        assert_eq!(finished.run.exit_code, exit);
        assert_eq!(finished.run.signal, signal);
        if state == ToolRunStateWire::Lost {
            assert!(finished.run.duration_ms.is_none());
            assert_eq!(
                finished.run.duration_missing.as_deref(),
                Some("exit time was not observed")
            );
        }
    }
}

#[test]
fn invalid_transition_is_an_error() {
    let (_temp, path) = store();
    let started = begin_named(&path, 1);
    finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: started.run.run_id.clone(),
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
            now_ts: Some(2),
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let error = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: started.run.run_id,
            event_id: None,
            state: ToolRunStateWire::Failed,
            exit_code: Some(1),
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
            now_ts: Some(3),
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(error, ToolRunError::InvalidTransition { .. }));
}

#[test]
fn event_replay_is_idempotent_and_conflicts_are_integrity_errors() {
    let (_temp, path) = store();
    let started = begin_named(&path, 1);
    let event = ToolRunEventWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        event_id: "evt-stage".into(),
        run_id: started.run.run_id.clone(),
        attempt: 1,
        kind: ToolRunEventKindWire::StageStarted,
        created_ts: 2,
        stage: Some(ToolStageWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            stage_id: "st1".into(),
            run_id: started.run.run_id.clone(),
            attempt: 1,
            description: "fmt".into(),
            started_ts: Some(2),
            finished_ts: None,
            elapsed_ms: None,
            exit_code: None,
            output_bytes: None,
            incomplete: true,
            diagnostics: Vec::new(),
        }),
        sample: None,
        exit_code: None,
        signal: None,
        reason: None,
        diagnostics: Vec::new(),
    };
    let first = append_event(
        &path,
        ToolRunAppendRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            event: event.clone(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(!first.replayed);
    let second = append_event(
        &path,
        ToolRunAppendRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            event: event.clone(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(second.replayed);
    let mut conflicting = event;
    conflicting.stage.as_mut().unwrap().description = "other".into();
    let error = append_event(
        &path,
        ToolRunAppendRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            event: conflicting,
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(error, ToolRunError::ConflictingEvent { .. }));
}

#[test]
fn queries_do_not_create_a_missing_store() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("missing").join("runs.sqlite");
    let listed = list_runs(
        &path,
        ToolRunListRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            tool: None,
            state: None,
            agent: None,
            project: None,
            include_all: false,
            limit: 50,
            cursor: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(listed.runs.is_empty());
    assert!(!path.exists());
    let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
    assert!(!stats.exists);
    assert!(!path.exists());
}

#[test]
fn newer_schema_is_refused_without_modification() {
    let (_temp, path) = store();
    begin_named(&path, 1);
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE meta SET value = '2' WHERE key = 'schema_version'",
        [],
    )
    .unwrap();
    drop(conn);
    let before = fs::read(&path).unwrap();
    let error = list_runs(
        &path,
        ToolRunListRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            tool: None,
            state: None,
            agent: None,
            project: None,
            include_all: false,
            limit: 10,
            cursor: None,
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(error, ToolRunError::NewerSchema { actual: 2, .. }));
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn corrupt_store_is_quarantined_on_write_and_recreated() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("runs.sqlite");
    fs::write(&path, b"not a sqlite database").unwrap();
    begin_named(&path, 1);
    let quarantined = fs::read_dir(temp.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".corrupt-"))
        .count();
    assert_eq!(quarantined, 1);
    assert_eq!(
        store_stats(&path, Duration::from_secs(1))
            .unwrap()
            .run_count,
        1
    );
}

fn retention_request(now: i64, log_days: u32) -> ToolRunRetentionRequestWire {
    ToolRunRetentionRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        policy: ToolRunRetentionPolicyWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            log_days,
            ..ToolRunRetentionPolicyWire::default()
        },
        now_ts: Some(now),
        dry_run: true,
    }
}

#[test]
fn retention_selects_quarantined_store_older_than_log_horizon() {
    let (_temp, path) = store();
    begin_named(&path, 1_000);
    let dir = path.parent().unwrap();
    // Nanos of 1 quarantine at the epoch: older than any horizon.
    let old = dir.join("runs.sqlite.corrupt-1");
    fs::write(&old, vec![b'q'; 64]).unwrap();
    let old_wal = dir.join("runs.sqlite.corrupt-1-wal");
    fs::write(&old_wal, vec![b'w'; 16]).unwrap();
    // Quarantined "now": younger than the 14-day log horizon.
    let now = 10_000_000i64;
    let young = dir.join(format!(
        "runs.sqlite.corrupt-{}",
        (now as u128) * 1_000_000_000
    ));
    fs::write(&young, vec![b'y'; 32]).unwrap();

    let preview = retention_preview(
        &path,
        retention_request(now, 14),
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(preview.dry_run);
    let mut selected: Vec<_> = preview
        .file_candidates
        .iter()
        .filter_map(|candidate| candidate.path.clone())
        .collect();
    selected.sort();
    assert_eq!(
        selected,
        vec![
            old.to_string_lossy().into_owned(),
            old_wal.to_string_lossy().into_owned(),
        ]
    );
    assert!(preview.file_candidates.iter().all(|candidate| {
        candidate.kind == "quarantined_store"
            && candidate.run_id.is_none()
            && !candidate.protected
            && candidate.reason.contains("log_days")
    }));
    // The young quarantine stays and its bytes stay accounted.
    assert_eq!(preview.retained_bytes, 32);
    assert_eq!(preview.over_target_bytes, 0);

    // Apply reports the same candidates but never deletes files: the
    // `tool_run_retention` reaper owns deletion.
    let applied = retention_apply(
        &path,
        ToolRunRetentionRequestWire {
            dry_run: false,
            ..retention_request(now, 14)
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(!applied.dry_run);
    assert_eq!(applied.file_candidates.len(), preview.file_candidates.len());
    assert!(old.exists() && old_wal.exists() && young.exists());
    assert_eq!(
        store_stats(&path, Duration::from_secs(1))
            .unwrap()
            .run_count,
        1
    );
}

#[test]
fn retention_previews_quarantined_store_without_live_store() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("tools").join("runs.sqlite");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let old = path.parent().unwrap().join("runs.sqlite.corrupt-1");
    fs::write(&old, vec![b'q'; 48]).unwrap();

    let preview = retention_preview(
        &path,
        retention_request(10_000_000, 14),
        Duration::from_secs(1),
    )
    .unwrap();
    let selected: Vec<_> = preview
        .file_candidates
        .iter()
        .filter_map(|candidate| candidate.path.clone())
        .collect();
    assert_eq!(selected, vec![old.to_string_lossy().into_owned()]);
    assert_eq!(preview.retained_bytes, 0);
    assert!(preview
        .diagnostics
        .iter()
        .any(|line| line.contains("does not exist")));
    assert!(!path.exists());
}

#[test]
fn retention_protects_unsettled_runs() {
    let (_temp, path) = store();
    let running = begin_named(&path, 1);
    let settled = begin_named(&path, 2);
    finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: settled.run.run_id,
            event_id: None,
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
            now_ts: Some(3),
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let preview = retention_preview(
        &path,
        ToolRunRetentionRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            policy: ToolRunRetentionPolicyWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                summary_days: 1,
                detail_days: 1,
                log_days: 1,
                ..ToolRunRetentionPolicyWire::default()
            },
            now_ts: Some(3 + 2 * 86400),
            dry_run: true,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(preview.protected_unsettled >= 1);
    assert!(preview.summary_rows >= 1);
    let applied = retention_apply(
        &path,
        ToolRunRetentionRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            policy: ToolRunRetentionPolicyWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                summary_days: 1,
                detail_days: 1,
                log_days: 1,
                ..ToolRunRetentionPolicyWire::default()
            },
            now_ts: Some(3 + 2 * 86400),
            dry_run: false,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(!applied.dry_run);
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: running.run.run_id,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(shown.run.is_some());
}

fn begin_with_log(path: &Path, now: i64, log: &Path, bytes: usize) -> String {
    fs::create_dir_all(log.parent().unwrap()).unwrap();
    fs::write(log, vec![b'x'; bytes]).unwrap();
    let normalized = normalize_tool_definition(definition()).unwrap();
    begin(
        path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: None,
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
            owner_kind: None,
            owner_id: None,
            parent_run_id: None,
            wrapper_pid: Some(4242),
            boot_id: Some("boot-1".into()),
            process_start_identity: Some("start-1".into()),
            events_path: None,
            log_stdout_path: Some(log.to_string_lossy().into_owned()),
            log_stderr_path: None,
            now_ts: Some(now),
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap()
    .run
    .run_id
}

fn settle_succeeded(path: &Path, run_id: &str, now: i64) {
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
            duration_ms: Some(5),
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(now),
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn aggregate_preview(
    path: &Path,
    log_max_bytes: u64,
) -> ToolRunRetentionResultWire {
    retention_preview(
        path,
        ToolRunRetentionRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            policy: ToolRunRetentionPolicyWire {
                log_max_bytes,
                ..ToolRunRetentionPolicyWire::default()
            },
            now_ts: Some(1_000),
            dry_run: true,
        },
        Duration::from_secs(1),
    )
    .unwrap()
}

#[test]
fn retention_aggregate_target_deletes_oldest_settled_logs_first() {
    let (temp, path) = store();
    let logs = temp.path().join("logs");
    let live = begin_with_log(&path, 1, &logs.join("live/stdout.log"), 900);
    let mut settled_logs = Vec::new();
    for (index, name) in ["oldest", "middle", "newest"].iter().enumerate() {
        let log = logs.join(name).join("stdout.log");
        let run_id = begin_with_log(&path, 10 + index as i64, &log, 1000);
        settle_succeeded(&path, &run_id, 20 + index as i64);
        settled_logs.push(log.to_string_lossy().into_owned());
    }

    // 3900 retained bytes against a 2500 target: the two oldest settled
    // runs go, the newest settled run and the unsettled run stay.
    let fits = aggregate_preview(&path, 2500);
    let selected: Vec<_> = fits
        .file_candidates
        .iter()
        .filter_map(|candidate| candidate.path.clone())
        .collect();
    assert_eq!(
        selected,
        vec![settled_logs[0].clone(), settled_logs[1].clone()]
    );
    assert!(fits
        .file_candidates
        .iter()
        .all(|candidate| candidate.reason.contains("log_max_bytes")));
    assert_eq!(fits.retained_bytes, 1900);
    assert_eq!(fits.protected_bytes, 900);
    assert_eq!(fits.over_target_bytes, 0);

    // Protected bytes alone exceed a 500-byte target: every settled log is
    // selected, the excess is reported, and the live run is never listed.
    let over = aggregate_preview(&path, 500);
    assert_eq!(over.file_candidates.len(), 3);
    assert!(over.file_candidates.iter().all(|candidate| {
        candidate.run_id.as_deref() != Some(live.as_str())
            && !candidate
                .path
                .as_deref()
                .unwrap_or_default()
                .contains("live")
    }));
    assert_eq!(over.retained_bytes, 900);
    assert_eq!(over.over_target_bytes, 400);

    // A target that already fits selects nothing; Rust never deletes files.
    let roomy = aggregate_preview(&path, 10_000);
    assert!(roomy.file_candidates.is_empty());
    assert_eq!(roomy.retained_bytes, 3900);
    assert!(logs.join("oldest/stdout.log").exists());
}

#[test]
fn reconcile_marks_dead_wrappers_lost_without_inventing_duration() {
    let (_temp, path) = store();
    let started = begin_named(&path, 10);
    let result = reconcile(
        &path,
        ToolRunReconcileRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            facts: vec![crate::tool_run::wire::ToolRunLivenessFactWire {
                run_id: started.run.run_id.clone(),
                wrapper_pid: Some(4242),
                boot_id: Some("boot-other".into()),
                process_start_identity: Some("start-1".into()),
                observation: ToolLivenessObservationWire::Dead,
                reason: Some("boot_changed".into()),
                owner: None,
            }],
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(result.marked_lost.len(), 1);
    assert_eq!(result.marked_lost[0], started.run.run_id);
    // No child facts were observed, so nothing is authorized for reaping.
    assert!(result.reap_candidates.is_empty());
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: started.run.run_id,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let run = shown.run.unwrap();
    assert_eq!(run.state, ToolRunStateWire::Lost);
    assert!(run.duration_ms.is_none());
    assert_eq!(run.lost_reason.as_deref(), Some("boot_changed"));
}

#[test]
fn unknown_liveness_is_not_proof_of_death() {
    let (_temp, path) = store();
    let started = begin_named(&path, 10);
    let result = reconcile(
        &path,
        ToolRunReconcileRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            facts: vec![crate::tool_run::wire::ToolRunLivenessFactWire {
                run_id: started.run.run_id.clone(),
                wrapper_pid: Some(4242),
                boot_id: None,
                process_start_identity: None,
                observation: ToolLivenessObservationWire::Unknown,
                reason: Some("permission_denied".into()),
                owner: None,
            }],
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(result.marked_lost.is_empty());
    assert!(result.reap_candidates.is_empty());
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: started.run.run_id,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(shown.run.unwrap().state, ToolRunStateWire::Running);
}

#[test]
fn version_rejection_and_unknown_evidence_slots() {
    let error = begin(
        Path::new("/tmp/unused"),
        ToolRunBeginRequestWire {
            schema_version: 9,
            run_id: None,
            created_event_id: None,
            running_event_id: None,
            tool_name: Some("check".into()),
            definition: definition(),
            extra_args: Vec::new(),
            display_argv: vec!["just".into()],
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
            now_ts: Some(1),
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ToolRunError::SchemaVersion { actual: 9, .. }
    ));
    let fingerprint = crate::tool_run::unknown_evidence("not observed");
    assert!(!fingerprint.completeness.complete);
    assert!(fingerprint.project_identity.is_none());
}

#[test]
fn finish_stores_canonical_fingerprints_and_mutated_input() {
    use crate::tool_run::wire::{
        ToolDirtyPathWire, ToolEvidenceCompletenessWire,
        ToolRepoFingerprintWire,
    };

    let (_temp, path) = store();
    let started = begin_named(&path, 10);
    let mut before = crate::tool_run::unknown_evidence("unused");
    before.project_identity = Some("sase".into());
    before.completeness = ToolEvidenceCompletenessWire {
        complete: true,
        missing: Vec::new(),
    };
    before.diagnostics.clear();
    before.repos = vec![ToolRepoFingerprintWire {
        identity: "sase".into(),
        head: Some("aaa".into()),
        index_tree: Some("bbb".into()),
        dirty_paths: vec![ToolDirtyPathWire {
            path: "z.py".into(),
            status: "modified".into(),
            kind: "file".into(),
            mode: Some("100644".into()),
            content_hash: Some("1".into()),
            incomplete: None,
        }],
        incomplete: None,
    }];
    let mut after = before.clone();
    after.repos[0].dirty_paths[0].content_hash = Some("2".into());
    let finished = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: started.run.run_id.clone(),
            event_id: None,
            state: ToolRunStateWire::Succeeded,
            exit_code: Some(0),
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: Some(12),
            fingerprint_before: Some(before),
            fingerprint_after: Some(after),
            mutated_input: None,
            now_ts: Some(22),
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(finished.run.mutated_input, Some(true));
    assert!(finished.run.evidence_completeness.complete);
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: started.run.run_id,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let run = shown.run.expect("run");
    assert_eq!(run.fingerprint_before.unwrap().repos[0].identity, "sase");
    assert_eq!(
        run.fingerprint_after.unwrap().repos[0].dirty_paths[0]
            .content_hash
            .as_deref(),
        Some("2")
    );
}

#[test]
fn incomplete_fingerprints_do_not_guess_mutated_input() {
    let (_temp, path) = store();
    let started = begin_named(&path, 10);
    let before = crate::tool_run::unknown_evidence("probe timeout");
    let after = crate::tool_run::unknown_evidence("probe timeout");
    let finished = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: started.run.run_id,
            event_id: None,
            state: ToolRunStateWire::Succeeded,
            exit_code: Some(0),
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: Some(3),
            fingerprint_before: Some(before),
            fingerprint_after: Some(after),
            mutated_input: None,
            now_ts: Some(13),
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(finished.run.mutated_input.is_none());
    assert!(!finished.run.evidence_completeness.complete);
    assert!(finished
        .run
        .evidence_completeness
        .missing
        .iter()
        .any(|item| item.contains("probe timeout")));
}

#[test]
fn concurrent_begins_do_not_livelock() {
    let (_temp, path) = store();
    begin_named(&path, 1);
    let barrier = Arc::new(Barrier::new(2));
    let path_a = path.clone();
    let path_b = path.clone();
    let barrier_a = barrier.clone();
    let handle = thread::spawn(move || {
        barrier_a.wait();
        begin(
            &path_a,
            ToolRunBeginRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: None,
                created_event_id: None,
                running_event_id: None,
                tool_name: Some("check".into()),
                definition: definition(),
                extra_args: Vec::new(),
                display_argv: vec!["just".into(), "check".into()],
                private_argv: None,
                project: Some("sase".into()),
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
                now_ts: Some(2),
                commit_running: true,
                launch_mode: None,
                launch: None,
                owner_log_path: None,
            },
            Duration::from_millis(250),
        )
    });
    barrier.wait();
    let second = begin(
        &path_b,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: None,
            created_event_id: None,
            running_event_id: None,
            tool_name: Some("check".into()),
            definition: definition(),
            extra_args: Vec::new(),
            display_argv: vec!["just".into(), "check".into()],
            private_argv: None,
            project: Some("sase".into()),
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
            now_ts: Some(3),
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_millis(250),
    );
    let first = handle.join().unwrap();
    assert!(first.is_ok() || second.is_ok());
    let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
    assert!(stats.run_count >= 2);
}

#[test]
fn busy_failure_is_bounded() {
    let (_temp, path) = store();
    begin_named(&path, 1);
    let holder = Connection::open(&path).unwrap();
    holder.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let started = Instant::now();
    let result = begin(
        &path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: None,
            created_event_id: None,
            running_event_id: None,
            tool_name: Some("check".into()),
            definition: definition(),
            extra_args: Vec::new(),
            display_argv: vec!["just".into(), "check".into()],
            private_argv: None,
            project: Some("sase".into()),
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
            now_ts: Some(2),
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_millis(25),
    );
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(2));
    holder.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn summary_never_substitutes_zero_for_missing_typical() {
    let (_temp, path) = store();
    let started = begin_named(&path, 1);
    let digest = started.run.definition_digest.clone();
    let summary = summarize(
        &path,
        ToolRunSummaryRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project: "sase".into(),
            tool_name: "check".into(),
            definition_digest: digest,
            now_ts: Some(1),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(summary.typical_duration_ms.is_none());
    assert_eq!(summary.typical_sample_count, 0);
    assert!(summary
        .diagnostics
        .iter()
        .any(|item| item.contains("unknown")));
}

#[test]
fn private_argv_is_not_serialized_on_queries() {
    let (_temp, path) = store();
    let started = begin(
        &path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: None,
            created_event_id: None,
            running_event_id: None,
            tool_name: None,
            definition: ToolDefinitionWire {
                name: String::new(),
                argv: vec!["secret".into(), "--token".into(), "abc".into()],
                ..definition()
            },
            extra_args: Vec::new(),
            display_argv: vec!["secret".into(), "--token".into(), "***".into()],
            private_argv: Some(vec![
                "secret".into(),
                "--token".into(),
                "abc".into(),
            ]),
            project: Some("sase".into()),
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
            now_ts: Some(1),
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(started.run.logs.has_private_argv);
    let encoded = serde_json::to_string(&started.run).unwrap();
    assert!(!encoded.contains("abc"));
    assert!(encoded.contains("***"));
}

fn observe_child(path: &Path, run_id: &str) -> ToolRunObserveResultWire {
    observe(
        path,
        ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            child_pid: Some(4242),
            child_pgid: Some(4242),
            child_process_start_identity: Some("boot-1:12345".into()),
            fingerprint_before: None,
        },
        Duration::from_secs(1),
    )
    .unwrap()
}

fn dead_wrapper_fact(run_id: &str) -> ToolRunLivenessFactWire {
    ToolRunLivenessFactWire {
        run_id: run_id.to_string(),
        wrapper_pid: Some(4242),
        boot_id: Some("boot-other".into()),
        process_start_identity: Some("start-1".into()),
        observation: ToolLivenessObservationWire::Dead,
        owner: None,
        reason: Some("runner gone".into()),
    }
}

#[test]
fn golden_observe_and_reap_fixtures_pin_the_wire_shape() {
    let request: ToolRunObserveRequestWire =
        serde_json::from_str(include_str!("../fixtures/observe_request.json"))
            .unwrap();
    assert_eq!(request.schema_version, TOOL_RUN_WIRE_SCHEMA_VERSION);
    assert_eq!(request.child_pid, Some(4242));
    assert_eq!(request.child_pgid, Some(4242));
    assert_eq!(
        request.child_process_start_identity.as_deref(),
        Some("boot-1:12345")
    );
    let candidate: crate::tool_run::wire::ToolRunReapCandidateWire =
        serde_json::from_str(include_str!("../fixtures/reap_candidate.json"))
            .unwrap();
    assert_eq!(candidate.run_id, request.run_id);
    assert_eq!(candidate.pgid, request.child_pgid.unwrap());
    assert_eq!(
        candidate.child_process_start_identity,
        request.child_process_start_identity
    );
    // A reap candidate authorizes exactly the observed pgid plus the
    // observed child identity: no wrapper fields leak into it.
    let encoded = serde_json::to_value(&candidate).unwrap();
    assert_eq!(
        encoded,
        serde_json::json!({
            "run_id": request.run_id,
            "pgid": 4242,
            "child_process_start_identity": "boot-1:12345",
        })
    );
}

#[test]
fn observe_then_finish_repeats_child_facts_idempotently() {
    let (_temp, path) = store();
    let started = begin_named(&path, 10);
    let observed = observe_child(&path, &started.run.run_id);
    assert!(!observed.replayed);
    assert_eq!(observed.run.child_pid, Some(4242));
    assert_eq!(observed.run.child_pgid, Some(4242));
    assert_eq!(
        observed.run.child_process_start_identity.as_deref(),
        Some("boot-1:12345")
    );
    let finished = finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: started.run.run_id.clone(),
            event_id: None,
            state: ToolRunStateWire::Succeeded,
            exit_code: Some(0),
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: Some(4242),
            child_pgid: Some(4242),
            child_process_start_identity: Some("boot-1:12345".into()),
            duration_ms: Some(12),
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(20),
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(finished.run.state, ToolRunStateWire::Succeeded);
    assert_eq!(finished.run.child_pid, Some(4242));
    assert_eq!(finished.run.child_pgid, Some(4242));
    assert_eq!(
        finished.run.child_process_start_identity.as_deref(),
        Some("boot-1:12345")
    );
}

#[test]
fn observe_then_lost_reports_an_authorized_reap_candidate() {
    let (_temp, path) = store();
    let started = begin_named(&path, 10);
    observe_child(&path, &started.run.run_id);
    let result = reconcile(
        &path,
        ToolRunReconcileRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            facts: vec![dead_wrapper_fact(&started.run.run_id)],
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(result.marked_lost, vec![started.run.run_id.clone()]);
    assert_eq!(result.reap_candidates.len(), 1);
    let candidate = &result.reap_candidates[0];
    assert_eq!(candidate.run_id, started.run.run_id);
    assert_eq!(candidate.pgid, 4242);
    assert_eq!(
        candidate.child_process_start_identity.as_deref(),
        Some("boot-1:12345")
    );
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: started.run.run_id,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let run = shown.run.unwrap();
    assert_eq!(run.state, ToolRunStateWire::Lost);
    assert_eq!(run.child_pgid, Some(4242));
}

#[test]
fn reconcile_without_an_observation_authorizes_no_reap() {
    let (_temp, path) = store();
    let started = begin_named(&path, 10);
    let result = reconcile(
        &path,
        ToolRunReconcileRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            facts: vec![dead_wrapper_fact(&started.run.run_id)],
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(result.marked_lost, vec![started.run.run_id]);
    assert!(result.reap_candidates.is_empty());
}

#[test]
fn repeated_observation_replays_and_settled_or_missing_runs_reject() {
    let (_temp, path) = store();
    let started = begin_named(&path, 10);
    assert!(!observe_child(&path, &started.run.run_id).replayed);
    assert!(observe_child(&path, &started.run.run_id).replayed);

    let missing = observe(
        &path,
        ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "does-not-exist".into(),
            child_pid: Some(1),
            child_pgid: Some(1),
            child_process_start_identity: None,
            fingerprint_before: None,
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(missing, ToolRunError::NotFound { .. }));

    finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: started.run.run_id.clone(),
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
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let settled = observe(
        &path,
        ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: started.run.run_id,
            child_pid: Some(4242),
            child_pgid: Some(4242),
            child_process_start_identity: None,
            fingerprint_before: None,
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(settled, ToolRunError::InvalidTransition { .. }));
}

#[test]
fn reconcile_reports_candidates_only_for_observed_runs() {
    let (_temp, path) = store();
    let observed = begin_named(&path, 10);
    let unobserved = begin_named(&path, 10);
    observe_child(&path, &observed.run.run_id);
    let result = reconcile(
        &path,
        ToolRunReconcileRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            facts: vec![
                dead_wrapper_fact(&observed.run.run_id),
                dead_wrapper_fact(&unobserved.run.run_id),
            ],
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(result.marked_lost.len(), 2);
    assert_eq!(result.reap_candidates.len(), 1);
    assert_eq!(result.reap_candidates[0].run_id, observed.run.run_id);
}
