//! Projection tests: buckets, glance, briefs, and node summaries.
//!
//! Helpers build real stores through `begin`/`append_event`/`finish`;
//! triage facts that need classified labels are seeded with direct SQL so
//! the tests exercise the read path, not the record path.

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::super::catalog::normalize_tool_definition;
use super::super::handoff_wire::{
    ToolRunStopRequestWire, ToolRunTerminalCauseWire,
};
use super::super::store::{
    append_event, begin, finish, request_stop, summarize,
};
use super::super::wire::{
    ToolArgsPolicyWire, ToolDefinitionWire, ToolFingerprintSpecWire,
    ToolLoadSampleWire, ToolRunAppendRequestWire, ToolRunBeginRequestWire,
    ToolRunBeginResultWire, ToolRunEventKindWire, ToolRunEventWire,
    ToolRunFinishRequestWire, ToolRunStateWire, ToolStageWire, ToolStagesWire,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::shared::bucket_for;
use super::wire::{
    ToolRunBriefOwnerWire, ToolRunBriefsRequestWire, ToolRunDetailRequestWire,
    ToolRunLiveGlanceRequestWire, ToolRunNodeSelectorWire,
    ToolRunNodeSummariesRequestWire, ToolRunVerdictBucketWire,
    TOOL_RUN_GLANCE_MAX_RUNS,
};
use super::{
    tool_run_briefs, tool_run_detail, tool_run_live_glance,
    tool_run_node_summaries,
};

const NOW: i64 = 1_700_000_000;

fn store() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("tools").join("runs.sqlite");
    (temp, path)
}

fn definition(name: &str, argv: &[&str]) -> ToolDefinitionWire {
    ToolDefinitionWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        name: name.to_string(),
        argv: argv.iter().map(|arg| arg.to_string()).collect(),
        description: name.to_string(),
        stages: ToolStagesWire::RunSilent,
        inputs: vec!["Justfile".into()],
        env: Vec::new(),
        args: ToolArgsPolicyWire::Deny,
        fingerprint: ToolFingerprintSpecWire::default(),
        receipt: None,
        duration_class: None,
        diagnostics: Vec::new(),
    }
}

#[derive(Default)]
struct BeginArgs {
    tool_name: Option<String>,
    def_name: String,
    def_argv: Vec<String>,
    display_argv: Vec<String>,
    private_argv: Option<Vec<String>>,
    project: Option<String>,
    agent: Option<String>,
    owner_kind: Option<String>,
    owner_id: Option<String>,
    parent_run_id: Option<String>,
    log_stdout_path: Option<String>,
    log_stderr_path: Option<String>,
    owner_log_path: Option<String>,
    now: i64,
    commit_running: bool,
}

impl BeginArgs {
    fn named(tool: &str) -> Self {
        Self {
            tool_name: Some(tool.to_string()),
            def_name: tool.to_string(),
            def_argv: vec!["just".into(), tool.into()],
            display_argv: vec!["just".into(), tool.into()],
            project: Some("sase".into()),
            agent: Some("agent-1".into()),
            now: NOW,
            commit_running: true,
            ..Self::default()
        }
    }
}

fn begin_run(path: &Path, args: BeginArgs) -> ToolRunBeginResultWire {
    let def_name = if args.def_name.is_empty() {
        "check"
    } else {
        &args.def_name
    };
    let def_argv = if args.def_argv.is_empty() {
        vec!["just".into(), "check".into()]
    } else {
        args.def_argv.clone()
    };
    let argv_refs: Vec<&str> = def_argv.iter().map(String::as_str).collect();
    let normalized =
        normalize_tool_definition(definition(def_name, &argv_refs)).unwrap();
    let display_argv = if args.display_argv.is_empty() {
        def_argv
    } else {
        args.display_argv
    };
    begin(
        path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: None,
            created_event_id: None,
            running_event_id: None,
            tool_name: args.tool_name,
            definition: normalized.definition,
            extra_args: Vec::new(),
            display_argv,
            private_argv: args.private_argv,
            project: args.project,
            agent: args.agent,
            workspace: None,
            bead: None,
            owner_kind: args.owner_kind,
            owner_id: args.owner_id,
            parent_run_id: args.parent_run_id,
            wrapper_pid: Some(4242),
            boot_id: Some("boot-1".into()),
            process_start_identity: Some("start-1".into()),
            events_path: None,
            log_stdout_path: args.log_stdout_path,
            log_stderr_path: args.log_stderr_path,
            now_ts: Some(args.now),
            commit_running: args.commit_running,
            launch_mode: None,
            launch: None,
            owner_log_path: args.owner_log_path,
        },
        Duration::from_secs(1),
    )
    .unwrap()
}

fn stage_started(
    path: &Path,
    run_id: &str,
    event_id: &str,
    stage_id: &str,
    description: &str,
    started_ms: i64,
) {
    append_event(
        path,
        ToolRunAppendRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            event: ToolRunEventWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                event_id: event_id.to_string(),
                run_id: run_id.to_string(),
                attempt: 1,
                kind: ToolRunEventKindWire::StageStarted,
                created_ts: started_ms.div_euclid(1000),
                stage: Some(ToolStageWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    stage_id: stage_id.to_string(),
                    run_id: run_id.to_string(),
                    attempt: 1,
                    description: description.to_string(),
                    started_ts: Some(started_ms),
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
            },
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn stage_finished(
    path: &Path,
    run_id: &str,
    event_id: &str,
    stage_id: &str,
    description: &str,
    started_ms: i64,
    finished_ms: i64,
) {
    append_event(
        path,
        ToolRunAppendRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            event: ToolRunEventWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                event_id: event_id.to_string(),
                run_id: run_id.to_string(),
                attempt: 1,
                kind: ToolRunEventKindWire::StageFinished,
                created_ts: finished_ms.div_euclid(1000),
                stage: Some(ToolStageWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    stage_id: stage_id.to_string(),
                    run_id: run_id.to_string(),
                    attempt: 1,
                    description: description.to_string(),
                    started_ts: Some(started_ms),
                    finished_ts: Some(finished_ms),
                    elapsed_ms: Some(finished_ms - started_ms),
                    exit_code: Some(0),
                    output_bytes: Some(100),
                    incomplete: false,
                    diagnostics: Vec::new(),
                }),
                sample: None,
                exit_code: None,
                signal: None,
                reason: None,
                diagnostics: Vec::new(),
            },
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

fn sample(path: &Path, run_id: &str, event_id: &str, observed_ts: i64) {
    append_event(
        path,
        ToolRunAppendRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            event: ToolRunEventWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                event_id: event_id.to_string(),
                run_id: run_id.to_string(),
                attempt: 1,
                kind: ToolRunEventKindWire::Sample,
                created_ts: observed_ts,
                stage: None,
                sample: Some(ToolLoadSampleWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    sample_id: format!("{event_id}-sample"),
                    run_id: run_id.to_string(),
                    attempt: 1,
                    observed_ts,
                    elapsed_ms: None,
                    loadavg_1: None,
                    loadavg_5: None,
                    loadavg_15: None,
                    logical_cpus: None,
                    psi_cpu_some: None,
                    psi_memory_some: None,
                    psi_io_some: None,
                    host_identity: None,
                    availability: Vec::new(),
                    diagnostics: Vec::new(),
                }),
                exit_code: None,
                signal: None,
                reason: None,
                diagnostics: Vec::new(),
            },
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
fn finish_run(
    path: &Path,
    run_id: &str,
    state: ToolRunStateWire,
    exit_code: Option<i32>,
    signal: Option<i32>,
    cause: Option<ToolRunTerminalCauseWire>,
    duration_ms: Option<i64>,
    now: i64,
) {
    finish(
        path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            event_id: None,
            state,
            exit_code,
            signal,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms,
            fingerprint_before: None,
            fingerprint_after: None,
            mutated_input: None,
            now_ts: Some(now),
            terminal_cause: cause,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
}

/// Seed classified triage facts with direct SQL: the read path under test
/// never validates these shapes, only reads them.
fn seed_triage(
    path: &Path,
    run_id: &str,
    stages: &[(&str, &str)],
    items: &[(&str, &str, &str)],
    recipe_finished: bool,
    triaged: bool,
) {
    let conn = rusqlite::Connection::open(path).unwrap();
    for (stage_key, status) in stages {
        conn.execute(
            "INSERT INTO tool_triage_stages(
                run_id, stage_key, extraction_status, created_ts
             ) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![run_id, stage_key, status, NOW],
        )
        .unwrap();
    }
    for (index, (stage_key, class, extractor)) in items.iter().enumerate() {
        conn.execute(
            "INSERT INTO tool_triage_items(
                item_id, run_id, stage_key, extractor, extractor_version,
                signature, display, locator_paths_json, occurrences,
                created_ts, class
             ) VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, '[]', 1, ?7, ?8)",
            rusqlite::params![
                format!("item-{run_id}-{index}"),
                run_id,
                stage_key,
                extractor,
                "a".repeat(64),
                format!("display {index}"),
                NOW,
                class,
            ],
        )
        .unwrap();
    }
    conn.execute(
        "INSERT INTO tool_triage_runs(
            run_id, recipe_finished_ts, triaged_ts, created_ts,
            diagnostics_json
         ) VALUES (?1, ?2, ?3, ?4, '[]')",
        rusqlite::params![
            run_id,
            recipe_finished.then_some(NOW),
            triaged.then_some(NOW),
            NOW,
        ],
    )
    .unwrap();
}

fn glance_request() -> ToolRunLiveGlanceRequestWire {
    ToolRunLiveGlanceRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        now_ts: Some(NOW + 1000),
    }
}

fn briefs_request() -> ToolRunBriefsRequestWire {
    ToolRunBriefsRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        project: None,
        tool: None,
        states: Vec::new(),
        agents: Vec::new(),
        owners: Vec::new(),
        since_ts: None,
        limit: 100,
        cursor: None,
    }
}

#[test]
fn bucket_precedence_covers_every_state_cause_and_verdict() {
    use ToolRunStateWire::{
        Created, Failed, Interrupted, Lost, Running, Signaled, Succeeded,
    };
    use ToolRunVerdictBucketWire::{
        Killed, KnownOnly, Lost as LostBucket, NewFailures, Pass,
        Running as RunningBucket, Stopped, Undetermined,
    };
    let live = [Created, Running];
    let settled = [Succeeded, Failed, Signaled, Interrupted, Lost];
    let causes = [
        None,
        Some("stop_requested"),
        Some("interrupt"),
        Some("signal"),
        Some("timeout"),
        Some("owner_lost"),
        Some("wrapper_lost"),
        Some("launch_failed"),
        Some("exited"),
    ];
    let verdicts = [
        None,
        Some("pass"),
        Some("new_failures"),
        Some("no_new_failures"),
        Some("undetermined"),
    ];
    for state in live {
        for cause in causes {
            for verdict in verdicts {
                assert_eq!(
                    bucket_for(state, cause, verdict),
                    RunningBucket,
                    "live {state:?} with {cause:?}/{verdict:?} must stay running"
                );
            }
        }
    }
    for state in settled {
        for verdict in verdicts {
            // Terminal causes outrank state and verdict alike.
            assert_eq!(
                bucket_for(state, Some("stop_requested"), verdict),
                Stopped
            );
            assert_eq!(bucket_for(state, Some("interrupt"), verdict), Stopped);
            assert_eq!(bucket_for(state, Some("signal"), verdict), Killed);
            assert_eq!(bucket_for(state, Some("timeout"), verdict), Killed);
            assert_eq!(
                bucket_for(state, Some("owner_lost"), verdict),
                LostBucket
            );
            assert_eq!(
                bucket_for(state, Some("wrapper_lost"), verdict),
                LostBucket
            );
            assert_eq!(
                bucket_for(state, Some("launch_failed"), verdict),
                LostBucket
            );
        }
        // A lost run without a cause is still lost.
        assert_eq!(
            bucket_for(state, None, Some("pass")),
            if state == Lost { LostBucket } else { Pass }
        );
        // Without a terminal cause the triage verdict decides.
        let expected = |verdict| match verdict {
            Some("pass") => Pass,
            Some("new_failures") => NewFailures,
            Some("no_new_failures") => KnownOnly,
            _ => Undetermined,
        };
        for verdict in verdicts {
            if state == Lost {
                assert_eq!(bucket_for(state, None, verdict), LostBucket);
                assert_eq!(
                    bucket_for(state, Some("exited"), verdict),
                    LostBucket
                );
            } else {
                assert_eq!(bucket_for(state, None, verdict), expected(verdict));
                assert_eq!(
                    bucket_for(state, Some("exited"), verdict),
                    expected(verdict)
                );
            }
        }
    }
}

#[test]
fn glance_lists_unsettled_newest_first() {
    let (_temp, path) = store();
    let created = begin_run(
        &path,
        BeginArgs {
            commit_running: false,
            now: NOW - 30,
            ..BeginArgs::named("check")
        },
    );
    assert_eq!(created.run.state, ToolRunStateWire::Created);
    let live = begin_run(
        &path,
        BeginArgs {
            now: NOW - 10,
            ..BeginArgs::named("check")
        },
    );
    let settled = begin_run(
        &path,
        BeginArgs {
            now: NOW - 20,
            ..BeginArgs::named("lint")
        },
    );
    finish_run(
        &path,
        &settled.run.run_id,
        ToolRunStateWire::Succeeded,
        Some(0),
        None,
        None,
        Some(5_000),
        NOW,
    );
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    assert!(glanced.store_exists);
    assert_eq!(glanced.silent_after_s, 60);
    assert!(!glanced.truncated);
    let ids: Vec<&str> =
        glanced.runs.iter().map(|run| run.run_id.as_str()).collect();
    // Newest first; the settled run never appears.
    assert_eq!(
        ids,
        vec![live.run.run_id.as_str(), created.run.run_id.as_str()]
    );
    let live_wire = &glanced.runs[0];
    assert_eq!(live_wire.label, "check");
    assert_eq!(live_wire.state, ToolRunStateWire::Running);
    assert_eq!(live_wire.last_activity_ts, NOW - 10);
    assert!(!live_wire.stop_requested);
}

#[test]
fn glance_caps_at_200_runs_with_truncated() {
    let (_temp, path) = store();
    for index in 0..(TOOL_RUN_GLANCE_MAX_RUNS + 1) {
        begin_run(
            &path,
            BeginArgs {
                now: NOW - i64::from(index),
                ..BeginArgs::named("check")
            },
        );
    }
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    assert_eq!(glanced.runs.len(), TOOL_RUN_GLANCE_MAX_RUNS as usize);
    assert!(glanced.truncated);
    // Newest first: the first row is the run created at NOW.
    assert_eq!(glanced.runs[0].created_ts, NOW);
}

#[test]
fn glance_reports_stage_progress_from_started_only_rows() {
    let (_temp, path) = store();
    let started = begin_run(&path, BeginArgs::named("check"));
    let run_id = &started.run.run_id;
    let base_ms = NOW * 1000;
    stage_finished(
        &path,
        run_id,
        "evt-s1",
        "st1",
        "fmt",
        base_ms + 1_000,
        base_ms + 4_000,
    );
    stage_started(
        &path,
        run_id,
        "evt-s2",
        "st2",
        "lint (mypy)",
        base_ms + 5_000,
    );
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    assert_eq!(glanced.runs.len(), 1);
    let wire = &glanced.runs[0];
    assert_eq!(wire.stages_done, 1);
    let current = wire.current_stage.as_ref().unwrap();
    assert_eq!(current.description, "lint (mypy)");
    assert_eq!(current.started_ms, base_ms + 5_000);
    // Last activity is the newest stage finish, converted to seconds.
    assert_eq!(wire.last_activity_ts, NOW + 5);
}

#[test]
fn glance_last_activity_prefers_newer_samples_or_stages() {
    let (_temp, path) = store();
    let first = begin_run(&path, BeginArgs::named("check"));
    stage_finished(
        &path,
        &first.run.run_id,
        "evt-s1",
        "st1",
        "fmt",
        NOW * 1000,
        (NOW + 100) * 1000,
    );
    sample(&path, &first.run.run_id, "evt-sample", NOW + 90);
    // Newest stage finish (ms) wins over the older sample (s).
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    assert_eq!(glanced.runs[0].last_activity_ts, NOW + 100);
    // A newer sample wins back.
    sample(&path, &first.run.run_id, "evt-sample-2", NOW + 400);
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    assert_eq!(glanced.runs[0].last_activity_ts, NOW + 400);
}

#[test]
fn glance_reference_run_needs_same_digest_and_complete_stages() {
    let (_temp, path) = store();
    // Reference candidate: same digest, two complete stages.
    let reference = begin_run(
        &path,
        BeginArgs {
            now: NOW - 500,
            ..BeginArgs::named("check")
        },
    );
    stage_finished(
        &path,
        &reference.run.run_id,
        "evt-r1",
        "rst1",
        "fmt",
        (NOW - 500) * 1000,
        (NOW - 490) * 1000,
    );
    stage_finished(
        &path,
        &reference.run.run_id,
        "evt-r2",
        "rst2",
        "lint",
        (NOW - 490) * 1000,
        (NOW - 480) * 1000,
    );
    finish_run(
        &path,
        &reference.run.run_id,
        ToolRunStateWire::Succeeded,
        Some(0),
        None,
        None,
        Some(20_000),
        NOW - 400,
    );
    // Newer success with a different digest must not match.
    let other = begin_run(
        &path,
        BeginArgs {
            def_name: "check".to_string(),
            def_argv: vec!["just".into(), "check".into(), "--other".into()],
            display_argv: vec!["just".into(), "check".into(), "--other".into()],
            tool_name: Some("check".into()),
            project: Some("sase".into()),
            agent: Some("agent-1".into()),
            now: NOW - 300,
            commit_running: true,
            ..BeginArgs::default()
        },
    );
    stage_finished(
        &path,
        &other.run.run_id,
        "evt-o1",
        "ost1",
        "fmt",
        (NOW - 300) * 1000,
        (NOW - 290) * 1000,
    );
    finish_run(
        &path,
        &other.run.run_id,
        ToolRunStateWire::Succeeded,
        Some(0),
        None,
        None,
        Some(10_000),
        NOW - 200,
    );
    // Newest same-digest success still has an incomplete stage: skipped.
    let incomplete = begin_run(
        &path,
        BeginArgs {
            now: NOW - 100,
            ..BeginArgs::named("check")
        },
    );
    stage_started(
        &path,
        &incomplete.run.run_id,
        "evt-i1",
        "ist1",
        "fmt",
        (NOW - 100) * 1000,
    );
    finish_run(
        &path,
        &incomplete.run.run_id,
        ToolRunStateWire::Succeeded,
        Some(0),
        None,
        None,
        Some(1_000),
        NOW - 50,
    );
    let live = begin_run(&path, BeginArgs::named("check"));
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    let wire = glanced
        .runs
        .iter()
        .find(|run| run.run_id == live.run.run_id)
        .unwrap();
    assert_eq!(
        wire.reference_run_id.as_deref(),
        Some(reference.run.run_id.as_str())
    );
    assert_eq!(wire.stages_expected, Some(2));
}

#[test]
fn glance_reference_is_empty_without_a_match() {
    let (_temp, path) = store();
    let live = begin_run(
        &path,
        BeginArgs {
            tool_name: Some("fresh-tool".into()),
            def_name: "fresh-tool".into(),
            def_argv: vec!["fresh-tool".into()],
            display_argv: vec!["fresh-tool".into()],
            project: Some("sase".into()),
            agent: Some("agent-1".into()),
            now: NOW,
            commit_running: true,
            ..BeginArgs::default()
        },
    );
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    let wire = glanced
        .runs
        .iter()
        .find(|run| run.run_id == live.run.run_id)
        .unwrap();
    assert_eq!(wire.reference_run_id, None);
    assert_eq!(wire.stages_expected, None);
}

#[test]
fn glance_typical_matches_summarize() {
    use super::super::wire::ToolRunSummaryRequestWire;
    let (_temp, path) = store();
    let first = begin_run(
        &path,
        BeginArgs {
            now: NOW - 500,
            ..BeginArgs::named("check")
        },
    );
    finish_run(
        &path,
        &first.run.run_id,
        ToolRunStateWire::Succeeded,
        Some(0),
        None,
        None,
        Some(20_000),
        NOW - 400,
    );
    let live = begin_run(&path, BeginArgs::named("check"));
    let summary = summarize(
        &path,
        ToolRunSummaryRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project: "sase".into(),
            tool_name: "check".into(),
            definition_digest: live.run.definition_digest.clone(),
            now_ts: Some(NOW + 1000),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    let wire = glanced
        .runs
        .iter()
        .find(|run| run.run_id == live.run.run_id)
        .unwrap();
    assert_eq!(wire.typical_ms, summary.typical_duration_ms);
    assert_eq!(wire.typical_samples, summary.typical_sample_count);
    assert_eq!(wire.typical_ms, Some(20_000));
}

#[test]
fn glance_and_briefs_flag_stop_requests() {
    let (_temp, path) = store();
    let started = begin_run(&path, BeginArgs::named("check"));
    request_stop(
        &path,
        ToolRunStopRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: started.run.run_id.clone(),
            requested_by: None,
            reason: Some("test".into()),
            now_ts: Some(NOW),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    assert!(glanced.runs[0].stop_requested);
    let briefed =
        tool_run_briefs(&path, briefs_request(), Duration::from_secs(1))
            .unwrap();
    assert!(briefed.runs[0].stop_requested);
}

#[test]
fn projections_never_emit_private_argv() {
    let (_temp, path) = store();
    let secret = "super-secret-private-argv-marker";
    let started = begin_run(
        &path,
        BeginArgs {
            display_argv: vec!["just".into(), "check".into()],
            private_argv: Some(vec![secret.into(), "--token".into()]),
            ..BeginArgs::named("check")
        },
    );
    // Fingerprint payloads stay out of projections too.
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE runs SET fingerprint_before_json = ?1 WHERE run_id = ?2",
        rusqlite::params![
            r#"{"marker":"super-secret-fingerprint-marker"}"#,
            started.run.run_id,
        ],
    )
    .unwrap();
    drop(conn);
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    let briefed =
        tool_run_briefs(&path, briefs_request(), Duration::from_secs(1))
            .unwrap();
    let nodes = tool_run_node_summaries(
        &path,
        ToolRunNodeSummariesRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            nodes: vec![ToolRunNodeSelectorWire {
                key: "agent-1".into(),
                agents: vec!["agent-1".into()],
                owners: Vec::new(),
                since_ts: None,
            }],
            per_node_limit: 20,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    for result in [
        serde_json::to_string(&glanced).unwrap(),
        serde_json::to_string(&briefed).unwrap(),
        serde_json::to_string(&nodes).unwrap(),
    ] {
        assert!(!result.contains(secret), "private argv leaked");
        assert!(
            !result.contains("super-secret-fingerprint-marker"),
            "fingerprint leaked"
        );
        // The tool label still reaches the surface; only secrets are cut.
        assert!(result.contains("check"), "tool label missing");
    }
}

fn brief_for(path: &Path, run_id: &str) -> super::wire::ToolRunBriefWire {
    let briefed =
        tool_run_briefs(path, briefs_request(), Duration::from_secs(1))
            .unwrap();
    briefed
        .runs
        .into_iter()
        .find(|run| run.run_id == run_id)
        .unwrap()
}

#[test]
fn brief_verdict_buckets_follow_state_cause_and_triage() {
    let (_temp, path) = store();
    let pass = begin_run(
        &path,
        BeginArgs {
            agent: Some("pass-agent".into()),
            ..BeginArgs::named("check")
        },
    );
    finish_run(
        &path,
        &pass.run.run_id,
        ToolRunStateWire::Succeeded,
        Some(0),
        None,
        None,
        Some(4_000),
        NOW + 10,
    );
    let new = begin_run(
        &path,
        BeginArgs {
            agent: Some("new-agent".into()),
            ..BeginArgs::named("check")
        },
    );
    seed_triage(
        &path,
        &new.run.run_id,
        &[("lint", "parsed")],
        &[("lint", "new", "pytest")],
        true,
        true,
    );
    finish_run(
        &path,
        &new.run.run_id,
        ToolRunStateWire::Failed,
        Some(3),
        None,
        Some(ToolRunTerminalCauseWire::Exited),
        Some(5_000),
        NOW + 10,
    );
    let known = begin_run(
        &path,
        BeginArgs {
            agent: Some("known-agent".into()),
            ..BeginArgs::named("check")
        },
    );
    seed_triage(
        &path,
        &known.run.run_id,
        &[("lint", "parsed")],
        &[("lint", "known", "pytest")],
        true,
        true,
    );
    finish_run(
        &path,
        &known.run.run_id,
        ToolRunStateWire::Failed,
        Some(3),
        None,
        Some(ToolRunTerminalCauseWire::Exited),
        Some(6_000),
        NOW + 10,
    );
    let undetermined = begin_run(
        &path,
        BeginArgs {
            agent: Some("und-agent".into()),
            ..BeginArgs::named("check")
        },
    );
    finish_run(
        &path,
        &undetermined.run.run_id,
        ToolRunStateWire::Failed,
        Some(3),
        None,
        Some(ToolRunTerminalCauseWire::Exited),
        Some(7_000),
        NOW + 10,
    );
    let stopped = begin_run(
        &path,
        BeginArgs {
            agent: Some("stop-agent".into()),
            ..BeginArgs::named("check")
        },
    );
    finish_run(
        &path,
        &stopped.run.run_id,
        ToolRunStateWire::Interrupted,
        None,
        None,
        Some(ToolRunTerminalCauseWire::StopRequested),
        None,
        NOW + 10,
    );
    let killed = begin_run(
        &path,
        BeginArgs {
            agent: Some("kill-agent".into()),
            ..BeginArgs::named("check")
        },
    );
    finish_run(
        &path,
        &killed.run.run_id,
        ToolRunStateWire::Signaled,
        None,
        Some(15),
        Some(ToolRunTerminalCauseWire::Signal),
        None,
        NOW + 10,
    );
    let lost = begin_run(
        &path,
        BeginArgs {
            agent: Some("lost-agent".into()),
            ..BeginArgs::named("check")
        },
    );
    finish_run(
        &path,
        &lost.run.run_id,
        ToolRunStateWire::Lost,
        None,
        None,
        Some(ToolRunTerminalCauseWire::OwnerLost),
        None,
        NOW + 10,
    );
    let pass_brief = brief_for(&path, &pass.run.run_id);
    assert_eq!(pass_brief.verdict.bucket, ToolRunVerdictBucketWire::Pass);
    assert_eq!(pass_brief.verdict.verdict.as_deref(), Some("pass"));
    let new_brief = brief_for(&path, &new.run.run_id);
    assert_eq!(
        new_brief.verdict.bucket,
        ToolRunVerdictBucketWire::NewFailures
    );
    assert_eq!(new_brief.verdict.verdict.as_deref(), Some("new_failures"));
    assert_eq!(new_brief.verdict.new, 1);
    assert_eq!(new_brief.verdict.reasons, vec!["has_new_item".to_string()]);
    let known_brief = brief_for(&path, &known.run.run_id);
    assert_eq!(
        known_brief.verdict.bucket,
        ToolRunVerdictBucketWire::KnownOnly
    );
    assert_eq!(
        known_brief.verdict.verdict.as_deref(),
        Some("no_new_failures")
    );
    assert_eq!(known_brief.verdict.known, 1);
    let und_brief = brief_for(&path, &undetermined.run.run_id);
    assert_eq!(
        und_brief.verdict.bucket,
        ToolRunVerdictBucketWire::Undetermined
    );
    assert_eq!(und_brief.verdict.verdict.as_deref(), Some("undetermined"));
    assert_eq!(und_brief.verdict.reasons, vec!["not_triaged".to_string()]);
    let stopped_brief = brief_for(&path, &stopped.run.run_id);
    assert_eq!(
        stopped_brief.verdict.bucket,
        ToolRunVerdictBucketWire::Stopped
    );
    let killed_brief = brief_for(&path, &killed.run.run_id);
    assert_eq!(
        killed_brief.verdict.bucket,
        ToolRunVerdictBucketWire::Killed
    );
    let lost_brief = brief_for(&path, &lost.run.run_id);
    assert_eq!(lost_brief.verdict.bucket, ToolRunVerdictBucketWire::Lost);
}

#[test]
fn briefs_filter_and_paginate() {
    let (_temp, path) = store();
    for (index, agent) in ["a1", "a2", "a1"].iter().enumerate() {
        begin_run(
            &path,
            BeginArgs {
                agent: Some(agent.to_string()),
                now: NOW + index as i64,
                ..BeginArgs::named("check")
            },
        );
    }
    let other = begin_run(
        &path,
        BeginArgs {
            project: Some("other".into()),
            agent: Some("a1".into()),
            now: NOW + 10,
            ..BeginArgs::named("lint")
        },
    );
    finish_run(
        &path,
        &other.run.run_id,
        ToolRunStateWire::Failed,
        Some(1),
        None,
        Some(ToolRunTerminalCauseWire::Exited),
        Some(1_000),
        NOW + 20,
    );
    // Newest first across the whole store.
    let all = tool_run_briefs(&path, briefs_request(), Duration::from_secs(1))
        .unwrap();
    assert_eq!(all.runs.len(), 4);
    assert_eq!(all.runs[0].run_id, other.run.run_id);
    // Project + tool + agent + state filters compose.
    let filtered = tool_run_briefs(
        &path,
        ToolRunBriefsRequestWire {
            project: Some("sase".into()),
            tool: Some("check".into()),
            agents: vec!["a1".into()],
            ..briefs_request()
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(filtered.runs.len(), 2);
    let failed = tool_run_briefs(
        &path,
        ToolRunBriefsRequestWire {
            states: vec![ToolRunStateWire::Failed],
            ..briefs_request()
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(failed.runs.len(), 1);
    assert_eq!(failed.runs[0].run_id, other.run.run_id);
    let since = tool_run_briefs(
        &path,
        ToolRunBriefsRequestWire {
            since_ts: Some(NOW + 10),
            ..briefs_request()
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(since.runs.len(), 1);
    // Cursor pagination walks newest first without repeats.
    let page_one = tool_run_briefs(
        &path,
        ToolRunBriefsRequestWire {
            limit: 2,
            ..briefs_request()
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(page_one.runs.len(), 2);
    let cursor = page_one.next_cursor.clone().unwrap();
    let page_two = tool_run_briefs(
        &path,
        ToolRunBriefsRequestWire {
            limit: 2,
            cursor: Some(cursor),
            ..briefs_request()
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(page_two.runs.len(), 2);
    assert_eq!(page_two.next_cursor, None);
    assert_ne!(page_one.runs[0].run_id, page_two.runs[0].run_id);
}

#[test]
fn briefs_match_owners_and_reject_bad_requests() {
    let (_temp, path) = store();
    let owned = begin_run(
        &path,
        BeginArgs {
            agent: Some("starter".into()),
            owner_kind: Some("monitor".into()),
            owner_id: Some("mon-1".into()),
            ..BeginArgs::named("check")
        },
    );
    let matched = tool_run_briefs(
        &path,
        ToolRunBriefsRequestWire {
            owners: vec![ToolRunBriefOwnerWire {
                kind: "monitor".into(),
                id: "mon-1".into(),
            }],
            ..briefs_request()
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(matched.runs.len(), 1);
    assert_eq!(matched.runs[0].run_id, owned.run.run_id);
    assert_eq!(matched.runs[0].owner_kind.as_deref(), Some("monitor"));
    let missing = tool_run_briefs(
        &path,
        ToolRunBriefsRequestWire {
            owners: vec![ToolRunBriefOwnerWire {
                kind: "monitor".into(),
                id: "nope".into(),
            }],
            ..briefs_request()
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(missing.runs.is_empty());
    for request in [
        ToolRunBriefsRequestWire {
            limit: 0,
            ..briefs_request()
        },
        ToolRunBriefsRequestWire {
            limit: 501,
            ..briefs_request()
        },
        ToolRunBriefsRequestWire {
            cursor: Some("bogus".into()),
            ..briefs_request()
        },
    ] {
        assert!(
            tool_run_briefs(&path, request, Duration::from_secs(1)).is_err()
        );
    }
    assert!(tool_run_briefs(
        &path,
        ToolRunBriefsRequestWire {
            schema_version: 999,
            ..briefs_request()
        },
        Duration::from_secs(1)
    )
    .is_err());
}

#[test]
fn brief_labels_derive_from_argv_and_cap_at_12_chars() {
    let (_temp, path) = store();
    let adhoc = begin_run(
        &path,
        BeginArgs {
            tool_name: None,
            display_argv: vec!["/usr/local/bin/super-long-tool-name".into()],
            ..BeginArgs::named("check")
        },
    );
    let brief = brief_for(&path, &adhoc.run.run_id);
    assert_eq!(brief.tool_name, None);
    assert_eq!(brief.label, "super-long-t");
    // begin stores the normalized definition name, so the long name goes
    // in the definition for this case.
    let named = begin_run(
        &path,
        BeginArgs {
            tool_name: Some("averylongtoolname".into()),
            def_name: "averylongtoolname".into(),
            def_argv: vec!["averylongtoolname".into()],
            display_argv: vec!["averylongtoolname".into()],
            project: Some("sase".into()),
            agent: Some("agent-1".into()),
            now: NOW,
            commit_running: true,
            ..BeginArgs::default()
        },
    );
    let brief = brief_for(&path, &named.run.run_id);
    assert_eq!(brief.label, "averylongtoo");
}

#[test]
fn brief_detail_pruned_marks_settled_runs_without_stages() {
    let (_temp, path) = store();
    let with_stages = begin_run(&path, BeginArgs::named("check"));
    stage_finished(
        &path,
        &with_stages.run.run_id,
        "evt-w1",
        "wst1",
        "fmt",
        NOW * 1000,
        (NOW + 5) * 1000,
    );
    finish_run(
        &path,
        &with_stages.run.run_id,
        ToolRunStateWire::Succeeded,
        Some(0),
        None,
        None,
        Some(5_000),
        NOW + 10,
    );
    let without_stages = begin_run(
        &path,
        BeginArgs {
            agent: Some("agent-2".into()),
            ..BeginArgs::named("check")
        },
    );
    finish_run(
        &path,
        &without_stages.run.run_id,
        ToolRunStateWire::Succeeded,
        Some(0),
        None,
        None,
        Some(5_000),
        NOW + 10,
    );
    let live = begin_run(
        &path,
        BeginArgs {
            agent: Some("agent-3".into()),
            ..BeginArgs::named("check")
        },
    );
    assert!(!brief_for(&path, &with_stages.run.run_id).detail_pruned);
    assert!(brief_for(&path, &without_stages.run.run_id).detail_pruned);
    assert!(!brief_for(&path, &live.run.run_id).detail_pruned);
}

#[test]
fn brief_page_of_50_runs_stays_under_64k_with_fingerprints() {
    let (_temp, path) = store();
    for index in 0..50 {
        let started = begin_run(
            &path,
            BeginArgs {
                now: NOW + i64::from(index),
                ..BeginArgs::named("check")
            },
        );
        finish_run(
            &path,
            &started.run.run_id,
            ToolRunStateWire::Succeeded,
            Some(0),
            None,
            None,
            Some(1_000 + i64::from(index)),
            NOW + 100 + i64::from(index),
        );
    }
    let conn = rusqlite::Connection::open(&path).unwrap();
    let blob = format!(r#"{{"fp":"{}"}}"#, "0123456789abcdef".repeat(320));
    conn.execute(
        "UPDATE runs SET fingerprint_before_json = ?1, fingerprint_after_json = ?1",
        rusqlite::params![blob],
    )
    .unwrap();
    drop(conn);
    let briefed = tool_run_briefs(
        &path,
        ToolRunBriefsRequestWire {
            limit: 50,
            ..briefs_request()
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(briefed.runs.len(), 50);
    let json = serde_json::to_string(&briefed).unwrap();
    assert!(
        json.len() < 64 * 1024,
        "50-run page is {} bytes",
        json.len()
    );
    assert!(!json.contains("0123456789abcdef0123456789abcdef0123"));
}

#[test]
fn node_summaries_match_agents_owners_and_since() {
    let (_temp, path) = store();
    let agent_only = begin_run(
        &path,
        BeginArgs {
            agent: Some("turn-1".into()),
            now: NOW,
            ..BeginArgs::named("check")
        },
    );
    let owned = begin_run(
        &path,
        BeginArgs {
            agent: Some("starter".into()),
            owner_kind: Some("monitor".into()),
            owner_id: Some("mon-1".into()),
            now: NOW + 1,
            ..BeginArgs::named("check")
        },
    );
    let both = begin_run(
        &path,
        BeginArgs {
            agent: Some("turn-1".into()),
            owner_kind: Some("monitor".into()),
            owner_id: Some("mon-1".into()),
            now: NOW + 2,
            ..BeginArgs::named("check")
        },
    );
    let _old = begin_run(
        &path,
        BeginArgs {
            agent: Some("turn-1".into()),
            now: NOW - 10_000,
            ..BeginArgs::named("check")
        },
    );
    let summarize = |nodes: Vec<ToolRunNodeSelectorWire>| {
        tool_run_node_summaries(
            &path,
            ToolRunNodeSummariesRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                nodes,
                per_node_limit: 20,
            },
            Duration::from_secs(1),
        )
        .unwrap()
    };
    let agent_key = "agent".to_string();
    let owner_key = "owner".to_string();
    let union_key = "union".to_string();
    let result = summarize(vec![
        ToolRunNodeSelectorWire {
            key: agent_key.clone(),
            agents: vec!["turn-1".into()],
            owners: Vec::new(),
            since_ts: Some(NOW - 100),
        },
        ToolRunNodeSelectorWire {
            key: owner_key.clone(),
            agents: Vec::new(),
            owners: vec![ToolRunBriefOwnerWire {
                kind: "monitor".into(),
                id: "mon-1".into(),
            }],
            since_ts: None,
        },
        ToolRunNodeSelectorWire {
            key: union_key.clone(),
            agents: vec!["turn-1".into()],
            owners: vec![ToolRunBriefOwnerWire {
                kind: "monitor".into(),
                id: "mon-1".into(),
            }],
            since_ts: None,
        },
    ]);
    assert!(result.store_exists);
    assert_eq!(result.silent_after_s, 60);
    let by_key: std::collections::HashMap<&str, _> = result
        .nodes
        .iter()
        .map(|node| (node.key.as_str(), node))
        .collect();
    // since_ts drops the old run from the agent node.
    assert_eq!(by_key[agent_key.as_str()].total_runs, 2);
    // Owner node matches both owned runs.
    assert_eq!(by_key[owner_key.as_str()].total_runs, 2);
    // Union dedupes the doubly-matching run.
    let union = by_key[union_key.as_str()];
    assert_eq!(union.total_runs, 4);
    let mut union_ids: Vec<&str> =
        union.runs.iter().map(|run| run.run_id.as_str()).collect();
    union_ids.sort_unstable();
    union_ids.dedup();
    assert_eq!(union_ids.len(), union.runs.len());
    assert!(
        union.runs.iter().any(|run| run.run_id == both.run.run_id)
            && union.runs.iter().any(|run| run.run_id == owned.run.run_id)
            && union
                .runs
                .iter()
                .any(|run| run.run_id == agent_only.run.run_id)
    );
    // An empty selector matches nothing, never the machine.
    let empty = summarize(vec![ToolRunNodeSelectorWire {
        key: "empty".into(),
        agents: Vec::new(),
        owners: Vec::new(),
        since_ts: None,
    }]);
    assert_eq!(empty.nodes[0].total_runs, 0);
    assert!(empty.nodes[0].runs.is_empty());
}

#[test]
fn node_summaries_page_and_group_by_tool() {
    let (_temp, path) = store();
    for index in 0..3 {
        begin_run(
            &path,
            BeginArgs {
                agent: Some("turn-9".into()),
                now: NOW + index,
                ..BeginArgs::named("check")
            },
        );
    }
    begin_run(
        &path,
        BeginArgs {
            tool_name: Some("lint".into()),
            def_name: "lint".into(),
            def_argv: vec!["just".into(), "lint".into()],
            display_argv: vec!["just".into(), "lint".into()],
            project: Some("sase".into()),
            agent: Some("turn-9".into()),
            now: NOW - 5,
            commit_running: true,
            ..BeginArgs::default()
        },
    );
    let selector = ToolRunNodeSelectorWire {
        key: "turn-9".into(),
        agents: vec!["turn-9".into()],
        owners: Vec::new(),
        since_ts: None,
    };
    let full = tool_run_node_summaries(
        &path,
        ToolRunNodeSummariesRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            nodes: vec![selector.clone()],
            per_node_limit: 10,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(full.nodes[0].total_runs, 4);
    assert!(!full.nodes[0].truncated);
    assert_eq!(full.nodes[0].runs.len(), 4);
    // One entry per label, newest first.
    let labels: Vec<&str> = full.nodes[0]
        .latest_by_tool
        .iter()
        .map(|run| run.label.as_str())
        .collect();
    assert_eq!(labels, vec!["check", "lint"]);
    // A live run also appears in the live list.
    assert_eq!(full.nodes[0].live.len(), 4);
    let paged = tool_run_node_summaries(
        &path,
        ToolRunNodeSummariesRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            nodes: vec![selector],
            per_node_limit: 2,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(paged.nodes[0].runs.len(), 2);
    assert!(paged.nodes[0].truncated);
    assert_eq!(paged.nodes[0].latest_by_tool.len(), 2);
}

#[test]
fn node_summaries_reject_bad_requests() {
    let (_temp, path) = store();
    let selector = ToolRunNodeSelectorWire {
        key: "k".into(),
        agents: vec!["a".into()],
        owners: Vec::new(),
        since_ts: None,
    };
    for request in [
        ToolRunNodeSummariesRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            nodes: Vec::new(),
            per_node_limit: 20,
        },
        ToolRunNodeSummariesRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            nodes: vec![selector.clone(); 65],
            per_node_limit: 20,
        },
        ToolRunNodeSummariesRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            nodes: vec![selector.clone()],
            per_node_limit: 0,
        },
        ToolRunNodeSummariesRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            nodes: vec![selector.clone()],
            per_node_limit: 101,
        },
        ToolRunNodeSummariesRequestWire {
            schema_version: 999,
            nodes: vec![selector],
            per_node_limit: 20,
        },
    ] {
        assert!(tool_run_node_summaries(
            &path,
            request,
            Duration::from_secs(1)
        )
        .is_err());
    }
}

#[test]
fn projections_report_a_missing_store() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("missing").join("runs.sqlite");
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    assert!(!glanced.store_exists);
    assert!(glanced.runs.is_empty());
    assert!(!glanced.diagnostics.is_empty());
    let briefed =
        tool_run_briefs(&path, briefs_request(), Duration::from_secs(1))
            .unwrap();
    assert!(!briefed.store_exists);
    assert!(briefed.runs.is_empty());
    let nodes = tool_run_node_summaries(
        &path,
        ToolRunNodeSummariesRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            nodes: vec![ToolRunNodeSelectorWire {
                key: "k".into(),
                agents: vec!["a".into()],
                owners: Vec::new(),
                since_ts: None,
            }],
            per_node_limit: 20,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(!nodes.store_exists);
    assert!(nodes.nodes.is_empty());
}

#[test]
fn projections_tolerate_missing_triage_tables() {
    let (_temp, path) = store();
    let failed = begin_run(&path, BeginArgs::named("check"));
    finish_run(
        &path,
        &failed.run.run_id,
        ToolRunStateWire::Failed,
        Some(3),
        None,
        Some(ToolRunTerminalCauseWire::Exited),
        Some(5_000),
        NOW + 10,
    );
    let conn = rusqlite::Connection::open(&path).unwrap();
    for table in [
        "tool_triage_items",
        "tool_triage_stages",
        "tool_triage_runs",
    ] {
        conn.execute(&format!("DROP TABLE {table}"), []).unwrap();
    }
    drop(conn);
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    assert!(glanced.store_exists);
    let briefed =
        tool_run_briefs(&path, briefs_request(), Duration::from_secs(1))
            .unwrap();
    assert_eq!(briefed.runs.len(), 1);
    // Without triage tables the verdict falls back to the unstored path.
    assert_eq!(
        briefed.runs[0].verdict.bucket,
        ToolRunVerdictBucketWire::Undetermined
    );
    let nodes = tool_run_node_summaries(
        &path,
        ToolRunNodeSummariesRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            nodes: vec![ToolRunNodeSelectorWire {
                key: "agent-1".into(),
                agents: vec!["agent-1".into()],
                owners: Vec::new(),
                since_ts: None,
            }],
            per_node_limit: 20,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(nodes.nodes[0].total_runs, 1);
}

#[test]
fn projections_work_on_a_pre_index_store() {
    let (_temp, path) = store();
    let live = begin_run(
        &path,
        BeginArgs {
            owner_kind: Some("monitor".into()),
            owner_id: Some("mon-9".into()),
            ..BeginArgs::named("check")
        },
    );
    let conn = rusqlite::Connection::open(&path).unwrap();
    for index in [
        "idx_tool_runs_agent_created",
        "idx_tool_runs_owner_created",
        "idx_tool_runs_reference",
    ] {
        conn.execute(&format!("DROP INDEX IF EXISTS {index}"), [])
            .unwrap();
    }
    drop(conn);
    let glanced =
        tool_run_live_glance(&path, glance_request(), Duration::from_secs(1))
            .unwrap();
    assert_eq!(glanced.runs.len(), 1);
    assert_eq!(glanced.runs[0].run_id, live.run.run_id);
    let briefed = tool_run_briefs(
        &path,
        ToolRunBriefsRequestWire {
            agents: vec!["agent-1".into()],
            ..briefs_request()
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(briefed.runs.len(), 1);
    let nodes = tool_run_node_summaries(
        &path,
        ToolRunNodeSummariesRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            nodes: vec![ToolRunNodeSelectorWire {
                key: "mon".into(),
                agents: Vec::new(),
                owners: vec![ToolRunBriefOwnerWire {
                    kind: "monitor".into(),
                    id: "mon-9".into(),
                }],
                since_ts: None,
            }],
            per_node_limit: 20,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(nodes.nodes[0].total_runs, 1);
    assert_eq!(nodes.nodes[0].live.len(), 1);
}

#[test]
fn schema_version_mismatches_are_rejected() {
    let (_temp, path) = store();
    let error = tool_run_live_glance(
        &path,
        ToolRunLiveGlanceRequestWire {
            schema_version: 999,
            now_ts: None,
        },
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(error, ToolRunError::SchemaVersion { .. }));
}

fn detail_for(
    path: &Path,
    run_id: &str,
    item_limit: u32,
    now: i64,
) -> super::wire::ToolRunDetailResultWire {
    tool_run_detail(
        path,
        ToolRunDetailRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            witness_window_days: 7,
            item_limit,
            now_ts: Some(now),
        },
        Duration::from_secs(1),
    )
    .unwrap()
}

#[test]
fn detail_returns_brief_stages_argv_logs_and_children() {
    let (_temp, path) = store();
    let parent = begin_run(
        &path,
        BeginArgs {
            display_argv: vec!["just".into(), "check".into(), "--fast".into()],
            private_argv: Some(vec![
                "just".into(),
                "check".into(),
                "--secret-token".into(),
            ]),
            log_stdout_path: Some("/tmp/out.log".into()),
            log_stderr_path: Some("/tmp/err.log".into()),
            ..BeginArgs::named("check")
        },
    );
    stage_finished(
        &path,
        &parent.run.run_id,
        "evt-s1",
        "stage-1",
        "lint",
        NOW * 1000,
        (NOW + 60) * 1000,
    );
    let child = begin_run(
        &path,
        BeginArgs {
            parent_run_id: Some(parent.run.run_id.clone()),
            now: NOW + 10,
            ..BeginArgs::named("check")
        },
    );
    let detail = detail_for(&path, &parent.run.run_id, 50, NOW + 100);
    assert!(detail.store_exists);
    assert!(detail.found);
    let brief = detail.brief.as_ref().expect("detail carries the run brief");
    assert_eq!(brief.run_id, parent.run.run_id);
    assert_eq!(brief.label, "check");
    assert_eq!(detail.display_argv, vec!["just", "check", "--fast"]);
    // Only the safe argv reaches the card; the private argv never does.
    let json = serde_json::to_value(&detail).unwrap();
    assert!(!json.to_string().contains("--secret-token"));
    assert_eq!(detail.stages.len(), 1);
    let stage = &detail.stages[0];
    assert_eq!(stage.description, "lint");
    assert_eq!(stage.started_ms, Some(NOW * 1000));
    assert_eq!(stage.finished_ms, Some((NOW + 60) * 1000));
    assert_eq!(stage.elapsed_ms, Some(60_000));
    assert_eq!(stage.exit_code, Some(0));
    assert!(!stage.incomplete);
    assert_eq!(stage.output_bytes, Some(100));
    // A live run with no reference run yet has no expected stages.
    assert!(detail.expected_stages.is_empty());
    assert!(!detail.items_truncated);
    assert!(detail.triage_items.is_empty());
    assert_eq!(detail.child_runs.len(), 1);
    assert_eq!(detail.child_runs[0].run_id, child.run.run_id);
    let logs = detail.logs.expect("detail carries log metadata");
    assert_eq!(logs.stdout_path.as_deref(), Some("/tmp/out.log"));
    assert_eq!(logs.stderr_path.as_deref(), Some("/tmp/err.log"));
    assert!(logs.has_private_argv);
    assert!(!detail.detail_pruned);
    assert!(detail.diagnostics.is_empty());
}

#[test]
fn detail_expected_stages_come_from_the_reference_run() {
    let (_temp, path) = store();
    let reference = begin_run(
        &path,
        BeginArgs {
            now: NOW - 1000,
            ..BeginArgs::named("check")
        },
    );
    stage_finished(
        &path,
        &reference.run.run_id,
        "evt-r1",
        "ref-1",
        "setup",
        (NOW - 1000) * 1000,
        (NOW - 990) * 1000,
    );
    stage_finished(
        &path,
        &reference.run.run_id,
        "evt-r2",
        "ref-2",
        "lint",
        (NOW - 990) * 1000,
        (NOW - 900) * 1000,
    );
    finish_run(
        &path,
        &reference.run.run_id,
        ToolRunStateWire::Succeeded,
        Some(0),
        None,
        None,
        Some(100_000),
        NOW - 900,
    );
    // A live run gets the full expected timeline for pending stages.
    let target = begin_run(&path, BeginArgs::named("check"));
    let live = detail_for(&path, &target.run.run_id, 50, NOW + 50);
    assert_eq!(live.expected_stages.len(), 2);
    assert_eq!(live.expected_stages[0].description, "setup");
    assert_eq!(live.expected_stages[0].elapsed_ms, Some(10_000));
    assert_eq!(live.expected_stages[1].description, "lint");
    // A run that died mid-stage is not its own reference (its stages are
    // incomplete), so it keeps the older timeline for not-reached stages.
    stage_finished(
        &path,
        &target.run.run_id,
        "evt-t1",
        "tgt-1",
        "setup",
        NOW * 1000,
        (NOW + 5) * 1000,
    );
    stage_started(
        &path,
        &target.run.run_id,
        "evt-t2",
        "tgt-2",
        "lint",
        (NOW + 5) * 1000,
    );
    finish_run(
        &path,
        &target.run.run_id,
        ToolRunStateWire::Failed,
        Some(1),
        None,
        None,
        Some(5_000),
        NOW + 60,
    );
    let settled_early = detail_for(&path, &target.run.run_id, 50, NOW + 70);
    assert_eq!(settled_early.expected_stages.len(), 2);
    // A run that reached every reference stage drops the timeline.
    let complete = begin_run(
        &path,
        BeginArgs {
            agent: Some("agent-2".into()),
            now: NOW + 80,
            ..BeginArgs::named("check")
        },
    );
    stage_finished(
        &path,
        &complete.run.run_id,
        "evt-c1",
        "cmp-1",
        "setup",
        (NOW + 80) * 1000,
        (NOW + 85) * 1000,
    );
    stage_finished(
        &path,
        &complete.run.run_id,
        "evt-c2",
        "cmp-2",
        "lint",
        (NOW + 85) * 1000,
        (NOW + 95) * 1000,
    );
    finish_run(
        &path,
        &complete.run.run_id,
        ToolRunStateWire::Succeeded,
        Some(0),
        None,
        None,
        Some(15_000),
        NOW + 95,
    );
    let settled_full = detail_for(&path, &complete.run.run_id, 50, NOW + 100);
    assert!(settled_full.expected_stages.is_empty());
}

#[test]
fn detail_triage_items_order_witnesses_and_truncate() {
    let (_temp, path) = store();
    let first = begin_run(&path, BeginArgs::named("check"));
    seed_triage(
        &path,
        &first.run.run_id,
        &[("*", "parsed")],
        &[
            ("*", "known", "ext-a"),
            ("*", "new", "ext-a"),
            ("*", "", "ext-a"),
        ],
        false,
        false,
    );
    // A second agent's run witnesses the same signature.
    let second = begin_run(
        &path,
        BeginArgs {
            agent: Some("agent-2".into()),
            now: NOW + 5,
            ..BeginArgs::named("check")
        },
    );
    seed_triage(
        &path,
        &second.run.run_id,
        &[("*", "parsed")],
        &[("*", "known", "ext-a")],
        false,
        false,
    );
    let detail = detail_for(&path, &first.run.run_id, 50, NOW + 100);
    assert_eq!(detail.triage_items.len(), 3);
    // Card order: NEW, UNKNOWN, unlabeled, KNOWN, FLAKY. The empty class
    // reads as unlabeled.
    let classes: Vec<Option<&str>> = detail
        .triage_items
        .iter()
        .map(|item| item.class.as_deref())
        .collect();
    assert_eq!(classes, vec![Some("new"), None, Some("known")]);
    let new = &detail.triage_items[0];
    assert_eq!(new.stage_key, "*");
    assert_eq!(new.occurrences, 1);
    assert!(new.locator_paths.is_empty());
    // Both runs share the seeded signature, so each item sees two
    // witness runs across two agents inside the window.
    assert_eq!(new.witness_runs, 2);
    assert_eq!(new.witness_agents, 2);
    assert_eq!(new.first_seen_ts, Some(NOW));
    assert_eq!(new.last_seen_ts, Some(NOW));
    assert!(!detail.items_truncated);
    let truncated = detail_for(&path, &first.run.run_id, 2, NOW + 100);
    assert_eq!(truncated.triage_items.len(), 2);
    assert!(truncated.items_truncated);
    assert_eq!(truncated.triage_items[0].class.as_deref(), Some("new"));
}

#[test]
fn detail_stage_counts_join_items_by_stage_and_cap_locators() {
    let (_temp, path) = store();
    let started = begin_run(&path, BeginArgs::named("check"));
    stage_started(
        &path,
        &started.run.run_id,
        "evt-s1",
        "stage-1",
        "lint",
        NOW * 1000,
    );
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "INSERT INTO tool_triage_items(
            item_id, run_id, stage_id, stage_key, extractor,
            extractor_version, signature, display, locator_paths_json,
            occurrences, created_ts, class
         ) VALUES ('item-1', ?1, 'stage-1', 'lint', 'ext-b', 2, ?2,
                   'broken lint', '[\"a\",\"b\",\"c\",\"d\",\"e\"]', 3, ?3, 'new')",
        rusqlite::params![
            started.run.run_id,
            "b".repeat(64),
            NOW,
        ],
    )
    .unwrap();
    drop(conn);
    let detail = detail_for(&path, &started.run.run_id, 50, NOW + 100);
    assert_eq!(detail.stages.len(), 1);
    assert!(detail.stages[0].incomplete);
    assert_eq!(detail.stages[0].counts.new, 1);
    assert_eq!(detail.stages[0].counts.known, 0);
    assert_eq!(detail.stages[0].counts.flaky, 0);
    assert_eq!(detail.stages[0].counts.unknown, 0);
    assert_eq!(detail.triage_items.len(), 1);
    let item = &detail.triage_items[0];
    assert_eq!(item.class.as_deref(), Some("new"));
    assert_eq!(item.locator_paths, vec!["a", "b", "c"]);
    assert_eq!(item.occurrences, 3);
    assert_eq!(item.witness_runs, 1);
    assert_eq!(item.witness_agents, 1);
}

#[test]
fn detail_reports_missing_stores_and_runs() {
    let (temp, path) = store();
    let missing = temp.path().join("missing.sqlite");
    let absent = tool_run_detail(
        &missing,
        ToolRunDetailRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "run-nope".to_string(),
            witness_window_days: 7,
            item_limit: 50,
            now_ts: Some(NOW),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(!absent.store_exists);
    assert!(!absent.found);
    assert!(absent.brief.is_none());
    assert!(!absent.diagnostics.is_empty());
    // The store file only exists after the first write.
    begin_run(&path, BeginArgs::named("check"));
    let unknown = detail_for(&path, "run-nope", 50, NOW);
    assert!(unknown.store_exists);
    assert!(!unknown.found);
    assert!(unknown.brief.is_none());
    assert!(unknown.stages.is_empty());
    assert!(unknown.diagnostics.join(" ").contains("run-nope"));
}

#[test]
fn detail_rejects_bad_requests() {
    let (_temp, path) = store();
    let request =
        |run_id: &str, days: u32, limit: u32| ToolRunDetailRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            witness_window_days: days,
            item_limit: limit,
            now_ts: Some(NOW),
        };
    for (run_id, days, limit) in [
        ("", 7, 50),
        ("run-1", 0, 50),
        ("run-1", 31, 50),
        ("run-1", 7, 0),
        ("run-1", 7, 201),
    ] {
        assert!(
            tool_run_detail(
                &path,
                request(run_id, days, limit),
                Duration::from_secs(1)
            )
            .is_err(),
            "run_id={run_id:?} days={days} limit={limit}"
        );
    }
    assert!(tool_run_detail(
        &path,
        ToolRunDetailRequestWire {
            schema_version: 999,
            run_id: "run-1".to_string(),
            witness_window_days: 7,
            item_limit: 50,
            now_ts: Some(NOW),
        },
        Duration::from_secs(1),
    )
    .is_err());
    // Request wires reject unknown fields.
    assert!(serde_json::from_value::<ToolRunDetailRequestWire>(
        serde_json::json!({
            "schema_version": 1,
            "run_id": "run-1",
            "nope": true,
        })
    )
    .is_err());
}
