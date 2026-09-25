use super::{definition, store};
use crate::tool_run::catalog::normalize_tool_definition;
use crate::tool_run::handoff_wire::ToolRunLaunchEnvelopeWire;
use crate::tool_run::handoff_wire::ToolRunStopRequestWire;
use crate::tool_run::store::{begin, list_runs, request_stop, show_run};
use crate::tool_run::wire::{
    ToolRunBeginRequestWire, ToolRunEventWire, ToolRunExecutorWire,
    ToolRunListRequestWire, ToolRunShowRequestWire, ToolRunSourceWire,
    ToolRunStateWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use rusqlite::Connection;
use std::time::Duration;

const OLD_RUN_COLUMNS: &str =
    "run_id, state, source, executor, attempt, tool_name,
    definition_digest, extra_args_digest, display_argv_json,
    private_argv_json, project, agent, workspace, bead,
    owner_kind, owner_id, parent_run_id, created_ts, running_ts,
    settled_ts, duration_ms, duration_missing, exit_code, signal,
    interruption_reason, lost_reason, wrapper_pid, boot_id,
    process_start_identity, child_pid, child_pgid,
    child_process_start_identity, mutated_input,
    fingerprint_before_json, fingerprint_after_json,
    log_stdout_path, log_stderr_path, events_path, evidence_json,
    diagnostics_json";

const OLD_SCHEMA_SQL: &str = r#"
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    source TEXT NOT NULL,
    executor TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    tool_name TEXT,
    definition_digest TEXT NOT NULL,
    extra_args_digest TEXT NOT NULL,
    display_argv_json TEXT NOT NULL,
    private_argv_json TEXT,
    project TEXT,
    agent TEXT,
    workspace TEXT,
    bead TEXT,
    owner_kind TEXT,
    owner_id TEXT,
    parent_run_id TEXT,
    created_ts INTEGER NOT NULL,
    running_ts INTEGER,
    settled_ts INTEGER,
    duration_ms INTEGER,
    duration_missing TEXT,
    exit_code INTEGER,
    signal INTEGER,
    interruption_reason TEXT,
    lost_reason TEXT,
    wrapper_pid INTEGER,
    boot_id TEXT,
    process_start_identity TEXT,
    child_pid INTEGER,
    child_pgid INTEGER,
    child_process_start_identity TEXT,
    mutated_input INTEGER,
    fingerprint_before_json TEXT,
    fingerprint_after_json TEXT,
    log_stdout_path TEXT,
    log_stderr_path TEXT,
    events_path TEXT,
    evidence_json TEXT NOT NULL,
    diagnostics_json TEXT NOT NULL,
    FOREIGN KEY (parent_run_id) REFERENCES runs(run_id)
);
CREATE TABLE IF NOT EXISTS attempts (
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    state TEXT NOT NULL,
    started_ts INTEGER,
    settled_ts INTEGER,
    exit_code INTEGER,
    signal INTEGER,
    diagnostics_json TEXT NOT NULL,
    PRIMARY KEY (run_id, attempt),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_ts INTEGER NOT NULL,
    FOREIGN KEY (run_id, attempt) REFERENCES attempts(run_id, attempt)
);
CREATE TABLE IF NOT EXISTS stages (
    stage_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    description TEXT NOT NULL,
    started_ts INTEGER,
    finished_ts INTEGER,
    elapsed_ms INTEGER,
    exit_code INTEGER,
    output_bytes INTEGER,
    incomplete INTEGER NOT NULL DEFAULT 1,
    diagnostics_json TEXT NOT NULL,
    FOREIGN KEY (run_id, attempt) REFERENCES attempts(run_id, attempt)
);
CREATE TABLE IF NOT EXISTS samples (
    sample_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    observed_ts INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    FOREIGN KEY (run_id, attempt) REFERENCES attempts(run_id, attempt)
);
CREATE INDEX IF NOT EXISTS idx_tool_runs_created
    ON runs(created_ts DESC, run_id DESC);
CREATE INDEX IF NOT EXISTS idx_tool_runs_project_tool
    ON runs(project, tool_name, created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_tool_runs_state
    ON runs(state, created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_tool_events_run
    ON events(run_id, created_ts);
CREATE INDEX IF NOT EXISTS idx_tool_stages_run
    ON stages(run_id, started_ts);
CREATE INDEX IF NOT EXISTS idx_tool_samples_run
    ON samples(run_id, observed_ts);
"#;

#[test]
fn old_shape_store_is_readable_and_migrates_on_write() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("tools").join("runs.sqlite");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(OLD_SCHEMA_SQL).unwrap();
    conn.execute(
        "INSERT INTO meta(key, value) VALUES ('schema_version', '1')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO runs(
            run_id, state, source, executor, attempt, tool_name,
            definition_digest, extra_args_digest, display_argv_json,
            evidence_json, diagnostics_json, created_ts
         ) VALUES (
            'old-1', 'running', 'native', 'inline', 1, 'check',
            'd1', 'e1', '[\"just\", \"check\"]',
            '{\"complete\": false, \"missing\": []}', '[]', 10
         )",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO attempts(
            run_id, attempt, state, started_ts, settled_ts, exit_code,
            signal, diagnostics_json
         ) VALUES ('old-1', 1, 'running', 10, NULL, NULL, NULL, '[]')",
        [],
    )
    .unwrap();
    drop(conn);
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "old-1".into(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let run = shown.run.expect("old run");
    assert_eq!(run.state, ToolRunStateWire::Running);
    assert!(run.launch_mode.is_none());
    assert!(run.terminal_cause.is_none());
    assert!(run.settled_by.is_none());
    assert!(run.stop_request.is_none());
    assert!(run.launcher.is_none());
    assert!(run.logs.owner_log_path.is_none());
    let listed = list_runs(
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
    .unwrap();
    assert_eq!(listed.runs.len(), 1);
    // A write migrates the store by adding the columns.
    let normalized = normalize_tool_definition(definition()).unwrap();
    begin(
        &path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some("new-1".into()),
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
            now_ts: Some(11),
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let conn = Connection::open(&path).unwrap();
    let mut stmt = conn.prepare("PRAGMA table_info(runs)").unwrap();
    let names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for column in [
        "launch_mode",
        "launch_envelope_json",
        "launcher_json",
        "terminal_cause",
        "settled_by",
        "stop_request_json",
        "owner_log_path",
    ] {
        assert!(names.contains(&column.to_string()), "missing {column}");
    }
}

#[test]
fn new_shape_store_stays_loadable_by_old_query() {
    let (_temp, path) = store();
    let normalized = normalize_tool_definition(definition()).unwrap();
    let digest = normalized.digest.clone();
    begin(
        &path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some("compat-1".into()),
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
            now_ts: Some(10),
            commit_running: false,
            launch_mode: Some(
                crate::tool_run::handoff_wire::ToolRunLaunchModeWire::Handoff,
            ),
            launch: Some(ToolRunLaunchEnvelopeWire {
                argv: vec!["just".into(), "check".into()],
                cwd: None,
                tool_name: Some("check".into()),
                extra_args: Vec::new(),
                display_argv: vec!["just".into(), "check".into()],
                private_argv: None,
                definition: definition(),
                digest: Some(digest),
                adhoc: false,
            }),
            owner_log_path: Some("logs/proc-1.log".into()),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    request_stop(
        &path,
        ToolRunStopRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "compat-1".into(),
            requested_by: Some("agent-1".into()),
            reason: None,
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let conn = Connection::open(&path).unwrap();
    let sql =
        format!("SELECT {OLD_RUN_COLUMNS} FROM runs WHERE run_id = 'compat-1'");
    let mut stmt = conn.prepare(&sql).unwrap();
    let mut rows = stmt.query([]).unwrap();
    let row = rows.next().unwrap().expect("row");
    let state: String = row.get(1).unwrap();
    let source: String = row.get(2).unwrap();
    let executor: String = row.get(3).unwrap();
    assert!(ToolRunStateWire::from_db(&state).is_ok());
    assert!(ToolRunSourceWire::from_db(&source).is_ok());
    assert!(ToolRunExecutorWire::from_db(&executor).is_ok());
    let mut events = conn
        .prepare("SELECT payload_json FROM events WHERE run_id = 'compat-1'")
        .unwrap();
    let payloads = events
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(!payloads.is_empty());
    for payload in payloads {
        let event: ToolRunEventWire = serde_json::from_str(&payload).unwrap();
        assert_eq!(event.schema_version, TOOL_RUN_WIRE_SCHEMA_VERSION);
    }
}

const OLD_RETENTION_DELETE_SQL: &[&str] = &[
    "DELETE FROM samples WHERE sample_id IN (
                SELECT m.sample_id FROM samples m
                JOIN runs r ON r.run_id = m.run_id
                WHERE r.state NOT IN ('created', 'running')
                  AND m.observed_ts < ?1
            )",
    "DELETE FROM stages WHERE stage_id IN (
                SELECT s.stage_id FROM stages s
                JOIN runs r ON r.run_id = s.run_id
                WHERE r.state NOT IN ('created', 'running')
                  AND COALESCE(s.finished_ts, s.started_ts, 0) < ?1
            )",
    "DELETE FROM events WHERE event_id IN (
                SELECT e.event_id FROM events e
                JOIN runs r ON r.run_id = e.run_id
                WHERE r.state NOT IN ('created', 'running')
                  AND e.kind IN ('stage_started', 'stage_finished', 'sample')
                  AND e.created_ts < ?1
            )",
    "DELETE FROM events WHERE run_id IN (
                SELECT run_id FROM runs
                WHERE state NOT IN ('created', 'running')
                  AND settled_ts IS NOT NULL
                  AND settled_ts < ?1
            )",
    "DELETE FROM stages WHERE run_id IN (
                SELECT run_id FROM runs
                WHERE state NOT IN ('created', 'running')
                  AND settled_ts IS NOT NULL
                  AND settled_ts < ?1
            )",
    "DELETE FROM samples WHERE run_id IN (
                SELECT run_id FROM runs
                WHERE state NOT IN ('created', 'running')
                  AND settled_ts IS NOT NULL
                  AND settled_ts < ?1
            )",
    "DELETE FROM attempts WHERE run_id IN (
                SELECT run_id FROM runs
                WHERE state NOT IN ('created', 'running')
                  AND settled_ts IS NOT NULL
                  AND settled_ts < ?1
            )",
    "DELETE FROM runs
             WHERE state NOT IN ('created', 'running')
               AND settled_ts IS NOT NULL
               AND settled_ts < ?1",
];

#[test]
fn old_store_without_triage_tables_show_empty_and_record_creates() {
    use crate::tool_run::store::{triage_record, triage_show};
    use crate::tool_run::triage::{
        ToolRunTriageExtractRequestWire, ToolRunTriageExtractionStatusWire,
        ToolRunTriageRecordRequestWire, ToolRunTriageShowRequestWire,
        ToolRunTriageStageRecordWire,
    };
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("tools").join("runs.sqlite");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(OLD_SCHEMA_SQL).unwrap();
    conn.execute(
        "INSERT INTO meta(key, value) VALUES ('schema_version', '1')",
        [],
    )
    .unwrap();
    // Minimal old run row.
    conn.execute(
        "INSERT INTO runs(
            run_id, state, source, executor, attempt, tool_name,
            definition_digest, extra_args_digest, display_argv_json,
            evidence_json, diagnostics_json, created_ts
         ) VALUES (
            'old-triage-1', 'running', 'native', 'inline', 1, 'check',
            'd1', 'e1', '[\"just\", \"check\"]',
            '{\"complete\": false, \"missing\": []}', '[]', 10
         )",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO attempts(
            run_id, attempt, state, started_ts, settled_ts, exit_code,
            signal, diagnostics_json
         ) VALUES ('old-triage-1', 1, 'running', 10, NULL, NULL, NULL, '[]')",
        [],
    )
    .unwrap();
    drop(conn);
    let shown = triage_show(
        &path,
        ToolRunTriageShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "old-triage-1".into(),
            owner_kind: None,
            owner_id: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(shown.run_found);
    assert!(!shown.triaged);
    assert!(shown.items.is_empty());
    let conn = Connection::open(&path).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'tool_triage%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    drop(conn);
    // A record then creates the tables and writes.
    let recorded = triage_record(
        &path,
        ToolRunTriageRecordRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "old-triage-1".into(),
            stages: vec![ToolRunTriageStageRecordWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                stage_key: "lint (mypy)".into(),
                stage_id: None,
                extraction_status: ToolRunTriageExtractionStatusWire::Parsed,
                output_path: None,
                decision: None,
                items: Vec::new(),
            }],
            run_facts: None,
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(recorded.refused.is_none());
    let _ = ToolRunTriageExtractRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        stage_key: "x".into(),
        stage_id: None,
        output: None,
        truncated: false,
        project_root: None,
        workspace_roots: Vec::new(),
    };
}

#[test]
fn old_queries_load_store_with_triage_rows() {
    use crate::tool_run::store::{triage_record, triage_show};
    use crate::tool_run::triage::{
        ToolRunTriageExtractionStatusWire, ToolRunTriageRecordRequestWire,
        ToolRunTriageShowRequestWire, ToolRunTriageStageRecordWire,
    };
    let (_temp, path) = store();
    let normalized = normalize_tool_definition(definition()).unwrap();
    begin(
        &path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some("with-triage-1".into()),
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
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    triage_record(
        &path,
        ToolRunTriageRecordRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "with-triage-1".into(),
            stages: vec![ToolRunTriageStageRecordWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                stage_key: "lint (mypy)".into(),
                stage_id: None,
                extraction_status: ToolRunTriageExtractionStatusWire::Parsed,
                output_path: None,
                decision: None,
                items: Vec::new(),
            }],
            run_facts: None,
            now_ts: Some(11),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    // Pre-change OLD_RUN_COLUMNS query still loads.
    let conn = Connection::open(&path).unwrap();
    let sql = format!(
        "SELECT {OLD_RUN_COLUMNS} FROM runs WHERE run_id = 'with-triage-1'"
    );
    {
        let mut stmt = conn.prepare(&sql).unwrap();
        let mut rows = stmt.query([]).unwrap();
        assert!(rows.next().unwrap().is_some());
    }
    drop(conn);
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "with-triage-1".into(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(shown.run.is_some());
    let listed = list_runs(
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
    .unwrap();
    assert!(!listed.runs.is_empty());
    let triage_shown = triage_show(
        &path,
        ToolRunTriageShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "with-triage-1".into(),
            owner_kind: None,
            owner_id: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(triage_shown.triaged);
}

#[test]
fn old_retention_deletes_cascade_triage_rows() {
    use crate::tool_run::store::{finish, triage_record};
    use crate::tool_run::triage::{
        ToolRunTriageExtractionStatusWire, ToolRunTriageRecordRequestWire,
        ToolRunTriageStageRecordWire,
    };
    use crate::tool_run::wire::{ToolRunFinishRequestWire, ToolRunStateWire};
    let (_temp, path) = store();
    let normalized = normalize_tool_definition(definition()).unwrap();
    begin(
        &path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some("cascade-1".into()),
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
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "cascade-1".into(),
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
            now_ts: Some(11),
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    triage_record(
        &path,
        ToolRunTriageRecordRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "cascade-1".into(),
            stages: vec![ToolRunTriageStageRecordWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                stage_key: "lint (mypy)".into(),
                stage_id: None,
                extraction_status: ToolRunTriageExtractionStatusWire::Parsed,
                output_path: None,
                decision: None,
                items: Vec::new(),
            }],
            run_facts: None,
            now_ts: Some(12),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    // Run the pre-change retention DELETEs verbatim with FK enforcement.
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
    for sql in OLD_RETENTION_DELETE_SQL {
        conn.execute(sql, [11 + 200 * 86400]).unwrap();
    }
    let remaining: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM tool_triage_stages WHERE run_id = 'cascade-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(remaining, 0);
    let remaining: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM tool_triage_runs WHERE run_id = 'cascade-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(remaining, 0);
}

#[test]
fn retention_reports_and_deletes_triage_and_stage_output() {
    use crate::tool_run::store::{
        finish, retention_apply, retention_preview, triage_record,
    };
    use crate::tool_run::triage::{
        ToolRunTriageExtractionStatusWire, ToolRunTriageRecordRequestWire,
        ToolRunTriageStageRecordWire,
    };
    use crate::tool_run::wire::ToolRunRetentionPolicyWire;
    use crate::tool_run::wire::{
        ToolRunFinishRequestWire, ToolRunRetentionRequestWire,
        ToolRunStateWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
    };
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("tools").join("runs.sqlite");
    let logs = temp.path().join("logs").join("run1");
    std::fs::create_dir_all(logs.join("stage_output")).unwrap();
    let events_path = logs.join("events.jsonl").to_string_lossy().into_owned();
    std::fs::write(&events_path, "{}\n").unwrap();
    let stage_file = logs.join("stage_output").join("lint__mypy.log");
    std::fs::write(&stage_file, "output").unwrap();
    let normalized = normalize_tool_definition(definition()).unwrap();
    // Settled run with stage output.
    begin(
        &path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some("ret-1".into()),
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
            events_path: Some(events_path.clone()),
            log_stdout_path: None,
            log_stderr_path: None,
            now_ts: Some(1_000),
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "ret-1".into(),
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
            now_ts: Some(1_001),
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    triage_record(
        &path,
        ToolRunTriageRecordRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: "ret-1".into(),
            stages: vec![ToolRunTriageStageRecordWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                stage_key: "lint (mypy)".into(),
                stage_id: None,
                extraction_status: ToolRunTriageExtractionStatusWire::Parsed,
                output_path: None,
                decision: None,
                items: Vec::new(),
            }],
            run_facts: None,
            now_ts: Some(1_002),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    // Unsettled run with stage output must never be selected.
    let logs2 = temp.path().join("logs").join("run2");
    std::fs::create_dir_all(logs2.join("stage_output")).unwrap();
    let events2 = logs2.join("events.jsonl").to_string_lossy().into_owned();
    std::fs::write(&events2, "{}\n").unwrap();
    let stage2 = logs2.join("stage_output").join("x.log");
    std::fs::write(&stage2, "output").unwrap();
    begin(
        &path,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some("ret-2".into()),
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
            events_path: Some(events2.clone()),
            log_stdout_path: None,
            log_stderr_path: None,
            now_ts: Some(1_000),
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let now = 1_001 + 15 * 86400;
    let preview = retention_preview(
        &path,
        ToolRunRetentionRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            policy: ToolRunRetentionPolicyWire::default(),
            now_ts: Some(now),
            dry_run: true,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let stage_paths: Vec<String> = preview
        .file_candidates
        .iter()
        .filter(|candidate| candidate.kind == "stage_output")
        .filter_map(|candidate| candidate.path.clone())
        .collect();
    assert!(stage_paths
        .iter()
        .any(|path| path.ends_with("lint__mypy.log")));
    assert!(!stage_paths.iter().any(|path| path.ends_with("x.log")));
    // Detail cut deletes triage rows.
    let applied = retention_apply(
        &path,
        ToolRunRetentionRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            policy: ToolRunRetentionPolicyWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                summary_days: 180,
                detail_days: 14,
                log_days: 1,
                ..ToolRunRetentionPolicyWire::default()
            },
            now_ts: Some(now),
            dry_run: false,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(applied.detail_rows >= 2);
}
