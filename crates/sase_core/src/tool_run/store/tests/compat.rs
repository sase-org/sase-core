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
