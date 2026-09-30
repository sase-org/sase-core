use super::super::catalog::normalize_tool_definition;
use super::super::demand_wire::{
    ToolRunDemandContextWire, ToolRunRecordDemandRequestWire,
    ToolRunResourceUsageWire, ToolRunWorkerGrantWire, DEMAND_MAX_DIAGNOSTICS,
    DEMAND_MAX_WORKER_GRANTS,
};
use super::super::wire::{
    ToolArgsPolicyWire, ToolDefinitionWire, ToolFingerprintSpecWire,
    ToolRunBeginRequestWire, ToolRunFinishRequestWire, ToolRunShowRequestWire,
    ToolRunStateWire, ToolStagesWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::{finish, record_demand, show_run};
use crate::tool_run::begin;
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::tempdir;

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
        receipt: None,
        duration_class: None,
        diagnostics: Vec::new(),
    }
}

fn store() -> (tempfile::TempDir, PathBuf) {
    let temp = tempdir().unwrap();
    let path = temp.path().join("tools").join("runs.sqlite");
    (temp, path)
}

fn begin_named(path: &Path, now: i64) -> String {
    let normalized = normalize_tool_definition(definition()).unwrap();
    begin(
        path,
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
            events_path: None,
            log_stdout_path: None,
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

fn context(provider: &str) -> ToolRunDemandContextWire {
    ToolRunDemandContextWire {
        provider: Some(provider.into()),
        sync_ceiling_seconds: Some(14400),
        sync_soft_ceiling_seconds: None,
    }
}

fn usage() -> ToolRunResourceUsageWire {
    ToolRunResourceUsageWire {
        cpu_user_ms: Some(200),
        cpu_system_ms: Some(50),
        max_process_rss_kib: Some(1_100_000),
        peak_tree_rss_kib: Some(9_800_000),
        tree_rss_samples: 54,
        availability: Vec::new(),
    }
}

fn grant(id: &str) -> ToolRunWorkerGrantWire {
    ToolRunWorkerGrantWire {
        grant_id: id.into(),
        source: "pytest".into(),
        observed_ts_ms: 1_759_240_000_000,
        lane: Some("fast".into()),
        path: "lease".into(),
        requested_floor: 4,
        requested_ceiling: 14,
        granted: 12,
        budget: Some(24),
        wait_ms: 182_000,
        selected_files: None,
        escalated_from: Some("scoped".into()),
    }
}

fn record(
    path: &Path,
    request: ToolRunRecordDemandRequestWire,
) -> crate::tool_run::demand_wire::ToolRunRecordDemandResultWire {
    record_demand(path, request, Duration::from_secs(1)).unwrap()
}

fn demand_request(run_id: &str) -> ToolRunRecordDemandRequestWire {
    ToolRunRecordDemandRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.to_string(),
        context: None,
        usage: None,
        worker_grants: Vec::new(),
        diagnostics: Vec::new(),
    }
}

#[test]
fn merge_combines_context_usage_and_grants_across_writes() {
    let (_temp, path) = store();
    let run_id = begin_named(&path, 10);
    let first = record(
        &path,
        ToolRunRecordDemandRequestWire {
            context: Some(context("Muse")),
            ..demand_request(&run_id)
        },
    );
    assert!(!first.replayed);
    assert_eq!(
        first.demand.context.as_ref().unwrap().provider.as_deref(),
        Some("Muse")
    );
    assert!(first.demand.usage.is_none());

    let second = record(
        &path,
        ToolRunRecordDemandRequestWire {
            usage: Some(usage()),
            ..demand_request(&run_id)
        },
    );
    assert!(!second.replayed);
    assert_eq!(
        second.demand.context.as_ref().unwrap().provider.as_deref(),
        Some("Muse")
    );
    assert_eq!(second.demand.usage.as_ref().unwrap().cpu_user_ms, Some(200));

    let third = record(
        &path,
        ToolRunRecordDemandRequestWire {
            worker_grants: vec![grant("grant-1")],
            ..demand_request(&run_id)
        },
    );
    assert!(!third.replayed);
    assert_eq!(third.demand.worker_grants.len(), 1);
    assert!(third.demand.context.is_some());
    assert!(third.demand.usage.is_some());

    // Rewriting the same context changes nothing.
    let replayed = record(
        &path,
        ToolRunRecordDemandRequestWire {
            context: Some(context("Muse")),
            ..demand_request(&run_id)
        },
    );
    assert!(replayed.replayed);
}

#[test]
fn grants_dedup_cap_and_invalid_drops() {
    let (_temp, path) = store();
    let run_id = begin_named(&path, 10);
    // A duplicate grant_id in one request lands once.
    let first = record(
        &path,
        ToolRunRecordDemandRequestWire {
            worker_grants: vec![grant("dup"), grant("dup")],
            ..demand_request(&run_id)
        },
    );
    assert_eq!(first.demand.worker_grants.len(), 1);

    // Invalid grants are dropped with a diagnostic; valid ones still land.
    let mut bad_floor = grant("bad-floor");
    bad_floor.requested_floor = 9;
    bad_floor.requested_ceiling = 4;
    let mut bad_id = grant("");
    bad_id.grant_id = String::new();
    let mut bad_lane = grant("bad-lane");
    bad_lane.lane = Some("x".repeat(65));
    let second = record(
        &path,
        ToolRunRecordDemandRequestWire {
            worker_grants: vec![grant("good"), bad_floor, bad_id, bad_lane],
            ..demand_request(&run_id)
        },
    );
    assert!(second
        .demand
        .worker_grants
        .iter()
        .any(|stored| stored.grant_id == "good"));
    assert_eq!(second.demand.worker_grants.len(), 2);
    assert_eq!(second.diagnostics.len(), 3);
    assert!(second
        .diagnostics
        .iter()
        .all(|line| line.contains("dropped invalid worker grant")));

    // Filling to the cap drops the overflow with one diagnostic.
    let mut overflow = Vec::new();
    for index in 0..(DEMAND_MAX_WORKER_GRANTS + 4) {
        overflow.push(grant(&format!("cap-{index}")));
    }
    let third = record(
        &path,
        ToolRunRecordDemandRequestWire {
            worker_grants: overflow,
            ..demand_request(&run_id)
        },
    );
    assert_eq!(third.demand.worker_grants.len(), DEMAND_MAX_WORKER_GRANTS);
    assert!(third
        .diagnostics
        .iter()
        .any(|line| line.contains("truncated to 64")));
}

#[test]
fn diagnostics_append_unique_and_cap() {
    let (_temp, path) = store();
    let run_id = begin_named(&path, 10);
    let first = record(
        &path,
        ToolRunRecordDemandRequestWire {
            diagnostics: vec!["note".into(), "note".into()],
            ..demand_request(&run_id)
        },
    );
    assert_eq!(first.demand.diagnostics, vec!["note".to_string()]);

    let mut many = Vec::new();
    for index in 0..(DEMAND_MAX_DIAGNOSTICS + 4) {
        many.push(format!("line-{index}"));
    }
    let second = record(
        &path,
        ToolRunRecordDemandRequestWire {
            diagnostics: many,
            ..demand_request(&run_id)
        },
    );
    assert_eq!(second.demand.diagnostics.len(), DEMAND_MAX_DIAGNOSTICS);
    assert!(second
        .demand
        .diagnostics
        .last()
        .unwrap()
        .contains("truncated to 16"));
}

#[test]
fn settled_runs_accept_demand_and_unknown_runs_are_not_found() {
    let (_temp, path) = store();
    let run_id = begin_named(&path, 10);
    finish(
        &path,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.clone(),
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
    let settled = record(
        &path,
        ToolRunRecordDemandRequestWire {
            usage: Some(usage()),
            ..demand_request(&run_id)
        },
    );
    assert!(!settled.replayed);
    assert!(settled.demand.usage.is_some());

    let missing = record_demand(
        &path,
        demand_request("does-not-exist"),
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(matches!(missing, ToolRunError::NotFound { .. }));
}

#[test]
fn legacy_store_gains_the_column_on_write_and_reads_without_it() {
    let (_temp, path) = store();
    let run_id = begin_named(&path, 10);
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute("ALTER TABLE runs DROP COLUMN demand_json", [])
            .unwrap();
    }
    // A read-only open of the unmigrated store still loads the run.
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.clone(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(shown.run.expect("run").demand.is_none());
    // The write migrates the column and the demand lands.
    let recorded = record(
        &path,
        ToolRunRecordDemandRequestWire {
            context: Some(context("Muse")),
            ..demand_request(&run_id)
        },
    );
    assert!(!recorded.replayed);
    assert_eq!(
        recorded
            .demand
            .context
            .as_ref()
            .unwrap()
            .provider
            .as_deref(),
        Some("Muse")
    );
}

#[test]
fn malformed_stored_demand_reads_as_absent_with_a_diagnostic() {
    let (_temp, path) = store();
    let run_id = begin_named(&path, 10);
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "UPDATE runs SET demand_json = '{\"context\": 42}' WHERE run_id = ?1",
            [&run_id],
        )
        .unwrap();
    }
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.clone(),
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let run = shown.run.expect("run");
    assert!(run.demand.is_none());
    assert!(run
        .diagnostics
        .iter()
        .any(|line| line.contains("stored demand was unreadable")));
    // A later write starts from empty and keeps its own facts.
    let recorded = record(
        &path,
        ToolRunRecordDemandRequestWire {
            usage: Some(usage()),
            ..demand_request(&run_id)
        },
    );
    assert!(recorded.demand.usage.is_some());
}

#[test]
fn run_without_demand_serializes_without_the_field() {
    let (_temp, path) = store();
    let run_id = begin_named(&path, 10);
    let shown = show_run(
        &path,
        ToolRunShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id,
        },
        Duration::from_secs(1),
    )
    .unwrap();
    let run = shown.run.expect("run");
    let encoded = serde_json::to_value(&run).unwrap();
    assert!(encoded.get("demand").is_none());
}
