use super::*;
use crate::agent_scan::index::{
    query_agent_artifact_index, rebuild_agent_artifact_index,
    AgentArtifactCandidateFieldWire, AgentArtifactCandidateFilterWire,
    AgentArtifactIndexFreshnessWire, AgentArtifactIndexQueryWire,
};
use crate::agent_scan::scanner::scan_agent_artifacts;
use crate::agent_scan::wire::{
    AgentArtifactRecordShapeWire, AgentArtifactScanOptionsWire,
};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

fn write_json(path: &Path, payload: Value) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, serde_json::to_string(&payload).unwrap()).unwrap();
}

fn artifact(root: &Path, ts: &str) -> PathBuf {
    artifact_for_workflow(root, "ace-run", ts)
}

fn artifact_for_workflow(root: &Path, workflow: &str, ts: &str) -> PathBuf {
    root.join("proj").join("artifacts").join(workflow).join(ts)
}

fn rebuild(tmp: &Path, projects: &Path) -> PathBuf {
    let index = tmp.join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    index
}

fn full_history_query() -> AgentArtifactIndexQueryWire {
    AgentArtifactIndexQueryWire {
        include_active: true,
        include_recent_completed: true,
        include_full_history: true,
        active_limit: None,
        recent_completed_limit: Some(50),
        include_hidden: false,
        freshness: AgentArtifactIndexFreshnessWire::Cached,
        only_monitors: false,
        record_shape: AgentArtifactRecordShapeWire::Full,
        window_limit: None,
        candidate_filter: None,
        agents_list_projection: false,
    }
}

fn json_contains_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(map) => {
            map.contains_key(key)
                || map.values().any(|child| json_contains_key(child, key))
        }
        Value::Array(items) => {
            items.iter().any(|child| json_contains_key(child, key))
        }
        _ => false,
    }
}

fn write_heavy_record(dir: &Path) {
    write_json(
        &dir.join("agent_meta.json"),
        json!({
            "name": "heavy",
            "cl_name": "cl_heavy",
            "linked_repos": [
                {"name": "core", "workspace_dir": "/tmp/core"}
            ],
            "output_variables": {"STOP": true, "note": "x".repeat(4000)},
            "agent_clan": "crew",
            "agent_clan_generation": "g1",
            "clan_tribe": "chop",
            "clan_summary": "Crew generation"
        }),
    );
    write_json(
        &dir.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1779999999.0,
            "name": "heavy",
            "cl_name": "cl_heavy",
            "traceback": "long traceback ".repeat(200),
            "markdown_pdf_paths": ["/tmp/a.pdf"],
            "image_paths": ["/tmp/a.png"],
            "video_paths": ["/tmp/a.mp4"],
            "step_output": {
                "_raw": "done raw ".repeat(500),
                "_data": {"body": "done data ".repeat(500)},
                "meta_patch": "keep",
                "diff_path": "/tmp/keep.diff"
            }
        }),
    );
    write_json(
        &dir.join("workflow_state.json"),
        json!({
            "workflow_name": "heavy",
            "status": "completed",
            "appears_as_agent": true,
            "current_step_index": 0,
            "error": "workflow error",
            "traceback": "workflow traceback",
            "steps": [
                {
                    "name": "build",
                    "status": "completed",
                    "output": {
                        "_raw": "workflow raw",
                        "_data": "workflow data",
                        "meta_workflow": "keep"
                    },
                    "error": null,
                    "traceback": "step traceback"
                }
            ]
        }),
    );
    write_json(
        &dir.join("prompt_step_001.json"),
        json!({
            "workflow_name": "heavy",
            "step_name": "build",
            "step_type": "exec",
            "status": "completed",
            "output": {
                "_raw": "prompt raw",
                "_data": "prompt data",
                "meta_prompt": "keep",
                "diff_path": "/tmp/step.diff"
            },
            "traceback": "prompt traceback"
        }),
    );
    fs::write(dir.join("raw_prompt.md"), "prompt body ".repeat(200)).unwrap();
}

#[test]
fn compact_index_query_matches_full_projection_and_order() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let older = artifact(&projects, "20260827090000");
    let newer = artifact(&projects, "20260827100000");
    write_heavy_record(&older);
    write_json(
        &newer.join("agent_meta.json"),
        json!({"name": "newer", "pid": 9}),
    );
    write_json(&newer.join("running.json"), json!({"pid": 9}));

    let index = rebuild(tmp.path(), &projects);
    let full = query_agent_artifact_index(
        &index,
        &projects,
        full_history_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let compact = query_agent_artifact_index_list(
        &index,
        &projects,
        full_history_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert_eq!(
        compact.schema_version,
        AGENT_ARTIFACT_LIST_WIRE_SCHEMA_VERSION
    );
    assert_eq!(compact.records.len(), full.records.len());
    assert_eq!(compact.clan_context, full.clan_context);
    assert_eq!(compact.index_completeness, full.index_completeness);
    assert_eq!(
        compact.stats.artifact_dirs_visited,
        full.stats.artifact_dirs_visited
    );
    let expected: Vec<_> = full
        .records
        .iter()
        .map(project_record_for_list_compact)
        .collect();
    assert_eq!(compact.records, expected);
    let timestamps: Vec<&str> = compact
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert_eq!(timestamps, vec!["20260827090000", "20260827100000"]);
}

#[test]
fn compact_serialized_payload_excludes_deferred_detail() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_heavy_record(&artifact(&projects, "20260827110000"));
    let index = rebuild(tmp.path(), &projects);
    let compact = query_agent_artifact_index_list(
        &index,
        &projects,
        full_history_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(compact.records.len(), 1);
    let value = serde_json::to_value(&compact.records[0]).unwrap();
    for forbidden in [
        "linked_repos",
        "traceback",
        "_raw",
        "_data",
        "raw_prompt_snippet",
        "used_macros",
        "used_xprompts",
        "markdown_pdf_paths",
        "image_paths",
        "video_paths",
        "output_variables",
        "agent_meta",
        "workflow_state",
    ] {
        assert!(
            !json_contains_key(&value, forbidden),
            "compact payload still contains {forbidden}: {value}"
        );
    }
    assert_eq!(value["record_shape"], json!("compact"));
    assert_eq!(value["meta"]["name"], json!("heavy"));
    assert!(value["meta"].get("linked_repos").is_none());
    assert_eq!(
        value["done"]["step_output"]["meta"]["meta_patch"],
        json!("keep")
    );
    assert_eq!(
        value["done"]["step_output"]["diff_path"],
        json!("/tmp/keep.diff")
    );
    assert!(value["prompt_steps"][0]["output"]["meta"]
        .get("meta_prompt")
        .is_none());
    assert!(value["workflow"]["steps"][0]["output"]["meta"]
        .get("meta_workflow")
        .is_none());
    let encoded = serde_json::to_string(&compact.records[0]).unwrap();
    assert!(!encoded.contains("done raw"));
    assert!(!encoded.contains("long traceback"));
}

#[test]
fn compact_query_preserves_window_limits_and_candidate_filters() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for (ts, machine) in [
        ("20260828130000", "athena"),
        ("20260828130100", "apollo"),
        ("20260828130200", "athena"),
    ] {
        let dir = artifact(&projects, ts);
        write_json(
            &dir.join("agent_meta.json"),
            json!({"name": ts, "source_machine": machine}),
        );
        write_json(
            &dir.join("done.json"),
            json!({"outcome": "completed", "name": ts}),
        );
    }
    let index = rebuild(tmp.path(), &projects);
    let windowed = query_agent_artifact_index_list(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            window_limit: Some(1),
            record_shape: AgentArtifactRecordShapeWire::List,
            ..AgentArtifactIndexQueryWire::default()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(windowed.records.len(), 1);
    assert_eq!(
        windowed.index_window.as_ref().unwrap().requested_limit,
        Some(1)
    );
    assert_eq!(windowed.records[0].timestamp, "20260828130200");

    let filtered = query_agent_artifact_index_list(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: false,
            include_full_history: true,
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            candidate_filter: Some(AgentArtifactCandidateFilterWire::Equals {
                field: AgentArtifactCandidateFieldWire::Machine,
                value: "athena".to_string(),
            }),
            ..full_history_query()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let timestamps: BTreeSet<&str> = filtered
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert_eq!(
        timestamps,
        BTreeSet::from(["20260828130000", "20260828130200"])
    );
}

#[test]
fn compact_query_preserves_hidden_rows_and_missing_markers() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let hidden = artifact(&projects, "20260829100000");
    write_json(
        &hidden.join("agent_meta.json"),
        json!({"name": "hidden", "hidden": true}),
    );
    write_json(
        &hidden.join("done.json"),
        json!({"outcome": "completed", "name": "hidden", "hidden": true}),
    );
    let waiting = artifact(&projects, "20260829100100");
    write_json(&waiting.join("agent_meta.json"), json!({"name": "waiting"}));
    write_json(&waiting.join("waiting.json"), json!({}));
    let markerless = artifact(&projects, "20260829100200");
    write_json(
        &markerless.join("agent_meta.json"),
        json!({"name": "markerless", "pid": 4}),
    );

    let index = rebuild(tmp.path(), &projects);
    let visible = query_agent_artifact_index_list(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_hidden: false,
            ..full_history_query()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let visible_ts: BTreeSet<&str> = visible
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert!(!visible_ts.contains("20260829100000"));
    assert!(visible_ts.contains("20260829100100"));
    assert!(visible_ts.contains("20260829100200"));

    let with_hidden = query_agent_artifact_index_list(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_hidden: true,
            ..full_history_query()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let hidden_row = with_hidden
        .records
        .iter()
        .find(|record| record.timestamp == "20260829100000")
        .unwrap();
    assert!(hidden_row.meta.as_ref().unwrap().hidden);
    assert!(hidden_row.done.as_ref().unwrap().hidden);

    let waiting_row = visible
        .records
        .iter()
        .find(|record| record.timestamp == "20260829100100")
        .unwrap();
    assert!(waiting_row.waiting.is_some());
    assert!(waiting_row.done.is_none());
    assert!(waiting_row.running.is_none());
    assert!(!waiting_row.has_done_marker);

    let markerless_row = visible
        .records
        .iter()
        .find(|record| record.timestamp == "20260829100200")
        .unwrap();
    assert!(markerless_row.done.is_none());
    assert!(markerless_row.running.is_none());
    assert!(markerless_row.waiting.is_none());
    assert!(markerless_row.workflow.is_none());
    assert!(markerless_row.pending_question.is_none());
}

#[test]
fn compact_query_preserves_clan_context_and_completeness() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let declarer = artifact(&projects, "20260701000000");
    write_json(
        &declarer.join("agent_meta.json"),
        json!({
            "name": "declarer",
            "agent_clan": "toobig-0",
            "agent_clan_generation": "generation-1",
            "clan_tribe": "chop",
            "clan_summary": "Chop generation"
        }),
    );
    write_json(
        &declarer.join("done.json"),
        json!({"outcome": "completed", "name": "declarer"}),
    );
    let joiner = artifact(&projects, "20260701000001");
    write_json(
        &joiner.join("agent_meta.json"),
        json!({
            "name": "joiner",
            "agent_clan": "toobig-0",
            "agent_clan_generation": "generation-1"
        }),
    );
    write_json(
        &joiner.join("waiting.json"),
        json!({"waiting_for": ["predecessor"]}),
    );

    let index = rebuild(tmp.path(), &projects);
    let query = AgentArtifactIndexQueryWire {
        freshness: AgentArtifactIndexFreshnessWire::Cached,
        include_full_history: true,
        agents_list_projection: true,
        ..full_history_query()
    };
    let full = query_agent_artifact_index(
        &index,
        &projects,
        query.clone(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let compact = query_agent_artifact_index_list(
        &index,
        &projects,
        query,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(compact.clan_context, full.clan_context);
    assert!(!compact.clan_context.is_empty());
    assert_eq!(compact.clan_context[0].clan_tribe.as_deref(), Some("chop"));
    assert_eq!(compact.index_completeness, full.index_completeness);
}

#[test]
fn compact_rows_cover_workflow_steps_and_session_turns() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let workflow = artifact_for_workflow(&projects, "run", "20260830100000");
    write_json(
        &workflow.join("agent_meta.json"),
        json!({
            "name": "wf",
            "plan_approved": true,
            "plan_action": "continue"
        }),
    );
    write_json(
        &workflow.join("workflow_state.json"),
        json!({
            "workflow_name": "build",
            "status": "running",
            "appears_as_agent": true,
            "current_step_index": 1,
            "steps": [
                {"name": "one", "status": "completed"},
                {
                    "name": "two",
                    "status": "running",
                    "output": {"meta_workspace": 3, "diff_path": "/tmp/w.diff"}
                }
            ]
        }),
    );
    write_json(
        &workflow.join("prompt_step_002.json"),
        json!({
            "workflow_name": "build",
            "step_name": "two",
            "step_type": "agent",
            "step_index": 1,
            "total_steps": 2,
            "status": "running",
            "model": "gpt-5"
        }),
    );

    let monitor = artifact(&projects, "20260830100100");
    write_json(
        &monitor.join("agent_meta.json"),
        json!({
            "name": "mon",
            "agent_session_role": "monitor",
            "agent_session_turn": {
                "kind": "monitor",
                "id": "m1",
                "state": "running",
                "label": "tail",
                "host_completion_status": "pending",
                "monitor": {"exit_code": null, "command": "sleep 1"}
            }
        }),
    );
    write_json(&monitor.join("running.json"), json!({"pid": 22}));

    let gate = artifact(&projects, "20260830100200");
    write_json(
        &gate.join("agent_meta.json"),
        json!({
            "name": "gate",
            "agent_session_role": "gate",
            "agent_session_turn": {
                "kind": "gate",
                "id": "g1",
                "state": "waiting",
                "gate": {
                    "kind": "approval",
                    "accent": "red",
                    "bundle_path": "/tmp/bundle",
                    "notification_id": "n1",
                    "creator_agent": "parent",
                    "decision_path": "/tmp/decision"
                }
            }
        }),
    );
    write_json(
        &gate.join("done.json"),
        json!({
            "outcome": "gated",
            "name": "gate",
            "status_label": "GATED",
            "agent_session_turn": {
                "kind": "gate",
                "id": "g1",
                "state": "waiting",
                "gate": {
                    "kind": "approval",
                    "bundle_path": "/tmp/bundle",
                    "notification_id": "n1"
                }
            }
        }),
    );

    let index = rebuild(tmp.path(), &projects);
    let compact = query_agent_artifact_index_list(
        &index,
        &projects,
        full_history_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let workflow_row = compact
        .records
        .iter()
        .find(|record| record.timestamp == "20260830100000")
        .unwrap();
    let workflow_state = workflow_row.workflow.as_ref().unwrap();
    assert_eq!(workflow_state.workflow_name, "build");
    assert_eq!(workflow_state.steps.len(), 2);
    assert_eq!(
        workflow_state.steps[1]
            .output
            .as_ref()
            .unwrap()
            .meta
            .get("meta_workspace"),
        Some(&json!(3))
    );
    assert_eq!(workflow_row.prompt_steps.len(), 1);
    assert_eq!(workflow_row.prompt_steps[0].step_type, "agent");
    assert_eq!(workflow_row.prompt_steps[0].model.as_deref(), Some("gpt-5"));

    let monitor_row = compact
        .records
        .iter()
        .find(|record| record.timestamp == "20260830100100")
        .unwrap();
    let turn = monitor_row
        .meta
        .as_ref()
        .unwrap()
        .agent_session_turn
        .as_ref()
        .unwrap();
    assert_eq!(turn.kind, "monitor");
    assert_eq!(turn.id.as_deref(), Some("m1"));
    assert!(turn.monitor.is_some());
    let encoded = serde_json::to_value(turn).unwrap();
    assert!(encoded.get("monitor").unwrap().get("command").is_none());

    let gate_row = compact
        .records
        .iter()
        .find(|record| record.timestamp == "20260830100200")
        .unwrap();
    let gate_turn = gate_row
        .done
        .as_ref()
        .unwrap()
        .agent_session_turn
        .as_ref()
        .unwrap();
    assert_eq!(gate_turn.kind, "gate");
    let gate_facts = gate_turn.gate.as_ref().unwrap();
    assert_eq!(gate_facts.kind.as_deref(), Some("approval"));
    assert_eq!(gate_facts.bundle_path.as_deref(), Some("/tmp/bundle"));
    let encoded_gate = serde_json::to_value(gate_turn).unwrap();
    assert!(encoded_gate
        .get("gate")
        .unwrap()
        .get("creator_agent")
        .is_none());
    assert!(encoded_gate
        .get("gate")
        .unwrap()
        .get("decision_path")
        .is_none());
}

#[test]
fn compact_source_scans_project_before_returning() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20260831100000");
    write_heavy_record(&dir);

    let scan = scan_agent_artifacts_list(
        &projects,
        AgentArtifactScanOptionsWire::default(),
    );
    assert_eq!(scan.schema_version, AGENT_ARTIFACT_LIST_WIRE_SCHEMA_VERSION);
    assert_eq!(scan.records.len(), 1);
    assert_eq!(
        scan.records[0].record_shape,
        AgentArtifactListShapeWire::Compact
    );
    assert!(scan.records[0].meta.as_ref().unwrap().name.is_some());
    let encoded = serde_json::to_value(&scan.records[0]).unwrap();
    assert!(!json_contains_key(&encoded, "linked_repos"));
    assert!(!json_contains_key(&encoded, "_raw"));

    let dirs = scan_agent_artifact_dirs_list(
        &projects,
        std::slice::from_ref(&dir),
        AgentArtifactScanOptionsWire::default(),
    );
    assert_eq!(dirs.records, scan.records);

    let one = scan_agent_artifact_dir_list(
        &projects,
        &dir,
        &AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(one, scan.records[0]);

    let full_scan = scan_agent_artifacts(
        &projects,
        AgentArtifactScanOptionsWire::default(),
    );
    assert_eq!(
        scan.records[0],
        project_record_for_list_compact(&full_scan.records[0])
    );
}

#[test]
fn compact_projection_preserves_monitor_identity_and_bounds_output_facts() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");

    let nested = artifact(&projects, "20260831110000");
    write_json(
        &nested.join("agent_meta.json"),
        json!({
            "name": "nested-monitor",
            "agent_session_role": "monitor",
            "process_identity": "boot:1",
            "agent_session_turn": {
                "kind": "monitor",
                "id": "nested-id",
                "monitor": {"supervisor_identity": "boot:9"}
            }
        }),
    );
    write_json(
        &nested.join("done.json"),
        json!({
            "outcome": "completed",
            "step_output": {
                "meta_workspace": 3,
                "meta_project": "project-name",
                "meta_patch": "patch-name",
                "meta_changespec": "legacy-change",
                "meta_new_cl": "legacy-cl",
                "meta_new_pr": "legacy-pr",
                "meta_commits": [{"diff": "y".repeat(100_000)}],
                "meta_details": {"_raw": "x".repeat(100_000)},
                "meta_unknown": {"nested": [1, 2, 3]}
            }
        }),
    );

    let flat = artifact(&projects, "20260831110001");
    write_json(
        &flat.join("agent_meta.json"),
        json!({
            "name": "flat-monitor",
            "agent_session_role": "monitor",
            "process_identity": "boot:2",
            "monitor_id": "flat-id",
            "monitor_state": "running",
            "monitor_supervisor_identity": "boot:10"
        }),
    );

    let missing = artifact(&projects, "20260831110002");
    write_json(
        &missing.join("agent_meta.json"),
        json!({
            "name": "missing-monitor",
            "agent_session_role": "monitor",
            "process_identity": "boot:3",
            "agent_session_turn": {
                "kind": "monitor",
                "id": "missing-id",
                "monitor": {}
            }
        }),
    );

    let empty = artifact(&projects, "20260831110003");
    write_json(
        &empty.join("agent_meta.json"),
        json!({
            "name": "empty-monitor",
            "agent_session_role": "monitor",
            "process_identity": "boot:4",
            "agent_session_turn": {
                "kind": "monitor",
                "id": "empty-id",
                "monitor": {"supervisor_identity": ""}
            }
        }),
    );

    let ordinary = artifact(&projects, "20260831110004");
    write_json(
        &ordinary.join("agent_meta.json"),
        json!({
            "name": "ordinary",
            "agent_session_role": "worker",
            "process_identity": "boot:5",
            "agent_session_turn": {
                "kind": "monitor",
                "id": "not-a-monitor-member",
                "monitor": {"supervisor_identity": "boot:11"}
            }
        }),
    );

    let snapshot = scan_agent_artifacts_list(
        &projects,
        AgentArtifactScanOptionsWire::default(),
    );
    let by_timestamp = |timestamp: &str| {
        snapshot
            .records
            .iter()
            .find(|record| record.timestamp == timestamp)
            .unwrap()
    };

    assert_eq!(
        by_timestamp("20260831110000")
            .meta
            .as_ref()
            .unwrap()
            .process_identity
            .as_deref(),
        Some("boot:9")
    );
    assert_eq!(
        by_timestamp("20260831110001")
            .meta
            .as_ref()
            .unwrap()
            .process_identity
            .as_deref(),
        Some("boot:10")
    );
    assert_eq!(
        by_timestamp("20260831110002")
            .meta
            .as_ref()
            .unwrap()
            .process_identity
            .as_deref(),
        Some("boot:3")
    );
    assert_eq!(
        by_timestamp("20260831110003")
            .meta
            .as_ref()
            .unwrap()
            .process_identity
            .as_deref(),
        Some("")
    );
    assert_eq!(
        by_timestamp("20260831110004")
            .meta
            .as_ref()
            .unwrap()
            .process_identity
            .as_deref(),
        Some("boot:5")
    );

    let output = by_timestamp("20260831110000")
        .done
        .as_ref()
        .unwrap()
        .step_output
        .as_ref()
        .unwrap();
    assert_eq!(output.meta.get("meta_workspace"), Some(&json!(3)));
    for key in [
        "meta_project",
        "meta_patch",
        "meta_changespec",
        "meta_new_cl",
        "meta_new_pr",
    ] {
        assert!(output.meta.contains_key(key), "missing {key}");
    }
    assert!(!output.meta.contains_key("meta_commits"));
    assert!(!output.meta.contains_key("meta_details"));
    assert!(!output.meta.contains_key("meta_unknown"));
    assert!(serde_json::to_vec(output).unwrap().len() < 4096);
}

#[test]
fn compact_query_ignores_caller_record_shape() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_json(
        &artifact(&projects, "20260901000000").join("agent_meta.json"),
        json!({"name": "shape"}),
    );
    let index = rebuild(tmp.path(), &projects);
    let compact = query_agent_artifact_index_list(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            record_shape: AgentArtifactRecordShapeWire::List,
            ..full_history_query()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(
        compact.records[0].record_shape,
        AgentArtifactListShapeWire::Compact
    );
}
