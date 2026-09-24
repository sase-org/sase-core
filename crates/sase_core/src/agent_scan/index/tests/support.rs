use super::super::dismissal::{
    record_is_dismissed, select_dismissal_reconcile_candidates,
};
use super::super::lineage::agent_session_root_dismissed_for_candidate;
use super::super::record_summary::RecordSummary;
use super::super::selection::{
    dismissed_identity_for_record,
    record_is_definitively_dead_for_dismissal_backfill,
};
use super::super::storage::open_index;
use super::super::*;
use crate::agent_scan::wire::{
    decode_agent_artifact_record_json, AgentArtifactRecordShapeWire,
    AgentArtifactRecordWire, AgentArtifactScanOptionsWire, AgentMetaWire,
    DoneMarkerWire,
};
use rusqlite::Connection;
use serde_json::json;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

pub(super) fn write_json(path: &Path, payload: serde_json::Value) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, serde_json::to_string(&payload).unwrap()).unwrap();
}

pub(super) fn artifact(root: &Path, ts: &str) -> PathBuf {
    artifact_for_project(root, "proj", ts)
}

pub(super) fn artifact_for_project(
    root: &Path,
    project: &str,
    ts: &str,
) -> PathBuf {
    root.join(project)
        .join("artifacts")
        .join("ace-run")
        .join(ts)
}

pub(super) fn windowed_index_query(limit: u32) -> AgentArtifactIndexQueryWire {
    AgentArtifactIndexQueryWire {
        freshness: AgentArtifactIndexFreshnessWire::Cached,
        record_shape: AgentArtifactRecordShapeWire::List,
        window_limit: Some(limit),
        ..AgentArtifactIndexQueryWire::default()
    }
}

pub(super) fn projection_windowed_query(
    limit: u32,
) -> AgentArtifactIndexQueryWire {
    AgentArtifactIndexQueryWire {
        agents_list_projection: true,
        ..windowed_index_query(limit)
    }
}

pub(super) fn projection_full_history_query() -> AgentArtifactIndexQueryWire {
    AgentArtifactIndexQueryWire {
        include_active: false,
        include_recent_completed: false,
        include_full_history: true,
        freshness: AgentArtifactIndexFreshnessWire::Cached,
        record_shape: AgentArtifactRecordShapeWire::List,
        agents_list_projection: true,
        ..AgentArtifactIndexQueryWire::default()
    }
}

pub(super) fn rebuild_index(tmp: &Path, projects: &Path) -> PathBuf {
    let index = tmp.join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    index
}

pub(super) fn machine_index_query(
    value: &str,
    negated: bool,
) -> AgentArtifactIndexQueryWire {
    let equals = AgentArtifactCandidateFilterWire::Equals {
        field: AgentArtifactCandidateFieldWire::Machine,
        value: value.to_string(),
    };
    AgentArtifactIndexQueryWire {
        include_active: false,
        include_recent_completed: false,
        include_full_history: true,
        freshness: AgentArtifactIndexFreshnessWire::Cached,
        candidate_filter: Some(if negated {
            AgentArtifactCandidateFilterWire::Not {
                filter: Box::new(equals),
            }
        } else {
            equals
        }),
        ..AgentArtifactIndexQueryWire::default()
    }
}

pub(super) fn query_timestamps(
    index: &Path,
    projects: &Path,
    query: AgentArtifactIndexQueryWire,
) -> BTreeSet<String> {
    query_agent_artifact_index(
        index,
        projects,
        query,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap()
    .records
    .into_iter()
    .map(|record| record.timestamp)
    .collect()
}

pub(super) fn write_text(path: &Path, body: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, body).unwrap();
}

pub(super) fn count_sql(index: &Path, sql: &str) -> i64 {
    Connection::open(index)
        .unwrap()
        .query_row(sql, [], |row| row.get(0))
        .unwrap()
}

pub(super) fn indexed_finished_at(
    index: &Path,
    artifact_dir: &Path,
) -> Option<f64> {
    Connection::open(index)
        .unwrap()
        .query_row(
            "SELECT finished_at FROM agent_artifacts WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| row.get(0),
        )
        .unwrap()
}

pub(super) fn alias_query(aliases: &[&str]) -> AgentAliasHistoryQueryWire {
    AgentAliasHistoryQueryWire {
        aliases: aliases.iter().map(|alias| alias.to_string()).collect(),
        limit_per_alias: 10,
        include_hidden: false,
        projects: Vec::new(),
        prompt_snippet_bytes: 240,
        freshness: AgentArtifactIndexFreshnessWire::Cached,
    }
}

pub(super) fn timestamps_from_artifact_dirs(paths: &[String]) -> Vec<&str> {
    paths
        .iter()
        .map(|path| Path::new(path).file_name().unwrap().to_str().unwrap())
        .collect()
}

pub(super) fn reconcile_n_plus_one(
    index_path: &Path,
    dry_run: bool,
) -> AgentArtifactIndexDismissalReconcileWire {
    let conn = open_index(index_path).unwrap();
    let candidates = select_dismissal_reconcile_candidates(&conn).unwrap();
    let mut report = AgentArtifactIndexDismissalReconcileWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        dry_run,
        candidate_rows: candidates.len() as u64,
        ..AgentArtifactIndexDismissalReconcileWire::default()
    };
    let mut additions = BTreeSet::new();
    for candidate in candidates {
        let Ok(record) =
            decode_agent_artifact_record_json(&candidate.record_json)
        else {
            report.rows_skipped_decode_errors += 1;
            continue;
        };
        if !record_is_definitively_dead_for_dismissal_backfill(&record) {
            report.rows_skipped_live_or_unknown += 1;
            continue;
        }
        let summary = RecordSummary::from_record(&record);
        if record_is_dismissed(&conn, &record, &summary).unwrap() {
            report.rows_already_dismissed += 1;
            continue;
        }
        if !agent_session_root_dismissed_for_candidate(&conn, &candidate.into())
            .unwrap()
        {
            report.rows_skipped_no_dismissed_root += 1;
            continue;
        }
        additions.insert(dismissed_identity_for_record(&record, &summary));
    }
    report.rows_backfilled = additions.len() as u64;
    report
}

pub(super) fn fixture_dead_agent_session_record(
    timestamp: &str,
    cl_name: &str,
    parent_timestamp: Option<&str>,
    agent_session: Option<&str>,
) -> AgentArtifactRecordWire {
    AgentArtifactRecordWire {
        project_name: "proj".to_string(),
        project_dir: "/proj".to_string(),
        project_file: "/proj/sase.sase".to_string(),
        workflow_dir_name: "ace-run".to_string(),
        artifact_dir: format!("/proj/artifacts/ace-run/{timestamp}"),
        timestamp: timestamp.to_string(),
        agent_meta: Some(AgentMetaWire {
            name: Some(cl_name.to_string()),
            cl_name: Some(cl_name.to_string()),
            agent_session: agent_session.map(str::to_string),
            parent_timestamp: parent_timestamp.map(str::to_string),
            ..AgentMetaWire::default()
        }),
        done: Some(DoneMarkerWire {
            outcome: Some("completed".to_string()),
            cl_name: Some(cl_name.to_string()),
            ..DoneMarkerWire::default()
        }),
        running: None,
        waiting: None,
        pending_question: None,
        workflow_state: None,
        plan_path: None,
        prompt_steps: Vec::new(),
        raw_prompt_snippet: None,
        used_xprompts: Vec::new(),
        has_done_marker: true,
        record_shape: AgentArtifactRecordShapeWire::Full,
    }
}

pub(super) fn default_query() -> AgentArtifactIndexQueryWire {
    AgentArtifactIndexQueryWire {
        include_active: true,
        include_recent_completed: true,
        include_full_history: false,
        active_limit: None,
        recent_completed_limit: Some(200),
        include_hidden: false,
        freshness: AgentArtifactIndexFreshnessWire::Revalidate,
        only_monitors: false,
        record_shape: AgentArtifactRecordShapeWire::Full,
        window_limit: None,
        candidate_filter: None,
        agents_list_projection: false,
    }
}

pub(super) fn full_history_revalidate_query() -> AgentArtifactIndexQueryWire {
    AgentArtifactIndexQueryWire {
        include_active: false,
        include_recent_completed: false,
        include_full_history: true,
        active_limit: None,
        recent_completed_limit: None,
        include_hidden: false,
        freshness: AgentArtifactIndexFreshnessWire::Revalidate,
        only_monitors: false,
        record_shape: AgentArtifactRecordShapeWire::Full,
        window_limit: None,
        candidate_filter: None,
        agents_list_projection: false,
    }
}

pub(super) fn full_history_cached_query() -> AgentArtifactIndexQueryWire {
    AgentArtifactIndexQueryWire {
        freshness: AgentArtifactIndexFreshnessWire::Cached,
        ..full_history_revalidate_query()
    }
}

pub(super) fn write_completed_artifact(dir: &Path, name: &str) {
    write_json(
        &dir.join("agent_meta.json"),
        json!({"name": name, "source_machine": "athena"}),
    );
    write_json(
        &dir.join("done.json"),
        json!({
            "outcome": "completed",
            "name": name,
            "source_machine": "athena"
        }),
    );
}

pub(super) fn write_gate_shell_artifact(
    projects: &Path,
    project: &str,
    ts: &str,
    gate_id: &str,
) -> PathBuf {
    let dir = artifact_for_project(projects, project, ts);
    write_json(
        &dir.join("agent_meta.json"),
        json!({
            "name": format!("{project}--gate"),
            "agent_family": "approvals",
            "agent_family_role": "gate",
            "gate_id": gate_id,
            "gate_kind": "approval",
            "gate_state": "pending",
            "gate_start_status": "WAITING",
            "gate_stop_status": "ANSWERED"
        }),
    );
    dir
}
