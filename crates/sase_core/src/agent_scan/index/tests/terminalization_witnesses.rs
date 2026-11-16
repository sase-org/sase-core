use super::super::terminalization::{
    MapClaimProbe, MapProcessProbe, ProcessIdentityObservation,
};
use super::super::*;
use super::support::{artifact, write_json};
use super::terminalization::{
    active_query, claim_witnesses, dead_process_witnesses,
    hidden_completed_query, matching_claim, process_witnesses,
    terminalize_with, terminalize_with_dead_processes, write_stale_meta,
};
use crate::agent_launch::WorkspaceClaimWire;
use crate::agent_scan::wire::AgentArtifactScanOptionsWire;
use serde_json::json;
use std::fs;
use tempfile::tempdir;

fn rebuild(index: &std::path::Path, projects: &std::path::Path) {
    rebuild_agent_artifact_index(
        index,
        projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
}

fn query_active(
    index: &std::path::Path,
    projects: &std::path::Path,
) -> Vec<String> {
    query_agent_artifact_index(
        index,
        projects,
        active_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap()
    .records
    .into_iter()
    .map(|record| record.timestamp)
    .collect()
}

fn query_hidden(
    index: &std::path::Path,
    projects: &std::path::Path,
) -> Vec<String> {
    query_agent_artifact_index(
        index,
        projects,
        hidden_completed_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap()
    .records
    .into_iter()
    .map(|record| {
        record
            .done
            .as_ref()
            .and_then(|done| done.outcome.clone())
            .unwrap_or_default()
    })
    .collect()
}

#[test]
fn terminalize_repairs_source_done_without_abandoning() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521161000");
    write_stale_meta(&artifact_dir, "later-done", 4242);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);
    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "completed",
            "name": "later-done",
            "hidden": false
        }),
    );

    let update = terminalize_with_dead_processes(&index, &projects, 0, None);
    assert_eq!(update.terminalization_source_done_repairs, 1);
    assert_eq!(update.terminalization_synthesized_rows, 0);
    assert_eq!(update.rows_indexed, 1);

    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: true,
            include_full_history: false,
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            recent_completed_limit: Some(10),
            ..AgentArtifactIndexQueryWire::default()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(
        snapshot.records[0]
            .done
            .as_ref()
            .and_then(|done| done.outcome.as_deref()),
        Some("completed")
    );
    assert!(!snapshot.records[0]
        .done
        .as_ref()
        .is_some_and(|done| done.hidden));
}

#[test]
fn terminalize_matching_live_without_running_marker_stays_active() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521161100");
    write_stale_meta(&artifact_dir, "live-workspace", 4242);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let update = terminalize_with(
        &index,
        &projects,
        0,
        None,
        &process_witnesses(MapProcessProbe::with(
            4242,
            ProcessIdentityObservation::MatchingLive,
        )),
    );
    assert_eq!(update.terminalization_synthesized_rows, 0);
    assert_eq!(update.terminalization_skipped_live, 1);
    assert_eq!(query_active(&index, &projects), ["20260521161100"]);
}

#[test]
fn terminalize_abandons_leftover_running_marker_when_process_is_dead() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521161200");
    write_stale_meta(&artifact_dir, "leftover-running", 4242);
    write_json(
        &artifact_dir.join("running.json"),
        json!({"pid": 4242, "process_identity": "boot-a:1"}),
    );
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let update = terminalize_with_dead_processes(&index, &projects, 0, None);
    assert_eq!(update.terminalization_synthesized_rows, 1);
    assert!(query_active(&index, &projects).is_empty());
    assert_eq!(query_hidden(&index, &projects), ["abandoned"]);
}

#[test]
fn terminalize_reused_pid_and_previous_boot_are_death_evidence() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let reused = artifact(&projects, "20260521161300");
    let rebooted = artifact(&projects, "20260521161301");
    write_stale_meta(&reused, "reused", 11);
    write_stale_meta(&rebooted, "rebooted", 12);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let mut process = MapProcessProbe::all(ProcessIdentityObservation::Unknown);
    process.insert(11, ProcessIdentityObservation::ReusedPid);
    process.insert(12, ProcessIdentityObservation::PreviousBoot);
    let update = terminalize_with(
        &index,
        &projects,
        0,
        None,
        &process_witnesses(process),
    );
    assert_eq!(update.terminalization_synthesized_rows, 2);
    assert!(query_active(&index, &projects).is_empty());
}

#[test]
fn terminalize_reused_pid_abandons_and_live_pid_is_preserved() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let reused = artifact(&projects, "20260521161400");
    let live = artifact(&projects, "20260521161401");
    write_stale_meta(&reused, "reused", 21);
    write_stale_meta(&live, "live", 22);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let mut process = MapProcessProbe::all(ProcessIdentityObservation::Unknown);
    process.insert(21, ProcessIdentityObservation::ReusedPid);
    process.insert(22, ProcessIdentityObservation::MatchingLive);
    let update = terminalize_with(
        &index,
        &projects,
        0,
        None,
        &process_witnesses(process),
    );
    assert_eq!(update.terminalization_synthesized_rows, 1);
    assert_eq!(update.terminalization_skipped_live, 1);
    assert_eq!(query_active(&index, &projects), ["20260521161401"]);
}

#[test]
fn terminalize_unknown_pid_is_preserved() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521161500");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "no-pid"}),
    );
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let update = terminalize_with_dead_processes(&index, &projects, 0, None);
    assert_eq!(update.terminalization_synthesized_rows, 0);
    assert_eq!(update.terminalization_skipped_unknown, 1);
    assert_eq!(query_active(&index, &projects), ["20260521161500"]);
}

#[test]
fn terminalize_foreign_machine_is_unknown() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521161600");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "imported",
            "pid": 4242,
            "process_identity": "boot-a:1",
            "source_machine": "other-host"
        }),
    );
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let update = terminalize_with_dead_processes(&index, &projects, 0, None);
    assert_eq!(update.terminalization_synthesized_rows, 0);
    assert_eq!(update.terminalization_skipped_unknown, 1);
    assert_eq!(query_active(&index, &projects), ["20260521161600"]);
}

#[test]
fn terminalize_waiting_question_and_workflow_are_live() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let waiting = artifact(&projects, "20260521161700");
    let question = artifact(&projects, "20260521161701");
    let workflow = artifact(&projects, "20260521161702");
    write_stale_meta(&waiting, "waiting", 31);
    write_stale_meta(&question, "question", 32);
    write_stale_meta(&workflow, "workflow", 33);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);
    write_json(&waiting.join("waiting.json"), json!({}));
    write_json(&question.join("pending_question.json"), json!({"id": "q"}));
    write_json(
        &workflow.join("workflow_state.json"),
        json!({"workflow_name": "ace-run", "status": "running"}),
    );

    let update = terminalize_with_dead_processes(&index, &projects, 0, None);
    assert_eq!(update.terminalization_synthesized_rows, 0);
    assert_eq!(update.terminalization_skipped_live, 3);
    assert_eq!(query_active(&index, &projects).len(), 3);
}

#[test]
fn terminalize_different_timestamp_claim_does_not_protect() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521161800");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "old-occupant",
            "workspace_num": 2,
            "pid": 4242,
            "process_identity": "boot-a:1"
        }),
    );
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let later = matching_claim("20260521169999", 2);
    let update = terminalize_with(
        &index,
        &projects,
        0,
        None,
        &claim_witnesses(
            MapProcessProbe::all(ProcessIdentityObservation::Missing),
            MapClaimProbe::claims(vec![later]),
        ),
    );
    assert_eq!(update.terminalization_skipped_claim, 0);
    assert_eq!(update.terminalization_synthesized_rows, 1);
}

#[test]
fn terminalize_legacy_workspace_claim_without_timestamp_protects() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521161900");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "legacy-claim",
            "workspace_num": 4,
            "pid": 4242,
            "process_identity": "boot-a:1"
        }),
    );
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let legacy = WorkspaceClaimWire {
        workspace_num: 4,
        workflow: "ace-run".to_string(),
        cl_name: Some("cl".to_string()),
        pid: 99,
        artifacts_timestamp: None,
        pinned: false,
    };
    let update = terminalize_with(
        &index,
        &projects,
        0,
        None,
        &claim_witnesses(
            MapProcessProbe::all(ProcessIdentityObservation::Missing),
            MapClaimProbe::claims(vec![legacy]),
        ),
    );
    assert_eq!(update.terminalization_skipped_claim, 1);
    assert_eq!(update.terminalization_synthesized_rows, 0);
}

#[test]
fn terminalize_unreadable_claims_are_unknown() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521162000");
    write_stale_meta(&artifact_dir, "unreadable-claim", 4242);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let update = terminalize_with(
        &index,
        &projects,
        0,
        None,
        &claim_witnesses(
            MapProcessProbe::all(ProcessIdentityObservation::Missing),
            MapClaimProbe::unreadable(),
        ),
    );
    assert_eq!(update.terminalization_skipped_unknown, 1);
    assert_eq!(update.terminalization_synthesized_rows, 0);
}

#[test]
fn terminalize_skips_race_when_done_marker_appears() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521162100");
    write_stale_meta(&artifact_dir, "raced", 4242);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let done_path = artifact_dir.join("done.json");
    let mut witnesses = dead_process_witnesses();
    witnesses.on_after_observe = Some(Box::new(move || {
        write_json(
            &done_path,
            json!({"outcome": "completed", "name": "raced"}),
        );
    }));
    let update = terminalize_with(&index, &projects, 0, None, &witnesses);
    assert_eq!(update.terminalization_skipped_race, 1);
    assert_eq!(update.terminalization_synthesized_rows, 0);
}

#[test]
fn terminalize_skips_race_when_claim_appears() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521162200");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "claim-race",
            "workspace_num": 7,
            "pid": 4242,
            "process_identity": "boot-a:1"
        }),
    );
    let project_file = projects.join("proj").join("proj.sase");
    fs::create_dir_all(project_file.parent().unwrap()).unwrap();
    fs::write(&project_file, "NAME: proj\n").unwrap();
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let claim_path = project_file.clone();
    let mut witnesses = dead_process_witnesses();
    witnesses.on_after_observe = Some(Box::new(move || {
        fs::write(
            &claim_path,
            "NAME: proj\nRUNNING:\n  #7 | 1 | ace-run | cl | 20260521162200\n",
        )
        .unwrap();
    }));
    let update = terminalize_with(&index, &projects, 0, None, &witnesses);
    assert_eq!(update.terminalization_skipped_race, 1);
    assert_eq!(update.terminalization_synthesized_rows, 0);
}

#[test]
fn terminalize_is_idempotent_and_survives_cached_query() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521162300");
    write_stale_meta(&artifact_dir, "once", 4242);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let first = terminalize_with_dead_processes(&index, &projects, 0, None);
    assert_eq!(first.terminalization_synthesized_rows, 1);
    let second = terminalize_with_dead_processes(&index, &projects, 0, None);
    assert_eq!(second.terminalization_synthesized_rows, 0);
    assert_eq!(second.terminalization_rows_examined, 0);
    assert_eq!(query_hidden(&index, &projects), ["abandoned"]);

    let revalidated = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: true,
            include_full_history: false,
            include_hidden: true,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            recent_completed_limit: Some(10),
            ..AgentArtifactIndexQueryWire::default()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(revalidated.records.len(), 1);
    assert_eq!(
        revalidated.records[0]
            .done
            .as_ref()
            .and_then(|done| done.outcome.as_deref()),
        Some("abandoned")
    );

    rebuild(&index, &projects);
    assert_eq!(query_active(&index, &projects), ["20260521162300"]);
    let again = terminalize_with_dead_processes(&index, &projects, 0, None);
    assert_eq!(again.terminalization_synthesized_rows, 1);
}

#[test]
fn agent_artifact_index_update_wire_defaults_new_diagnostics() {
    let parsed: AgentArtifactIndexUpdateWire = serde_json::from_value(json!({
        "schema_version": 37,
        "index_path": "/tmp/i",
        "projects_root": "/tmp/p",
        "rows_indexed": 0,
        "rows_deleted": 0,
        "rows_skipped": 0
    }))
    .unwrap();
    assert_eq!(parsed.terminalization_rows_examined, 0);
    assert!(!parsed.terminalization_wrapped);
}
