use super::super::refresh::projects_root_key;
use super::super::terminalization::TERMINALIZATION_CURSOR_KEY_PREFIX;
use super::super::*;
use super::support::{artifact, write_json};
use super::terminalization::{
    active_query, terminalize_with_dead_processes, write_stale_meta,
};
use crate::agent_scan::wire::AgentArtifactScanOptionsWire;
use rusqlite::Connection;
use serde_json::json;
use tempfile::tempdir;

fn rebuild(index: &std::path::Path, projects: &std::path::Path) {
    rebuild_agent_artifact_index(
        index,
        projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
}

fn cursor_key(projects: &std::path::Path) -> String {
    format!(
        "{TERMINALIZATION_CURSOR_KEY_PREFIX}{}",
        projects_root_key(projects)
    )
}

fn load_cursor(
    index: &std::path::Path,
    projects: &std::path::Path,
) -> Option<String> {
    read_agent_artifact_index_meta(index, &cursor_key(projects)).unwrap()
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

fn seed_unknown_prefix(projects: &std::path::Path, timestamps: &[&str]) {
    for ts in timestamps {
        let dir = artifact(projects, ts);
        write_json(
            &dir.join("agent_meta.json"),
            json!({"name": format!("skip-{ts}")}),
        );
    }
}

#[test]
fn terminalize_max_rows_zero_examines_nothing() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_stale_meta(&artifact(&projects, "20260521163000"), "dead", 4242);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let update = terminalize_with_dead_processes(&index, &projects, 0, Some(0));
    assert_eq!(update.terminalization_rows_examined, 0);
    assert!(!update.terminalization_wrapped);
    assert_eq!(update.terminalization_synthesized_rows, 0);
    assert_eq!(query_active(&index, &projects), ["20260521163000"]);
    assert!(load_cursor(&index, &projects).is_none());
}

#[test]
fn terminalize_advances_past_skipped_prefix_larger_than_batch() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    seed_unknown_prefix(
        &projects,
        &["20260521163100", "20260521163101", "20260521163102"],
    );
    write_stale_meta(&artifact(&projects, "20260521163103"), "eligible", 4242);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let first = terminalize_with_dead_processes(&index, &projects, 0, Some(2));
    assert_eq!(first.terminalization_rows_examined, 2);
    assert_eq!(first.terminalization_synthesized_rows, 0);
    assert_eq!(first.terminalization_skipped_unknown, 2);
    assert!(!first.terminalization_wrapped);
    assert!(load_cursor(&index, &projects).is_some());
    assert!(query_active(&index, &projects).contains(&"20260521163103".into()));

    let second = terminalize_with_dead_processes(&index, &projects, 0, Some(2));
    assert_eq!(second.terminalization_rows_examined, 2);
    assert_eq!(second.terminalization_synthesized_rows, 1);
    assert_eq!(second.terminalization_skipped_unknown, 1);
    assert!(!second.terminalization_wrapped);
    assert!(load_cursor(&index, &projects).is_some());
    let active = query_active(&index, &projects);
    assert!(!active.contains(&"20260521163103".into()));
    assert_eq!(active.len(), 3);

    let wrap = terminalize_with_dead_processes(&index, &projects, 0, Some(2));
    assert!(wrap.terminalization_wrapped);
    assert_eq!(wrap.terminalization_rows_examined, 0);
    assert!(load_cursor(&index, &projects).is_none());
}

#[test]
fn terminalize_wraps_and_visits_rows_inserted_before_cursor() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    seed_unknown_prefix(&projects, &["20260521163210", "20260521163220"]);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let first = terminalize_with_dead_processes(&index, &projects, 0, Some(1));
    assert!(!first.terminalization_wrapped);
    assert!(load_cursor(&index, &projects).is_some());

    let inserted = artifact(&projects, "20260521163200");
    write_stale_meta(&inserted, "inserted", 4242);
    upsert_agent_artifact_index_row(
        &index,
        &projects,
        &inserted,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert!(load_cursor(&index, &projects).is_some());

    let second = terminalize_with_dead_processes(&index, &projects, 0, Some(1));
    assert_eq!(second.terminalization_synthesized_rows, 0);
    assert!(query_active(&index, &projects).contains(&"20260521163200".into()));

    let wrap = terminalize_with_dead_processes(&index, &projects, 0, Some(1));
    assert!(wrap.terminalization_wrapped);
    assert_eq!(wrap.terminalization_rows_examined, 0);

    let after_wrap =
        terminalize_with_dead_processes(&index, &projects, 0, Some(1));
    assert_eq!(after_wrap.terminalization_synthesized_rows, 1);
    assert!(!query_active(&index, &projects).contains(&"20260521163200".into()));
}

#[test]
fn terminalize_corrupt_cursor_resets_to_start() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_stale_meta(&artifact(&projects, "20260521163300"), "dead", 4242);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);
    write_agent_artifact_index_meta(&index, &cursor_key(&projects), "not-json")
        .unwrap();

    let update =
        terminalize_with_dead_processes(&index, &projects, 0, Some(10));
    assert_eq!(update.terminalization_synthesized_rows, 1);
    assert!(update.terminalization_wrapped);
    assert!(load_cursor(&index, &projects).is_none());
}

#[test]
fn terminalize_rebuild_clears_cursor() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    seed_unknown_prefix(
        &projects,
        &["20260521163400", "20260521163401", "20260521163402"],
    );
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);
    let first = terminalize_with_dead_processes(&index, &projects, 0, Some(1));
    assert!(load_cursor(&index, &projects).is_some());
    assert!(!first.terminalization_wrapped);

    rebuild(&index, &projects);
    assert!(load_cursor(&index, &projects).is_none());
}

#[test]
fn terminalize_delete_during_traversal_does_not_stall() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_stale_meta(&artifact(&projects, "20260521163500"), "first", 11);
    write_stale_meta(&artifact(&projects, "20260521163501"), "second", 12);
    write_stale_meta(&artifact(&projects, "20260521163502"), "third", 13);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let first = terminalize_with_dead_processes(&index, &projects, 0, Some(1));
    assert_eq!(first.terminalization_synthesized_rows, 1);
    assert!(load_cursor(&index, &projects).is_some());

    let second_dir = artifact(&projects, "20260521163501");
    {
        let conn = Connection::open(&index).unwrap();
        conn.execute(
            "DELETE FROM agent_artifacts WHERE artifact_dir = ?1",
            [second_dir.to_string_lossy().as_ref()],
        )
        .unwrap();
    }

    let second = terminalize_with_dead_processes(&index, &projects, 0, Some(1));
    assert_eq!(second.terminalization_synthesized_rows, 1);
    assert!(!second.terminalization_wrapped);
    assert!(query_active(&index, &projects).is_empty());

    let wrap = terminalize_with_dead_processes(&index, &projects, 0, Some(1));
    assert!(wrap.terminalization_wrapped);
    assert_eq!(wrap.terminalization_rows_examined, 0);
    assert!(load_cursor(&index, &projects).is_none());
}

#[test]
fn terminalize_unbounded_pass_always_wraps() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_stale_meta(&artifact(&projects, "20260521163600"), "dead", 4242);
    let index = tmp.path().join("index.sqlite");
    rebuild(&index, &projects);

    let update = terminalize_with_dead_processes(&index, &projects, 0, None);
    assert!(update.terminalization_wrapped);
    assert!(load_cursor(&index, &projects).is_none());
}
