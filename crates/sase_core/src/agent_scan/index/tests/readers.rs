use super::super::storage::{open_index, read_index_schema_version};
use super::super::*;
use super::support::{artifact, write_json};
use crate::agent_scan::wire::AgentArtifactScanOptionsWire;
use rusqlite::Connection;
use serde_json::json;
use std::fs;
use std::time::Duration;
use tempfile::tempdir;

fn catalog_columns() -> [&'static str; 19] {
    [
        "artifact_dir",
        "project_name",
        "workflow_name",
        "agent_type",
        "cl_name",
        "model",
        "llm_provider",
        "status",
        "workflow_status",
        "hidden",
        "started_at",
        "finished_at",
        "retry_attempt",
        "agent_clan",
        "clan_tribe",
        "parent_timestamp",
        "retry_of_timestamp",
        "retried_as_timestamp",
        "retry_chain_root_timestamp",
    ]
}

fn provenance_columns() -> [&'static str; 5] {
    [
        "artifact_dir",
        "project_name",
        "workflow_dir_name",
        "timestamp",
        "record_json",
    ]
}

fn build_index() -> (tempfile::TempDir, std::path::PathBuf) {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20261013120000");
    write_json(
        &dir.join("agent_meta.json"),
        json!({"name": "reader-agent", "cl_name": "reader"}),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    (tmp, index)
}

#[test]
fn schema_version_is_none_when_file_is_missing() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("missing.sqlite");
    assert_eq!(read_agent_artifact_index_schema_version(&index), None);
}

#[test]
fn schema_version_is_none_when_file_is_unreadable() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("garbage.sqlite");
    fs::write(&index, b"not a database").unwrap();
    assert_eq!(read_agent_artifact_index_schema_version(&index), None);
}

#[test]
fn schema_version_is_zero_when_meta_table_is_missing() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("no-meta.sqlite");
    Connection::open(&index)
        .unwrap()
        .execute_batch("CREATE TABLE other (id INTEGER PRIMARY KEY);")
        .unwrap();
    assert_eq!(read_agent_artifact_index_schema_version(&index), Some(0));
}

#[test]
fn schema_version_is_zero_when_meta_key_is_missing() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("empty-meta.sqlite");
    Connection::open(&index)
        .unwrap()
        .execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
        )
        .unwrap();
    assert_eq!(read_agent_artifact_index_schema_version(&index), Some(0));
}

#[test]
fn schema_version_read_does_not_migrate_a_stale_index() {
    let (_tmp, index) = build_index();
    Connection::open(&index)
        .unwrap()
        .execute(
            "UPDATE meta SET value = '33' WHERE key = 'schema_version'",
            [],
        )
        .unwrap();

    assert_eq!(read_agent_artifact_index_schema_version(&index), Some(33));
    let conn = Connection::open(&index).unwrap();
    assert_eq!(read_index_schema_version(&conn).unwrap(), 33);
}

#[test]
fn projection_serves_catalog_and_provenance_column_sets() {
    let (_tmp, index) = build_index();
    let catalog = load_agent_artifact_index_projection(
        &index,
        &catalog_columns(),
        AGENT_ARTIFACT_INDEX_PROJECTION_BUSY_TIMEOUT,
    )
    .unwrap();
    assert_eq!(
        catalog.schema_version,
        i64::from(AGENT_ARTIFACT_INDEX_SCHEMA_VERSION)
    );
    assert_eq!(catalog.rows.len(), 1);
    assert_eq!(catalog.rows[0].len(), catalog_columns().len());
    assert_eq!(
        catalog.rows[0][0],
        json!(index
            .parent()
            .unwrap()
            .join("projects/proj/artifacts/ace-run/20261013120000")
            .to_string_lossy()
            .into_owned())
    );
    assert_eq!(catalog.rows[0][1], json!("proj"));

    let provenance = load_agent_artifact_index_projection(
        &index,
        &provenance_columns(),
        Duration::from_millis(250),
    )
    .unwrap();
    assert_eq!(provenance.rows.len(), 1);
    assert_eq!(provenance.rows[0].len(), 5);
    assert!(provenance.rows[0][4]
        .as_str()
        .unwrap()
        .contains("reader-agent"));
}

#[test]
fn projection_does_not_migrate_a_stale_index() {
    let (_tmp, index) = build_index();
    Connection::open(&index)
        .unwrap()
        .execute(
            "UPDATE meta SET value = '12' WHERE key = 'schema_version'",
            [],
        )
        .unwrap();
    let projection = load_agent_artifact_index_projection(
        &index,
        &["artifact_dir"],
        AGENT_ARTIFACT_INDEX_PROJECTION_BUSY_TIMEOUT,
    )
    .unwrap();
    assert_eq!(projection.schema_version, 12);
    let conn = Connection::open(&index).unwrap();
    assert_eq!(read_index_schema_version(&conn).unwrap(), 12);
}

#[test]
fn projection_rejects_unknown_column_names() {
    let (_tmp, index) = build_index();
    let err = load_agent_artifact_index_projection(
        &index,
        &["artifact_dir", "not_a_column; DROP TABLE agent_artifacts"],
        AGENT_ARTIFACT_INDEX_PROJECTION_BUSY_TIMEOUT,
    )
    .unwrap_err();
    assert!(err.contains("unsupported agent_artifacts column"), "{err}");
    assert!(err.contains("not_a_column"), "{err}");
}

#[test]
fn projection_rejects_an_empty_column_list() {
    let (_tmp, index) = build_index();
    let err = load_agent_artifact_index_projection(
        &index,
        &[] as &[&str],
        AGENT_ARTIFACT_INDEX_PROJECTION_BUSY_TIMEOUT,
    )
    .unwrap_err();
    assert_eq!(err, "columns must not be empty");
}

#[test]
fn aliases_round_trip_upsert_list_and_delete() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    upsert_agent_artifact_alias(&index, "/old/path", "/new/path").unwrap();
    upsert_agent_artifact_alias(&index, "/keep/path", "/keep/dir").unwrap();
    let listed = list_agent_artifact_aliases(&index).unwrap();
    assert_eq!(
        listed,
        vec![
            AgentArtifactAliasWire {
                alias_path: "/keep/path".into(),
                artifact_dir: "/keep/dir".into(),
            },
            AgentArtifactAliasWire {
                alias_path: "/old/path".into(),
                artifact_dir: "/new/path".into(),
            },
        ]
    );

    delete_agent_artifact_alias(&index, "/old/path", "/new/path").unwrap();
    let listed = list_agent_artifact_aliases(&index).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].alias_path, "/keep/path");

    delete_agent_artifact_alias(&index, "/unused", "/keep/dir").unwrap();
    assert!(list_agent_artifact_aliases(&index).unwrap().is_empty());
}

#[test]
fn aliases_are_empty_when_the_index_is_missing() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("missing.sqlite");
    assert!(list_agent_artifact_aliases(&index).unwrap().is_empty());
    delete_agent_artifact_alias(&index, "/old", "/new").unwrap();
    assert!(!index.exists());
}

#[test]
fn schema_version_read_does_not_create_an_index() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    assert_eq!(read_agent_artifact_index_schema_version(&index), None);
    assert!(!index.exists());
    drop(open_index(&index).unwrap());
    assert_eq!(
        read_agent_artifact_index_schema_version(&index),
        Some(i64::from(AGENT_ARTIFACT_INDEX_SCHEMA_VERSION))
    );
}
