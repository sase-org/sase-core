use super::super::storage::{
    is_sqlite_index_corruption_error, open_index, open_index_read_only,
    read_index_schema_version, replace_unusable_index_file,
    sqlite_sidecar_path,
};
use super::super::*;
use super::support::{artifact, count_sql, write_json};
use crate::agent_scan::scanner::scan_agent_artifacts;
use crate::agent_scan::wire::{
    AgentArtifactRecordWire, AgentArtifactScanOptionsWire,
};
use rusqlite::{params, Connection};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn read_only_open_falls_back_when_index_missing() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    assert!(!index.exists());

    let conn = open_index_read_only(&index).unwrap();
    assert_eq!(
        read_index_schema_version(&conn).unwrap(),
        AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
    );
}

#[test]
fn read_only_open_cannot_write() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    drop(open_index(&index).unwrap());

    let conn = open_index_read_only(&index).unwrap();
    let result = conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('probe', 'x')",
        [],
    );
    assert!(result.is_err());
}

#[test]
fn status_reports_freelist_and_file_size() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20260827090000");
    write_json(&dir.join("agent_meta.json"), json!({"name": "sized"}));
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let status = agent_artifact_index_status(&index).unwrap();
    assert!(status.file_size_bytes > 0);
    assert_eq!(
        status.file_size_bytes,
        std::fs::metadata(&index).unwrap().len(),
    );
}

#[test]
fn vacuum_reclaims_freelist_pages_and_preserves_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let index = tmp.path().join("agent_artifact_index.sqlite");
    let padding = "x".repeat(4096);
    let mut dirs = Vec::new();
    for n in 0..50 {
        let dir = artifact(&projects, &format!("202608270900{n:02}"));
        write_json(
            &dir.join("agent_meta.json"),
            json!({"name": format!("agent-{n}-{padding}")}),
        );
        dirs.push(dir);
    }
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    // Remove all but one artifact dir from disk, then rebuild so the
    // index reconciles away the missing rows, leaving real freelist
    // pages behind for VACUUM to reclaim.
    for dir in &dirs[1..] {
        fs::remove_dir_all(dir).unwrap();
    }
    let reconciled = rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(reconciled.rows_indexed, 1);

    let before = agent_artifact_index_status(&index).unwrap();
    assert!(before.freelist_pages > 0);

    let update = vacuum_agent_artifact_index(&index).unwrap();

    assert_eq!(update.freelist_pages_before, before.freelist_pages);
    assert_eq!(update.freelist_pages_after, 0);
    assert!(update.file_size_bytes_after < update.file_size_bytes_before);
    assert_eq!(
        update.bytes_reclaimed,
        update.file_size_bytes_before - update.file_size_bytes_after,
    );

    let after = agent_artifact_index_status(&index).unwrap();
    assert_eq!(after.freelist_pages, 0);
    // VACUUM never removes or alters surviving rows.
    assert_eq!(after.agent_artifacts_rows, before.agent_artifacts_rows);
    assert_eq!(after.agent_artifacts_rows, 1);
}

#[test]
fn late_xprompts_file_refreshes_cached_record() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260729121000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "late-xprompt-user"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let initial = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire::default(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(initial.records.len(), 1);
    assert!(initial.records[0].used_xprompts.is_empty());

    write_json(
        &artifact_dir.join("xprompts.json"),
        json!([
            {"name": "gh", "kind": "workflow", "tags": ["vcs"]},
            {"name": "gh", "kind": "workflow", "tags": ["vcs"]},
            {"name": "research_swarm", "kind": "swarm", "tags": ["research"]}
        ]),
    );

    let refreshed = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire::default(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(refreshed.records.len(), 1);
    assert_eq!(refreshed.records[0].used_xprompts.len(), 2);
    assert_eq!(refreshed.records[0].used_xprompts[0].name, "gh");
    assert_eq!(refreshed.records[0].used_xprompts[0].references, 2);
    assert_eq!(refreshed.records[0].used_xprompts[1].name, "research_swarm");
    assert_eq!(refreshed.records[0].used_xprompts[1].kind, "swarm");
    assert_eq!(refreshed.records[0].used_xprompts[1].references, 1);

    let conn = Connection::open(&index).unwrap();
    let (signature, record_json): (Option<String>, String) = conn
        .query_row(
            "SELECT xprompts_sig, record_json FROM agent_artifacts \
             WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(signature.is_some());
    let stored: AgentArtifactRecordWire =
        serde_json::from_str(&record_json).unwrap();
    assert_eq!(stored.used_xprompts, refreshed.records[0].used_xprompts);
}

#[test]
fn cached_query_returns_rebuilt_records_without_revalidation() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let first = artifact(&projects, "20260729122000");
    let second = artifact(&projects, "20260729122100");
    write_json(
        &first.join("agent_meta.json"),
        json!({"name": "active", "pid": 123, "model": "gpt"}),
    );
    write_json(
        &second.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1816856460.0,
            "name": "done",
            "cl_name": "cl_alpha"
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let cached = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_full_history: true,
            recent_completed_limit: None,
            include_hidden: true,
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            ..AgentArtifactIndexQueryWire::default()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let source = scan_agent_artifacts(
        &projects,
        AgentArtifactScanOptionsWire::default(),
    );
    assert_eq!(cached.records, source.records);
}

#[test]
fn cached_query_does_not_refresh_stale_marker_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260729122500");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "late-xprompt-user"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    write_json(
        &artifact_dir.join("xprompts.json"),
        json!([{"name": "gh", "kind": "workflow", "tags": ["vcs"]}]),
    );

    let cached = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            ..AgentArtifactIndexQueryWire::default()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(cached.records.len(), 1);
    assert!(cached.records[0].used_xprompts.is_empty());

    let conn = Connection::open(&index).unwrap();
    let (signature, record_json): (Option<String>, String) = conn
        .query_row(
            "SELECT xprompts_sig, record_json FROM agent_artifacts \
             WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(signature.is_none());
    let stored: AgentArtifactRecordWire =
        serde_json::from_str(&record_json).unwrap();
    assert!(stored.used_xprompts.is_empty());
}

#[test]
fn rebuild_replaces_corrupt_existing_index() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521143000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "active", "pid": 123}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    fs::write(&index, b"this is not a sqlite database").unwrap();
    fs::write(sqlite_sidecar_path(&index, "-wal"), b"stale wal").unwrap();
    fs::write(sqlite_sidecar_path(&index, "-shm"), b"stale shm").unwrap();

    let update = rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(update.rows_indexed, 1);

    let conn = Connection::open(&index).unwrap();
    let version: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION.to_string());
    let quarantined: Vec<PathBuf> = fs::read_dir(tmp.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            name.starts_with("agent_artifact_index.sqlite.corrupt-")
                && !name.ends_with("-wal")
                && !name.ends_with("-shm")
        })
        .collect();
    assert_eq!(quarantined.len(), 1);

    let indexed = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire::default(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(indexed.records.len(), 1);
    assert_eq!(indexed.records[0].timestamp, "20260521143000");
}

#[test]
fn replace_unusable_index_file_renames_sidecars() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    fs::write(&index, b"this is not a sqlite database").unwrap();
    fs::write(sqlite_sidecar_path(&index, "-wal"), b"stale wal").unwrap();
    fs::write(sqlite_sidecar_path(&index, "-shm"), b"stale shm").unwrap();

    replace_unusable_index_file(&index).unwrap();

    let quarantined: Vec<PathBuf> = fs::read_dir(tmp.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            name.starts_with("agent_artifact_index.sqlite.corrupt-")
                && !name.ends_with("-wal")
                && !name.ends_with("-shm")
        })
        .collect();
    assert_eq!(quarantined.len(), 1);
    assert_eq!(
        fs::read(&quarantined[0]).unwrap(),
        b"this is not a sqlite database"
    );
    assert_eq!(
        fs::read(sqlite_sidecar_path(&quarantined[0], "-wal")).unwrap(),
        b"stale wal"
    );
    assert_eq!(
        fs::read(sqlite_sidecar_path(&quarantined[0], "-shm")).unwrap(),
        b"stale shm"
    );
    assert!(!index.exists());
    assert!(!sqlite_sidecar_path(&index, "-wal").exists());
    assert!(!sqlite_sidecar_path(&index, "-shm").exists());
}

#[test]
fn query_keeps_corrupt_existing_index_strict() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let index = tmp.path().join("agent_artifact_index.sqlite");
    fs::write(&index, b"this is not a sqlite database").unwrap();

    let err = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire::default(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap_err();

    assert!(is_sqlite_index_corruption_error(&err), "{err}");
    assert_eq!(fs::read(&index).unwrap(), b"this is not a sqlite database");
}

#[test]
fn upsert_and_delete_one_artifact_row() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260504121212");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "active", "pid": 123}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    let update = upsert_agent_artifact_index_row(
        &index,
        &projects,
        &artifact_dir,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(update.rows_indexed, 1);

    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire::default(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].timestamp, "20260504121212");

    let deleted =
        delete_agent_artifact_index_row(&index, &artifact_dir).unwrap();
    assert_eq!(deleted.rows_deleted, 1);
    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire::default(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert!(snapshot.records.is_empty());
}

#[test]
fn hidden_terminal_retention_bounds_rebuild_and_preserves_anchors() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for index in 0..4_700 {
        let dir = artifact(&projects, &format!("20260817{index:06}"));
        write_json(
            &dir.join("agent_meta.json"),
            json!({"name": format!("hidden-{index}"), "hidden": true}),
        );
        write_json(
            &dir.join("done.json"),
            json!({"outcome": "completed", "hidden": true}),
        );
    }

    let pruned_dir = artifact(&projects, "20260817000000");
    write_json(
        &pruned_dir.join("agent_meta.json"),
        json!({
            "name": "old-projected",
            "hidden": true,
            "output_variables": {"status": "old"},
            "model_alias": "large",
            "model_alias_trail": ["large"]
        }),
    );
    write_json(
        &pruned_dir.join("done.json"),
        json!({"outcome": "completed", "hidden": true}),
    );

    let visible = artifact(&projects, "20200101000000");
    write_json(
        &visible.join("done.json"),
        json!({"outcome": "completed", "hidden": false}),
    );
    let active_hidden = artifact(&projects, "20200101000001");
    write_json(
        &active_hidden.join("agent_meta.json"),
        json!({"name": "active-hidden", "hidden": true}),
    );
    let parent = artifact(&projects, "20200101000002");
    write_json(
        &parent.join("agent_meta.json"),
        json!({"name": "hidden-parent", "hidden": true}),
    );
    write_json(
        &parent.join("done.json"),
        json!({"outcome": "completed", "hidden": true}),
    );
    let visible_child = artifact(&projects, "20260918000000");
    write_json(
        &visible_child.join("agent_meta.json"),
        json!({
            "name": "visible-child",
            "parent_timestamp": "20200101000002"
        }),
    );
    write_json(
        &visible_child.join("done.json"),
        json!({"outcome": "completed", "hidden": false}),
    );
    let hidden_lineage = artifact(&projects, "20200101000003");
    write_json(
        &hidden_lineage.join("agent_meta.json"),
        json!({
            "name": "hidden-lineage",
            "hidden": true,
            "parent_timestamp": "20200101000000"
        }),
    );
    write_json(
        &hidden_lineage.join("done.json"),
        json!({"outcome": "completed", "hidden": true}),
    );
    let clan_source = artifact(&projects, "20200101000004");
    write_json(
        &clan_source.join("agent_meta.json"),
        json!({
            "name": "clan-source",
            "hidden": true,
            "agent_clan": "ship",
            "agent_clan_generation": "gen-1",
            "clan_tribe": "release"
        }),
    );
    write_json(
        &clan_source.join("done.json"),
        json!({"outcome": "completed", "hidden": true}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    let update = rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert!(update.hidden_terminal_rows_pruned >= 600);
    assert!(update.hidden_terminal_rows_retained >= 4_096);
    let status = agent_artifact_index_status(&index).unwrap();
    assert_eq!(status.hidden_terminal_rows_prunable, 0);
    assert_eq!(
        count_sql(
            &index,
            "SELECT COUNT(*) FROM agent_artifacts \
             WHERE artifact_dir LIKE '%20260817000000'"
        ),
        0
    );
    for timestamp in [
        "20200101000000",
        "20200101000001",
        "20200101000002",
        "20200101000003",
        "20200101000004",
    ] {
        assert_eq!(
            count_sql(
                &index,
                &format!(
                    "SELECT COUNT(*) FROM agent_artifacts \
                     WHERE timestamp = '{timestamp}'"
                ),
            ),
            1,
            "{timestamp}"
        );
    }
    assert_eq!(
        count_sql(
            &index,
            "SELECT COUNT(*) FROM agent_output_variables \
             WHERE artifact_dir LIKE '%20260817000000'"
        ),
        0
    );
    assert_eq!(
        count_sql(
            &index,
            "SELECT COUNT(*) FROM agent_artifact_model_aliases \
             WHERE artifact_dir LIKE '%20260817000000'"
        ),
        0
    );

    let related =
        query_related_agent_artifact_dirs(&index, &visible_child, &[]).unwrap();
    assert!(related
        .iter()
        .any(|path| path == parent.to_string_lossy().as_ref()));
}

#[test]
fn hidden_terminal_retention_prunes_dependents_and_is_idempotent() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20260818000000");
    write_json(
        &dir.join("agent_meta.json"),
        json!({
            "name": "hidden",
            "hidden": true,
            "output_variables": {"status": "old"},
            "model_alias": "large",
            "model_alias_trail": ["large"]
        }),
    );
    write_json(
        &dir.join("done.json"),
        json!({"outcome": "completed", "hidden": true}),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    upsert_agent_artifact_index_row(
        &index,
        &projects,
        &dir,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    {
        let conn = Connection::open(&index).unwrap();
        conn.execute(
            "INSERT INTO agent_artifact_aliases(alias_path, artifact_dir) \
             VALUES (?1, ?2)",
            params![
                "/legacy/artifacts/20260818000000",
                dir.to_string_lossy().as_ref(),
            ],
        )
        .unwrap();
    }

    let pruned =
        prune_hidden_terminal_agent_artifact_index_rows(&index, Some(0))
            .unwrap();
    assert_eq!(pruned.hidden_terminal_rows_pruned, 1);
    assert_eq!(pruned.hidden_terminal_rows_retained, 0);
    assert_eq!(
        agent_artifact_index_status(&index)
            .unwrap()
            .agent_artifacts_rows,
        0
    );
    assert_eq!(
        count_sql(&index, "SELECT COUNT(*) FROM agent_artifact_aliases"),
        0
    );
    assert_eq!(
        count_sql(&index, "SELECT COUNT(*) FROM agent_output_variables"),
        0
    );
    assert_eq!(
        count_sql(&index, "SELECT COUNT(*) FROM agent_artifact_model_aliases"),
        0
    );

    let second =
        prune_hidden_terminal_agent_artifact_index_rows(&index, Some(0))
            .unwrap();
    assert_eq!(second.hidden_terminal_rows_pruned, 0);
    assert_eq!(second.hidden_terminal_rows_retained, 0);
}

#[test]
fn bounded_artifact_index_delete_skips_locked_database() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260504121212");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "active", "pid": 123}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    upsert_agent_artifact_index_row(
        &index,
        &projects,
        &artifact_dir,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let blocker = Connection::open(&index).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let result = delete_agent_artifact_index_row_with_busy_timeout(
        &index,
        &artifact_dir,
        Duration::from_millis(10),
    );
    blocker.execute_batch("ROLLBACK").unwrap();

    assert!(result.is_err());
    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire::default(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(snapshot.records.len(), 1);
}
