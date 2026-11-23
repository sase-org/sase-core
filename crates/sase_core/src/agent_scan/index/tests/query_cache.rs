use super::super::*;
use super::support::{artifact, write_json};
use crate::agent_scan::index::storage::open_index;
use crate::agent_scan::wire::{
    AgentArtifactIndexReusableSnapshotWire, AgentArtifactRecordShapeWire,
    AgentArtifactScanOptionsWire,
};
use rusqlite::params;
use serde_json::json;
use std::time::Instant;
use tempfile::tempdir;

fn revalidate_active_query() -> AgentArtifactIndexQueryWire {
    AgentArtifactIndexQueryWire {
        include_active: true,
        include_recent_completed: false,
        include_full_history: false,
        active_limit: None,
        recent_completed_limit: Some(10),
        include_hidden: false,
        freshness: AgentArtifactIndexFreshnessWire::Revalidate,
        only_monitors: false,
        record_shape: AgentArtifactRecordShapeWire::Full,
        window_limit: None,
        candidate_filter: None,
        agents_list_projection: false,
    }
}

fn write_active_artifacts(projects: &std::path::Path, count: usize) {
    for index in 0..count {
        let artifact_dir =
            artifact(projects, &format!("202605141200{index:02}"));
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": format!("agent-{index}")}),
        );
    }
}

fn query_snapshot(
    index: &std::path::Path,
    projects: &std::path::Path,
    query: AgentArtifactIndexQueryWire,
) -> crate::agent_scan::wire::AgentArtifactScanWire {
    query_agent_artifact_index(
        index,
        projects,
        query,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap()
}

fn query_reusable_snapshot(
    index: &std::path::Path,
    projects: &std::path::Path,
    query: AgentArtifactIndexQueryWire,
    known_generation: Option<u64>,
) -> AgentArtifactIndexReusableSnapshotWire {
    query_agent_artifact_index_reusable(
        index,
        projects,
        query,
        AgentArtifactScanOptionsWire::default(),
        known_generation,
    )
    .unwrap()
}

#[test]
fn identical_revalidate_skips_record_json_decode() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_active_artifacts(&projects, 5);
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let first = query_snapshot(&index, &projects, revalidate_active_query());
    assert_eq!(first.records.len(), 5);
    assert_eq!(first.stats.record_json_decoded, 5);
    assert!(first.stats.marker_signatures_checked >= 5);
    let names: Vec<String> = first
        .records
        .iter()
        .map(|record| {
            record
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.name.clone())
                .unwrap_or_default()
        })
        .collect();

    let second = query_snapshot(&index, &projects, revalidate_active_query());
    assert_eq!(second.records.len(), 5);
    assert_eq!(second.stats.record_json_decoded, 0);
    assert!(second.stats.marker_signatures_checked >= 5);
    assert_eq!(second.stats.rows_repaired, 0);
    let second_names: Vec<String> = second
        .records
        .iter()
        .map(|record| {
            record
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.name.clone())
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(second_names, names);
    assert_eq!(
        second
            .index_completeness
            .as_ref()
            .map(|c| c.record_json_decoded),
        Some(0)
    );
}

#[test]
fn marker_change_misses_cache_and_repairs() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_active_artifacts(&projects, 2);
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let first = query_snapshot(&index, &projects, revalidate_active_query());
    assert_eq!(first.stats.record_json_decoded, 2);

    let warmed = query_snapshot(&index, &projects, revalidate_active_query());
    assert_eq!(warmed.stats.record_json_decoded, 0);

    let changed = artifact(&projects, "20260514120000");
    write_json(
        &changed.join("agent_meta.json"),
        json!({"name": "renamed-agent"}),
    );

    let repaired = query_snapshot(&index, &projects, revalidate_active_query());
    assert!(repaired.stats.record_json_decoded > 0);
    assert!(repaired.stats.rows_repaired >= 1);
    let names: Vec<String> = repaired
        .records
        .iter()
        .filter_map(|record| {
            record
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.name.clone())
        })
        .collect();
    assert!(names.contains(&"renamed-agent".to_string()));
}

#[test]
fn cached_freshness_second_query_skips_decode() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_active_artifacts(&projects, 3);
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let mut query = revalidate_active_query();
    query.freshness = AgentArtifactIndexFreshnessWire::Cached;
    let first = query_snapshot(&index, &projects, query.clone());
    assert_eq!(first.stats.record_json_decoded, 3);
    let second = query_snapshot(&index, &projects, query);
    assert_eq!(second.stats.record_json_decoded, 0);
    assert_eq!(second.records.len(), first.records.len());
}

#[test]
fn list_projection_revalidate_reuses_payload() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_active_artifacts(&projects, 4);
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let mut query = revalidate_active_query();
    query.record_shape = AgentArtifactRecordShapeWire::List;
    let first = query_snapshot(&index, &projects, query.clone());
    assert_eq!(first.records.len(), 4);
    assert!(first.stats.record_json_decoded > 0);
    let second = query_snapshot(&index, &projects, query);
    assert_eq!(second.stats.record_json_decoded, 0);
    assert_eq!(second.records.len(), first.records.len());
}

#[test]
fn identical_revalidate_is_cheaper_than_cold_decode() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_active_artifacts(&projects, 40);
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let cold_started = Instant::now();
    let first = query_snapshot(&index, &projects, revalidate_active_query());
    let cold = cold_started.elapsed();
    assert_eq!(first.stats.record_json_decoded, 40);

    let warm_started = Instant::now();
    let second = query_snapshot(&index, &projects, revalidate_active_query());
    let warm = warm_started.elapsed();
    assert_eq!(second.stats.record_json_decoded, 0);
    eprintln!(
        "index-query-cost: cold decode={} {cold:?}; warm decode={} {warm:?}",
        first.stats.record_json_decoded, second.stats.record_json_decoded
    );
    assert!(
        warm < cold,
        "warm revalidate {warm:?} should beat cold decode {cold:?}"
    );
}

#[test]
fn reusable_snapshot_omits_records_only_for_the_exact_generation() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_active_artifacts(&projects, 5);
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let first = query_reusable_snapshot(
        &index,
        &projects,
        revalidate_active_query(),
        None,
    );
    assert!(!first.records_reused);
    assert_eq!(first.snapshot.records.len(), 5);
    let generation = first.payload_generation.unwrap();

    let unchanged = query_reusable_snapshot(
        &index,
        &projects,
        revalidate_active_query(),
        Some(generation),
    );
    assert!(unchanged.records_reused);
    assert_eq!(unchanged.payload_generation, Some(generation));
    assert!(unchanged.snapshot.records.is_empty());
    assert_eq!(unchanged.snapshot.stats.record_json_decoded, 0);
    assert!(unchanged.snapshot.stats.marker_signatures_checked >= 5);
    assert_eq!(
        unchanged
            .snapshot
            .index_completeness
            .as_ref()
            .map(|facts| facts.record_json_decoded),
        Some(0)
    );

    let mismatched = query_reusable_snapshot(
        &index,
        &projects,
        revalidate_active_query(),
        Some(generation.wrapping_add(1)),
    );
    assert!(!mismatched.records_reused);
    assert_eq!(mismatched.payload_generation, Some(generation));
    assert_eq!(mismatched.snapshot.records.len(), 5);

    let mut different_identity = revalidate_active_query();
    different_identity.include_hidden = true;
    let different = query_reusable_snapshot(
        &index,
        &projects,
        different_identity,
        Some(generation),
    );
    assert!(!different.records_reused);
    assert_eq!(different.snapshot.records.len(), 5);
    assert_ne!(different.payload_generation, Some(generation));
}

#[test]
fn index_file_mutation_invalidates_reuse_even_when_marker_signatures_match() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_active_artifacts(&projects, 2);
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let first = query_reusable_snapshot(
        &index,
        &projects,
        revalidate_active_query(),
        None,
    );
    let generation = first.payload_generation.unwrap();
    let artifact_dir = artifact(&projects, "20260514120000");
    let conn = open_index(&index).unwrap();
    let record_json: String = conn
        .query_row(
            "SELECT record_json FROM agent_artifacts WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| row.get(0),
        )
        .unwrap();
    let mut value: serde_json::Value =
        serde_json::from_str(&record_json).unwrap();
    value["agent_meta"]["name"] = serde_json::json!("index-only-change");
    conn.execute(
        "UPDATE agent_artifacts SET record_json = ?1 WHERE artifact_dir = ?2",
        params![value.to_string(), artifact_dir.to_string_lossy().as_ref()],
    )
    .unwrap();
    drop(conn);

    let changed = query_reusable_snapshot(
        &index,
        &projects,
        revalidate_active_query(),
        Some(generation),
    );
    assert!(!changed.records_reused);
    assert_ne!(changed.payload_generation, Some(generation));
    assert!(changed.snapshot.records.iter().any(|record| {
        record
            .agent_meta
            .as_ref()
            .and_then(|meta| meta.name.as_deref())
            == Some("index-only-change")
    }));
}

#[test]
fn reusable_shapes_and_options_have_separate_identities() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_active_artifacts(&projects, 2);
    let index = tmp.path().join("index.sqlite");
    let options = AgentArtifactScanOptionsWire::default();
    rebuild_agent_artifact_index(&index, &projects, options.clone()).unwrap();
    let first = query_reusable_snapshot(
        &index,
        &projects,
        revalidate_active_query(),
        None,
    );
    let compact = crate::agent_scan::query_agent_artifact_index_list_reusable(
        &index,
        &projects,
        revalidate_active_query(),
        options.clone(),
        first.payload_generation,
    )
    .unwrap();
    assert!(!compact.records_reused);
    assert_eq!(compact.snapshot.records.len(), 2);
    assert_ne!(compact.payload_generation, first.payload_generation);
    let full = query_reusable_snapshot(
        &index,
        &projects,
        revalidate_active_query(),
        compact.payload_generation,
    );
    assert!(!full.records_reused);
    let mut changed_options = options;
    changed_options.include_prompt_step_markers = false;
    let changed = query_agent_artifact_index_reusable(
        &index,
        &projects,
        revalidate_active_query(),
        changed_options,
        first.payload_generation,
    )
    .unwrap();
    assert!(!changed.records_reused);
    assert_ne!(changed.payload_generation, first.payload_generation);
}

#[test]
fn malformed_index_rows_never_receive_a_reusable_generation() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_active_artifacts(&projects, 2);
    let index = tmp.path().join("index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let before = query_reusable_snapshot(
        &index,
        &projects,
        revalidate_active_query(),
        None,
    );
    let conn = open_index(&index).unwrap();
    conn.execute("UPDATE agent_artifacts SET record_json = 'not json'", [])
        .unwrap();
    for _ in 0..2 {
        let result = query_reusable_snapshot(
            &index,
            &projects,
            revalidate_active_query(),
            before.payload_generation,
        );
        assert!(!result.records_reused);
        assert_eq!(result.payload_generation, None);
        assert_eq!(result.snapshot.stats.json_decode_errors, 2);
    }
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let repaired = query_reusable_snapshot(
        &index,
        &projects,
        revalidate_active_query(),
        before.payload_generation,
    );
    assert_eq!(repaired.snapshot.stats.json_decode_errors, 0);
    // Rebuilding identical data can reuse the original valid generation.
    assert!(repaired.records_reused || repaired.snapshot.records.len() == 2);
}

#[test]
fn concurrent_commit_cannot_certify_precommit_records() {
    use super::super::query_cache::{
        current_data_version, remember, QueryIdentity,
    };
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_active_artifacts(&projects, 2);
    let index = tmp.path().join("index.sqlite");
    let options = AgentArtifactScanOptionsWire::default();
    rebuild_agent_artifact_index(&index, &projects, options.clone()).unwrap();
    let query = revalidate_active_query();
    let conn = open_index(&index).unwrap();
    let version = current_data_version(&conn).unwrap();
    let snapshot = query_snapshot(&index, &projects, query.clone());
    // Deterministically interleave a separate connection's commit after the
    // records were read, but before their reuse signature is collected.
    let writer = open_index(&index).unwrap();
    writer
        .execute("UPDATE agent_artifacts SET record_json = '{}'", [])
        .unwrap();
    let identity = QueryIdentity {
        index_path: &index,
        projects_root: &projects,
        query: &query,
        options: &options,
        compact: false,
    };
    assert_eq!(
        remember(
            &identity,
            &conn,
            None,
            std::sync::Arc::new(snapshot.records),
            version
        ),
        None
    );
}

#[test]
fn reusable_membership_and_missing_index_are_revalidated() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_active_artifacts(&projects, 2);
    let index = tmp.path().join("index.sqlite");
    let options = AgentArtifactScanOptionsWire::default();
    rebuild_agent_artifact_index(&index, &projects, options.clone()).unwrap();
    let before = query_reusable_snapshot(
        &index,
        &projects,
        revalidate_active_query(),
        None,
    );
    let dir = artifact(&projects, "20260514120000");
    std::fs::remove_dir_all(&dir).unwrap();
    let removed = query_reusable_snapshot(
        &index,
        &projects,
        revalidate_active_query(),
        before.payload_generation,
    );
    assert!(!removed.records_reused);
    assert_eq!(removed.snapshot.records.len(), 1);
    assert_eq!(removed.snapshot.stats.rows_removed, 1);
    std::fs::remove_file(&index).unwrap();
    let mut query = revalidate_active_query();
    query.freshness = AgentArtifactIndexFreshnessWire::Cached;
    // The established cached-query contract creates a missing index. It
    // must return a new, incomplete empty snapshot, never the old payload.
    let missing = query_agent_artifact_index_reusable(
        &index,
        &projects,
        query,
        options,
        before.payload_generation,
    )
    .unwrap();
    assert!(!missing.records_reused);
    assert!(missing.snapshot.records.is_empty());
    assert_ne!(missing.payload_generation, before.payload_generation);
    assert!(
        !missing
            .snapshot
            .index_completeness
            .unwrap()
            .complete_history
    );
}
