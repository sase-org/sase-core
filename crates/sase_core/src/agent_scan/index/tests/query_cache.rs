use super::super::*;
use super::support::{artifact, write_json};
use crate::agent_scan::wire::{
    AgentArtifactRecordShapeWire, AgentArtifactScanOptionsWire,
};
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
