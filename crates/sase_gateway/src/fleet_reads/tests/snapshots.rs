//! Snapshot-cache tests: coalescing, ageing, history pages, and
//! failed-rebuild retention.

use std::fs;

use chrono::Utc;
use sase_core::{
    agent_scan::AgentArtifactScanOptionsWire,
    fleet_contract::{
        FleetCatalogContinuationStateWire, FleetCatalogQueryWire,
        FleetCatalogResetReasonWire, FleetCatalogScopeWire,
        ObservationFreshnessWire,
    },
};
use tempfile::tempdir;

use super::super::resolution::FLEET_SNAPSHOT_STALE_SECONDS;
use super::support::*;

#[tokio::test]
async fn concurrent_snapshot_reads_are_coalesced() {
    let (_temp, service) = seed_home();
    let (left, right) = tokio::join!(service.summary(), service.summary());
    assert!(left.is_ok());
    assert!(right.is_ok());
    assert_eq!(service.refresh_count_for_test(), 1);
}

#[tokio::test]
async fn aged_cache_triggers_exactly_one_coalesced_rebuild() {
    let (_temp, service) = seed_home();
    service.summary().await.unwrap();
    assert_eq!(service.refresh_count_for_test(), 1);

    service.age_cache_for_test(FLEET_SNAPSHOT_STALE_SECONDS + 1.0);
    let (left, right) = tokio::join!(service.summary(), service.summary());
    assert!(left.is_ok());
    assert!(right.is_ok());
    assert_eq!(
        service.refresh_count_for_test(),
        2,
        "a stale cache should trigger exactly one rebuild, coalesced \
             across concurrent readers"
    );
}

#[tokio::test]
async fn failed_rebuild_retains_stale_partial_prior_snapshot() {
    let (_temp, service) = seed_home();
    let first = service.summary().await.unwrap();
    assert_eq!(first.counts.logical_agent_total, 2);
    assert_eq!(service.refresh_count_for_test(), 1);

    service.age_cache_for_test(FLEET_SNAPSHOT_STALE_SECONDS + 1.0);
    // Replace the index file with a directory so the next rebuild
    // attempt fails deterministically instead of relying on timing.
    fs::remove_file(service.index_path()).unwrap();
    fs::create_dir_all(service.index_path()).unwrap();

    let second = service.summary().await.unwrap();
    assert_eq!(
        second.counts.logical_agent_total, 2,
        "a failed rebuild must retain the previous snapshot's data"
    );
    assert_eq!(second.freshness.freshness, ObservationFreshnessWire::Stale);
    assert!(second.freshness.partial);
    assert!(second.freshness.error.is_some());
    assert_eq!(
        service.refresh_count_for_test(),
        1,
        "a failed rebuild must not be counted as a successful refresh"
    );
}

#[tokio::test]
async fn explicit_history_pages_beyond_presentation_without_eager_cache() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    let now = Utc::now();
    for index in 0..205 {
        let finished = now - chrono::Duration::minutes(index);
        let timestamp = finished.format("%Y%m%d%H%M%S").to_string();
        seed_done_agent(
            &projects,
            &timestamp,
            &format!("done-{index:03}"),
            "done output",
            finished.timestamp() as f64,
        );
    }
    let old_finished = now - chrono::Duration::days(8);
    seed_done_agent(
        &projects,
        &old_finished.format("%Y%m%d%H%M%S").to_string(),
        "old-done",
        "old output",
        old_finished.timestamp() as f64,
    );
    let service = build_service(&home, &projects);

    let summary = service.summary().await.unwrap();
    assert_eq!(summary.catalog_scope, FleetCatalogScopeWire::Presentation);
    assert_eq!(summary.counts.logical_agent_total, 0);
    assert!(!service.history_cache_initialized_for_test());

    let presentation = service.catalog(catalog_query()).await.unwrap();
    assert_eq!(presentation.page.scope, FleetCatalogScopeWire::Presentation);
    assert_eq!(presentation.page.total_matching_rows, 200);
    assert_eq!(presentation.counts.logical_agent_total, 0);
    assert!(!presentation.page.rows.iter().any(|row| row
        .labels
        .agent_label
        .as_deref()
        == Some("old-done")));
    assert!(!service.history_cache_initialized_for_test());

    let first_history = service
        .catalog(history_catalog_query(100, None))
        .await
        .unwrap();
    assert_eq!(first_history.page.scope, FleetCatalogScopeWire::History);
    assert_eq!(first_history.page.total_matching_rows, 206);
    assert_eq!(first_history.counts.logical_agent_total, 0);
    assert_eq!(first_history.count_revision, summary.count_revision);
    assert!(first_history.page.has_more);
    assert!(service.history_cache_initialized_for_test());
    assert_eq!(service.history_refresh_count_for_test(), 1);

    let second_history = service
        .catalog(history_catalog_query(
            100,
            first_history.page.next_cursor.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(second_history.page.rows.len(), 100);
    assert!(second_history.page.has_more);
    let third_history = service
        .catalog(history_catalog_query(
            100,
            second_history.page.next_cursor.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(third_history.page.rows.len(), 6);
    assert!(!third_history.page.has_more);
    assert!(third_history
        .page
        .rows
        .iter()
        .any(|row| { row.labels.agent_label.as_deref() == Some("old-done") }));
}

#[tokio::test]
async fn stale_snapshot_bound_cursor_returns_restart_page() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    seed_agent(&projects, &recent_timestamp(3), "alpha", "alpha output");
    seed_agent(&projects, &recent_timestamp(2), "beta", "beta output");
    let service = build_service(&home, &projects);

    let first = service
        .catalog(FleetCatalogQueryWire {
            limit: Some(1),
            ..catalog_query()
        })
        .await
        .unwrap();
    let stale_cursor = first.page.next_cursor.clone();
    seed_agent(&projects, &recent_timestamp(1), "gamma", "gamma output");
    sase_core::rebuild_agent_artifact_index(
        &home.join("agent_artifact_index.sqlite"),
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    service.age_cache_for_test(FLEET_SNAPSHOT_STALE_SECONDS + 1.0);

    let restart = service
        .catalog(FleetCatalogQueryWire {
            cursor: stale_cursor,
            limit: Some(1),
            ..catalog_query()
        })
        .await
        .unwrap();
    assert!(restart.page.rows.is_empty());
    assert_eq!(
        restart.page.state,
        FleetCatalogContinuationStateWire::ResyncRequired
    );
    assert_eq!(
        restart.page.reset_reason,
        Some(FleetCatalogResetReasonWire::SnapshotMismatch)
    );
    assert!(restart.page.next_cursor.is_none());
}

#[tokio::test]
async fn failed_history_rebuild_retains_stale_partial_prior_snapshot() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    let finished = Utc::now() - chrono::Duration::days(8);
    seed_done_agent(
        &projects,
        &finished.format("%Y%m%d%H%M%S").to_string(),
        "old-done",
        "old output",
        finished.timestamp() as f64,
    );
    let service = build_service(&home, &projects);
    let first = service
        .catalog(history_catalog_query(10, None))
        .await
        .unwrap();
    assert_eq!(first.page.total_matching_rows, 1);
    assert_eq!(service.history_refresh_count_for_test(), 1);

    service.age_history_cache_for_test(FLEET_SNAPSHOT_STALE_SECONDS + 1.0);
    fs::remove_file(service.index_path()).unwrap();
    fs::create_dir_all(service.index_path()).unwrap();

    let second = service
        .catalog(history_catalog_query(10, None))
        .await
        .unwrap();
    assert_eq!(second.page.total_matching_rows, 1);
    assert_eq!(second.freshness.freshness, ObservationFreshnessWire::Stale);
    assert!(second.freshness.partial);
    assert!(second.freshness.error.is_some());
    assert_eq!(service.history_refresh_count_for_test(), 1);
}
