//! Read-API tests: summary, catalog, batch lookup, detail, content,
//! and project eligibility.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use sase_core::fleet_contract::{
    FleetCatalogQueryWire, FleetCatalogScopeWire, FleetContentReadRequestWire,
    FleetDetailRequestWire, FleetLogicalBatchRequestWire,
    FleetProjectEligibilityRequestWire, ResourceRevisionWire,
    FLEET_CONTRACT_SCHEMA_VERSION,
};
use tempfile::tempdir;

use super::super::errors::FleetReadError;
use super::support::*;

/// An ordinary multiline prompt is the exact payload that made a host's
/// hello/summary/catalog/detail reads fail owner-side validation. The
/// scanner only trims the snippet, so interior newlines reach projection.
#[tokio::test]
async fn ordinary_multiline_prompt_stays_presentable_across_read_apis() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    seed_agent_with_raw_prompt(
        &projects,
        &recent_timestamp(2),
        "alpha",
        "Refactor the widget\nand update the tests\r\nplease\tthanks",
    );
    let service = build_service(&home, &projects);

    let summary = service.summary().await.unwrap();
    assert_eq!(summary.counts.logical_agent_total, 1);
    assert!(!summary.freshness.partial);
    assert_eq!(summary.freshness.error, None);

    let catalog = service.catalog(catalog_query()).await.unwrap();
    assert_eq!(catalog.page.rows.len(), 1);
    let row = catalog.page.rows[0].clone();
    let intent = row.intent.clone().unwrap();
    assert!(!intent.chars().any(char::is_control));
    assert_eq!(
        intent,
        "Refactor the widget and update the tests  please thanks"
    );

    let detail = service
        .detail(FleetDetailRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical_key: row.logical_key.clone(),
        })
        .await
        .unwrap();
    assert_eq!(detail.detail.summary.intent, Some(intent));
}

/// One malformed owner-produced display value must not erase the host's
/// whole presentable set: the bad row drops out, every other row is still
/// served, and the snapshot says so through `partial` and a safe reason.
#[tokio::test]
async fn one_unprojectable_row_does_not_erase_the_presentable_set() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    seed_agent(&projects, &recent_timestamp(3), "alpha", "alpha output");
    seed_agent(&projects, &recent_timestamp(2), "beta", "beta output");
    // `agent_label` is byte-bounded but not control-character normalized,
    // so this name still fails owner-side label validation.
    seed_agent(&projects, &recent_timestamp(1), "bad\nname", "gamma");
    let service = build_service(&home, &projects);

    let summary = service.summary().await.unwrap();
    assert_eq!(summary.counts.logical_agent_total, 2);
    assert!(summary.freshness.partial);
    assert_eq!(
        summary.freshness.error.as_deref(),
        Some("unresolved rows: 1 (validation)")
    );

    let catalog = service.catalog(catalog_query()).await.unwrap();
    let labels = catalog
        .page
        .rows
        .iter()
        .map(|row| row.labels.agent_label.clone().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(labels, vec!["alpha".to_string(), "beta".to_string()]);
}

#[tokio::test]
async fn summary_counts_are_independent_of_catalog_page_size() {
    let (_temp, service) = seed_home();
    let summary = service.summary().await.unwrap();
    assert_eq!(summary.counts.logical_agent_total, 2);

    let first = service
        .catalog(FleetCatalogQueryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: None,
            limit: Some(1),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: true,
        })
        .await
        .unwrap();
    assert_eq!(first.page.rows.len(), 1);
    assert_eq!(first.counts.logical_agent_total, 2);
    assert!(first.page.has_more);
    let second = service
        .catalog(FleetCatalogQueryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: first.page.next_cursor,
            limit: Some(1),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: true,
        })
        .await
        .unwrap();
    assert_eq!(second.page.rows.len(), 1);
    assert_ne!(
        first.page.rows[0].logical_key,
        second.page.rows[0].logical_key
    );
}

#[tokio::test]
async fn batch_lookup_reads_followed_id_outside_first_page() {
    let (_temp, service) = seed_home();
    let page = service
        .catalog(FleetCatalogQueryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: None,
            limit: Some(1),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: true,
        })
        .await
        .unwrap();
    let all = service
        .catalog(FleetCatalogQueryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: None,
            limit: Some(10),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: true,
        })
        .await
        .unwrap();
    let outside_first_page = all
        .page
        .rows
        .iter()
        .find(|row| row.logical_key != page.page.rows[0].logical_key)
        .unwrap()
        .logical_key
        .clone();
    let batch = service
        .batch_lookup(FleetLogicalBatchRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical_keys: vec![outside_first_page.clone()],
        })
        .await
        .unwrap();
    assert_eq!(batch.entries.len(), 1);
    assert_eq!(
        batch.entries[0].summary.as_ref().unwrap().logical_key,
        outside_first_page
    );
}

#[tokio::test]
async fn detail_and_content_reads_use_opaque_handles() {
    let (_temp, service) = seed_home();
    let all = service
        .catalog(FleetCatalogQueryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: None,
            limit: Some(10),
            project_ids: Vec::new(),
            query: Some("alpha".to_string()),
            status_buckets: Vec::new(),
            include_terminal: true,
        })
        .await
        .unwrap();
    let key = all.page.rows[0].logical_key.clone();
    let detail = service
        .detail(FleetDetailRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical_key: key,
        })
        .await
        .unwrap();
    let value = serde_json::to_value(&detail).unwrap();
    assert_no_paths_or_pids(&value);
    let handle = detail.detail.content_handles[0].clone();
    let revision = handle.revision.clone().unwrap();
    let read = service
        .content(FleetContentReadRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            handle_id: handle.id.clone(),
            row_revision: revision.clone(),
            offset: 0,
            limit: Some(5),
        })
        .await
        .unwrap();
    assert_eq!(read.returned_bytes, 5);
    assert_eq!(BASE64.decode(read.data_base64).unwrap(), b"alpha");
    assert!(read.next_offset.is_some());
    assert!(service
        .content(FleetContentReadRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            handle_id: "../secret".to_string(),
            row_revision: revision.clone(),
            offset: 0,
            limit: Some(5),
        })
        .await
        .is_err());
    assert!(matches!(
        service
            .content(FleetContentReadRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                handle_id: handle.id,
                row_revision: ResourceRevisionWire {
                    revision: revision.revision.saturating_add(1),
                    ..revision
                },
                offset: 0,
                limit: Some(5),
            })
            .await,
        Err(FleetReadError::Stale(_))
    ));
}

#[tokio::test]
async fn project_eligibility_is_bounded_and_path_free() {
    let (_temp, service) = seed_home();
    let response = service
        .project_eligibility(FleetProjectEligibilityRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            project_ids: Vec::new(),
            limit: Some(1),
        })
        .await
        .unwrap();
    assert_eq!(response.projects.len(), 1);
    let value = serde_json::to_value(&response).unwrap();
    assert_no_paths_or_pids(&value);
    assert!(service
        .project_eligibility(FleetProjectEligibilityRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            project_ids: vec![
                "proj".to_string();
                sase_core::FLEET_READ_MAX_PROJECT_IDS + 1
            ],
            limit: None,
        })
        .await
        .is_err());
}
