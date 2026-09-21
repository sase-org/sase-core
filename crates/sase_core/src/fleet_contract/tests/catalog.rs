use super::super::*;
use super::support::*;

#[test]
fn catalog_query_is_bounded_and_pages_deterministically() {
    let alpha = project_resolved_agent_summary(&projection_request(
        logical('a', "alpha"),
        Some(exact('a', "alpha", "run-1")),
        1,
        record_running(),
    ))
    .unwrap();
    let mut beta_record = record_running();
    beta_record.agent_meta.as_mut().unwrap().name =
        Some("athena.beta".to_string());
    let beta = project_resolved_agent_summary(&projection_request(
        logical('a', "beta"),
        Some(exact('a', "beta", "run-1")),
        2,
        beta_record,
    ))
    .unwrap();
    let query = FleetCatalogQueryWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        scope: FleetCatalogScopeWire::Presentation,
        snapshot_id: None,
        cursor: None,
        limit: Some(1),
        project_ids: Vec::new(),
        query: None,
        status_buckets: Vec::new(),
        include_terminal: true,
    };
    let first =
        select_fleet_catalog_page(&query, &[beta.clone(), alpha.clone()])
            .unwrap();
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.total_matching_rows, 2);
    assert!(first.has_more);
    assert_eq!(first.scope, FleetCatalogScopeWire::Presentation);
    assert!(first
        .next_cursor
        .as_deref()
        .is_some_and(|cursor| cursor.starts_with("catcur_v1:p:")));
    let second = select_fleet_catalog_page(
        &FleetCatalogQueryWire {
            cursor: first.next_cursor,
            ..query
        },
        &[alpha, beta],
    )
    .unwrap();
    assert_eq!(second.rows.len(), 1);
    assert_ne!(first.rows[0].logical_key, second.rows[0].logical_key);

    assert!(validate_fleet_catalog_query(&FleetCatalogQueryWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        scope: FleetCatalogScopeWire::Presentation,
        snapshot_id: None,
        cursor: Some("../bad".to_string()),
        limit: Some(1),
        project_ids: Vec::new(),
        query: None,
        status_buckets: Vec::new(),
        include_terminal: false,
    })
    .is_err());
    assert!(validate_fleet_catalog_query(&FleetCatalogQueryWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        scope: FleetCatalogScopeWire::Presentation,
        snapshot_id: None,
        cursor: None,
        limit: Some(FLEET_READ_MAX_PAGE_ROWS + 1),
        project_ids: Vec::new(),
        query: None,
        status_buckets: Vec::new(),
        include_terminal: false,
    })
    .is_err());
}
#[test]
fn catalog_snapshot_identity_is_scope_and_stable_content_scoped() {
    let alpha = project_resolved_agent_summary(&projection_request(
        logical('a', "alpha"),
        Some(exact('a', "alpha", "run-1")),
        1,
        record_running(),
    ))
    .unwrap();
    let mut alpha_observed_later = alpha.clone();
    alpha_observed_later.observed_at_unix += 60.0;
    alpha_observed_later.freshness = ObservationFreshnessWire::Stale;
    let same_content_id = catalog_snapshot_id(
        FleetCatalogScopeWire::Presentation,
        std::slice::from_ref(&alpha),
    );
    assert_eq!(
        same_content_id,
        catalog_snapshot_id(
            FleetCatalogScopeWire::Presentation,
            std::slice::from_ref(&alpha_observed_later),
        )
    );
    assert_ne!(
        same_content_id,
        catalog_snapshot_id(
            FleetCatalogScopeWire::History,
            std::slice::from_ref(&alpha),
        )
    );
    let empty = catalog_snapshot_id(FleetCatalogScopeWire::Presentation, &[]);
    assert_eq!(
        empty,
        catalog_snapshot_id(FleetCatalogScopeWire::Presentation, &[])
    );

    let mut beta_record = record_running();
    beta_record.agent_meta.as_mut().unwrap().name =
        Some("athena.beta".to_string());
    let beta = project_resolved_agent_summary(&projection_request(
        logical('a', "beta"),
        Some(exact('a', "beta", "run-1")),
        1,
        beta_record,
    ))
    .unwrap();
    assert_ne!(
        same_content_id,
        catalog_snapshot_id(
            FleetCatalogScopeWire::Presentation,
            std::slice::from_ref(&beta),
        )
    );

    let mut revised_alpha = alpha.clone();
    revised_alpha.row_revision.revision += 1;
    assert_ne!(
        same_content_id,
        catalog_snapshot_id(
            FleetCatalogScopeWire::Presentation,
            std::slice::from_ref(&revised_alpha),
        )
    );
}
#[test]
fn catalog_cursor_scope_or_snapshot_mismatch_returns_restart_page() {
    let alpha = project_resolved_agent_summary(&projection_request(
        logical('a', "alpha"),
        Some(exact('a', "alpha", "run-1")),
        1,
        record_running(),
    ))
    .unwrap();
    let beta = project_resolved_agent_summary(&projection_request(
        logical('a', "beta"),
        Some(exact('a', "beta", "run-1")),
        2,
        record_running(),
    ))
    .unwrap();
    let gamma = project_resolved_agent_summary(&projection_request(
        logical('a', "gamma"),
        Some(exact('a', "gamma", "run-1")),
        3,
        record_running(),
    ))
    .unwrap();
    let query = FleetCatalogQueryWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        scope: FleetCatalogScopeWire::Presentation,
        snapshot_id: None,
        cursor: None,
        limit: Some(1),
        project_ids: Vec::new(),
        query: None,
        status_buckets: Vec::new(),
        include_terminal: true,
    };
    let first =
        select_fleet_catalog_page(&query, &[alpha.clone(), beta]).unwrap();
    let stale = select_fleet_catalog_page(
        &FleetCatalogQueryWire {
            cursor: first.next_cursor.clone(),
            ..query.clone()
        },
        &[alpha.clone(), gamma.clone()],
    )
    .unwrap();
    assert!(stale.rows.is_empty());
    assert_eq!(
        stale.state,
        FleetCatalogContinuationStateWire::ResyncRequired
    );
    assert_eq!(
        stale.reset_reason,
        Some(FleetCatalogResetReasonWire::SnapshotMismatch)
    );
    assert!(stale.next_cursor.is_none());

    let cross_scope = select_fleet_catalog_page(
        &FleetCatalogQueryWire {
            scope: FleetCatalogScopeWire::History,
            cursor: first.next_cursor,
            ..query
        },
        &[alpha, gamma],
    )
    .unwrap();
    assert_eq!(
        cross_scope.reset_reason,
        Some(FleetCatalogResetReasonWire::ScopeMismatch)
    );
    assert!(validate_fleet_catalog_cursor("off:1").is_err());
}
#[test]
fn catalog_accumulation_uses_generations_and_snapshot_equality() {
    let alpha = project_resolved_agent_summary(&projection_request(
        logical('a', "alpha"),
        Some(exact('a', "alpha", "run-1")),
        1,
        record_running(),
    ))
    .unwrap();
    let beta = project_resolved_agent_summary(&projection_request(
        logical('a', "beta"),
        Some(exact('a', "beta", "run-1")),
        2,
        record_running(),
    ))
    .unwrap();
    let rows = vec![alpha.clone(), beta.clone()];
    let query = FleetCatalogQueryWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        scope: FleetCatalogScopeWire::Presentation,
        snapshot_id: None,
        cursor: None,
        limit: Some(1),
        project_ids: Vec::new(),
        query: None,
        status_buckets: Vec::new(),
        include_terminal: true,
    };
    let first_page = select_fleet_catalog_page(&query, &rows).unwrap();
    let first_cursor = first_page.next_cursor.clone();
    let first =
        accumulate_fleet_catalog_page(&FleetCatalogAccumulationRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            current: None,
            request_generation: 2,
            requested_scope: FleetCatalogScopeWire::Presentation,
            requested_snapshot_id: None,
            requested_cursor: None,
            incoming: catalog_response(first_page, &rows),
        })
        .unwrap();
    assert_eq!(first.action, FleetCatalogAccumulationActionWire::Replaced);
    assert_eq!(first.state.rows.len(), 1);

    let older =
        accumulate_fleet_catalog_page(&FleetCatalogAccumulationRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            current: Some(first.state.clone()),
            request_generation: 1,
            requested_scope: FleetCatalogScopeWire::Presentation,
            requested_snapshot_id: first.state.snapshot_id.clone(),
            requested_cursor: None,
            incoming: catalog_response(
                select_fleet_catalog_page(&query, &[]).unwrap(),
                &[],
            ),
        })
        .unwrap();
    assert_eq!(
        older.action,
        FleetCatalogAccumulationActionWire::IgnoredOlderRequest
    );
    assert_eq!(older.state.rows, first.state.rows);

    let second_page = select_fleet_catalog_page(
        &FleetCatalogQueryWire {
            cursor: first_cursor.clone(),
            ..query.clone()
        },
        &rows,
    )
    .unwrap();
    let merged =
        accumulate_fleet_catalog_page(&FleetCatalogAccumulationRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            current: Some(first.state.clone()),
            request_generation: 2,
            requested_scope: FleetCatalogScopeWire::Presentation,
            requested_snapshot_id: first.state.snapshot_id.clone(),
            requested_cursor: first_cursor,
            incoming: catalog_response(second_page, &rows),
        })
        .unwrap();
    assert_eq!(merged.action, FleetCatalogAccumulationActionWire::Merged);
    assert_eq!(merged.state.rows.len(), 2);

    let mut revised_alpha = alpha.clone();
    revised_alpha.row_revision.revision += 10;
    let duplicate_page = FleetCatalogPageSelectionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        scope: FleetCatalogScopeWire::Presentation,
        snapshot_id: merged.state.snapshot_id.clone().unwrap(),
        rows: vec![revised_alpha.clone()],
        limit: 1,
        total_matching_rows: 2,
        next_cursor: None,
        has_more: false,
        state: FleetCatalogContinuationStateWire::Finished,
        reset_reason: None,
    };
    let revised =
        accumulate_fleet_catalog_page(&FleetCatalogAccumulationRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            current: Some(merged.state.clone()),
            request_generation: 2,
            requested_scope: FleetCatalogScopeWire::Presentation,
            requested_snapshot_id: merged.state.snapshot_id.clone(),
            requested_cursor: Some(catalog_cursor(
                FleetCatalogScopeWire::Presentation,
                merged.state.snapshot_id.as_deref().unwrap(),
                1,
            )),
            incoming: catalog_response(duplicate_page, &rows),
        })
        .unwrap();
    assert_eq!(
        revised
            .state
            .rows
            .iter()
            .find(|row| row.logical_key == revised_alpha.logical_key)
            .unwrap()
            .row_revision
            .revision,
        revised_alpha.row_revision.revision
    );

    let replacement =
        accumulate_fleet_catalog_page(&FleetCatalogAccumulationRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            current: Some(revised.state),
            request_generation: 3,
            requested_scope: FleetCatalogScopeWire::Presentation,
            requested_snapshot_id: None,
            requested_cursor: None,
            incoming: catalog_response(
                select_fleet_catalog_page(&query, &[]).unwrap(),
                &[],
            ),
        })
        .unwrap();
    assert_eq!(
        replacement.action,
        FleetCatalogAccumulationActionWire::Replaced
    );
    assert!(replacement.state.rows.is_empty());
    assert_eq!(replacement.state.total_matching_rows, 0);
    assert!(replacement.state.next_cursor.is_none());
}
