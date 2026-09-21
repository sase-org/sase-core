use super::super::*;
use super::support::*;
use serde_json::json;

#[test]
fn federation_catalog_normalization_preserves_authoritative_counts_freshness_and_cursors(
) {
    let apollo_done = summary_done('b', "followed-done", 7, 1_700_000_000.0);
    let mac_running = project_resolved_agent_summary(&projection_request(
        logical('c', "running"),
        Some(exact('c', "running", "run-1")),
        8,
        record_running(),
    ))
    .unwrap();
    let apollo_counts = authoritative_counts(9, 9, Some(1_800_000_000.0));
    let mac_counts = authoritative_counts(2, 2, Some(1_700_000_001.0));
    let apollo_snapshot_id = catalog_snapshot_id(
        FleetCatalogScopeWire::Presentation,
        std::slice::from_ref(&apollo_done),
    );
    let mac_snapshot_id = catalog_snapshot_id(
        FleetCatalogScopeWire::Presentation,
        std::slice::from_ref(&mac_running),
    );
    let response = json!({
        "schema_version": 1,
        "operation": "catalog",
        "configured_hosts": 2,
        "hosts": [
            {
                "schema_version": 1,
                "alias": "apollo",
                "provider_ref": "apollo-provider",
                "installation_id": id('b'),
                "endpoint": "https://apollo.example.test",
                "status": "ok",
                "cached": false,
                "age_seconds": null,
                "payload": {
                    "schema_version": 1,
                    "cursor": {
                        "schema_version": 1,
                        "store_generation": "gen-apollo",
                        "sequence": 12
                    },
                    "catalog_scope": "presentation",
                    "catalog_snapshot_id": apollo_snapshot_id.clone(),
                    "counts": apollo_counts,
                    "count_revision": 99,
                    "freshness": {
                        "schema_version": 1,
                        "freshness": "fresh",
                        "partial": true,
                        "refreshed_at_unix": 1_800_000_000.0,
                        "error": "catalog_partial"
                    },
                    "page": {
                        "schema_version": 1,
                        "scope": "presentation",
                        "snapshot_id": apollo_snapshot_id.clone(),
                        "rows": [apollo_done],
                        "limit": 50,
                        "total_matching_rows": 42,
                        "next_cursor": catalog_cursor(
                            FleetCatalogScopeWire::Presentation,
                            &apollo_snapshot_id,
                            50,
                        ),
                        "has_more": true,
                        "state": "ready"
                    }
                },
                "error": null
            },
            {
                "schema_version": 1,
                "alias": "mac",
                "provider_ref": "mac-provider",
                "installation_id": id('c'),
                "endpoint": "https://mac.example.test",
                "status": "ok",
                "cached": true,
                "age_seconds": 2.0,
                "payload": {
                    "schema_version": 1,
                    "cursor": {
                        "schema_version": 1,
                        "store_generation": "gen-mac",
                        "sequence": 3
                    },
                    "catalog_scope": "presentation",
                    "catalog_snapshot_id": mac_snapshot_id.clone(),
                    "counts": mac_counts,
                    "freshness": "fresh",
                    "page": {
                        "schema_version": 1,
                        "scope": "presentation",
                        "snapshot_id": mac_snapshot_id.clone(),
                        "rows": [mac_running],
                        "limit": 20,
                        "total_matching_rows": 25,
                        "next_cursor": catalog_cursor(
                            FleetCatalogScopeWire::Presentation,
                            &mac_snapshot_id,
                            20,
                        ),
                        "has_more": true,
                        "state": "ready"
                    }
                },
                "error": null
            }
        ]
    });

    let normalized = normalize_fleet_federation_response(
        &FleetFederationNormalizeRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            response: response.clone(),
        },
    )
    .unwrap();

    assert_eq!(normalized.operation.as_deref(), Some("catalog"));
    assert!(normalized.partial);
    assert_eq!(normalized.summaries.len(), 2);
    assert_eq!(
        normalized.hosts[0]
            .catalog
            .as_ref()
            .unwrap()
            .next_cursor
            .as_deref(),
        Some(
            catalog_cursor(
                FleetCatalogScopeWire::Presentation,
                &apollo_snapshot_id,
                50,
            )
            .as_str()
        )
    );
    assert_eq!(
        normalized.hosts[1]
            .catalog
            .as_ref()
            .unwrap()
            .next_cursor
            .as_deref(),
        Some(
            catalog_cursor(
                FleetCatalogScopeWire::Presentation,
                &mac_snapshot_id,
                20,
            )
            .as_str()
        )
    );
    assert_eq!(
        normalized.hosts[0].catalog_scope,
        Some(FleetCatalogScopeWire::Presentation)
    );
    assert_eq!(
        normalized.hosts[0].catalog_snapshot_id.as_deref(),
        Some(apollo_snapshot_id.as_str())
    );
    assert_eq!(
        normalized.hosts[0]
            .catalog
            .as_ref()
            .unwrap()
            .snapshot_cursor
            .as_ref()
            .unwrap()
            .store_generation,
        "gen-apollo"
    );
    assert_eq!(normalized.hosts[0].observed_at_unix, Some(1_800_000_000.0));
    assert_eq!(
        normalized.hosts[0]
            .authoritative_counts
            .as_ref()
            .unwrap()
            .running,
        9
    );
    assert!(normalized.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "fleet_host_partial"
            && diagnostic.alias.as_deref() == Some("apollo")
    }));

    let counted = count_focus_and_fleet_from_federation(
        &FocusFleetFederationCountsRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            local_summaries: Vec::new(),
            followed_response: None,
            fleet_response: Some(response),
        },
    )
    .unwrap();
    assert_eq!(counted.fleet.counts.running, 11);
    assert_eq!(counted.fleet.counts.occupied_runner_slots, 11);
    assert!(counted.fleet.partial);
    assert_eq!(counted.fleet.unknown_origins, vec![id('b')]);
}
#[test]
fn federation_catalog_accepts_readable_capability_schema_versions() {
    let mut v1_row = project_resolved_agent_summary(&projection_request(
        logical('b', "v1-caps"),
        Some(exact('b', "v1-caps", "run-1")),
        1,
        record_running(),
    ))
    .unwrap();
    v1_row.schema_version = 1;
    v1_row.capabilities.schema_version = 1;
    let mut v2_row = project_resolved_agent_summary(&projection_request(
        logical('b', "v2-caps"),
        Some(exact('b', "v2-caps", "run-1")),
        2,
        record_running(),
    ))
    .unwrap();
    v2_row.schema_version = 2;
    v2_row.capabilities.schema_version = 2;
    let rows = vec![v1_row.clone(), v2_row.clone()];
    let snapshot_id =
        catalog_snapshot_id(FleetCatalogScopeWire::Presentation, &rows);
    let response = json!({
        "schema_version": 1,
        "operation": "catalog",
        "configured_hosts": 1,
        "hosts": [{
            "schema_version": 1,
            "alias": "apollo",
            "provider_ref": "apollo-provider",
            "installation_id": id('b'),
            "endpoint": "https://apollo.example.test",
            "status": "ok",
            "cached": false,
            "age_seconds": null,
            "payload": {
                "schema_version": 1,
                "cursor": {
                    "schema_version": 1,
                    "store_generation": "gen-apollo",
                    "sequence": 12
                },
                "catalog_scope": "presentation",
                "catalog_snapshot_id": snapshot_id.clone(),
                "counts": authoritative_counts(2, 2, Some(2_000.0)),
                "freshness": "fresh",
                "page": {
                    "schema_version": 1,
                    "scope": "presentation",
                    "snapshot_id": snapshot_id,
                    "rows": rows,
                    "limit": 50,
                    "total_matching_rows": 2,
                    "next_cursor": null,
                    "has_more": false,
                    "state": "finished"
                }
            },
            "error": null
        }]
    });

    let normalized = normalize_fleet_federation_response(
        &FleetFederationNormalizeRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            response: response.clone(),
        },
    )
    .unwrap();

    assert_eq!(normalized.hosts[0].status, "ok");
    assert_eq!(normalized.summaries.len(), 2);
    assert_eq!(normalized.summaries[0].capabilities.schema_version, 1);
    assert_eq!(normalized.summaries[1].capabilities.schema_version, 2);
    assert!(!normalized
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "fleet_envelope_invalid"));

    let counted = count_focus_and_fleet_from_federation(
        &FocusFleetFederationCountsRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            local_summaries: Vec::new(),
            followed_response: None,
            fleet_response: Some(response),
        },
    )
    .unwrap();
    assert_eq!(counted.fleet.counts.running, 2);
    assert!(counted.fleet.unknown_origins.is_empty());
}
#[test]
fn federation_catalog_rejects_unnormalized_readable_capability_content() {
    let mut row = project_resolved_agent_summary(&projection_request(
        logical('b', "bad-caps"),
        Some(exact('b', "bad-caps", "run-1")),
        1,
        record_running(),
    ))
    .unwrap();
    row.schema_version = 1;
    row.capabilities.schema_version = 1;
    row.capabilities.resource =
        vec!["stop".to_string(), "content.read".to_string()];
    let snapshot_id =
        catalog_snapshot_id(FleetCatalogScopeWire::Presentation, &[]);
    let response = json!({
        "schema_version": 1,
        "operation": "catalog",
        "configured_hosts": 1,
        "hosts": [{
            "schema_version": 1,
            "alias": "apollo",
            "provider_ref": "apollo-provider",
            "installation_id": id('b'),
            "endpoint": "https://apollo.example.test",
            "status": "ok",
            "cached": false,
            "age_seconds": null,
            "payload": {
                "schema_version": 1,
                "cursor": {
                    "schema_version": 1,
                    "store_generation": "gen-apollo",
                    "sequence": 12
                },
                "catalog_scope": "presentation",
                "catalog_snapshot_id": snapshot_id.clone(),
                "counts": authoritative_counts(1, 1, Some(2_000.0)),
                "freshness": "fresh",
                "page": {
                    "schema_version": 1,
                    "scope": "presentation",
                    "snapshot_id": snapshot_id,
                    "rows": [row],
                    "limit": 50,
                    "total_matching_rows": 1,
                    "next_cursor": null,
                    "has_more": false,
                    "state": "finished"
                }
            },
            "error": null
        }]
    });

    let normalized = normalize_fleet_federation_response(
        &FleetFederationNormalizeRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            response,
        },
    )
    .unwrap();

    assert_eq!(normalized.hosts[0].status, "invalid");
    assert!(normalized.summaries.is_empty());
    assert!(normalized.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "fleet_envelope_invalid"
            && diagnostic.alias.as_deref() == Some("apollo")
    }));
}
#[test]
fn federation_followed_batch_counts_only_resolved_requested_entries() {
    let local = project_resolved_agent_summary(&projection_request(
        logical('a', "local"),
        Some(exact('a', "local", "run-1")),
        1,
        record_running(),
    ))
    .unwrap();
    let followed_done = summary_done('b', "followed-done", 7, 2_000.0);
    let response = json!({
        "schema_version": 1,
        "operation": "followed_batch",
        "configured_hosts": 1,
        "hosts": [{
            "schema_version": 1,
            "alias": "apollo",
            "provider_ref": "apollo-provider",
            "installation_id": id('b'),
            "endpoint": "https://apollo.example.test",
            "status": "ok",
            "cached": false,
            "age_seconds": null,
            "payload": {
                "schema_version": 1,
                "cursor": {
                    "schema_version": 1,
                    "store_generation": "gen-apollo",
                    "sequence": 12
                },
                "counts": authoritative_counts(9, 9, Some(2_000.0)),
                "freshness": {
                    "schema_version": 1,
                    "freshness": "fresh",
                    "partial": false,
                    "refreshed_at_unix": 2_000.0,
                    "error": null
                },
                "entries": [
                    {
                        "schema_version": 1,
                        "requested_logical_key": followed_done.logical_key,
                        "summary": followed_done
                    },
                    {
                        "schema_version": 1,
                        "requested_logical_key": "missing-logical-key",
                        "summary": null
                    }
                ]
            },
            "error": null
        }]
    });

    let normalized = normalize_fleet_federation_response(
        &FleetFederationNormalizeRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            response: response.clone(),
        },
    )
    .unwrap();
    assert_eq!(
        normalized.hosts[0].unresolved_logical_keys,
        vec!["missing-logical-key".to_string()]
    );
    assert!(normalized.hosts[0].authoritative_counts.is_none());

    let counted = count_focus_and_fleet_from_federation(
        &FocusFleetFederationCountsRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            local_summaries: vec![local],
            followed_response: Some(response),
            fleet_response: None,
        },
    )
    .unwrap();

    assert_eq!(counted.focus.counts.running, 1);
    assert_eq!(counted.focus.host_counts[0].counts.running, 0);
    assert!(!counted.focus.partial);
}
#[test]
fn federation_malformed_host_degrades_without_losing_healthy_hosts() {
    let healthy = project_resolved_agent_summary(&projection_request(
        logical('b', "healthy"),
        Some(exact('b', "healthy", "run-1")),
        2,
        record_running(),
    ))
    .unwrap();
    let healthy_snapshot_id = catalog_snapshot_id(
        FleetCatalogScopeWire::Presentation,
        std::slice::from_ref(&healthy),
    );
    let response = json!({
        "schema_version": 1,
        "operation": "catalog",
        "configured_hosts": 3,
        "hosts": [
            {
                "schema_version": 1,
                "alias": "apollo",
                "provider_ref": "apollo-provider",
                "installation_id": id('b'),
                "endpoint": "https://apollo.example.test",
                "status": "ok",
                "cached": false,
                "age_seconds": null,
                "payload": {
                    "schema_version": 1,
                    "cursor": {
                        "schema_version": 1,
                        "store_generation": "gen-apollo",
                        "sequence": 12
                    },
                    "catalog_scope": "presentation",
                    "catalog_snapshot_id": healthy_snapshot_id.clone(),
                    "counts": authoritative_counts(1, 1, Some(2_000.0)),
                    "freshness": "fresh",
                    "page": {
                        "schema_version": 1,
                        "scope": "presentation",
                        "snapshot_id": healthy_snapshot_id,
                        "rows": [healthy],
                        "limit": 50,
                        "total_matching_rows": 1,
                        "next_cursor": null,
                        "has_more": false,
                        "state": "finished"
                    }
                },
                "error": null
            },
            {
                "schema_version": 1,
                "alias": "bad",
                "provider_ref": "bad-provider",
                "installation_id": id('c'),
                "endpoint": "https://bad.example.test",
                "status": "ok",
                "cached": false,
                "age_seconds": null,
                "payload": {
                    "schema_version": 1,
                    "freshness": "fresh",
                    "page": {
                        "schema_version": 1,
                        "rows": [{"schema_version": 1, "logical_key": "bad"}],
                        "limit": 50,
                        "total_matching_rows": 1,
                        "next_cursor": null,
                        "has_more": false
                    }
                },
                "error": null
            },
            {
                "schema_version": 1,
                "alias": "offline",
                "provider_ref": "offline-provider",
                "installation_id": id('d'),
                "endpoint": "https://offline.example.test",
                "status": "timeout",
                "cached": false,
                "age_seconds": null,
                "payload": null,
                "error": {
                    "schema_version": 1,
                    "code": "timeout",
                    "message": "host did not reply before deadline",
                    "target": "offline",
                    "details": null
                }
            }
        ]
    });

    let normalized = normalize_fleet_federation_response(
        &FleetFederationNormalizeRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            response: response.clone(),
        },
    )
    .unwrap();

    assert!(normalized.partial);
    assert_eq!(normalized.summaries.len(), 1);
    assert_eq!(normalized.hosts[1].status, "invalid");
    assert!(normalized.hosts[1].partial);
    assert!(normalized.hosts[2].partial);
    assert!(normalized.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "fleet_envelope_invalid"
            && diagnostic.alias.as_deref() == Some("bad")
    }));
    assert!(normalized.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "timeout"
            && diagnostic.alias.as_deref() == Some("offline")
    }));

    let counted = count_focus_and_fleet_from_federation(
        &FocusFleetFederationCountsRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            local_summaries: Vec::new(),
            followed_response: None,
            fleet_response: Some(response),
        },
    )
    .unwrap();
    assert_eq!(counted.fleet.counts.running, 1);
    assert!(counted.fleet.partial);
    assert_eq!(counted.fleet.unknown_origins, vec![id('c'), id('d')]);
}
#[test]
fn federation_malformed_external_catalog_cursor_rejects_host_wire() {
    let row = project_resolved_agent_summary(&projection_request(
        logical('b', "cursor"),
        Some(exact('b', "cursor", "run-1")),
        2,
        record_running(),
    ))
    .unwrap();
    let snapshot_id = catalog_snapshot_id(
        FleetCatalogScopeWire::Presentation,
        std::slice::from_ref(&row),
    );
    let response = json!({
        "schema_version": 1,
        "operation": "catalog",
        "configured_hosts": 1,
        "hosts": [{
            "schema_version": 1,
            "alias": "apollo",
            "provider_ref": "apollo-provider",
            "installation_id": id('b'),
            "endpoint": "https://apollo.example.test",
            "status": "ok",
            "cached": false,
            "age_seconds": null,
            "payload": {
                "schema_version": 1,
                "cursor": {
                    "schema_version": 1,
                    "store_generation": "gen-apollo",
                    "sequence": 12
                },
                "counts": authoritative_counts(1, 1, Some(2_000.0)),
                "freshness": "fresh",
                "page": {
                    "schema_version": 1,
                    "scope": "presentation",
                    "snapshot_id": snapshot_id,
                    "rows": [row],
                    "limit": 50,
                    "total_matching_rows": 2,
                    "next_cursor": "../other-host",
                    "has_more": true,
                    "state": "ready"
                }
            },
            "error": null
        }]
    });

    let normalized = normalize_fleet_federation_response(
        &FleetFederationNormalizeRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            response,
        },
    )
    .unwrap();
    assert_eq!(normalized.hosts[0].status, "invalid");
    assert!(normalized.hosts[0].catalog.is_none());
    assert!(normalized.hosts[0].partial);
    assert!(normalized.summaries.is_empty());
    assert!(normalized.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "fleet_envelope_invalid"
            && diagnostic.alias.as_deref() == Some("apollo")
    }));
}
#[test]
fn federation_typed_catalog_resync_preserves_healthy_hosts() {
    let row = project_resolved_agent_summary(&projection_request(
        logical('c', "healthy"),
        Some(exact('c', "healthy", "run-1")),
        3,
        record_running(),
    ))
    .unwrap();
    let restart_snapshot_id =
        catalog_snapshot_id(FleetCatalogScopeWire::History, &[]);
    let healthy_snapshot_id = catalog_snapshot_id(
        FleetCatalogScopeWire::Presentation,
        std::slice::from_ref(&row),
    );
    let response = json!({
        "schema_version": 1,
        "operation": "catalog",
        "configured_hosts": 2,
        "hosts": [
            {
                "schema_version": 1,
                "alias": "history",
                "provider_ref": "history-provider",
                "installation_id": id('b'),
                "endpoint": "https://history.example.test",
                "status": "ok",
                "cached": false,
                "age_seconds": null,
                "payload": {
                    "schema_version": 1,
                    "cursor": {
                        "schema_version": 1,
                        "store_generation": "gen-history",
                        "sequence": 8
                    },
                    "catalog_scope": "history",
                    "catalog_snapshot_id": restart_snapshot_id.clone(),
                    "counts": authoritative_counts(0, 0, None),
                    "freshness": "fresh",
                    "page": {
                        "schema_version": 1,
                        "scope": "history",
                        "snapshot_id": restart_snapshot_id.clone(),
                        "rows": [],
                        "limit": 50,
                        "total_matching_rows": 0,
                        "next_cursor": null,
                        "has_more": false,
                        "state": "resync_required",
                        "reset_reason": "snapshot_mismatch"
                    }
                },
                "error": null
            },
            {
                "schema_version": 1,
                "alias": "healthy",
                "provider_ref": "healthy-provider",
                "installation_id": id('c'),
                "endpoint": "https://healthy.example.test",
                "status": "ok",
                "cached": false,
                "age_seconds": null,
                "payload": {
                    "schema_version": 1,
                    "cursor": {
                        "schema_version": 1,
                        "store_generation": "gen-healthy",
                        "sequence": 9
                    },
                    "catalog_scope": "presentation",
                    "catalog_snapshot_id": healthy_snapshot_id.clone(),
                    "counts": authoritative_counts(1, 1, Some(1_000.0)),
                    "freshness": "fresh",
                    "page": {
                        "schema_version": 1,
                        "scope": "presentation",
                        "snapshot_id": healthy_snapshot_id,
                        "rows": [row],
                        "limit": 50,
                        "total_matching_rows": 1,
                        "next_cursor": null,
                        "has_more": false,
                        "state": "finished"
                    }
                },
                "error": null
            }
        ]
    });

    let normalized = normalize_fleet_federation_response(
        &FleetFederationNormalizeRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            response,
        },
    )
    .unwrap();

    assert!(normalized.partial);
    assert_eq!(normalized.summaries.len(), 1);
    let catalog = normalized.hosts[0].catalog.as_ref().unwrap();
    assert_eq!(
        catalog.state,
        FleetCatalogContinuationStateWire::ResyncRequired
    );
    assert_eq!(
        catalog.reset_reason,
        Some(FleetCatalogResetReasonWire::SnapshotMismatch)
    );
    assert_eq!(normalized.hosts[1].status, "ok");
}
