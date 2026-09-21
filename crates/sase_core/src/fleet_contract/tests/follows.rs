use super::super::*;
use super::support::*;

#[test]
fn follow_reconciliation_promotes_singleton_to_family_identity() {
    let from = singleton('a', "worker");
    let to = logical('a', "worker");
    let record = follow_record(
        from.clone(),
        FollowCreatedByWire::Explicit,
        FollowStateWire::Active,
        10.0,
    );

    let reconciled =
        reconcile_follow_records(&FollowReconciliationRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            records: vec![record],
            tombstones: Vec::new(),
            promotions: vec![FollowFamilyPromotionWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                from,
                to: to.clone(),
            }],
            activations: Vec::new(),
            now_unix: 12.0,
        })
        .unwrap();

    assert!(reconciled.changed);
    assert_eq!(reconciled.records.len(), 1);
    assert_eq!(reconciled.records[0].logical_locator, to);
    assert_eq!(
        reconciled.records[0].logical_key,
        logical_key_unchecked(&to)
    );
    assert_eq!(
        reconciled.records[0].created_by,
        FollowCreatedByWire::Explicit
    );
    assert_eq!(reconciled.records[0].updated_at_unix, 12.0);
}
#[test]
fn follow_tombstones_suppress_dispatch_recreation_and_activation() {
    let locator = logical('a', "worker");
    let record = follow_record(
        locator.clone(),
        FollowCreatedByWire::Dispatch,
        FollowStateWire::Pending,
        10.0,
    );
    let dispatch_tombstone = tombstone(locator.clone(), 11.0);

    let reconciled =
        reconcile_follow_records(&FollowReconciliationRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            records: vec![record],
            tombstones: vec![dispatch_tombstone],
            promotions: Vec::new(),
            activations: vec![FollowActivationWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                logical_locator: locator,
                operation_key: Some(operation_key("op-1")),
                activated_at_unix: 12.0,
            }],
            now_unix: 12.0,
        })
        .unwrap();

    assert!(reconciled.records.is_empty());
    assert_eq!(reconciled.tombstones.len(), 1);
    assert!(reconciled.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "follow_tombstone_blocked"
            || diagnostic.code == "follow_activation_tombstoned"
    }));

    let singleton = singleton('a', "worker");
    let family = logical('a', "worker");
    let resurrected =
        reconcile_follow_records(&FollowReconciliationRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            records: vec![follow_record(
                singleton.clone(),
                FollowCreatedByWire::Dispatch,
                FollowStateWire::Pending,
                20.0,
            )],
            tombstones: vec![tombstone(singleton.clone(), 21.0)],
            promotions: vec![FollowFamilyPromotionWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                from: singleton,
                to: family,
            }],
            activations: Vec::new(),
            now_unix: 22.0,
        })
        .unwrap();
    assert!(resurrected.records.is_empty());
    assert!(resurrected.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "follow_promotion_source_tombstoned"
    }));
}
#[test]
fn focus_and_fleet_counts_stay_separate_and_propagate_unknown_hosts() {
    let local = project_resolved_agent_summary(&projection_request(
        logical('a', "local"),
        Some(exact('a', "local", "run-1")),
        1,
        record_running(),
    ))
    .unwrap();
    let remote_followed = project_resolved_agent_summary(&projection_request(
        logical('b', "followed"),
        Some(exact('b', "followed", "run-1")),
        2,
        record_running(),
    ))
    .unwrap();
    let remote_unfollowed =
        project_resolved_agent_summary(&projection_request(
            logical('c', "unfollowed"),
            Some(exact('c', "unfollowed", "run-1")),
            3,
            record_running(),
        ))
        .unwrap();

    let counted = count_focus_and_fleet(&FocusFleetCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        local_summaries: vec![local],
        followed_remote_hosts: vec![FleetHostCountInputWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            origin: origin('b'),
            summaries: vec![remote_followed.clone()],
            observed_at_unix: Some(2000.0),
            freshness: ObservationFreshnessWire::Fresh,
            authoritative_counts: None,
            partial: false,
        }],
        fleet_hosts: vec![
            FleetHostCountInputWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin: origin('b'),
                summaries: vec![remote_followed],
                observed_at_unix: Some(2000.0),
                freshness: ObservationFreshnessWire::Fresh,
                authoritative_counts: None,
                partial: false,
            },
            FleetHostCountInputWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin: origin('c'),
                summaries: vec![remote_unfollowed],
                observed_at_unix: Some(1500.0),
                freshness: ObservationFreshnessWire::Aging,
                authoritative_counts: None,
                partial: false,
            },
            FleetHostCountInputWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin: origin('d'),
                summaries: Vec::new(),
                observed_at_unix: None,
                freshness: ObservationFreshnessWire::Unknown,
                authoritative_counts: None,
                partial: false,
            },
        ],
    })
    .unwrap();

    assert_eq!(counted.focus.counts.running, 2);
    assert_eq!(counted.fleet.counts.running, 2);
    assert!(!counted.focus.partial);
    assert!(counted.fleet.partial);
    assert_eq!(counted.fleet.unknown_origins, vec![id('d')]);
    assert_eq!(counted.fleet.host_counts.len(), 3);
    assert_eq!(counted.fleet.observed_at_unix_max, Some(2000.0));
}
