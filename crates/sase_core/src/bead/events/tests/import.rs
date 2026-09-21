//! Import tests, covering `super::super::import`: ref/link ingestion
//! and idempotent replay.

use crate::artifact_link::{
    ArtifactLinkOriginWire, BeadLinkDirectionWire, BeadLinkWire,
};

use super::super::*;
use super::support::*;

fn reference_event(
    event_id: &str,
    operation: BeadEventOperationWire,
    reference: &str,
) -> BeadEventRecordWire {
    let payload = match operation {
        BeadEventOperationWire::ReferenceAdded => {
            BeadEventPayloadWire::ReferenceAdded {
                reference: reference.to_string(),
            }
        }
        BeadEventOperationWire::ReferenceRemoved => {
            BeadEventPayloadWire::ReferenceRemoved {
                reference: reference.to_string(),
            }
        }
        _ => panic!("reference_event requires a reference operation"),
    };
    BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: event_id.to_string(),
        timestamp: "2026-01-01T00:01:00Z".to_string(),
        actor: "owner@example.com".to_string(),
        operation,
        issue_id: "sase-1".to_string(),
        payload,
    }
}

#[test]
fn refs_import_as_individual_events_and_replay_idempotently() {
    let issue = issue_with_refs(vec![
        "research:202607/report.md".to_string(),
        "bead:sase-bb.1".to_string(),
    ]);
    let mut streams =
        import_issues_to_event_streams(std::slice::from_ref(&issue)).unwrap();
    assert_eq!(
        streams[0]
            .events
            .iter()
            .map(|event| event.operation)
            .collect::<Vec<_>>(),
        vec![
            BeadEventOperationWire::IssueCreated,
            BeadEventOperationWire::ReferenceAdded,
            BeadEventOperationWire::ReferenceAdded,
        ]
    );
    let BeadEventPayloadWire::IssueCreated { issue: created } =
        &streams[0].events[0].payload
    else {
        panic!("first event should create the issue");
    };
    assert!(created.refs.is_empty());

    streams[0].events.extend([
        reference_event(
            "duplicate-add",
            BeadEventOperationWire::ReferenceAdded,
            "research:202607/report.md",
        ),
        reference_event(
            "absent-remove",
            BeadEventOperationWire::ReferenceRemoved,
            "bead:sase-missing",
        ),
    ]);
    assert_eq!(reduce_event_streams(&streams).unwrap(), vec![issue]);

    streams[0].events.push(reference_event(
        "real-remove",
        BeadEventOperationWire::ReferenceRemoved,
        "research:202607/report.md",
    ));
    assert_eq!(
        reduce_event_streams(&streams).unwrap()[0].refs,
        vec!["bead:sase-bb.1"]
    );
}

fn link_event(
    event_id: &str,
    operation: BeadEventOperationWire,
    target_ref: &str,
    relation: &str,
    description: &str,
) -> BeadEventRecordWire {
    let payload = match operation {
        BeadEventOperationWire::LinkAdded => BeadEventPayloadWire::LinkAdded {
            target_ref: target_ref.to_string(),
            relation: relation.to_string(),
            description: description.to_string(),
            origin: ArtifactLinkOriginWire::Manual,
            direction: BeadLinkDirectionWire::Out,
            uses: 1,
            operation_id: None,
        },
        BeadEventOperationWire::LinkRemoved => {
            BeadEventPayloadWire::LinkRemoved {
                target_ref: target_ref.to_string(),
                relation: relation.to_string(),
                direction: BeadLinkDirectionWire::Out,
                operation_id: None,
            }
        }
        _ => panic!("link_event requires a link operation"),
    };
    BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: event_id.to_string(),
        timestamp: "2026-01-01T00:01:00Z".to_string(),
        actor: "owner@example.com".to_string(),
        operation,
        issue_id: "sase-1".to_string(),
        payload,
    }
}

#[test]
fn links_import_round_trip_and_ignore_unknown_historical_payloads() {
    let mut issue = issue_with_refs(Vec::new());
    issue.links = vec![BeadLinkWire {
        target_ref: "bead:sase-ct".to_string(),
        relation: "related".to_string(),
        description: "shares the ACE-TUI flake root cause".to_string(),
        origin: ArtifactLinkOriginWire::Manual,
        direction: BeadLinkDirectionWire::Out,
        uses: 1,
    }];
    let mut streams =
        import_issues_to_event_streams(std::slice::from_ref(&issue)).unwrap();
    assert_eq!(
        streams[0]
            .events
            .iter()
            .map(|event| event.operation)
            .collect::<Vec<_>>(),
        vec![
            BeadEventOperationWire::IssueCreated,
            BeadEventOperationWire::LinkAdded,
        ]
    );
    let BeadEventPayloadWire::IssueCreated { issue: created } =
        &streams[0].events[0].payload
    else {
        panic!("first event should create the issue");
    };
    assert!(created.links.is_empty());
    assert_eq!(reduce_event_streams(&streams).unwrap(), vec![issue.clone()]);

    streams[0].events.push(link_event(
        "rewrite",
        BeadEventOperationWire::LinkAdded,
        "bead:sase-ct",
        "related",
        "updated why",
    ));
    let reduced = reduce_event_streams(&streams).unwrap();
    assert_eq!(reduced[0].links.len(), 1);
    assert_eq!(reduced[0].links[0].description, "updated why");

    streams[0].events.push(link_event(
        "remove",
        BeadEventOperationWire::LinkRemoved,
        "bead:sase-ct",
        "related",
        "",
    ));
    assert!(reduce_event_streams(&streams).unwrap()[0].links.is_empty());

    let reserved = link_event(
        "blocks",
        BeadEventOperationWire::LinkAdded,
        "bead:sase-ct",
        "blocks",
        "nope",
    );
    assert!(reserved
        .validate()
        .unwrap_err()
        .message
        .contains("sase bead dep"));
}

#[test]
fn link_added_provenance_tracks_rewrite_removal_and_readd() {
    let issue = issue_with_refs(Vec::new());
    let mut streams =
        import_issues_to_event_streams(std::slice::from_ref(&issue)).unwrap();

    let mut added = link_event(
        "add",
        BeadEventOperationWire::LinkAdded,
        "plan:202608/a.md",
        "implements",
        "first why",
    );
    added.timestamp = "2026-01-02T00:00:00Z".to_string();
    added.actor = "alice".to_string();
    streams[0].events.push(added);

    let (issues, provenance) =
        reduce_event_streams_with_link_provenance(&streams).unwrap();
    assert_eq!(issues[0].links.len(), 1);
    assert_eq!(provenance.len(), 1);
    let row = provenance.values().next().unwrap();
    assert_eq!(row.actor, "alice");
    assert_eq!(row.timestamp, "2026-01-02T00:00:00Z");
    assert_eq!(row.description, "first why");

    let mut rewrite = link_event(
        "rewrite",
        BeadEventOperationWire::LinkAdded,
        "plan:202608/a.md",
        "implements",
        "second why",
    );
    rewrite.timestamp = "2026-01-03T00:00:00Z".to_string();
    rewrite.actor = "bob".to_string();
    streams[0].events.push(rewrite);

    let (_issues, provenance) =
        reduce_event_streams_with_link_provenance(&streams).unwrap();
    assert_eq!(provenance.len(), 1);
    let row = provenance.values().next().unwrap();
    assert_eq!(row.actor, "bob");
    assert_eq!(row.timestamp, "2026-01-03T00:00:00Z");
    assert_eq!(row.description, "second why");

    streams[0].events.push(link_event(
        "remove",
        BeadEventOperationWire::LinkRemoved,
        "plan:202608/a.md",
        "implements",
        "",
    ));
    let (_issues, provenance) =
        reduce_event_streams_with_link_provenance(&streams).unwrap();
    assert!(provenance.is_empty());

    let mut readd = link_event(
        "readd",
        BeadEventOperationWire::LinkAdded,
        "plan:202608/a.md",
        "implements",
        "third why",
    );
    readd.timestamp = "2026-01-04T00:00:00Z".to_string();
    readd.actor = "carol".to_string();
    streams[0].events.push(readd);
    let (_issues, provenance) =
        reduce_event_streams_with_link_provenance(&streams).unwrap();
    let row = provenance.values().next().unwrap();
    assert_eq!(row.actor, "carol");
    assert_eq!(row.timestamp, "2026-01-04T00:00:00Z");
    assert_eq!(row.description, "third why");
}

#[test]
fn inbound_link_provenance_projects_bead_as_target() {
    let issue = issue_with_refs(Vec::new());
    let mut streams =
        import_issues_to_event_streams(std::slice::from_ref(&issue)).unwrap();

    let mut inbound = BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: "inbound".to_string(),
        timestamp: "2026-01-02T00:00:00Z".to_string(),
        actor: "alice".to_string(),
        operation: BeadEventOperationWire::LinkAdded,
        issue_id: issue.id.clone(),
        payload: BeadEventPayloadWire::LinkAdded {
            target_ref: "plan:202608/a.md".to_string(),
            relation: "implements".to_string(),
            description: "the plan implements this bead".to_string(),
            origin: ArtifactLinkOriginWire::Manual,
            direction: BeadLinkDirectionWire::In,
            uses: 1,
            operation_id: None,
        },
    };
    inbound.event_id = mint_bead_event_id(
        &issue.id,
        2,
        &inbound.timestamp,
        &inbound.actor,
        inbound.operation,
        &inbound.issue_id,
        &inbound.payload,
    )
    .unwrap();
    streams[0].events.push(inbound);

    let (issues, provenance) =
        reduce_event_streams_with_link_provenance(&streams).unwrap();
    assert_eq!(issues[0].links.len(), 1);
    assert_eq!(issues[0].links[0].direction, BeadLinkDirectionWire::In);
    assert_eq!(issues[0].links[0].target_ref, "plan:202608/a.md");

    assert_eq!(provenance.len(), 1);
    let row =
        artifact_link_row_from_provenance(provenance.values().next().unwrap());
    assert_eq!(row.source_ref, "plan:202608/a.md");
    assert_eq!(row.target_ref, format!("bead:{}", issue.id));
    assert_eq!(row.relation, "implements");
}
