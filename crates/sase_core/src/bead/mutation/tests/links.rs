use super::super::*;
use super::support::*;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::jsonl::read_event_store;
use crate::bead::jsonl::write_event_store;
use crate::bead::mutation::store::store_io_stats;
use crate::bead::mutation::store::MutableStore;
use crate::bead::read::read_store_issues;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::PhaseSizeWire;
use crate::bead::wire::StatusWire;
use std::collections::BTreeMap;
use std::fs;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use tempfile::tempdir;

use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::events::reduce_event_streams;
use crate::bead::jsonl::import_issues_from_jsonl;
use crate::bead::mutation::store::bead_mutation_lock_path;
use crate::bead::mutation::store::lock_bead_mutation_with_timeout;
use crate::bead::wire::BeadReopenCauseWire;
use std::collections::BTreeSet;
use std::time::Duration;
#[test]
fn link_mutations_round_trip_and_keep_related_undirected() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let left = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Left".to_string(),
            issue_type: IssueTypeWire::Plan,
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let right = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Right".to_string(),
            issue_type: IssueTypeWire::Plan,
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    let added = add_bead_link(
        &beads_dir,
        &left.id,
        &format!("bead:{}", right.id),
        "related",
        "shares the ACE-TUI flake root cause",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:01:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(added.changed);
    assert_reprojection_byte_stable(&beads_dir, "link add");
    assert_eq!(added.issue.as_ref().unwrap().links.len(), 1);
    assert_eq!(
        added.issue.as_ref().unwrap().links[0].target_ref,
        format!("bead:{}", right.id)
    );

    let reverse = add_bead_link(
        &beads_dir,
        &right.id,
        &format!("bead:{}", left.id),
        "related",
        "shares the ACE-TUI flake root cause",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(!reverse.changed);
    let issues = read_store_issues(&beads_dir).unwrap();
    let left_issue = issues.iter().find(|issue| issue.id == left.id).unwrap();
    let right_issue = issues.iter().find(|issue| issue.id == right.id).unwrap();
    assert_eq!(left_issue.links.len(), 1);
    assert!(right_issue.links.is_empty());

    let rewritten = add_bead_link(
        &beads_dir,
        &right.id,
        &format!("bead:{}", left.id),
        "related",
        "updated rationale",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:03:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(rewritten.changed);
    let issues = read_store_issues(&beads_dir).unwrap();
    let left_issue = issues.iter().find(|issue| issue.id == left.id).unwrap();
    assert_eq!(left_issue.links[0].description, "updated rationale");

    let reserved = add_bead_link(
        &beads_dir,
        &left.id,
        &format!("bead:{}", right.id),
        "blocks",
        "scheduling",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        None,
        None,
    )
    .unwrap_err();
    assert_eq!(reserved.kind, "reserved");
    assert!(reserved.message.contains("sase bead dep"));

    let removed = remove_bead_link(
        &beads_dir,
        &right.id,
        &format!("bead:{}", left.id),
        Some("related"),
        BeadLinkDirectionWire::Out,
        Some("2026-01-01T00:04:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(removed.changed);
    assert_reprojection_byte_stable(&beads_dir, "link remove");
    let issues = read_store_issues(&beads_dir).unwrap();
    let left_issue = issues.iter().find(|issue| issue.id == left.id).unwrap();
    assert!(left_issue.links.is_empty());

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let ops: Vec<_> = streams
        .iter()
        .flat_map(|stream| stream.events.iter().map(|event| event.operation))
        .collect();
    assert!(ops.contains(&BeadEventOperationWire::LinkAdded));
    assert!(ops.contains(&BeadEventOperationWire::LinkRemoved));
}

#[test]
fn inbound_direction_link_add_and_remove_round_trip() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let target = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Target".to_string(),
            issue_type: IssueTypeWire::Plan,
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    let added = add_bead_link(
        &beads_dir,
        &target.id,
        "plan:202608/a.md",
        "implements",
        "the plan implements this bead",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::In,
        1,
        Some("2026-01-01T00:01:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(added.changed);
    assert_reprojection_byte_stable(&beads_dir, "inbound link add");
    let issue = added.issue.unwrap();
    assert_eq!(issue.links.len(), 1);
    assert_eq!(issue.links[0].target_ref, "plan:202608/a.md");
    assert_eq!(issue.links[0].direction, BeadLinkDirectionWire::In);

    let unchanged = add_bead_link(
        &beads_dir,
        &target.id,
        "plan:202608/a.md",
        "implements",
        "the plan implements this bead",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::In,
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(!unchanged.changed);

    let removed = remove_bead_link(
        &beads_dir,
        &target.id,
        "plan:202608/a.md",
        Some("implements"),
        BeadLinkDirectionWire::In,
        Some("2026-01-01T00:03:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(removed.changed);
    assert_reprojection_byte_stable(&beads_dir, "inbound link remove");
    let issues = read_store_issues(&beads_dir).unwrap();
    let target_issue =
        issues.iter().find(|issue| issue.id == target.id).unwrap();
    assert!(target_issue.links.is_empty());
}

#[test]
fn out_and_in_direction_links_to_same_target_do_not_collide() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let left = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Left".to_string(),
            issue_type: IssueTypeWire::Plan,
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let right = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Right".to_string(),
            issue_type: IssueTypeWire::Plan,
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    // left implements right: stored on left as an outbound link.
    add_bead_link(
        &beads_dir,
        &left.id,
        &format!("bead:{}", right.id),
        "implements",
        "left implements right",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:01:00Z".to_string()),
        None,
    )
    .unwrap();
    // right implements left: left is the target of that separate edge,
    // so it is stored on left as an inbound link with the same peer and
    // relation as the outbound one above.
    add_bead_link(
        &beads_dir,
        &left.id,
        &format!("bead:{}", right.id),
        "implements",
        "right implements left",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::In,
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        None,
    )
    .unwrap();

    let issues = read_store_issues(&beads_dir).unwrap();
    let left_issue = issues.iter().find(|issue| issue.id == left.id).unwrap();
    assert_eq!(left_issue.links.len(), 2);
    assert!(left_issue
        .links
        .iter()
        .any(|link| link.direction == BeadLinkDirectionWire::Out
            && link.description == "left implements right"));
    assert!(left_issue
        .links
        .iter()
        .any(|link| link.direction == BeadLinkDirectionWire::In
            && link.description == "right implements left"));

    remove_bead_link(
        &beads_dir,
        &left.id,
        &format!("bead:{}", right.id),
        Some("implements"),
        BeadLinkDirectionWire::In,
        Some("2026-01-01T00:03:00Z".to_string()),
        None,
    )
    .unwrap();
    let issues = read_store_issues(&beads_dir).unwrap();
    let left_issue = issues.iter().find(|issue| issue.id == left.id).unwrap();
    assert_eq!(left_issue.links.len(), 1);
    assert_eq!(left_issue.links[0].direction, BeadLinkDirectionWire::Out);
}

#[test]
fn link_operation_ids_make_replay_idempotent_but_keep_distinct_reads() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let issue = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Plan".to_string(),
            issue_type: IssueTypeWire::Plan,
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let first_operation = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let second_operation = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();

    let added = add_bead_link(
        &beads_dir,
        &issue.id,
        "plan:202609/a.md",
        "related",
        "read once",
        ArtifactLinkOriginWire::Read,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:01:00Z".to_string()),
        Some(first_operation.clone()),
    )
    .unwrap();
    assert!(added.changed);

    let replayed = add_bead_link(
        &beads_dir,
        &issue.id,
        "plan:202609/a.md",
        "related",
        "read once",
        ArtifactLinkOriginWire::Read,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        Some(first_operation.clone()),
    )
    .unwrap();
    assert!(!replayed.changed);

    let distinct = add_bead_link(
        &beads_dir,
        &issue.id,
        "plan:202609/a.md",
        "related",
        "read again",
        ArtifactLinkOriginWire::Read,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:03:00Z".to_string()),
        Some(second_operation.clone()),
    )
    .unwrap();
    assert!(distinct.changed);

    let issues = read_store_issues(&beads_dir).unwrap();
    let issue = issues.iter().find(|item| item.id == issue.id).unwrap();
    assert_eq!(issue.links.len(), 1);
    assert_eq!(issue.links[0].uses, 2);
    assert_eq!(issue.links[0].description, "read again");

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let link_added_operation_ids: Vec<_> = streams
        .iter()
        .flat_map(|stream| stream.events.iter())
        .filter_map(|event| match &event.payload {
            BeadEventPayloadWire::LinkAdded {
                operation_id: Some(operation_id),
                ..
            } => Some(operation_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        link_added_operation_ids,
        vec![first_operation, second_operation]
    );
}

#[test]
fn link_projection_receipts_are_scoped_to_edge_and_direction() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let issue = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Plan".to_string(),
            issue_type: IssueTypeWire::Plan,
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let operation_id = "cccccccccccccccccccccccccccccccc".to_string();

    let first = set_bead_link_projection(
        &beads_dir,
        &issue.id,
        "plan:202609/a.md",
        "related",
        BeadLinkDirectionWire::Out,
        true,
        Some("first projected edge".to_string()),
        Some(ArtifactLinkOriginWire::Manual),
        1,
        Some("2026-01-01T00:01:00Z".to_string()),
        operation_id.clone(),
    )
    .unwrap();
    assert!(first.changed);

    let second = set_bead_link_projection(
        &beads_dir,
        &issue.id,
        "plan:202609/b.md",
        "related",
        BeadLinkDirectionWire::Out,
        true,
        Some("second projected edge".to_string()),
        Some(ArtifactLinkOriginWire::Manual),
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        operation_id.clone(),
    )
    .unwrap();
    assert!(second.changed);

    let replay = set_bead_link_projection(
        &beads_dir,
        &issue.id,
        "plan:202609/b.md",
        "related",
        BeadLinkDirectionWire::Out,
        true,
        Some("second projected edge".to_string()),
        Some(ArtifactLinkOriginWire::Manual),
        1,
        Some("2026-01-01T00:03:00Z".to_string()),
        operation_id,
    )
    .unwrap();
    assert!(!replay.changed);

    let issues = read_store_issues(&beads_dir).unwrap();
    let issue = issues.iter().find(|item| item.id == issue.id).unwrap();
    let targets: BTreeSet<_> = issue
        .links
        .iter()
        .map(|link| link.target_ref.as_str())
        .collect();
    assert_eq!(
        targets,
        BTreeSet::from(["plan:202609/a.md", "plan:202609/b.md"])
    );
}

#[test]
fn link_projection_receipt_does_not_hide_changed_reduced_state() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let issue = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Plan".to_string(),
            issue_type: IssueTypeWire::Plan,
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let first_operation = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let second_operation = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();

    let first = set_bead_link_projection(
        &beads_dir,
        &issue.id,
        "plan:202609/hot.md",
        "read",
        BeadLinkDirectionWire::In,
        true,
        Some("partial read projection".to_string()),
        Some(ArtifactLinkOriginWire::Read),
        1,
        Some("2026-01-01T00:01:00Z".to_string()),
        first_operation.clone(),
    )
    .unwrap();
    assert!(first.changed);

    let second = set_bead_link_projection(
        &beads_dir,
        &issue.id,
        "plan:202609/hot.md",
        "read",
        BeadLinkDirectionWire::In,
        true,
        Some("partial read projection".to_string()),
        Some(ArtifactLinkOriginWire::Read),
        1,
        Some("2026-01-01T00:02:00Z".to_string()),
        second_operation,
    )
    .unwrap();
    assert!(second.changed);

    let repaired = set_bead_link_projection(
        &beads_dir,
        &issue.id,
        "plan:202609/hot.md",
        "read",
        BeadLinkDirectionWire::In,
        true,
        Some("complete read projection".to_string()),
        Some(ArtifactLinkOriginWire::Read),
        2,
        Some("2026-01-01T00:03:00Z".to_string()),
        first_operation.clone(),
    )
    .unwrap();
    assert!(repaired.changed);

    let stable = set_bead_link_projection(
        &beads_dir,
        &issue.id,
        "plan:202609/hot.md",
        "read",
        BeadLinkDirectionWire::In,
        true,
        Some("complete read projection".to_string()),
        Some(ArtifactLinkOriginWire::Read),
        2,
        Some("2026-01-01T00:04:00Z".to_string()),
        first_operation,
    )
    .unwrap();
    assert!(!stable.changed);

    let issues = read_store_issues(&beads_dir).unwrap();
    let issue = issues.iter().find(|item| item.id == issue.id).unwrap();
    assert_eq!(issue.links.len(), 1);
    assert_eq!(issue.links[0].target_ref, "plan:202609/hot.md");
    assert_eq!(issue.links[0].description, "complete read projection");
    assert_eq!(issue.links[0].uses, 2);
}

#[test]
fn link_projection_batch_matches_singleton_bytes_for_mixed_requests() {
    let (_temp, beads_dir, first_id, second_id) = two_issue_store();
    let singleton_dir = beads_dir.parent().unwrap().join("singleton");
    copy_dir(&beads_dir, &singleton_dir);

    let first_op = hex_operation_id(1);
    let second_op = hex_operation_id(2);
    let third_op = hex_operation_id(3);
    let fourth_op = hex_operation_id(4);
    let requests = vec![
        projection_request(
            &first_id,
            "plan:202609/a.md",
            "related",
            BeadLinkDirectionWire::Out,
            true,
            &first_op,
            "related to a",
            ArtifactLinkOriginWire::Manual,
            1,
            "2026-01-01T00:01:00Z",
        ),
        projection_request(
            &first_id,
            "plan:202609/hot.md",
            "read",
            BeadLinkDirectionWire::In,
            true,
            &second_op,
            "partial read",
            ArtifactLinkOriginWire::Read,
            1,
            "2026-01-01T00:02:00Z",
        ),
        absent_projection_request(
            &first_id,
            "plan:202609/a.md",
            "related",
            BeadLinkDirectionWire::Out,
            &third_op,
            "2026-01-01T00:03:00Z",
        ),
        projection_request(
            &second_id,
            &format!("bead:{first_id}"),
            "related",
            BeadLinkDirectionWire::Out,
            true,
            &fourth_op,
            "peer related",
            ArtifactLinkOriginWire::Manual,
            1,
            "2026-01-01T00:04:00Z",
        ),
    ];

    let batch = set_bead_link_projections(&beads_dir, &requests).unwrap();
    assert!(batch.changed);
    assert!(batch.issue_ids.contains(&first_id));
    assert!(batch.issue_ids.contains(&second_id));

    for request in &requests {
        let outcome = set_bead_link_projection(
            &singleton_dir,
            &request.issue_id,
            &request.target_ref,
            &request.relation,
            request.direction,
            request.present,
            request.description.clone(),
            request.origin,
            request.uses,
            request.now.clone(),
            request.operation_id.clone(),
        )
        .unwrap();
        assert!(outcome.changed);
    }

    assert_eq!(
        persisted_claim_state(&beads_dir),
        persisted_claim_state(&singleton_dir)
    );
}

#[test]
fn link_projection_batch_uses_canonical_aliases_and_undirected_holder() {
    let (_temp, beads_dir, first_id, second_id) = two_issue_store();
    let first_suffix = first_id.rsplit_once('-').unwrap().1.to_string();
    let first_op = hex_operation_id(11);
    let second_op = hex_operation_id(12);

    let outcome = set_bead_link_projections(
        &beads_dir,
        &[
            projection_request(
                &first_id,
                &format!("bead:{second_id}"),
                "related",
                BeadLinkDirectionWire::Out,
                true,
                &first_op,
                "held on first",
                ArtifactLinkOriginWire::Manual,
                1,
                "2026-01-01T00:01:00Z",
            ),
            projection_request(
                &first_suffix,
                &format!("bead:{second_id}"),
                "related",
                BeadLinkDirectionWire::Out,
                true,
                &second_op,
                "still held on first",
                ArtifactLinkOriginWire::Manual,
                2,
                "2026-01-01T00:02:00Z",
            ),
        ],
    )
    .unwrap();
    assert!(outcome.changed);
    assert_eq!(outcome.issue_ids, vec![first_id.clone()]);

    let issues = read_store_issues(&beads_dir).unwrap();
    let first = issues.iter().find(|issue| issue.id == first_id).unwrap();
    let second = issues.iter().find(|issue| issue.id == second_id).unwrap();
    assert_eq!(first.links.len(), 1);
    assert_eq!(first.links[0].target_ref, format!("bead:{second_id}"));
    assert_eq!(first.links[0].description, "still held on first");
    assert_eq!(first.links[0].uses, 2);
    assert!(second.links.is_empty());
}

#[test]
fn link_projection_batch_is_transactional_on_invalid_middle_request() {
    let (_temp, beads_dir, first_id, second_id) = two_issue_store();
    let before = persisted_claim_state(&beads_dir);
    let error = set_bead_link_projections(
        &beads_dir,
        &[
            projection_request(
                &first_id,
                "plan:202609/a.md",
                "related",
                BeadLinkDirectionWire::Out,
                true,
                &hex_operation_id(21),
                "should roll back",
                ArtifactLinkOriginWire::Manual,
                1,
                "2026-01-01T00:01:00Z",
            ),
            projection_request(
                "missing-issue",
                "plan:202609/b.md",
                "related",
                BeadLinkDirectionWire::Out,
                true,
                &hex_operation_id(22),
                "invalid middle",
                ArtifactLinkOriginWire::Manual,
                1,
                "2026-01-01T00:02:00Z",
            ),
            projection_request(
                &second_id,
                "plan:202609/c.md",
                "related",
                BeadLinkDirectionWire::Out,
                true,
                &hex_operation_id(23),
                "never applied",
                ArtifactLinkOriginWire::Manual,
                1,
                "2026-01-01T00:03:00Z",
            ),
        ],
    )
    .unwrap_err();
    assert_eq!(error.kind, "not_found");
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn link_projection_batch_repairs_seen_receipt_then_replays_without_rewrite() {
    let (_temp, beads_dir, first_id, _second_id) = two_issue_store();
    let first_op = hex_operation_id(31);
    let second_op = hex_operation_id(32);
    let first = set_bead_link_projections(
        &beads_dir,
        &[
            projection_request(
                &first_id,
                "plan:202609/hot.md",
                "read",
                BeadLinkDirectionWire::In,
                true,
                &first_op,
                "partial",
                ArtifactLinkOriginWire::Read,
                1,
                "2026-01-01T00:01:00Z",
            ),
            projection_request(
                &first_id,
                "plan:202609/hot.md",
                "read",
                BeadLinkDirectionWire::In,
                true,
                &second_op,
                "partial",
                ArtifactLinkOriginWire::Read,
                1,
                "2026-01-01T00:02:00Z",
            ),
        ],
    )
    .unwrap();
    assert!(first.changed);

    let repaired = set_bead_link_projections(
        &beads_dir,
        &[projection_request(
            &first_id,
            "plan:202609/hot.md",
            "read",
            BeadLinkDirectionWire::In,
            true,
            &first_op,
            "complete",
            ArtifactLinkOriginWire::Read,
            2,
            "2026-01-01T00:03:00Z",
        )],
    )
    .unwrap();
    assert!(repaired.changed);

    let before_replay = persisted_claim_state(&beads_dir);
    store_io_stats::reset();
    let replay = set_bead_link_projections(
        &beads_dir,
        &[
            projection_request(
                &first_id,
                "plan:202609/hot.md",
                "read",
                BeadLinkDirectionWire::In,
                true,
                &first_op,
                "complete",
                ArtifactLinkOriginWire::Read,
                2,
                "2026-01-01T00:04:00Z",
            ),
            projection_request(
                &first_id,
                "plan:202609/hot.md",
                "read",
                BeadLinkDirectionWire::In,
                true,
                &second_op,
                "complete",
                ArtifactLinkOriginWire::Read,
                2,
                "2026-01-01T00:05:00Z",
            ),
        ],
    )
    .unwrap();
    assert!(!replay.changed);
    assert_eq!(store_io_stats::loads(), 1);
    assert_eq!(store_io_stats::saves(), 0);
    assert_eq!(persisted_claim_state(&beads_dir), before_replay);
}

#[test]
fn phase_size_round_trips_through_create_update_events_and_projection() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let epic = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let phase = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Sized phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.id.clone()),
            size: Some(PhaseSizeWire::Medium),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(phase.size, Some(PhaseSizeWire::Medium));

    let updated = update_issue(
        &beads_dir,
        &phase.id,
        BeadUpdateFieldsWire {
            size: Some(PhaseSizeWire::Large),
            now: Some("2026-01-01T00:02:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(updated.size, Some(PhaseSizeWire::Large));

    let jsonl = fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
    assert!(jsonl.contains(r#""size":"large""#));
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    assert!(streams
        .iter()
        .flat_map(|stream| &stream.events)
        .any(|event| {
            matches!(
                &event.payload,
                BeadEventPayloadWire::IssueUpdated { fields }
                    if fields.size == Some(PhaseSizeWire::Large)
            )
        }));
    let reloaded = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(
        reloaded.get_issue(&phase.id).unwrap().size,
        Some(PhaseSizeWire::Large)
    );

    let error = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Invalid sized plan".to_string(),
            issue_type: IssueTypeWire::Plan,
            size: Some(PhaseSizeWire::Small),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(
        error.message,
        "Only phase and task issues can carry size metadata"
    );
}

#[test]
fn task_type_round_trips_through_create_events_and_projection() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let mut fields = BTreeMap::new();
    fields.insert("node_id".to_string(), "tests/foo.py::test_bar".to_string());
    fields.insert("evidence".to_string(), "failed then passed".to_string());
    let task = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Flaky test".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("flake".to_string()),
            task_type_fields: fields.clone(),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(task.task_type.as_deref(), Some("flake"));
    assert_eq!(task.task_type_fields, fields);

    let jsonl = fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
    assert!(jsonl.contains(r#""task_type":"flake""#));
    assert!(jsonl.contains(r#""node_id":"tests/foo.py::test_bar""#));
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    assert!(streams
        .iter()
        .flat_map(|stream| &stream.events)
        .any(|event| {
            matches!(
                &event.payload,
                BeadEventPayloadWire::IssueCreated { issue }
                    if issue.task_type.as_deref() == Some("flake")
                        && issue.task_type_fields == fields
            )
        }));
    let reloaded = MutableStore::load(&beads_dir).unwrap();
    let stored = reloaded.get_issue(&task.id).unwrap();
    assert_eq!(stored.task_type.as_deref(), Some("flake"));
    assert_eq!(stored.task_type_fields, fields);
    assert_eq!(reduces_to_store(&beads_dir), vec![stored.clone()]);
}

#[test]
fn concurrent_update_and_claim_preserve_both_events_and_projection() {
    let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
    let lock_path = bead_mutation_lock_path(&beads_dir);
    let holder = lock_bead_mutation_with_timeout(
        &beads_dir,
        &lock_path,
        Duration::from_secs(1),
        "test_holder",
    )
    .unwrap();

    let update_beads_dir = beads_dir.clone();
    let update_phase_id = phase_id.clone();
    let (update_tx, update_rx) = mpsc::channel();
    let update_handle = thread::spawn(move || {
        let result = update_issue(
            &update_beads_dir,
            &update_phase_id,
            BeadUpdateFieldsWire {
                title: Some("Updated concurrently".to_string()),
                now: Some("2026-01-01T00:02:00Z".to_string()),
                ..Default::default()
            },
        );
        update_tx.send(()).unwrap();
        result
    });
    assert!(matches!(
        update_rx.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));

    let claim_beads_dir = beads_dir.clone();
    let claim_phase_id = phase_id.clone();
    let (claim_tx, claim_rx) = mpsc::channel();
    let claim_handle = thread::spawn(move || {
        let result = claim_for_agent_launch(
            &claim_beads_dir,
            &claim_phase_id,
            "agent-1",
            Some("2026-01-01T00:03:00Z".to_string()),
        );
        claim_tx.send(()).unwrap();
        result
    });
    assert!(matches!(
        claim_rx.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));

    holder.release().unwrap();
    let update_outcome = update_handle.join().unwrap().unwrap();
    let claim_outcome = claim_handle.join().unwrap().unwrap();
    assert!(update_outcome.lock_wait_ms > 0);
    assert!(claim_outcome.lock_wait_ms > 0);

    let projected = import_issues_from_jsonl(&beads_dir.join("issues.jsonl"))
        .unwrap()
        .issues;
    let phase = projected.iter().find(|issue| issue.id == phase_id).unwrap();
    assert_eq!(phase.title, "Updated concurrently");
    assert_eq!(phase.status, StatusWire::InProgress);
    assert_eq!(phase.assignee, "agent-1");

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let update_events: Vec<_> = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .filter(|event| {
            event.issue_id == phase_id
                && event.operation == BeadEventOperationWire::IssueUpdated
        })
        .collect();
    assert_eq!(update_events.len(), 2);
    assert!(update_events.iter().any(|event| matches!(
        &event.payload,
        BeadEventPayloadWire::IssueUpdated { fields }
            if fields.title.as_deref() == Some("Updated concurrently")
    )));
    assert!(update_events.iter().any(|event| matches!(
        &event.payload,
        BeadEventPayloadWire::IssueUpdated { fields }
            if fields.status == Some(StatusWire::InProgress)
                && fields.assignee.as_deref() == Some("agent-1")
    )));
}

#[test]
fn concurrent_launch_claims_preserve_sibling_events_and_projection() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let epic = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let phases: Vec<IssueWire> = ["First", "Second"]
        .into_iter()
        .map(|title| {
            create_issue(
                &beads_dir,
                BeadCreateRequestWire {
                    title: title.to_string(),
                    issue_type: IssueTypeWire::Phase,
                    parent_id: Some(epic.id.clone()),
                    ..Default::default()
                },
            )
            .unwrap()
            .issue
            .unwrap()
        })
        .collect();

    let barrier = Arc::new(Barrier::new(3));
    let handles: Vec<_> = phases
        .iter()
        .enumerate()
        .map(|(index, phase)| {
            let beads_dir = beads_dir.clone();
            let bead_id = phase.id.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                claim_for_agent_launch(
                    &beads_dir,
                    &bead_id,
                    &format!("agent-{}", index + 1),
                    Some(format!("2026-01-01T00:0{}:00Z", index + 1)),
                )
            })
        })
        .collect();
    barrier.wait();
    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let claim_events: Vec<_> = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .filter(|event| {
            event.operation == BeadEventOperationWire::IssueUpdated
                && phases.iter().any(|phase| phase.id == event.issue_id)
        })
        .collect();
    assert_eq!(claim_events.len(), 2);
    let projected = import_issues_from_jsonl(&beads_dir.join("issues.jsonl"))
        .unwrap()
        .issues;
    for (index, phase) in phases.iter().enumerate() {
        let issue =
            projected.iter().find(|issue| issue.id == phase.id).unwrap();
        assert_eq!(issue.status, StatusWire::InProgress);
        assert_eq!(issue.assignee, format!("agent-{}", index + 1));
    }
}

#[test]
fn mutations_create_canonical_events_and_regenerate_projection() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let epic = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let child = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Child".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.id.clone()),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    update_issue(
        &beads_dir,
        &child.id,
        BeadUpdateFieldsWire {
            status: Some("in_progress".to_string()),
            assignee: Some("agent".to_string()),
            now: Some("2026-01-01T00:02:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(
        streams[0]
            .events
            .iter()
            .map(|event| event.operation)
            .collect::<Vec<_>>(),
        vec![
            BeadEventOperationWire::IssueCreated,
            BeadEventOperationWire::IssueCreated,
            BeadEventOperationWire::IssueUpdated,
        ]
    );

    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    export_jsonl(&beads_dir).unwrap();
    let regenerated =
        fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
    assert!(regenerated.contains(r#""id":"sase-1""#));
    assert!(regenerated.contains(r#""id":"sase-1.1""#));
    assert!(regenerated.contains(r#""assignee":"agent""#));
}

#[test]
fn projection_writers_are_byte_stable_for_the_same_store_state() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let first_epic = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "First epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Second epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "First phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(first_epic.id),
            now: Some("2026-01-01T00:02:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let mutation_projection = fs::read(beads_dir.join("issues.jsonl")).unwrap();
    let manifest_before =
        fs::read(beads_dir.join("events/manifest.json")).unwrap();
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let reduced = reduce_event_streams(&streams).unwrap();

    // The conflict/rebuild binding returns reducer rows for its caller to
    // serialize directly, so reducer order is itself a writer contract.
    let reduced_projection = reduced
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join("\n")
        + "\n";
    assert_eq!(reduced_projection.as_bytes(), mutation_projection);

    let direct_export =
        crate::bead::jsonl::export_issues_to_jsonl(&reduced).unwrap();
    assert_eq!(direct_export.as_bytes(), mutation_projection);

    fs::write(beads_dir.join("issues.jsonl"), "stale projection\n").unwrap();
    export_jsonl(&beads_dir).unwrap();
    assert_eq!(
        fs::read(beads_dir.join("issues.jsonl")).unwrap(),
        mutation_projection
    );

    let mut reversed_streams = streams;
    reversed_streams.reverse();
    write_event_store(&beads_dir, &reversed_streams).unwrap();
    assert_eq!(
        fs::read(beads_dir.join("events/manifest.json")).unwrap(),
        manifest_before
    );
    assert_eq!(
        fs::read(beads_dir.join("issues.jsonl")).unwrap(),
        mutation_projection
    );
}

#[test]
fn open_issue_no_longer_leaves_stale_close_metadata_in_the_projection() {
    let (_temp, beads_dir, ids) = close_history_fixture();
    let phase_id = ids[1].clone();
    close_for_history(&beads_dir, &phase_id, "2026-01-02T00:00:00Z");
    open_issue(
        &beads_dir,
        &phase_id,
        Some("2026-01-04T00:00:00Z".to_string()),
    )
    .unwrap();

    let (projected, _) = projected_and_reduced(&beads_dir, &phase_id);
    assert_eq!(projected.status, StatusWire::Open);
    assert_eq!(projected.closed_at, None);
    assert_eq!(projected.close_reason, None);
    assert_eq!(projected.close_history.len(), 1);
    assert_eq!(
        projected.close_history[0].reopened_via,
        BeadReopenCauseWire::Open
    );
    assert_eq!(projected.close_history[0].reopened_by, None);
    assert_eq!(
        projected.close_history[0].reopened_at.as_str(),
        "2026-01-04T00:00:00Z"
    );
}

#[test]
fn a_pre_close_history_projection_recovers_its_reason_on_the_next_load() {
    let (_temp, beads_dir, ids) = close_history_fixture();
    let task_id = ids[2].clone();
    close_for_history(&beads_dir, &task_id, "2026-01-02T00:00:00Z");
    add_task_plus_one(
        &beads_dir,
        &task_id,
        "claude.probe",
        "Still flaky.",
        &[],
        Some("2026-01-03T00:00:00Z".to_string()),
        None,
    )
    .unwrap();

    // Rewrite issues.jsonl the way a pre-change build left it: the close
    // reason destroyed, no close_history key at all. Only the event log
    // still holds the bytes.
    let jsonl = fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
    let damaged = jsonl
        .lines()
        .map(|line| {
            let mut row: serde_json::Value =
                serde_json::from_str(line).unwrap();
            row.as_object_mut().unwrap().remove("close_history");
            serde_json::to_string(&row).unwrap()
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    assert!(!damaged.contains("close_history"));
    fs::write(beads_dir.join("issues.jsonl"), damaged).unwrap();

    let recovered = MutableStore::load(&beads_dir)
        .unwrap()
        .get_issue(&task_id)
        .unwrap()
        .clone();
    assert_eq!(recovered.close_history.len(), 1);
    assert_eq!(
        recovered.close_history[0].close_reason.as_deref(),
        Some("Not reproducible on main.")
    );
    assert_eq!(
        recovered.close_history[0].resolution,
        Some(BeadResolutionWire::Canceled)
    );
}
