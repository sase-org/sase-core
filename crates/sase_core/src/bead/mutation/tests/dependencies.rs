use super::super::*;
use super::support::*;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::jsonl::read_event_store;
use crate::bead::mutation::store::MutableStore;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use tempfile::tempdir;

#[test]
fn create_add_and_remove_references_use_individual_events_and_noop_cleanly() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let created = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Referenced plan".to_string(),
            issue_type: IssueTypeWire::Plan,
            refs: vec![
                "research:202607/report.md".to_string(),
                "research:202607/report.md".to_string(),
                "bead:sase-bb.1".to_string(),
            ],
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    let issue = created.issue.unwrap();
    assert_eq!(
        issue.refs,
        vec![
            "research:202607/report.md".to_string(),
            "bead:sase-bb.1".to_string(),
        ]
    );
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
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

    let added = add_bead_references(
        &beads_dir,
        &issue.id,
        &[
            "bead:sase-bb.1".to_string(),
            "agent:bbugyi200.athena.9w".to_string(),
        ],
        Some("2026-01-01T00:01:00Z".to_string()),
    )
    .unwrap();
    assert!(added.changed);
    assert_reprojection_byte_stable(&beads_dir, "reference add");
    assert_eq!(
        added.references,
        vec!["agent:bbugyi200.athena.9w".to_string()]
    );
    let event_count = read_event_store(&beads_dir).unwrap().1[0].events.len();

    let duplicate = add_bead_references(
        &beads_dir,
        &issue.id,
        &["agent:bbugyi200.athena.9w".to_string()],
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();
    assert!(!duplicate.changed);
    assert_eq!(
        read_event_store(&beads_dir).unwrap().1[0].events.len(),
        event_count
    );

    let removed = remove_bead_references(
        &beads_dir,
        &issue.id,
        &[
            "bead:sase-missing".to_string(),
            "research:202607/report.md".to_string(),
        ],
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap();
    assert!(removed.changed);
    assert_reprojection_byte_stable(&beads_dir, "reference remove");
    assert_eq!(
        removed.references,
        vec!["research:202607/report.md".to_string()]
    );
    assert_eq!(
        removed.issue.unwrap().refs,
        vec![
            "bead:sase-bb.1".to_string(),
            "agent:bbugyi200.athena.9w".to_string(),
        ]
    );

    let absent = remove_bead_references(
        &beads_dir,
        &issue.id,
        &["bead:sase-missing".to_string()],
        Some("2026-01-01T00:04:00Z".to_string()),
    )
    .unwrap();
    assert!(!absent.changed);
}

#[test]
fn reference_mutations_reject_malformed_entries_without_writing() {
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
    let before = persisted_claim_state(&beads_dir);

    let error = add_bead_references(
        &beads_dir,
        &issue.id,
        &["not-a-reference".to_string()],
        None,
    )
    .unwrap_err();

    assert_eq!(error.kind, "validation");
    assert!(error.message.contains("artifact reference list entry 1"));
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn create_uses_explicit_creator_for_issue_and_reference_events() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");

    let issue = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Attributed task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            refs: vec!["bead:sase-parent".to_string()],
            created_by: Some("  bbugyi200.athena.q8  ".to_string()),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    assert_eq!(issue.owner, "owner@example.com");
    assert_eq!(issue.created_by, "bbugyi200.athena.q8");
    let (_, streams) = read_event_store(&beads_dir).unwrap();
    let stream = streams
        .iter()
        .find(|stream| stream.events[0].issue_id == issue.id)
        .unwrap();
    assert_eq!(stream.events.len(), 2);
    assert_eq!(
        stream
            .events
            .iter()
            .map(|event| event.operation)
            .collect::<Vec<_>>(),
        [
            BeadEventOperationWire::IssueCreated,
            BeadEventOperationWire::ReferenceAdded,
        ]
    );
    assert!(stream
        .events
        .iter()
        .all(|event| event.actor == "bbugyi200.athena.q8"));
}

#[test]
fn remove_dependencies_records_the_full_removed_edge() {
    let (_temp, beads_dir, source_id, target_ids) =
        dependency_mutation_fixture();

    let result = remove_dependencies(
        &beads_dir,
        &source_id,
        &[target_ids[0].clone()],
        Some("2026-01-01T00:10:00Z".to_string()),
    )
    .unwrap();

    assert_eq!(result.operation, "dep_rm");
    assert_eq!(
        result.issue_ids,
        vec![source_id.clone(), target_ids[0].clone()]
    );
    assert_eq!(result.dependencies.len(), 1);
    assert_eq!(result.dependencies[0].issue_id, source_id);
    assert_eq!(result.dependencies[0].depends_on_id, target_ids[0]);
    assert_eq!(result.dependencies[0].created_at, "2026-01-01T00:03:00Z");
    assert_eq!(result.dependencies[0].created_by, "owner@example.com");
    assert_eq!(
        result
            .issue
            .unwrap()
            .dependencies
            .iter()
            .map(|dependency| dependency.depends_on_id.as_str())
            .collect::<Vec<_>>(),
        vec![target_ids[1].as_str()]
    );

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let event = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .find(|event| {
            event.operation == BeadEventOperationWire::DependencyRemoved
        })
        .unwrap();
    assert_eq!(event.timestamp, "2026-01-01T00:10:00Z");
    assert_eq!(event.actor, "owner@example.com");
    assert!(matches!(
        &event.payload,
        BeadEventPayloadWire::DependencyRemoved { dependency }
            if dependency.depends_on_id == target_ids[0]
                && dependency.created_at == "2026-01-01T00:03:00Z"
    ));
}

#[test]
fn remove_dependencies_batches_and_deduplicates_targets() {
    let (_temp, beads_dir, source_id, target_ids) =
        dependency_mutation_fixture();

    let result = remove_dependencies(
        &beads_dir,
        &source_id,
        &[
            target_ids[0].clone(),
            target_ids[1].clone(),
            target_ids[0].clone(),
        ],
        Some("2026-01-01T00:10:00Z".to_string()),
    )
    .unwrap();

    assert_eq!(
        result.issue_ids,
        vec![
            source_id.clone(),
            target_ids[0].clone(),
            target_ids[1].clone()
        ]
    );
    assert_eq!(result.dependencies.len(), 2);
    assert!(MutableStore::load(&beads_dir)
        .unwrap()
        .get_issue(&source_id)
        .unwrap()
        .dependencies
        .is_empty());
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    assert_eq!(
        streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.operation == BeadEventOperationWire::DependencyRemoved
            })
            .count(),
        2
    );
}

#[test]
fn remove_dependencies_validates_the_whole_batch_before_writing() {
    let (_temp, beads_dir, source_id, target_ids) =
        dependency_mutation_fixture();
    let before = persisted_claim_state(&beads_dir);

    let error = remove_dependencies(
        &beads_dir,
        &source_id,
        &[target_ids[0].clone(), "sase-missing-edge".to_string()],
        Some("2026-01-01T00:10:00Z".to_string()),
    )
    .unwrap_err();

    assert_eq!(error.kind, "validation");
    assert_eq!(
            error.message,
            format!(
                "Dependency does not exist: {source_id} does not depend on sase-missing-edge"
            )
        );
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn remove_dependencies_rejects_an_unknown_source_without_writing() {
    let (_temp, beads_dir, _source_id, target_ids) =
        dependency_mutation_fixture();
    let before = persisted_claim_state(&beads_dir);

    let error = remove_dependencies(
        &beads_dir,
        "sase-missing",
        &[target_ids[0].clone()],
        None,
    )
    .unwrap_err();

    assert_eq!(error.kind, "not_found");
    assert_eq!(error.message, "Issue not found: sase-missing");
    assert_eq!(persisted_claim_state(&beads_dir), before);
}
