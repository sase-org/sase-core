use std::fs;

use crate::bead::events::import_issues_to_event_streams;
use crate::bead::jsonl::{parse_issues_jsonl, write_event_store};
use crate::bead::read::zombie_plan_tree_snapshot;
use crate::bead::read_model::{
    read_model_cache_path_for_store, rebuild_read_model_at,
};
use crate::bead::wire::{BeadTierWire, IssueTypeWire, StatusWire};
use crate::staged_handoff::{
    InactiveStageDecisionRequestWire, InactiveStageEvidenceValidityWire,
    InactiveStageExecutionStateWire, InactiveStageOriginOutcomeWire,
    InactiveStageReceiptOutcomeWire, InactiveStageRecoveryStateWire,
};
use crate::stranded_work::StrandedWorkMemberFactsWire;
use tempfile::tempdir;

use super::*;

fn epic(id: &str) -> ZombiePlanBeadWire {
    ZombiePlanBeadWire {
        id: id.to_string(),
        title: format!("Epic {id}"),
        status: StatusWire::InProgress,
        issue_type: IssueTypeWire::Plan,
        tier: Some(BeadTierWire::Epic),
        assignee: format!("{id}--land"),
        ..ZombiePlanBeadWire::default()
    }
}

fn closed(
    id: &str,
    parent: &str,
    closed_at: &str,
    event_id: &str,
) -> ZombiePlanBeadWire {
    ZombiePlanBeadWire {
        id: id.to_string(),
        title: format!("Bead {id}"),
        status: StatusWire::Closed,
        issue_type: IssueTypeWire::Phase,
        parent_id: Some(parent.to_string()),
        closed_at: Some(closed_at.to_string()),
        close_event_id: Some(event_id.to_string()),
        ..ZombiePlanBeadWire::default()
    }
}

fn completed_member(name: &str) -> StrandedWorkMemberFactsWire {
    StrandedWorkMemberFactsWire {
        name: name.to_string(),
        turn_kind: "agent".to_string(),
        lifecycle: "done".to_string(),
        started_at: Some(1_767_225_600.0),
        ended_at: Some(1_767_225_700.0),
        ..StrandedWorkMemberFactsWire::default()
    }
}

fn facts(
    members: Vec<StrandedWorkMemberFactsWire>,
) -> ZombiePlanOwnerFactsWire {
    ZombiePlanOwnerFactsWire {
        plan_id: "z".to_string(),
        records_available: true,
        members,
        ..ZombiePlanOwnerFactsWire::default()
    }
}

fn request(
    beads: Vec<ZombiePlanBeadWire>,
    owners: Vec<ZombiePlanOwnerFactsWire>,
) -> ZombiePlanEvaluationInputWire {
    ZombiePlanEvaluationInputWire {
        schema_version: ZOMBIE_PLAN_WIRE_SCHEMA_VERSION,
        snapshot: ZombiePlanTreeSnapshotWire {
            schema_version: ZOMBIE_PLAN_WIRE_SCHEMA_VERSION,
            complete: true,
            beads,
            ..ZombiePlanTreeSnapshotWire::default()
        },
        owners,
    }
}

fn only_row(input: &ZombiePlanEvaluationInputWire) -> ZombiePlanVerdictWire {
    let report = evaluate_zombie_plans(input);
    assert_eq!(report.rows.len(), 1);
    report.rows.into_iter().next().unwrap()
}

fn closed_tree() -> Vec<ZombiePlanBeadWire> {
    vec![
        epic("z"),
        closed("z.phase", "z", "2026-01-01T00:00:00Z", "phase-close"),
        ZombiePlanBeadWire {
            id: "z.nested".to_string(),
            title: "Nested plan".to_string(),
            status: StatusWire::Closed,
            issue_type: IssueTypeWire::Plan,
            tier: Some(BeadTierWire::Plan),
            parent_id: Some("z.phase".to_string()),
            closed_at: Some("2026-01-02T00:00:00Z".to_string()),
            close_event_id: Some("nested-close".to_string()),
            ..ZombiePlanBeadWire::default()
        },
        closed(
            "z.nested.phase",
            "z.nested",
            "2026-01-03T00:00:00Z",
            "latest-close",
        ),
    ]
}

#[test]
fn closed_transitive_tree_uses_latest_current_close_event() {
    let row = only_row(&request(
        closed_tree(),
        vec![facts(vec![completed_member("z--land")])],
    ));
    assert_eq!(row.state, "zombie");
    assert!(row.zombie);
    assert!(row.recovery_eligible);
    assert_eq!(row.descendant_count, 3);
    assert_eq!(row.all_descendants_closed, Some(true));
    assert_eq!(row.latest_descendant_id.as_deref(), Some("z.nested.phase"));
    assert_eq!(
        row.latest_descendant_close_event_id.as_deref(),
        Some("latest-close")
    );
    assert_eq!(
        row.close_token.as_deref(),
        Some("z.nested.phase:latest-close")
    );
}

#[test]
fn zero_children_and_open_descendants_are_not_zombies() {
    let empty = only_row(&request(
        vec![epic("z")],
        vec![facts(vec![completed_member("z--land")])],
    ));
    assert_eq!(empty.state, "not_zombie");
    assert_eq!(empty.reason.as_deref(), Some("no_descendants"));

    let mut open_child =
        closed("z.child", "z", "2026-01-01T00:00:00Z", "child-close");
    open_child.status = StatusWire::InProgress;
    let open = only_row(&request(
        vec![epic("z"), open_child],
        vec![facts(vec![completed_member("z--land")])],
    ));
    assert_eq!(open.state, "not_zombie");
    assert_eq!(open.reason.as_deref(), Some("open_descendant"));

    let mut open_grandchild = closed(
        "z.grandchild",
        "z.child",
        "2026-01-02T00:00:00Z",
        "grandchild-close",
    );
    open_grandchild.status = StatusWire::Open;
    let nested_open = only_row(&request(
        vec![
            epic("z"),
            closed("z.child", "z", "2026-01-01T00:00:00Z", "child-close"),
            open_grandchild,
        ],
        vec![facts(vec![completed_member("z--land")])],
    ));
    assert_eq!(nested_open.state, "not_zombie");
    assert_eq!(nested_open.reason.as_deref(), Some("open_descendant"));
}

#[test]
fn shared_ownership_policy_preserves_owners_and_inactive_failed_stages() {
    let live = StrandedWorkMemberFactsWire {
        name: "z--live".to_string(),
        turn_kind: "agent".to_string(),
        lifecycle: "waiting".to_string(),
        process_live: true,
        ..StrandedWorkMemberFactsWire::default()
    };
    let live_row = only_row(&request(closed_tree(), vec![facts(vec![live])]));
    assert_eq!(live_row.state, "owned");
    assert!(!live_row.recovery_eligible);

    let finalizing = StrandedWorkMemberFactsWire {
        name: "z--finalizing".to_string(),
        lifecycle: "done".to_string(),
        finalizing: true,
        ..StrandedWorkMemberFactsWire::default()
    };
    let finalizing_row =
        only_row(&request(closed_tree(), vec![facts(vec![finalizing])]));
    assert_eq!(finalizing_row.state, "owned");
    assert_eq!(finalizing_row.reason.as_deref(), Some("finalizing"));
    assert_eq!(
        finalizing_row.latest_descendant_id.as_deref(),
        Some("z.nested.phase")
    );
    assert_eq!(
        finalizing_row.latest_descendant_close_event_id.as_deref(),
        Some("latest-close")
    );

    let active_continuation = StrandedWorkMemberFactsWire {
        name: "z--monitor".to_string(),
        turn_kind: "monitor".to_string(),
        lifecycle: "running".to_string(),
        process_live: true,
        turn_state: Some("pending".to_string()),
        ..StrandedWorkMemberFactsWire::default()
    };
    let continuation_row = only_row(&request(
        closed_tree(),
        vec![facts(vec![active_continuation])],
    ));
    assert_eq!(continuation_row.state, "owned");
    assert_eq!(continuation_row.reason.as_deref(), Some("monitor"));

    let mut inactive_origin = StrandedWorkMemberFactsWire {
        name: "z--failed-origin".to_string(),
        turn_kind: "agent".to_string(),
        lifecycle: "failed".to_string(),
        pending_handoff: Some("monitor".to_string()),
        staged_handoff: Some("monitor".to_string()),
        staged_receipt_outcome: Some("handoff_not_adopted".to_string()),
        ..StrandedWorkMemberFactsWire::default()
    };
    inactive_origin.inactive_stage = Some(InactiveStageDecisionRequestWire {
        schema_version: 1,
        stage_validity: InactiveStageEvidenceValidityWire::Valid,
        stage_id: Some("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee".to_string()),
        receipt_validity: InactiveStageEvidenceValidityWire::Valid,
        receipt_id: Some("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee".to_string()),
        receipt_outcome: Some(InactiveStageReceiptOutcomeWire::Held),
        origin_id: Some("origin-run".to_string()),
        origin_identity_valid: true,
        origin_outcome: InactiveStageOriginOutcomeWire::Failed,
        origin_finalizing: false,
        execution_state: InactiveStageExecutionStateWire::Absent,
        recovery_state: InactiveStageRecoveryStateWire::Inactive,
        successor_delivered: false,
    });
    let inactive_row =
        only_row(&request(closed_tree(), vec![facts(vec![inactive_origin])]));
    assert_eq!(inactive_row.state, "zombie");
    assert!(inactive_row.recovery_eligible);
}

#[test]
fn remote_and_unavailable_ownership_evidence_fail_closed() {
    let remote = StrandedWorkMemberFactsWire {
        name: "z--remote".to_string(),
        turn_kind: "agent".to_string(),
        lifecycle: "running".to_string(),
        remote: true,
        ..StrandedWorkMemberFactsWire::default()
    };
    let remote_row =
        only_row(&request(closed_tree(), vec![facts(vec![remote])]));
    assert_eq!(remote_row.state, "owned");

    let unavailable = ZombiePlanOwnerFactsWire {
        plan_id: "z".to_string(),
        records_available: false,
        unknown_reason: Some("liveness_unavailable".to_string()),
        members: vec![completed_member("z--land")],
    };
    let unknown = only_row(&request(closed_tree(), vec![unavailable]));
    assert_eq!(unknown.state, "unknown");
    assert_eq!(unknown.reason.as_deref(), Some("liveness_unavailable"));
    assert_eq!(
        unknown.unknown_reason.as_deref(),
        Some("liveness_unavailable")
    );
    assert_eq!(
        unknown.latest_descendant_id.as_deref(),
        Some("z.nested.phase")
    );

    let complete_empty_scan = ZombiePlanOwnerFactsWire {
        plan_id: "z".to_string(),
        records_available: true,
        ..ZombiePlanOwnerFactsWire::default()
    };
    let no_records =
        only_row(&request(closed_tree(), vec![complete_empty_scan]));
    assert_eq!(no_records.state, "unknown");
    assert_eq!(no_records.reason.as_deref(), Some("ownership_no_records"));

    let partial_dead_scan = ZombiePlanOwnerFactsWire {
        plan_id: "z".to_string(),
        records_available: false,
        members: vec![completed_member("z--land")],
        ..ZombiePlanOwnerFactsWire::default()
    };
    let partial = only_row(&request(closed_tree(), vec![partial_dead_scan]));
    assert_eq!(partial.state, "unknown");
    assert_eq!(
        partial.unknown_reason.as_deref(),
        Some("ownership_records_unavailable")
    );
}

#[test]
fn tree_faults_and_legacy_close_identity_never_authorize_recovery() {
    let mut missing_parent = epic("z");
    missing_parent.parent_id = Some("missing".to_string());
    let missing = only_row(&request(
        vec![
            missing_parent,
            closed("z.child", "z", "2026-01-01T00:00:00Z", "child-close"),
        ],
        vec![facts(vec![completed_member("z--land")])],
    ));
    assert_eq!(missing.state, "unknown");
    assert_eq!(missing.reason.as_deref(), Some("missing_parent:missing"));

    let mut cycle = epic("z");
    cycle.parent_id = Some("z.child".to_string());
    let cyclic = only_row(&request(
        vec![
            cycle,
            closed("z.child", "z", "2026-01-01T00:00:00Z", "child-close"),
        ],
        vec![facts(vec![completed_member("z--land")])],
    ));
    assert_eq!(cyclic.state, "unknown");
    assert!(cyclic
        .reason
        .as_deref()
        .unwrap()
        .starts_with("parent_cycle:"));

    let mut legacy = closed_tree();
    legacy[1].closed_at = None;
    legacy[3].close_event_id = None;
    let legacy_row = only_row(&request(
        legacy,
        vec![facts(vec![completed_member("z--land")])],
    ));
    assert_eq!(legacy_row.state, "zombie");
    assert!(!legacy_row.recovery_eligible);
    assert_eq!(
        legacy_row.recovery_reason.as_deref(),
        Some("legacy_close_timestamp")
    );
    assert!(legacy_row.close_token.is_none());
}

#[test]
fn timestamp_ties_and_reclose_event_identity_are_deterministic() {
    let beads = vec![
        epic("z"),
        closed("z.a", "z", "2026-01-01T00:00:00Z", "same-event"),
        closed("z.b", "z", "2026-01-01T00:00:00Z", "same-event"),
        closed("z.c", "z", "2026-01-01T00:00:00Z", "z-event"),
    ];
    let input = request(beads, vec![facts(vec![completed_member("z--land")])]);
    let first = only_row(&input);
    assert_eq!(first.latest_descendant_id.as_deref(), Some("z.c"));
    assert_eq!(
        first.latest_descendant_close_event_id.as_deref(),
        Some("z-event")
    );

    let mut reclosed = input.clone();
    let latest = reclosed
        .snapshot
        .beads
        .iter_mut()
        .find(|bead| bead.id == "z.c")
        .unwrap();
    latest.close_event_id = Some("zz-reclose-event".to_string());
    let second = only_row(&reclosed);
    assert_ne!(first.close_token, second.close_token);
    assert_eq!(second.close_token.as_deref(), Some("z.c:zz-reclose-event"));
}

#[test]
fn unsupported_plan_tiers_are_reported_but_not_recovery_eligible() {
    let mut beads = closed_tree();
    beads[0].tier = Some(BeadTierWire::Plan);
    let row = only_row(&request(
        beads,
        vec![facts(vec![completed_member("z--land")])],
    ));
    assert!(row.zombie);
    assert!(!row.recovery_eligible);
    assert_eq!(
        row.recovery_reason.as_deref(),
        Some("unsupported_plan_tier")
    );
    assert_eq!(
        row.close_token.as_deref(),
        Some("z.nested.phase:latest-close")
    );
}

#[test]
fn tree_snapshot_matches_indexed_and_replay_paths_and_reads_legacy_stores() {
    let jsonl = concat!(
        "{\"id\":\"z\",\"title\":\"Epic\",\"status\":\"in_progress\",\"issue_type\":\"plan\",\"tier\":\"epic\",\"created_at\":\"2026-01-01T00:00:00Z\",\"updated_at\":\"2026-01-01T00:00:00Z\"}\n",
        "{\"id\":\"z.phase\",\"title\":\"Phase\",\"status\":\"closed\",\"issue_type\":\"phase\",\"parent_id\":\"z\",\"closed_at\":\"2026-01-02T00:00:00Z\",\"close_event_id\":\"phase-close\",\"created_at\":\"2026-01-01T00:00:01Z\",\"updated_at\":\"2026-01-02T00:00:00Z\"}\n",
    );

    let legacy = tempdir().unwrap();
    let legacy_beads = legacy.path().join("beads");
    fs::create_dir_all(&legacy_beads).unwrap();
    fs::write(legacy_beads.join("config.json"), "{}\n").unwrap();
    fs::write(legacy_beads.join("issues.jsonl"), jsonl).unwrap();
    let legacy_snapshot = zombie_plan_tree_snapshot(&legacy_beads).unwrap();
    assert!(legacy_snapshot.complete);
    assert_eq!(legacy_snapshot.beads.len(), 2);

    let event_store = tempdir().unwrap();
    fs::create_dir_all(event_store.path().join(".git")).unwrap();
    let beads_dir = event_store.path().join("beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("issues.jsonl"), jsonl).unwrap();
    let parsed = parse_issues_jsonl(jsonl);
    let streams = import_issues_to_event_streams(&parsed.issues).unwrap();
    write_event_store(&beads_dir, &streams).unwrap();

    let replayed = zombie_plan_tree_snapshot(&beads_dir).unwrap();
    let cache_path = read_model_cache_path_for_store(&beads_dir).unwrap();
    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    let indexed = zombie_plan_tree_snapshot(&beads_dir).unwrap();
    assert_eq!(indexed, replayed);
    assert!(indexed.complete);
    assert_eq!(indexed.beads.len(), 2);
}
