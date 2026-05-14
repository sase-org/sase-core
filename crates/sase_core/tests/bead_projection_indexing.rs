use std::fs;

use sase_core::bead::wire::BeadTierWire;
use sase_core::bead::{
    export_issues_to_jsonl, IssueTypeWire, IssueWire, StatusWire,
};
use sase_core::projections::{
    bead_backfill_snapshot, bead_projection_shadow_diff, discover_bead_stores,
    BeadProjectionEventContextWire, EventSourceWire, ProjectionDb,
    ShadowDiffCategoryWire, ShadowDiffCountsWire,
};
use tempfile::tempdir;

const PROJECT_ID: &str = "project-a";

#[test]
fn discovers_vc_and_non_vc_bead_stores() {
    let temp = tempdir().unwrap();
    let vc = temp.path().join("sdd/beads");
    let non_vc = temp.path().join(".sase/sdd/beads");
    fs::create_dir_all(&vc).unwrap();
    fs::create_dir_all(&non_vc).unwrap();
    fs::write(vc.join("issues.jsonl"), "").unwrap();
    fs::write(vc.join("config.json"), "{}\n").unwrap();
    fs::write(vc.join("beads.db"), "").unwrap();

    let stores = discover_bead_stores(temp.path(), Some(PROJECT_ID));

    assert_eq!(stores.len(), 2);
    assert_eq!(stores[0].layout, "vc");
    assert_eq!(stores[0].project_id, PROJECT_ID);
    assert!(stores[0]
        .source_paths
        .iter()
        .any(|path| path.ends_with("sdd/beads/issues.jsonl")));
    assert!(stores[0]
        .sqlite_paths
        .iter()
        .any(|path| path.ends_with("sdd/beads/beads.db")));
    assert_eq!(stores[1].layout, "non_vc");
    assert!(stores[1].beads_dir.ends_with(".sase/sdd/beads"));
}

#[test]
fn backfill_snapshot_and_shadow_diff_match_bead_read_helpers() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();
    let epic = issue(
        "gold-1",
        "Epic",
        IssueTypeWire::Plan,
        None,
        StatusWire::Open,
        "2026-01-01T00:00:00Z",
    );
    let phase = issue(
        "gold-1.1",
        "Phase",
        IssueTypeWire::Phase,
        Some("gold-1"),
        StatusWire::Open,
        "2026-01-01T00:01:00Z",
    );
    fs::write(
        beads_dir.join("issues.jsonl"),
        export_issues_to_jsonl(&[epic, phase.clone()]).unwrap(),
    )
    .unwrap();
    let mut db = ProjectionDb::open_in_memory().unwrap();

    let outcome =
        bead_backfill_snapshot(&mut db, context(), &beads_dir).unwrap();
    assert!(!outcome.duplicate);
    let clean =
        bead_projection_shadow_diff(db.connection(), PROJECT_ID, &beads_dir)
            .unwrap();
    assert_eq!(clean.counts, ShadowDiffCountsWire::default());
    assert!(clean.records.is_empty());

    let mut changed_phase = phase;
    changed_phase.title = "Changed Phase".to_string();
    fs::write(
        beads_dir.join("issues.jsonl"),
        export_issues_to_jsonl(&[changed_phase]).unwrap(),
    )
    .unwrap();

    let diff =
        bead_projection_shadow_diff(db.connection(), PROJECT_ID, &beads_dir)
            .unwrap();
    assert!(diff.counts.stale > 0);
    assert!(diff.records.iter().any(|record| {
        record.handle.as_deref() == Some("gold-1.1")
            && record.category == ShadowDiffCategoryWire::Stale
    }));
}

#[test]
fn corrupt_bead_jsonl_is_reported_as_soft_shadow_diff() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        "not-json\n{\"id\":\"gold-1\"",
    )
    .unwrap();
    let db = ProjectionDb::open_in_memory().unwrap();

    let diff =
        bead_projection_shadow_diff(db.connection(), PROJECT_ID, &beads_dir)
            .unwrap();

    assert_eq!(diff.counts.corrupt, 1);
    assert!(diff.records[0].message.contains("2 invalid JSONL"));
}

fn context() -> BeadProjectionEventContextWire {
    BeadProjectionEventContextWire {
        created_at: Some("2026-05-13T21:00:00.000Z".to_string()),
        source: EventSourceWire {
            source_type: "test".to_string(),
            name: "bead-indexing-test".to_string(),
            ..EventSourceWire::default()
        },
        host_id: "host-a".to_string(),
        project_id: PROJECT_ID.to_string(),
        idempotency_key: None,
        causality: vec![],
        source_path: None,
        source_revision: None,
    }
}

fn issue(
    id: &str,
    title: &str,
    issue_type: IssueTypeWire,
    parent_id: Option<&str>,
    status: StatusWire,
    created_at: &str,
) -> IssueWire {
    IssueWire {
        id: id.to_string(),
        title: title.to_string(),
        status,
        issue_type: issue_type.clone(),
        tier: (issue_type == IssueTypeWire::Plan).then_some(BeadTierWire::Epic),
        parent_id: parent_id.map(str::to_string),
        owner: String::new(),
        assignee: String::new(),
        created_at: created_at.to_string(),
        created_by: String::new(),
        updated_at: created_at.to_string(),
        closed_at: None,
        close_reason: None,
        description: String::new(),
        notes: String::new(),
        design: String::new(),
        model: String::new(),
        is_ready_to_work: false,
        epic_count: None,
        changespec_name: String::new(),
        changespec_bug_id: String::new(),
        dependencies: vec![],
    }
}
