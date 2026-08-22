use std::fs;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, SecondsFormat, Utc};
use sase_core::bead::BeadResolutionWire;
use sase_core::{
    add_bead_link, bead_blocked_issues, bead_create_issue, bead_doctor,
    bead_doctor_report, bead_doctor_with_plan_roots, bead_get_epic_children,
    bead_init_store, bead_list_issues, bead_ready_issues, bead_show_issue,
    bead_show_issue_detail, bead_show_issue_detail_with_options, bead_stats,
    bead_update_issue, import_issues_from_jsonl,
    import_issues_to_event_streams, remove_bead_link,
    repair_event_store_manifest, ArtifactLinkOriginWire, BeadCreateRequestWire,
    BeadEventManifestRepairStatusWire, BeadEventOperationWire,
    BeadEventPayloadWire, BeadEventRecordWire, BeadEventStoreManifestWire,
    BeadEventStreamWire, BeadUpdateFieldsWire, IssueTypeWire,
    BEAD_EVENT_SCHEMA_VERSION,
};
use tempfile::tempdir;

#[test]
fn read_queries_match_python_contract_ordering() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        [
            issue("beads-1", "Epic", "plan", None, "open", "2026-01-01T00:00:00Z", ""),
            issue("beads-1.1", "First", "phase", Some("beads-1"), "open", "2026-01-01T00:01:00Z", ""),
            issue("beads-1.2", "Second", "phase", Some("beads-1"), "open", "2026-01-01T00:02:00Z", r#","dependencies":[{"issue_id":"beads-1.2","depends_on_id":"beads-1.1","created_at":"2026-01-01T00:02:00Z","created_by":""}]"#),
            issue("beads-2", "Closed", "plan", None, "closed", "2026-01-01T00:03:00Z", ""),
            issue("beads-3", "Ready task", "task", None, "ready", "2026-01-01T00:04:00Z", ""),
            issue("beads-4", "Blocked task", "task", None, "ready", "2026-01-01T00:05:00Z", r#","dependencies":[{"issue_id":"beads-4","depends_on_id":"beads-3","created_at":"2026-01-01T00:05:00Z","created_by":""}]"#),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    assert_eq!(
        bead_show_issue(&beads_dir, "beads-1").unwrap().title,
        "Epic"
    );
    assert_eq!(
        ids(bead_list_issues(&beads_dir, None, None, None).unwrap()),
        vec![
            "beads-1",
            "beads-1.1",
            "beads-1.2",
            "beads-2",
            "beads-3",
            "beads-4"
        ]
    );
    assert_eq!(
        ids(bead_list_issues(
            &beads_dir,
            Some(&["open".to_string()]),
            None,
            None,
        )
        .unwrap()),
        vec!["beads-1", "beads-1.1", "beads-1.2"]
    );
    assert_eq!(ids(bead_ready_issues(&beads_dir).unwrap()), vec!["beads-3"]);
    assert_eq!(
        ids(bead_blocked_issues(&beads_dir).unwrap()),
        vec!["beads-1.2", "beads-4"]
    );
    assert_eq!(
        ids(bead_get_epic_children(&beads_dir, "beads-1").unwrap()),
        vec!["beads-1.1", "beads-1.2"]
    );
    let detail = bead_show_issue_detail(&beads_dir, "beads-1").unwrap();
    assert!(detail.ancestors.is_empty());
    assert_eq!(ids(detail.children), vec!["beads-1.1", "beads-1.2"]);
    assert!(detail.depends_on.is_empty());
    assert!(detail.blocks.is_empty());

    let detail = bead_show_issue_detail(&beads_dir, "beads-1.1").unwrap();
    assert_eq!(
        detail.issue,
        bead_show_issue(&beads_dir, "beads-1.1").unwrap()
    );
    assert_eq!(
        detail.ancestors,
        vec![Some(bead_show_issue(&beads_dir, "beads-1").unwrap())]
    );
    assert_eq!(
        ids(detail.children),
        ids(bead_get_epic_children(&beads_dir, "beads-1.1").unwrap())
    );
    assert!(detail.depends_on.is_empty());
    assert_eq!(ids(detail.blocks), vec!["beads-1.2"]);

    let detail = bead_show_issue_detail(&beads_dir, "beads-1.2").unwrap();
    assert_eq!(
        detail.depends_on,
        vec![Some(bead_show_issue(&beads_dir, "beads-1.1").unwrap())]
    );
    let missing =
        bead_show_issue_detail(&beads_dir, "beads-missing").unwrap_err();
    assert_eq!(missing.kind, "not_found");
    assert_eq!(missing.message, "Issue not found: beads-missing");

    let stats = bead_stats(&beads_dir).unwrap();
    assert_eq!(stats["total"], 6);
    assert_eq!(stats["ready"], 2);
    assert_eq!(stats["task"], 2);
    assert_eq!(
        bead_doctor(&beads_dir).unwrap(),
        vec!["OK: no issues found"]
    );
}

#[test]
fn issue_detail_preserves_unresolved_relationship_slots() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        issue(
            "beads-1",
            "Dangling relationships",
            "phase",
            Some("beads-missing-parent"),
            "open",
            "2026-01-01T00:00:00Z",
            r#","dependencies":[{"issue_id":"beads-1","depends_on_id":"beads-missing-dependency","created_at":"2026-01-01T00:00:00Z","created_by":""}]"#,
        ) + "\n",
    )
    .unwrap();

    let detail = bead_show_issue_detail(&beads_dir, "beads-1").unwrap();

    assert_eq!(detail.ancestors, vec![None]);
    assert_eq!(detail.depends_on, vec![None]);
}

#[test]
fn event_store_wins_over_stale_legacy_projection() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();

    let canonical_jsonl = issue(
        "beads-1",
        "Canonical Epic",
        "plan",
        None,
        "open",
        "2026-01-01T00:00:00Z",
        "",
    ) + "\n";
    let canonical_issues =
        import_issues_from_jsonl_content(&canonical_jsonl).unwrap();
    write_event_store(&beads_dir, &canonical_issues).unwrap();

    fs::write(
        beads_dir.join("issues.jsonl"),
        issue(
            "beads-1",
            "Stale Legacy Epic",
            "plan",
            None,
            "open",
            "2026-01-01T00:00:00Z",
            "",
        ) + "\n",
    )
    .unwrap();

    assert_eq!(
        bead_show_issue(&beads_dir, "beads-1").unwrap().title,
        "Canonical Epic"
    );
    assert!(bead_doctor(&beads_dir).unwrap().contains(
        &"WARNING: issues.jsonl is 1 row(s) stale versus the canonical event streams; run 'sase bead doctor --fix-projection'".to_string()
    ));
}

#[test]
fn doctor_reports_projection_fields_and_redundant_close_census() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();
    let canonical = import_issues_from_jsonl_content(
        &(issue(
            "beads-1",
            "Epic",
            "plan",
            None,
            "open",
            "2026-01-01T00:00:00Z",
            "",
        ) + "\n"),
    )
    .unwrap();
    let mut streams = import_issues_to_event_streams(&canonical).unwrap();
    let now: DateTime<Utc> = SystemTime::now().into();
    let first_close = (now - Duration::from_secs(120))
        .to_rfc3339_opts(SecondsFormat::Secs, true);
    let second_close = (now - Duration::from_secs(60))
        .to_rfc3339_opts(SecondsFormat::Secs, true);
    streams[0].events.extend([
        close_event("close-1", &first_close, Some("shipped")),
        close_event("close-2", &second_close, None),
    ]);
    write_streams(&beads_dir, &streams).unwrap();

    let mut stale = canonical[0].clone();
    stale.status = sase_core::StatusWire::Closed;
    stale.closed_at = Some(second_close.clone());
    stale.close_reason = None;
    stale.resolution = Some(BeadResolutionWire::Done);
    stale.updated_at = second_close;
    fs::write(
        beads_dir.join("issues.jsonl"),
        sase_core::export_issues_to_jsonl(&[stale]).unwrap(),
    )
    .unwrap();

    let report = bead_doctor_report(&beads_dir).unwrap();

    assert_eq!(report.projection_drift.len(), 1);
    assert_eq!(report.projection_drift[0].issue_id, "beads-1");
    assert_eq!(
        report.projection_drift[0].changed_fields,
        vec!["close_reason", "closed_at", "updated_at"]
    );
    assert_eq!(report.redundant_close_events, 1);
    assert_eq!(report.redundant_close_issues, 1);
    assert_eq!(report.redundant_close_events_recent, 1);
    assert!(report.messages.iter().any(|message| {
        message.contains("issues.jsonl is 1 row(s) stale")
            && message.contains("--fix-projection")
    }));
    assert!(report.messages.iter().any(|message| {
        message.contains("1 redundant close event(s) across 1 bead(s)")
            && message.contains("1 in the last 7 days")
    }));
}

#[test]
fn event_manifest_repair_recounts_missing_or_stale_metadata_idempotently() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("beads");
    fs::create_dir_all(&beads_dir).unwrap();
    let canonical_jsonl = issue(
        "beads-1",
        "Canonical Epic",
        "plan",
        None,
        "open",
        "2026-01-01T00:00:00Z",
        "",
    ) + "\n";
    let canonical_issues =
        import_issues_from_jsonl_content(&canonical_jsonl).unwrap();
    write_event_store(&beads_dir, &canonical_issues).unwrap();
    let manifest_path = beads_dir.join("events/manifest.json");
    fs::write(
        &manifest_path,
        r#"{"schema_version":1,"stream_count":0,"generated_from":"issues.jsonl","migration_tool":"sase-core bead events"}"#,
    )
    .unwrap();

    let repaired = repair_event_store_manifest(&beads_dir).unwrap();
    assert_eq!(repaired.status, BeadEventManifestRepairStatusWire::Repaired);
    assert_eq!(repaired.stream_count, 1);
    let manifest: BeadEventStoreManifestWire =
        serde_json::from_str(&fs::read_to_string(&manifest_path).unwrap())
            .unwrap();
    assert_eq!(manifest.stream_count, 1);

    let unchanged = repair_event_store_manifest(&beads_dir).unwrap();
    assert_eq!(unchanged.status, BeadEventManifestRepairStatusWire::Noop);

    fs::remove_file(&manifest_path).unwrap();
    let recreated = repair_event_store_manifest(&beads_dir).unwrap();
    assert_eq!(
        recreated.status,
        BeadEventManifestRepairStatusWire::Repaired
    );
    assert!(manifest_path.exists());
}

#[test]
fn event_manifest_repair_refuses_invalid_canonical_streams() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("beads");
    let streams_dir = beads_dir.join("events/streams");
    fs::create_dir_all(&streams_dir).unwrap();
    let manifest_path = beads_dir.join("events/manifest.json");
    fs::write(&manifest_path, "stale manifest\n").unwrap();
    fs::write(streams_dir.join("beads-1.jsonl"), "not json\n").unwrap();
    let before = fs::read(&manifest_path).unwrap();

    let outcome = repair_event_store_manifest(&beads_dir).unwrap();

    assert_eq!(
        outcome.status,
        BeadEventManifestRepairStatusWire::InvalidStream
    );
    assert!(outcome.error.unwrap().contains("invalid bead event stream"));
    assert_eq!(fs::read(&manifest_path).unwrap(), before);
}

#[test]
fn event_manifest_repair_refuses_unsupported_event_schema() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("beads");
    fs::create_dir_all(&beads_dir).unwrap();
    let canonical_jsonl = issue(
        "beads-1",
        "Canonical Epic",
        "plan",
        None,
        "open",
        "2026-01-01T00:00:00Z",
        "",
    ) + "\n";
    let canonical_issues =
        import_issues_from_jsonl_content(&canonical_jsonl).unwrap();
    write_event_store(&beads_dir, &canonical_issues).unwrap();
    let stream_path = beads_dir.join("events/streams/beads-1.jsonl");
    let mut event: serde_json::Value = serde_json::from_str(
        fs::read_to_string(&stream_path)
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    event["schema_version"] = serde_json::json!(2);
    fs::write(&stream_path, serde_json::to_string(&event).unwrap() + "\n")
        .unwrap();

    let outcome = repair_event_store_manifest(&beads_dir).unwrap();

    assert_eq!(
        outcome.status,
        BeadEventManifestRepairStatusWire::InvalidStream
    );
    assert!(outcome
        .error
        .unwrap()
        .contains("unsupported bead event schema_version: 2"));
}

#[test]
fn doctor_reports_orphans_in_stale_legacy_projection() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();

    let canonical_jsonl = [
        issue(
            "beads-1",
            "Epic",
            "plan",
            None,
            "open",
            "2026-01-01T00:00:00Z",
            "",
        ),
        issue(
            "beads-1.1",
            "Child",
            "phase",
            Some("beads-1"),
            "open",
            "2026-01-01T00:01:00Z",
            "",
        ),
    ]
    .join("\n")
        + "\n";
    let canonical_issues =
        import_issues_from_jsonl_content(&canonical_jsonl).unwrap();
    write_event_store(&beads_dir, &canonical_issues).unwrap();

    fs::write(
        beads_dir.join("issues.jsonl"),
        issue(
            "beads-1.1",
            "Child",
            "phase",
            Some("beads-1"),
            "open",
            "2026-01-01T00:01:00Z",
            "",
        ) + "\n",
    )
    .unwrap();

    let messages = bead_doctor(&beads_dir).unwrap();
    assert!(messages.iter().any(|message| {
        message.contains("orphan phase records in issues.jsonl")
            && message.contains("beads-1.1")
    }));
}

#[test]
fn doctor_reports_orphan_nested_plan_records() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        issue(
            "beads-1.1",
            "Nested epic",
            "plan",
            Some("beads-1"),
            "open",
            "2026-01-01T00:01:00Z",
            "",
        ) + "\n",
    )
    .unwrap();

    let messages = bead_doctor(&beads_dir).unwrap();
    assert!(messages.iter().any(|message| {
        message.contains("orphan nested plan records after reduction")
            && message.contains("beads-1.1")
    }));
}

#[test]
fn event_store_supports_read_queries_without_legacy_projection() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();

    let canonical_jsonl = [
        issue(
            "beads-1",
            "Epic",
            "plan",
            None,
            "open",
            "2026-01-01T00:00:00Z",
            "",
        ),
        issue(
            "beads-1.1",
            "Child",
            "phase",
            Some("beads-1"),
            "open",
            "2026-01-01T00:01:00Z",
            "",
        ),
    ]
    .join("\n")
        + "\n";
    let canonical_issues =
        import_issues_from_jsonl_content(&canonical_jsonl).unwrap();
    write_event_store(&beads_dir, &canonical_issues).unwrap();

    assert_eq!(
        ids(bead_list_issues(&beads_dir, None, None, None).unwrap()),
        vec!["beads-1", "beads-1.1"]
    );
    assert_eq!(
        ids(bead_get_epic_children(&beads_dir, "beads-1").unwrap()),
        vec!["beads-1.1"]
    );
    assert!(bead_doctor(&beads_dir)
        .unwrap()
        .contains(&"WARNING: issues.jsonl missing".to_string()));
}

#[test]
fn doctor_reports_invalid_event_store_without_legacy_fallback() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    let streams_dir = beads_dir.join("events/streams");
    fs::create_dir_all(&streams_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();
    fs::write(
        beads_dir.join("events/manifest.json"),
        r#"{"schema_version":1,"stream_count":1,"generated_from":"issues.jsonl","migration_tool":"test"}"#,
    )
    .unwrap();
    fs::write(streams_dir.join("beads-1.jsonl"), "not json\n").unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        issue(
            "beads-1",
            "Legacy Epic",
            "plan",
            None,
            "open",
            "2026-01-01T00:00:00Z",
            "",
        ) + "\n",
    )
    .unwrap();

    let messages = bead_doctor(&beads_dir).unwrap();
    assert!(messages.iter().any(|message| {
        message.starts_with("ERROR: invalid bead event store:")
            && message.contains("invalid bead event stream")
    }));
    assert!(bead_show_issue(&beads_dir, "beads-1").is_err());
}

#[test]
fn doctor_groups_plan_reference_diagnostics_without_changing_compatibility() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("beads");
    let plans_root = temp.path().join("plans");
    fs::create_dir_all(plans_root.join("202607")).unwrap();
    fs::create_dir_all(plans_root.join("202608")).unwrap();
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();

    let exact = plans_root.join("202607/exact.md");
    let legacy = plans_root.join("202607/legacy.md");
    let drifted = plans_root.join("202608/drifted.md");
    let mismatch = plans_root.join("202607/mismatch.md");
    fs::write(
        &exact,
        "---\nbead_id: beads-exact\nbead: wrong-owner\n---\n# Exact\n",
    )
    .unwrap();
    fs::write(&legacy, "---\nbead_id: beads-legacy\n---\n# Legacy\n").unwrap();
    fs::write(&drifted, "---\nbead: beads-drifted\n---\n# Drifted\n").unwrap();
    fs::write(&mismatch, "---\nbead_id: another-bead\n---\n# Mismatch\n")
        .unwrap();
    fs::write(
        plans_root.join("202607/duplicate.md"),
        "---\nbead_id: one\n---\n",
    )
    .unwrap();
    fs::write(
        plans_root.join("202608/duplicate.md"),
        "---\nbead_id: two\n---\n",
    )
    .unwrap();

    fs::write(
        beads_dir.join("issues.jsonl"),
        [
            issue_with_design("beads-exact", "plan:202607/exact.md"),
            issue_with_design("beads-legacy", legacy.to_str().unwrap()),
            issue_with_design("beads-drifted", "plan:202606/drifted.md"),
            issue_with_design("beads-missing", "plan:202607/missing.md"),
            issue_with_design("beads-malformed", "plan:../malformed.md"),
            issue_with_design("beads-ambiguous", "plan:202606/duplicate.md"),
            issue_with_design("beads-mismatch", "plan:202607/mismatch.md"),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    assert_eq!(
        bead_doctor(&beads_dir).unwrap(),
        vec!["OK: no issues found"]
    );

    let messages = bead_doctor_with_plan_roots(
        &beads_dir,
        Some(std::slice::from_ref(&plans_root)),
    )
    .unwrap();
    assert!(messages.iter().any(|message| {
        message.contains("missing or malformed bead design references (2)")
            && message.contains("beads-missing")
            && message.contains("beads-malformed")
    }));
    assert!(messages.iter().any(|message| {
        message.contains("ambiguous bead design references (1)")
            && message.contains("beads-ambiguous")
    }));
    assert!(messages.iter().any(|message| {
        message.contains("bead design reference owner mismatches (1)")
            && message.contains("beads-mismatch")
            && message.contains("another-bead")
    }));
    assert!(!messages.iter().any(|message| {
        message.contains("beads-exact")
            || message.contains("beads-legacy")
            || message.contains("beads-drifted")
    }));
}

#[test]
fn doctor_notes_explicitly_unavailable_plan_roots() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        issue_with_design("beads-1", "plan:202607/plan.md") + "\n",
    )
    .unwrap();

    assert_eq!(
        bead_doctor_with_plan_roots(&beads_dir, None).unwrap(),
        vec![
            "NOTE: bead design reference validation skipped: plan roots unavailable"
        ]
    );
}

#[test]
fn issue_detail_link_neighborhood_preserves_event_provenance() {
    let temp = tempdir().unwrap();
    bead_init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let left = bead_create_issue(
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
    let right = bead_create_issue(
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

    add_bead_link(
        &beads_dir,
        &left.id,
        "plan:202608/example.md",
        "implements",
        "lands the approved CLI design",
        ArtifactLinkOriginWire::Manual,
        Some("2026-08-22T14:10:00Z".to_string()),
    )
    .unwrap();
    add_bead_link(
        &beads_dir,
        &right.id,
        &format!("bead:{}", left.id),
        "implements",
        "right implements left",
        ArtifactLinkOriginWire::Manual,
        Some("2026-08-22T14:11:00Z".to_string()),
    )
    .unwrap();
    add_bead_link(
        &beads_dir,
        &left.id,
        &format!("bead:{}", right.id),
        "related",
        "shares the same rendering contract",
        ArtifactLinkOriginWire::Migrated,
        Some("2026-08-20T09:00:00Z".to_string()),
    )
    .unwrap();
    add_bead_link(
        &beads_dir,
        &right.id,
        &format!("bead:{}", left.id),
        "cites",
        "prompt citation of the left bead",
        ArtifactLinkOriginWire::PromptRef,
        Some("2026-08-22T15:00:00Z".to_string()),
    )
    .unwrap();

    bead_update_issue(
        &beads_dir,
        &left.id,
        BeadUpdateFieldsWire {
            title: Some("Left updated".to_string()),
            now: Some("2026-08-23T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let suffix = left.id.rsplit_once('-').unwrap().1;
    let detail =
        bead_show_issue_detail_with_options(&beads_dir, suffix, true).unwrap();
    assert_eq!(detail.issue.id, left.id);
    assert_eq!(detail.issue.title, "Left updated");
    assert_eq!(detail.issue.updated_at, "2026-08-23T00:00:00Z");
    assert_eq!(detail.artifact_links.len(), 4);

    let implements = detail
        .artifact_links
        .iter()
        .find(|row| {
            row.relation == "implements"
                && row.source_ref == format!("bead:{}", left.id)
        })
        .unwrap();
    assert_eq!(implements.target_ref, "plan:202608/example.md");
    assert_eq!(implements.created_at, "2026-08-22T14:10:00Z");
    assert_eq!(implements.created_by, "owner@example.com");
    assert_eq!(implements.origin, ArtifactLinkOriginWire::Manual);
    assert_ne!(implements.created_at, detail.issue.updated_at);

    let incoming = detail
        .artifact_links
        .iter()
        .find(|row| {
            row.relation == "implements"
                && row.source_ref == format!("bead:{}", right.id)
        })
        .unwrap();
    assert_eq!(incoming.target_ref, format!("bead:{}", left.id));
    assert_eq!(incoming.created_at, "2026-08-22T14:11:00Z");

    let related = detail
        .artifact_links
        .iter()
        .find(|row| row.relation == "related")
        .unwrap();
    assert_eq!(related.origin, ArtifactLinkOriginWire::Migrated);
    assert_eq!(related.created_at, "2026-08-20T09:00:00Z");

    let cites = detail
        .artifact_links
        .iter()
        .find(|row| row.relation == "cites")
        .unwrap();
    assert_eq!(cites.origin, ArtifactLinkOriginWire::PromptRef);
    assert_eq!(cites.created_at, "2026-08-22T15:00:00Z");

    let keys: Vec<(String, String, String, String)> = detail
        .artifact_links
        .iter()
        .map(|row| {
            (
                row.source_ref.clone(),
                row.relation.clone(),
                row.target_ref.clone(),
                row.created_at.clone(),
            )
        })
        .collect();
    let mut expected = keys.clone();
    expected.sort();
    assert_eq!(keys, expected);

    add_bead_link(
        &beads_dir,
        &left.id,
        "plan:202608/example.md",
        "implements",
        "rewritten why",
        ArtifactLinkOriginWire::Manual,
        Some("2026-08-24T12:00:00Z".to_string()),
    )
    .unwrap();
    let rewritten =
        bead_show_issue_detail_with_options(&beads_dir, &left.id, true)
            .unwrap();
    let implements = rewritten
        .artifact_links
        .iter()
        .find(|row| {
            row.relation == "implements"
                && row.source_ref == format!("bead:{}", left.id)
        })
        .unwrap();
    assert_eq!(implements.description, "rewritten why");
    assert_eq!(implements.created_at, "2026-08-24T12:00:00Z");

    remove_bead_link(
        &beads_dir,
        &left.id,
        &format!("bead:{}", right.id),
        Some("related"),
        Some("2026-08-24T13:00:00Z".to_string()),
    )
    .unwrap();
    let after_remove =
        bead_show_issue_detail_with_options(&beads_dir, &left.id, true)
            .unwrap();
    assert!(!after_remove
        .artifact_links
        .iter()
        .any(|row| row.relation == "related"));

    add_bead_link(
        &beads_dir,
        &left.id,
        &format!("bead:{}", right.id),
        "related",
        "re-added related",
        ArtifactLinkOriginWire::Manual,
        Some("2026-08-24T14:00:00Z".to_string()),
    )
    .unwrap();
    let after_readd =
        bead_show_issue_detail_with_options(&beads_dir, &left.id, true)
            .unwrap();
    let related = after_readd
        .artifact_links
        .iter()
        .find(|row| row.relation == "related")
        .unwrap();
    assert_eq!(related.created_at, "2026-08-24T14:00:00Z");
    assert_eq!(related.description, "re-added related");

    let disabled =
        bead_show_issue_detail_with_options(&beads_dir, &left.id, false)
            .unwrap();
    assert!(disabled.artifact_links.is_empty());
    assert!(!disabled.issue.links.is_empty());
}

#[test]
fn issue_detail_legacy_snapshot_keeps_import_compatible_fallback() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();
    let mut payload: serde_json::Value = serde_json::from_str(&issue(
        "beads-1",
        "Legacy linked",
        "plan",
        None,
        "open",
        "2026-01-01T00:00:00Z",
        "",
    ))
    .unwrap();
    payload["updated_at"] = serde_json::json!("2026-01-02T00:00:00Z");
    payload["created_by"] = serde_json::json!("owner@example.com");
    payload["links"] = serde_json::json!([{
        "target_ref": "plan:202608/a.md",
        "relation": "implements",
        "description": "lands the design",
        "origin": "manual"
    }]);
    fs::write(
        beads_dir.join("issues.jsonl"),
        serde_json::to_string(&payload).unwrap() + "\n",
    )
    .unwrap();

    let detail = bead_show_issue_detail(&beads_dir, "beads-1").unwrap();
    assert_eq!(detail.artifact_links.len(), 1);
    assert_eq!(detail.artifact_links[0].source_ref, "bead:beads-1");
    assert_eq!(detail.artifact_links[0].target_ref, "plan:202608/a.md");
    assert_eq!(detail.artifact_links[0].created_at, "2026-01-02T00:00:00Z");
    assert_eq!(detail.artifact_links[0].created_by, "owner@example.com");
    assert_eq!(
        detail.artifact_links[0].origin,
        ArtifactLinkOriginWire::Manual
    );
}

fn ids(issues: Vec<sase_core::IssueWire>) -> Vec<String> {
    issues.into_iter().map(|issue| issue.id).collect()
}

fn import_issues_from_jsonl_content(
    contents: &str,
) -> Result<Vec<sase_core::IssueWire>, sase_core::BeadError> {
    let temp = tempdir().unwrap();
    let path = temp.path().join("issues.jsonl");
    fs::write(&path, contents).unwrap();
    Ok(import_issues_from_jsonl(&path)?.issues)
}

fn write_event_store(
    beads_dir: &std::path::Path,
    issues: &[sase_core::IssueWire],
) -> Result<(), Box<dyn std::error::Error>> {
    let streams = import_issues_to_event_streams(issues)?;
    write_streams(beads_dir, &streams)
}

fn write_streams(
    beads_dir: &std::path::Path,
    streams: &[BeadEventStreamWire],
) -> Result<(), Box<dyn std::error::Error>> {
    let events_dir = beads_dir.join("events");
    let streams_dir = events_dir.join("streams");
    fs::create_dir_all(&streams_dir)?;
    let manifest = BeadEventStoreManifestWire::from_streams(streams);
    fs::write(
        events_dir.join("manifest.json"),
        serde_json::to_string(&manifest)?,
    )?;
    for stream in streams {
        fs::write(
            streams_dir.join(format!("{}.jsonl", stream.stream_id)),
            serialize_stream(stream)?,
        )?;
    }
    Ok(())
}

fn close_event(
    event_id: &str,
    timestamp: &str,
    close_reason: Option<&str>,
) -> BeadEventRecordWire {
    BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: event_id.to_string(),
        timestamp: timestamp.to_string(),
        actor: "test".to_string(),
        operation: BeadEventOperationWire::IssueClosed,
        issue_id: "beads-1".to_string(),
        payload: BeadEventPayloadWire::IssueClosed {
            close_reason: close_reason.map(str::to_string),
            resolution: Some(BeadResolutionWire::Done),
            forced_descendant_ids: Vec::new(),
        },
    }
}

fn serialize_stream(
    stream: &BeadEventStreamWire,
) -> Result<String, serde_json::Error> {
    let mut out = String::new();
    for event in &stream.events {
        out.push_str(&serde_json::to_string(event)?);
        out.push('\n');
    }
    Ok(out)
}

fn issue(
    id: &str,
    title: &str,
    issue_type: &str,
    parent_id: Option<&str>,
    status: &str,
    timestamp: &str,
    extra: &str,
) -> String {
    let parent = match parent_id {
        Some(value) => format!(r#""{value}""#),
        None => "null".to_string(),
    };
    let dependencies = if extra.is_empty() {
        r#","dependencies":[]"#.to_string()
    } else {
        extra.to_string()
    };
    format!(
        r#"{{"id":"{id}","title":"{title}","status":"{status}","issue_type":"{issue_type}","parent_id":{parent},"owner":"","assignee":"","created_at":"{timestamp}","created_by":"","updated_at":"{timestamp}","closed_at":null,"close_reason":null,"description":"","notes":"","design":"","is_ready_to_work":false,"changespec_name":"","changespec_bug_id":""{dependencies}}}"#
    )
}

fn issue_with_design(id: &str, design: &str) -> String {
    let mut value: serde_json::Value = serde_json::from_str(&issue(
        id,
        id,
        "plan",
        None,
        "open",
        "2026-01-01T00:00:00Z",
        "",
    ))
    .unwrap();
    value["design"] = serde_json::json!(design);
    serde_json::to_string(&value).unwrap()
}
