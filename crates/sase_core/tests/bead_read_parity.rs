use std::fs;

use sase_core::{
    bead_blocked_issues, bead_doctor, bead_get_epic_children, bead_list_issues,
    bead_ready_issues, bead_show_issue, bead_stats, import_issues_from_jsonl,
    import_issues_to_event_streams, BeadEventStoreManifestWire,
    BeadEventStreamWire,
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
        vec!["beads-1", "beads-1.1", "beads-1.2", "beads-2"]
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
    assert_eq!(
        ids(bead_ready_issues(&beads_dir).unwrap()),
        vec!["beads-1", "beads-1.1"]
    );
    assert_eq!(
        ids(bead_blocked_issues(&beads_dir).unwrap()),
        vec!["beads-1.2"]
    );
    assert_eq!(
        ids(bead_get_epic_children(&beads_dir, "beads-1").unwrap()),
        vec!["beads-1.1", "beads-1.2"]
    );
    assert_eq!(bead_stats(&beads_dir).unwrap()["total"], 4);
    assert_eq!(
        bead_doctor(&beads_dir).unwrap(),
        vec!["OK: no issues found"]
    );
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
        &"WARNING: issues.jsonl projection drift from bead events".to_string()
    ));
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
    let events_dir = beads_dir.join("events");
    let streams_dir = events_dir.join("streams");
    fs::create_dir_all(&streams_dir)?;
    let manifest = BeadEventStoreManifestWire::from_streams(&streams);
    fs::write(
        events_dir.join("manifest.json"),
        serde_json::to_string(&manifest)?,
    )?;
    for stream in streams {
        fs::write(
            streams_dir.join(format!("{}.jsonl", stream.stream_id)),
            serialize_stream(&stream)?,
        )?;
    }
    Ok(())
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
