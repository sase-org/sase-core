use std::fs;

use sase_core::{
    bead_blocked_issues, bead_doctor, bead_get_epic_children, bead_list_issues,
    bead_ready_issues, bead_show_issue, bead_stats,
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

fn ids(issues: Vec<sase_core::IssueWire>) -> Vec<String> {
    issues.into_iter().map(|issue| issue.id).collect()
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
