//! JSONL import/export for git-portable bead storage.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use std::collections::BTreeSet;

use super::wire::{
    deserialize_valid_issue, invalid_record_error, BeadError, BeadTierWire,
    IssueTypeWire, IssueWire,
};

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct JsonlLoadOutcome {
    pub issues: Vec<IssueWire>,
    pub loaded_rows: usize,
    pub blank_lines: usize,
    pub invalid_json_lines: usize,
    pub invalid_record_lines: usize,
}

pub fn parse_issues_jsonl(input: &str) -> JsonlLoadOutcome {
    let mut outcome = JsonlLoadOutcome::default();
    if input.trim().is_empty() {
        return outcome;
    }

    for line in input.lines() {
        let line = line.trim();
        if line.is_empty() {
            outcome.blank_lines += 1;
            continue;
        }
        let value = match serde_json::from_str::<serde_json::Value>(line) {
            Ok(value) => value,
            Err(_) => {
                outcome.invalid_json_lines += 1;
                continue;
            }
        };
        match deserialize_valid_issue(value) {
            Ok(issue) => {
                outcome.loaded_rows += 1;
                outcome.issues.push(issue);
            }
            Err(error) => {
                let _ = invalid_record_error(error);
                outcome.invalid_record_lines += 1;
            }
        }
    }

    apply_missing_tiers(&mut outcome.issues);
    outcome.issues.retain(|issue| issue.validate().is_ok());
    outcome
        .issues
        .sort_by(|a, b| issue_import_key(a).cmp(&issue_import_key(b)));
    outcome
}

pub fn import_issues_from_jsonl(
    path: &Path,
) -> Result<JsonlLoadOutcome, BeadError> {
    if !path.exists() {
        return Ok(JsonlLoadOutcome::default());
    }
    let contents = fs::read_to_string(path)?;
    Ok(parse_issues_jsonl(&contents))
}

pub fn export_issues_to_jsonl(
    issues: &[IssueWire],
) -> Result<String, BeadError> {
    let mut sorted = issues.to_vec();
    sorted.sort_by(|a, b| a.id.cmp(&b.id));

    let mut output = String::new();
    for issue in sorted {
        issue.validate()?;
        output.push_str(&serde_json::to_string(&issue)?);
        output.push('\n');
    }
    Ok(output)
}

fn issue_import_key(issue: &IssueWire) -> (u8, &str) {
    let kind_order = match issue.issue_type {
        IssueTypeWire::Plan => 0,
        IssueTypeWire::Phase => 1,
    };
    (kind_order, issue.id.as_str())
}

pub(crate) fn apply_missing_tiers(issues: &mut [IssueWire]) {
    let phase_parent_ids: BTreeSet<String> = issues
        .iter()
        .filter(|issue| issue.issue_type == IssueTypeWire::Phase)
        .filter_map(|issue| issue.parent_id.clone())
        .collect();
    for issue in issues {
        if issue.issue_type == IssueTypeWire::Plan && issue.tier.is_none() {
            issue.tier = Some(if phase_parent_ids.contains(&issue.id) {
                BeadTierWire::Epic
            } else {
                BeadTierWire::Plan
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bead::wire::{DependencyWire, StatusWire};

    fn plan(id: &str) -> IssueWire {
        IssueWire {
            id: id.to_string(),
            title: "Plan".to_string(),
            status: StatusWire::Open,
            issue_type: IssueTypeWire::Plan,
            tier: Some(BeadTierWire::Epic),
            parent_id: None,
            owner: String::new(),
            assignee: String::new(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            created_by: String::new(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
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

    #[test]
    fn corrupt_lines_are_skipped() {
        let outcome = parse_issues_jsonl(
            r#"not json
{"id":"ok","title":"OK","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}
{"id":
"#,
        );
        assert_eq!(outcome.issues.len(), 1);
        assert_eq!(outcome.invalid_json_lines, 2);
        assert_eq!(outcome.invalid_record_lines, 0);
    }

    #[test]
    fn export_sorts_by_id_and_uses_compact_json() {
        let mut child = plan("gold-1.1");
        child.issue_type = IssueTypeWire::Phase;
        child.tier = None;
        child.parent_id = Some("gold-1".to_string());
        child.dependencies = vec![DependencyWire {
            issue_id: "gold-1.1".to_string(),
            depends_on_id: "gold-1".to_string(),
            created_at: "2026-01-01T00:02:00Z".to_string(),
            created_by: String::new(),
        }];
        let output = export_issues_to_jsonl(&[child, plan("gold-1")]).unwrap();
        assert!(output.starts_with(r#"{"id":"gold-1","#));
        assert!(output.contains(r#""dependencies":[{"issue_id":"gold-1.1""#));
        assert!(!output.contains(": "));
    }

    #[test]
    fn import_defaults_missing_plan_tiers_from_phase_children() {
        let outcome = parse_issues_jsonl(
            r#"{"id":"solo","title":"Solo","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}
{"id":"epic","title":"Epic","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}
{"id":"epic.1","title":"Phase","status":"open","issue_type":"phase","parent_id":"epic","created_at":"","updated_at":"","dependencies":[]}
"#,
        );

        let solo = outcome
            .issues
            .iter()
            .find(|issue| issue.id == "solo")
            .unwrap();
        let epic = outcome
            .issues
            .iter()
            .find(|issue| issue.id == "epic")
            .unwrap();

        assert_eq!(solo.tier, Some(BeadTierWire::Plan));
        assert_eq!(epic.tier, Some(BeadTierWire::Epic));
    }

    #[test]
    fn import_defaults_missing_epic_count_to_none() {
        let outcome = parse_issues_jsonl(
            r#"{"id":"legend","title":"Legend","status":"open","issue_type":"plan","tier":"legend","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}"#,
        );

        assert_eq!(outcome.issues[0].epic_count, None);
    }

    #[test]
    fn import_defaults_missing_model_to_empty() {
        let outcome = parse_issues_jsonl(
            r#"{"id":"epic","title":"Epic","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}"#,
        );

        assert_eq!(outcome.issues[0].model, "");
    }

    #[test]
    fn import_preserves_model() {
        let outcome = parse_issues_jsonl(
            r##"{"id":"epic","title":"Epic","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","model":"#pro","dependencies":[]}"##,
        );

        assert_eq!(outcome.issues[0].model, "#pro");
    }

    #[test]
    fn import_rejects_model_control_characters() {
        let outcome = parse_issues_jsonl(
            "{\"id\":\"epic\",\"title\":\"Epic\",\"status\":\"open\",\"issue_type\":\"plan\",\"parent_id\":null,\"created_at\":\"\",\"updated_at\":\"\",\"model\":\"bad\\n%tag:x\",\"dependencies\":[]}",
        );

        assert_eq!(outcome.issues.len(), 0);
        assert_eq!(outcome.invalid_record_lines, 1);
    }

    #[test]
    fn import_preserves_legend_epic_count() {
        let outcome = parse_issues_jsonl(
            r#"{"id":"legend","title":"Legend","status":"open","issue_type":"plan","tier":"legend","parent_id":null,"created_at":"","updated_at":"","epic_count":5,"dependencies":[]}"#,
        );

        assert_eq!(outcome.issues[0].epic_count, Some(5));
    }
}
