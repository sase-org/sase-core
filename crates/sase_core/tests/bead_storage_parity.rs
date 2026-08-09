use std::fs;

use sase_core::{
    export_issues_to_jsonl, import_issues_from_jsonl, load_config_from_str,
    parse_issues_jsonl, BeadConfigWire, IssueTypeWire, PhaseSizeWire,
    StatusWire,
};
use tempfile::tempdir;

const CURRENT_SCHEMA: &str =
    include_str!("fixtures/bead/jsonl/current_schema.jsonl");
const PRE_READY_SCHEMA: &str =
    include_str!("fixtures/bead/jsonl/pre_is_ready_to_work_schema.jsonl");
const PRE_PATCH_SCHEMA: &str =
    include_str!("fixtures/bead/jsonl/pre_changespec_metadata_schema.jsonl");
const CORRUPT_LINES: &str =
    include_str!("fixtures/bead/jsonl/corrupt_lines.jsonl");
const EMPTY_JSONL: &str = include_str!("fixtures/bead/jsonl/empty.jsonl");
const CURRENT_CONFIG: &str = include_str!("fixtures/bead/config/current.json");

#[test]
fn current_schema_fixture_loads_hierarchy_dependencies_and_metadata() {
    let outcome = parse_issues_jsonl(CURRENT_SCHEMA);
    assert_eq!(outcome.loaded_rows, 2);
    assert_eq!(outcome.invalid_json_lines, 0);
    assert_eq!(outcome.invalid_record_lines, 0);

    let parent = outcome
        .issues
        .iter()
        .find(|issue| issue.id == "gold-1")
        .unwrap();
    let child = outcome
        .issues
        .iter()
        .find(|issue| issue.id == "gold-1.1")
        .unwrap();

    assert_eq!(parent.issue_type, IssueTypeWire::Plan);
    assert!(parent.is_ready_to_work);
    assert_eq!(parent.model, "claude/opus");
    assert_eq!(parent.changespec_name, "current_changespec");
    assert_eq!(parent.changespec_bug_id, "BUG-100");
    assert_eq!(child.parent_id.as_deref(), Some("gold-1"));
    assert_eq!(child.model, "codex/gpt-5.5");
    assert_eq!(child.dependencies[0].depends_on_id, "gold-1");
}

#[test]
fn legacy_jsonl_fixtures_get_python_defaults() {
    let pre_ready = parse_issues_jsonl(PRE_READY_SCHEMA);
    assert_eq!(pre_ready.issues[0].id, "legacy-ready-1");
    assert!(!pre_ready.issues[0].is_ready_to_work);

    let pre_patch = parse_issues_jsonl(PRE_PATCH_SCHEMA);
    assert_eq!(pre_patch.issues[0].id, "legacy-meta-1");
    assert_eq!(pre_patch.issues[0].changespec_name, "");
    assert_eq!(pre_patch.issues[0].changespec_bug_id, "");
    assert_eq!(pre_patch.issues[0].model, "");
}

#[test]
fn corrupt_and_empty_fixtures_match_python_tolerance() {
    let corrupt = parse_issues_jsonl(CORRUPT_LINES);
    assert_eq!(corrupt.issues.len(), 1);
    assert_eq!(corrupt.issues[0].id, "corrupt-1");
    assert_eq!(corrupt.invalid_json_lines, 2);

    let empty = parse_issues_jsonl(EMPTY_JSONL);
    assert!(empty.issues.is_empty());
}

#[test]
fn import_missing_file_returns_empty_outcome() {
    let temp = tempdir().unwrap();
    let outcome =
        import_issues_from_jsonl(&temp.path().join("missing.jsonl")).unwrap();
    assert!(outcome.issues.is_empty());
}

#[test]
fn export_current_fixture_is_byte_compatible() {
    let outcome = parse_issues_jsonl(CURRENT_SCHEMA);
    let exported = export_issues_to_jsonl(&outcome.issues).unwrap();
    assert_eq!(exported, CURRENT_SCHEMA);
}

#[test]
fn load_config_fixture_matches_python_shape() {
    let config = load_config_from_str(CURRENT_CONFIG).unwrap();
    assert_eq!(
        config,
        BeadConfigWire {
            issue_prefix: "gold".to_string(),
            next_counter: 42,
            owner: "owner@example.com".to_string(),
        }
    );
}

#[test]
fn import_from_file_uses_same_parser() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("issues.jsonl");
    fs::write(&path, CURRENT_SCHEMA).unwrap();
    let outcome = import_issues_from_jsonl(&path).unwrap();
    assert_eq!(outcome.issues.len(), 2);
}

#[test]
fn task_and_ready_values_round_trip_with_python_wire_spelling() {
    let content = r#"{"id":"gold-task","title":"Follow-up","status":"ready","issue_type":"task","parent_id":null,"owner":"","assignee":"","created_at":"2026-01-01T00:00:00Z","created_by":"","updated_at":"2026-01-01T00:00:00Z","closed_at":null,"close_reason":null,"description":"","notes":"","design":"","model":"","size":"medium","is_ready_to_work":false,"changespec_name":"","changespec_bug_id":"","dependencies":[]}
"#;

    let outcome = parse_issues_jsonl(content);

    assert_eq!(outcome.loaded_rows, 1);
    assert_eq!(outcome.issues[0].issue_type, IssueTypeWire::Task);
    assert_eq!(outcome.issues[0].status, StatusWire::Ready);
    assert_eq!(outcome.issues[0].size, Some(PhaseSizeWire::Medium));
    assert_eq!(export_issues_to_jsonl(&outcome.issues).unwrap(), content);
}
