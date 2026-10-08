//! Frozen JSON parity contract for strict plan validation.
//!
//! The fixture is the shape consumed by the Python facade. Keeping it as a
//! literal catches field-name, nullability, list, and schema-version drift at
//! the Rust boundary without requiring Python at test time.

use sase_core::{plan_validate, plan_validate_with_mode};
use serde_json::{json, Value};

const EPIC_PLAN: &str = r#"---
tier: epic
title: Workspace GC rewrite
goal: Stale workspaces are collected safely
model: claude/opus
patch: workspace_gc
bug_id: 123
parent_bead: sase-7z.1
bead: sase-88.1
parent: sase/repos/plans/202607/parent.md
phases:
  - id: core
    title: GC planner
    depends_on: []
    size: large
  - id: smoke
    title: Smoke exercise
    depends_on: [core]
    description: Exercise the completed workflow
    size: small
    model: claude/haiku
---
# Plan

Implement the validated workflow.
"#;

#[test]
fn plan_validate_matches_python_facade_fixture() {
    let rust_value =
        serde_json::to_value(plan_validate(EPIC_PLAN, "epic").unwrap())
            .unwrap();
    let python_facade_fixture: Value = json!({
        "schema_version": 3,
        "ok": true,
        "diagnostics": [
            {
                "severity": "warning",
                "code": "parent-frontmatter-deprecated",
                "field_path": "parent",
                "message": "`parent` frontmatter is deprecated; use the Markdown PARENT header bullet",
                "line": 10
            },
            {
                "severity": "warning",
                "code": "phase-description-missing",
                "field_path": "phases[0].description",
                "message": "phase `core` has no `description`; add one naming its plan-body section and briefly summarizing that section",
                "line": 12
            }
        ],
        "plan": {
            "tier": "epic",
            "goal": "Stale workspaces are collected safely",
            "size": null,
            "model": "claude/opus",
            "title": "Workspace GC rewrite",
            "phases": [
                {
                    "id": "core",
                    "title": "GC planner",
                    "depends_on": [],
                    "description": null,
                    "size": "large",
                    "model": null
                },
                {
                    "id": "smoke",
                    "title": "Smoke exercise",
                    "depends_on": ["core"],
                    "description": "Exercise the completed workflow",
                    "size": "small",
                    "model": "claude/haiku"
                }
            ],
            "changespec": "workspace_gc",
            "bug_id": 123,
            "parent_bead": "sase-7z.1",
            "bead": "sase-88.1",
            "proposed_by": null,
            "parent": "sase/repos/plans/202607/parent.md"
        }
    });
    assert_eq!(rust_value, python_facade_fixture);
}

const DECISIONS_TALE: &str = r#"---
tier: tale
title: Parity decisions
goal: Decisions serialize additively
size: small
decided_by: reviewer
decided_via: tui
decisions:
  tui_note:
    ask: Edit the TUI note?
    default: false
    memory:
      - tui.md
    answer: false
  grouping:
    ask: How to group?
    choices:
      mode: Group by mode
      pane: Group by pane
    default: mode
    why: Keeps the review short
    answer: pane
---
# Plan

Ship tui_note and grouping.

> [!decision] tui_note
> Covers the note edits.

> [!decision] grouping = pane
"#;

#[test]
fn plan_validate_decisions_wire_shape_is_additive() {
    // Stamped plans validate in Archived mode; Authoring forbids answers.
    let rust_value = serde_json::to_value(
        plan_validate_with_mode(DECISIONS_TALE, "tale", "archived").unwrap(),
    )
    .unwrap();
    assert_eq!(rust_value["schema_version"], json!(3));
    assert_eq!(rust_value["ok"], json!(true));
    assert_eq!(
        rust_value["plan"]["decisions"],
        json!([
            {
                "id": "tui_note",
                "kind": "toggle",
                "ask": "Edit the TUI note?",
                "default": false,
                "memory": {"selectors": ["tui.md"]},
                "answer": false,
            },
            {
                "id": "grouping",
                "kind": "choice",
                "ask": "How to group?",
                "why": "Keeps the review short",
                "choices": [
                    {"key": "mode", "label": "Group by mode"},
                    {"key": "pane", "label": "Group by pane"},
                ],
                "default": "mode",
                "answer": "pane",
            }
        ])
    );
    assert_eq!(
        rust_value["plan"]["decision_callouts"],
        json!([
            {"id": "tui_note", "branch": "yes", "start_line": 28, "end_line": 29},
            {"id": "grouping", "key": "pane", "branch": "choice", "start_line": 31, "end_line": 31},
        ])
    );
    assert_eq!(rust_value["plan"]["decided_by"], json!("reviewer"));
    assert_eq!(rust_value["plan"]["decided_via"], json!("tui"));
}
