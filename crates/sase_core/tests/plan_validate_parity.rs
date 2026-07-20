//! Frozen JSON parity contract for strict plan validation.
//!
//! The fixture is the shape consumed by the Python facade. Keeping it as a
//! literal catches field-name, nullability, list, and schema-version drift at
//! the Rust boundary without requiring Python at test time.

use sase_core::plan_validate;
use serde_json::{json, Value};

const EPIC_PLAN: &str = r#"---
tier: epic
title: Workspace GC rewrite
goal: Stale workspaces are collected safely
model: claude/opus
changespec: workspace_gc
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
        "schema_version": 2,
        "ok": true,
        "diagnostics": [
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
            "parent": "sase/repos/plans/202607/parent.md"
        }
    });
    assert_eq!(rust_value, python_facade_fixture);
}
