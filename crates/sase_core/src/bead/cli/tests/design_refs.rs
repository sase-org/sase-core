//! Design-reference display tests, covering
//! `super::super::design_refs` through the show command.

use super::support::*;
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn show_renders_reference_above_its_resolved_path() {
    let plans = tempdir().unwrap();
    let root = seed_plan_root(&plans, "202607", "durable.md");

    let plan = show_plan_section(
        "plan:202607/durable.md",
        std::slice::from_ref(&root),
        Path::new("/repo"),
        false,
    );

    assert_eq!(
        plan,
        format!(
            "  plan:202607/durable.md\n  → {}\n",
            root.join("202607/durable.md").display()
        )
    );
}

#[test]
fn show_marks_a_reference_resolved_through_month_drift() {
    let plans = tempdir().unwrap();
    let root = seed_plan_root(&plans, "202607", "drifted.md");

    let plan = show_plan_section(
        "plan:202606/drifted.md",
        std::slice::from_ref(&root),
        Path::new("/repo"),
        false,
    );

    assert_eq!(
        plan,
        format!(
            "  plan:202606/drifted.md\n  → {} (month drift)\n",
            root.join("202607/drifted.md").display()
        )
    );
}

#[test]
fn show_says_plainly_when_a_reference_resolves_nowhere() {
    let plans = tempdir().unwrap();
    let root = plans.path().to_path_buf();

    let plan = show_plan_section(
        "plan:202607/gone.md",
        std::slice::from_ref(&root),
        Path::new("/repo"),
        false,
    );

    assert_eq!(
        plan,
        concat!(
            "  plan:202607/gone.md\n",
            "  → (unresolved: no plan file found)\n",
        )
    );
}

#[test]
fn show_reports_an_ambiguous_reference_instead_of_guessing() {
    let plans = tempdir().unwrap();
    seed_plan_root(&plans, "202606", "twin.md");
    let root = seed_plan_root(&plans, "202607", "twin.md");

    let plan = show_plan_section(
        "plan:202605/twin.md",
        std::slice::from_ref(&root),
        Path::new("/repo"),
        false,
    );

    assert_eq!(
        plan,
        concat!(
            "  plan:202605/twin.md\n",
            "  → (ambiguous: multiple plans match this reference)\n",
        )
    );
}

#[test]
fn show_reports_a_malformed_reference() {
    let plan =
        show_plan_section("plan:../escape.md", &[], Path::new("/repo"), false);

    assert_eq!(
        plan,
        concat!(
            "  plan:../escape.md\n",
            "  → (unresolved: malformed plan reference)\n",
        )
    );
}

#[test]
fn show_keeps_one_line_when_a_legacy_path_resolves_to_itself() {
    let workspace = tempdir().unwrap();
    let plan_path = workspace.path().join("plans/legacy.md");
    fs::create_dir_all(plan_path.parent().unwrap()).unwrap();
    fs::write(&plan_path, "# Plan\n").unwrap();

    let plan =
        show_plan_section("plans/legacy.md", &[], workspace.path(), true);

    assert_eq!(plan, "  plans/legacy.md\n");
}

#[test]
fn show_resolves_a_legacy_path_against_the_working_directory() {
    let workspace = tempdir().unwrap();
    let plan_path = workspace.path().join("plans/legacy.md");
    fs::create_dir_all(plan_path.parent().unwrap()).unwrap();
    fs::write(&plan_path, "# Plan\n").unwrap();

    let plan =
        show_plan_section("plans/legacy.md", &[], workspace.path(), false);

    assert_eq!(
        plan,
        format!("  plans/legacy.md\n  → {}\n", plan_path.display())
    );
}
