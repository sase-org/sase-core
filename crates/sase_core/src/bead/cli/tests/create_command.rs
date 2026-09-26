//! Create-command tests, covering `super::super::create_command`:
//! creation, design-path storage, and plan-root resolution.

use super::super::create_command::{design_plan_roots, design_storage_root};
use super::super::*;
use super::support::*;
use crate::bead::read::read_store_issues;
use crate::bead::wire::{BeadTierWire, StatusWire};
use serde_json::Value;
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn design_plan_roots_resolves_the_beads_sidecar_to_its_plans_sibling() {
    let workspace = Path::new("/ws/sase_1");
    let beads_dir = workspace.join("sase/repos/beads");

    assert_eq!(
        design_plan_roots(workspace, &beads_dir),
        vec![workspace.join("sase/repos/plans")]
    );
    // The bead-in-plans sidecar shape must keep resolving to itself.
    let nested = workspace.join("sase/repos/plans/beads");
    assert_eq!(
        design_plan_roots(workspace, &nested),
        vec![workspace.join("sase/repos/plans")]
    );
}

#[test]
fn design_storage_root_resolves_the_beads_sidecar_to_the_workspace() {
    let cwd = Path::new("/elsewhere");
    let workspace = Path::new("/ws/sase_1");
    let beads_dir = workspace.join("sase/repos/beads");

    assert_eq!(design_storage_root(cwd, &beads_dir), workspace);
    // The bead-in-plans sidecar shape must keep resolving to the workspace.
    let nested = workspace.join("sase/repos/plans/beads");
    assert_eq!(design_storage_root(cwd, &nested), workspace);
}

#[test]
fn search_design_matches_canonical_plan_reference() {
    let mut epic = plan_issue(
        "beads-1",
        "Linked epic",
        "",
        StatusWire::Open,
        "2026-01-01T00:01:00Z",
    );
    epic.design = "plan:202607/roadmap.md".to_string();
    let store = seed_issues(vec![epic]);

    let outcome = execute_search(
        &store.beads_dir,
        &["search", "202607", "--color", "never"],
    );

    assert_eq!(outcome.exit_code, 0);
    assert_eq!(
        outcome.stdout,
        "▸ ○ beads-1 · Linked epic\n  design: \"plan:202607/roadmap.md\"\n"
    );

    let old_prefix = execute_search(
        &store.beads_dir,
        &["search", "sdd/plans", "--color", "never"],
    );
    assert_eq!(old_prefix.stdout, "No beads match \"sdd/plans\".\n");
}

#[test]
fn create_and_remove_are_handled_with_mutation_summaries() {
    let store = seed_issues(Vec::new());
    let plan_path = store.beads_dir.parent().unwrap().join("plan.md");
    fs::write(&plan_path, "# Plan\n").unwrap();
    let create_args = vec![
        "create".to_string(),
        "--title".to_string(),
        "Fast plan".to_string(),
        "--type".to_string(),
        format!("plan({})", plan_path.display()),
        "--tier".to_string(),
        "epic".to_string(),
        "--model".to_string(),
        "codex/test".to_string(),
    ];

    let created = execute_bead_cli(
        &create_args,
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        store.beads_dir.parent().unwrap(),
        false,
        &[],
    )
    .unwrap();

    assert!(created.handled);
    assert_eq!(created.exit_code, 0);
    assert!(created
        .stdout
        .starts_with("Created plan: beads-1 — Fast plan"));
    let summary = created.mutation_summary.unwrap();
    assert_eq!(summary.operation, "create");
    assert_eq!(summary.issue_ids, vec!["beads-1"]);
    let issue = read_store_issues(&store.beads_dir).unwrap().remove(0);
    assert_eq!(issue.design, "sdd/plan.md");
    assert_eq!(issue.tier, Some(BeadTierWire::Epic));
    assert_eq!(issue.model, "codex/test");

    let removed = execute_bead_cli(
        &["rm".to_string(), "beads-1".to_string()],
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        store.beads_dir.parent().unwrap(),
        false,
        &[],
    )
    .unwrap();
    assert_eq!(removed.exit_code, 0);
    assert_eq!(removed.mutation_summary.unwrap().operation, "rm");
    assert!(read_store_issues(&store.beads_dir).unwrap().is_empty());
}

#[test]
fn create_reason_passes_through_and_blank_is_rejected() {
    let store = seed_issues(Vec::new());
    let plan_path = store.beads_dir.parent().unwrap().join("plan.md");
    fs::write(&plan_path, "# Plan\n").unwrap();

    let created = execute_search(
        &store.beads_dir,
        &[
            "create",
            "--title",
            "Reasoned plan",
            "--type",
            &format!("plan({})", plan_path.display()),
            "-w",
            "  filed from the flake triage  ",
        ],
    );
    assert_eq!(created.exit_code, 0);
    let issue = read_store_issues(&store.beads_dir).unwrap().remove(0);
    assert_eq!(issue.creation_reason, "filed from the flake triage");

    let rejected = execute_search(
        &store.beads_dir,
        &[
            "create",
            "--title",
            "Blank reason",
            "--type",
            &format!("plan({})", plan_path.display()),
            "--reason",
            "   ",
        ],
    );
    assert_eq!(rejected.exit_code, 1);
    assert!(rejected.stderr.contains("cannot be empty or blank"));
    assert_eq!(read_store_issues(&store.beads_dir).unwrap().len(), 1);
}

#[test]
fn create_show_and_ref_verbs_honor_the_reference_contract() {
    let store = seed_issues(Vec::new());
    let plan_path = store.beads_dir.parent().unwrap().join("plan.md");
    fs::write(&plan_path, "# Plan\n").unwrap();
    let created = execute_search(
        &store.beads_dir,
        &[
            "create",
            "--title",
            "Referenced plan",
            "--type",
            &format!("plan({})", plan_path.display()),
            "--ref",
            "research:202607/report.md",
            "-R",
            "bead:sase-bb.1",
        ],
    );
    assert_eq!(created.exit_code, 0);
    let issue = read_store_issues(&store.beads_dir).unwrap().remove(0);
    assert_eq!(
        issue.refs,
        vec!["research:202607/report.md", "bead:sase-bb.1"]
    );

    let shown = execute_search(&store.beads_dir, &["show", issue.id.as_str()]);
    assert!(shown.stdout.contains(concat!(
        "\nREFS\n",
        "  research:202607/report.md\n",
        "  bead:sase-bb.1\n",
    )));

    let listed =
        execute_search(&store.beads_dir, &["ref", "list", issue.id.as_str()]);
    assert_eq!(listed.stdout, "research:202607/report.md\nbead:sase-bb.1\n");
    let bare = execute_search(&store.beads_dir, &["ref"]);
    assert!(bare
        .stdout
        .contains(&format!("{}  research:202607/report.md", issue.id)));
    let json = execute_search(
        &store.beads_dir,
        &["ref", "list", issue.id.as_str(), "--json"],
    );
    let parsed: Value = serde_json::from_str(&json.stdout).unwrap();
    assert_eq!(parsed["count"], 2);
    assert_eq!(parsed["results"][0]["issue_id"], issue.id);

    let added = execute_search(
        &store.beads_dir,
        &["ref", "add", issue.id.as_str(), "agent:bbugyi200.athena.9w"],
    );
    assert_eq!(added.exit_code, 0);
    assert_eq!(added.mutation_summary.unwrap().operation, "ref_add");
    let removed = execute_search(
        &store.beads_dir,
        &["ref", "rm", issue.id.as_str(), "research:202607/report.md"],
    );
    assert_eq!(removed.exit_code, 0);
    assert_eq!(removed.mutation_summary.unwrap().operation, "ref_rm");
    assert_eq!(
        read_store_issues(&store.beads_dir).unwrap()[0].refs,
        vec!["bead:sase-bb.1", "agent:bbugyi200.athena.9w"]
    );

    let resolve = execute_search(
        &store.beads_dir,
        &["ref", "list", issue.id.as_str(), "--resolve"],
    );
    assert!(!resolve.handled);
}

#[test]
fn create_plan_path_is_relative_to_store_workspace_from_nested_cwd() {
    let store = seed_issues(Vec::new());
    let workspace = store.beads_dir.ancestors().nth(2).unwrap();
    let nested = workspace.join("src/pkg");
    let plan_path = workspace.join("plans/plan.md");
    fs::create_dir_all(&nested).unwrap();
    fs::create_dir_all(plan_path.parent().unwrap()).unwrap();
    fs::write(&plan_path, "# Plan\n").unwrap();
    let outcome = execute_bead_cli(
        &[
            "create".to_string(),
            "--title".to_string(),
            "Nested plan".to_string(),
            "--type".to_string(),
            format!("plan({})", plan_path.display()),
        ],
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        &nested,
        true,
        &[],
    )
    .unwrap();
    assert_eq!(outcome.exit_code, 0);
    let issue = read_store_issues(&store.beads_dir).unwrap().remove(0);
    assert_eq!(issue.design, "plans/plan.md");
}

#[cfg(unix)]
#[test]
fn create_plan_path_is_relative_through_symlinked_workspace_root() {
    // macOS checks out temp dirs under a symlinked ancestor (/tmp ->
    // /private/tmp): the plan path canonicalizes but the store workspace
    // stays caller-supplied, so a one-sided strip_prefix degrades the
    // stored design to an absolute, machine-specific path. Reproduce that
    // asymmetry here with a symlinked workspace root.
    let temp = tempdir().unwrap();
    let real = temp.path().join("real");
    std::fs::create_dir_all(&real).unwrap();
    std::os::unix::fs::symlink(&real, temp.path().join("link")).unwrap();
    let beads_dir = temp.path().join("link/sdd/beads");
    let store = seed_issues_at(temp, beads_dir, Vec::new());
    let workspace = store.beads_dir.ancestors().nth(2).unwrap();
    let plan_path = workspace.join("plans/plan.md");
    fs::create_dir_all(plan_path.parent().unwrap()).unwrap();
    fs::write(&plan_path, "# Plan\n").unwrap();
    let outcome = execute_bead_cli(
        &[
            "create".to_string(),
            "--title".to_string(),
            "Linked plan".to_string(),
            "--type".to_string(),
            format!("plan({})", plan_path.display()),
        ],
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        workspace,
        true,
        &[],
    )
    .unwrap();
    assert_eq!(outcome.exit_code, 0);
    let issue = read_store_issues(&store.beads_dir).unwrap().remove(0);
    assert_eq!(issue.design, "plans/plan.md");
}

#[test]
fn create_plan_under_in_tree_plans_root_stores_canonical_reference() {
    let store = seed_issues(Vec::new());
    let workspace = store.beads_dir.ancestors().nth(2).unwrap();
    let plan_path = workspace.join("sdd/plans/202607/roadmap.md");
    fs::create_dir_all(plan_path.parent().unwrap()).unwrap();
    fs::write(&plan_path, "# Roadmap\n").unwrap();

    let outcome = execute_bead_cli(
        &[
            "create".to_string(),
            "--title".to_string(),
            "Canonical plan".to_string(),
            "--type".to_string(),
            format!("plan({})", plan_path.display()),
        ],
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        workspace,
        true,
        &[],
    )
    .unwrap();

    assert_eq!(outcome.exit_code, 0);
    let issue = read_store_issues(&store.beads_dir).unwrap().remove(0);
    assert_eq!(issue.design, "plan:202607/roadmap.md");
}

#[test]
fn create_plan_under_sidecar_plans_root_stores_canonical_reference() {
    let temp = tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let beads_dir = workspace.join("sase/repos/plans/beads");
    let store = seed_issues_at(temp, beads_dir, Vec::new());
    let plan_path = workspace.join("sase/repos/plans/202607/roadmap.md");
    fs::create_dir_all(plan_path.parent().unwrap()).unwrap();
    fs::write(&plan_path, "# Roadmap\n").unwrap();

    let outcome = execute_bead_cli(
        &[
            "create".to_string(),
            "--title".to_string(),
            "Sidecar plan".to_string(),
            "--type".to_string(),
            format!("plan({})", plan_path.display()),
        ],
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        &workspace,
        false,
        &[],
    )
    .unwrap();

    assert_eq!(outcome.exit_code, 0);
    let issue = read_store_issues(&store.beads_dir).unwrap().remove(0);
    assert_eq!(issue.design, "plan:202607/roadmap.md");
}
