//! Argument-parser unit tests, covering `super::super::parsing` and
//! the create parser in `super::super::create_command`.

use super::super::create_command::{parse_create_type, CREATE_TYPE_EXPECTED};
use super::super::parsing::{
    close_note_author, parse_close_args, parse_search_args, SearchArgs,
    SearchParseOutcome,
};
use super::super::*;
use super::support::*;
use crate::bead::read::read_store_issues;
use crate::bead::wire::{BeadResolutionWire, StatusWire};
use std::path::Path;

#[test]
fn search_regex_flag_is_fast_path_only_as_bare_flag() {
    let parsed = parse_search_args(&string_args(&["needle", "--regex"]));
    assert!(matches!(
        parsed,
        SearchParseOutcome::Parsed(SearchArgs { regex: true, .. })
    ));

    let short = parse_search_args(&string_args(&["needle", "-e"]));
    assert!(matches!(
        short,
        SearchParseOutcome::Parsed(SearchArgs { regex: true, .. })
    ));

    assert_eq!(
        parse_search_args(&string_args(&["needle", "--regex=true"])),
        SearchParseOutcome::Defer
    );
}

#[test]
fn search_regex_invalid_pattern_is_usage_error_across_formats() {
    let store = seed_issues(Vec::new());

    for args in [
        &["search", "[", "--regex"][..],
        &["search", "[", "--regex", "--format", "json"][..],
        &["search", "[", "--regex", "--format", "full"][..],
    ] {
        let outcome = execute_search(&store.beads_dir, args);

        assert_eq!(outcome.exit_code, 2, "args: {args:?}");
        assert!(outcome.stdout.is_empty(), "args: {args:?}");
        assert!(
            outcome.stderr.starts_with("Error: invalid search regex: "),
            "stderr for {args:?}: {}",
            outcome.stderr
        );
    }
}

#[test]
fn parse_create_type_rejects_retired_flag_form() {
    let error =
        parse_create_type("flag(demo_key,2026-12-01,0.19.0)").unwrap_err();
    assert!(error.contains(CREATE_TYPE_EXPECTED));

    let error = parse_create_type("flag").unwrap_err();
    assert!(error.contains(CREATE_TYPE_EXPECTED));
}

#[test]
fn create_rejects_bare_task_constructor_without_size() {
    let store = seed_issues(Vec::new());

    let created = execute_search(
        &store.beads_dir,
        &["create", "--title", "Follow-up", "--type", "task"],
    );

    assert_eq!(created.exit_code, 1);
    assert!(created.stderr.contains("requires an explicit size"));
    assert!(read_store_issues(&store.beads_dir).unwrap().is_empty());
}

#[test]
fn close_parser_accepts_force_with_reason_and_resolution() {
    let (ids, force, note, reason, resolution) = parse_close_args(&[
        "beads-1".to_string(),
        "--force".to_string(),
        "-n".to_string(),
        "verified".to_string(),
        "--reason".to_string(),
        "Requirements changed".to_string(),
        "--resolution=canceled".to_string(),
    ])
    .unwrap();

    assert_eq!(ids, vec!["beads-1"]);
    assert!(force);
    assert_eq!(note.as_deref(), Some("verified"));
    assert_eq!(reason.as_deref(), Some("Requirements changed"));
    assert_eq!(resolution, Some(BeadResolutionWire::Canceled));
}

#[test]
fn update_fast_path_defers_size_flag_to_python() {
    let store = seed_issues(vec![task_issue(
        "beads-1",
        "First task",
        "",
        StatusWire::Open,
        "2026-01-01T00:00:00Z",
    )]);
    let outcome = execute_bead_cli(
        &[
            "update".to_string(),
            "beads-1".to_string(),
            "-z".to_string(),
            "medium".to_string(),
        ],
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        Path::new("/repo"),
        false,
        &[],
    )
    .unwrap();

    assert!(!outcome.handled);
}

#[test]
fn close_note_author_ignores_the_bare_launcher_flag() {
    let saved_name = std::env::var("SASE_AGENT_NAME").ok();
    let saved_agent = std::env::var("SASE_AGENT").ok();
    std::env::remove_var("SASE_AGENT_NAME");
    std::env::remove_var("SASE_AGENT");

    // No identity configured.
    assert_eq!(close_note_author(), None);

    // The bare launcher flag carries no identity.
    std::env::set_var("SASE_AGENT", "1");
    assert_eq!(close_note_author(), None);

    // A real agent name is returned, with `SASE_AGENT_NAME` winning.
    std::env::set_var("SASE_AGENT", "worker");
    assert_eq!(close_note_author().as_deref(), Some("worker"));
    std::env::set_var("SASE_AGENT_NAME", "named-worker");
    assert_eq!(close_note_author().as_deref(), Some("named-worker"));

    std::env::remove_var("SASE_AGENT_NAME");
    std::env::remove_var("SASE_AGENT");
    if let Some(value) = saved_name {
        std::env::set_var("SASE_AGENT_NAME", value);
    }
    if let Some(value) = saved_agent {
        std::env::set_var("SASE_AGENT", value);
    }
}
