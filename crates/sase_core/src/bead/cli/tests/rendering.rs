//! Renderer tests, covering `super::super::rendering` (and the
//! `super::super::presentation` vocabulary it draws through): compact,
//! json, and full search views, colors, and match highlighting.

use super::super::presentation::{color_issue_type_cell, ANSI_HIGHLIGHT};
use super::support::*;
use crate::bead::wire::{IssueTypeWire, StatusWire};
use serde_json::Value;
use unicode_width::UnicodeWidthStr;

#[test]
fn search_compact_renders_name_and_description() {
    let store = seed_issues(vec![phase_issue(
        "beads-1.1",
        "Fix Auth Token",
        "Rotate auth tokens safely.",
        StatusWire::InProgress,
        "2026-01-01T00:01:00Z",
    )]);

    let outcome = execute_search(
        &store.beads_dir,
        &["search", "auth", "--format", "compact", "--color", "never"],
    );

    assert_eq!(outcome.exit_code, 0);
    assert_eq!(
        outcome.stdout,
        "↳ ◐ beads-1.1 · Fix Auth Token\n  Rotate auth tokens safely.\n"
    );
}

#[test]
fn search_compact_renders_aligned_glyph_only_type_column() {
    let store = seed_issues(vec![
        plan_issue(
            "beads-1",
            "Needle plan",
            "",
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        ),
        phase_issue(
            "beads-1.1",
            "Needle phase",
            "",
            StatusWire::InProgress,
            "2026-01-01T00:02:00Z",
        ),
        task_issue(
            "beads-2",
            "Needle task",
            "",
            StatusWire::Ready,
            "2026-01-01T00:03:00Z",
        ),
    ]);

    let outcome = execute_search(
        &store.beads_dir,
        &[
            "search", "needle", "--format", "compact", "--color", "never",
        ],
    );

    assert_eq!(outcome.exit_code, 0);
    assert_eq!(
        outcome.stdout,
        concat!(
            "◆ ◇ beads-2 · Needle task\n",
            "↳ ◐ beads-1.1 · Needle phase\n",
            "▸ ○ beads-1 · Needle plan\n",
        )
    );
    let type_prefix_widths: Vec<usize> = outcome
        .stdout
        .lines()
        .map(|line| {
            let status_index = line
                .char_indices()
                .find(|(_, ch)| matches!(ch, '○' | '◐' | '◇'))
                .map(|(index, _)| index)
                .expect("compact search row should contain a status glyph");
            line[..status_index].width()
        })
        .collect();
    assert!(type_prefix_widths.windows(2).all(|pair| pair[0] == pair[1]));
}

#[test]
fn list_compact_renders_aligned_glyph_only_type_column() {
    let store = seed_issues(vec![
        plan_issue(
            "beads-1",
            "Plan bead",
            "",
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        ),
        phase_issue(
            "beads-1.1",
            "Phase bead",
            "",
            StatusWire::InProgress,
            "2026-01-01T00:02:00Z",
        ),
        task_issue(
            "beads-2",
            "Task bead",
            "",
            StatusWire::Ready,
            "2026-01-01T00:03:00Z",
        ),
    ]);

    let list = execute_search(
        &store.beads_dir,
        &["list", "--format", "compact", "--color", "never"],
    );

    assert_eq!(
        list.stdout,
        concat!(
            "▸ ○ beads-1 · Plan bead\n",
            "↳ ◐ beads-1.1 · Phase bead ← beads-1\n",
            "◆ ◇ beads-2 · Task bead\n",
        )
    );
}

#[test]
fn list_compact_colors_shared_type_status_and_id_vocabulary() {
    let store = seed_issues(vec![plan_issue(
        "beads-1",
        "Plan bead",
        "",
        StatusWire::Open,
        "2026-01-01T00:01:00Z",
    )]);

    let list = execute_search(&store.beads_dir, &["list", "--color", "always"]);

    assert_eq!(
        list.stdout,
        concat!(
            "\x1b[38;5;220m▸\x1b[0m ",
            "\x1b[36m○\x1b[0m ",
            "\x1b[1;34mbeads-1\x1b[0m · Plan bead\n",
        )
    );
}

#[test]
fn colored_type_cell_keeps_alignment_padding_outside_ansi_span() {
    assert_eq!(
        color_issue_type_cell(&IssueTypeWire::Task, true, 2),
        "\x1b[38;5;177m◆\x1b[0m "
    );
}

#[test]
fn search_json_renders_stable_uncolored_envelope() {
    let store = seed_issues(vec![phase_issue(
        "beads-1.1",
        "Auth JSON",
        "Structured output",
        StatusWire::Open,
        "2026-01-01T00:01:00Z",
    )]);

    let outcome = execute_search(
        &store.beads_dir,
        &["search", "auth", "-f", "json", "--color", "always"],
    );

    assert_eq!(outcome.exit_code, 0);
    assert!(!outcome.stdout.contains("\x1b["));
    let parsed: Value = serde_json::from_str(&outcome.stdout).unwrap();
    assert_eq!(parsed["query"], "auth");
    assert_eq!(parsed["regex"], false);
    assert_eq!(parsed["count"], 1);
    assert_eq!(parsed["results"][0]["issue"]["id"], "beads-1.1");
    assert_eq!(
        parsed["results"][0]["matched_fields"],
        serde_json::json!(["title"])
    );
}

#[test]
fn search_regex_matches_patterns_and_highlights_ranges() {
    let store = seed_issues(vec![phase_issue(
        "beads-1.1",
        "AuthToken",
        "Rotate the token.",
        StatusWire::Open,
        "2026-01-01T00:01:00Z",
    )]);

    let outcome = execute_search(
        &store.beads_dir,
        &["search", r"auth\w+", "--regex", "--color", "always"],
    );

    assert_eq!(outcome.exit_code, 0);
    assert!(outcome.stdout.contains("\x1b[30;43mAuthToken\x1b[39;49m"));
}

#[test]
fn search_regex_json_marks_regex_mode() {
    let store = seed_issues(vec![phase_issue(
        "beads-1.1",
        "Auth JSON",
        "Structured output",
        StatusWire::Open,
        "2026-01-01T00:01:00Z",
    )]);

    let outcome = execute_search(
        &store.beads_dir,
        &["search", r"auth\s+json", "-e", "-f", "json"],
    );

    assert_eq!(outcome.exit_code, 0);
    let parsed: Value = serde_json::from_str(&outcome.stdout).unwrap();
    assert_eq!(parsed["query"], r"auth\s+json");
    assert_eq!(parsed["regex"], true);
    assert_eq!(parsed["count"], 1);
}

#[test]
fn search_regex_zero_width_only_pattern_matches_without_empty_highlights() {
    let store = seed_issues(vec![phase_issue(
        "beads-1.1",
        "Auth boundary",
        "Structured output",
        StatusWire::Open,
        "2026-01-01T00:01:00Z",
    )]);

    let outcome = execute_search(
        &store.beads_dir,
        &["search", r"\b", "-e", "--color", "always"],
    );

    assert_eq!(outcome.exit_code, 0);
    assert!(outcome.stdout.contains("Auth boundary"));
    assert!(!outcome.stdout.contains(ANSI_HIGHLIGHT));
}

#[test]
fn search_compact_color_always_highlights_matches() {
    let store = seed_issues(vec![phase_issue(
        "beads-1.1",
        "Auth token",
        "Rotate Auth token safely.",
        StatusWire::Closed,
        "2026-01-01T00:01:00Z",
    )]);

    let outcome = execute_search(
        &store.beads_dir,
        &["search", "auth", "--color", "always"],
    );

    assert_eq!(outcome.exit_code, 0);
    assert!(outcome.stdout.contains("\x1b[38;5;117m↳\x1b[0m"));
    assert!(outcome.stdout.contains("\x1b[32m✓\x1b[0m"));
    assert!(outcome.stdout.contains("\x1b[30;43mAuth\x1b[39;49m token"));
    assert!(outcome.stdout.contains(
        "\x1b[2m  Rotate \x1b[30;43mAuth\x1b[39;49m token safely.\x1b[0m"
    ));
}
