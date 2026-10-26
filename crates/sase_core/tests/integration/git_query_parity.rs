//! Phase 5C parity gate: mirror the Phase 5B Python golden tests in
//! `sase_100/tests/test_core_git_query.py` so a parity drift fails the
//! Rust-side `cargo test` before the Python facade ever sees the
//! binding.
//!
//! The fixtures are inlined byte-for-byte from the Python source. If
//! `sase_100/tests/test_core_git_query.py` changes, mirror the change
//! here.

use sase_core::git_query::{
    derive_git_workspace_name, parse_git_branch_name,
    parse_git_conflicted_files, parse_git_local_changes,
    parse_git_name_status_z, GitNameStatusEntryWire,
    GIT_QUERY_WIRE_SCHEMA_VERSION,
};
use serde_json::json;

fn entry(status: &str, path: &str) -> GitNameStatusEntryWire {
    GitNameStatusEntryWire {
        status: status.to_string(),
        path: path.to_string(),
    }
}

// -- parse_git_name_status_z ----------------------------------------------

#[test]
fn parse_name_status_empty_stream_returns_empty_list() {
    assert!(parse_git_name_status_z("").is_empty());
}

#[test]
fn parse_name_status_trailing_nul_is_ignored() {
    let stream = "M\0src/a.py\0";
    assert_eq!(
        parse_git_name_status_z(stream),
        vec![entry("M", "src/a.py")]
    );
}

#[test]
fn parse_name_status_simple_status_letters() {
    let stream =
        "A\0added.py\0M\0modified.py\0D\0deleted.py\0T\0type_changed.py\0U\0conflicted.py\0";
    assert_eq!(
        parse_git_name_status_z(stream),
        vec![
            entry("A", "added.py"),
            entry("M", "modified.py"),
            entry("D", "deleted.py"),
            entry("T", "type_changed.py"),
            entry("U", "conflicted.py"),
        ]
    );
}

#[test]
fn parse_name_status_rename_with_score_carries_paired_paths() {
    let stream = "R100\0old_name.py\0new_name.py\0";
    assert_eq!(
        parse_git_name_status_z(stream),
        vec![entry("R100", "old_name.py\tnew_name.py")]
    );
}

#[test]
fn parse_name_status_copy_with_score_carries_paired_paths() {
    let stream = "C75\0src/orig.py\0src/copy.py\0";
    assert_eq!(
        parse_git_name_status_z(stream),
        vec![entry("C75", "src/orig.py\tsrc/copy.py")]
    );
}

#[test]
fn parse_name_status_mixed_simple_and_rename_in_one_stream() {
    let stream = "M\0a.py\0R100\0b_old.py\0b_new.py\0D\0c.py\0";
    assert_eq!(
        parse_git_name_status_z(stream),
        vec![
            entry("M", "a.py"),
            entry("R100", "b_old.py\tb_new.py"),
            entry("D", "c.py"),
        ]
    );
}

#[test]
fn parse_name_status_truncated_status_only_drops_entry() {
    let stream = "M\0a.py\0M";
    assert_eq!(parse_git_name_status_z(stream), vec![entry("M", "a.py")]);
}

#[test]
fn parse_name_status_truncated_rename_falls_back_to_single_path() {
    let stream = "M\0a.py\0R100\0only_one_path.py";
    assert_eq!(
        parse_git_name_status_z(stream),
        vec![entry("M", "a.py"), entry("R100", "only_one_path.py"),]
    );
}

#[test]
fn parse_name_status_skips_empty_status_tokens() {
    let stream = "\0M\0a.py\0";
    assert_eq!(parse_git_name_status_z(stream), vec![entry("M", "a.py")]);
}

// -- parse_git_branch_name ------------------------------------------------

#[test]
fn parse_branch_name_simple_value() {
    assert_eq!(
        parse_git_branch_name("feature-x\n"),
        Some("feature-x".to_string())
    );
}

#[test]
fn parse_branch_name_detached_head_returns_none() {
    assert_eq!(parse_git_branch_name("HEAD\n"), None);
}

#[test]
fn parse_branch_name_empty_stdout_returns_none() {
    assert_eq!(parse_git_branch_name(""), None);
}

#[test]
fn parse_branch_name_whitespace_only_returns_none() {
    assert_eq!(parse_git_branch_name("   \n"), None);
}

#[test]
fn parse_branch_name_strips_surrounding_whitespace() {
    assert_eq!(
        parse_git_branch_name("  feature-x  \n"),
        Some("feature-x".to_string())
    );
}

// -- derive_git_workspace_name -------------------------------------------

#[test]
fn derive_workspace_name_https_remote_with_dot_git_suffix() {
    assert_eq!(
        derive_git_workspace_name(
            Some("https://github.com/sase-org/sase_100.git"),
            None
        ),
        Some("sase_100".to_string())
    );
}

#[test]
fn derive_workspace_name_https_remote_without_dot_git_suffix() {
    assert_eq!(
        derive_git_workspace_name(
            Some("https://github.com/sase-org/sase_100"),
            None
        ),
        Some("sase_100".to_string())
    );
}

#[test]
fn derive_workspace_name_ssh_remote_with_dot_git_suffix() {
    assert_eq!(
        derive_git_workspace_name(
            Some("git@github.com:sase-org/sase_100.git"),
            None
        ),
        Some("sase_100".to_string())
    );
}

#[test]
fn derive_workspace_name_path_like_remote() {
    assert_eq!(
        derive_git_workspace_name(Some("/srv/git/sase_100.git"), None),
        Some("sase_100".to_string())
    );
}

#[test]
fn derive_workspace_name_falls_back_to_root_path_when_remote_blank() {
    assert_eq!(
        derive_git_workspace_name(
            Some(""),
            Some("/home/user/projects/sase_100")
        ),
        Some("sase_100".to_string())
    );
}

#[test]
fn derive_workspace_name_falls_back_to_root_path_when_remote_none() {
    assert_eq!(
        derive_git_workspace_name(None, Some("/home/user/projects/sase_100")),
        Some("sase_100".to_string())
    );
}

#[test]
fn derive_workspace_name_remote_takes_priority_over_root() {
    assert_eq!(
        derive_git_workspace_name(
            Some("https://github.com/sase-org/repo_a.git"),
            Some("/tmp/repo_b")
        ),
        Some("repo_a".to_string())
    );
}

#[test]
fn derive_workspace_name_returns_none_when_both_inputs_empty() {
    assert_eq!(derive_git_workspace_name(None, None), None);
    assert_eq!(derive_git_workspace_name(Some(""), Some("")), None);
}

#[test]
fn derive_workspace_name_remote_dot_git_only_returns_none() {
    assert_eq!(
        derive_git_workspace_name(Some(".git"), Some("/tmp/fallback")),
        None
    );
}

// -- parse_git_conflicted_files ------------------------------------------

#[test]
fn parse_conflicted_files_empty_stdout_returns_empty_list() {
    assert!(parse_git_conflicted_files("").is_empty());
}

#[test]
fn parse_conflicted_files_strips_blank_lines() {
    let stdout = "src/a.py\n\nsrc/b.py\n\n";
    assert_eq!(
        parse_git_conflicted_files(stdout),
        vec!["src/a.py".to_string(), "src/b.py".to_string()]
    );
}

#[test]
fn parse_conflicted_files_preserves_path_order() {
    let stdout = "z.py\na.py\nm.py\n";
    assert_eq!(
        parse_git_conflicted_files(stdout),
        vec!["z.py".to_string(), "a.py".to_string(), "m.py".to_string()]
    );
}

#[test]
fn parse_conflicted_files_only_blank_lines_returns_empty() {
    assert!(parse_git_conflicted_files("\n\n   \n").is_empty());
}

// -- parse_git_local_changes ---------------------------------------------

#[test]
fn parse_local_changes_clean_tree_returns_none() {
    assert_eq!(parse_git_local_changes(""), None);
}

#[test]
fn parse_local_changes_whitespace_only_returns_none() {
    assert_eq!(parse_git_local_changes("   \n"), None);
}

#[test]
fn parse_local_changes_dirty_tree_returns_stripped_text() {
    let stdout = "M src/a.py\n?? new.py\n";
    assert_eq!(
        parse_git_local_changes(stdout),
        Some("M src/a.py\n?? new.py".to_string())
    );
}

// -- Wire helpers --------------------------------------------------------

#[test]
fn git_query_wire_schema_version_is_one() {
    assert_eq!(GIT_QUERY_WIRE_SCHEMA_VERSION, 1);
}

#[test]
fn git_name_status_entry_wire_serializes_to_python_shape() {
    let row = GitNameStatusEntryWire {
        status: "R100".to_string(),
        path: "old.py\tnew.py".to_string(),
    };
    let value = serde_json::to_value(&row).unwrap();
    assert_eq!(
        value,
        json!({
            "status": "R100",
            "path": "old.py\tnew.py",
        })
    );
}

#[test]
fn git_name_status_entry_wire_round_trips_through_json() {
    let row = GitNameStatusEntryWire {
        status: "M".to_string(),
        path: "src/a.py".to_string(),
    };
    let value = serde_json::to_value(&row).unwrap();
    let back: GitNameStatusEntryWire = serde_json::from_value(value).unwrap();
    assert_eq!(back, row);
}
