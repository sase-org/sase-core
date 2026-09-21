//! Store-backed issue identity resolution shared by every command.

use std::path::{Path, PathBuf};

use super::super::read::{
    read_store_issues, resolve_issue_id_in_issues, resolve_issue_ids,
};
use super::super::wire::{BeadError, IssueWire};
use super::dispatch::{error, BeadCliOutcomeWire};

pub(super) fn read_issues(
    _read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
) -> Result<Vec<IssueWire>, BeadError> {
    read_store_issues(write_beads_dir)
}

pub(super) fn find_issue<'a>(
    issues: &'a [IssueWire],
    issue_id: &str,
) -> Option<&'a IssueWire> {
    issues.iter().find(|issue| issue.id == issue_id)
}

pub(super) fn resolve_cli_issue_id(
    issues: &[IssueWire],
    issue_id: &str,
) -> Result<String, BeadError> {
    resolve_issue_id_in_issues(issues, issue_id)
}

pub(super) fn resolve_cli_parent_id(
    issues: &[IssueWire],
    issue_id: &str,
) -> Result<String, BeadError> {
    resolve_issue_id_in_issues(issues, issue_id)
}

pub(super) fn resolve_cli_issue_ids(
    beads_dir: &Path,
    issue_ids: &[String],
) -> Result<Vec<String>, BeadError> {
    resolve_issue_ids(beads_dir, issue_ids)
}

pub(super) fn issue_resolution_outcome(
    requested_issue_id: &str,
    err: BeadError,
) -> BeadCliOutcomeWire {
    if err.kind == "not_found" {
        let issue_id = err
            .message
            .strip_prefix("Issue not found: ")
            .unwrap_or(requested_issue_id);
        error(format!("Error: issue not found: {issue_id}\n"))
    } else {
        error(format!("Error: {}\n", err.message))
    }
}

pub(super) fn parent_resolution_outcome(
    requested_parent_id: &str,
    err: BeadError,
) -> BeadCliOutcomeWire {
    if err.kind == "not_found" {
        error(format!(
            "Error: parent bead not found: {requested_parent_id}\n"
        ))
    } else {
        error(format!("Error: {}\n", err.message))
    }
}

pub(super) fn issue_ids_resolution_outcome(
    err: BeadError,
) -> BeadCliOutcomeWire {
    issue_resolution_outcome(&err.message.clone(), err)
}
