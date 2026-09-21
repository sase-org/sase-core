//! Bead CLI entry point and outcome plumbing.
//!
//! Dispatches argv to per-command handlers and builds the
//! `BeadCliOutcomeWire` envelope (success, error, usage error, defer)
//! plus mutation summaries.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use super::super::mutation::BeadMutationOutcomeWire;
use super::super::wire::{BeadError, IssueWire};
use super::create_command::handle_create;
use super::mutate_commands::{
    handle_close, handle_dep, handle_open, handle_ref, handle_rm, handle_update,
};
use super::presentation::status_value;
use super::read_commands::{
    handle_blocked, handle_list, handle_ready, handle_search, handle_show,
    handle_stats,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadCliOutcomeWire {
    pub handled: bool,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    #[serde(default)]
    pub mutation_summary: Option<BeadCliMutationSummaryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BeadCliMutationSummaryWire {
    pub operation: String,
    #[serde(default)]
    pub changed: bool,
    #[serde(default)]
    pub issue_ids: Vec<String>,
    #[serde(default)]
    pub status_transitions: Vec<BeadCliStatusTransitionWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadCliStatusTransitionWire {
    pub from_status: String,
    pub to_status: String,
}

pub fn execute_bead_cli(
    argv: &[String],
    read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
    cwd: &Path,
    relativize_design_paths: bool,
    plan_roots: &[PathBuf],
) -> Result<BeadCliOutcomeWire, BeadError> {
    if argv.is_empty() || argv.iter().any(|arg| arg == "-h" || arg == "--help")
    {
        return Ok(defer());
    }

    match argv[0].as_str() {
        "list" => handle_list(&argv[1..], read_beads_dirs, write_beads_dir),
        "show" => handle_show(
            &argv[1..],
            read_beads_dirs,
            write_beads_dir,
            cwd,
            relativize_design_paths,
            plan_roots,
        ),
        "search" => handle_search(
            &argv[1..],
            read_beads_dirs,
            write_beads_dir,
            cwd,
            relativize_design_paths,
            plan_roots,
        ),
        "ready" => handle_ready(&argv[1..], read_beads_dirs, write_beads_dir),
        "blocked" => {
            handle_blocked(&argv[1..], read_beads_dirs, write_beads_dir)
        }
        "stats" => handle_stats(&argv[1..], read_beads_dirs, write_beads_dir),
        "create" => handle_create(
            &argv[1..],
            write_beads_dir,
            cwd,
            relativize_design_paths,
        ),
        "open" => handle_open(&argv[1..], write_beads_dir),
        "ref" => handle_ref(&argv[1..], write_beads_dir),
        "update" => handle_update(&argv[1..], write_beads_dir),
        "close" => handle_close(&argv[1..], write_beads_dir),
        "dep" => handle_dep(&argv[1..], write_beads_dir),
        "rm" => handle_rm(&argv[1..], write_beads_dir),
        _ => Ok(defer()),
    }
}

pub(super) fn mutation_summary(
    operation: &str,
    outcome: &BeadMutationOutcomeWire,
    old_issue: Option<&IssueWire>,
) -> BeadCliMutationSummaryWire {
    let mut status_transitions = Vec::new();
    if let (Some(old), Some(new)) = (old_issue, outcome.issue.as_ref()) {
        if old.status != new.status {
            status_transitions.push(BeadCliStatusTransitionWire {
                from_status: status_value(&old.status).to_string(),
                to_status: status_value(&new.status).to_string(),
            });
        }
    }
    BeadCliMutationSummaryWire {
        operation: operation.to_string(),
        changed: outcome.changed,
        issue_ids: outcome.issue_ids.clone(),
        status_transitions,
    }
}

pub(super) fn success(stdout: String) -> BeadCliOutcomeWire {
    BeadCliOutcomeWire {
        handled: true,
        exit_code: 0,
        stdout,
        stderr: String::new(),
        mutation_summary: None,
    }
}

pub(super) fn success_with_mutation(
    stdout: String,
    mutation_summary: BeadCliMutationSummaryWire,
) -> BeadCliOutcomeWire {
    BeadCliOutcomeWire {
        handled: true,
        exit_code: 0,
        stdout,
        stderr: String::new(),
        mutation_summary: Some(mutation_summary),
    }
}

pub(super) fn error(stderr: String) -> BeadCliOutcomeWire {
    BeadCliOutcomeWire {
        handled: true,
        exit_code: 1,
        stdout: String::new(),
        stderr,
        mutation_summary: None,
    }
}

pub(super) fn usage_error(stderr: String) -> BeadCliOutcomeWire {
    BeadCliOutcomeWire {
        handled: true,
        exit_code: 2,
        stdout: String::new(),
        stderr,
        mutation_summary: None,
    }
}

pub(super) fn defer() -> BeadCliOutcomeWire {
    BeadCliOutcomeWire {
        handled: false,
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
        mutation_summary: None,
    }
}
