//! Bead `create` handler, its argument parsing, and the design-storage
//! path resolution it writes through.

use std::fs;
use std::path::{Path, PathBuf};

use crate::plan::canonicalize_plan_reference;

use super::super::mutation::{create_issue, BeadCreateRequestWire};
use super::super::read::read_store_issues;
use super::super::wire::{BeadError, BeadTierWire, IssueTypeWire};
use super::dispatch::{
    defer, error, mutation_summary, success_with_mutation, BeadCliOutcomeWire,
};
use super::parsing::parse_tier;
use super::presentation::issue_type_value;
use super::resolution::{parent_resolution_outcome, resolve_cli_parent_id};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CreateArgs {
    title: String,
    issue_type: IssueTypeWire,
    parent_id: Option<String>,
    plan_path: Option<String>,
    description: String,
    assignee: String,
    tier: Option<BeadTierWire>,
    changespec_name: String,
    changespec_bug_id: String,
    external_ref: String,
    model: String,
    refs: Vec<String>,
}

pub(super) fn handle_create(
    args: &[String],
    write_beads_dir: &Path,
    cwd: &Path,
    _relativize_design_paths: bool,
) -> Result<BeadCliOutcomeWire, BeadError> {
    let mut parsed = match parse_create_args(args) {
        Ok(Some(parsed)) => parsed,
        Ok(None) => return Ok(defer()),
        Err(message) => return Ok(error(format!("Error: {message}\n"))),
    };

    if parsed.issue_type != IssueTypeWire::Plan
        && (!parsed.changespec_name.is_empty()
            || !parsed.changespec_bug_id.is_empty())
    {
        return Ok(error(
            "Error: Patch metadata can only be attached to plan beads\n"
                .to_string(),
        ));
    }
    if !parsed.changespec_bug_id.is_empty() && parsed.changespec_name.is_empty()
    {
        return Ok(error(
            "Error: --bug-id requires --changespec\n".to_string(),
        ));
    }
    if parsed.issue_type != IssueTypeWire::Plan && parsed.tier.is_some() {
        return Ok(error(
            "Error: --tier can only be set on plan beads\n".to_string(),
        ));
    }
    if let Some(parent_id) = parsed.parent_id.as_deref() {
        let issues = read_store_issues(write_beads_dir).unwrap_or_default();
        parsed.parent_id = match resolve_cli_parent_id(&issues, parent_id) {
            Ok(parent_id) => Some(parent_id),
            Err(err) => return Ok(parent_resolution_outcome(parent_id, err)),
        };
    }

    let design = match parsed.plan_path.as_deref() {
        Some(plan_path) => {
            match storage_design_path(plan_path, cwd, write_beads_dir) {
                Ok(path) => path,
                Err(message) => {
                    return Ok(error(format!("Error: {message}\n")))
                }
            }
        }
        None => String::new(),
    };
    let request = BeadCreateRequestWire {
        title: parsed.title,
        issue_type: parsed.issue_type,
        tier: parsed.tier,
        parent_id: parsed.parent_id,
        description: parsed.description,
        design,
        model: parsed.model,
        assignee: parsed.assignee,
        changespec_name: parsed.changespec_name,
        changespec_bug_id: parsed.changespec_bug_id,
        external_ref: parsed.external_ref,
        refs: parsed.refs,
        ..BeadCreateRequestWire::default()
    };
    match create_issue(write_beads_dir, request) {
        Ok(outcome) => {
            let issue =
                outcome.issue.as_ref().expect("create outcome has issue");
            Ok(success_with_mutation(
                format!(
                    "Created {}: {} — {}\n",
                    issue_type_value(&issue.issue_type),
                    issue.id,
                    issue.title
                ),
                mutation_summary("create", &outcome, None),
            ))
        }
        Err(err) if err.kind == "validation" || err.kind == "not_found" => {
            Ok(error(format!("Error: {}\n", err.message)))
        }
        Err(err) => Err(err),
    }
}

pub(super) fn storage_design_path(
    raw_path: &str,
    cwd: &Path,
    write_beads_dir: &Path,
) -> Result<String, String> {
    let supplied = Path::new(raw_path);
    let resolved = if supplied.is_absolute() {
        supplied.to_path_buf()
    } else {
        cwd.join(supplied)
    };
    if !resolved.is_file() {
        return Err(format!("plan file not found: {raw_path}"));
    }
    let normalized = fs::canonicalize(&resolved).unwrap_or(resolved);
    let storage_root = design_storage_root(cwd, write_beads_dir);
    // Canonicalize both sides of the relativization: on platforms where the
    // workspace sits under a symlinked ancestor (/tmp -> /private/tmp on
    // macOS) `normalized` is resolved while `storage_root` is
    // caller-supplied, so a one-sided strip_prefix never matches and every
    // design path degrades to an absolute, machine-specific value.
    let canonical_storage_root = fs::canonicalize(storage_root)
        .unwrap_or_else(|_| storage_root.to_path_buf());
    let plan_roots = design_plan_roots(storage_root, write_beads_dir)
        .into_iter()
        .map(|root| fs::canonicalize(&root).unwrap_or(root))
        .collect::<Vec<_>>();
    if let Some(reference) =
        canonicalize_plan_reference(&normalized, &plan_roots)
            .map_err(|err| err.message)?
    {
        return Ok(reference);
    }
    Ok(normalized
        .strip_prefix(&canonical_storage_root)
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| normalized.display().to_string()))
}

pub(super) fn design_plan_roots(
    storage_root: &Path,
    beads_dir: &Path,
) -> Vec<PathBuf> {
    let components = beads_dir
        .components()
        .rev()
        .take(4)
        .map(|component| component.as_os_str())
        .collect::<Vec<_>>();
    if components
        .iter()
        .take(3)
        .map(|value| value.to_string_lossy())
        .eq(["beads", "repos", "sase"])
    {
        return vec![beads_dir
            .parent()
            .expect("matched beads sidecar directory has a parent")
            .join("plans")];
    }
    if components
        .iter()
        .map(|value| value.to_string_lossy())
        .eq(["beads", "plans", "repos", "sase"])
    {
        return vec![beads_dir
            .parent()
            .expect("matched sidecar beads directory has a parent")
            .to_path_buf()];
    }
    if components
        .iter()
        .take(3)
        .map(|value| value.to_string_lossy())
        .eq(["beads", "sdd", ".sase"])
    {
        return vec![beads_dir
            .parent()
            .expect("matched local beads directory has a parent")
            .join("plans")];
    }
    if components
        .iter()
        .take(2)
        .map(|value| value.to_string_lossy())
        .eq(["beads", "sdd"])
    {
        return vec![beads_dir
            .parent()
            .expect("matched in-tree beads directory has a parent")
            .join("plans")];
    }
    vec![storage_root.join("plans")]
}

pub(super) fn design_storage_root<'a>(
    cwd: &'a Path,
    beads_dir: &'a Path,
) -> &'a Path {
    let components = beads_dir
        .components()
        .rev()
        .take(4)
        .map(|component| component.as_os_str())
        .collect::<Vec<_>>();
    if components
        .iter()
        .take(3)
        .map(|value| value.to_string_lossy())
        .eq(["beads", "repos", "sase"])
    {
        return beads_dir
            .ancestors()
            .nth(3)
            .expect("three matched path components have a parent");
    }
    if components
        .iter()
        .map(|value| value.to_string_lossy())
        .eq(["beads", "plans", "repos", "sase"])
    {
        return beads_dir
            .ancestors()
            .nth(4)
            .expect("four matched path components have a parent");
    }
    if components
        .iter()
        .take(3)
        .map(|value| value.to_string_lossy())
        .eq(["beads", "sdd", ".sase"])
    {
        return beads_dir
            .ancestors()
            .nth(3)
            .expect("three matched path components have a parent");
    }
    if components
        .iter()
        .take(2)
        .map(|value| value.to_string_lossy())
        .eq(["beads", "sdd"])
    {
        return beads_dir
            .ancestors()
            .nth(2)
            .expect("two matched path components have a parent");
    }
    cwd
}

pub(super) fn parse_create_args(
    args: &[String],
) -> Result<Option<CreateArgs>, String> {
    let mut title = None;
    let mut type_arg = None;
    let mut description = String::new();
    let mut assignee = String::new();
    let mut tier = None;
    let mut changespec_name = String::new();
    let mut changespec_bug_id = String::new();
    let mut external_ref = String::new();
    let mut model = String::new();
    let mut refs = Vec::new();
    let mut idx = 0;
    while idx < args.len() {
        let arg = &args[idx];
        let (name, value) = if matches!(
            arg.as_str(),
            "-t" | "--title"
                | "-T"
                | "--type"
                | "-d"
                | "--description"
                | "-a"
                | "--assignee"
                | "-r"
                | "--tier"
                | "-c"
                | "--changespec"
                | "-b"
                | "--bug-id"
                | "-x"
                | "--external-ref"
                | "-m"
                | "--model"
                | "-R"
                | "--ref"
        ) {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return Ok(None);
            };
            (arg.as_str(), value.clone())
        } else if let Some((name, value)) = arg.split_once('=') {
            (name, value.to_string())
        } else {
            return Ok(None);
        };
        match name {
            "-t" | "--title" => title = Some(value),
            "-T" | "--type" => type_arg = Some(value),
            "-d" | "--description" => description = value,
            "-a" | "--assignee" => assignee = value,
            "-r" | "--tier" => {
                tier =
                    Some(parse_tier(&value).ok_or_else(|| {
                        format!("invalid --tier value: {value}")
                    })?)
            }
            "-c" | "--changespec" => changespec_name = value,
            "-b" | "--bug-id" => changespec_bug_id = value,
            "-x" | "--external-ref" => external_ref = value,
            "-m" | "--model" => model = value,
            "-R" | "--ref" => refs.push(value),
            _ => return Ok(None),
        }
        idx += 1;
    }
    let (Some(title), Some(type_arg)) = (title, type_arg) else {
        return Ok(None);
    };
    let parsed_type = parse_create_type(&type_arg)?;
    Ok(Some(CreateArgs {
        title,
        issue_type: parsed_type.issue_type,
        parent_id: parsed_type.parent_id,
        plan_path: parsed_type.plan_path,
        description,
        assignee,
        tier,
        changespec_name,
        changespec_bug_id,
        external_ref,
        model,
        refs,
    }))
}

pub(super) const CREATE_TYPE_EXPECTED: &str = "Expected: plan(<plan_file>), plan(<plan_file>,<parent_id>), phase(<parent_id>), or task";

#[derive(Debug)]
pub(super) struct ParsedCreateType {
    issue_type: IssueTypeWire,
    plan_path: Option<String>,
    parent_id: Option<String>,
}

pub(super) fn parse_create_type(
    value: &str,
) -> Result<ParsedCreateType, String> {
    if value == "task" {
        return Ok(ParsedCreateType {
            issue_type: IssueTypeWire::Task,
            plan_path: None,
            parent_id: None,
        });
    }
    let Some((kind, rest)) = value.split_once('(') else {
        return Err(format!(
            "invalid --type value: {value}\n{CREATE_TYPE_EXPECTED}"
        ));
    };
    let Some(inner) = rest.strip_suffix(')') else {
        return Err(format!(
            "invalid --type value: {value}\n{CREATE_TYPE_EXPECTED}"
        ));
    };
    let parts = inner
        .split(',')
        .map(str::trim)
        .map(str::to_string)
        .collect::<Vec<_>>();
    match (kind, parts.as_slice()) {
        ("plan", [path]) => Ok(ParsedCreateType {
            issue_type: IssueTypeWire::Plan,
            plan_path: Some(path.clone()),
            parent_id: None,
        }),
        ("plan", [path, parent]) => Ok(ParsedCreateType {
            issue_type: IssueTypeWire::Plan,
            plan_path: Some(path.clone()),
            parent_id: Some(parent.clone()),
        }),
        ("plan", _) => Err(format!(
            "plan() expects 1 or 2 arguments, got {}",
            parts.len()
        )),
        ("phase", [parent]) => Ok(ParsedCreateType {
            issue_type: IssueTypeWire::Phase,
            plan_path: None,
            parent_id: Some(parent.clone()),
        }),
        ("phase", _) => Err(format!(
            "phase() expects exactly 1 argument, got {}",
            parts.len()
        )),
        _ => Err(format!(
            "invalid --type value: {value}\n{CREATE_TYPE_EXPECTED}"
        )),
    }
}
