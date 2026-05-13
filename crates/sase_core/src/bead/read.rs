//! Read-only bead store queries.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::jsonl::import_issues_from_jsonl;
use super::wire::{
    BeadError, BeadTierWire, IssueTypeWire, IssueWire, StatusWire,
};

pub const BEAD_READ_WIRE_SCHEMA_VERSION: u64 = 1;

pub fn read_store_issues(
    beads_dir: &Path,
) -> Result<Vec<IssueWire>, BeadError> {
    if !beads_dir.is_dir() {
        return Err(BeadError::io(format!(
            "No beads directory found at {}",
            beads_dir.display()
        )));
    }
    Ok(import_issues_from_jsonl(&beads_dir.join("issues.jsonl"))?.issues)
}

pub fn show_issue(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<IssueWire, BeadError> {
    show_issue_in_issues(read_store_issues(beads_dir)?, issue_id)
}

pub fn list_issues(
    beads_dir: &Path,
    statuses: Option<&[String]>,
    issue_types: Option<&[String]>,
    tiers: Option<&[String]>,
) -> Result<Vec<IssueWire>, BeadError> {
    list_issues_in_issues(
        read_store_issues(beads_dir)?,
        statuses,
        issue_types,
        tiers,
    )
}

pub fn ready_issues(beads_dir: &Path) -> Result<Vec<IssueWire>, BeadError> {
    ready_issues_in_issues(read_store_issues(beads_dir)?)
}

pub fn blocked_issues(beads_dir: &Path) -> Result<Vec<IssueWire>, BeadError> {
    blocked_issues_in_issues(read_store_issues(beads_dir)?)
}

pub fn stats(beads_dir: &Path) -> Result<BTreeMap<String, usize>, BeadError> {
    Ok(stats_for_issues(&read_store_issues(beads_dir)?))
}

pub fn get_epic_children(
    beads_dir: &Path,
    epic_id: &str,
) -> Result<Vec<IssueWire>, BeadError> {
    get_epic_children_in_issues(read_store_issues(beads_dir)?, epic_id)
}

pub fn doctor(beads_dir: &Path) -> Result<Vec<String>, BeadError> {
    if !beads_dir.is_dir() {
        return Err(BeadError::io(format!(
            "No beads directory found at {}",
            beads_dir.display()
        )));
    }

    let mut messages = Vec::new();
    if !beads_dir.join("config.json").exists() {
        messages.push("WARNING: config.json missing".to_string());
    }
    if !beads_dir.join("issues.jsonl").exists() {
        messages.push("WARNING: issues.jsonl missing".to_string());
    }
    if !beads_dir.join("beads.db").exists() {
        messages.push("WARNING: beads.db missing".to_string());
    }

    let issues = read_store_issues(beads_dir)?;
    let ids: BTreeSet<&str> =
        issues.iter().map(|issue| issue.id.as_str()).collect();
    let orphan_ids: Vec<&str> = issues
        .iter()
        .filter(|issue| issue.issue_type == IssueTypeWire::Phase)
        .filter_map(|issue| {
            let parent_id = issue.parent_id.as_deref()?;
            (!ids.contains(parent_id)).then_some(issue.id.as_str())
        })
        .collect();
    if !orphan_ids.is_empty() {
        messages.push(format!(
            "WARNING: orphan children (missing parent): {}",
            orphan_ids.join(", ")
        ));
    }

    if messages.is_empty() {
        messages.push("OK: no issues found".to_string());
    }
    Ok(messages)
}

pub(crate) fn show_issue_in_issues(
    issues: Vec<IssueWire>,
    issue_id: &str,
) -> Result<IssueWire, BeadError> {
    issues
        .into_iter()
        .find(|issue| issue.id == issue_id)
        .ok_or_else(|| BeadError {
            kind: "not_found".to_string(),
            message: format!("Issue not found: {issue_id}"),
        })
}

pub(crate) fn list_issues_in_issues(
    mut issues: Vec<IssueWire>,
    statuses: Option<&[String]>,
    issue_types: Option<&[String]>,
    tiers: Option<&[String]>,
) -> Result<Vec<IssueWire>, BeadError> {
    let statuses = parse_statuses(statuses)?;
    let issue_types = parse_issue_types(issue_types)?;
    let tiers = parse_tiers(tiers)?;
    issues.retain(|issue| {
        statuses
            .as_ref()
            .map_or(true, |values| values.contains(&issue.status))
            && issue_types
                .as_ref()
                .map_or(true, |values| values.contains(&issue.issue_type))
            && tiers.as_ref().map_or(true, |values| {
                issue
                    .tier
                    .as_ref()
                    .is_some_and(|tier| values.contains(tier))
            })
    });
    sort_by_created_at(&mut issues);
    Ok(issues)
}

pub(crate) fn ready_issues_in_issues(
    mut issues: Vec<IssueWire>,
) -> Result<Vec<IssueWire>, BeadError> {
    sort_by_created_at(&mut issues);
    let status_by_id: BTreeMap<String, StatusWire> = issues
        .iter()
        .map(|issue| (issue.id.clone(), issue.status.clone()))
        .collect();
    Ok(issues
        .into_iter()
        .filter(|issue| issue.status == StatusWire::Open)
        .filter(is_ready_surface_issue)
        .filter(|issue| !has_active_blocker(issue, &status_by_id))
        .collect())
}

fn is_ready_surface_issue(issue: &IssueWire) -> bool {
    issue.issue_type == IssueTypeWire::Phase
        || issue.tier == Some(BeadTierWire::Epic)
}

pub(crate) fn blocked_issues_in_issues(
    mut issues: Vec<IssueWire>,
) -> Result<Vec<IssueWire>, BeadError> {
    sort_by_created_at(&mut issues);
    let status_by_id: BTreeMap<String, StatusWire> = issues
        .iter()
        .map(|issue| (issue.id.clone(), issue.status.clone()))
        .collect();
    Ok(issues
        .into_iter()
        .filter(|issue| has_active_blocker(issue, &status_by_id))
        .collect())
}

pub(crate) fn get_epic_children_in_issues(
    mut issues: Vec<IssueWire>,
    epic_id: &str,
) -> Result<Vec<IssueWire>, BeadError> {
    issues.retain(|issue| issue.parent_id.as_deref() == Some(epic_id));
    sort_by_created_at(&mut issues);
    Ok(issues)
}

fn has_active_blocker(
    issue: &IssueWire,
    status_by_id: &BTreeMap<String, StatusWire>,
) -> bool {
    issue.dependencies.iter().any(|dep| {
        status_by_id
            .get(dep.depends_on_id.as_str())
            .is_some_and(|status| {
                matches!(*status, StatusWire::Open | StatusWire::InProgress)
            })
    })
}

pub(crate) fn stats_for_issues(
    issues: &[IssueWire],
) -> BTreeMap<String, usize> {
    let mut stats = BTreeMap::new();
    for issue in issues {
        *stats
            .entry(status_as_str(&issue.status).to_string())
            .or_insert(0) += 1;
        *stats
            .entry(issue_type_as_str(&issue.issue_type).to_string())
            .or_insert(0) += 1;
    }
    stats.insert("total".to_string(), issues.len());
    stats
}

fn parse_statuses(
    statuses: Option<&[String]>,
) -> Result<Option<Vec<StatusWire>>, BeadError> {
    statuses
        .map(|values| values.iter().map(|value| parse_status(value)).collect())
        .transpose()
}

fn parse_issue_types(
    issue_types: Option<&[String]>,
) -> Result<Option<Vec<IssueTypeWire>>, BeadError> {
    issue_types
        .map(|values| {
            values.iter().map(|value| parse_issue_type(value)).collect()
        })
        .transpose()
}

fn parse_status(value: &str) -> Result<StatusWire, BeadError> {
    match value {
        "open" => Ok(StatusWire::Open),
        "in_progress" => Ok(StatusWire::InProgress),
        "closed" => Ok(StatusWire::Closed),
        _ => Err(BeadError::validation(format!(
            "invalid bead status: {value}"
        ))),
    }
}

fn parse_issue_type(value: &str) -> Result<IssueTypeWire, BeadError> {
    match value {
        "plan" => Ok(IssueTypeWire::Plan),
        "phase" => Ok(IssueTypeWire::Phase),
        _ => Err(BeadError::validation(format!(
            "invalid bead issue_type: {value}"
        ))),
    }
}

fn parse_tiers(
    tiers: Option<&[String]>,
) -> Result<Option<Vec<BeadTierWire>>, BeadError> {
    tiers
        .map(|values| values.iter().map(|value| parse_tier(value)).collect())
        .transpose()
}

fn parse_tier(value: &str) -> Result<BeadTierWire, BeadError> {
    match value {
        "plan" => Ok(BeadTierWire::Plan),
        "epic" => Ok(BeadTierWire::Epic),
        "legend" => Ok(BeadTierWire::Legend),
        _ => Err(BeadError::validation(format!("invalid bead tier: {value}"))),
    }
}

fn sort_by_created_at(issues: &mut [IssueWire]) {
    issues.sort_by(|a, b| a.created_at.cmp(&b.created_at));
}

fn status_as_str(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "open",
        StatusWire::InProgress => "in_progress",
        StatusWire::Closed => "closed",
    }
}

fn issue_type_as_str(issue_type: &IssueTypeWire) -> &'static str {
    match issue_type {
        IssueTypeWire::Plan => "plan",
        IssueTypeWire::Phase => "phase",
    }
}
