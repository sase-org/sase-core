//! Read-only bead store queries.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use super::events::{
    apply_event, merge_stream_events, reduce_event_streams,
    validated_event_streams, BeadEventOperationWire, BeadEventStreamWire,
};
use super::jsonl::{
    event_manifest_path, event_store_present, event_streams_dir,
    import_issues_from_jsonl, read_event_store,
};
use super::wire::{
    flag_removal_due, BeadError, BeadTierWire, IssueTypeWire, IssueWire,
    StatusWire,
};
use crate::artifact_ref::{resolve_artifact_ref_list, ArtifactRefContextWire};
use crate::plan::resolve_plan_reference;

pub const BEAD_READ_WIRE_SCHEMA_VERSION: u64 = 1;
const REDUNDANT_CLOSE_RECENT_WINDOW_DAYS: i64 = 7;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadProjectionDriftWire {
    pub issue_id: String,
    pub changed_fields: Vec<String>,
    pub current: Option<IssueWire>,
    pub reduced: Option<IssueWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadDoctorReportWire {
    pub messages: Vec<String>,
    pub projection_drift: Vec<BeadProjectionDriftWire>,
    pub redundant_close_events: usize,
    pub redundant_close_issues: usize,
    pub redundant_close_events_recent: usize,
    pub redundant_close_recent_window_days: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadIssueDetailWire {
    pub issue: IssueWire,
    pub ancestors: Vec<Option<IssueWire>>,
    pub children: Vec<IssueWire>,
    pub depends_on: Vec<Option<IssueWire>>,
    pub blocks: Vec<IssueWire>,
}

pub fn read_store_issues(
    beads_dir: &Path,
) -> Result<Vec<IssueWire>, BeadError> {
    if !beads_dir.is_dir() {
        return Err(BeadError::io(format!(
            "No beads directory found at {}",
            beads_dir.display()
        )));
    }
    if event_store_present(beads_dir) {
        return read_event_store_issues(beads_dir);
    }
    read_legacy_jsonl_issues(beads_dir)
}

pub fn read_event_store_issues(
    beads_dir: &Path,
) -> Result<Vec<IssueWire>, BeadError> {
    let (_manifest, streams) = read_event_store(beads_dir)?;
    reduce_event_streams(&streams)
}

pub fn read_legacy_jsonl_issues(
    beads_dir: &Path,
) -> Result<Vec<IssueWire>, BeadError> {
    Ok(import_issues_from_jsonl(&beads_dir.join("issues.jsonl"))?.issues)
}

pub fn show_issue(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<IssueWire, BeadError> {
    show_issue_in_issues(read_store_issues(beads_dir)?, issue_id)
}

pub fn show_issue_detail(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<BeadIssueDetailWire, BeadError> {
    let issues = read_store_issues(beads_dir)?;
    let resolved_id = resolve_issue_id_in_issues(&issues, issue_id)?;
    show_issue_detail_in_issues(&issues, &resolved_id)
}

pub fn resolve_issue_id(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<String, BeadError> {
    resolve_issue_id_in_issues(&read_store_issues(beads_dir)?, issue_id)
}

pub fn resolve_issue_ids(
    beads_dir: &Path,
    issue_ids: &[String],
) -> Result<Vec<String>, BeadError> {
    let issues = read_store_issues(beads_dir)?;
    issue_ids
        .iter()
        .map(|issue_id| resolve_issue_id_in_issues(&issues, issue_id))
        .collect()
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
    Ok(doctor_report_impl(
        beads_dir,
        PlanRootMode::NotRequested,
        ReferenceContextMode::NotRequested,
    )?
    .messages)
}

pub fn doctor_with_plan_roots(
    beads_dir: &Path,
    plan_roots: Option<&[PathBuf]>,
) -> Result<Vec<String>, BeadError> {
    Ok(doctor_report_impl(
        beads_dir,
        plan_roots.map_or(PlanRootMode::Unavailable, PlanRootMode::Available),
        ReferenceContextMode::NotRequested,
    )?
    .messages)
}

pub fn doctor_with_contexts(
    beads_dir: &Path,
    plan_roots: Option<&[PathBuf]>,
    reference_context: Option<&ArtifactRefContextWire>,
) -> Result<Vec<String>, BeadError> {
    Ok(
        doctor_report_with_contexts(beads_dir, plan_roots, reference_context)?
            .messages,
    )
}

pub fn doctor_report(
    beads_dir: &Path,
) -> Result<BeadDoctorReportWire, BeadError> {
    doctor_report_impl(
        beads_dir,
        PlanRootMode::NotRequested,
        ReferenceContextMode::NotRequested,
    )
}

pub fn doctor_report_with_contexts(
    beads_dir: &Path,
    plan_roots: Option<&[PathBuf]>,
    reference_context: Option<&ArtifactRefContextWire>,
) -> Result<BeadDoctorReportWire, BeadError> {
    doctor_report_impl(
        beads_dir,
        plan_roots.map_or(PlanRootMode::Unavailable, PlanRootMode::Available),
        reference_context.map_or(
            ReferenceContextMode::Unavailable,
            ReferenceContextMode::Available,
        ),
    )
}

enum PlanRootMode<'a> {
    NotRequested,
    Unavailable,
    Available(&'a [PathBuf]),
}

enum ReferenceContextMode<'a> {
    NotRequested,
    Unavailable,
    Available(&'a ArtifactRefContextWire),
}

fn doctor_report_impl(
    beads_dir: &Path,
    plan_root_mode: PlanRootMode<'_>,
    reference_context_mode: ReferenceContextMode<'_>,
) -> Result<BeadDoctorReportWire, BeadError> {
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
    let event_store_is_present = event_store_present(beads_dir);
    let manifest_path = event_manifest_path(beads_dir);
    let streams_dir = event_streams_dir(beads_dir);
    let legacy_path = beads_dir.join("issues.jsonl");

    if event_store_is_present {
        if !manifest_path.exists() {
            messages.push("WARNING: bead events manifest missing".to_string());
        }
        if !streams_dir.is_dir() {
            messages.push("WARNING: bead event streams missing".to_string());
        }
    }
    if !legacy_path.exists() {
        messages.push("WARNING: issues.jsonl missing".to_string());
    }
    if !beads_dir.join("beads.db").exists() {
        messages.push("WARNING: beads.db missing".to_string());
    }

    let (issues, streams) = if event_store_is_present {
        match read_event_store(beads_dir) {
            Ok((_manifest, streams)) => {
                (reduce_event_streams(&streams)?, Some(streams))
            }
            Err(err) => {
                messages.push(format!(
                    "ERROR: invalid bead event store: {}",
                    err.message
                ));
                return Ok(empty_doctor_report(messages));
            }
        }
    } else {
        (read_legacy_jsonl_issues(beads_dir)?, None)
    };
    let (redundant_close_events, redundant_close_issues, redundant_recent) =
        match streams.as_deref() {
            Some(streams) => redundant_close_census(streams)?,
            None => (0, 0, 0),
        };
    let orphan_ids = orphan_phase_ids(&issues);
    if !orphan_ids.is_empty() {
        messages.push(format!(
            "WARNING: orphan phase records after reduction: {}",
            orphan_ids.join(", ")
        ));
    }
    let orphan_plan_ids = orphan_nested_plan_ids(&issues);
    if !orphan_plan_ids.is_empty() {
        messages.push(format!(
            "WARNING: orphan nested plan records after reduction: {}",
            orphan_plan_ids.join(", ")
        ));
    }
    match plan_root_mode {
        PlanRootMode::NotRequested => {}
        PlanRootMode::Unavailable => messages.push(
            "NOTE: bead design reference validation skipped: plan roots unavailable"
                .to_string(),
        ),
        PlanRootMode::Available(plan_roots) => {
            messages.extend(design_reference_diagnostics(&issues, plan_roots));
        }
    }
    match reference_context_mode {
        ReferenceContextMode::NotRequested => {}
        ReferenceContextMode::Unavailable => messages.push(
            "NOTE: bead artifact reference validation skipped: reference context unavailable"
                .to_string(),
        ),
        ReferenceContextMode::Available(context) => {
            messages.extend(reference_diagnostics(&issues, context));
        }
    }

    let mut projection_drift = Vec::new();
    if event_store_is_present && legacy_path.exists() {
        let legacy_issues = read_legacy_jsonl_issues(beads_dir)?;
        let legacy_orphan_ids = orphan_phase_ids(&legacy_issues);
        if !legacy_orphan_ids.is_empty() {
            messages.push(format!(
                "WARNING: orphan phase records in issues.jsonl: {}",
                legacy_orphan_ids.join(", ")
            ));
        }
        let legacy_orphan_plan_ids = orphan_nested_plan_ids(&legacy_issues);
        if !legacy_orphan_plan_ids.is_empty() {
            messages.push(format!(
                "WARNING: orphan nested plan records in issues.jsonl: {}",
                legacy_orphan_plan_ids.join(", ")
            ));
        }
        projection_drift = compute_projection_drift(&legacy_issues, &issues)?;
        if !projection_drift.is_empty() {
            messages.push(format!(
                "WARNING: issues.jsonl is {} row(s) stale versus the canonical \
                 event streams; run 'sase bead doctor --fix-projection'",
                projection_drift.len()
            ));
        }
    }
    if redundant_close_events > 0 {
        messages.push(format!(
            "NOTE: {redundant_close_events} redundant close event(s) across \
             {redundant_close_issues} bead(s); {redundant_recent} in the last \
             {REDUNDANT_CLOSE_RECENT_WINDOW_DAYS} days"
        ));
    }

    if messages.is_empty() {
        messages.push("OK: no issues found".to_string());
    }
    Ok(BeadDoctorReportWire {
        messages,
        projection_drift,
        redundant_close_events,
        redundant_close_issues,
        redundant_close_events_recent: redundant_recent,
        redundant_close_recent_window_days: REDUNDANT_CLOSE_RECENT_WINDOW_DAYS,
    })
}

fn empty_doctor_report(messages: Vec<String>) -> BeadDoctorReportWire {
    BeadDoctorReportWire {
        messages,
        projection_drift: Vec::new(),
        redundant_close_events: 0,
        redundant_close_issues: 0,
        redundant_close_events_recent: 0,
        redundant_close_recent_window_days: REDUNDANT_CLOSE_RECENT_WINDOW_DAYS,
    }
}

fn redundant_close_census(
    streams: &[BeadEventStreamWire],
) -> Result<(usize, usize, usize), BeadError> {
    let streams = validated_event_streams(streams)?;
    let mut issues = BTreeMap::new();
    let mut issue_ids = BTreeSet::new();
    let mut event_count = 0;
    let mut recent_count = 0;
    let now: DateTime<Utc> = SystemTime::now().into();
    let cutoff = now - Duration::days(REDUNDANT_CLOSE_RECENT_WINDOW_DAYS);

    for event in merge_stream_events(&streams) {
        if event.operation == BeadEventOperationWire::IssueClosed
            && issues
                .get(&event.issue_id)
                .is_some_and(|issue: &IssueWire| {
                    issue.status == StatusWire::Closed
                        && issue.closed_at.is_some()
                })
        {
            event_count += 1;
            issue_ids.insert(event.issue_id.clone());
            if DateTime::parse_from_rfc3339(&event.timestamp)
                .map(|timestamp| {
                    let timestamp = timestamp.with_timezone(&Utc);
                    timestamp >= cutoff && timestamp <= now
                })
                .unwrap_or(false)
            {
                recent_count += 1;
            }
        }
        apply_event(&mut issues, event)?;
    }
    Ok((event_count, issue_ids.len(), recent_count))
}

fn compute_projection_drift(
    current: &[IssueWire],
    reduced: &[IssueWire],
) -> Result<Vec<BeadProjectionDriftWire>, BeadError> {
    let current_by_id = current
        .iter()
        .map(|issue| (issue.id.as_str(), issue))
        .collect::<BTreeMap<_, _>>();
    let reduced_by_id = reduced
        .iter()
        .map(|issue| (issue.id.as_str(), issue))
        .collect::<BTreeMap<_, _>>();
    let ids = current_by_id
        .keys()
        .chain(reduced_by_id.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut drift = Vec::new();

    for issue_id in ids {
        let current_issue = current_by_id.get(issue_id).copied();
        let reduced_issue = reduced_by_id.get(issue_id).copied();
        if current_issue == reduced_issue {
            continue;
        }
        let changed_fields = match (current_issue, reduced_issue) {
            (Some(current), Some(reduced)) => {
                changed_issue_fields(current, reduced)?
            }
            _ => vec!["row".to_string()],
        };
        drift.push(BeadProjectionDriftWire {
            issue_id: issue_id.to_string(),
            changed_fields,
            current: current_issue.cloned(),
            reduced: reduced_issue.cloned(),
        });
    }
    Ok(drift)
}

fn changed_issue_fields(
    current: &IssueWire,
    reduced: &IssueWire,
) -> Result<Vec<String>, BeadError> {
    let current = serde_json::to_value(current)?;
    let reduced = serde_json::to_value(reduced)?;
    let current = current.as_object().ok_or_else(|| {
        BeadError::json("serialized current issue is not an object")
    })?;
    let reduced = reduced.as_object().ok_or_else(|| {
        BeadError::json("serialized reduced issue is not an object")
    })?;
    Ok(current
        .keys()
        .chain(reduced.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|field| current.get(*field) != reduced.get(*field))
        .cloned()
        .collect())
}

pub fn reference_diagnostics(
    issues: &[IssueWire],
    context: &ArtifactRefContextWire,
) -> Vec<String> {
    let entries = issues
        .iter()
        .flat_map(|issue| {
            issue
                .refs
                .iter()
                .map(move |reference| (issue.id.as_str(), reference.as_str()))
        })
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return Vec::new();
    }
    let raw = entries
        .iter()
        .map(|(_, reference)| *reference)
        .collect::<Vec<_>>();
    let resolution = match resolve_artifact_ref_list(&raw, context) {
        Ok(resolution) => resolution,
        Err(_) => {
            return vec![
                "NOTE: bead artifact reference validation skipped: reference context unavailable"
                    .to_string(),
            ];
        }
    };

    let mut unknown = Vec::new();
    let mut missing = Vec::new();
    let mut ambiguous = Vec::new();
    for ((issue_id, reference), entry) in
        entries.iter().zip(resolution.entries.iter())
    {
        let rendered = format!("{issue_id} [{reference}]");
        match entry.resolution.status.as_str() {
            "unknown_kind" | "unknown_repo" | "unknown_project" => {
                unknown.push(rendered);
            }
            "missing" => missing.push(rendered),
            "ambiguous" => ambiguous.push(rendered),
            "vcs_backed" | "exact" | "drifted" => {}
            _ => {}
        }
    }

    let mut messages = Vec::new();
    if !unknown.is_empty() {
        messages.push(format!(
            "WARNING: artifact references with unknown kinds ({}): {}",
            unknown.len(),
            unknown.join(", ")
        ));
    }
    if !missing.is_empty() {
        messages.push(format!(
            "WARNING: unresolvable artifact references ({}): {}",
            missing.len(),
            missing.join(", ")
        ));
    }
    if !ambiguous.is_empty() {
        messages.push(format!(
            "WARNING: ambiguous artifact references ({}): {}",
            ambiguous.len(),
            ambiguous.join(", ")
        ));
    }
    messages
}

fn design_reference_diagnostics(
    issues: &[IssueWire],
    plan_roots: &[PathBuf],
) -> Vec<String> {
    let mut missing_or_malformed = Vec::new();
    let mut ambiguous = Vec::new();
    let mut owner_mismatches = Vec::new();

    for issue in issues
        .iter()
        .filter(|issue| !issue.design.trim().is_empty())
    {
        let resolution =
            match resolve_plan_reference(issue.design.trim(), plan_roots) {
                Ok(resolution) => resolution,
                Err(_) => {
                    missing_or_malformed
                        .push(format!("{} [{}]", issue.id, issue.design));
                    continue;
                }
            };
        match resolution.status.as_str() {
            "missing" => missing_or_malformed
                .push(format!("{} [{}]", issue.id, issue.design)),
            "ambiguous" => {
                ambiguous.push(format!("{} [{}]", issue.id, issue.design));
            }
            "exact" | "drifted" => {
                let Some(resolved_path) = resolution.resolved_path else {
                    continue;
                };
                let Some(owner) = read_plan_owner(Path::new(&resolved_path))
                else {
                    continue;
                };
                if owner != issue.id {
                    owner_mismatches.push(format!(
                        "{} (plan names {}; {})",
                        issue.id, owner, issue.design
                    ));
                }
            }
            _ => {}
        }
    }

    let mut messages = Vec::new();
    if !missing_or_malformed.is_empty() {
        messages.push(format!(
            "WARNING: missing or malformed bead design references ({}): {}",
            missing_or_malformed.len(),
            missing_or_malformed.join(", ")
        ));
    }
    if !ambiguous.is_empty() {
        messages.push(format!(
            "WARNING: ambiguous bead design references ({}): {}",
            ambiguous.len(),
            ambiguous.join(", ")
        ));
    }
    if !owner_mismatches.is_empty() {
        messages.push(format!(
            "WARNING: bead design reference owner mismatches ({}): {}",
            owner_mismatches.len(),
            owner_mismatches.join(", ")
        ));
    }
    messages
}

fn read_plan_owner(plan_path: &Path) -> Option<String> {
    let content = std::fs::read_to_string(plan_path).ok()?;
    let frontmatter = content.strip_prefix("---\n")?;
    let end = frontmatter.find("\n---")?;
    let parsed: serde_yaml::Value =
        serde_yaml::from_str(&frontmatter[..end]).ok()?;
    let mapping = parsed.as_mapping()?;
    for field in ["bead_id", "bead"] {
        let value = mapping
            .get(serde_yaml::Value::String(field.to_string()))
            .and_then(serde_yaml::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let Some(value) = value {
            return Some(value.to_string());
        }
    }
    None
}

fn orphan_phase_ids(issues: &[IssueWire]) -> Vec<&str> {
    orphan_child_ids(issues, IssueTypeWire::Phase)
}

fn orphan_nested_plan_ids(issues: &[IssueWire]) -> Vec<&str> {
    orphan_child_ids(issues, IssueTypeWire::Plan)
}

fn orphan_child_ids(
    issues: &[IssueWire],
    issue_type: IssueTypeWire,
) -> Vec<&str> {
    let ids: BTreeSet<&str> =
        issues.iter().map(|issue| issue.id.as_str()).collect();
    issues
        .iter()
        .filter(|issue| issue.issue_type == issue_type)
        .filter_map(|issue| {
            let parent_id = issue.parent_id.as_deref()?;
            (!ids.contains(parent_id)).then_some(issue.id.as_str())
        })
        .collect()
}

fn show_issue_in_issues(
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

fn show_issue_detail_in_issues(
    issues: &[IssueWire],
    issue_id: &str,
) -> Result<BeadIssueDetailWire, BeadError> {
    let issue = issues
        .iter()
        .find(|issue| issue.id == issue_id)
        .cloned()
        .ok_or_else(|| BeadError {
            kind: "not_found".to_string(),
            message: format!("Issue not found: {issue_id}"),
        })?;

    let ancestors = issue_ancestors_in_issues(issues, &issue)?;
    let mut ordered_issues = issues.to_vec();
    sort_by_created_at(&mut ordered_issues);
    let children = ordered_issues
        .iter()
        .filter(|candidate| candidate.parent_id.as_deref() == Some(issue_id))
        .cloned()
        .collect();
    let depends_on = issue
        .dependencies
        .iter()
        .map(|dependency| {
            resolve_optional_issue_in_issues(issues, &dependency.depends_on_id)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let blocks = ordered_issues
        .into_iter()
        .filter(|candidate| {
            candidate
                .dependencies
                .iter()
                .any(|dependency| dependency.depends_on_id == issue.id)
        })
        .collect();

    Ok(BeadIssueDetailWire {
        issue,
        ancestors,
        children,
        depends_on,
        blocks,
    })
}

fn issue_ancestors_in_issues(
    issues: &[IssueWire],
    issue: &IssueWire,
) -> Result<Vec<Option<IssueWire>>, BeadError> {
    let mut ancestors = Vec::new();
    let mut parent_id = issue.parent_id.clone();
    let mut seen = BTreeSet::from([issue.id.clone()]);
    while let Some(current_parent_id) = parent_id {
        if !seen.insert(current_parent_id.clone()) {
            ancestors.push(None);
            break;
        }
        let Some(parent) =
            resolve_optional_issue_in_issues(issues, &current_parent_id)?
        else {
            ancestors.push(None);
            break;
        };
        parent_id = parent.parent_id.clone();
        ancestors.push(Some(parent));
    }
    Ok(ancestors)
}

fn resolve_optional_issue_in_issues(
    issues: &[IssueWire],
    issue_id: &str,
) -> Result<Option<IssueWire>, BeadError> {
    let resolved_id = match resolve_issue_id_in_issues(issues, issue_id) {
        Ok(resolved_id) => resolved_id,
        Err(error) if error.kind == "not_found" => return Ok(None),
        Err(error) => return Err(error),
    };
    Ok(issues.iter().find(|issue| issue.id == resolved_id).cloned())
}

pub fn resolve_issue_id_in_issues(
    issues: &[IssueWire],
    issue_id: &str,
) -> Result<String, BeadError> {
    if issue_id.is_empty() || issue_id.contains('-') {
        return Ok(issue_id.to_string());
    }
    let mut candidates = issues
        .iter()
        .filter_map(|issue| {
            issue.id.rsplit_once('-').and_then(|(_, suffix)| {
                (suffix == issue_id).then(|| issue.id.clone())
            })
        })
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.dedup();
    match candidates.as_slice() {
        [resolved] => Ok(resolved.clone()),
        [] => Err(BeadError {
            kind: "not_found".to_string(),
            message: format!("Issue not found: {issue_id}"),
        }),
        _ => Err(BeadError {
            kind: "ambiguous".to_string(),
            message: format!(
                "ambiguous bead ID shorthand {issue_id:?}: {}",
                candidates.join(", ")
            ),
        }),
    }
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

fn ready_issues_in_issues(
    mut issues: Vec<IssueWire>,
) -> Result<Vec<IssueWire>, BeadError> {
    sort_by_created_at(&mut issues);
    let status_by_id: BTreeMap<String, StatusWire> = issues
        .iter()
        .map(|issue| (issue.id.clone(), issue.status.clone()))
        .collect();
    Ok(issues
        .into_iter()
        .filter(|issue| {
            issue.status == StatusWire::Ready
                && issue.issue_type == IssueTypeWire::Task
        })
        .filter(|issue| !has_active_blocker(issue, &status_by_id))
        .collect())
}

fn blocked_issues_in_issues(
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

fn get_epic_children_in_issues(
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
                matches!(
                    *status,
                    StatusWire::Open
                        | StatusWire::Claimed
                        | StatusWire::Ready
                        | StatusWire::Snoozed
                        | StatusWire::InProgress
                )
            })
    })
}

fn stats_for_issues(issues: &[IssueWire]) -> BTreeMap<String, usize> {
    let mut stats = BTreeMap::new();
    let mut plus_one_total = 0;
    let today = current_date();
    let release = env!("CARGO_PKG_VERSION");
    for issue in issues {
        *stats
            .entry(status_as_str(&issue.status).to_string())
            .or_insert(0) += 1;
        *stats
            .entry(issue_type_as_str(&issue.issue_type).to_string())
            .or_insert(0) += 1;
        if issue
            .flag
            .as_ref()
            .is_some_and(|flag| flag_removal_due(flag, today, release))
        {
            *stats.entry("due_flag".to_string()).or_insert(0) += 1;
        }
        plus_one_total += issue.plus_one_count();
    }
    stats.insert("total".to_string(), issues.len());
    stats.insert("plus_one".to_string(), plus_one_total);
    stats
}

fn current_date() -> NaiveDate {
    let now: DateTime<Utc> = SystemTime::now().into();
    now.date_naive()
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
        "claimed" => Ok(StatusWire::Claimed),
        "ready" => Ok(StatusWire::Ready),
        "snoozed" => Ok(StatusWire::Snoozed),
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
        "task" => Ok(IssueTypeWire::Task),
        "flag" => Ok(IssueTypeWire::Flag),
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
        _ => Err(BeadError::validation(format!("invalid bead tier: {value}"))),
    }
}

fn sort_by_created_at(issues: &mut [IssueWire]) {
    issues.sort_by(|a, b| a.created_at.cmp(&b.created_at));
}

fn status_as_str(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "open",
        StatusWire::Claimed => "claimed",
        StatusWire::Ready => "ready",
        StatusWire::Snoozed => "snoozed",
        StatusWire::InProgress => "in_progress",
        StatusWire::Closed => "closed",
    }
}

fn issue_type_as_str(issue_type: &IssueTypeWire) -> &'static str {
    match issue_type {
        IssueTypeWire::Plan => "plan",
        IssueTypeWire::Phase => "phase",
        IssueTypeWire::Task => "task",
        IssueTypeWire::Flag => "flag",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_ref::ArtifactRefBeadStoreWire;
    use crate::bead::wire::{DependencyWire, IssueTypeWire};
    use std::fs;
    use tempfile::tempdir;

    fn phase(id: &str, status: StatusWire) -> IssueWire {
        IssueWire {
            id: id.to_string(),
            title: id.to_string(),
            status,
            issue_type: IssueTypeWire::Phase,
            tier: None,
            parent_id: Some("epic".to_string()),
            owner: String::new(),
            assignee: String::new(),
            created_at: String::new(),
            created_by: String::new(),
            updated_at: String::new(),
            closed_at: None,
            close_reason: None,
            resolution: None,
            close_history: Vec::new(),
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            refs: Vec::new(),
            plus_one_evidence: Vec::new(),
            snooze: None,
            flag: None,
            model: String::new(),
            size: None,
            is_ready_to_work: false,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            external_ref: String::new(),
            dependencies: Vec::new(),
        }
    }

    fn task(id: &str, status: StatusWire) -> IssueWire {
        let mut issue = phase(id, status);
        issue.issue_type = IssueTypeWire::Task;
        issue.parent_id = None;
        issue
    }

    #[test]
    fn resolve_issue_id_accepts_full_ids_and_unique_suffixes() {
        let issues = vec![
            task("sase-a1", StatusWire::Open),
            phase("sase-a1.2", StatusWire::Open),
            task("sase-ai-a1", StatusWire::Open),
            task("sase-ai-b7", StatusWire::Open),
        ];

        assert_eq!(
            resolve_issue_id_in_issues(&issues, "sase-a1").unwrap(),
            "sase-a1"
        );
        assert_eq!(
            resolve_issue_id_in_issues(&issues, "sase-missing").unwrap(),
            "sase-missing"
        );
        assert_eq!(
            resolve_issue_id_in_issues(&issues, "a1.2").unwrap(),
            "sase-a1.2"
        );
        assert_eq!(
            resolve_issue_id_in_issues(&issues, "b7").unwrap(),
            "sase-ai-b7"
        );
    }

    #[test]
    fn resolve_issue_id_reports_unknown_and_ambiguous_suffixes() {
        let issues = vec![
            task("sase-a1", StatusWire::Open),
            task("sase-ai-a1", StatusWire::Open),
        ];

        let missing = resolve_issue_id_in_issues(&issues, "zz").unwrap_err();
        assert_eq!(missing.kind, "not_found");
        assert_eq!(missing.message, "Issue not found: zz");

        let ambiguous = resolve_issue_id_in_issues(&issues, "a1").unwrap_err();
        assert_eq!(ambiguous.kind, "ambiguous");
        assert_eq!(
            ambiguous.message,
            "ambiguous bead ID shorthand \"a1\": sase-a1, sase-ai-a1"
        );
    }

    #[test]
    fn claimed_dependency_is_an_active_blocker() {
        let blocker = phase("blocker", StatusWire::Claimed);
        let mut dependent = phase("dependent", StatusWire::Open);
        dependent.dependencies.push(DependencyWire {
            issue_id: dependent.id.clone(),
            depends_on_id: blocker.id.clone(),
            created_at: String::new(),
            created_by: String::new(),
        });

        let blocked =
            blocked_issues_in_issues(vec![blocker, dependent]).unwrap();
        assert_eq!(
            blocked
                .iter()
                .map(|issue| issue.id.as_str())
                .collect::<Vec<_>>(),
            vec!["dependent"]
        );
    }

    #[test]
    fn ready_query_returns_only_unblocked_ready_tasks() {
        let blocker = task("blocker", StatusWire::Ready);
        let mut blocked = task("blocked", StatusWire::Ready);
        blocked.dependencies.push(DependencyWire {
            issue_id: blocked.id.clone(),
            depends_on_id: blocker.id.clone(),
            created_at: String::new(),
            created_by: String::new(),
        });
        let open_task = task("draft", StatusWire::Open);
        let ready_task = task("ready", StatusWire::Ready);
        let phase = phase("phase", StatusWire::Open);

        let ready = ready_issues_in_issues(vec![
            blocker, blocked, open_task, ready_task, phase,
        ])
        .unwrap();

        assert_eq!(
            ready
                .iter()
                .map(|issue| issue.id.as_str())
                .collect::<Vec<_>>(),
            vec!["blocker", "ready"]
        );
    }

    #[test]
    fn claimed_status_filter_is_parsed_and_counted() {
        let issues = vec![
            phase("claimed", StatusWire::Claimed),
            phase("open", StatusWire::Open),
        ];
        let claimed = list_issues_in_issues(
            issues.clone(),
            Some(&["claimed".to_string()]),
            None,
            None,
        )
        .unwrap();

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, "claimed");
        assert_eq!(stats_for_issues(&issues).get("claimed"), Some(&1));
    }

    #[test]
    fn stats_derive_plus_one_total_from_structured_evidence() {
        use crate::bead::wire::TaskPlusOneEvidenceWire;

        let mut corroborated = task("sase-task", StatusWire::Ready);
        corroborated.plus_one_evidence = vec![
            TaskPlusOneEvidenceWire {
                timestamp: "2026-01-01T00:00:00Z".to_string(),
                observed_since: None,
                reporter: "agent-a".to_string(),
                note: "first".to_string(),
                refs: Vec::new(),
            },
            TaskPlusOneEvidenceWire {
                timestamp: "2026-01-02T00:00:00Z".to_string(),
                observed_since: None,
                reporter: "agent-b".to_string(),
                note: "second".to_string(),
                refs: Vec::new(),
            },
        ];

        let stats =
            stats_for_issues(&[corroborated, task("plain", StatusWire::Open)]);

        assert_eq!(stats.get("plus_one"), Some(&2));
    }

    #[test]
    fn reference_diagnostics_groups_unknown_missing_and_ambiguous_entries() {
        let first = tempdir().unwrap();
        let second = tempdir().unwrap();
        for root in [first.path(), second.path()] {
            let page = root.join("pages/foo-1/README.md");
            fs::create_dir_all(page.parent().unwrap()).unwrap();
            fs::write(page, "# bead\n").unwrap();
        }
        let context = ArtifactRefContextWire {
            bead_stores: vec![
                ArtifactRefBeadStoreWire {
                    project: "first".to_string(),
                    prefix: "foo".to_string(),
                    root: first.path().display().to_string(),
                },
                ArtifactRefBeadStoreWire {
                    project: "second".to_string(),
                    prefix: "foo".to_string(),
                    root: second.path().display().to_string(),
                },
            ],
            ..Default::default()
        };
        let mut issue = phase("epic.1", StatusWire::Open);
        issue.refs = vec![
            "reserch:202607/report.md".to_string(),
            "bead:foo-2".to_string(),
            "bead:foo-1".to_string(),
        ];

        let messages = reference_diagnostics(&[issue], &context);

        assert_eq!(messages.len(), 3);
        assert!(messages[0].starts_with(
            "WARNING: artifact references with unknown kinds (1):"
        ));
        assert!(messages[0].contains("epic.1 [reserch:202607/report.md]"));
        assert!(messages[1]
            .starts_with("WARNING: unresolvable artifact references (1):"));
        assert!(messages[1].contains("epic.1 [bead:foo-2]"));
        assert!(messages[2]
            .starts_with("WARNING: ambiguous artifact references (1):"));
        assert!(messages[2].contains("epic.1 [bead:foo-1]"));
    }

    #[test]
    fn doctor_marks_unavailable_reference_context_as_skipped() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        fs::create_dir_all(&beads_dir).unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        fs::write(beads_dir.join("beads.db"), "").unwrap();
        fs::write(beads_dir.join("config.json"), "{}").unwrap();

        let messages = doctor_with_contexts(&beads_dir, None, None).unwrap();

        assert!(messages.iter().any(|message| message
            == "NOTE: bead artifact reference validation skipped: reference context unavailable"));
    }

    #[test]
    fn doctor_ignores_transient_mutation_holder_metadata() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        fs::create_dir_all(&beads_dir).unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        fs::write(beads_dir.join("beads.db"), "").unwrap();
        fs::write(beads_dir.join("config.json"), "{}").unwrap();
        fs::write(
            beads_dir.join(".bead-mutation-lock.holder"),
            r#"{"pid":42,"operation":"test","acquired_at":"now"}"#,
        )
        .unwrap();

        assert_eq!(doctor(&beads_dir).unwrap(), vec!["OK: no issues found"]);
    }
}
