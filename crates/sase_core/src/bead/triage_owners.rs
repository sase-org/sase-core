//! Owner-candidate projection for ToolRun failure triage.
//!
//! Failure triage suggests the task bead that probably owns a failing test
//! or source file. This module owns which beads are eligible and the five
//! narrow fields that cross into the classifier's
//! `ToolRunTriageOwnerCandidateWire`:
//!
//! - only `task` beads whose `task_type` is `ci`, `flake`, or `bug`;
//! - every non-closed status (open, ready, claimed, in-progress, snoozed)
//!   projects as wire status `open`;
//! - a closed task projects as `closed` only when its close time parses
//!   and falls within the seven-day lookback ending at `now_ts`; a closed
//!   task with an unknown or expired close time is excluded;
//! - `node_id` is the bead ID, while `location` carries the task's
//!   `node_id` field (the failing pytest node) in preference to its
//!   `location` field (a source path), and `title` is kept for the
//!   classifier's title fallback.
//!
//! No candidate cap applies: capping by recency before matching could drop
//! an older open bead that owns the exact failing test. The read model
//! serves the warm path from indexed columns plus a JSON projection of the
//! few row fields it does not materialize, so description, design, notes,
//! and relationships are never decoded. The replay below serves stores
//! without a usable cache and returns identical values.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};

use super::read::read_store_issues;
use super::wire::{BeadError, IssueTypeWire, IssueWire, StatusWire};

pub const BEAD_TRIAGE_OWNER_CANDIDATES_WIRE_SCHEMA_VERSION: u64 = 1;

/// Task types whose beads may own a failing check item.
pub const TRIAGE_OWNER_TASK_TYPES: [&str; 3] = ["ci", "flake", "bug"];

/// How long a closed task stays a "possibly fixed" candidate.
///
/// Matches the classifier's `TOOL_RUN_TRIAGE_LOOKBACK_SECS`.
pub const TRIAGE_OWNER_CLOSED_LOOKBACK_SECS: i64 = 7 * 24 * 3600;

/// Wire status of one owner candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BeadTriageOwnerStatusWire {
    Open,
    Closed,
}

/// One owner candidate, shaped exactly like the classifier's
/// `ToolRunTriageOwnerCandidateWire` so it passes through unchanged.
///
/// Every key always serializes (absent values as `null`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTriageOwnerCandidateWire {
    /// The bead ID.
    pub node_id: String,
    /// The task's failing pytest node, else its source path.
    pub location: Option<String>,
    pub title: Option<String>,
    pub status: BeadTriageOwnerStatusWire,
    /// Parsed close time, kept for any task whose `closed_at` parses.
    pub closed_ts: Option<i64>,
}

/// Owner candidates in replay (`created_at`) order plus read diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTriageOwnerCandidatesWire {
    pub schema_version: u64,
    pub candidates: Vec<BeadTriageOwnerCandidateWire>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

/// The narrow fields of one eligible-type task, from either lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OwnerCandidateRow {
    pub id: String,
    pub title: String,
    pub closed: bool,
    pub closed_at: Option<String>,
    pub node_id_field: Option<String>,
    pub location_field: Option<String>,
}

/// Is `task_type` one of the owner-eligible task types?
pub(crate) fn is_owner_task_type(task_type: Option<&str>) -> bool {
    task_type.is_some_and(|value| TRIAGE_OWNER_TASK_TYPES.contains(&value))
}

/// Project eligible rows (already in replay order) through the policy.
pub(crate) fn project_owner_candidates(
    rows: impl IntoIterator<Item = OwnerCandidateRow>,
    now_ts: i64,
) -> BeadTriageOwnerCandidatesWire {
    let cutoff = now_ts.saturating_sub(TRIAGE_OWNER_CLOSED_LOOKBACK_SECS);
    let mut candidates = Vec::new();
    let mut unparseable_close: Vec<String> = Vec::new();
    for row in rows {
        let closed_at = row.closed_at.as_deref().map(str::trim);
        let closed_ts = closed_at.and_then(parse_close_timestamp);
        let status = if row.closed {
            match closed_ts {
                Some(ts) if ts >= cutoff => BeadTriageOwnerStatusWire::Closed,
                Some(_) => continue,
                None => {
                    if closed_at.is_some_and(|value| !value.is_empty()) {
                        unparseable_close.push(row.id);
                    }
                    continue;
                }
            }
        } else {
            BeadTriageOwnerStatusWire::Open
        };
        let location = [row.node_id_field, row.location_field]
            .into_iter()
            .flatten()
            .find(|value| !value.is_empty());
        candidates.push(BeadTriageOwnerCandidateWire {
            node_id: row.id,
            location,
            title: (!row.title.is_empty()).then_some(row.title),
            status,
            closed_ts,
        });
    }
    let mut diagnostics = Vec::new();
    if !unparseable_close.is_empty() {
        let shown: Vec<&str> = unparseable_close
            .iter()
            .take(3)
            .map(String::as_str)
            .collect();
        diagnostics.push(format!(
            "owner candidates skipped {} closed task(s) with an unparseable closed_at: {}",
            unparseable_close.len(),
            shown.join(", ")
        ));
    }
    BeadTriageOwnerCandidatesWire {
        schema_version: BEAD_TRIAGE_OWNER_CANDIDATES_WIRE_SCHEMA_VERSION,
        candidates,
        diagnostics,
    }
}

/// Parse a bead close time into Unix seconds.
///
/// Accepts what the Python gatherer's `datetime.fromisoformat` accepted
/// for bead timestamps: RFC 3339 with `Z` or an offset, an offset-less
/// date-time read as UTC (`T` or space separator, optional seconds and
/// fraction), or a bare date at UTC midnight.
pub(crate) fn parse_close_timestamp(value: &str) -> Option<i64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return Some(parsed.timestamp());
    }
    for format in [
        "%Y-%m-%dT%H:%M:%S%.f%:z",
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%.f%z",
        "%Y-%m-%d %H:%M:%S%.f%z",
        "%Y-%m-%dT%H:%M%:z",
        "%Y-%m-%d %H:%M%:z",
    ] {
        if let Ok(parsed) = DateTime::parse_from_str(value, format) {
            return Some(parsed.timestamp());
        }
    }
    for format in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(value, format) {
            return Some(parsed.and_utc().timestamp());
        }
    }
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|midnight| midnight.and_utc().timestamp())
}

fn current_unix_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)
        })
}

/// Owner candidates for failure triage, served from the read model when
/// it is fresh and from replay otherwise.
///
/// `now_ts` anchors the closed-task lookback; `None` means the current
/// time.
pub fn triage_owner_candidates(
    beads_dir: &Path,
    now_ts: Option<i64>,
) -> Result<BeadTriageOwnerCandidatesWire, BeadError> {
    let now_ts = now_ts.unwrap_or_else(current_unix_ts);
    if let Some(cached) =
        super::read_model::cached_triage_owner_candidates(beads_dir, now_ts)?
    {
        return cached;
    }
    Ok(triage_owner_candidates_in_issues(
        &read_store_issues(beads_dir)?,
        now_ts,
    ))
}

/// The replay lane: project owner candidates from hydrated issues.
pub(crate) fn triage_owner_candidates_in_issues(
    issues: &[IssueWire],
    now_ts: i64,
) -> BeadTriageOwnerCandidatesWire {
    let mut eligible: Vec<&IssueWire> = issues
        .iter()
        .filter(|issue| {
            issue.issue_type == IssueTypeWire::Task
                && is_owner_task_type(issue.task_type.as_deref())
        })
        .collect();
    // Stable, so equal `created_at` keeps replay order, matching the
    // indexed lane's `ORDER BY created_at, position`.
    eligible.sort_by(|left, right| left.created_at.cmp(&right.created_at));
    project_owner_candidates(
        eligible.into_iter().map(|issue| OwnerCandidateRow {
            id: issue.id.clone(),
            title: issue.title.clone(),
            closed: issue.status == StatusWire::Closed,
            closed_at: issue.closed_at.clone(),
            node_id_field: issue.task_type_fields.get("node_id").cloned(),
            location_field: issue.task_type_fields.get("location").cloned(),
        }),
        now_ts,
    )
}

#[cfg(test)]
mod tests;
