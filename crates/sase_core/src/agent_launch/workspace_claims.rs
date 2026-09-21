//! Workspace-claim file content: parse, plan, transfer, allocate, and
//! occupant-conflict decisions for numbered launch workspaces.
use super::wires::{
    OccupancyCallerWire, OccupancyConflictDecisionWire, OccupantRecordWire,
    WorkspaceClaimOutcomeWire, WorkspaceClaimPlanWire,
    WorkspaceClaimRequestWire, WorkspaceClaimWire,
};
use std::collections::BTreeSet;

pub fn list_workspace_claims_from_content(
    content: &str,
) -> Vec<WorkspaceClaimWire> {
    let mut claims = Vec::new();
    let mut in_running_field = false;

    for line in content.split('\n') {
        if line.starts_with("RUNNING:") {
            in_running_field = true;
            continue;
        }
        if !in_running_field {
            continue;
        }
        if !is_running_continuation_line(line) {
            break;
        }
        if let Some(claim) = WorkspaceClaimLine::parse(line) {
            claims.push(claim.into_wire());
        }
    }

    claims
}

pub fn plan_claim_workspace_from_content(
    content: &str,
    request: &WorkspaceClaimRequestWire,
) -> WorkspaceClaimPlanWire {
    let mut lines: Vec<String> =
        content.split('\n').map(ToString::to_string).collect();
    let (_running_idx, running_end_idx) = find_running_field_bounds(&lines);

    if request.workspace_num != 0 {
        for line in running_claim_lines(&lines) {
            if let Some(existing) = WorkspaceClaimLine::parse(line) {
                if existing.workspace_num == request.workspace_num {
                    return claim_plan(
                        content.to_string(),
                        false,
                        request,
                        Some(format!(
                            "workspace #{} is already claimed",
                            request.workspace_num
                        )),
                        false,
                    );
                }
            }
        }
    }

    let new_claim = WorkspaceClaimLine::from_request(request);
    if let Some(end) = running_end_idx {
        lines.insert(end + 1, new_claim.to_line());
    } else {
        lines.insert(0, String::new());
        lines.insert(0, new_claim.to_line());
        lines.insert(0, "RUNNING:".to_string());
    }

    claim_plan(
        normalize_running_field_spacing(&lines.join("\n")),
        true,
        request,
        None,
        true,
    )
}

pub fn plan_transfer_workspace_claim_from_content(
    content: &str,
    request: &WorkspaceClaimRequestWire,
) -> WorkspaceClaimPlanWire {
    let Some(from_pid) = request.transfer_from_pid else {
        return claim_plan(
            content.to_string(),
            false,
            request,
            Some("transfer_from_pid is required".to_string()),
            false,
        );
    };

    let mut lines: Vec<String> =
        content.split('\n').map(ToString::to_string).collect();
    let mut in_running_field = false;

    for line in &mut lines {
        if line.starts_with("RUNNING:") {
            in_running_field = true;
            continue;
        }
        if in_running_field && is_running_continuation_line(line) {
            if let Some(claim) = WorkspaceClaimLine::parse(line) {
                let cl_matches = request.workspace_num != 0
                    || request.cl_name.is_empty()
                    || claim.cl_name.as_deref()
                        == Some(request.cl_name.as_str());
                if claim.workspace_num == request.workspace_num
                    && claim.pid == from_pid
                    && cl_matches
                {
                    let replacement = claim.transfer_to(request);
                    *line = replacement.to_line();
                    return claim_plan(
                        lines.join("\n"),
                        true,
                        request,
                        None,
                        true,
                    );
                }
            }
        } else {
            in_running_field = false;
        }
    }

    claim_plan(
        content.to_string(),
        false,
        request,
        Some(format!(
            "workspace #{} with pid {from_pid} was not found",
            request.workspace_num
        )),
        false,
    )
}

pub fn allocate_and_claim_workspace_from_content(
    content: &str,
    min_workspace: u32,
    max_workspace: u32,
    request: &WorkspaceClaimRequestWire,
) -> WorkspaceClaimPlanWire {
    let claimed: BTreeSet<u32> = list_workspace_claims_from_content(content)
        .into_iter()
        .map(|claim| claim.workspace_num)
        .collect();
    let Some(workspace_num) =
        (min_workspace..=max_workspace).find(|n| !claimed.contains(n))
    else {
        return claim_plan(
            content.to_string(),
            false,
            request,
            Some(format!(
                "all workspaces ({min_workspace}-{max_workspace}) are claimed"
            )),
            false,
        );
    };

    let mut allocated_request = request.clone();
    allocated_request.workspace_num = workspace_num;
    plan_claim_workspace_from_content(content, &allocated_request)
}

fn claim_plan(
    content: String,
    success: bool,
    request: &WorkspaceClaimRequestWire,
    error: Option<String>,
    changed: bool,
) -> WorkspaceClaimPlanWire {
    WorkspaceClaimPlanWire {
        content,
        outcome: WorkspaceClaimOutcomeWire {
            success,
            workspace_num: request.workspace_num,
            project_file: request.project_file.clone(),
            pid: Some(request.pid),
            error,
        },
        changed,
    }
}

/// Decide whether a destructive workspace-preparation step (clean, reset,
/// checkout) may proceed against a checkout that may be occupied by another
/// live agent.
///
/// `occupant` is the parsed `.sase/occupant.json` record for the checkout,
/// if one exists; a missing record is always treated as unoccupied so
/// checkouts created before this guard existed are never bricked.
/// `occupant_pid_alive` and `running_claim_pid_alive` are supplied by the
/// caller, which alone knows how to probe process liveness.  `running_claim`
/// is the RUNNING-field claim row for `caller.workspace_num`, used only to
/// cross-check against the occupant record; a disagreement between the two
/// sources of truth is itself treated as a conflict.
pub fn decide_workspace_occupant_conflict(
    occupant: Option<&OccupantRecordWire>,
    caller: &OccupancyCallerWire,
    occupant_pid_alive: bool,
    running_claim: Option<&WorkspaceClaimWire>,
    running_claim_pid_alive: bool,
) -> OccupancyConflictDecisionWire {
    let Some(occupant) = occupant else {
        return OccupancyConflictDecisionWire {
            may_proceed: true,
            conflict: false,
            reason:
                "no occupant record present; treating checkout as unoccupied"
                    .to_string(),
        };
    };

    let occupant_is_live_other =
        occupant.pid != caller.pid && occupant_pid_alive;
    let claim_is_live_other = running_claim
        .map(|claim| claim.pid != caller.pid && running_claim_pid_alive)
        .unwrap_or(false);

    if !occupant_is_live_other {
        if claim_is_live_other {
            let claim =
                running_claim.expect("claim_is_live_other implies Some");
            return OccupancyConflictDecisionWire {
                may_proceed: false,
                conflict: true,
                reason: format!(
                    "occupant record for workspace #{} is stale but the RUNNING \
                     field still claims it for pid {} (workflow {}); refusing to \
                     prepare until that claim is resolved",
                    caller.workspace_num, claim.pid, claim.workflow
                ),
            };
        }
        return OccupancyConflictDecisionWire {
            may_proceed: true,
            conflict: false,
            reason: if occupant.pid == caller.pid {
                "caller already holds this checkout".to_string()
            } else {
                format!(
                    "occupant pid {} is not alive; treating as stale and allowing \
                     takeover",
                    occupant.pid
                )
            },
        };
    }

    let disagrees_with_running_field = match running_claim {
        Some(claim) => claim.pid != occupant.pid,
        None => true,
    };
    let occupant_label = occupant
        .agent_name
        .clone()
        .unwrap_or_else(|| occupant.workflow.clone());
    let artifacts_part = occupant
        .artifacts_timestamp
        .as_ref()
        .map(|ts| format!(", artifacts {ts}"))
        .unwrap_or_default();
    let mut reason = format!(
        "workspace #{} checkout is occupied by {} (pid {}, live{})",
        caller.workspace_num, occupant_label, occupant.pid, artifacts_part
    );
    if disagrees_with_running_field {
        reason.push_str(
            "; RUNNING field and occupant record disagree, which itself \
             indicates a corrupted claim state",
        );
    }
    OccupancyConflictDecisionWire {
        may_proceed: false,
        conflict: true,
        reason,
    }
}

fn running_claim_lines(lines: &[String]) -> impl Iterator<Item = &str> {
    let (start, end) = find_running_field_bounds(lines);
    let start = start.unwrap_or(0);
    let end = end.unwrap_or(0);
    lines
        .iter()
        .enumerate()
        .filter(move |(idx, _)| *idx > start && *idx <= end)
        .map(|(_, line)| line.as_str())
}

fn find_running_field_bounds(
    lines: &[String],
) -> (Option<usize>, Option<usize>) {
    for (i, line) in lines.iter().enumerate() {
        if line.starts_with("RUNNING:") {
            let mut running_end_idx = i;
            for (j, candidate) in lines.iter().enumerate().skip(i + 1) {
                if is_running_continuation_line(candidate) {
                    running_end_idx = j;
                } else {
                    break;
                }
            }
            return (Some(i), Some(running_end_idx));
        }
    }
    (None, None)
}

fn is_running_continuation_line(line: &str) -> bool {
    line.starts_with("  ")
        && (line.trim().starts_with('#') || line.trim().starts_with('|'))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspaceClaimLine {
    workspace_num: u32,
    pid: u32,
    workflow: String,
    cl_name: Option<String>,
    artifacts_timestamp: Option<String>,
    pinned: bool,
    suffix_parts: Vec<WorkspaceClaimSuffixPart>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WorkspaceClaimSuffixPart {
    Timestamp(String),
    Pinned,
    Unknown(String),
}

impl WorkspaceClaimSuffixPart {
    fn raw_value(&self) -> &str {
        match self {
            Self::Timestamp(value) | Self::Unknown(value) => value,
            Self::Pinned => "PINNED",
        }
    }
}

impl WorkspaceClaimLine {
    fn parse(line: &str) -> Option<Self> {
        let trimmed = line.trim();
        if !trimmed.starts_with('#') {
            return None;
        }
        let parts: Vec<&str> = trimmed.split('|').map(str::trim).collect();
        if parts.len() < 4 {
            return None;
        }

        let workspace_num = parts[0].strip_prefix('#')?.parse::<u32>().ok()?;
        let pid = parts[1].parse::<u32>().ok()?;
        let workflow = parts[2];
        if workflow.is_empty() {
            return None;
        }

        let mut artifacts_timestamp = None;
        let mut pinned = false;
        let mut suffix_parts = Vec::new();
        for part in parts.iter().skip(4) {
            if *part == "PINNED" {
                pinned = true;
                suffix_parts.push(WorkspaceClaimSuffixPart::Pinned);
            } else if is_timestamp_part(part) {
                let value = (*part).to_string();
                if artifacts_timestamp.is_none() {
                    artifacts_timestamp = Some(value.clone());
                    suffix_parts
                        .push(WorkspaceClaimSuffixPart::Timestamp(value));
                } else {
                    suffix_parts.push(WorkspaceClaimSuffixPart::Unknown(value));
                }
            } else {
                suffix_parts.push(WorkspaceClaimSuffixPart::Unknown(
                    (*part).to_string(),
                ));
            }
        }

        Some(Self {
            workspace_num,
            pid,
            workflow: workflow.to_string(),
            cl_name: if parts[3].is_empty() {
                None
            } else {
                Some(parts[3].to_string())
            },
            artifacts_timestamp,
            pinned,
            suffix_parts,
        })
    }

    fn from_request(request: &WorkspaceClaimRequestWire) -> Self {
        let mut suffix_parts = Vec::new();
        let artifacts_timestamp = if request.artifacts_timestamp.is_empty() {
            None
        } else {
            suffix_parts.push(WorkspaceClaimSuffixPart::Timestamp(
                request.artifacts_timestamp.clone(),
            ));
            Some(request.artifacts_timestamp.clone())
        };
        if request.pinned {
            suffix_parts.push(WorkspaceClaimSuffixPart::Pinned);
        }
        Self {
            workspace_num: request.workspace_num,
            pid: request.pid,
            workflow: request.workflow_name.clone(),
            cl_name: if request.cl_name.is_empty() {
                None
            } else {
                Some(request.cl_name.clone())
            },
            artifacts_timestamp,
            pinned: request.pinned,
            suffix_parts,
        }
    }

    fn transfer_to(&self, request: &WorkspaceClaimRequestWire) -> Self {
        let mut replacement = self.clone();
        replacement.pid = request.pid;
        replacement.workflow = request.workflow_name.clone();
        replacement.cl_name = if request.cl_name.is_empty() {
            None
        } else {
            Some(request.cl_name.clone())
        };
        if !request.artifacts_timestamp.is_empty() {
            replacement
                .set_artifacts_timestamp(request.artifacts_timestamp.clone());
        }
        replacement
    }

    fn set_artifacts_timestamp(&mut self, value: String) {
        self.artifacts_timestamp = Some(value.clone());
        for part in &mut self.suffix_parts {
            if matches!(part, WorkspaceClaimSuffixPart::Timestamp(_)) {
                *part = WorkspaceClaimSuffixPart::Timestamp(value);
                return;
            }
        }
        let insert_idx = self
            .suffix_parts
            .iter()
            .position(|part| matches!(part, WorkspaceClaimSuffixPart::Pinned))
            .unwrap_or(self.suffix_parts.len());
        self.suffix_parts
            .insert(insert_idx, WorkspaceClaimSuffixPart::Timestamp(value));
    }

    fn into_wire(self) -> WorkspaceClaimWire {
        WorkspaceClaimWire {
            workspace_num: self.workspace_num,
            workflow: self.workflow,
            cl_name: self.cl_name,
            pid: self.pid,
            artifacts_timestamp: self.artifacts_timestamp,
            pinned: self.pinned,
        }
    }

    fn to_line(&self) -> String {
        let cl_part = self.cl_name.as_deref().unwrap_or("");
        let suffix = self
            .suffix_parts
            .iter()
            .map(WorkspaceClaimSuffixPart::raw_value)
            .collect::<Vec<_>>()
            .join(" | ");
        let suffix_part = if suffix.is_empty() {
            String::new()
        } else {
            format!(" | {suffix}")
        };
        format!(
            "  #{} | {} | {} | {}{}",
            self.workspace_num, self.pid, self.workflow, cl_part, suffix_part
        )
    }
}

fn is_timestamp_part(value: &str) -> bool {
    (value.len() == 14 && value.as_bytes().iter().all(u8::is_ascii_digit))
        || (value.len() == 15
            && value.as_bytes()[0..8].iter().all(u8::is_ascii_digit)
            && value.as_bytes()[8] == b'_'
            && value.as_bytes()[9..15].iter().all(u8::is_ascii_digit))
        || (value.len() == 13
            && value.as_bytes()[0..6].iter().all(u8::is_ascii_digit)
            && value.as_bytes()[6] == b'_'
            && value.as_bytes()[7..13].iter().all(u8::is_ascii_digit))
}

fn normalize_running_field_spacing(content: &str) -> String {
    let lines: Vec<&str> = content.split('\n').collect();
    let mut result_lines = Vec::with_capacity(lines.len());
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i];
        if line.starts_with("RUNNING:") {
            result_lines.push(line.to_string());
            i += 1;
            while i < lines.len() {
                let entry_line = lines[i];
                if entry_line.starts_with("  ")
                    && entry_line.trim().starts_with('#')
                {
                    result_lines.push(entry_line.to_string());
                    i += 1;
                } else {
                    break;
                }
            }
            while i < lines.len() && lines[i].trim().is_empty() {
                i += 1;
            }
            if i < lines.len() {
                result_lines.push(String::new());
                result_lines.push(String::new());
            }
        } else {
            result_lines.push(line.to_string());
            i += 1;
        }
    }

    result_lines.join("\n")
}
