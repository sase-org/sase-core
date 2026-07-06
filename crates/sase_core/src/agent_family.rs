//! Deterministic helpers for user-initiated agent-family attachment.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const AGENT_FAMILY_RESOLUTION_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFamilyParentCandidateWire {
    pub name: String,
    #[serde(default)]
    pub workflow_name: Option<String>,
    pub project_name: String,
    pub artifact_dir: String,
    pub timestamp: String,
    pub cl_name: String,
    #[serde(default)]
    pub raw_suffix: Option<String>,
    #[serde(default)]
    pub parent_timestamp: Option<String>,
    #[serde(default)]
    pub is_terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFamilyDismissedIdentityWire {
    pub agent_type: String,
    pub cl_name: String,
    #[serde(default)]
    pub raw_suffix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFamilyParentResolutionRequestWire {
    pub schema_version: u32,
    pub parent_name: String,
    pub project_name: String,
    #[serde(default)]
    pub candidates: Vec<AgentFamilyParentCandidateWire>,
    #[serde(default)]
    pub dismissed: Vec<AgentFamilyDismissedIdentityWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFamilyParentResolutionWire {
    pub schema_version: u32,
    pub kind: String,
    #[serde(default)]
    pub parent: Option<AgentFamilyParentCandidateWire>,
    #[serde(default)]
    pub candidates: Vec<AgentFamilyParentCandidateWire>,
}

pub fn resolve_agent_family_parent(
    request: AgentFamilyParentResolutionRequestWire,
) -> Result<AgentFamilyParentResolutionWire, String> {
    if request.schema_version != AGENT_FAMILY_RESOLUTION_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported AgentFamilyParentResolutionRequestWire schema_version {}; expected {}",
            request.schema_version, AGENT_FAMILY_RESOLUTION_WIRE_SCHEMA_VERSION
        ));
    }

    let mut matching: Vec<AgentFamilyParentCandidateWire> = request
        .candidates
        .into_iter()
        .filter(|candidate| candidate.project_name == request.project_name)
        .filter(|candidate| candidate.parent_timestamp.is_none())
        .filter(|candidate| {
            candidate.name == request.parent_name
                || candidate.workflow_name.as_deref()
                    == Some(request.parent_name.as_str())
        })
        .collect();

    matching.sort_by(|left, right| {
        right
            .timestamp
            .cmp(&left.timestamp)
            .then_with(|| left.artifact_dir.cmp(&right.artifact_dir))
    });

    if matching.is_empty() {
        return Ok(result("absent", None, Vec::new()));
    }

    let dismissed = dismissed_indexes(&request.dismissed);
    let (visible, dismissed_matches): (Vec<_>, Vec<_>) = matching
        .into_iter()
        .partition(|candidate| !candidate_is_dismissed(candidate, &dismissed));

    if visible.is_empty() {
        return Ok(result("dismissed", None, dismissed_matches));
    }

    let newest_timestamp = visible[0].timestamp.clone();
    let newest: Vec<_> = visible
        .iter()
        .filter(|candidate| candidate.timestamp == newest_timestamp)
        .cloned()
        .collect();
    if newest.len() > 1 {
        return Ok(result("ambiguous", None, newest));
    }

    let parent = newest[0].clone();
    if !parent.is_terminal {
        return Ok(result("running", Some(parent.clone()), vec![parent]));
    }

    Ok(result("resolved", Some(parent.clone()), vec![parent]))
}

type DismissedIndexes = (BTreeSet<(String, String, String)>, BTreeSet<String>);

fn dismissed_indexes(
    dismissed: &[AgentFamilyDismissedIdentityWire],
) -> DismissedIndexes {
    let mut exact = BTreeSet::new();
    let mut suffixes = BTreeSet::new();
    for identity in dismissed {
        let Some(raw_suffix) = identity.raw_suffix.as_ref() else {
            continue;
        };
        exact.insert((
            identity.agent_type.clone(),
            identity.cl_name.clone(),
            raw_suffix.clone(),
        ));
        suffixes.insert(raw_suffix.clone());
    }
    (exact, suffixes)
}

fn candidate_is_dismissed(
    candidate: &AgentFamilyParentCandidateWire,
    dismissed: &DismissedIndexes,
) -> bool {
    let Some(raw_suffix) = candidate.raw_suffix.as_ref() else {
        return false;
    };
    dismissed.0.contains(&(
        "workflow".to_string(),
        candidate.cl_name.clone(),
        raw_suffix.clone(),
    )) || dismissed.0.contains(&(
        "run".to_string(),
        candidate.cl_name.clone(),
        raw_suffix.clone(),
    )) || dismissed.1.contains(raw_suffix)
}

fn result(
    kind: &str,
    parent: Option<AgentFamilyParentCandidateWire>,
    candidates: Vec<AgentFamilyParentCandidateWire>,
) -> AgentFamilyParentResolutionWire {
    AgentFamilyParentResolutionWire {
        schema_version: AGENT_FAMILY_RESOLUTION_WIRE_SCHEMA_VERSION,
        kind: kind.to_string(),
        parent,
        candidates,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(
        name: &str,
        timestamp: &str,
    ) -> AgentFamilyParentCandidateWire {
        AgentFamilyParentCandidateWire {
            name: name.to_string(),
            workflow_name: None,
            project_name: "sase".to_string(),
            artifact_dir: format!("/tmp/{timestamp}"),
            timestamp: timestamp.to_string(),
            cl_name: "sase".to_string(),
            raw_suffix: Some(timestamp.to_string()),
            parent_timestamp: None,
            is_terminal: true,
        }
    }

    fn request(
        candidates: Vec<AgentFamilyParentCandidateWire>,
    ) -> AgentFamilyParentResolutionRequestWire {
        AgentFamilyParentResolutionRequestWire {
            schema_version: AGENT_FAMILY_RESOLUTION_WIRE_SCHEMA_VERSION,
            parent_name: "foo".to_string(),
            project_name: "sase".to_string(),
            candidates,
            dismissed: Vec::new(),
        }
    }

    #[test]
    fn newest_terminal_visible_parent_wins() {
        let result = resolve_agent_family_parent(request(vec![
            candidate("foo", "20260701010101"),
            candidate("foo", "20260702020202"),
        ]))
        .unwrap();

        assert_eq!(result.kind, "resolved");
        assert_eq!(result.parent.unwrap().timestamp, "20260702020202");
    }

    #[test]
    fn absent_parent_reports_absent() {
        let result = resolve_agent_family_parent(request(vec![candidate(
            "bar",
            "20260701010101",
        )]))
        .unwrap();

        assert_eq!(result.kind, "absent");
        assert!(result.candidates.is_empty());
    }

    #[test]
    fn dismissed_parent_reports_dismissed() {
        let mut req = request(vec![candidate("foo", "20260701010101")]);
        req.dismissed.push(AgentFamilyDismissedIdentityWire {
            agent_type: "workflow".to_string(),
            cl_name: "sase".to_string(),
            raw_suffix: Some("20260701010101".to_string()),
        });

        let result = resolve_agent_family_parent(req).unwrap();

        assert_eq!(result.kind, "dismissed");
        assert_eq!(result.candidates[0].timestamp, "20260701010101");
    }

    #[test]
    fn non_terminal_parent_reports_running() {
        let mut active = candidate("foo", "20260701010101");
        active.is_terminal = false;

        let result =
            resolve_agent_family_parent(request(vec![active])).unwrap();

        assert_eq!(result.kind, "running");
        assert_eq!(result.parent.unwrap().timestamp, "20260701010101");
    }

    #[test]
    fn duplicate_newest_timestamp_reports_ambiguous() {
        let mut first = candidate("foo", "20260701010101");
        first.artifact_dir = "/tmp/a".to_string();
        let mut second = candidate("foo", "20260701010101");
        second.artifact_dir = "/tmp/b".to_string();

        let result =
            resolve_agent_family_parent(request(vec![first, second])).unwrap();

        assert_eq!(result.kind, "ambiguous");
        assert_eq!(result.candidates.len(), 2);
    }
}
