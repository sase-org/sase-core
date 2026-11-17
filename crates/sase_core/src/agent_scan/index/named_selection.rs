//! Rust-owned candidate eligibility and ranking for named-agent lookup.

use super::lookup::AgentArtifactLookupHitWire;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedAgentObservationWire {
    pub artifact_dir: String,
    #[serde(default)]
    pub process_alive: bool,
    #[serde(default)]
    pub done_outcome: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedAgentSelectionRequestWire {
    pub names: Vec<String>,
    #[serde(default)]
    pub hits: Vec<AgentArtifactLookupHitWire>,
    #[serde(default)]
    pub observations: Vec<NamedAgentObservationWire>,
    #[serde(default)]
    pub only_done: bool,
    #[serde(default)]
    pub include_dead: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedAgentSelectionHitWire {
    pub requested_name: String,
    pub hit: AgentArtifactLookupHitWire,
    pub is_done: bool,
    pub outcome: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedAgentSelectionResultWire {
    pub selected: Option<NamedAgentSelectionHitWire>,
    pub selections: Vec<Option<NamedAgentSelectionHitWire>>,
}

/// Select the first eligible spelling, then rank its candidates by state,
/// exactness, and timestamp. Eligibility is evaluated for every spelling so
/// an ineligible workflow match cannot hide a later exact match.
pub fn select_named_agent(
    request: NamedAgentSelectionRequestWire,
) -> NamedAgentSelectionResultWire {
    let observations: HashMap<&str, &NamedAgentObservationWire> = request
        .observations
        .iter()
        .map(|observation| (observation.artifact_dir.as_str(), observation))
        .collect();

    let mut selections = Vec::with_capacity(request.names.len());
    for requested_name in &request.names {
        let mut best: Option<((u8, bool, String), NamedAgentSelectionHitWire)> =
            None;
        for hit in &request.hits {
            if hit.workflow_dir_name != "ace-run" {
                continue;
            }
            let exact = hit.agent_name.as_deref() == Some(requested_name);
            let workflow = hit.workflow_name.as_deref() == Some(requested_name);
            if !exact && !workflow {
                continue;
            }

            let observation =
                observations.get(hit.artifact_dir.as_str()).copied();
            let prefixed_dismissal = is_dismissed_prefixed(requested_name);
            let is_done = hit.has_done_marker || prefixed_dismissal;
            let outcome = if hit.has_done_marker {
                observation.and_then(|item| item.done_outcome.clone())
            } else if prefixed_dismissal {
                Some("dismissed".to_string())
            } else {
                None
            };

            let state_priority = if is_done {
                1
            } else {
                if request.only_done {
                    continue;
                }
                if !exact && hit.parent_timestamp.is_some() {
                    continue;
                }
                let alive = observation.is_some_and(|item| item.process_alive);
                if !alive && !request.include_dead {
                    continue;
                }
                if request.include_dead && !alive {
                    2
                } else {
                    3
                }
            };

            let candidate = NamedAgentSelectionHitWire {
                requested_name: requested_name.clone(),
                hit: hit.clone(),
                is_done,
                outcome,
            };
            let priority = (state_priority, exact, hit.timestamp.clone());
            if best.as_ref().is_none_or(|(current, _)| priority > *current) {
                best = Some((priority, candidate));
            }
        }
        selections.push(best.map(|(_, selected)| selected));
    }

    let selected = selections.iter().flatten().next().cloned();
    NamedAgentSelectionResultWire {
        selected,
        selections,
    }
}

fn is_dismissed_prefixed(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() >= 7
        && bytes[..6].iter().all(u8::is_ascii_digit)
        && bytes[6] == b'.'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(
        agent_name: &str,
        workflow_name: Option<&str>,
        timestamp: &str,
        parent_timestamp: Option<&str>,
        has_done_marker: bool,
    ) -> AgentArtifactLookupHitWire {
        AgentArtifactLookupHitWire {
            artifact_dir: format!("/projects/p/ace-run/{timestamp}"),
            project_name: "p".to_string(),
            workflow_dir_name: "ace-run".to_string(),
            timestamp: timestamp.to_string(),
            agent_name: Some(agent_name.to_string()),
            workflow_name: workflow_name.map(str::to_string),
            agent_session: None,
            lookup_session: None,
            agent_clan: None,
            agent_clan_generation: None,
            parent_timestamp: parent_timestamp.map(str::to_string),
            has_done_marker,
            has_running_marker: !has_done_marker,
            hidden: false,
            match_kind: None,
        }
    }

    fn observation(
        hit: &AgentArtifactLookupHitWire,
        alive: bool,
    ) -> NamedAgentObservationWire {
        NamedAgentObservationWire {
            artifact_dir: hit.artifact_dir.clone(),
            process_alive: alive,
            done_outcome: None,
        }
    }

    #[test]
    fn exact_live_child_survives_ineligible_earlier_workflow_spelling() {
        let child = hit(
            "alice.athena.foo",
            Some("foo"),
            "2026111701",
            Some("root"),
            false,
        );
        let selected = select_named_agent(NamedAgentSelectionRequestWire {
            names: vec!["foo".to_string(), "alice.athena.foo".to_string()],
            hits: vec![child.clone()],
            observations: vec![observation(&child, true)],
            only_done: false,
            include_dead: false,
        })
        .selected
        .unwrap();
        assert_eq!(selected.requested_name, "alice.athena.foo");
        assert!(!selected.is_done);

        let ranked = select_named_agent(NamedAgentSelectionRequestWire {
            names: vec!["foo".to_string(), "alice.athena.foo".to_string()],
            hits: vec![child.clone()],
            observations: vec![observation(&child, true)],
            only_done: false,
            include_dead: false,
        });
        assert!(ranked.selections[0].is_none());
        assert_eq!(
            ranked.selections[1].as_ref().unwrap().requested_name,
            "alice.athena.foo"
        );
    }

    #[test]
    fn candidate_spelling_order_precedes_state_and_exactness() {
        let earlier = hit("foo", None, "2026111701", None, true);
        let later = hit("alice.athena.foo", None, "2026111702", None, false);
        let selected = select_named_agent(NamedAgentSelectionRequestWire {
            names: vec!["foo".to_string(), "alice.athena.foo".to_string()],
            hits: vec![earlier.clone(), later.clone()],
            observations: vec![
                observation(&earlier, false),
                observation(&later, true),
            ],
            only_done: false,
            include_dead: false,
        })
        .selected
        .unwrap();
        assert_eq!(selected.requested_name, "foo");
        assert!(selected.is_done);
    }

    #[test]
    fn running_workflow_beats_done_exact_within_one_spelling() {
        let exact = hit("foo", None, "2026111702", None, true);
        let workflow = hit("child", Some("foo"), "2026111701", None, false);
        let selected = select_named_agent(NamedAgentSelectionRequestWire {
            names: vec!["foo".to_string()],
            hits: vec![exact.clone(), workflow.clone()],
            observations: vec![
                observation(&exact, false),
                observation(&workflow, true),
            ],
            only_done: false,
            include_dead: false,
        })
        .selected
        .unwrap();
        assert_eq!(selected.hit.agent_name.as_deref(), Some("child"));
    }

    #[test]
    fn dismissed_prefix_is_historical_without_done_marker() {
        let dismissed = hit("260428.foo", None, "2026111701", None, false);
        let selected = select_named_agent(NamedAgentSelectionRequestWire {
            names: vec!["260428.foo".to_string()],
            hits: vec![dismissed],
            observations: Vec::new(),
            only_done: true,
            include_dead: false,
        })
        .selected
        .unwrap();
        assert!(selected.is_done);
        assert_eq!(selected.outcome.as_deref(), Some("dismissed"));
    }
}
